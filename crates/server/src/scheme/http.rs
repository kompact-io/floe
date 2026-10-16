//! http: `floe-server`'s REST transport -- one route per RPC
//! `scheme::grpc`'s `QueryService`/`SubmitService` implements (see
//! `../../../plan-dto-http.md`): `ReadUtxos`, `SearchUtxos`, `ReadData`,
//! `ReadTx`, `UtxoChanges`, `SubmitTx`, all over the same
//! `store::query` layer. DTO conversion is its own pass here,
//! separate from `scheme::grpc`'s proto conversion, same split
//! `domain.rs` already draws between transport-agnostic domain logic
//! (`reconstruct_address`) and per-transport assembly.
//!
//! Wire format is chosen per request by the `Accept` header
//! (`application/cbor` for CBOR, anything else for JSON). `POST /tx`'s
//! body is always raw tx bytes (mirroring gRPC's `AnyChainTx.raw`);
//! only its response is format-negotiated.
//!
//! `metrics` (below) is `/health`/`/metrics` -- reporting apparatus
//! over the crate-root `health`/`metrics` core, not this scheme's own
//! query routes.

pub mod metrics;

use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
// `axum_extra`'s `Query` (serde_html_form), not `axum::extract::Query`
// (serde_urlencoded) -- the latter can't collect a repeated key
// (`?ref=a&ref=b`) into a `Vec<String>` (`UtxosQuery::refs`).
use axum_extra::extract::Query;
use pallas_addresses::Network;
use pallas_codec::minicbor as pallas_minicbor;
use pallas_codec::utils::PositiveCoin;
use pallas_primitives::conway::Multiasset;
use rusqlite::Connection;
use serde::Deserialize;
use tracing::info;

use floe_dto_http::{
    AddressDto, AssetDto, ChainPointDto, ChangesDto, ChangesResponseDto, DatumDto,
    DatumResponseDto, ErrorBody, MultiassetDto, PageDto, ScriptDto, StaleDto, StaleReasonDto,
    SubmitTxResponseDto, TxDto, TxResponseDto, UtxoDto,
};

use crate::domain::reconstruct_address;
use crate::decode::OutputRef;
use crate::query::{self, BlockPoint, ResolvedDatum, ResolvedTx, ResolvedUtxo, StaleReason};
use crate::submit;

#[derive(Clone)]
struct AppState {
    db_path: Arc<str>,
    network: Network,
    max_page_size: u32,
    /// `None` means `/tx` always 503s instead of forwarding anywhere
    /// -- the REST equivalent of gRPC's `SubmitService` not mounted.
    amaru_submit_api: Option<String>,
}

/// Each request opens its own short-lived read-only DB connection
/// inside `spawn_blocking`, same policy `grpc.rs`/`metrics.rs` follow.
pub async fn serve(
    addr: &str,
    db_path: Arc<str>,
    network: Network,
    max_page_size: u32,
    amaru_submit_api: Option<String>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, max_page_size, submit = amaru_submit_api.is_some(), "http server listening");
    axum::serve(
        listener,
        build_router(AppState {
            db_path,
            network,
            max_page_size,
            amaru_submit_api,
        }),
    )
    .await?;
    Ok(())
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/utxos", get(utxos_handler))
        .route("/data/{hash}", get(data_handler))
        .route("/tx/{hash}", get(tx_handler))
        .route("/changes", get(changes_handler))
        .route("/tx", post(submit_handler))
        .with_state(state)
}

// --- error model -----------------------------------------------------
//
// Mirrors `grpc.rs`'s `tonic::Code` choices onto HTTP status:
// invalid_argument -> 400, not_found -> 404, unavailable (DB
// mid-catchup, or amaru's submit-api unreachable) -> 503, else 500.
// An RPC gRPC doesn't implement simply has no route -- a 404 from the
// router stands in for `unimplemented`.

enum ApiError {
    BadRequest(String),
    NotFound(String),
    Unavailable(String),
    Internal(String),
}

impl ApiError {
    fn bad_request(msg: impl Into<String>) -> Self {
        Self::BadRequest(msg.into())
    }
    fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }
    fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    fn message(self) -> String {
        match self {
            Self::BadRequest(m) | Self::NotFound(m) | Self::Unavailable(m) | Self::Internal(m) => {
                m
            }
        }
    }
}

fn join_err(e: tokio::task::JoinError) -> ApiError {
    ApiError::Internal(e.to_string())
}

/// `query::NotReady` (still in catch-up) maps to 503, same as
/// `grpc.rs`'s `UNAVAILABLE`; everything else is 500.
fn map_err(e: anyhow::Error) -> ApiError {
    if e.downcast_ref::<query::NotReady>().is_some() {
        ApiError::Unavailable(e.to_string())
    } else {
        ApiError::Internal(e.to_string())
    }
}

// --- wire format negotiation ------------------------------------------

#[derive(Clone, Copy)]
enum Format {
    Json,
    Cbor,
}

fn negotiate(headers: &HeaderMap) -> Format {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.contains("application/cbor") {
        Format::Cbor
    } else {
        Format::Json
    }
}

fn render<T>(fmt: Format, status: StatusCode, value: &T) -> Response
where
    T: serde::Serialize + minicbor::Encode<()>,
{
    match fmt {
        Format::Json => {
            let body = serde_json::to_vec(value).expect("dto JSON serialization is infallible");
            (
                status,
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response()
        }
        Format::Cbor => {
            let mut buf = Vec::new();
            minicbor::encode(value, &mut buf).expect("dto CBOR encoding is infallible");
            (status, [(header::CONTENT_TYPE, "application/cbor")], buf).into_response()
        }
    }
}

fn err_response(fmt: Format, e: ApiError) -> Response {
    let status = e.status();
    render(fmt, status, &ErrorBody { error: e.message() })
}

// --- domain -> DTO conversion ------------------------------------------
//
// Own small pass over the same `store::query` inputs `grpc.rs`'s
// `any_utxo_data`/`any_chain_tx` convert (see module doc for why
// they're separate, and floe-dto-http's own doc for the
// ScriptDto/assets simplifications).

fn chain_point_dto(p: Option<BlockPoint>) -> ChainPointDto {
    match p {
        None => ChainPointDto::default(),
        Some(p) => ChainPointDto {
            slot: p.slot,
            hash: p.hash.to_vec(),
            height: p.height.unwrap_or(0),
            timestamp: 0,
        },
    }
}

/// Decodes the raw CBOR multiasset map floe stores into
/// `MultiassetDto` -- same source bytes and pallas type `grpc.rs`'s
/// `decode_assets` decodes, into a different target shape.
fn asset_dtos(bytes: &[u8]) -> Result<Vec<MultiassetDto>> {
    let ma: Multiasset<PositiveCoin> = pallas_minicbor::decode(bytes)?;
    Ok(ma
        .into_iter()
        .map(|(policy, assets)| MultiassetDto {
            policy_id: policy.to_vec(),
            assets: assets
                .into_iter()
                .map(|(name, qty)| AssetDto {
                    name: name.to_vec(),
                    quantity: u64::from(qty),
                })
                .collect(),
        })
        .collect())
}

fn datum_dto(d: ResolvedDatum) -> Option<DatumDto> {
    match d {
        ResolvedDatum::None => None,
        ResolvedDatum::Hash(h) => Some(DatumDto {
            hash: h,
            original_cbor: None,
        }),
        ResolvedDatum::HashResolved(h, cbor) | ResolvedDatum::Inline(h, cbor) => Some(DatumDto {
            hash: h,
            original_cbor: Some(cbor),
        }),
    }
}

fn utxo_dto(conn: &Connection, u: ResolvedUtxo, network: Network) -> Result<UtxoDto> {
    let block = chain_point_dto(query::block_point(conn, u.slot)?);
    let address = AddressDto(reconstruct_address(&u, network));
    let assets = match &u.assets {
        None => vec![],
        Some(bytes) => asset_dtos(bytes)?,
    };
    Ok(UtxoDto {
        tx_hash: u.output_ref.tx_hash,
        ix: u.output_ref.ix,
        block,
        spent_by: u.spent_by,
        address,
        coin: u.coin,
        assets,
        datum: datum_dto(u.datum.clone()),
        script: u
            .script
            .clone()
            .map(|s| ScriptDto {
                lang: s.lang,
                cbor: s.cbor,
            }),
    })
}

/// `ReadTx`'s own, deliberately leaner `UtxoDto` for a tx's
/// inputs/outputs -- no `block` (no DB lookup for it) and no
/// `spent_by` (always `None`), unlike `utxo_dto`'s full hydration for
/// every other route. This isn't a REST limitation (nothing stops
/// `utxo_dto` from being reused here) -- it's `ReadTx` reporting the
/// same thing on both transports: `scheme::grpc`'s `ReadTx` equivalent
/// serializes `TxOutput`/`TxInput` (`cardano.proto`), proto messages
/// with no field for either one, so gRPC genuinely cannot do better.
/// Matching that here keeps `ReadTx`'s content identical across
/// schemes rather than letting REST, incidentally, say more just
/// because its own domain layer happens to have the data on hand.
///
/// `spent_by` in particular would otherwise be curious for an input:
/// *trivially* "this tx" for every input (it's already implied by
/// being listed under `inputs`, not information `ReadTx` is
/// recovering), but genuinely unknown for an output -- `None` here
/// doesn't mean "confirmedly unspent" the way it does from
/// `utxo_dto`/every other route, just "`ReadTx` doesn't say."
fn tx_row_dto(u: ResolvedUtxo, network: Network) -> Result<UtxoDto> {
    let address = AddressDto(reconstruct_address(&u, network));
    let assets = match &u.assets {
        None => vec![],
        Some(bytes) => asset_dtos(bytes)?,
    };
    Ok(UtxoDto {
        tx_hash: u.output_ref.tx_hash,
        ix: u.output_ref.ix,
        block: ChainPointDto::default(),
        spent_by: None,
        address,
        coin: u.coin,
        assets,
        datum: datum_dto(u.datum.clone()),
        script: u
            .script
            .clone()
            .map(|s| ScriptDto {
                lang: s.lang,
                cbor: s.cbor,
            }),
    })
}

fn tx_dto(conn: &Connection, t: ResolvedTx, network: Network) -> Result<TxDto> {
    let block = chain_point_dto(query::block_point(conn, t.block_slot)?);
    let inputs = t
        .inputs
        .into_iter()
        .map(|u| tx_row_dto(u, network))
        .collect::<Result<Vec<_>>>()?;
    let outputs = t
        .outputs
        .into_iter()
        .map(|u| tx_row_dto(u, network))
        .collect::<Result<Vec<_>>>()?;
    Ok(TxDto {
        hash: t.hash,
        block,
        inputs,
        outputs,
        // Hardcoded, same tracked gap as `grpc.rs`'s `any_chain_tx` --
        // see `TODO.md`.
        successful: true,
    })
}

fn stale_dto(reason: StaleReason) -> StaleDto {
    match reason {
        StaleReason::Unknown => StaleDto {
            reason: StaleReasonDto::Unknown,
            oldest_retained_slot: None,
        },
        StaleReason::RolledBack {
            oldest_retained_slot,
        } => StaleDto {
            reason: StaleReasonDto::RolledBack,
            oldest_retained_slot: Some(oldest_retained_slot),
        },
        StaleReason::TooOld {
            oldest_retained_slot,
        } => StaleDto {
            reason: StaleReasonDto::TooOld,
            oldest_retained_slot: Some(oldest_retained_slot),
        },
    }
}

// --- shared request-parsing helpers ------------------------------------

/// Mirrors `grpc.rs`'s own `<tx_hash_hex>:<ix>` cursor format --
/// shared here between `ReadUtxos`'s `?ref=` key list and every
/// paginated route's `?after=`/`?created_after=`/`?spent_after=`.
fn parse_output_ref(s: &str) -> Result<OutputRef, ApiError> {
    let (h, i) = s
        .split_once(':')
        .ok_or_else(|| ApiError::bad_request("must be <tx_hash_hex>:<ix>"))?;
    let tx_hash: [u8; 32] = hex::decode(h)
        .map_err(|_| ApiError::bad_request("tx_hash must be hex"))?
        .try_into()
        .map_err(|_| ApiError::bad_request("tx_hash must be 32 bytes"))?;
    let ix: u16 = i
        .parse()
        .map_err(|_| ApiError::bad_request("ix must be a number"))?;
    Ok(OutputRef { tx_hash, ix })
}

fn format_cursor(r: OutputRef) -> String {
    format!("{}:{}", hex::encode(r.tx_hash), r.ix)
}

fn parse_hash32(s: &str) -> Result<[u8; 32], ApiError> {
    hex::decode(s)
        .ok()
        .and_then(|v| v.try_into().ok())
        .ok_or_else(|| ApiError::bad_request("must be 32 hex bytes"))
}

fn cred28(s: &str) -> Result<[u8; 28], ApiError> {
    hex::decode(s)
        .ok()
        .and_then(|v| v.try_into().ok())
        .ok_or_else(|| ApiError::bad_request("credential must be 28 hex bytes"))
}

/// `pay_cred`/`stake_cred` only, from this route's own query params --
/// same single-credential restriction as `grpc.rs`'s `AddressFilter`.
#[derive(Clone, Copy)]
enum AddressFilter {
    PayOnly([u8; 28]),
    PayAndStake([u8; 28], [u8; 28]),
    StakeOnly([u8; 28]),
}

fn address_filter(pay_cred: Option<&str>, stake_cred: Option<&str>) -> Result<AddressFilter, ApiError> {
    match (pay_cred, stake_cred) {
        (Some(p), Some(d)) => Ok(AddressFilter::PayAndStake(cred28(p)?, cred28(d)?)),
        (Some(p), None) => Ok(AddressFilter::PayOnly(cred28(p)?)),
        (None, Some(d)) => Ok(AddressFilter::StakeOnly(cred28(d)?)),
        (None, None) => Err(ApiError::bad_request(
            "need pay_cred and/or stake_cred",
        )),
    }
}

/// Same clamp-not-reject policy as `grpc.rs`'s `clamp_limit`.
fn clamp_limit(requested: Option<i32>, max_page_size: u32) -> u32 {
    requested
        .unwrap_or(100)
        .clamp(1, max_page_size.min(i32::MAX as u32) as i32) as u32
}

// --- ReadUtxos / SearchUtxos: GET /utxos --------------------------------

#[derive(Deserialize)]
struct UtxosQuery {
    #[serde(default, rename = "ref")]
    refs: Vec<String>,
    pay_cred: Option<String>,
    stake_cred: Option<String>,
    after: Option<String>,
    limit: Option<i32>,
}

async fn utxos_handler(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<UtxosQuery>,
) -> Response {
    let fmt = negotiate(&headers);
    let result = if !q.refs.is_empty() {
        read_utxos(&st, &q.refs).await
    } else if q.pay_cred.is_some() || q.stake_cred.is_some() {
        search_utxos(&st, q).await
    } else {
        Err(ApiError::bad_request(
            "need ref=<tx_hash_hex>:<ix> (repeatable), or pay_cred=/stake_cred=",
        ))
    };
    match result {
        Ok(page) => render(fmt, StatusCode::OK, &page),
        Err(e) => err_response(fmt, e),
    }
}

async fn read_utxos(st: &AppState, refs: &[String]) -> Result<PageDto, ApiError> {
    let keys = refs
        .iter()
        .map(|s| parse_output_ref(s))
        .collect::<Result<Vec<_>, _>>()?;
    let db_path = st.db_path.clone();
    let network = st.network;
    let (items, tip) = tokio::task::spawn_blocking(move || -> Result<_> {
        let conn = query::open_reader(&db_path)?;
        let mut items = Vec::new();
        for r in keys {
            if let Some(u) = query::by_ref(&conn, r)? {
                items.push(utxo_dto(&conn, u, network)?);
            }
        }
        let tip = query::tip_point(&conn)?;
        Ok((items, tip))
    })
    .await
    .map_err(join_err)?
    .map_err(map_err)?;

    Ok(PageDto {
        items,
        ledger_tip: chain_point_dto(tip),
        next: None,
    })
}

async fn search_utxos(st: &AppState, q: UtxosQuery) -> Result<PageDto, ApiError> {
    let filter = address_filter(q.pay_cred.as_deref(), q.stake_cred.as_deref())?;
    let limit = clamp_limit(q.limit, st.max_page_size);
    let after = q.after.as_deref().map(parse_output_ref).transpose()?;
    let db_path = st.db_path.clone();
    let network = st.network;
    let (items, last_ref, tip) = tokio::task::spawn_blocking(move || -> Result<_> {
        let conn = query::open_reader(&db_path)?;
        let rows = match filter {
            AddressFilter::PayOnly(p) => query::by_pay_cred_any_script(&conn, p, after, limit)?,
            AddressFilter::PayAndStake(p, d) => {
                query::by_credentials_any_script(&conn, p, Some(d), after, limit)?
            }
            AddressFilter::StakeOnly(d) => query::by_stake_cred(&conn, d, after, limit)?,
        };
        let last_ref = rows.last().map(|u| u.output_ref);
        let tip = query::tip_point(&conn)?;
        let items = rows
            .into_iter()
            .map(|u| utxo_dto(&conn, u, network))
            .collect::<Result<Vec<_>>>()?;
        Ok((items, last_ref, tip))
    })
    .await
    .map_err(join_err)?
    .map_err(map_err)?;

    Ok(PageDto {
        items,
        ledger_tip: chain_point_dto(tip),
        next: last_ref.map(format_cursor),
    })
}

// --- ReadData: GET /data/{hash} -----------------------------------------

async fn data_handler(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path(hash_hex): Path<String>,
) -> Response {
    let fmt = negotiate(&headers);
    match read_data(&st, &hash_hex).await {
        Ok(dto) => render(fmt, StatusCode::OK, &dto),
        Err(e) => err_response(fmt, e),
    }
}

async fn read_data(st: &AppState, hash_hex: &str) -> Result<DatumResponseDto, ApiError> {
    let hash = parse_hash32(hash_hex)?;
    let db_path = st.db_path.clone();
    let (datum, tip) = tokio::task::spawn_blocking(move || -> Result<_> {
        let conn = query::open_reader(&db_path)?;
        let datum = query::datum_by_hash(&conn, hash)?;
        let tip = query::tip_point(&conn)?;
        Ok((datum, tip))
    })
    .await
    .map_err(join_err)?
    .map_err(map_err)?;

    let datum = datum.ok_or_else(|| ApiError::not_found("datum not found"))?;
    Ok(DatumResponseDto {
        key: datum.hash,
        native_bytes: datum.cbor,
        ledger_tip: chain_point_dto(tip),
    })
}

// --- ReadTx: GET /tx/{hash} ----------------------------------------------

async fn tx_handler(
    State(st): State<AppState>,
    headers: HeaderMap,
    Path(hash_hex): Path<String>,
) -> Response {
    let fmt = negotiate(&headers);
    match read_tx(&st, &hash_hex).await {
        Ok(dto) => render(fmt, StatusCode::OK, &dto),
        Err(e) => err_response(fmt, e),
    }
}

async fn read_tx(st: &AppState, hash_hex: &str) -> Result<TxResponseDto, ApiError> {
    let hash = parse_hash32(hash_hex)?;
    let db_path = st.db_path.clone();
    let network = st.network;
    let (tx, tip) = tokio::task::spawn_blocking(move || -> Result<_> {
        let conn = query::open_reader(&db_path)?;
        let tx = query::by_tx_hash(&conn, hash)?
            .map(|t| tx_dto(&conn, t, network))
            .transpose()?;
        let tip = query::tip_point(&conn)?;
        Ok((tx, tip))
    })
    .await
    .map_err(join_err)?
    .map_err(map_err)?;

    let tx = tx.ok_or_else(|| ApiError::not_found("tx not found"))?;
    Ok(TxResponseDto {
        tx,
        ledger_tip: chain_point_dto(tip),
    })
}

// --- UtxoChanges: GET /changes --------------------------------------------

#[derive(Deserialize)]
struct ChangesQuery {
    pay_cred: Option<String>,
    stake_cred: Option<String>,
    since_slot: Option<u64>,
    since_hash: Option<String>,
    created_after: Option<String>,
    spent_after: Option<String>,
    limit: Option<i32>,
}

enum ChangesOutcome {
    Changes(ChangesDto),
    Stale(StaleReason),
}

async fn changes_handler(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<ChangesQuery>,
) -> Response {
    let fmt = negotiate(&headers);
    match changes(&st, q).await {
        Ok(dto) => render(fmt, StatusCode::OK, &dto),
        Err(e) => err_response(fmt, e),
    }
}

async fn changes(st: &AppState, q: ChangesQuery) -> Result<ChangesResponseDto, ApiError> {
    let filter = address_filter(q.pay_cred.as_deref(), q.stake_cred.as_deref())?;
    let since_hash = q.since_hash.as_deref().map(parse_hash32).transpose()?;
    let since_slot_req = q.since_slot;
    let limit = clamp_limit(q.limit, st.max_page_size);
    let created_after = q.created_after.as_deref().map(parse_output_ref).transpose()?;
    let spent_after = q.spent_after.as_deref().map(parse_output_ref).transpose()?;

    let db_path = st.db_path.clone();
    let network = st.network;
    let outcome = tokio::task::spawn_blocking(move || -> Result<ChangesOutcome> {
        let mut conn = query::open_reader(&db_path)?;
        let tx = conn.transaction()?;
        let since_slot = match query::check_point(&tx, since_slot_req, since_hash)? {
            query::PointCheck::Stale(reason) => return Ok(ChangesOutcome::Stale(reason)),
            query::PointCheck::NoHashGiven(s) => s,
            query::PointCheck::Valid(s) => Some(s),
        };
        let created = match filter {
            AddressFilter::PayOnly(p) => query::created_since_by_pay_cred_any_script(
                &tx,
                p,
                since_slot,
                created_after,
                limit,
            )?,
            AddressFilter::PayAndStake(p, d) => query::created_since_by_credentials_any_script(
                &tx,
                p,
                Some(d),
                since_slot,
                created_after,
                limit,
            )?,
            AddressFilter::StakeOnly(d) => {
                query::created_since_by_stake_cred(&tx, d, since_slot, created_after, limit)?
            }
        };
        let spent = match filter {
            AddressFilter::PayOnly(p) => {
                query::spent_since_by_pay_cred_any_script(&tx, p, since_slot, spent_after, limit)?
            }
            AddressFilter::PayAndStake(p, d) => query::spent_since_by_credentials_any_script(
                &tx,
                p,
                Some(d),
                since_slot,
                spent_after,
                limit,
            )?,
            AddressFilter::StakeOnly(d) => {
                query::spent_since_by_stake_cred(&tx, d, since_slot, spent_after, limit)?
            }
        };
        let created_last = created.last().map(|u| u.output_ref);
        let spent_last = spent.last().map(|u| u.output_ref);
        let tip = query::tip_point(&tx)?;
        let created_dto = created
            .into_iter()
            .map(|u| utxo_dto(&tx, u, network))
            .collect::<Result<Vec<_>>>()?;
        let spent_dto = spent
            .into_iter()
            .map(|u| utxo_dto(&tx, u, network))
            .collect::<Result<Vec<_>>>()?;
        Ok(ChangesOutcome::Changes(ChangesDto {
            created: created_dto,
            spent: spent_dto,
            ledger_tip: chain_point_dto(tip),
            created_next: created_last.map(format_cursor),
            spent_next: spent_last.map(format_cursor),
        }))
    })
    .await
    .map_err(join_err)?
    .map_err(map_err)?;

    Ok(match outcome {
        ChangesOutcome::Changes(c) => ChangesResponseDto::Changes(c),
        ChangesOutcome::Stale(reason) => ChangesResponseDto::Stale(stale_dto(reason)),
    })
}

// --- SubmitTx: POST /tx ----------------------------------------------------

async fn submit_handler(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let fmt = negotiate(&headers);
    match submit_tx(&st, &body).await {
        Ok(dto) => render(fmt, StatusCode::OK, &dto),
        Err(e) => err_response(fmt, e),
    }
}

/// Reuses `submit::decode_tx_bytes`/`submit::post_tx` -- the
/// forwarding logic to amaru's submit-api lives there once.
async fn submit_tx(st: &AppState, raw: &[u8]) -> Result<SubmitTxResponseDto, ApiError> {
    let Some(amaru_submit_api) = &st.amaru_submit_api else {
        return Err(ApiError::Unavailable(
            "submit is not configured on this server".into(),
        ));
    };
    let tx = submit::decode_tx_bytes(raw).map_err(|e| ApiError::bad_request(e.to_string()))?;
    let tx_hash = tx.hash().to_vec();

    match submit::post_tx(amaru_submit_api, raw).await {
        Ok((status, _body)) if (200..300).contains(&status) => {
            Ok(SubmitTxResponseDto { r#ref: tx_hash })
        }
        Ok((_status, body)) => Err(ApiError::bad_request(format!("rejected by amaru: {body}"))),
        Err(e) => Err(ApiError::Unavailable(format!(
            "amaru submit-api unavailable: {e}"
        ))),
    }
}
