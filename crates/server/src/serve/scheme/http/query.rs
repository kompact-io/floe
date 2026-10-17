//! http::query: one route per RPC `scheme::grpc`'s `QueryService`/
//! `SubmitService` implements: `ReadUtxos`, `SearchUtxos`, `ReadData`,
//! `ReadTx`, `SubmitTx`, all over `Ctx`'s already-resolved rows. DTO
//! conversion is its own pass here, separate from `scheme::grpc`'s
//! proto conversion (see `floe-dto-http`'s own doc for the
//! `cardano::Script`/assets simplifications). No `query::`/`Connection`
//! access here at all -- see `Ctx`'s own module doc.
//!
//! Everything here is codec-agnostic: routes and business logic never
//! branch on wire format directly. json/cbor share this one listener
//! and address -- unlike `--grpc`, which is its own port -- and which
//! wire format a given request gets is negotiated per request from
//! its `Accept` header (`negotiate`, mirroring `http::metrics`'s own
//! `negotiate`), not fixed per listener: a reverse proxy in front of
//! this forwards `Accept` unfiltered, so this works the same whether
//! or not one sits in front. `--http-json`/`--http-cbor` (`HttpFormats`,
//! set by `main.rs`, already checked there against which of this
//! binary's own `http-json`/`http-cbor` features are compiled in) pick
//! which formats this listener answers at all. `SubmitTx`'s body is
//! always raw tx bytes regardless of format (mirroring gRPC's
//! `AnyChainTx.raw`) -- negotiation only governs response encoding.
//! Route paths themselves (`floe_dto_http::query`/`submit`'s
//! `*_PATH` constants) live next to their request/response DTOs, not
//! here -- `floe-client` imports the same constants, so the two can't
//! drift on a path string.

use anyhow::Result;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use pallas_codec::minicbor as pallas_minicbor;
use pallas_codec::utils::PositiveCoin;
use pallas_primitives::conway::Multiasset;
use tracing::info;

use floe_dto_http::ErrorBody;
use floe_dto_http::cardano as ca;
use floe_dto_http::health::Scheme;
use floe_dto_http::query as q;
use floe_dto_http::submit as s;

use crate::ingest::decode::OutputRef;
use crate::serve::ctx::{
    AddressFilter, BlockPoint, Ctx, Cursor, NotReady, ResolvedDatum, Stale, StaleReason, Tx, Utxo,
};
use crate::serve::submit::SubmitOutcome;

/// Which wire formats `--http` was told to accept (`main.rs` has
/// already checked each requested one against this binary's own
/// `http-json`/`http-cbor` features before constructing this).
#[derive(Clone, Copy)]
pub struct HttpFormats {
    pub json: bool,
    pub cbor: bool,
}

/// This request's negotiated wire format. Variants are cfg-gated the
/// same as the feature that can produce them (see `json_format`/
/// `cbor_format`), so `render`/`scheme` below stay exhaustive however
/// this binary was compiled.
#[derive(Clone, Copy)]
enum Format {
    #[cfg(feature = "http-json")]
    Json,
    #[cfg(feature = "http-cbor")]
    Cbor,
}

#[cfg(feature = "http-json")]
fn json_format() -> Option<Format> {
    Some(Format::Json)
}
#[cfg(not(feature = "http-json"))]
fn json_format() -> Option<Format> {
    None
}

#[cfg(feature = "http-cbor")]
fn cbor_format() -> Option<Format> {
    Some(Format::Cbor)
}
#[cfg(not(feature = "http-cbor"))]
fn cbor_format() -> Option<Format> {
    None
}

impl Format {
    fn scheme(self) -> Scheme {
        match self {
            #[cfg(feature = "http-json")]
            Format::Json => Scheme::HttpJson,
            #[cfg(feature = "http-cbor")]
            Format::Cbor => Scheme::HttpCbor,
        }
    }
}

/// `Accept: application/json` or `application/cbor` picks that format
/// outright (`406` if `formats` doesn't allow it); anything else --
/// absent, `*/*`, unrecognized, or both at once -- prefers JSON when
/// `formats.json`, else CBOR, matching `http::metrics::negotiate`'s own
/// default-to-text-when-unspecified leniency.
fn negotiate(headers: &HeaderMap, formats: HttpFormats) -> Result<Format, &'static str> {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let wants_json = accept.contains("application/json");
    let wants_cbor = accept.contains("application/cbor");

    let json = formats.json.then(json_format).flatten();
    let cbor = formats.cbor.then(cbor_format).flatten();

    if wants_cbor && !wants_json {
        return cbor.ok_or("this listener doesn't accept application/cbor");
    }
    if wants_json && !wants_cbor {
        return json.ok_or("this listener doesn't accept application/json");
    }
    json.or(cbor).ok_or("this listener accepts no wire format")
}

/// Each request opens its own short-lived read-only DB connection
/// inside `spawn_blocking`, same policy `grpc.rs`/`metrics.rs` follow.
pub async fn serve(addr: &str, ctx: Ctx, formats: HttpFormats) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, max_page_size = ctx.max_page_size, "http server listening");
    axum::serve(listener, build_router(ctx, formats)).await?;
    Ok(())
}

fn build_router(state: Ctx, formats: HttpFormats) -> Router {
    Router::new()
        .route(q::READ_UTXO_PATH, post(read_utxo_handler))
        .route(q::SEARCH_UTXOS_PATH, post(search_utxos_handler))
        .route(q::READ_DATA_PATH, post(data_handler))
        .route(q::READ_TX_PATH, post(tx_handler))
        .route(s::SUBMIT_TX_PATH, post(submit_handler))
        .layer(Extension(formats))
        .with_state(state)
}

// --- error model -----------------------------------------------------
//
// Mirrors `grpc.rs`'s `tonic::Code` choices onto HTTP status:
// invalid_argument -> 400, not_found -> 404, unavailable (DB
// mid-catchup, or amaru's submit-api unreachable) -> 503, gone/
// conflict (stale `since`) -> 410/409, else 500. An RPC gRPC doesn't
// implement simply has no route -- a 404 from the router stands in
// for `unimplemented`.

enum ApiError {
    BadRequest(String),
    NotFound(String),
    Unavailable(String),
    /// `since`/`Point` too old -- gone for good.
    Gone(String),
    /// `since`/`Point` rolled back -- retry at an earlier point.
    Conflict(String),
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
            Self::Gone(_) => StatusCode::GONE,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    fn message(self) -> String {
        match self {
            Self::BadRequest(m)
            | Self::NotFound(m)
            | Self::Unavailable(m)
            | Self::Gone(m)
            | Self::Conflict(m)
            | Self::Internal(m) => m,
        }
    }
}

/// `NotReady` (still in catch-up) maps to 503, same as `grpc.rs`'s
/// `UNAVAILABLE`. `Stale` (a `since`/`Point` no longer resolvable)
/// maps to two distinguishable status codes -- `410 Gone` (too old)
/// vs `409 Conflict` (rolled back, retry) -- so a consumer branches on
/// the status code, not the body text. Everything else is 500.
fn map_err(e: anyhow::Error) -> ApiError {
    if let Some(&Stale(reason)) = e.downcast_ref::<Stale>() {
        match reason {
            StaleReason::TooOld { .. } | StaleReason::Unknown => ApiError::Gone(e.to_string()),
            StaleReason::RolledBack { .. } => ApiError::Conflict(e.to_string()),
        }
    } else if e.downcast_ref::<NotReady>().is_some() {
        ApiError::Unavailable(e.to_string())
    } else {
        ApiError::Internal(e.to_string())
    }
}

// --- wire format rendering ------------------------------------------

fn render<T>(fmt: Format, status: StatusCode, value: &T) -> Response
where
    T: serde::Serialize + minicbor::Encode<()>,
{
    match fmt {
        #[cfg(feature = "http-json")]
        Format::Json => {
            let body = serde_json::to_vec(value).expect("dto JSON serialization is infallible");
            (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
        }
        #[cfg(feature = "http-cbor")]
        Format::Cbor => {
            let mut body = Vec::new();
            minicbor::encode(value, &mut body).expect("dto CBOR encoding is infallible");
            (status, [(header::CONTENT_TYPE, "application/cbor")], body).into_response()
        }
    }
}

fn err_response(fmt: Format, e: ApiError) -> Response {
    let status = e.status();
    render(fmt, status, &ErrorBody { error: e.message() })
}

fn not_acceptable(msg: &'static str) -> Response {
    (StatusCode::NOT_ACCEPTABLE, msg).into_response()
}

/// Decodes a request body in `fmt` -- the mirror of `render`, and the
/// other half of why a request DTO gets real binary CBOR rather than
/// hex-in-text: the body is negotiated the same way the response is,
/// not sniffed from `Content-Type`.
fn parse_body<T>(fmt: Format, body: &[u8]) -> Result<T, ApiError>
where
    T: serde::de::DeserializeOwned + for<'de> minicbor::Decode<'de, ()>,
{
    match fmt {
        #[cfg(feature = "http-json")]
        Format::Json => serde_json::from_slice(body)
            .map_err(|e| ApiError::bad_request(format!("invalid JSON body: {e}"))),
        #[cfg(feature = "http-cbor")]
        Format::Cbor => minicbor::decode(body)
            .map_err(|e| ApiError::bad_request(format!("invalid CBOR body: {e}"))),
    }
}

// --- domain -> DTO conversion ------------------------------------------
//
// Own small pass over `Ctx`'s already-resolved rows, same inputs
// `grpc.rs`'s `any_utxo_data`/`any_chain_tx` convert (see module doc
// for why they're separate).

fn chain_point_dto(p: Option<BlockPoint>) -> q::ChainPoint {
    match p {
        None => q::ChainPoint::default(),
        Some(p) => q::ChainPoint {
            slot: p.slot,
            hash: p.hash.to_vec(),
            height: p.height.unwrap_or(0),
            timestamp: 0,
        },
    }
}

/// Decodes the raw CBOR multiasset map floe stores into
/// `ca::Multiasset` -- same source bytes and pallas type `grpc.rs`'s
/// `decode_assets` decodes, into a different target shape.
fn asset_dtos(bytes: &[u8]) -> Result<Vec<ca::Multiasset>> {
    let ma: Multiasset<PositiveCoin> = pallas_minicbor::decode(bytes)?;
    Ok(ma
        .into_iter()
        .map(|(policy, assets)| ca::Multiasset {
            policy_id: policy.to_vec(),
            assets: assets
                .into_iter()
                .map(|(name, qty)| ca::Asset {
                    name: name.to_vec(),
                    quantity: u64::from(qty),
                })
                .collect(),
        })
        .collect())
}

fn datum_dto(d: ResolvedDatum) -> Option<ca::Datum> {
    match d {
        ResolvedDatum::None => None,
        ResolvedDatum::Hash(h) => Some(ca::Datum {
            hash: h,
            original_cbor: None,
        }),
        ResolvedDatum::HashResolved(h, cbor) | ResolvedDatum::Inline(h, cbor) => Some(ca::Datum {
            hash: h,
            original_cbor: Some(cbor),
        }),
    }
}

/// The `cardano::TxOutput` common to every row shape below -- a
/// `Utxo`'s address/coin/assets/datum/script, with no opinion on
/// identity (`txo_ref`) or status (`block_ref`/`spent_by`); callers
/// add those around it as their own shape requires.
fn tx_output_dto(u: &Utxo) -> Result<ca::TxOutput> {
    let assets = match &u.assets {
        None => vec![],
        Some(bytes) => asset_dtos(bytes)?,
    };
    Ok(ca::TxOutput {
        address: q::Address(u.address.clone()),
        coin: u.coin,
        assets,
        datum: datum_dto(u.datum.clone()),
        script: u.script.clone().map(|s| ca::Script {
            lang: s.lang,
            cbor: s.cbor,
        }),
    })
}

fn utxo_dto(u: Utxo) -> Result<q::Utxo> {
    Ok(q::Utxo {
        txo_ref: q::TxoRef {
            hash: u.output_ref.tx_hash,
            index: u.output_ref.ix,
        },
        output: tx_output_dto(&u)?,
        block_ref: chain_point_dto(u.block),
        spent_by: u.spent_by,
    })
}

/// `ReadTx`'s own input row: `cardano::TxInput`'s `tx_hash`/
/// `output_index` are the prior output's own ref (what's being spent),
/// not anything about this tx -- `Ctx::read_tx` already resolves each
/// input row's `output_ref` that way (see `query::ResolvedTx`).
fn tx_input_dto(u: Utxo) -> Result<ca::TxInput> {
    Ok(ca::TxInput {
        tx_hash: u.output_ref.tx_hash,
        output_index: u.output_ref.ix,
        as_output: tx_output_dto(&u)?,
    })
}

fn tx_dto(t: Tx, block: Option<BlockPoint>) -> Result<q::Tx> {
    let inputs = t
        .inputs
        .into_iter()
        .map(tx_input_dto)
        .collect::<Result<Vec<_>>>()?;
    let outputs = t
        .outputs
        .iter()
        .map(tx_output_dto)
        .collect::<Result<Vec<_>>>()?;
    Ok(q::Tx {
        hash: t.hash,
        block_ref: chain_point_dto(block),
        inputs,
        outputs,
        // Hardcoded, same tracked gap as `grpc.rs`'s `any_chain_tx` --
        // see `TODO.md`.
        successful: true,
    })
}

// --- shared request-parsing helpers ------------------------------------

/// `pay_hash`/`stake_hash` only, from this RPC's own request fields --
/// same single-hash restriction as `grpc.rs`'s `AddressFilter`
/// (`Ctx`'s own type, shared by both schemes).
fn address_filter(
    pay_hash: Option<[u8; 28]>,
    stake_hash: Option<[u8; 28]>,
) -> Result<AddressFilter, ApiError> {
    match (pay_hash, stake_hash) {
        (Some(p), Some(d)) => Ok(AddressFilter::PayAndStake(p, d)),
        (Some(p), None) => Ok(AddressFilter::PayOnly(p)),
        (None, Some(d)) => Ok(AddressFilter::StakeOnly(d)),
        (None, None) => Err(ApiError::bad_request("need pay_hash and/or stake_hash")),
    }
}

// --- ReadUtxos: POST /query/read-utxos ------------------------------------

async fn read_utxo_handler(
    State(st): State<Ctx>,
    Extension(formats): Extension<HttpFormats>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let fmt = match negotiate(&headers, formats) {
        Ok(fmt) => fmt,
        Err(msg) => return not_acceptable(msg),
    };
    st.requests.record(fmt.scheme(), q::READ_UTXO_PATH);

    let req: q::ReadUtxosRequest = match parse_body(fmt, &body) {
        Ok(req) => req,
        Err(e) => return err_response(fmt, e),
    };
    if req.keys.is_empty() {
        return err_response(fmt, ApiError::bad_request("need at least one key"));
    }
    match read_utxos(&st, &req.keys).await {
        Ok(page) => render(fmt, StatusCode::OK, &page),
        Err(e) => err_response(fmt, e),
    }
}

async fn read_utxos(st: &Ctx, keys: &[q::TxoRef]) -> Result<q::ReadUtxosResponse, ApiError> {
    let keys = keys
        .iter()
        .map(|k| OutputRef {
            tx_hash: k.hash,
            ix: k.index,
        })
        .collect();
    let (rows, tip) = st.read_utxos(keys).await.map_err(map_err)?;
    let items = rows
        .into_iter()
        .map(utxo_dto)
        .collect::<Result<Vec<_>>>()
        .map_err(map_err)?;

    Ok(q::ReadUtxosResponse {
        items,
        ledger_tip: chain_point_dto(tip),
    })
}

// --- SearchUtxos: POST /query/search-utxos -------------------------------

async fn search_utxos_handler(
    State(st): State<Ctx>,
    Extension(formats): Extension<HttpFormats>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let fmt = match negotiate(&headers, formats) {
        Ok(fmt) => fmt,
        Err(msg) => return not_acceptable(msg),
    };
    st.requests.record(fmt.scheme(), q::SEARCH_UTXOS_PATH);
    let req: q::SearchUtxosRequest = match parse_body(fmt, &body) {
        Ok(req) => req,
        Err(e) => return err_response(fmt, e),
    };
    match search_utxos(&st, req).await {
        Ok(page) => render(fmt, StatusCode::OK, &page),
        Err(e) => err_response(fmt, e),
    }
}

async fn search_utxos(
    st: &Ctx,
    req: q::SearchUtxosRequest,
) -> Result<q::SearchUtxosResponse, ApiError> {
    let filter = address_filter(req.pay_hash, req.stake_hash)?;
    let after = req
        .start_token
        .as_deref()
        .map(Cursor::parse)
        .transpose()
        .map_err(|e| ApiError::bad_request(format!("start_token: {e}")))?;
    let (rows, last_ref, tip) = st
        .search_utxos(filter, req.since_slot, req.since_hash, after, req.max_items)
        .await
        .map_err(map_err)?;
    let items = rows
        .into_iter()
        .map(utxo_dto)
        .collect::<Result<Vec<_>>>()
        .map_err(map_err)?;

    Ok(q::SearchUtxosResponse {
        items,
        ledger_tip: chain_point_dto(tip),
        next_token: last_ref.map(Cursor::format),
    })
}

// --- ReadData: POST /query/read-data -------------------------------------

async fn data_handler(
    State(st): State<Ctx>,
    Extension(formats): Extension<HttpFormats>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let fmt = match negotiate(&headers, formats) {
        Ok(fmt) => fmt,
        Err(msg) => return not_acceptable(msg),
    };
    st.requests.record(fmt.scheme(), q::READ_DATA_PATH);
    let req: q::ReadDataRequest = match parse_body(fmt, &body) {
        Ok(req) => req,
        Err(e) => return err_response(fmt, e),
    };
    match read_data(&st, req.key).await {
        Ok(dto) => render(fmt, StatusCode::OK, &dto),
        Err(e) => err_response(fmt, e),
    }
}

async fn read_data(st: &Ctx, hash: [u8; 32]) -> Result<q::ReadDataResponse, ApiError> {
    let (datum, tip) = st.read_data(hash).await.map_err(map_err)?;
    let datum = datum.ok_or_else(|| ApiError::not_found("datum not found"))?;
    Ok(q::ReadDataResponse {
        value: q::AnyChainDatum {
            native_bytes: datum.cbor,
            key: datum.hash,
        },
        ledger_tip: chain_point_dto(tip),
    })
}

// --- ReadTx: POST /query/read-tx -------------------------------------------

async fn tx_handler(
    State(st): State<Ctx>,
    Extension(formats): Extension<HttpFormats>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let fmt = match negotiate(&headers, formats) {
        Ok(fmt) => fmt,
        Err(msg) => return not_acceptable(msg),
    };
    st.requests.record(fmt.scheme(), q::READ_TX_PATH);
    let req: q::ReadTxRequest = match parse_body(fmt, &body) {
        Ok(req) => req,
        Err(e) => return err_response(fmt, e),
    };
    match read_tx(&st, req.hash).await {
        Ok(dto) => render(fmt, StatusCode::OK, &dto),
        Err(e) => err_response(fmt, e),
    }
}

async fn read_tx(st: &Ctx, hash: [u8; 32]) -> Result<q::ReadTxResponse, ApiError> {
    let (tx, tip) = st.read_tx(hash).await.map_err(map_err)?;
    let (tx, block) = tx.ok_or_else(|| ApiError::not_found("tx not found"))?;
    let tx = tx_dto(tx, block).map_err(map_err)?;
    Ok(q::ReadTxResponse {
        tx,
        ledger_tip: chain_point_dto(tip),
    })
}

// --- SubmitTx: POST /submit/submit-tx -------------------------------------

async fn submit_handler(
    State(st): State<Ctx>,
    Extension(formats): Extension<HttpFormats>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let fmt = match negotiate(&headers, formats) {
        Ok(fmt) => fmt,
        Err(msg) => return not_acceptable(msg),
    };
    st.requests.record(fmt.scheme(), s::SUBMIT_TX_PATH);
    match submit_tx(&st, &body).await {
        Ok(dto) => render(fmt, StatusCode::OK, &dto),
        Err(e) => err_response(fmt, e),
    }
}

async fn submit_tx(st: &Ctx, raw: &[u8]) -> Result<s::SubmitTxResponse, ApiError> {
    match st.submit(raw).await {
        SubmitOutcome::Accepted { tx_hash } => Ok(s::SubmitTxResponse { r#ref: tx_hash }),
        SubmitOutcome::Rejected { reason } => Err(ApiError::bad_request(reason)),
        SubmitOutcome::Unavailable { reason } => Err(ApiError::Unavailable(reason)),
    }
}
