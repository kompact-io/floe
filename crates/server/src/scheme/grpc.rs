//! grpc: `floe-server`'s gRPC transport -- `QueryService` always, and
//! `SubmitService` (see `submit.rs`) when `serve()` is given a node
//! connection. All domain->proto conversion for `QueryService` lives
//! here; routing is just the generated trait impl below.
//!
//! `QueryService` support: `ReadUtxos` (by `TxoRef`); `SearchUtxos`
//! restricted to a single address-pattern predicate
//! (`payment_part`/`delegation_part`, no `exact_address`, no
//! asset/cert patterns, no `not`/`all_of`/`any_of` composition);
//! `ReadData` (by datum hash, `native_bytes` only -- `PlutusData`
//! parsing isn't implemented); `ReadTx` (by tx hash, in/outputs only
//! -- no certs/mint/fee/collateral/withdrawals/auxiliary_data/
//! redeemer, and `successful` isn't tracked at all, see
//! `any_chain_tx`'s doc comment); `UtxoChanges`, floe's own addition
//! -- a one-shot "what changed at this address since this `Point`"
//! diff, same address-pattern restrictions as `SearchUtxos`. Every
//! other RPC, and every unsupported `SearchUtxos`/`UtxoChanges`
//! shape, returns `Status::unimplemented`/`Status::invalid_argument`/
//! `Status::not_found` with a message -- never silently 500s or
//! panics.
//!
//! `metrics`/`health` (below) are reporting apparatus only, over the
//! crate-root `crate::metrics`/`crate::health` core: `metrics`'s tower
//! `Layer` records into it, `health` implements floe's own
//! `HealthService` RPC (`GetHealth`/`GetMetrics`), just snapshotting
//! and converting. No standard `grpc.health.v1.Health` yet -- deferred,
//! see `../../plan-server-health-and-metrics.md`.

pub mod health;
pub mod metrics;

use std::sync::Arc;

use anyhow::Result;
use pallas_addresses::Network;
use pallas_codec::minicbor;
use pallas_primitives::conway::NativeScript as PallasNativeScript;
use rusqlite::Connection;
use tonic::{Request, Response, Status, transport::Server};
use tracing::info;

use floe_dto_grpc::floe::v1::cardano as c;
use floe_dto_grpc::floe::v1::query as q;
use floe_dto_grpc::floe::v1::submit::submit_service_server::SubmitServiceServer;
use q::query_service_server::{QueryService, QueryServiceServer};

use crate::decode::OutputRef;
use crate::domain::reconstruct_address;
use crate::health::Health;
use crate::query::{self, BlockPoint, NotReady, ResolvedDatum, ResolvedScript, ResolvedUtxo};
use crate::submit;

struct Svc {
    db_path: Arc<str>,
    network: Network,
    /// Hard ceiling on `SearchUtxos.max_items` and `UtxoChanges.max_items`
    /// (per list) -- a request asking for more just gets clamped, not
    /// rejected, same as the un-configurable `1000` this replaces.
    /// Operator-configurable via `--max-page-size` (see `main.rs`).
    max_page_size: u32,
}

/// `SubmitService` on the same port when given amaru's submit-api
/// address; omit it to serve `QueryService` alone. `health` is `Some`
/// for `run` (tracks its own lifecycle), `None` for `serve` (a
/// stateless read replica, see `health::Svc`).
pub async fn serve(
    addr: &str,
    db_path: Arc<str>,
    network: Network,
    max_page_size: u32,
    amaru_submit_api: Option<String>,
    health: Option<Arc<Health>>,
    requests: Arc<crate::metrics::Metrics>,
) -> Result<()> {
    let addr = addr.parse()?;
    info!(%addr, max_page_size, submit = amaru_submit_api.is_some(), "grpc server listening");
    let mut server = Server::builder()
        .layer(metrics::MetricsLayer(requests.clone()))
        .add_service(health::HealthServiceServer::new(health::Svc {
            db_path: db_path.clone(),
            health,
            requests,
        }))
        .add_service(QueryServiceServer::new(Svc {
            db_path,
            network,
            max_page_size,
        }));
    if let Some(amaru_submit_api) = amaru_submit_api {
        server = server.add_service(SubmitServiceServer::new(submit::Svc::new(amaru_submit_api)));
    }
    server.serve(addr).await?;
    Ok(())
}

/// Clamps a request's own `max_items` (per-list for `UtxoChanges`,
/// whole-response for `SearchUtxos`) to `[1, max_page_size]` --
/// shared so both RPCs enforce the same operator-configured ceiling
/// the same way.
fn clamp_limit(requested: Option<i32>, max_page_size: u32) -> u32 {
    requested
        .unwrap_or(100)
        .clamp(1, max_page_size.min(i32::MAX as u32) as i32) as u32
}

fn join_err(e: tokio::task::JoinError) -> Status {
    Status::internal(e.to_string())
}

/// `query::NotReady` (still in catch-up) maps to `UNAVAILABLE`, same
/// as REST's old 503 -- every other failure is this server's own
/// invariant or a DB error, `INTERNAL`.
fn map_err(e: anyhow::Error) -> Status {
    if e.downcast_ref::<NotReady>().is_some() {
        Status::unavailable(e.to_string())
    } else {
        Status::internal(e.to_string())
    }
}

fn txo_ref_to_output_ref(r: &q::TxoRef) -> Result<OutputRef, Status> {
    let tx_hash: [u8; 32] = r
        .hash
        .clone()
        .try_into()
        .map_err(|_| Status::invalid_argument("TxoRef.hash must be 32 bytes"))?;
    let ix: u16 = r
        .index
        .try_into()
        .map_err(|_| Status::invalid_argument("TxoRef.index out of range"))?;
    Ok(OutputRef { tx_hash, ix })
}

fn parse_cursor(s: &str) -> Result<OutputRef, Status> {
    let (h, i) = s
        .split_once(':')
        .ok_or_else(|| Status::invalid_argument("start_token must be <tx_hash_hex>:<ix>"))?;
    let tx_hash: [u8; 32] = hex::decode(h)
        .map_err(|_| Status::invalid_argument("start_token's tx_hash must be hex"))?
        .try_into()
        .map_err(|_| Status::invalid_argument("start_token's tx_hash must be 32 bytes"))?;
    let ix: u16 = i
        .parse()
        .map_err(|_| Status::invalid_argument("start_token's ix must be a number"))?;
    Ok(OutputRef { tx_hash, ix })
}

/// Mirrors `http.rs`'s old `<tx_hash_hex>:<ix>` cursor format.
fn format_cursor(r: OutputRef) -> String {
    format!("{}:{}", hex::encode(r.tx_hash), r.ix)
}

fn chain_point(p: Option<BlockPoint>) -> q::ChainPoint {
    match p {
        None => q::ChainPoint {
            slot: 0,
            hash: vec![],
            height: 0,
            timestamp: 0,
        },
        Some(p) => q::ChainPoint {
            slot: p.slot,
            hash: p.hash.to_vec(),
            // `height` is `None` for the seed-anchor checkpoint; `0` on
            // the wire is an accepted gap (see plan-rpc.md). No
            // `timestamp`: ReadGenesis/ReadEraSummary (needed to derive
            // one) aren't supported.
            height: p.height.unwrap_or(0),
            timestamp: 0,
        },
    }
}

fn big_int_u64(v: u64) -> c::BigInt {
    let kind = match i64::try_from(v) {
        Ok(i) => c::big_int::BigInt::Int(i),
        Err(_) => c::big_int::BigInt::BigUInt(v.to_be_bytes().to_vec()),
    };
    c::BigInt {
        big_int: Some(kind),
    }
}

/// Decodes the raw CBOR multiasset map floe stores (`minicbor::to_vec`
/// of a `pallas_primitives::conway::Multiasset<PositiveCoin>`, see
/// `decode.rs`'s `decode_output`) back into UTxORPC's structured
/// `Multiasset` shape. Round-trips through the same pallas type that
/// produced the bytes, not a hand-rolled parser.
fn decode_assets(bytes: &[u8]) -> Result<Vec<c::Multiasset>> {
    let ma: pallas_primitives::conway::Multiasset<pallas_codec::utils::PositiveCoin> =
        minicbor::decode(bytes)?;
    Ok(ma
        .into_iter()
        .map(|(policy, assets)| c::Multiasset {
            policy_id: policy.to_vec(),
            assets: assets
                .into_iter()
                .map(|(name, qty)| c::Asset {
                    name: name.to_vec(),
                    quantity: Some(big_int_u64(u64::from(qty))),
                })
                .collect(),
        })
        .collect())
}

/// Re-decodes a reference script's stored raw CBOR back into
/// UTxORPC's structured `NativeScript` shape, for the `Native` lang
/// case -- the bytes are exactly what `ns.raw_cbor()` wrote at decode
/// time (see `decode.rs`'s `decode_output`), so decoding them with
/// pallas's own `NativeScript` type is a round-trip, not a parser
/// floe has to maintain itself.
/// Takes `&PallasNativeScript`, not by value: pallas's `NativeScript`
/// implements `Drop` (an internal caching detail), which forbids
/// moving its enum fields out by pattern match -- only borrowing them.
fn native_script(ns: &PallasNativeScript) -> c::NativeScript {
    let variant = match ns {
        PallasNativeScript::ScriptPubkey(h) => {
            c::native_script::NativeScript::ScriptPubkeyHash(h.to_vec())
        }
        PallasNativeScript::ScriptAll(items) => {
            c::native_script::NativeScript::ScriptAll(c::NativeScriptList {
                items: items.iter().map(native_script).collect(),
            })
        }
        PallasNativeScript::ScriptAny(items) => {
            c::native_script::NativeScript::ScriptAny(c::NativeScriptList {
                items: items.iter().map(native_script).collect(),
            })
        }
        PallasNativeScript::ScriptNOfK(k, items) => {
            c::native_script::NativeScript::ScriptNOfK(c::ScriptNOfK {
                k: *k as u32,
                scripts: items.iter().map(native_script).collect(),
            })
        }
        PallasNativeScript::InvalidBefore(slot) => {
            c::native_script::NativeScript::InvalidBefore(*slot)
        }
        PallasNativeScript::InvalidHereafter(slot) => {
            c::native_script::NativeScript::InvalidHereafter(*slot)
        }
    };
    c::NativeScript {
        native_script: Some(variant),
    }
}

/// `PlutusData`/full native-script parsing beyond the top-level shape
/// isn't implemented -- `original_cbor` always carries the raw bytes
/// (lossless), `payload` stays unset. Not a cut corner specific to
/// this RPC: matches `ReadTx`'s own partial-support posture elsewhere
/// (plan-rpc.md).
fn script(s: ResolvedScript) -> Result<c::Script> {
    let variant = match s.lang {
        crate::decode::ScriptLang::Native => {
            let ns: PallasNativeScript = minicbor::decode(&s.cbor)?;
            c::script::Script::Native(native_script(&ns))
        }
        crate::decode::ScriptLang::PlutusV1 => c::script::Script::PlutusV1(s.cbor),
        crate::decode::ScriptLang::PlutusV2 => c::script::Script::PlutusV2(s.cbor),
        crate::decode::ScriptLang::PlutusV3 => c::script::Script::PlutusV3(s.cbor),
        crate::decode::ScriptLang::PlutusV4 => c::script::Script::PlutusV4(s.cbor),
    };
    Ok(c::Script {
        script: Some(variant),
    })
}

fn datum(d: ResolvedDatum) -> Option<c::Datum> {
    match d {
        ResolvedDatum::None => None,
        ResolvedDatum::Hash(h) => Some(c::Datum {
            hash: h.to_vec(),
            payload: None,
            original_cbor: None,
        }),
        ResolvedDatum::HashResolved(h, cbor) | ResolvedDatum::Inline(h, cbor) => Some(c::Datum {
            hash: h.to_vec(),
            payload: None,
            original_cbor: Some(cbor),
        }),
    }
}

fn tx_output(u: &ResolvedUtxo, network: Network) -> Result<c::TxOutput> {
    let address = reconstruct_address(u, network).to_vec();
    let assets = match &u.assets {
        None => vec![],
        Some(bytes) => decode_assets(bytes)?,
    };
    Ok(c::TxOutput {
        address,
        coin: Some(big_int_u64(u.coin)),
        assets,
        datum: datum(u.datum.clone()),
        script: u.script.clone().map(script).transpose()?,
        original_cbor: None,
    })
}

/// `block_point` finding no row for `u.slot` isn't an invariant
/// violation: `prune` drops `block` rows past the retention window
/// independent of whether unspent `utxo` rows still carry that slot as
/// their creation slot, and the seed anchor itself stands in for
/// whatever the UTxO's *real* origin block was (unknowable -- it may
/// predate the snapshot). Both are an expected "origin block unknown"
/// case, not corruption -- `chain_point(None)` is exactly that
/// encoding (slot 0, empty hash, height 0), already used elsewhere.
fn any_utxo_data(conn: &Connection, u: ResolvedUtxo, network: Network) -> Result<q::AnyUtxoData> {
    let block_ref = query::block_point(conn, u.slot)?;
    let txo_ref = q::TxoRef {
        hash: u.output_ref.tx_hash.to_vec(),
        index: u.output_ref.ix as u32,
    };
    let spent_by = u.spent_by.map(|h| h.to_vec());
    let cardano_output = tx_output(&u, network)?;
    Ok(q::AnyUtxoData {
        native_bytes: vec![],
        txo_ref: Some(txo_ref),
        block_ref: Some(chain_point(block_ref)),
        spent_by,
        parsed_state: Some(q::any_utxo_data::ParsedState::Cardano(cardano_output)),
    })
}

/// `TxInput.as_output` is the spent output's full pre-spend content --
/// `ResolvedUtxo` already carries exactly that (`spent_by`/`spent_slot`
/// set, everything else as it was when created), so this is the same
/// `tx_output` conversion `any_utxo_data` uses, not a separate shape.
/// `redeemer` is shelved (see "Redeemer: storage vs. exposure" in
/// plan-rpc.md) -- never represented as a bare property of an output
/// elsewhere, only here once unshelved.
fn tx_input(u: &ResolvedUtxo, network: Network) -> Result<c::TxInput> {
    Ok(c::TxInput {
        tx_hash: u.output_ref.tx_hash.to_vec(),
        output_index: u.output_ref.ix as u32,
        as_output: Some(tx_output(u, network)?),
        redeemer: None,
    })
}

/// `ResolvedTx` already carries only what `floe` decomposes
/// (in/outputs, hydrated from `utxo` rows) -- certs/mint/fee/
/// collateral/withdrawals/auxiliary_data stay unset, per `ReadTx`'s
/// documented partial support (plan-rpc.md). `successful` isn't
/// tracked at all (no `valid` column on `tx`, by design -- see
/// plan-rpc.md's `tx` table note) and defaults to `true`: a real gap,
/// not a considered answer -- an invalid tx's `inputs`/`outputs` here
/// are actually its collateral (`decode::decode_tx` records whatever
/// `MultiEraTx::consumes`/`produces` return for its validity, which
/// for an invalid tx *is* collateral), with no flag distinguishing
/// that from a valid tx's real body.
fn any_chain_tx(
    conn: &Connection,
    t: query::ResolvedTx,
    network: Network,
) -> Result<q::AnyChainTx> {
    // Same "origin block unknown" case as `any_utxo_data`, above: `tx`
    // rows are only pruned when orphaned, not by slot age, so one can
    // easily outlive the `block` row at its own `block_slot` (pruned,
    // or the seed anchor standing in for an unknowable real origin).
    let block_ref = query::block_point(conn, t.block_slot)?;
    let inputs = t
        .inputs
        .iter()
        .map(|u| tx_input(u, network))
        .collect::<Result<Vec<_>>>()?;
    let outputs = t
        .outputs
        .iter()
        .map(|u| tx_output(u, network))
        .collect::<Result<Vec<_>>>()?;
    Ok(q::AnyChainTx {
        native_bytes: vec![],
        block_ref: Some(chain_point(block_ref)),
        chain: Some(q::any_chain_tx::Chain::Cardano(c::Tx {
            inputs,
            outputs,
            certificates: vec![],
            withdrawals: vec![],
            mint: vec![],
            reference_inputs: vec![],
            witnesses: None,
            collateral: None,
            fee: None,
            validity: None,
            successful: true,
            auxiliary: None,
            hash: t.hash.to_vec(),
            proposals: vec![],
            votes: vec![],
        })),
    })
}

#[derive(Clone, Copy)]
enum AddressFilter {
    PayOnly([u8; 28]),
    PayAndStake([u8; 28], [u8; 28]),
    StakeOnly([u8; 28]),
}

fn cred28(b: Vec<u8>, name: &str) -> Result<[u8; 28], Status> {
    b.try_into()
        .map_err(|_| Status::invalid_argument(format!("{name} must be 28 bytes")))
}

/// Only a single address-pattern predicate is supported -- see the
/// module doc and `plan-rpc.md`'s "RPC surface" table for what's
/// deliberately out of scope (assets, certs, `exact_address`,
/// `not`/`all_of`/`any_of` composition).
/// Shared by `SearchUtxos` (via `parse_predicate`, below) and
/// `UtxoChanges` (which takes a bare `AddressPattern`, no
/// `UtxoPredicate` wrapper, by design -- see `UtxoChangesRequest`'s
/// doc comment).
fn address_filter_from_pattern(
    address: c::AddressPattern,
    rpc: &str,
) -> Result<AddressFilter, Status> {
    if address.exact_address.is_some() {
        return Err(Status::unimplemented(format!(
            "{rpc} by exact_address is not supported -- utxo stores no raw address bytes (plan-rpc.md)"
        )));
    }
    match (address.payment_part, address.delegation_part) {
        (Some(p), Some(d)) => Ok(AddressFilter::PayAndStake(
            cred28(p, "payment_part")?,
            cred28(d, "delegation_part")?,
        )),
        (Some(p), None) => Ok(AddressFilter::PayOnly(cred28(p, "payment_part")?)),
        (None, Some(d)) => Ok(AddressFilter::StakeOnly(cred28(d, "delegation_part")?)),
        (None, None) => Err(Status::invalid_argument(
            "address pattern needs at least one of payment_part/delegation_part",
        )),
    }
}

fn parse_predicate(predicate: Option<q::UtxoPredicate>) -> Result<AddressFilter, Status> {
    let predicate =
        predicate.ok_or_else(|| Status::invalid_argument("SearchUtxos requires a predicate"))?;
    if !predicate.not.is_empty() || !predicate.all_of.is_empty() || !predicate.any_of.is_empty() {
        return Err(Status::unimplemented(
            "SearchUtxos only supports a single address-pattern predicate; \
             not/all_of/any_of composition is not supported",
        ));
    }
    let m = predicate
        .r#match
        .ok_or_else(|| Status::invalid_argument("predicate.match is required"))?;
    let pattern = match m.utxo_pattern {
        Some(q::any_utxo_pattern::UtxoPattern::Cardano(p)) => p,
        None => return Err(Status::invalid_argument("predicate.match is empty")),
    };
    if pattern.asset.is_some() {
        return Err(Status::unimplemented(
            "SearchUtxos by asset pattern is not supported -- assets are out of scope (plan-rpc.md)",
        ));
    }
    let address = pattern.address.ok_or_else(|| {
        Status::unimplemented(
            "SearchUtxos requires an address pattern; no other match kind is supported",
        )
    })?;
    address_filter_from_pattern(address, "SearchUtxos")
}

#[tonic::async_trait]
impl QueryService for Svc {
    async fn read_utxos(
        &self,
        request: Request<q::ReadUtxosRequest>,
    ) -> Result<Response<q::ReadUtxosResponse>, Status> {
        let req = request.into_inner();
        let keys = req
            .keys
            .iter()
            .map(txo_ref_to_output_ref)
            .collect::<Result<Vec<_>, Status>>()?;
        let db_path = self.db_path.clone();
        let network = self.network;
        let (items, tip) = tokio::task::spawn_blocking(move || -> Result<_> {
            let conn = query::open_reader(&db_path)?;
            let mut items = Vec::new();
            for r in keys {
                if let Some(u) = query::by_ref(&conn, r)? {
                    items.push(any_utxo_data(&conn, u, network)?);
                }
            }
            let tip = query::tip_point(&conn)?;
            Ok((items, tip))
        })
        .await
        .map_err(join_err)?
        .map_err(map_err)?;

        Ok(Response::new(q::ReadUtxosResponse {
            items,
            ledger_tip: Some(chain_point(tip)),
        }))
    }

    async fn search_utxos(
        &self,
        request: Request<q::SearchUtxosRequest>,
    ) -> Result<Response<q::SearchUtxosResponse>, Status> {
        let req = request.into_inner();
        let filter = parse_predicate(req.predicate)?;
        let limit = clamp_limit(req.max_items, self.max_page_size);
        let after = req.start_token.as_deref().map(parse_cursor).transpose()?;
        let db_path = self.db_path.clone();
        let network = self.network;
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
                .map(|u| any_utxo_data(&conn, u, network))
                .collect::<Result<Vec<_>>>()?;
            Ok((items, last_ref, tip))
        })
        .await
        .map_err(join_err)?
        .map_err(map_err)?;

        Ok(Response::new(q::SearchUtxosResponse {
            items,
            ledger_tip: Some(chain_point(tip)),
            next_token: last_ref.map(format_cursor),
        }))
    }

    async fn read_params(
        &self,
        _request: Request<q::ReadParamsRequest>,
    ) -> Result<Response<q::ReadParamsResponse>, Status> {
        Err(Status::unimplemented(
            "ReadParams is not supported (deferred, see plan-rpc.md)",
        ))
    }

    async fn read_data(
        &self,
        request: Request<q::ReadDataRequest>,
    ) -> Result<Response<q::ReadDataResponse>, Status> {
        let req = request.into_inner();
        let hash: [u8; 32] = req
            .key
            .try_into()
            .map_err(|_| Status::invalid_argument("key must be 32 bytes"))?;
        let db_path = self.db_path.clone();
        let (datum, tip) = tokio::task::spawn_blocking(move || -> Result<_> {
            let conn = query::open_reader(&db_path)?;
            let datum = query::datum_by_hash(&conn, hash)?;
            let tip = query::tip_point(&conn)?;
            Ok((datum, tip))
        })
        .await
        .map_err(join_err)?
        .map_err(map_err)?;

        let datum = datum.ok_or_else(|| Status::not_found("datum not found"))?;
        Ok(Response::new(q::ReadDataResponse {
            value: Some(q::AnyChainDatum {
                native_bytes: datum.cbor,
                key: datum.hash.to_vec(),
                // `PlutusData` parsing isn't implemented (see the
                // module doc) -- `native_bytes` alone is lossless.
                parsed_state: None,
            }),
            ledger_tip: Some(chain_point(tip)),
        }))
    }

    async fn read_tx(
        &self,
        request: Request<q::ReadTxRequest>,
    ) -> Result<Response<q::ReadTxResponse>, Status> {
        let req = request.into_inner();
        let hash: crate::decode::TxHash = req
            .hash
            .try_into()
            .map_err(|_| Status::invalid_argument("hash must be 32 bytes"))?;
        let db_path = self.db_path.clone();
        let network = self.network;
        let (tx, tip) = tokio::task::spawn_blocking(move || -> Result<_> {
            let conn = query::open_reader(&db_path)?;
            let tx = query::by_tx_hash(&conn, hash)?
                .map(|t| any_chain_tx(&conn, t, network))
                .transpose()?;
            let tip = query::tip_point(&conn)?;
            Ok((tx, tip))
        })
        .await
        .map_err(join_err)?
        .map_err(map_err)?;

        let tx = tx.ok_or_else(|| Status::not_found("tx not found"))?;
        Ok(Response::new(q::ReadTxResponse {
            tx: Some(tx),
            ledger_tip: Some(chain_point(tip)),
        }))
    }

    async fn read_genesis(
        &self,
        _request: Request<q::ReadGenesisRequest>,
    ) -> Result<Response<q::ReadGenesisResponse>, Status> {
        Err(Status::unimplemented(
            "ReadGenesis is not supported (deferred, see plan-rpc.md)",
        ))
    }

    async fn read_era_summary(
        &self,
        _request: Request<q::ReadEraSummaryRequest>,
    ) -> Result<Response<q::ReadEraSummaryResponse>, Status> {
        Err(Status::unimplemented(
            "ReadEraSummary is not supported (deferred, see plan-rpc.md)",
        ))
    }

    async fn read_state(
        &self,
        _request: Request<q::ReadStateRequest>,
    ) -> Result<Response<q::ReadStateResponse>, Status> {
        Err(Status::unimplemented(
            "ReadState is not supported (deferred, see plan-rpc.md)",
        ))
    }

    /// floe's own addition (not in the utxorpc spec) -- see
    /// `UtxoChangesRequest`'s doc comment and plan-rpc.md's "New RPC:
    /// UTxO change feed since a `Point`".
    ///
    /// The `since` validity check and the diff itself run inside one
    /// explicit read transaction on one connection -- an
    /// implementation constraint, not optional: two bare statements
    /// on the same connection aren't guaranteed to see the same
    /// snapshot in WAL mode if a writer commits in between, which
    /// would reopen exactly the inconsistent-if-a-rollback-lands-
    /// mid-request race this whole design exists to avoid.
    async fn utxo_changes(
        &self,
        request: Request<q::UtxoChangesRequest>,
    ) -> Result<Response<q::UtxoChangesResponse>, Status> {
        let req = request.into_inner();
        let address = req
            .address
            .ok_or_else(|| Status::invalid_argument("UtxoChanges requires an address pattern"))?;
        let filter = address_filter_from_pattern(address, "UtxoChanges")?;
        let (slot, hash) = match req.since {
            None => (None, None),
            Some(p) => {
                let hash = p
                    .hash
                    .map(|h| -> Result<[u8; 32], Status> {
                        h.try_into()
                            .map_err(|_| Status::invalid_argument("since.hash must be 32 bytes"))
                    })
                    .transpose()?;
                (p.slot, hash)
            }
        };
        let limit = clamp_limit(req.max_items, self.max_page_size);
        let created_after = req
            .created_start_token
            .as_deref()
            .map(parse_cursor)
            .transpose()?;
        let spent_after = req
            .spent_start_token
            .as_deref()
            .map(parse_cursor)
            .transpose()?;

        let db_path = self.db_path.clone();
        let network = self.network;
        let outcome = tokio::task::spawn_blocking(move || -> Result<_> {
            let mut conn = query::open_reader(&db_path)?;
            let tx = conn.transaction()?;
            let since_slot = match query::check_point(&tx, slot, hash)? {
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
                AddressFilter::PayOnly(p) => query::spent_since_by_pay_cred_any_script(
                    &tx,
                    p,
                    since_slot,
                    spent_after,
                    limit,
                )?,
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
            let tip = query::tip_point(&tx)?;
            let created_next = created.last().map(|u| u.output_ref);
            let spent_next = spent.last().map(|u| u.output_ref);
            let created = created
                .into_iter()
                .map(|u| any_utxo_data(&tx, u, network))
                .collect::<Result<Vec<_>>>()?;
            let spent = spent
                .into_iter()
                .map(|u| any_utxo_data(&tx, u, network))
                .collect::<Result<Vec<_>>>()?;
            tx.commit()?;
            Ok(ChangesOutcome::Changes {
                created,
                created_next,
                spent,
                spent_next,
                tip,
            })
        })
        .await
        .map_err(join_err)?
        .map_err(map_err)?;

        let result = match outcome {
            ChangesOutcome::Stale(reason) => {
                q::utxo_changes_response::Result::Stale(stale_proto(reason))
            }
            ChangesOutcome::Changes {
                created,
                created_next,
                spent,
                spent_next,
                tip,
            } => q::utxo_changes_response::Result::Changes(q::Changes {
                created,
                spent,
                ledger_tip: Some(chain_point(tip)),
                created_next_token: created_next.map(format_cursor),
                spent_next_token: spent_next.map(format_cursor),
            }),
        };
        Ok(Response::new(q::UtxoChangesResponse {
            result: Some(result),
        }))
    }
}

enum ChangesOutcome {
    Stale(query::StaleReason),
    Changes {
        created: Vec<q::AnyUtxoData>,
        created_next: Option<OutputRef>,
        spent: Vec<q::AnyUtxoData>,
        spent_next: Option<OutputRef>,
        tip: Option<BlockPoint>,
    },
}

fn stale_proto(reason: query::StaleReason) -> q::Stale {
    match reason {
        query::StaleReason::Unknown => q::Stale {
            reason: q::StaleReason::Unspecified as i32,
            oldest_retained_slot: None,
        },
        query::StaleReason::RolledBack {
            oldest_retained_slot,
        } => q::Stale {
            reason: q::StaleReason::RolledBack as i32,
            oldest_retained_slot: Some(oldest_retained_slot),
        },
        query::StaleReason::TooOld {
            oldest_retained_slot,
        } => q::Stale {
            reason: q::StaleReason::TooOld as i32,
            oldest_retained_slot: Some(oldest_retained_slot),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catchup::Engine;
    use crate::decode::{DatumKind, DecodedBlock, TxRow, Utxo};
    use crate::store::{self, BlockMeta};
    use q::query_service_client::QueryServiceClient;
    use std::net::SocketAddr;
    use std::net::TcpListener as StdTcpListener;
    use std::time::Duration;
    use tonic::transport::Channel;

    struct TempDb(std::path::PathBuf);

    impl TempDb {
        fn create() -> Self {
            let path = std::env::temp_dir().join(format!(
                "floe_grpc_test_{}_{}.sqlite",
                std::process::id(),
                rand_suffix()
            ));
            Self(path)
        }
        fn path_str(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
            }
        }
    }

    fn rand_suffix() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// `address`'s header byte (0x61 everywhere this fixture is used) is
    /// mainnet/enterprise/key-hash -- same fixture `http.rs`'s deleted
    /// test suite used, see its own doc comment on why `pay_cred` is
    /// just the trailing 28 bytes.
    fn utxo(ix_byte: u8, address: Vec<u8>) -> Utxo {
        let pay_cred: [u8; 28] = address[1..29].try_into().unwrap();
        Utxo {
            output_ref: OutputRef {
                tx_hash: [ix_byte; 32],
                ix: 0,
            },
            pay_cred,
            pay_script: false,
            stake_cred: None,
            stake_script: None,
            coin: 7,
            assets: None,
            datum_kind: DatumKind::None,
            datum_hash: None,
            script_hash: None,
        }
    }

    fn seeded_db() -> TempDb {
        let db = TempDb::create();
        let mut conn = Connection::open(db.path_str()).unwrap();
        store::init(&conn).unwrap();
        let addr = vec![0x61u8; 29];
        let decoded = DecodedBlock {
            txs: (0..3u8).map(|i| TxRow { hash: [i; 32] }).collect(),
            spent: Default::default(),
            created: (0..3u8).map(|i| utxo(i, addr.clone())).collect(),
            datums: vec![],
            scripts: vec![],
        };
        store::apply(
            &mut conn,
            &BlockMeta {
                slot: 1,
                hash: [1; 32],
                prev_hash: [0; 32],
                block_no: Some(1),
            },
            &decoded,
        )
        .unwrap();
        Engine::new(store::tip(&conn).unwrap())
            .switch_to_running(&mut conn)
            .unwrap();
        db
    }

    /// Two blocks: slot 1 creates `utxo(0, addr)`; slot 2 spends it
    /// (via tx `[9;32]`) and creates `utxo(9, addr)` -- enough to
    /// exercise `UtxoChanges`'s both halves (newly created, newly
    /// spent) against a real "since" boundary.
    fn two_block_db() -> TempDb {
        let db = TempDb::create();
        let mut conn = Connection::open(db.path_str()).unwrap();
        store::init(&conn).unwrap();
        let addr = vec![0x61u8; 29];

        store::apply(
            &mut conn,
            &BlockMeta {
                slot: 1,
                hash: [1; 32],
                prev_hash: [0; 32],
                block_no: Some(1),
            },
            &DecodedBlock {
                txs: vec![TxRow { hash: [0u8; 32] }],
                spent: Default::default(),
                created: vec![utxo(0, addr.clone())],
                datums: vec![],
                scripts: vec![],
            },
        )
        .unwrap();

        let mut spent = std::collections::BTreeMap::new();
        spent.insert(
            OutputRef {
                tx_hash: [0u8; 32],
                ix: 0,
            },
            [9u8; 32],
        );
        store::apply(
            &mut conn,
            &BlockMeta {
                slot: 2,
                hash: [2; 32],
                prev_hash: [1; 32],
                block_no: Some(2),
            },
            &DecodedBlock {
                txs: vec![TxRow { hash: [9u8; 32] }],
                spent,
                created: vec![utxo(9, addr.clone())],
                datums: vec![],
                scripts: vec![],
            },
        )
        .unwrap();

        Engine::new(store::tip(&conn).unwrap())
            .switch_to_running(&mut conn)
            .unwrap();
        db
    }

    /// Binds an OS-assigned port synchronously, closes the listener, and
    /// hands the freed port to `serve` -- a small bind-race accepted for
    /// test simplicity (same tradeoff this crate already takes with
    /// `rand_suffix`'s temp-file naming). Polls until the server is
    /// actually accepting before returning, so callers never race its
    /// startup.
    async fn spawn_server(db_path: &str, network: Network) -> SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let db_path: Arc<str> = Arc::from(db_path);
        tokio::spawn(async move {
            serve(
                &addr.to_string(),
                db_path,
                network,
                1000,
                None,
                None,
                Arc::new(crate::metrics::Metrics::default()),
            )
            .await
            .unwrap();
        });
        for _ in 0..200 {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        addr
    }

    async fn client(addr: SocketAddr) -> QueryServiceClient<Channel> {
        QueryServiceClient::connect(format!("http://{addr}"))
            .await
            .unwrap()
    }

    fn address_predicate(
        payment_part: Option<Vec<u8>>,
        delegation_part: Option<Vec<u8>>,
    ) -> q::UtxoPredicate {
        q::UtxoPredicate {
            r#match: Some(q::AnyUtxoPattern {
                utxo_pattern: Some(q::any_utxo_pattern::UtxoPattern::Cardano(
                    c::TxOutputPattern {
                        address: Some(c::AddressPattern {
                            exact_address: None,
                            payment_part,
                            delegation_part,
                        }),
                        asset: None,
                    },
                )),
            }),
            not: vec![],
            all_of: vec![],
            any_of: vec![],
        }
    }

    #[tokio::test]
    async fn read_utxos_returns_found_items_and_skips_missing_keys() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let resp = c
            .read_utxos(q::ReadUtxosRequest {
                keys: vec![
                    q::TxoRef {
                        hash: vec![0u8; 32],
                        index: 0,
                    },
                    q::TxoRef {
                        hash: vec![0xFFu8; 32],
                        index: 0,
                    },
                ],
                field_mask: None,
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(resp.items.len(), 1, "the second key doesn't exist");
        let item = &resp.items[0];
        assert_eq!(item.txo_ref.as_ref().unwrap().hash, vec![0u8; 32]);
        match item.parsed_state.as_ref().unwrap() {
            q::any_utxo_data::ParsedState::Cardano(out) => {
                assert_eq!(out.coin, Some(big_int_u64(7)));
            }
        }
        assert!(item.spent_by.is_none());
        assert!(resp.ledger_tip.is_some());
    }

    #[tokio::test]
    async fn search_utxos_by_payment_part_paginates() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;
        let pay_cred = vec![0x61u8; 29][1..29].to_vec();
        let predicate = address_predicate(Some(pay_cred), None);

        let resp = c
            .search_utxos(q::SearchUtxosRequest {
                predicate: Some(predicate.clone()),
                field_mask: None,
                max_items: Some(2),
                start_token: None,
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.items.len(), 2);
        let next = resp.next_token.clone().expect("more rows remain");

        let resp2 = c
            .search_utxos(q::SearchUtxosRequest {
                predicate: Some(predicate),
                field_mask: None,
                max_items: Some(2),
                start_token: Some(next),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp2.items.len(), 1, "one row left on the second page");
    }

    #[tokio::test]
    async fn search_utxos_reports_unavailable_in_catchup() {
        let db = TempDb::create();
        {
            let conn = Connection::open(db.path_str()).unwrap();
            store::init(&conn).unwrap();
        }
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;
        let predicate = address_predicate(Some(vec![0u8; 28]), None);

        let err = c
            .search_utxos(q::SearchUtxosRequest {
                predicate: Some(predicate),
                field_mask: None,
                max_items: None,
                start_token: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn search_utxos_rejects_asset_pattern() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;
        let predicate = q::UtxoPredicate {
            r#match: Some(q::AnyUtxoPattern {
                utxo_pattern: Some(q::any_utxo_pattern::UtxoPattern::Cardano(
                    c::TxOutputPattern {
                        address: None,
                        asset: Some(c::AssetPattern {
                            policy_id: Some(vec![1u8; 28]),
                            asset_name: None,
                        }),
                    },
                )),
            }),
            not: vec![],
            all_of: vec![],
            any_of: vec![],
        };

        let err = c
            .search_utxos(q::SearchUtxosRequest {
                predicate: Some(predicate),
                field_mask: None,
                max_items: None,
                start_token: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn search_utxos_rejects_exact_address() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;
        let predicate = q::UtxoPredicate {
            r#match: Some(q::AnyUtxoPattern {
                utxo_pattern: Some(q::any_utxo_pattern::UtxoPattern::Cardano(
                    c::TxOutputPattern {
                        address: Some(c::AddressPattern {
                            exact_address: Some(vec![0x61u8; 29]),
                            payment_part: None,
                            delegation_part: None,
                        }),
                        asset: None,
                    },
                )),
            }),
            not: vec![],
            all_of: vec![],
            any_of: vec![],
        };

        let err = c
            .search_utxos(q::SearchUtxosRequest {
                predicate: Some(predicate),
                field_mask: None,
                max_items: None,
                start_token: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn unsupported_rpcs_return_unimplemented() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let err = c
            .read_params(q::ReadParamsRequest { field_mask: None })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn read_tx_returns_resolved_inputs_and_outputs() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        // tx 1's output (ix 0) is spent by tx 2 (see `seeded_db`'s
        // fixture: `created[i]`'s `tx_hash = [i; 32]`, all three in the
        // same block, none spent by each other) -- so instead build a
        // spend directly via `store::apply` for a clean input/output
        // pair, same as `store::query::tests` does.
        let resp = c
            .read_tx(q::ReadTxRequest {
                hash: vec![0u8; 32],
                field_mask: None,
            })
            .await
            .unwrap()
            .into_inner();
        let tx = resp.tx.expect("tx 0 was indexed by seeded_db");
        match tx.chain.unwrap() {
            q::any_chain_tx::Chain::Cardano(t) => {
                assert_eq!(t.hash, vec![0u8; 32]);
                assert_eq!(t.outputs.len(), 1, "tx 0 created exactly one output");
                assert!(t.inputs.is_empty(), "tx 0 spent nothing in this fixture");
                assert_eq!(t.outputs[0].coin, Some(big_int_u64(7)));
            }
        }
        assert!(resp.ledger_tip.is_some());
    }

    #[tokio::test]
    async fn read_tx_reports_not_found_for_an_unknown_hash() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let err = c
            .read_tx(q::ReadTxRequest {
                hash: vec![0xFFu8; 32],
                field_mask: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn read_data_resolves_a_witnessed_hash_datum() {
        let db = TempDb::create();
        let mut conn = Connection::open(db.path_str()).unwrap();
        store::init(&conn).unwrap();
        let datum_hash = [0x42u8; 32];
        let datum_cbor = vec![1, 2, 3, 4];
        let decoded = DecodedBlock {
            txs: vec![TxRow { hash: [0u8; 32] }],
            spent: Default::default(),
            created: vec![Utxo {
                output_ref: OutputRef {
                    tx_hash: [0u8; 32],
                    ix: 0,
                },
                pay_cred: [0x61u8; 28],
                pay_script: false,
                stake_cred: None,
                stake_script: None,
                coin: 7,
                assets: None,
                datum_kind: DatumKind::Hash,
                datum_hash: Some(datum_hash),
                script_hash: None,
            }],
            datums: vec![crate::decode::DatumRow {
                hash: datum_hash,
                cbor: datum_cbor.clone(),
            }],
            scripts: vec![],
        };
        store::apply(
            &mut conn,
            &BlockMeta {
                slot: 1,
                hash: [1; 32],
                prev_hash: [0; 32],
                block_no: Some(1),
            },
            &decoded,
        )
        .unwrap();
        Engine::new(store::tip(&conn).unwrap())
            .switch_to_running(&mut conn)
            .unwrap();
        drop(conn);

        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let resp = c
            .read_data(q::ReadDataRequest {
                key: datum_hash.to_vec(),
                field_mask: None,
            })
            .await
            .unwrap()
            .into_inner();
        let value = resp.value.expect("datum was witnessed in this fixture");
        assert_eq!(value.key, datum_hash.to_vec());
        assert_eq!(value.native_bytes, datum_cbor);

        let err = c
            .read_data(q::ReadDataRequest {
                key: vec![0u8; 32],
                field_mask: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    fn pay_cred_address() -> c::AddressPattern {
        c::AddressPattern {
            exact_address: None,
            payment_part: Some(vec![0x61u8; 28]),
            delegation_part: None,
        }
    }

    /// `UtxoChangesRequest`'s three pagination fields default to "no
    /// paging" -- a shorthand for test bodies that don't care.
    fn changes_request(since: Option<q::Point>) -> q::UtxoChangesRequest {
        q::UtxoChangesRequest {
            address: Some(pay_cred_address()),
            since,
            max_items: None,
            created_start_token: None,
            spent_start_token: None,
        }
    }

    #[tokio::test]
    async fn utxo_changes_with_no_since_reports_the_live_row_as_created_and_the_spend_as_spent() {
        let db = two_block_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let resp = c
            .utxo_changes(changes_request(None))
            .await
            .unwrap()
            .into_inner();
        let changes = match resp.result.unwrap() {
            q::utxo_changes_response::Result::Changes(c) => c,
            q::utxo_changes_response::Result::Stale(_) => panic!("no since given, can't be stale"),
        };
        // No lower bound: every live row is "created", every
        // ever-spent row is "spent" -- tx9's output is live, tx0's
        // was spent by tx9.
        assert_eq!(changes.created.len(), 1);
        assert_eq!(
            changes.created[0].txo_ref.as_ref().unwrap().hash,
            vec![9u8; 32]
        );
        assert_eq!(changes.spent.len(), 1);
        assert_eq!(
            changes.spent[0].txo_ref.as_ref().unwrap().hash,
            vec![0u8; 32]
        );
        assert_eq!(changes.spent[0].spent_by, Some(vec![9u8; 32]));
        // Keyset pagination: `next_token` means "resume from here",
        // not "this is the last page" -- set whenever the page is
        // non-empty, same convention `SearchUtxos` already uses.
        assert!(changes.created_next_token.is_some());
        assert!(changes.spent_next_token.is_some());
    }

    #[tokio::test]
    async fn utxo_changes_since_slot_one_permissive_excludes_the_already_known_row() {
        let db = two_block_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        // A caller who already knew the chain up to slot 1 (no hash,
        // just a bare numeric lower bound -- permissive mode, no
        // chain-validity check) should see tx9's creation and tx0's
        // spend, since both happened at slot 2.
        let resp = c
            .utxo_changes(changes_request(Some(q::Point {
                slot: Some(1),
                hash: None,
            })))
            .await
            .unwrap()
            .into_inner();
        let changes = match resp.result.unwrap() {
            q::utxo_changes_response::Result::Changes(c) => c,
            q::utxo_changes_response::Result::Stale(_) => panic!("permissive mode is never stale"),
        };
        assert_eq!(changes.created.len(), 1);
        assert_eq!(changes.spent.len(), 1);
    }

    #[tokio::test]
    async fn utxo_changes_accepts_a_valid_hash_point() {
        let db = two_block_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        // Block 1's own hash, strict mode -- still on the canonical
        // chain, so this must behave exactly like the permissive
        // `slot: 1` case above, not report stale.
        let resp = c
            .utxo_changes(changes_request(Some(q::Point {
                slot: None,
                hash: Some(vec![1u8; 32]),
            })))
            .await
            .unwrap()
            .into_inner();
        let changes = match resp.result.unwrap() {
            q::utxo_changes_response::Result::Changes(c) => c,
            q::utxo_changes_response::Result::Stale(_) => {
                panic!("block 1's hash is still canonical")
            }
        };
        assert_eq!(changes.created.len(), 1);
        assert_eq!(changes.spent.len(), 1);
    }

    /// `hash` given with no `slot` to compare against the retention
    /// window -- nothing to classify beyond "not found".
    #[tokio::test]
    async fn utxo_changes_reports_unspecified_stale_for_an_unknown_hash_with_no_slot() {
        let db = two_block_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let resp = c
            .utxo_changes(changes_request(Some(q::Point {
                slot: None,
                hash: Some(vec![0xFFu8; 32]),
            })))
            .await
            .unwrap()
            .into_inner();
        match resp.result.unwrap() {
            q::utxo_changes_response::Result::Stale(stale) => {
                assert_eq!(stale.reason, q::StaleReason::Unspecified as i32);
                assert!(stale.oldest_retained_slot.is_none());
            }
            q::utxo_changes_response::Result::Changes(_) => panic!("unknown hash must be stale"),
        }
    }

    /// Block 1's real hash, but claiming it's at slot 2 (it's at slot
    /// 1): slot 2 is still within the retained window (oldest
    /// retained is slot 1), so the server has data covering it and
    /// this hash isn't part of it -- a real reorg signal, not "too
    /// old".
    #[tokio::test]
    async fn utxo_changes_reports_rolled_back_when_hash_and_slot_disagree_within_history() {
        let db = two_block_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let resp = c
            .utxo_changes(changes_request(Some(q::Point {
                slot: Some(2),
                hash: Some(vec![1u8; 32]),
            })))
            .await
            .unwrap()
            .into_inner();
        match resp.result.unwrap() {
            q::utxo_changes_response::Result::Stale(stale) => {
                assert_eq!(stale.reason, q::StaleReason::RolledBack as i32);
                assert_eq!(stale.oldest_retained_slot, Some(1));
            }
            q::utxo_changes_response::Result::Changes(_) => {
                panic!("mismatched slot/hash within history must be rolled_back")
            }
        }
    }

    /// Slot 0 predates the oldest slot the fixture ever has (slot 1)
    /// -- genuinely out of history, not a reorg question at all.
    #[tokio::test]
    async fn utxo_changes_reports_too_old_for_a_slot_before_retained_history() {
        let db = two_block_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let resp = c
            .utxo_changes(changes_request(Some(q::Point {
                slot: Some(0),
                hash: Some(vec![0xFFu8; 32]),
            })))
            .await
            .unwrap()
            .into_inner();
        match resp.result.unwrap() {
            q::utxo_changes_response::Result::Stale(stale) => {
                assert_eq!(stale.reason, q::StaleReason::TooOld as i32);
                assert_eq!(stale.oldest_retained_slot, Some(1));
            }
            q::utxo_changes_response::Result::Changes(_) => {
                panic!("a slot before retained history must be too_old")
            }
        }
    }

    #[tokio::test]
    async fn utxo_changes_paginates_created_and_spent_independently() {
        let db = two_block_db();
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        let resp = c
            .utxo_changes(q::UtxoChangesRequest {
                address: Some(pay_cred_address()),
                since: None,
                max_items: Some(1),
                created_start_token: None,
                spent_start_token: None,
            })
            .await
            .unwrap()
            .into_inner();
        let changes = match resp.result.unwrap() {
            q::utxo_changes_response::Result::Changes(c) => c,
            q::utxo_changes_response::Result::Stale(_) => panic!("no since given, can't be stale"),
        };
        assert_eq!(changes.created.len(), 1);
        assert_eq!(changes.spent.len(), 1);
        let created_next = changes.created_next_token.clone().expect("non-empty page");
        let spent_next = changes.spent_next_token.clone().expect("non-empty page");

        // Resuming with each cursor must come back empty -- keyset
        // pagination only confirms exhaustion this way, never by a
        // short first page (same convention `SearchUtxos` uses).
        let resp2 = c
            .utxo_changes(q::UtxoChangesRequest {
                address: Some(pay_cred_address()),
                since: None,
                max_items: Some(1),
                created_start_token: Some(created_next),
                spent_start_token: Some(spent_next),
            })
            .await
            .unwrap()
            .into_inner();
        let changes2 = match resp2.result.unwrap() {
            q::utxo_changes_response::Result::Changes(c) => c,
            q::utxo_changes_response::Result::Stale(_) => panic!("no since given, can't be stale"),
        };
        assert!(changes2.created.is_empty());
        assert!(changes2.spent.is_empty());
        assert!(changes2.created_next_token.is_none());
        assert!(changes2.spent_next_token.is_none());
    }

    #[tokio::test]
    async fn utxo_changes_reports_not_ready_during_catchup() {
        let db = TempDb::create();
        {
            let conn = Connection::open(db.path_str()).unwrap();
            store::init(&conn).unwrap();
        }
        let addr = spawn_server(db.path_str(), Network::Mainnet).await;
        let mut c = client(addr).await;

        // Even a plainly-unusable `since` (unknown hash) must not be
        // reached during catch-up -- `check_point` requires running
        // mode itself, same `NotReady` every other secondary-index
        // lookup reports, rather than attempting (and possibly
        // misjudging) a point check against incomplete history.
        let err = c
            .utxo_changes(changes_request(Some(q::Point {
                slot: None,
                hash: Some(vec![0xFFu8; 32]),
            })))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }
}
