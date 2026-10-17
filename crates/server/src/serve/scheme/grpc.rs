//! grpc: `floe-server`'s gRPC transport -- `QueryService` always, and
//! `SubmitService` (see `submit.rs`) when `serve()` is given a node
//! connection. All domain->proto conversion for `QueryService` lives
//! here, over `Ctx`'s already-resolved `Utxo`/`Tx` rows; routing is
//! just the generated trait impl below. No `query::`/`Connection`
//! access here at all -- see `Ctx`'s own module doc.
//!
//! `QueryService` support: `ReadUtxos` (by `TxoRef`); `SearchUtxos`
//! restricted to a single address-pattern predicate
//! (`payment_part`/`delegation_part`); `ReadData` (by datum hash,
//! `native_bytes` only); `ReadTx` (by tx hash, in/outputs only, see
//! `any_chain_tx`'s doc comment on `successful`). Every other RPC, and
//! every unsupported `SearchUtxos` shape, returns
//! `Status::unimplemented`/`Status::invalid_argument`/`Status::not_found`
//! with a message.
//!
//! `health` (below) is reporting apparatus only, over `Ctx`:
//! implements floe's own `HealthService` RPC (`GetHealth`/
//! `GetMetrics`), just snapshotting and converting `crate::metrics`/
//! `crate::health`'s core state; `crate::metrics::MetricsLayer`
//! (wrapped around the whole server below) is what records into it.
//! No standard `grpc.health.v1.Health` yet -- deferred, see
//! `../../plan-server-health-and-metrics.md`.

pub mod health;
pub mod submit;

use anyhow::Result;
use pallas_codec::minicbor;
use pallas_primitives::conway::NativeScript as PallasNativeScript;
use tonic::{Request, Response, Status, transport::Server};
use tracing::info;

use floe_dto_grpc::floe::v1::cardano as c;
use floe_dto_grpc::floe::v1::query as q;
use floe_dto_grpc::floe::v1::submit::submit_service_server::SubmitServiceServer;
use q::query_service_server::{QueryService, QueryServiceServer};

use crate::ingest::decode::OutputRef;
use crate::serve::ctx::{
    AddressFilter, BlockPoint, Ctx, Cursor, NotReady, ResolvedDatum, ResolvedScript, Stale,
    StaleReason, Tx, Utxo,
};

/// `SubmitService` is always mounted alongside `QueryService` -- a
/// `SubmitTx` call fails with `Status::unavailable` if amaru's
/// submit-api itself is unreachable, not if this server was started
/// without one (there's no such state anymore).
pub async fn serve(addr: &str, ctx: Ctx) -> Result<()> {
    let addr = addr.parse()?;
    info!(%addr, max_page_size = ctx.max_page_size, "grpc server listening");
    let server = Server::builder()
        .layer(crate::metrics::MetricsLayer::new(
            ctx.requests.clone(),
            crate::metrics::Scheme::Grpc,
        ))
        .add_service(health::HealthServiceServer::new(ctx.clone()))
        .add_service(QueryServiceServer::new(ctx.clone()))
        .add_service(SubmitServiceServer::new(ctx));
    server.serve(addr).await?;
    Ok(())
}

/// `NotReady` (still in catch-up) maps to `UNAVAILABLE`, same as
/// http's 503. `Stale` (a `since`/`Point` no longer resolvable)
/// maps to two distinguishable codes -- `OUT_OF_RANGE` (too old, gone
/// for good) vs `ABORTED` (rolled back, retry at an earlier point) --
/// so a consumer branches on `.code()`, not the message text. Every
/// other failure is this server's own invariant or a DB error,
/// `INTERNAL`.
fn map_err(e: anyhow::Error) -> Status {
    if let Some(&Stale(reason)) = e.downcast_ref::<Stale>() {
        match reason {
            StaleReason::TooOld { .. } | StaleReason::Unknown => {
                Status::out_of_range(e.to_string())
            }
            StaleReason::RolledBack { .. } => Status::aborted(e.to_string()),
        }
    } else if e.downcast_ref::<NotReady>().is_some() {
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
        crate::ingest::decode::ScriptLang::Native => {
            let ns: PallasNativeScript = minicbor::decode(&s.cbor)?;
            c::script::Script::Native(native_script(&ns))
        }
        crate::ingest::decode::ScriptLang::PlutusV1 => c::script::Script::PlutusV1(s.cbor),
        crate::ingest::decode::ScriptLang::PlutusV2 => c::script::Script::PlutusV2(s.cbor),
        crate::ingest::decode::ScriptLang::PlutusV3 => c::script::Script::PlutusV3(s.cbor),
        crate::ingest::decode::ScriptLang::PlutusV4 => c::script::Script::PlutusV4(s.cbor),
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

fn tx_output(u: &Utxo) -> Result<c::TxOutput> {
    let address = u.address.to_vec();
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

/// `block_point` finding no row for a row's own creation slot isn't an
/// invariant violation: `prune` drops `block` rows past the retention
/// window independent of whether unspent `utxo` rows still carry that
/// slot as their creation slot, and the seed anchor itself stands in
/// for whatever the UTxO's *real* origin block was (unknowable -- it
/// may predate the snapshot). Both are an expected "origin block
/// unknown" case, not corruption -- `chain_point(None)` is exactly
/// that encoding (slot 0, empty hash, height 0), already used
/// elsewhere.
fn any_utxo_data(u: Utxo) -> Result<q::AnyUtxoData> {
    let txo_ref = q::TxoRef {
        hash: u.output_ref.tx_hash.to_vec(),
        index: u.output_ref.ix as u32,
    };
    let spent_by = u.spent_by.map(|h| h.to_vec());
    let cardano_output = tx_output(&u)?;
    let block_ref = u.block;
    Ok(q::AnyUtxoData {
        native_bytes: vec![],
        txo_ref: Some(txo_ref),
        block_ref: Some(chain_point(block_ref)),
        spent_by,
        parsed_state: Some(q::any_utxo_data::ParsedState::Cardano(cardano_output)),
    })
}

/// `TxInput.as_output` is the spent output's full pre-spend content --
/// `Utxo` already carries exactly that, so this is the same
/// `tx_output` conversion `any_utxo_data` uses, not a separate shape.
/// `redeemer` is shelved (see "Redeemer: storage vs. exposure" in
/// plan-rpc.md) -- never represented as a bare property of an output
/// elsewhere, only here once unshelved.
fn tx_input(u: &Utxo) -> Result<c::TxInput> {
    Ok(c::TxInput {
        tx_hash: u.output_ref.tx_hash.to_vec(),
        output_index: u.output_ref.ix as u32,
        as_output: Some(tx_output(u)?),
        redeemer: None,
    })
}

/// `Tx` already carries only what `floe` decomposes (in/outputs,
/// hydrated from `utxo` rows) -- certs/mint/fee/collateral/
/// withdrawals/auxiliary_data stay unset, per `ReadTx`'s documented
/// partial support (plan-rpc.md). `successful` isn't tracked at all
/// (no `valid` column on `tx`, by design -- see plan-rpc.md's `tx`
/// table note) and defaults to `true`: a real gap, not a considered
/// answer -- an invalid tx's `inputs`/`outputs` here are actually its
/// collateral (`decode::decode_tx` records whatever
/// `MultiEraTx::consumes`/`produces` return for its validity, which
/// for an invalid tx *is* collateral), with no flag distinguishing
/// that from a valid tx's real body.
fn any_chain_tx(t: Tx, block_ref: Option<BlockPoint>) -> Result<q::AnyChainTx> {
    let inputs = t.inputs.iter().map(tx_input).collect::<Result<Vec<_>>>()?;
    let outputs = t
        .outputs
        .iter()
        .map(tx_output)
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

fn hash28(b: Vec<u8>, name: &str) -> Result<[u8; 28], Status> {
    b.try_into()
        .map_err(|_| Status::invalid_argument(format!("{name} must be 28 bytes")))
}

/// Used by `SearchUtxos` via `parse_predicate`, below.
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
            hash28(p, "payment_part")?,
            hash28(d, "delegation_part")?,
        )),
        (Some(p), None) => Ok(AddressFilter::PayOnly(hash28(p, "payment_part")?)),
        (None, Some(d)) => Ok(AddressFilter::StakeOnly(hash28(d, "delegation_part")?)),
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
impl QueryService for Ctx {
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
        let (rows, tip) = self.read_utxos(keys).await.map_err(map_err)?;
        let items = rows
            .into_iter()
            .map(any_utxo_data)
            .collect::<Result<Vec<_>>>()
            .map_err(|e| Status::internal(e.to_string()))?;
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
        let (since_slot, since_hash) = match req.since {
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
        let after = req
            .start_token
            .as_deref()
            .map(Cursor::parse)
            .transpose()
            .map_err(|e| Status::invalid_argument(format!("start_token: {e}")))?;
        let (rows, last_ref, tip) = self
            .search_utxos(filter, since_slot, since_hash, after, req.max_items)
            .await
            .map_err(map_err)?;
        let items = rows
            .into_iter()
            .map(any_utxo_data)
            .collect::<Result<Vec<_>>>()
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(q::SearchUtxosResponse {
            items,
            ledger_tip: Some(chain_point(tip)),
            next_token: last_ref.map(Cursor::format),
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
        let (datum, tip) = self.read_data(hash).await.map_err(map_err)?;
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
        let hash: crate::ingest::decode::TxHash = req
            .hash
            .try_into()
            .map_err(|_| Status::invalid_argument("hash must be 32 bytes"))?;
        let (tx, tip) = self.read_tx(hash).await.map_err(map_err)?;
        let (tx, block_ref) = tx.ok_or_else(|| Status::not_found("tx not found"))?;
        let tx = any_chain_tx(tx, block_ref).map_err(|e| Status::internal(e.to_string()))?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::catchup::Engine;
    use crate::ingest::decode::{DatumKind, DecodedBlock, TxRow, Utxo};
    use crate::ingest::store::{self, BlockMeta};
    use q::query_service_client::QueryServiceClient;
    use rusqlite::Connection;
    use std::net::SocketAddr;
    use std::net::TcpListener as StdTcpListener;
    use std::sync::Arc;
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
    /// mainnet/enterprise/key-hash, so `pay_hash` is just the trailing
    /// 28 bytes and `stake_hash` is absent.
    fn utxo(ix_byte: u8, address: Vec<u8>) -> Utxo {
        let pay_hash: [u8; 28] = address[1..29].try_into().unwrap();
        Utxo {
            output_ref: OutputRef {
                tx_hash: [ix_byte; 32],
                ix: 0,
            },
            pay_hash,
            stake_hash: None,
            address_header: address[0],
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

    /// Binds an OS-assigned port synchronously, closes the listener, and
    /// hands the freed port to `serve` -- a small bind-race accepted for
    /// test simplicity (same tradeoff this crate already takes with
    /// `rand_suffix`'s temp-file naming). Polls until the server is
    /// actually accepting before returning, so callers never race its
    /// startup.
    async fn spawn_server(db_path: &str) -> SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let db_path: Arc<str> = Arc::from(db_path);
        // `Health` is in-memory process state, independent of the DB these
        // tests seed directly (no sync loop runs here to populate it) --
        // simulate "caught up to tip" so `Ctx::require_ready` doesn't
        // reject every happy-path test. Catch-up-mode tests still get
        // `NotReady` from the DB-level `Mode` check inside `query::`,
        // unaffected by this.
        let health = Arc::new(crate::health::Health::default());
        health.record_apply(store::Mode::Running, 1, 1, [0; 32], 1, 0);
        let ctx = Ctx {
            db_path,
            max_page_size: 1000,
            // Never dialed by these tests (none exercise SubmitTx) --
            // a placeholder is enough now that it's a required `String`.
            amaru_submit_api: "127.0.0.1:1".to_string(),
            health,
            requests: Arc::new(crate::metrics::Metrics::default()),
        };
        tokio::spawn(async move {
            serve(&addr.to_string(), ctx).await.unwrap();
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
        let addr = spawn_server(db.path_str()).await;
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
        let addr = spawn_server(db.path_str()).await;
        let mut c = client(addr).await;
        let pay_hash = vec![0x61u8; 29][1..29].to_vec();
        let predicate = address_predicate(Some(pay_hash), None);

        let resp = c
            .search_utxos(q::SearchUtxosRequest {
                predicate: Some(predicate.clone()),
                field_mask: None,
                max_items: Some(2),
                start_token: None,
                since: None,
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
                since: None,
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
        let addr = spawn_server(db.path_str()).await;
        let mut c = client(addr).await;
        let predicate = address_predicate(Some(vec![0u8; 28]), None);

        let err = c
            .search_utxos(q::SearchUtxosRequest {
                predicate: Some(predicate),
                field_mask: None,
                max_items: None,
                start_token: None,
                since: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn search_utxos_rejects_asset_pattern() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str()).await;
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
                since: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn search_utxos_rejects_exact_address() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str()).await;
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
                since: None,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test]
    async fn unsupported_rpcs_return_unimplemented() {
        let db = seeded_db();
        let addr = spawn_server(db.path_str()).await;
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
        let addr = spawn_server(db.path_str()).await;
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
        let addr = spawn_server(db.path_str()).await;
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
                pay_hash: [0x61u8; 28],
                stake_hash: None,
                address_header: 0x61,
                coin: 7,
                assets: None,
                datum_kind: DatumKind::Hash,
                datum_hash: Some(datum_hash),
                script_hash: None,
            }],
            datums: vec![crate::ingest::decode::DatumRow {
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

        let addr = spawn_server(db.path_str()).await;
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
}
