//! Mirrors `floe_dto_grpc::floe::v1::health` -- a typed, wire-format-
//! agnostic snapshot of `floe-server`'s health and metrics state,
//! replacing the old `health::Health`'s hand-written
//! `prometheus_text()` with a real DTO. The live atomic-counter state
//! itself lives in `floe-server`'s own `health`/`metrics` core modules
//! (this crate can't depend on `floe-server`'s domain types); these are
//! just the snapshots taken from it.
//!
//! `Display` renders Prometheus text directly -- `.to_string()`/`{v}`
//! *is* "render as Prometheus text", no separately-named method.
//! `Serialize`/`Decode` follow this crate's usual
//! `#[cfg_attr(feature = "json"/"cbor", derive(...))]` pattern, gated
//! together behind this module's own `metrics` feature (independent of
//! `json`/`cbor`, see ../Cargo.toml).

use std::fmt;

#[cfg(feature = "cbor")]
use minicbor::{Decode, Encode};
#[cfg(feature = "schema")]
use schemars::JsonSchema;
#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};

/// Where the writer process (`run` mode) is in its lifecycle --
/// independent of `floe-server::store::Mode` (catchup/running only,
/// persisted, a one-way switch). Seeding happens before `Mode` means
/// anything and isn't persisted; a fresh process starts here, so it
/// reports that distinctly instead of looking indistinguishable from
/// "down" (unreachable). No `Unspecified` case (unlike
/// `health.Stage`'s zero value) -- every reachable process is always in
/// one of these three, so there's nothing for an unspecified value to
/// mean.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
#[cfg_attr(feature = "json", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "cbor", cbor(index_only))]
pub enum Stage {
    #[default]
    #[cfg_attr(feature = "cbor", n(0))]
    Seeding = 0,
    #[cfg_attr(feature = "cbor", n(1))]
    Catchup = 1,
    #[cfg_attr(feature = "cbor", n(2))]
    Running = 2,
}

impl Stage {
    pub fn is_ready(self) -> bool {
        self == Stage::Running
    }
}

impl fmt::Display for Stage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Stage::Seeding => "seeding",
            Stage::Catchup => "catchup",
            Stage::Running => "running",
        };
        f.write_str(s)
    }
}

/// `GetHealth`'s own route -- bodyless GET, unlike every `/query`/
/// `/submit` RPC's POST (see `query.rs`'s own `*_PATH` constants).
pub const HEALTH_PATH: &str = "/health";

/// Mirrors `health.RunHealth`: `run` mode's `/health` body -- `stage`
/// plus chain-freshness. Staleness (how far behind the peer this node
/// is) is a readiness concern, so it lives here, not in
/// `MetricsResponse`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct RunHealth {
    #[cfg_attr(feature = "cbor", n(0))]
    pub stage: Stage,
    #[cfg_attr(feature = "cbor", n(1))]
    pub tip_slot: u64,
    #[cfg_attr(feature = "cbor", n(2))]
    pub block_no: u64,
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_array"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(3), cbor(with = "minicbor::bytes"))]
    pub tip_hash: [u8; 32],
    #[cfg_attr(feature = "cbor", n(4))]
    pub peer_tip_block_no: u64,
}

impl fmt::Display for RunHealth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let depth = self.peer_tip_block_no.saturating_sub(self.block_no);
        write!(
            f,
            "# HELP floe_stage 0 = seeding, 1 = catchup, 2 = running\n\
             # TYPE floe_stage gauge\n\
             floe_stage {}\n\
             # HELP floe_tip_slot Slot of the last applied/checkpointed block\n\
             # TYPE floe_tip_slot gauge\n\
             floe_tip_slot {}\n\
             # HELP floe_block_no Block number of the last applied/checkpointed block\n\
             # TYPE floe_block_no gauge\n\
             floe_block_no {}\n\
             # HELP floe_peer_tip_block_no Peer-reported chain tip block number as of the last applied block\n\
             # TYPE floe_peer_tip_block_no gauge\n\
             floe_peer_tip_block_no {}\n\
             # HELP floe_depth peer_tip_block_no minus the last applied block's block_no\n\
             # TYPE floe_depth gauge\n\
             floe_depth {depth}\n\
             # HELP floe_last_block Last applied block's header, as labels (value is always 1; compare slot/block_no/hash against an explorer)\n\
             # TYPE floe_last_block gauge\n\
             floe_last_block{{slot=\"{}\",block_no=\"{}\",hash=\"{}\"}} 1\n",
            self.stage as u8,
            self.tip_slot,
            self.block_no,
            self.peer_tip_block_no,
            self.tip_slot,
            self.block_no,
            hex::encode(self.tip_hash),
        )
    }
}

/// Mirrors `health.HealthResponse`'s oneof -- `GetHealth`'s/`/health`'s
/// payload.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "json", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
#[cfg_attr(feature = "cbor", cbor(flat))]
pub enum HealthResponse {
    #[cfg_attr(feature = "cbor", n(0))]
    Run(#[cfg_attr(feature = "cbor", n(0))] RunHealth),
}

impl fmt::Display for HealthResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthResponse::Run(h) => h.fmt(f),
        }
    }
}

/// Mirrors `health.Scheme` -- the transport a request arrived over.
/// Scheme-labeled, not gRPC-only, even though gRPC's per-RPC-method
/// counts are the only producer today (nothing forecloses a future
/// per-route HTTP counter landing in the same field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
#[cfg_attr(feature = "json", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "cbor", cbor(index_only))]
pub enum Scheme {
    #[cfg_attr(feature = "cbor", n(0))]
    Grpc,
    #[cfg_attr(feature = "cbor", n(1))]
    HttpJson,
    #[cfg_attr(feature = "cbor", n(2))]
    HttpCbor,
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Scheme::Grpc => "grpc",
            Scheme::HttpJson => "http_json",
            Scheme::HttpCbor => "http_cbor",
        };
        f.write_str(s)
    }
}

/// Mirrors `health.RequestCount`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct RequestCount {
    #[cfg_attr(feature = "cbor", n(0))]
    pub scheme: Scheme,
    #[cfg_attr(feature = "cbor", n(1))]
    pub method: String,
    #[cfg_attr(feature = "cbor", n(2))]
    pub count: u64,
}

/// `GetMetrics`'s own route -- same bodyless-GET shape as
/// `HEALTH_PATH`.
pub const METRICS_PATH: &str = "/metrics";

/// Mirrors `health.MetricsResponse` -- `/metrics`'s payload: pure
/// counters/gauges, nothing readiness-shaped (see `RunHealth`'s doc for
/// why staleness lives there instead).
///
/// No live UTxO-set gauge (there used to be one, `utxo_count`): the
/// only way to answer "how many unspent rows right now" is `SELECT
/// COUNT(*) FROM utxo WHERE spent_slot IS NULL`, and that condition
/// matches no index on the table (the existing `spent_slot IS NOT
/// NULL` partial index is the complement, which doesn't help the
/// opposite condition) -- so every `GetMetrics` call was a full scan
/// over every row ever indexed, spent or not, measured live at 1s+ and
/// climbing as spent rows pile up between `prune` runs. `utxos_created_total`/
/// `utxos_spent_total` are the replacement: both are in-memory counters
/// bumped once per applied block (see `floe-server`'s `Health`), so
/// "created minus spent" gives the same sanity-check shape (is the live
/// set roughly growing or shrinking) with no query at all -- just not
/// adjusted for reorgs (a rolled-back block's creates/spends aren't
/// subtracted back out), same caveat `applied_total`/`rolled_back_total`
/// already have.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct MetricsResponse {
    #[cfg_attr(feature = "cbor", n(0))]
    pub applied_total: u64,
    #[cfg_attr(feature = "cbor", n(1))]
    pub rolled_back_total: u64,
    #[cfg_attr(feature = "cbor", n(2))]
    pub utxos_created_total: u64,
    #[cfg_attr(feature = "cbor", n(5))]
    pub utxos_spent_total: u64,
    #[cfg_attr(feature = "cbor", n(4))]
    pub requests: Vec<RequestCount>,
}

impl fmt::Display for MetricsResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "# HELP floe_applied_total Blocks applied since process start\n\
             # TYPE floe_applied_total counter\n\
             floe_applied_total {}\n\
             # HELP floe_rolled_back_total Rollback events handled since process start\n\
             # TYPE floe_rolled_back_total counter\n\
             floe_rolled_back_total {}\n\
             # HELP floe_utxos_created_total UTxOs created since process start\n\
             # TYPE floe_utxos_created_total counter\n\
             floe_utxos_created_total {}\n\
             # HELP floe_utxos_spent_total UTxOs spent since process start\n\
             # TYPE floe_utxos_spent_total counter\n\
             floe_utxos_spent_total {}\n\
             # HELP floe_requests_total Requests received, by scheme and method\n\
             # TYPE floe_requests_total counter\n",
            self.applied_total,
            self.rolled_back_total,
            self.utxos_created_total,
            self.utxos_spent_total,
        )?;
        for r in &self.requests {
            writeln!(
                f,
                "floe_requests_total{{scheme=\"{}\",method=\"{}\"}} {}",
                r.scheme, r.method, r.count
            )?;
        }
        Ok(())
    }
}
