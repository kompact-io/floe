//! `HealthDto`/`MetricsDto`: typed, wire-format-agnostic snapshots of
//! `floe-server`'s health and metrics state -- replacing the old
//! `health::Health`'s hand-written `prometheus_text()` with a real
//! DTO. The live atomic-counter state itself lives in `floe-server`'s
//! own `health`/`metrics` core modules now (this crate can't depend on
//! `floe-server`'s domain types); these are just the snapshots taken
//! from it.
//!
//! `Display` renders Prometheus text directly -- `.to_string()`/`{dto}`
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
use serde::Serialize;

/// Where the writer process (`run` mode) is in its lifecycle --
/// independent of `floe-server::store::Mode` (catchup/running only,
/// persisted, a one-way switch). Seeding happens before `Mode` means
/// anything and isn't persisted; a fresh process starts here, so it
/// reports that distinctly instead of looking indistinguishable from
/// "down" (unreachable).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "json", derive(Serialize))]
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

/// `serve` (read-replica) mode's readiness: no seed/catchup/running
/// lifecycle at all (no sync loop) -- the only question is "can I read
/// the DB right now."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "json", derive(Serialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
#[cfg_attr(feature = "json", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "cbor", cbor(index_only))]
pub enum Readiness {
    #[cfg_attr(feature = "cbor", n(0))]
    Ready,
    #[cfg_attr(feature = "cbor", n(1))]
    Unavailable,
}

impl Readiness {
    pub fn is_ready(self) -> bool {
        self == Readiness::Ready
    }
}

/// `run` mode's `/health` body: `stage` plus chain-freshness --
/// staleness (how far behind the peer this node is) is a readiness
/// concern, so it lives here, not in `MetricsDto`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode))]
pub struct HealthDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub stage: Stage,
    #[cfg_attr(feature = "cbor", n(1))]
    pub tip_slot: u64,
    #[cfg_attr(feature = "cbor", n(2))]
    pub block_no: u64,
    #[cfg_attr(feature = "json", serde(serialize_with = "crate::ser_hex"))]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(3), cbor(with = "minicbor::bytes"))]
    pub tip_hash: [u8; 32],
    #[cfg_attr(feature = "cbor", n(4))]
    pub peer_tip_block_no: u64,
}

impl fmt::Display for HealthDto {
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

/// `serve` (read-replica) mode's `/health` body -- `upstream_mode` is
/// the upstream writer's `store::Mode` as of the last replicated write
/// (0 = catchup, 1 = running).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode))]
pub struct ReplicaHealthDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub readiness: Readiness,
    #[cfg_attr(feature = "cbor", n(1))]
    pub tip_slot: Option<u64>,
    #[cfg_attr(feature = "cbor", n(2))]
    pub utxo_count: u64,
    #[cfg_attr(feature = "cbor", n(3))]
    pub upstream_mode: u8,
}

impl fmt::Display for ReplicaHealthDto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "# HELP floe_tip_slot Slot of the last block in the replicated DB\n\
             # TYPE floe_tip_slot gauge\n\
             floe_tip_slot {}\n\
             # HELP floe_utxo_count Unspent UTxOs in the replicated DB\n\
             # TYPE floe_utxo_count gauge\n\
             floe_utxo_count {}\n\
             # HELP floe_upstream_mode The upstream writer's mode as of the last replicated write: 0 = catchup, 1 = running\n\
             # TYPE floe_upstream_mode gauge\n\
             floe_upstream_mode {}\n",
            self.tip_slot.unwrap_or(0),
            self.utxo_count,
            self.upstream_mode,
        )
    }
}

/// `GetHealth`'s/`/health`'s payload -- `run` and `serve` modes report
/// different shapes (see module doc on `ReplicaHealthDto`), not the
/// same `HealthDto` with unreachable fields nulled out.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "json", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "cbor", derive(Encode))]
#[cfg_attr(feature = "cbor", cbor(flat))]
pub enum HealthResponseDto {
    #[cfg_attr(feature = "cbor", n(0))]
    Run(#[cfg_attr(feature = "cbor", n(0))] HealthDto),
    #[cfg_attr(feature = "cbor", n(1))]
    Replica(#[cfg_attr(feature = "cbor", n(0))] ReplicaHealthDto),
}

impl fmt::Display for HealthResponseDto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HealthResponseDto::Run(h) => h.fmt(f),
            HealthResponseDto::Replica(h) => h.fmt(f),
        }
    }
}

/// The transport a request arrived over -- `MetricsDto::requests` is
/// scheme-labeled, not gRPC-only, even though gRPC's per-RPC-method
/// counts are the only producer today (nothing forecloses a future
/// per-route HTTP counter landing in the same field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "json", derive(Serialize))]
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

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode))]
pub struct RequestCount {
    #[cfg_attr(feature = "cbor", n(0))]
    pub scheme: Scheme,
    #[cfg_attr(feature = "cbor", n(1))]
    pub method: String,
    #[cfg_attr(feature = "cbor", n(2))]
    pub count: u64,
}

/// `/metrics`'s payload, both modes: pure counters/gauges, nothing
/// readiness-shaped (see `HealthDto`'s doc for why staleness lives
/// there instead). `applied_total`/`rolled_back_total`/
/// `utxos_created_total` are always 0 in `serve` mode -- no sync loop
/// applies blocks there.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "json", derive(Serialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode))]
pub struct MetricsDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub applied_total: u64,
    #[cfg_attr(feature = "cbor", n(1))]
    pub rolled_back_total: u64,
    #[cfg_attr(feature = "cbor", n(2))]
    pub utxos_created_total: u64,
    #[cfg_attr(feature = "cbor", n(3))]
    pub utxo_count: u64,
    #[cfg_attr(feature = "cbor", n(4))]
    pub requests: Vec<RequestCount>,
}

impl fmt::Display for MetricsDto {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "# HELP floe_utxo_count Unspent UTxOs in the index\n\
             # TYPE floe_utxo_count gauge\n\
             floe_utxo_count {}\n\
             # HELP floe_applied_total Blocks applied since process start\n\
             # TYPE floe_applied_total counter\n\
             floe_applied_total {}\n\
             # HELP floe_rolled_back_total Rollback events handled since process start\n\
             # TYPE floe_rolled_back_total counter\n\
             floe_rolled_back_total {}\n\
             # HELP floe_utxos_created_total UTxOs created since process start (throughput; the\n\
             # live UTxO set also shrinks as these get spent, see floe_utxo_count)\n\
             # TYPE floe_utxos_created_total counter\n\
             floe_utxos_created_total {}\n\
             # HELP floe_requests_total Requests received, by scheme and method\n\
             # TYPE floe_requests_total counter\n",
            self.utxo_count, self.applied_total, self.rolled_back_total, self.utxos_created_total,
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
