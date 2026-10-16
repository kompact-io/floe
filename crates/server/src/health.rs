//! health: core, scheme-independent readiness state. `run` mode's
//! `Health` tracks the writer process's own seed/catchup/running
//! lifecycle plus chain-freshness in-memory; `serve` (read-replica)
//! mode has no lifecycle of its own and is snapshotted fresh from the
//! DB on every call instead (see `replica_snapshot`). Zero knowledge of
//! HTTP, gRPC, or wire format -- `scheme::grpc::health`/
//! `scheme::http::metrics` only snapshot and render this.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use floe_dto_http::{HealthDto, ReplicaHealthDto};
pub use floe_dto_http::{Readiness, Stage};

use crate::decode::TxHash;
use crate::store::Mode;

/// `Stage` has no `Mode` dependency (it's defined in `floe-dto-http`,
/// which can't depend on this crate's domain types), so the conversion
/// lives here instead, where both types are in scope.
impl From<Mode> for Stage {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Catchup => Stage::Catchup,
            Mode::Running => Stage::Running,
        }
    }
}

#[derive(Debug, Default)]
pub struct Health {
    magic: AtomicU64,
    stage: AtomicU8,
    tip_slot: AtomicU64,
    block_no: AtomicU64,
    peer_tip_block_no: AtomicU64,
    applied: AtomicU64,
    rolled_back: AtomicU64,
    utxos_created: AtomicU64,
    /// Not an atomic: a 32-byte hash, written once per applied block --
    /// nowhere near hot enough to need lock-free.
    tip_hash: Mutex<TxHash>,
}

impl Health {
    pub fn set_stage(&self, stage: Stage) {
        self.stage.store(stage as u8, Ordering::Relaxed);
    }

    pub fn set_magic(&self, magic: u64) {
        self.magic.store(magic, Ordering::Relaxed);
    }

    pub fn record_apply(
        &self,
        stage: Stage,
        slot: u64,
        block_no: u64,
        hash: TxHash,
        peer_tip_block_no: u64,
        utxos_created: u64,
    ) {
        self.set_stage(stage);
        self.tip_slot.store(slot, Ordering::Relaxed);
        self.block_no.store(block_no, Ordering::Relaxed);
        self.peer_tip_block_no
            .store(peer_tip_block_no, Ordering::Relaxed);
        self.applied.fetch_add(1, Ordering::Relaxed);
        self.utxos_created
            .fetch_add(utxos_created, Ordering::Relaxed);
        *self.tip_hash.lock().unwrap() = hash;
    }

    pub fn record_rollback(&self) {
        self.rolled_back.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stage(&self) -> Stage {
        match self.stage.load(Ordering::Relaxed) {
            0 => Stage::Seeding,
            1 => Stage::Catchup,
            _ => Stage::Running,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.stage().is_ready()
    }

    pub fn applied_total(&self) -> u64 {
        self.applied.load(Ordering::Relaxed)
    }

    pub fn rolled_back_total(&self) -> u64 {
        self.rolled_back.load(Ordering::Relaxed)
    }

    pub fn utxos_created_total(&self) -> u64 {
        self.utxos_created.load(Ordering::Relaxed)
    }

    /// `run` mode's `/health`/`GetHealth` payload.
    pub fn snapshot(&self) -> HealthDto {
        HealthDto {
            stage: self.stage(),
            tip_slot: self.tip_slot.load(Ordering::Relaxed),
            block_no: self.block_no.load(Ordering::Relaxed),
            tip_hash: *self.tip_hash.lock().unwrap(),
            peer_tip_block_no: self.peer_tip_block_no.load(Ordering::Relaxed),
        }
    }
}

/// `serve` (read-replica) mode's `/health`/`GetHealth` payload: no
/// in-memory lifecycle of its own (see module doc) -- `readiness` is
/// whatever the caller already determined by attempting a DB read
/// (`Readiness::Unavailable` if that read failed), not computed here.
pub fn replica_snapshot(
    readiness: Readiness,
    tip_slot: Option<u64>,
    utxo_count: u64,
    upstream_mode: Mode,
) -> ReplicaHealthDto {
    ReplicaHealthDto {
        readiness,
        tip_slot,
        utxo_count,
        upstream_mode: upstream_mode as u8,
    }
}
