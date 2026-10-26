//! catchup: the chunk buffer and mode switch for catch-up mode.
//!
//! `Engine` accumulates many blocks' worth of UTxO changes in memory,
//! cancelling same-chunk create+spend pairs before they ever touch disk,
//! then flushes everything in one sorted transaction. Continuity (the
//! `prev_hash` chain) is checked in memory block-by-block as blocks are
//! buffered, never against the DB — that's what makes catch-up's per-block
//! cost O(1) with no DB reads.

use std::collections::HashMap;
use std::mem::size_of;

use anyhow::{Result, bail};
use rusqlite::Connection;
use tracing::{info, instrument};

use crate::ingest::decode::{DatumRow, DecodedBlock, OutputRef, ScriptRow, TxHash, Utxo};
use crate::ingest::store::{self, BlockMeta, Mode};

/// Flush trigger: whichever of the two limits is hit first. Defaults are a
/// starting point to tune by measurement, not a result of one — there's no
/// real catch-up dataset to benchmark against in this repo yet.
#[derive(Debug, Clone, Copy)]
pub struct ChunkConfig {
    pub max_bytes: usize,
    pub max_blocks: u64,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024,
            max_blocks: 10_000,
        }
    }
}

fn utxo_bytes(u: &Utxo) -> usize {
    size_of::<Utxo>() + u.assets.as_ref().map_or(0, Vec::len)
}

fn datum_bytes(d: &DatumRow) -> usize {
    size_of::<DatumRow>() + d.cbor.len()
}

fn script_bytes(s: &ScriptRow) -> usize {
    size_of::<ScriptRow>() + s.cbor.len()
}

/// The pending-changes buffer for the current chunk, plus the continuity
/// state that must survive across flushes.
pub struct Engine {
    /// Slot of each pending tx, keyed by hash -- buffered until flush.
    txs: HashMap<TxHash, u64>,
    created: HashMap<OutputRef, (u64, Utxo)>,
    tombstones: Vec<OutputRef>,
    datums: HashMap<[u8; 32], (u64, DatumRow)>,
    scripts: HashMap<[u8; 28], (u64, ScriptRow)>,
    bytes: usize,
    blocks_in_chunk: u64,
    /// Hash of the last block applied (in this chunk or an earlier,
    /// already-flushed one). `None` only before the very first block.
    last_hash: Option<[u8; 32]>,
    /// Last applied block; becomes the checkpoint row on the next flush.
    last_meta: Option<BlockMeta>,
    /// `last_meta`'s own header, re-encoded CBOR -- written as
    /// `resume_header` inside `flush`'s own transaction, atomically with
    /// the checkpoint row. This is what makes a hard kill mid-catchup
    /// (no graceful-shutdown path exists) safe to resume from: the cached
    /// resume point can now never be ahead of what's actually committed,
    /// so a restart at worst replays the still-buffered tail of the
    /// current, never-flushed chunk -- it can no longer land on a
    /// "continuity violation" against its own resume cursor.
    last_header_cbor: Option<Vec<u8>>,
}

impl Engine {
    /// `tip` seeds continuity from the DB's current checkpoint
    /// (`store::tip`), or `None` for a brand new database.
    pub fn new(tip: Option<(u64, [u8; 32])>) -> Self {
        Self {
            txs: HashMap::new(),
            created: HashMap::new(),
            tombstones: Vec::new(),
            datums: HashMap::new(),
            scripts: HashMap::new(),
            bytes: 0,
            blocks_in_chunk: 0,
            last_hash: tip.map(|(_, hash)| hash),
            last_meta: None,
            last_header_cbor: None,
        }
    }

    pub fn pending_blocks(&self) -> u64 {
        self.blocks_in_chunk
    }

    pub fn pending_bytes(&self) -> usize {
        self.bytes
    }

    pub fn should_flush(&self, cfg: &ChunkConfig) -> bool {
        self.blocks_in_chunk >= cfg.max_blocks || self.bytes >= cfg.max_bytes
    }

    /// O(1), no DB reads. Creates fold in before spends (as in
    /// `store::apply`): a chain never spends an output before it exists,
    /// so a same-block spend always finds it and cancels it in memory —
    /// an intra-chunk create+spend never hits disk.
    ///
    /// Lossy for a future lineage feature (a cancelled output never
    /// existed as far as the DB is concerned); revisit before lineage lands.
    pub fn apply_block(
        &mut self,
        meta: BlockMeta,
        decoded: &DecodedBlock,
        header_cbor: Option<&[u8]>,
    ) -> Result<()> {
        if let Some(last_hash) = self.last_hash
            && last_hash != meta.prev_hash
        {
            bail!(
                "continuity violation: last applied hash {} != block {}'s prev_hash {}",
                hex::encode(last_hash),
                meta.slot,
                hex::encode(meta.prev_hash)
            );
        }

        for t in &decoded.txs {
            if !self.txs.contains_key(&t.hash) {
                self.bytes += size_of::<TxHash>();
                self.txs.insert(t.hash, meta.slot);
            }
        }
        for u in &decoded.created {
            self.bytes += utxo_bytes(u);
            self.created.insert(u.output_ref, (meta.slot, u.clone()));
        }
        // `spent_by` (the hash of the spending tx) is only meaningful on a
        // row that stays on disk as a spent-but-retained record; a
        // tombstone here is a hard delete (see `flush`), so it's dropped.
        for r in decoded.spent.keys() {
            if self.created.remove(r).is_none() {
                self.tombstones.push(*r);
            }
        }
        for d in &decoded.datums {
            if !self.datums.contains_key(&d.hash) {
                self.bytes += datum_bytes(d);
                self.datums.insert(d.hash, (meta.slot, d.clone()));
            }
        }
        for s in &decoded.scripts {
            if !self.scripts.contains_key(&s.hash) {
                self.bytes += script_bytes(s);
                self.scripts.insert(s.hash, (meta.slot, s.clone()));
            }
        }

        self.blocks_in_chunk += 1;
        self.last_hash = Some(meta.hash);
        self.last_meta = Some(meta);
        if let Some(h) = header_cbor {
            self.last_header_cbor = Some(h.to_vec());
        }
        Ok(())
    }

    /// Sorted inserts in one transaction. A no-op if nothing's been
    /// applied yet.
    #[instrument(skip(self, conn), fields(pending_blocks = self.blocks_in_chunk, pending_bytes = self.bytes))]
    pub fn flush(&mut self, conn: &mut Connection) -> Result<()> {
        let Some(meta) = self.last_meta else {
            return Ok(());
        };
        let tx = conn.transaction()?;

        let mut tombstones = std::mem::take(&mut self.tombstones);
        tombstones.sort();
        for r in &tombstones {
            store::delete_utxo(&tx, r)?;
        }

        let mut txs: Vec<(TxHash, u64)> = self.txs.drain().collect();
        txs.sort();
        for (hash, slot) in &txs {
            store::insert_tx(&tx, *slot, hash)?;
        }

        let mut created: Vec<(u64, Utxo)> = self.created.drain().map(|(_, v)| v).collect();
        created.sort_by_key(|(_, u)| u.output_ref);
        for (slot, u) in &created {
            store::insert_utxo(&tx, *slot, u)?;
        }

        let mut datums: Vec<(u64, DatumRow)> = self.datums.drain().map(|(_, v)| v).collect();
        datums.sort_by_key(|(_, d)| d.hash);
        for (slot, d) in &datums {
            store::insert_datum(&tx, *slot, d)?;
        }

        let mut scripts: Vec<(u64, ScriptRow)> = self.scripts.drain().map(|(_, v)| v).collect();
        scripts.sort_by_key(|(_, s)| s.hash);
        for (slot, s) in &scripts {
            store::insert_script(&tx, *slot, s)?;
        }

        store::insert_block_row(&tx, &meta)?;
        if let Some(header_cbor) = &self.last_header_cbor {
            store::set_resume_header(&tx, header_cbor)?;
        }
        tx.commit()?;

        store::wal_checkpoint(conn)?;

        info!(
            checkpoint_slot = meta.slot,
            txs = txs.len(),
            created = created.len(),
            tombstones = tombstones.len(),
            datums = datums.len(),
            scripts = scripts.len(),
            "flushed catch-up chunk"
        );
        self.bytes = 0;
        self.blocks_in_chunk = 0;
        Ok(())
    }

    /// Idempotent and crash-safe: every step tolerates repetition, and
    /// `meta.mode` flips last so a reader never sees a half-indexed DB
    /// report itself ready.
    #[instrument(skip(self, conn))]
    pub fn switch_to_running(&mut self, conn: &mut Connection) -> Result<()> {
        info!("switching to running mode: flushing final catch-up chunk");
        self.flush(conn)?;
        store::create_secondary_indexes(conn)?;
        store::sweep(conn)?;
        // `create_secondary_indexes`/`sweep` are the single biggest write
        // burst in the process's life (4 indexes over a multi-million-row
        // `utxo` table) -- `flush`'s own checkpoint, just above, ran before
        // either of them. Nothing in running mode calls `wal_checkpoint`
        // (`store::apply`/`store::prune` don't), so without this the WAL
        // left behind by that burst sits unchecked for the rest of the
        // process's life. Confirmed live: an otherwise-idle instance's WAL
        // was still exactly its post-index-build size 40s into running mode.
        store::wal_checkpoint(conn)?;
        store::set_mode(conn, Mode::Running)?;
        info!("switched to running mode");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::decode::{DatumKind, TxRow};
    use crate::ingest::store::Mode;
    use proptest::prelude::*;
    use rusqlite::OptionalExtension;
    use std::collections::VecDeque;

    fn fresh_ref(counter: &mut u64) -> OutputRef {
        let mut tx_hash = [0u8; 32];
        tx_hash[..8].copy_from_slice(&counter.to_le_bytes());
        *counter += 1;
        OutputRef { tx_hash, ix: 0 }
    }

    fn dummy_utxo(output_ref: OutputRef) -> Utxo {
        Utxo {
            output_ref,
            pay_hash: [0x60; 28],
            stake_hash: None,
            address_header: 0x60,
            coin: 1,
            assets: None,
            datum_kind: DatumKind::None,
            datum_hash: None,
            script_hash: None,
        }
    }

    /// `tx_hash` is the real FK now: every creating tx (one per `creates`
    /// entry) and the shared dummy spending tx (`[0xFF; 32]`, reused
    /// across every call -- `insert_tx`'s `OR IGNORE` makes that fine)
    /// need a `TxRow` alongside the `utxo` effects they produce.
    /// A fresh, globally-unique dummy hash per call -- see `store::tests`'
    /// identical helper for why a single fixed dummy hash is wrong now
    /// that `spent_by` is load-bearing.
    fn fresh_spender() -> [u8; 32] {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut h = [0xFFu8; 32];
        h[..8].copy_from_slice(&n.to_le_bytes());
        h
    }

    fn decoded_block(creates: &[OutputRef], spends: &[OutputRef]) -> DecodedBlock {
        let mut txs: Vec<TxRow> = creates.iter().map(|r| TxRow { hash: r.tx_hash }).collect();
        let spender = fresh_spender();
        if !spends.is_empty() {
            txs.push(TxRow { hash: spender });
        }
        DecodedBlock {
            txs,
            spent: spends.iter().map(|r| (*r, spender)).collect(),
            created: creates.iter().map(|r| dummy_utxo(*r)).collect(),
            datums: Vec::new(),
            scripts: Vec::new(),
        }
    }

    fn block_meta(slot: u64, prev_hash: [u8; 32]) -> (BlockMeta, [u8; 32]) {
        let mut hash = [0u8; 32];
        hash[..8].copy_from_slice(&slot.to_le_bytes());
        (
            BlockMeta {
                slot,
                hash,
                prev_hash,
                block_no: Some(slot),
            },
            hash,
        )
    }

    fn snapshot(conn: &Connection) -> Vec<(OutputRef, u64, Option<u64>)> {
        let mut stmt = conn
            .prepare("SELECT tx_hash, ix, slot, spent_slot FROM utxo ORDER BY tx_hash, ix")
            .unwrap();
        let mut rows = stmt.query([]).unwrap();
        let mut out = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            let tx_hash: Vec<u8> = row.get(0).unwrap();
            let ix: i64 = row.get(1).unwrap();
            let slot: u64 = row.get(2).unwrap();
            let spent_slot: Option<u64> = row.get(3).unwrap();
            out.push((
                OutputRef {
                    tx_hash: tx_hash.try_into().unwrap(),
                    ix: ix as u16,
                },
                slot,
                spent_slot,
            ));
        }
        out
    }

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        store::init(&conn).unwrap();
        conn
    }

    /// `None` if the row doesn't exist at all, `Some(spent_slot)`
    /// otherwise. Raw SQL, not `serve::ctx`'s (private) query layer --
    /// `ingest`'s own tests check what `ingest` itself wrote, not what
    /// a reader would resolve.
    fn spent_slot_of(conn: &Connection, r: OutputRef) -> Option<Option<u64>> {
        conn.query_row(
            "SELECT spent_slot FROM utxo WHERE tx_hash = ?1 AND ix = ?2",
            rusqlite::params![r.tx_hash.as_slice(), r.ix as i64],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
    }

    #[test]
    fn same_chunk_create_and_spend_never_touches_disk() {
        let mut conn = fresh_db();
        let mut engine = Engine::new(store::tip(&conn).unwrap());

        let mut counter = 0;
        let r = fresh_ref(&mut counter);
        let (meta, _) = block_meta(1, [0u8; 32]);
        engine
            .apply_block(meta, &decoded_block(&[r], &[r]), None)
            .unwrap();

        assert_eq!(engine.pending_blocks(), 1);
        engine.flush(&mut conn).unwrap();

        assert!(
            snapshot(&conn).is_empty(),
            "cancelled pair must never hit disk"
        );
    }

    #[test]
    fn cross_chunk_create_then_spend_deletes_on_second_flush() {
        let mut conn = fresh_db();
        let mut engine = Engine::new(store::tip(&conn).unwrap());

        let mut counter = 0;
        let r = fresh_ref(&mut counter);
        let (meta1, hash1) = block_meta(1, [0u8; 32]);
        engine
            .apply_block(meta1, &decoded_block(&[r], &[]), None)
            .unwrap();
        engine.flush(&mut conn).unwrap();

        assert_eq!(snapshot(&conn), vec![(r, 1, None)]);

        let (meta2, _) = block_meta(2, hash1);
        engine
            .apply_block(meta2, &decoded_block(&[], &[r]), None)
            .unwrap();
        engine.flush(&mut conn).unwrap();

        assert!(
            snapshot(&conn).is_empty(),
            "tombstone must delete the row flushed by the prior chunk"
        );
    }

    #[test]
    fn flush_writes_one_checkpoint_row_for_the_whole_chunk() {
        let mut conn = fresh_db();
        let mut engine = Engine::new(store::tip(&conn).unwrap());

        let mut prev_hash = [0u8; 32];
        for slot in 1..=5u64 {
            let (meta, hash) = block_meta(slot, prev_hash);
            prev_hash = hash;
            engine
                .apply_block(meta, &decoded_block(&[], &[]), None)
                .unwrap();
        }
        engine.flush(&mut conn).unwrap();

        let count: u64 = conn
            .query_row("SELECT COUNT(*) FROM block", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "catch-up checkpoints once per chunk, not once per block"
        );
        assert_eq!(store::tip(&conn).unwrap(), Some((5, prev_hash)));
    }

    /// No graceful-shutdown path exists (no SIGTERM/SIGINT handling at all)
    /// -- a hard kill mid-chunk is the *normal* way this process stops
    /// during catch-up, not an edge case. `resume_header` must never point
    /// past the last flushed checkpoint, or a restart hits a continuity
    /// violation resuming from a block that was never actually committed
    /// (reproduced live against mindex-test on 2026-10-26: `systemctl
    /// stop` mid-catchup left a cached header several thousand blocks
    /// ahead of the real tip, and the next `floe run` exited immediately).
    #[test]
    fn resume_header_never_advances_past_the_last_flushed_checkpoint() {
        let mut conn = fresh_db();
        let mut engine = Engine::new(store::tip(&conn).unwrap());

        let (meta1, hash1) = block_meta(1, [0u8; 32]);
        engine
            .apply_block(meta1, &decoded_block(&[], &[]), Some(b"header-1"))
            .unwrap();
        engine.flush(&mut conn).unwrap();
        assert_eq!(
            store::get_resume_header(&conn).unwrap(),
            Some(b"header-1".to_vec())
        );

        // Buffered in memory, never flushed -- exactly what's lost on a
        // hard kill right here.
        let (meta2, _) = block_meta(2, hash1);
        engine
            .apply_block(meta2, &decoded_block(&[], &[]), Some(b"header-2"))
            .unwrap();
        assert_eq!(
            store::get_resume_header(&conn).unwrap(),
            Some(b"header-1".to_vec()),
            "resume_header must still match the durable checkpoint, not the buffered block"
        );

        engine.flush(&mut conn).unwrap();
        assert_eq!(
            store::get_resume_header(&conn).unwrap(),
            Some(b"header-2".to_vec()),
            "a real flush must advance resume_header to match"
        );
    }

    #[test]
    fn in_memory_continuity_violation_is_rejected_without_touching_disk() {
        let mut conn = fresh_db();
        let mut engine = Engine::new(store::tip(&conn).unwrap());

        let (meta1, _hash1) = block_meta(1, [0u8; 32]);
        engine
            .apply_block(meta1, &decoded_block(&[], &[]), None)
            .unwrap();

        let (bad_meta, _) = block_meta(2, [0xFFu8; 32]); // wrong prev_hash
        assert!(
            engine
                .apply_block(bad_meta, &decoded_block(&[], &[]), None)
                .is_err()
        );
        // The rejected block must not have been folded into the buffer.
        assert_eq!(engine.pending_blocks(), 1);

        engine.flush(&mut conn).unwrap();
        let count: u64 = conn
            .query_row("SELECT COUNT(*) FROM block", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn switch_to_running_is_idempotent_across_a_simulated_crash() {
        let mut conn = fresh_db();
        let mut engine = Engine::new(store::tip(&conn).unwrap());

        let mut counter = 0;
        let r = fresh_ref(&mut counter);
        let (meta, _) = block_meta(1, [0u8; 32]);
        engine
            .apply_block(meta, &decoded_block(&[r], &[]), None)
            .unwrap();

        // Simulate a crash partway through the switch: flush and create the
        // indexes, but stop before the sweep and the mode flip land.
        engine.flush(&mut conn).unwrap();
        store::create_secondary_indexes(&conn).unwrap();
        assert_eq!(store::get_mode(&conn).unwrap(), Mode::Catchup);

        // "Restart": call the full switch again. It must finish correctly
        // rather than erroring on the already-created indexes or the
        // already-flushed (now empty) chunk.
        engine.switch_to_running(&mut conn).unwrap();

        assert_eq!(store::get_mode(&conn).unwrap(), Mode::Running);
        let index_count: u64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='utxo_pay'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(index_count, 1);
        assert_eq!(snapshot(&conn), vec![(r, 1, None)]);

        // Calling it again (e.g. a second spurious restart) must still be a no-op.
        engine.switch_to_running(&mut conn).unwrap();
        assert_eq!(store::get_mode(&conn).unwrap(), Mode::Running);
    }

    #[test]
    fn running_apply_continues_correctly_after_a_catchup_switch() {
        let mut conn = fresh_db();
        let mut engine = Engine::new(store::tip(&conn).unwrap());

        let (meta1, hash1) = block_meta(1, [0u8; 32]);
        engine
            .apply_block(meta1, &decoded_block(&[], &[]), None)
            .unwrap();
        engine.switch_to_running(&mut conn).unwrap();

        // The running-mode path (a separate writer entirely) must see the
        // checkpoint the engine left behind and accept the next block.
        let (meta2, _) = block_meta(2, hash1);
        store::apply(&mut conn, &meta2, &decoded_block(&[], &[]), None).unwrap();
        assert_eq!(store::tip(&conn).unwrap().unwrap().0, 2);
    }

    /// A catch-up-origin UTxO is untouched by `switch_to_running` (catch-up
    /// never writes `spent_slot` — a same-chunk spend cancels in memory, a
    /// cross-chunk one hard-deletes), but once spent in running mode and
    /// aged past `k` blocks, `prune` removes it as normal.
    #[test]
    fn catchup_created_utxo_is_prunable_once_spent_and_aged_in_running_mode() {
        let mut conn = fresh_db();
        let mut engine = Engine::new(store::tip(&conn).unwrap());

        let r = OutputRef {
            tx_hash: [0xAA; 32],
            ix: 0,
        };
        let (meta1, hash1) = block_meta(1, [0u8; 32]);
        engine
            .apply_block(meta1, &decoded_block(&[r], &[]), None)
            .unwrap();
        engine.switch_to_running(&mut conn).unwrap();

        assert_eq!(
            spent_slot_of(&conn, r),
            Some(None),
            "catch-up must never leave r spent"
        );

        // Running mode: spend r at slot 2, then run far enough past it
        // that the spend is unambiguously more than k=1 blocks deep.
        let mut prev_hash = hash1;
        let (meta2, hash2) = block_meta(2, prev_hash);
        store::apply(&mut conn, &meta2, &decoded_block(&[], &[r]), None).unwrap();
        prev_hash = hash2;
        for slot in 3..=6u64 {
            let (meta, hash) = block_meta(slot, prev_hash);
            store::apply(&mut conn, &meta, &decoded_block(&[], &[]), None).unwrap();
            prev_hash = hash;
        }

        let stats = store::prune(&mut conn, 1).unwrap();
        assert!(
            stats.utxo_rows >= 1,
            "the catch-up-origin, now-spent row must be prunable: {stats:?}"
        );
        assert_eq!(spent_slot_of(&conn, r), None, "r must be gone after prune");
    }

    // Model: ref -> (created_slot, spent_slot). Same shape as store.rs's
    // model, re-derived here because it's the plain spec, not shared code.
    #[derive(Debug, Default, Clone)]
    struct Model(std::collections::BTreeMap<OutputRef, (u64, Option<u64>)>);

    impl Model {
        fn apply(&mut self, slot: u64, creates: &[OutputRef], spends: &[OutputRef]) {
            for r in creates {
                self.0.insert(*r, (slot, None));
            }
            for r in spends {
                if let Some(entry) = self.0.get_mut(r)
                    && entry.1.is_none()
                {
                    entry.1 = Some(slot);
                }
            }
        }

        fn snapshot(&self) -> Vec<(OutputRef, u64, Option<u64>)> {
            self.0
                .iter()
                .map(|(r, (slot, spent))| (*r, *slot, *spent))
                .collect()
        }
    }

    proptest! {
        /// Flushing at arbitrary points must land on the same TIP SET
        /// (unspent refs) as the plain model. Full row history isn't
        /// compared: an intra-chunk create-then-spend never reaches disk
        /// in catch-up, but running mode keeps it with `spent_slot` set —
        /// the two only need to agree on what's live at the end.
        #[test]
        fn chunked_apply_matches_model_tip_set_regardless_of_flush_points(
            ops in prop::collection::vec((0usize..=3, 0usize..=2, bool::arbitrary()), 1..40),
        ) {
            let mut conn = fresh_db();
            let mut engine = Engine::new(store::tip(&conn).unwrap());

            let mut model = Model::default();
            let mut prev_hash = [0u8; 32];
            let mut counter = 0u64;
            let mut unspent: VecDeque<OutputRef> = VecDeque::new();

            for (i, (n_creates, n_spends_req, flush_now)) in ops.iter().enumerate() {
                let slot = (i + 1) as u64;
                let creates: Vec<OutputRef> = (0..*n_creates).map(|_| fresh_ref(&mut counter)).collect();
                unspent.extend(creates.iter().copied());

                let n_spend = (*n_spends_req).min(unspent.len());
                let spends: Vec<OutputRef> = (0..n_spend).map(|_| unspent.pop_front().unwrap()).collect();

                let (meta, hash) = block_meta(slot, prev_hash);
                prev_hash = hash;

                engine.apply_block(meta, &decoded_block(&creates, &spends), None).unwrap();
                model.apply(slot, &creates, &spends);

                if *flush_now {
                    engine.flush(&mut conn).unwrap();
                }
            }
            engine.flush(&mut conn).unwrap();

            let mut got: Vec<OutputRef> = snapshot(&conn)
                .into_iter()
                .filter(|(_, _, spent_slot)| spent_slot.is_none())
                .map(|(r, _, _)| r)
                .collect();
            let mut want: Vec<OutputRef> = model
                .snapshot()
                .into_iter()
                .filter(|(_, _, spent_slot)| spent_slot.is_none())
                .map(|(r, _, _)| r)
                .collect();
            got.sort();
            want.sort();
            prop_assert_eq!(got, want);
        }
    }
}
