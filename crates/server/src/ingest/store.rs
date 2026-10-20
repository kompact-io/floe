//! store: SQLite writer for floe's UTxO index — the only module with SQL
//! text in it (including `query`, below).
//!
//! Functions take an explicit `&Connection`/`&Transaction` rather than
//! `&self` on a connection-owning type, since `catchup` and `seed` need to
//! batch several inserts into one transaction by passing `&tx` through
//! plain calls.

use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, params};
use tracing::{info, instrument};

use crate::ingest::decode::{DatumRow, DecodedBlock, OutputRef, ScriptRow, TxHash, Utxo};

const BASE_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v);

CREATE TABLE IF NOT EXISTS block(
  slot INTEGER PRIMARY KEY, hash BLOB NOT NULL, prev_hash BLOB NOT NULL, block_no INTEGER
);
-- Backs `query::check_point`'s by-hash lookup (`SearchUtxos`'s
-- `since`). Created upfront, not deferred to the catch-up->running
-- switch like `SECONDARY_INDEXES_SQL`'s `utxo` indexes -- `block`'s
-- insert rate (one row per block in running mode, one per catch-up
-- chunk checkpoint otherwise) is nowhere near `utxo`'s, so there's no
-- bulk-insert performance case to defer this for.
CREATE INDEX IF NOT EXISTS block_hash ON block(hash);

-- Minimal by design: just a tx's id, its parent block, and
-- `required_signers` -- no validity flag, no fee, no other body field.
-- `ReadTx` (UTxORPC) hydrates inputs/outputs from `utxo` alone (see
-- `query::by_tx_hash`); add further columns here only when a concrete
-- need shows up, not preemptively. `required_signers` is concatenated
-- fixed-28-byte hashes (NULL if none), unindexed -- not a queryable
-- field, just carried through for `ReadTx` to return.
--
-- `block_slot` is deliberately NOT a FK to `block(slot)`: catch-up mode
-- checkpoints one `block` row per whole chunk, not one per block (see
-- `catchup::Engine::flush`), so most `tx` rows' own slot has no matching
-- `block` row while buffered -- a real FK here would reject every
-- catch-up flush. Rollback instead deletes `tx` directly by slot range
-- (see `rollback`, below), independent of `block`'s own deletion.
CREATE TABLE IF NOT EXISTS tx(
  hash BLOB PRIMARY KEY,
  block_slot INTEGER NOT NULL,
  required_signers BLOB
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS datum(
  hash BLOB PRIMARY KEY, cbor BLOB NOT NULL, slot INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS script(
  hash BLOB PRIMARY KEY, lang INTEGER NOT NULL, cbor BLOB NOT NULL, slot INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS utxo(
  tx_hash BLOB NOT NULL REFERENCES tx(hash) ON DELETE CASCADE
    DEFERRABLE INITIALLY DEFERRED,
  ix INTEGER NOT NULL,
  slot INTEGER NOT NULL,
  spent_slot INTEGER,
  -- No FK here (unlike tx_hash above): a spending tx isn't this row's
  -- parent, so it must not cascade-delete the row on rollback -- it
  -- should un-spend it instead. See `utxo_unspend_on_tx_delete` below.
  spent_by BLOB,
  -- No raw address bytes: an indexed output's address is always fully
  -- reconstructable from (pay_hash, stake_hash, address_header) alone
  -- (floe doesn't support pointer addresses, the one case that would
  -- be lossy -- see `decode::Credentials`). `address_header` is the
  -- one place key-vs-script lives; queries never filter on it, it
  -- only matters for rebuilding the address at serve time (`ctx.rs`'s
  -- `reconstruct_address`) -- key and script hashes are
  -- cryptographically impossible to collide, so there's nothing to
  -- disambiguate *for a lookup*.
  pay_hash BLOB NOT NULL, stake_hash BLOB, address_header INTEGER NOT NULL,
  coin INTEGER NOT NULL,
  assets BLOB,
  datum_kind INTEGER NOT NULL,
  datum_hash BLOB,
  script_hash BLOB,
  PRIMARY KEY(tx_hash, ix)
) WITHOUT ROWID;

-- Deleting a tx un-spends any utxo row it had spent, rather than
-- leaving a dangling `spent_by`. `tx` rows are only ever deleted
-- directly (by `rollback`/`prune`, never cascaded into), so this
-- always fires -- no `recursive_triggers` needed.
CREATE TRIGGER IF NOT EXISTS utxo_unspend_on_tx_delete BEFORE DELETE ON tx BEGIN
  UPDATE utxo SET spent_slot = NULL, spent_by = NULL WHERE spent_by = OLD.hash;
END;
"#;

/// Deferred until the catch-up -> running switch: sorted inserts during
/// catch-up beat maintaining these on every insert.
///
/// `utxo_pay`/`utxo_stk`/`utxo_script` are NOT filtered to unspent-only
/// (no `WHERE spent_by IS NULL`): whether a lookup wants only the live
/// tip set or everything including spent/historical rows is the
/// *query's* call, made via its own `WHERE`, not something baked into
/// the index. `utxo_pay`/`utxo_stk` order by `COALESCE(spent_slot,
/// slot)` -- each row's last-relevant-event slot -- matching
/// `query::by_pay_hash`/`by_credentials`/`by_stake_hash`'s own
/// pagination order (see their doc comments): a cursor over this never
/// skips a row that only just became relevant (created, or newly
/// spent). `utxo_script` keeps `script_hash IS NOT NULL` -- that's a
/// sparse-column optimization (most rows have no script), unrelated to
/// spent status. `utxo_spent` stays filtered on `spent_slot IS NOT
/// NULL`: that one directly backs `prune`'s own `spent_slot`-keyed
/// cutoff scan, not a "hide spent rows elsewhere" concern.
///
/// No `utxo_dhash` (no query needs "find UTxOs by datum hash" --
/// `utxo.datum_hash` is still used, just only to join a row to *its
/// own* datum in `datum`, which needs no index on this table at all,
/// that table's own PRIMARY KEY does the work).
///
/// `utxo_spent_by` backs no query either -- it exists purely so
/// `utxo_unspend_on_tx_delete` (fired by every `tx` row a rollback or
/// `sweep_orphaned_txs`/`prune` deletes) is an indexed point lookup
/// instead of a full `utxo` scan. Measured live without it: deleting
/// ~450k orphaned `tx` rows against a ~4.5M-row `utxo` table did ~450k
/// full scans, i.e. ~2*10^12 row touches -- not slow, effectively never
/// finishes. This must exist before the first `sweep`/rollback/prune
/// call, same as every other index here.
///
const SECONDARY_INDEXES_SQL: &str = r#"
CREATE INDEX IF NOT EXISTS utxo_created ON utxo(slot);
CREATE INDEX IF NOT EXISTS utxo_spent   ON utxo(spent_slot)  WHERE spent_slot IS NOT NULL;
CREATE INDEX IF NOT EXISTS utxo_pay     ON utxo(pay_hash, COALESCE(spent_slot, slot));
CREATE INDEX IF NOT EXISTS utxo_stk     ON utxo(stake_hash, COALESCE(spent_slot, slot));
CREATE INDEX IF NOT EXISTS utxo_script  ON utxo(script_hash) WHERE script_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS utxo_spent_by ON utxo(spent_by) WHERE spent_by IS NOT NULL;
"#;

/// `page_size` must be set before any table exists, so this runs first.
/// Secondary indexes are deferred to `create_secondary_indexes`: catch-up
/// wants sorted inserts without them.
pub fn init(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "page_size", 16384)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "temp_store", "MEMORY")?;
    // SQLite's own default (-2000, ~2MiB) is nowhere near enough once
    // `create_secondary_indexes` has to build four indexes over a
    // multi-million-row `utxo` table at the catch-up -> running switch:
    // observed live, that working set thrashed the page cache hard
    // enough to spend ~90% of a core's time in read() syscalls re-faulting
    // pages that had just been evicted, not in any real computation.
    // Negative = KiB, not pages (positive would be page count); 128MiB is
    // per-connection, released when the connection closes, not persisted
    // in the DB file.
    conn.pragma_update(None, "cache_size", -131_072)?;
    // Per-connection, never persisted in the DB file -- every connection
    // that writes needs this (enforces `utxo.tx_hash -> tx.hash`).
    conn.pragma_update(None, "foreign_keys", true)?;
    conn.execute_batch(BASE_SCHEMA_SQL)?;
    // One-time cleanup for a DB from before these were dropped: `CREATE
    // INDEX IF NOT EXISTS` only ever adds, so an existing index not named
    // in `SECONDARY_INDEXES_SQL` would otherwise sit there forever as dead
    // weight -- worse, SQLite keeps maintaining it on every write
    // regardless of whether anything still queries it. No-op on a DB that
    // never had them. Doesn't reclaim the freed pages' disk space itself;
    // `VACUUM` does that, but it's slow on a multi-GB DB and isn't worth
    // doing unprompted on every startup.
    conn.execute_batch("DROP INDEX IF EXISTS utxo_addr; DROP INDEX IF EXISTS utxo_dhash;")?;
    conn.execute(
        "INSERT OR IGNORE INTO meta(k, v) VALUES ('mode', ?1)",
        params![Mode::Catchup.as_str()],
    )?;
    Ok(())
}

/// Catch-up vs running, persisted in `meta.mode`. One-way switch: only
/// `switch_to_running` ever moves `Catchup` -> `Running`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Catchup,
    Running,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Catchup => "catchup",
            Mode::Running => "running",
        }
    }
}

pub fn get_mode(conn: &Connection) -> Result<Mode> {
    let v: String = conn.query_row("SELECT v FROM meta WHERE k = 'mode'", [], |r| r.get(0))?;
    match v.as_str() {
        "catchup" => Ok(Mode::Catchup),
        "running" => Ok(Mode::Running),
        other => bail!("unknown meta.mode value {other:?}"),
    }
}

pub(crate) fn set_mode(conn: &Connection, mode: Mode) -> Result<()> {
    conn.execute(
        "UPDATE meta SET v = ?1 WHERE k = 'mode'",
        params![mode.as_str()],
    )?;
    Ok(())
}

/// Safe to call repeatedly (e.g. after a crash mid-switch).
#[instrument(skip(conn))]
pub fn create_secondary_indexes(conn: &Connection) -> Result<()> {
    info!("building secondary indexes");
    conn.execute_batch(SECONDARY_INDEXES_SQL)?;
    conn.execute_batch("ANALYZE;")?;
    info!("secondary indexes built");
    Ok(())
}

/// Removes datum and script rows with no referencing UTxO. A
/// spent-but-retained row still counts as a reference.
#[instrument(skip(conn))]
pub fn sweep(conn: &Connection) -> Result<()> {
    let datum_rows = conn.execute(
        "DELETE FROM datum WHERE hash NOT IN (SELECT datum_hash FROM utxo WHERE datum_hash IS NOT NULL)",
        [],
    )?;
    let script_rows = conn.execute(
        "DELETE FROM script WHERE hash NOT IN (SELECT script_hash FROM utxo WHERE script_hash IS NOT NULL)",
        [],
    )?;
    info!(datum_rows, script_rows, "swept orphaned datum/script rows");
    // Catch-up's in-memory create+spend cancellation (`Engine::apply_block`)
    // still records the tx itself (`decoded.txs` is unconditional), so a
    // tx whose only `utxo` effect was cancelled before ever touching disk
    // leaves an orphaned `tx` row with nothing in `utxo` pointing at it.
    sweep_orphaned_txs(conn)
}

/// A `tx` row is safe to delete only when `utxo` points at it from
/// *neither* side: not as a creator (`tx_hash`) and not as a spender
/// (`spent_by`). Checking only the creator side is actively wrong, not
/// just incomplete -- deleting a spend-only tx's row (one that
/// consumed inputs but created nothing still on disk) fires
/// `utxo_unspend_on_tx_delete` and silently un-spends whatever it
/// spent.
const TX_ORPHAN_CHECK_SQL: &str = "hash NOT IN (SELECT tx_hash FROM utxo)
     AND hash NOT IN (SELECT spent_by FROM utxo WHERE spent_by IS NOT NULL)";

fn sweep_orphaned_txs(conn: &Connection) -> Result<()> {
    let tx_rows = conn.execute(&format!("DELETE FROM tx WHERE {TX_ORPHAN_CHECK_SQL}"), [])?;
    info!(tx_rows, "swept orphaned tx rows");
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockMeta {
    pub slot: u64,
    pub hash: [u8; 32],
    pub prev_hash: [u8; 32],
    /// `None` only for the seed anchor's checkpoint (the archive name has
    /// no block number). `prune` treats unknown as "too old to use" —
    /// safe, since it only prunes less, never more.
    pub block_no: Option<u64>,
}

pub fn tip(conn: &Connection) -> Result<Option<(u64, [u8; 32])>> {
    let row: Option<(u64, Vec<u8>)> = conn
        .query_row(
            "SELECT slot, hash FROM block ORDER BY slot DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    Ok(row.map(|(slot, hash)| (slot, hash.try_into().expect("hash column must be 32 bytes"))))
}

/// Continuity is the caller's job. `INSERT OR IGNORE` so re-writing the
/// same checkpoint (e.g. a retried `flush`) is a no-op, not a `PRIMARY
/// KEY` error.
pub(crate) fn insert_block_row(conn: &Connection, meta: &BlockMeta) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO block(slot, hash, prev_hash, block_no) VALUES (?1, ?2, ?3, ?4)",
        params![
            meta.slot,
            meta.hash.as_slice(),
            meta.prev_hash.as_slice(),
            meta.block_no
        ],
    )?;
    Ok(())
}

/// `OR IGNORE`, like `insert_datum`/`insert_script`: a real tx hash is
/// globally unique so this never actually ignores anything in
/// production, but it keeps the function idempotent (retried
/// flush/apply) without needing a continuity check the way the single
/// `block` row per slot does.
pub(crate) fn insert_tx(
    conn: &Connection,
    slot: u64,
    hash: &TxHash,
    required_signers: &[[u8; 28]],
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO tx(hash, block_slot, required_signers) VALUES (?1, ?2, ?3)",
        params![
            hash.as_slice(),
            slot,
            (!required_signers.is_empty()).then(|| required_signers.concat())
        ],
    )?;
    Ok(())
}

pub(crate) fn insert_utxo(conn: &Connection, slot: u64, u: &Utxo) -> Result<()> {
    conn.execute(
        "INSERT INTO utxo(
            tx_hash, ix, slot, spent_slot, pay_hash, stake_hash, address_header,
            coin, assets, datum_kind, datum_hash, script_hash
         ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            u.output_ref.tx_hash.as_slice(),
            u.output_ref.ix as i64,
            slot,
            u.pay_hash.as_slice(),
            u.stake_hash.map(|h| h.to_vec()),
            u.address_header,
            u.coin,
            u.assets,
            u.datum_kind as i64,
            u.datum_hash.map(|h| h.to_vec()),
            u.script_hash.map(|h| h.to_vec()),
        ],
    )?;
    Ok(())
}

/// Unknown ref or an already-spent row is a no-op: 0 rows match.
pub(crate) fn mark_spent(
    conn: &Connection,
    slot: u64,
    r: &OutputRef,
    spent_by: &TxHash,
) -> Result<()> {
    conn.execute(
        "UPDATE utxo SET spent_slot = ?1, spent_by = ?2 WHERE tx_hash = ?3 AND ix = ?4 AND spent_slot IS NULL",
        params![slot, spent_by.as_slice(), r.tx_hash.as_slice(), r.ix as i64],
    )?;
    Ok(())
}

/// Catch-up's tombstone delete: the row was buffered, never written, so
/// this is a hard delete rather than `mark_spent`'s `UPDATE`.
pub(crate) fn delete_utxo(conn: &Connection, r: &OutputRef) -> Result<()> {
    conn.execute(
        "DELETE FROM utxo WHERE tx_hash = ?1 AND ix = ?2",
        params![r.tx_hash.as_slice(), r.ix as i64],
    )?;
    Ok(())
}

pub(crate) fn insert_datum(conn: &Connection, slot: u64, d: &DatumRow) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO datum(hash, cbor, slot) VALUES (?1, ?2, ?3)",
        params![d.hash.as_slice(), d.cbor, slot],
    )?;
    Ok(())
}

pub(crate) fn insert_script(conn: &Connection, slot: u64, s: &ScriptRow) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO script(hash, lang, cbor, slot) VALUES (?1, ?2, ?3, ?4)",
        params![s.hash.as_slice(), s.lang as i64, s.cbor, slot],
    )?;
    Ok(())
}

/// Creates are applied before spends, not tx-by-tx: a chain never spends
/// an output before it exists, so an intra-block chain is always an
/// earlier tx's output spent by a later one — applying creates first
/// guarantees that's already on disk. The row still lands with both
/// `slot` and `spent_slot` set, just excluded from the tip view.
pub fn apply(conn: &mut Connection, meta: &BlockMeta, decoded: &DecodedBlock) -> Result<()> {
    let tx = conn.transaction()?;

    if let Some((_, tip_hash)) = tip(&tx)?
        && tip_hash != meta.prev_hash
    {
        bail!(
            "continuity violation: tip hash {} != block {}'s prev_hash {}",
            hex::encode(tip_hash),
            meta.slot,
            hex::encode(meta.prev_hash)
        );
    }

    // Plain INSERT, not insert_block_row's OR IGNORE: the continuity check
    // above already turns a duplicate into an error, not a silent no-op.
    tx.execute(
        "INSERT INTO block(slot, hash, prev_hash, block_no) VALUES (?1, ?2, ?3, ?4)",
        params![
            meta.slot,
            meta.hash.as_slice(),
            meta.prev_hash.as_slice(),
            meta.block_no
        ],
    )?;

    for t in &decoded.txs {
        insert_tx(&tx, meta.slot, &t.hash, &t.required_signers)?;
    }

    for u in &decoded.created {
        insert_utxo(&tx, meta.slot, u)?;
    }

    for (r, spent_by) in &decoded.spent {
        mark_spent(&tx, meta.slot, r, spent_by)?;
    }

    for d in &decoded.datums {
        insert_datum(&tx, meta.slot, d)?;
    }

    for s in &decoded.scripts {
        insert_script(&tx, meta.slot, s)?;
    }

    tx.commit()?;
    Ok(())
}

/// Rolls back everything after slot `p`, in a single transaction. Only
/// called in `Mode::Running` (see `sync.rs`), where `apply` always
/// writes one `block`/`tx` row pair per block -- the 1:1 invariant
/// `tx.block_slot` deliberately isn't allowed to assume globally (catch-
/// up's chunked checkpointing breaks it, see `BASE_SCHEMA_SQL`), but
/// which holds here, making a plain slot-range delete correct.
///
/// Deleting `tx` directly (not via cascade from `block` -- there is
/// none) does almost all of the work via the real `utxo.tx_hash` FK:
/// it drops the `utxo` rows *created* by a rolled-back tx (correct --
/// those outputs never should have existed) and, via
/// `utxo_unspend_on_tx_delete`, clears `spent_slot`/`spent_by` on any
/// *other* `utxo` row a rolled-back tx had spent (also correct -- that
/// output's creation stands, only its spend is undone). `datum`/
/// `script`/`block` aren't owned by a single tx, so they get their own
/// explicit deletes.
#[instrument(skip(conn))]
pub fn rollback(conn: &mut Connection, p: u64) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute("DELETE FROM datum WHERE slot > ?1", params![p])?;
    tx.execute("DELETE FROM script WHERE slot > ?1", params![p])?;
    tx.execute("DELETE FROM tx WHERE block_slot > ?1", params![p])?;
    tx.execute("DELETE FROM block WHERE slot > ?1", params![p])?;
    tx.commit()?;
    Ok(())
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PruneStats {
    pub utxo_rows: usize,
    pub block_rows: usize,
    pub tx_rows: usize,
}

/// Deletes `utxo` rows spent more than `k` blocks before the tip, old
/// `block` rows, and the `tx` rows left with nothing in `utxo` pointing
/// at them as a result (same orphan check as `sweep`'s datum/script
/// cleanup -- deliberately NOT "`tx` rows older than the cutoff": a
/// `tx` can easily have created a UTxO that's still live today, and
/// `tx.block_slot` isn't FK-cascaded into `utxo` precisely so that a
/// slot-based delete here can never take a live row down with it; see
/// `BASE_SCHEMA_SQL`). Depth is in block numbers, not slots: slots tick
/// faster than blocks are produced, so a slot-based cutoff would
/// under-retain relative to true `k`-block reorg safety.
///
/// A no-op if depth can't be established yet — safe, since it only prunes
/// less, never something still in range.
#[instrument(skip(conn))]
pub fn prune(conn: &mut Connection, k: u64) -> Result<PruneStats> {
    let Some(tip_block_no): Option<u64> = conn
        .query_row(
            "SELECT block_no FROM block ORDER BY slot DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .optional()?
        .flatten()
    else {
        return Ok(PruneStats::default());
    };
    let Some(target) = tip_block_no.checked_sub(k) else {
        return Ok(PruneStats::default());
    };

    // Oldest block still in the retained window; its slot is a safe
    // cutoff — anything older is unambiguously more than `k` blocks deep.
    let Some(cutoff_slot): Option<u64> = conn
        .query_row(
            "SELECT MIN(slot) FROM block WHERE block_no >= ?1",
            params![target],
            |r| r.get(0),
        )
        .optional()?
        .flatten()
    else {
        return Ok(PruneStats::default());
    };

    let tx = conn.transaction()?;
    let utxo_rows = tx.execute(
        "DELETE FROM utxo WHERE spent_slot IS NOT NULL AND spent_slot < ?1",
        params![cutoff_slot],
    )?;
    let block_rows = tx.execute("DELETE FROM block WHERE slot < ?1", params![cutoff_slot])?;
    let tx_rows = tx.execute(&format!("DELETE FROM tx WHERE {TX_ORPHAN_CHECK_SQL}"), [])?;
    tx.commit()?;

    if utxo_rows > 0 || block_rows > 0 || tx_rows > 0 {
        info!(cutoff_slot, utxo_rows, block_rows, tx_rows, "pruned");
    }
    Ok(PruneStats {
        utxo_rows,
        block_rows,
        tx_rows,
    })
}

/// Bounds WAL growth across a long catch-up run.
pub(crate) fn wal_checkpoint(conn: &Connection) -> Result<()> {
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    Ok(())
}

/// Has `seed::run` already completed on this DB (`meta.seed = 'done'`)?
pub fn is_seed_done(conn: &Connection) -> Result<bool> {
    let v: Option<String> = conn
        .query_row("SELECT v FROM meta WHERE k = 'seed'", [], |r| r.get(0))
        .optional()?;
    Ok(v.as_deref() == Some("done"))
}

pub(crate) fn mark_seed_done(conn: &Connection) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO meta(k, v) VALUES ('seed', 'done')",
        [],
    )?;
    Ok(())
}

/// Row count alone can't tell a partial load from a complete one, which is
/// why completion is tracked by `meta.seed` instead.
pub(crate) fn wipe_for_reseed(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "DELETE FROM utxo; DELETE FROM datum; DELETE FROM script; DELETE FROM block;",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::decode::{DatumKind, OutputRef, TxRow, Utxo};
    use proptest::prelude::*;
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

    /// `tx_hash` is a real FK now: every creating tx (one per `creates`
    /// entry) and the shared dummy spending tx (`[0xFF; 32]`, reused
    /// across every call -- `insert_tx`'s `OR IGNORE` makes that fine)
    /// need a `TxRow` alongside the `utxo` effects they produce.
    /// A fresh, globally-unique dummy hash per call -- `spent_by` is now
    /// load-bearing (rollback un-spends by deleting the *spending* tx's
    /// row), so reusing one fixed dummy hash across unrelated calls would
    /// wrongly tie unrelated spends to the same tx, breaking rollback's
    /// "only this tx's spends are undone" granularity.
    fn fresh_spender() -> [u8; 32] {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut h = [0xFFu8; 32];
        h[..8].copy_from_slice(&n.to_le_bytes());
        h
    }

    fn decoded_block(creates: &[OutputRef], spends: &[OutputRef]) -> DecodedBlock {
        let mut txs: Vec<TxRow> = creates
            .iter()
            .map(|r| TxRow {
                hash: r.tx_hash,
                required_signers: vec![],
            })
            .collect();
        let spender = fresh_spender();
        if !spends.is_empty() {
            txs.push(TxRow {
                hash: spender,
                required_signers: vec![],
            });
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

    #[test]
    fn continuity_violation_is_rejected() {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();

        let (meta1, hash1) = block_meta(1, [0u8; 32]);
        apply(&mut conn, &meta1, &decoded_block(&[], &[])).unwrap();

        let (bad_meta, _) = block_meta(2, [0xFFu8; 32]); // wrong prev_hash
        assert!(apply(&mut conn, &bad_meta, &decoded_block(&[], &[])).is_err());

        let (meta2, _) = block_meta(2, hash1); // correct prev_hash
        apply(&mut conn, &meta2, &decoded_block(&[], &[])).unwrap();
    }

    #[test]
    fn spend_of_unknown_ref_is_a_no_op() {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();

        let mut counter = 0;
        let ghost = fresh_ref(&mut counter); // never created

        let (meta, _) = block_meta(1, [0u8; 32]);
        apply(&mut conn, &meta, &decoded_block(&[], &[ghost])).unwrap();

        assert!(snapshot(&conn).is_empty());
    }

    #[test]
    fn double_spend_attempt_is_a_no_op() {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();

        let mut counter = 0;
        let r = fresh_ref(&mut counter);

        let (meta1, hash1) = block_meta(1, [0u8; 32]);
        apply(&mut conn, &meta1, &decoded_block(&[r], &[])).unwrap();
        let (meta2, hash2) = block_meta(2, hash1);
        apply(&mut conn, &meta2, &decoded_block(&[], &[r])).unwrap();
        let (meta3, _) = block_meta(3, hash2);
        // Already spent at slot 2; this must not move spent_slot to 3.
        apply(&mut conn, &meta3, &decoded_block(&[], &[r])).unwrap();

        let rows = snapshot(&conn);
        assert_eq!(rows, vec![(r, 1, Some(2))]);
    }

    #[test]
    fn same_block_create_and_spend_lands_as_created_and_spent() {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();

        let mut counter = 0;
        let r = fresh_ref(&mut counter);

        let (meta, _) = block_meta(1, [0u8; 32]);
        apply(&mut conn, &meta, &decoded_block(&[r], &[r])).unwrap();

        // Running mode does not cancel this in memory (that's catch-up only):
        // the row persists, created and spent in the same block.
        assert_eq!(snapshot(&conn), vec![(r, 1, Some(1))]);
    }

    /// Applies `n` empty blocks starting right after `prev_hash`, returning
    /// the last applied hash (so callers can keep chaining).
    fn apply_n_empty_blocks(
        conn: &mut Connection,
        from_slot: u64,
        n: u64,
        mut prev_hash: [u8; 32],
    ) -> [u8; 32] {
        for slot in from_slot..from_slot + n {
            let (meta, hash) = block_meta(slot, prev_hash);
            apply(conn, &meta, &decoded_block(&[], &[])).unwrap();
            prev_hash = hash;
        }
        prev_hash
    }

    #[test]
    fn prune_is_a_no_op_before_the_chain_is_k_blocks_deep() {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();
        apply_n_empty_blocks(&mut conn, 1, 5, [0u8; 32]);

        // Tip block_no is 5 (block_meta sets block_no = slot); k=10 is
        // deeper than the whole chain, so there's nothing safe to prune.
        let stats = prune(&mut conn, 10).unwrap();
        assert_eq!(stats, PruneStats::default());
        let count: u64 = conn
            .query_row("SELECT COUNT(*) FROM block", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 5);
    }

    #[test]
    fn prune_is_a_no_op_when_tip_block_no_is_unknown() {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();
        // Simulates a DB with only the seed anchor's checkpoint so far
        // (BlockMeta::block_no = None), before any real block has landed.
        insert_block_row(
            &conn,
            &BlockMeta {
                slot: 1,
                hash: [1; 32],
                prev_hash: [0; 32],
                block_no: None,
            },
        )
        .unwrap();

        let stats = prune(&mut conn, 0).unwrap();
        assert_eq!(stats, PruneStats::default());
        let count: u64 = conn
            .query_row("SELECT COUNT(*) FROM block", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1, "the unusable row must not be swept away either");
    }

    #[test]
    fn prune_removes_only_what_is_more_than_k_blocks_deep() {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();

        let mut counter = 0u64;
        let old = fresh_ref(&mut counter); // created and spent early
        let recent = fresh_ref(&mut counter); // created early, spent recently
        let mut prev_hash = [0u8; 32];

        let (meta1, hash1) = block_meta(1, prev_hash);
        apply(&mut conn, &meta1, &decoded_block(&[old, recent], &[])).unwrap();
        prev_hash = hash1;

        let (meta2, hash2) = block_meta(2, prev_hash);
        apply(&mut conn, &meta2, &decoded_block(&[], &[old])).unwrap(); // old spent at slot 2
        prev_hash = hash2;

        prev_hash = apply_n_empty_blocks(&mut conn, 3, 10, prev_hash); // blocks 3..=12

        let (meta13, hash13) = block_meta(13, prev_hash);
        apply(&mut conn, &meta13, &decoded_block(&[], &[recent])).unwrap(); // recent spent at slot 13
        apply_n_empty_blocks(&mut conn, 14, 1, hash13); // tip is now block_no 14

        // Tip block_no 14, k=10 -> cutoff at block_no 4 -> cutoff_slot 4.
        // `old`'s spend (slot 2) is before the cutoff; `recent`'s (slot 13) is not.
        let stats = prune(&mut conn, 10).unwrap();
        assert!(
            stats.utxo_rows >= 1,
            "the long-spent row must be pruned: {stats:?}"
        );
        assert_eq!(
            stats.block_rows, 3,
            "block rows 1..=3 (slot < 4) must be pruned"
        );

        let rows = snapshot(&conn);
        assert!(
            !rows.iter().any(|(r, _, _)| *r == old),
            "old's row must be gone"
        );
        assert!(
            rows.iter()
                .any(|(r, _, spent)| *r == recent && *spent == Some(13)),
            "recent's row must survive, still spent at slot 13: {rows:?}"
        );

        let min_slot: u64 = conn
            .query_row("SELECT MIN(slot) FROM block", [], |r| r.get(0))
            .unwrap();
        assert_eq!(min_slot, 4);
    }

    #[test]
    fn prune_called_again_with_nothing_newly_prunable_is_a_no_op() {
        let mut conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();
        apply_n_empty_blocks(&mut conn, 1, 20, [0u8; 32]);

        let first = prune(&mut conn, 5).unwrap();
        assert!(first.block_rows > 0);

        let second = prune(&mut conn, 5).unwrap();
        assert_eq!(
            second,
            PruneStats::default(),
            "nothing new fell past the same cutoff"
        );
    }

    // Model: ref -> (created_slot, spent_slot).
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

        fn rollback(&mut self, p: u64) {
            self.0.retain(|_, (slot, _)| *slot <= p);
            for (_, spent) in self.0.values_mut() {
                if matches!(*spent, Some(s) if s > p) {
                    *spent = None;
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
        #[test]
        fn apply_n_rollback_m_matches_model(
            ops in prop::collection::vec((0usize..=3, 0usize..=2), 1..30),
            rollback_frac in 0.0f64..=1.0,
        ) {
            let mut conn = Connection::open_in_memory().unwrap();
            init(&conn).unwrap();

            let mut model = Model::default();
            let mut prev_hash = [0u8; 32];
            let mut counter = 0u64;
            // Oldest-unspent-first pool; a block's own creates are pushed
            // before that block's spends are drawn, so a small pool can
            // (and does, across the generated sequence) exercise same-block
            // create+spend chains, not just cross-block ones.
            let mut unspent: VecDeque<OutputRef> = VecDeque::new();

            for (i, (n_creates, n_spends_req)) in ops.iter().enumerate() {
                let slot = (i + 1) as u64;
                let creates: Vec<OutputRef> = (0..*n_creates).map(|_| fresh_ref(&mut counter)).collect();
                unspent.extend(creates.iter().copied());

                let n_spend = (*n_spends_req).min(unspent.len());
                let spends: Vec<OutputRef> = (0..n_spend).map(|_| unspent.pop_front().unwrap()).collect();

                let (meta, hash) = block_meta(slot, prev_hash);
                prev_hash = hash;

                apply(&mut conn, &meta, &decoded_block(&creates, &spends)).unwrap();
                model.apply(slot, &creates, &spends);
            }

            let n = ops.len() as u64;
            let p = ((n as f64) * rollback_frac).floor() as u64;

            rollback(&mut conn, p).unwrap();
            model.rollback(p);

            let mut got = snapshot(&conn);
            let mut want = model.snapshot();
            got.sort();
            want.sort();
            prop_assert_eq!(got, want);
        }
    }
}
