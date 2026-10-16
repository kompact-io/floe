//! query: read-only lookups against the index.
//!
//! Every lookup except `by_ref` requires running mode: catch-up has no
//! secondary indexes and a mid-flight tip set, so callers get "not ready"
//! instead. Use `open_reader` for a short-lived connection per
//! request/batch — a long-lived reader blocks WAL checkpoints.

use anyhow::Result;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, params};

use crate::decode::{OutputRef, ScriptLang, TxHash};
use crate::store::{self, Mode};

/// Distinguish with `err.downcast_ref::<NotReady>()`, e.g. to map to a 503.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotReady;

impl std::fmt::Display for NotReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "index is still in catch-up mode; only by-ref lookups are available"
        )
    }
}

impl std::error::Error for NotReady {}

fn require_running(conn: &Connection) -> Result<()> {
    if store::get_mode(conn)? != Mode::Running {
        return Err(NotReady.into());
    }
    Ok(())
}

/// A hash-kind datum with no matching `datum` row resolves to `Hash` (the
/// unresolved case); an inline datum always resolves, since ingest
/// guarantees its row exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedDatum {
    None,
    /// Hash-kind, no preimage seen in any witness set yet.
    Hash([u8; 32]),
    /// Hash-kind, preimage resolved.
    HashResolved([u8; 32], Vec<u8>),
    /// Inline (always resolved).
    Inline([u8; 32], Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedScript {
    pub hash: [u8; 28],
    pub lang: ScriptLang,
    pub cbor: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedUtxo {
    pub output_ref: OutputRef,
    /// Slot this output was created in -- needed to resolve its
    /// `ChainPoint` (join against `block`) for gRPC's `block_ref`.
    pub slot: u64,
    /// `None` = unspent. `by_ref` can return a spent row; every other
    /// lookup only returns unspent ones.
    pub spent_slot: Option<u64>,
    /// Hash of the tx that spent this output; `None` iff `spent_slot` is.
    pub spent_by: Option<TxHash>,
    /// Always present -- see `decode::Utxo::pay_cred`.
    pub pay_cred: [u8; 28],
    pub pay_script: bool,
    pub stake_cred: Option<[u8; 28]>,
    /// `None` iff `stake_cred` is.
    pub stake_script: Option<bool>,
    pub coin: u64,
    pub assets: Option<Vec<u8>>,
    pub datum: ResolvedDatum,
    pub script: Option<ResolvedScript>,
}

const RESOLVE_SELECT: &str = "
    SELECT utxo.tx_hash, utxo.ix, utxo.spent_slot, utxo.pay_cred, utxo.pay_script,
           utxo.stake_cred, utxo.stake_script, utxo.coin, utxo.assets,
           utxo.datum_kind, utxo.datum_hash, utxo.script_hash,
           datum.cbor, script.cbor, script.lang, utxo.spent_by, utxo.slot
    FROM utxo
    LEFT JOIN datum ON datum.hash = utxo.datum_hash
    LEFT JOIN script ON script.hash = utxo.script_hash
";

fn row_to_resolved(row: &Row) -> Result<ResolvedUtxo> {
    let tx_hash: Vec<u8> = row.get(0)?;
    let ix: i64 = row.get(1)?;
    let spent_slot: Option<u64> = row.get(2)?;
    let pay_cred: Vec<u8> = row.get(3)?;
    let pay_script: bool = row.get(4)?;
    let stake_cred: Option<Vec<u8>> = row.get(5)?;
    let stake_script: Option<bool> = row.get(6)?;
    let coin: u64 = row.get(7)?;
    let assets: Option<Vec<u8>> = row.get(8)?;
    let datum_kind: i64 = row.get(9)?;
    let datum_hash: Option<Vec<u8>> = row.get(10)?;
    let script_hash: Option<Vec<u8>> = row.get(11)?;
    let datum_cbor: Option<Vec<u8>> = row.get(12)?;
    let script_cbor: Option<Vec<u8>> = row.get(13)?;
    let script_lang: Option<i64> = row.get(14)?;
    let spent_by: Option<Vec<u8>> = row.get(15)?;
    let slot: u64 = row.get(16)?;

    let datum = match datum_kind {
        0 => ResolvedDatum::None,
        1 => {
            let h: [u8; 32] = datum_hash
                .expect("datum_kind=Hash implies datum_hash is set")
                .try_into()
                .unwrap();
            match datum_cbor {
                Some(cbor) => ResolvedDatum::HashResolved(h, cbor),
                None => ResolvedDatum::Hash(h),
            }
        }
        2 => {
            let h: [u8; 32] = datum_hash
                .expect("datum_kind=Inline implies datum_hash is set")
                .try_into()
                .unwrap();
            let cbor = datum_cbor.ok_or_else(|| {
                anyhow::anyhow!(
                    "invariant violation: inline datum_hash {} has no datum row",
                    hex::encode(h)
                )
            })?;
            ResolvedDatum::Inline(h, cbor)
        }
        k => anyhow::bail!("unknown datum_kind {k}"),
    };

    let script = match script_hash {
        None => None,
        Some(h) => {
            let hash: [u8; 28] = h.try_into().unwrap();
            let cbor = script_cbor.ok_or_else(|| {
                anyhow::anyhow!(
                    "invariant violation: script_hash {} has no script row",
                    hex::encode(hash)
                )
            })?;
            let lang = match script_lang.expect("script_hash implies script_lang is set") {
                0 => ScriptLang::Native,
                1 => ScriptLang::PlutusV1,
                2 => ScriptLang::PlutusV2,
                3 => ScriptLang::PlutusV3,
                l => anyhow::bail!("unknown script lang {l}"),
            };
            Some(ResolvedScript { hash, lang, cbor })
        }
    };

    Ok(ResolvedUtxo {
        output_ref: OutputRef {
            tx_hash: tx_hash.try_into().unwrap(),
            ix: ix as u16,
        },
        slot,
        spent_slot,
        spent_by: spent_by.map(|v| v.try_into().expect("spent_by column must be 32 bytes")),
        pay_cred: pay_cred
            .try_into()
            .expect("pay_cred column must be 28 bytes"),
        pay_script,
        stake_cred: stake_cred.map(|v| v.try_into().expect("stake_cred column must be 28 bytes")),
        stake_script,
        coin,
        assets,
        datum,
        script,
    })
}

/// `?N IS NULL OR (tx_hash, ix) > (?N, ?N+1)` lets one SQL string serve
/// both "from the start" and "resume after this ref".
fn cursor_params(after: Option<OutputRef>) -> (Option<Vec<u8>>, Option<i64>) {
    match after {
        Some(r) => (Some(r.tx_hash.to_vec()), Some(r.ix as i64)),
        None => (None, None),
    }
}

fn collect_resolved(
    stmt: &mut rusqlite::Statement,
    params: impl rusqlite::Params,
) -> Result<Vec<ResolvedUtxo>> {
    let mut rows = stmt.query(params)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(row_to_resolved(row)?);
    }
    Ok(out)
}

/// Primary-key lookup. Available in both modes; not restricted to the tip
/// set, so it can also report a recently-spent (not yet pruned) row.
pub fn by_ref(conn: &Connection, r: OutputRef) -> Result<Option<ResolvedUtxo>> {
    let sql = format!("{RESOLVE_SELECT} WHERE utxo.tx_hash = ?1 AND utxo.ix = ?2");
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![r.tx_hash.as_slice(), r.ix as i64])?;
    match rows.next()? {
        Some(row) => Ok(Some(row_to_resolved(row)?)),
        None => Ok(None),
    }
}

/// A tx, hydrated entirely from `utxo` rows -- no tx body is stored
/// anywhere (see `store::BASE_SCHEMA_SQL`'s `tx` table comment).
/// `inputs` are the rows *this tx spent*, already carrying their
/// original (pre-spend) content -- UTxORPC's `TxInput.as_output`, for
/// free, via `spent_by`. Everything the `Tx` message has beyond
/// in/outputs (fee, certs, mint, ...) isn't represented here at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTx {
    pub hash: TxHash,
    pub block_slot: u64,
    pub inputs: Vec<ResolvedUtxo>,
    pub outputs: Vec<ResolvedUtxo>,
}

/// Primary-key lookup, available in both modes like `by_ref`. `None`
/// means the hash was never indexed as a tx at all (unknown, or a
/// seed-anchor-era tx predating `tx` rows existing); an indexed tx with
/// no outputs still resolving (e.g. no indexed outputs for other
/// reasons) is `Some` with an empty `outputs`/`inputs` as appropriate,
/// not `None`.
pub fn by_tx_hash(conn: &Connection, hash: TxHash) -> Result<Option<ResolvedTx>> {
    let block_slot: Option<u64> = conn
        .query_row(
            "SELECT block_slot FROM tx WHERE hash = ?1",
            params![hash.as_slice()],
            |r| r.get(0),
        )
        .optional()?;
    let Some(block_slot) = block_slot else {
        return Ok(None);
    };

    let outputs = {
        let sql = format!("{RESOLVE_SELECT} WHERE utxo.tx_hash = ?1 ORDER BY utxo.ix");
        let mut stmt = conn.prepare(&sql)?;
        collect_resolved(&mut stmt, params![hash.as_slice()])?
    };
    let inputs = {
        let sql =
            format!("{RESOLVE_SELECT} WHERE utxo.spent_by = ?1 ORDER BY utxo.tx_hash, utxo.ix");
        let mut stmt = conn.prepare(&sql)?;
        collect_resolved(&mut stmt, params![hash.as_slice()])?
    };

    Ok(Some(ResolvedTx {
        hash,
        block_slot,
        inputs,
        outputs,
    }))
}

/// A resolved datum, by its own hash -- for `ReadData`, independent of
/// any `utxo` row (unlike `ResolvedDatum`, which is always resolved in
/// the context of one output's `datum_hash`/`datum_kind`). `None` if
/// no witness/inline occurrence of this hash has ever been seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatumRecord {
    pub hash: [u8; 32],
    pub cbor: Vec<u8>,
}

/// Primary-key lookup, available in both modes like `by_ref`/`by_tx_hash`.
pub fn datum_by_hash(conn: &Connection, hash: [u8; 32]) -> Result<Option<DatumRecord>> {
    conn.query_row(
        "SELECT cbor FROM datum WHERE hash = ?1",
        params![hash.as_slice()],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .optional()
    .map(|opt| opt.map(|cbor| DatumRecord { hash, cbor }))
    .map_err(Into::into)
}

/// Tip-set lookup by address, decomposed into credentials (uses `utxo_pay`;
/// there is no index on the raw address bytes). `stake_cred` uses `IS`, not
/// `=`: an enterprise address (`stake_cred: None`) must match only rows
/// where `utxo.stake_cred IS NULL`, not every row regardless of stake part
/// -- `IS` is NULL-safe equality, giving exactly that in one comparison.
/// `pay_cred: None` (a Byron address) matches no rows via plain `=`,
/// needing no special case: see `api::parse_address`'s doc comment.
pub fn by_address(
    conn: &Connection,
    pay_cred: Option<[u8; 28]>,
    pay_script: bool,
    stake_cred: Option<[u8; 28]>,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_cred = ?1 AND utxo.pay_script = ?2 AND utxo.stake_cred IS ?3
           AND utxo.spent_by IS NULL
           AND (?4 IS NULL OR (utxo.tx_hash, utxo.ix) > (?4, ?5))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?6"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_cred.map(|c| c.to_vec()),
            pay_script,
            stake_cred.map(|c| c.to_vec()),
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Tip-set lookup by payment credential (uses `utxo_pay`). `pay_script`
/// disambiguates a script-hash credential from a key-hash sharing the
/// same 28 bytes.
pub fn by_pay_cred(
    conn: &Connection,
    pay_cred: [u8; 28],
    pay_script: bool,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_cred = ?1 AND utxo.pay_script = ?2 AND utxo.spent_by IS NULL
           AND (?3 IS NULL OR (utxo.tx_hash, utxo.ix) > (?3, ?4))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?5"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_cred.as_slice(),
            pay_script,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Tip-set lookup by stake credential (uses `utxo_stk`).
pub fn by_stake_cred(
    conn: &Connection,
    stake_cred: [u8; 28],
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.stake_cred = ?1 AND utxo.spent_by IS NULL
           AND (?2 IS NULL OR (utxo.tx_hash, utxo.ix) > (?2, ?3))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?4"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![stake_cred.as_slice(), after_tx_hash, after_ix, limit],
    )
}

/// Tip-set lookup of reference-script holders (uses `utxo_script`).
pub fn by_script_hash(
    conn: &Connection,
    script_hash: [u8; 28],
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.script_hash = ?1 AND utxo.spent_by IS NULL
           AND (?2 IS NULL OR (utxo.tx_hash, utxo.ix) > (?2, ?3))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?4"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![script_hash.as_slice(), after_tx_hash, after_ix, limit],
    )
}

/// Like `by_pay_cred`, but doesn't filter on `pay_script` -- for a
/// caller that only has a credential hash with no indication of
/// whether it's a key or script hash. UTxORPC's `AddressPattern` is
/// exactly this: `payment_part`/`delegation_part` are bare `bytes`,
/// with no key-vs-script flag anywhere on the message.
pub fn by_pay_cred_any_script(
    conn: &Connection,
    pay_cred: [u8; 28],
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_cred = ?1 AND utxo.spent_by IS NULL
           AND (?2 IS NULL OR (utxo.tx_hash, utxo.ix) > (?2, ?3))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?4"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![pay_cred.as_slice(), after_tx_hash, after_ix, limit],
    )
}

/// Like `by_address`, but doesn't filter on `pay_script` -- see
/// `by_pay_cred_any_script`'s doc comment for why.
pub fn by_credentials_any_script(
    conn: &Connection,
    pay_cred: [u8; 28],
    stake_cred: Option<[u8; 28]>,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_cred = ?1 AND utxo.stake_cred IS ?2 AND utxo.spent_by IS NULL
           AND (?3 IS NULL OR (utxo.tx_hash, utxo.ix) > (?3, ?4))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?5"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_cred.as_slice(),
            stake_cred.map(|c| c.to_vec()),
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// A block's coordinates, for UTxORPC's `ChainPoint`. `height` is
/// `None` for the seed anchor checkpoint (`BlockMeta::block_no`'s own
/// doc comment) -- rendered as `0` on the wire, an accepted gap (see
/// plan-rpc.md). No `timestamp` here at all: deriving it needs
/// genesis/era data floe doesn't have (`ReadGenesis`/`ReadEraSummary`
/// are both "not supported"), so it isn't this type's job to pretend
/// otherwise.
pub struct BlockPoint {
    pub slot: u64,
    pub hash: [u8; 32],
    pub height: Option<u64>,
}

fn row_to_block_point(row: &Row) -> Result<BlockPoint> {
    let slot: u64 = row.get(0)?;
    let hash: Vec<u8> = row.get(1)?;
    let height: Option<u64> = row.get(2)?;
    Ok(BlockPoint {
        slot,
        hash: hash.try_into().expect("block.hash column must be 32 bytes"),
        height,
    })
}

/// The current tip's `ChainPoint` coordinates (every gRPC response
/// needs one as `ledger_tip`). `None` only when `block` is empty.
pub fn tip_point(conn: &Connection) -> Result<Option<BlockPoint>> {
    let mut stmt =
        conn.prepare("SELECT slot, hash, block_no FROM block ORDER BY slot DESC LIMIT 1")?;
    let mut rows = stmt.query([])?;
    match rows.next()? {
        Some(row) => Ok(Some(row_to_block_point(row)?)),
        None => Ok(None),
    }
}

/// A specific block's `ChainPoint` coordinates, by the slot a `utxo`/
/// `tx` row recorded (e.g. `ResolvedUtxo::slot`) -- for an item's own
/// `block_ref`.
pub fn block_point(conn: &Connection, slot: u64) -> Result<Option<BlockPoint>> {
    let mut stmt = conn.prepare("SELECT slot, hash, block_no FROM block WHERE slot = ?1")?;
    let mut rows = stmt.query(params![slot])?;
    match rows.next()? {
        Some(row) => Ok(Some(row_to_block_point(row)?)),
        None => Ok(None),
    }
}

/// Why a `since`-style reorg-safety check didn't come back `Valid` --
/// see `PointCheck::Stale` and `plan-rpc.md`'s `Point` design.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleReason {
    /// `hash` was given with no `slot` to compare against the
    /// retention window -- nothing further to classify: not found,
    /// full stop.
    Unknown,
    /// `slot` is within the server's retained history, but `hash`
    /// doesn't match what's there (or isn't there at all) -- this
    /// *is* a reorg: the server has data covering that slot and this
    /// hash isn't part of it. Carries the retention horizon this was
    /// judged against, for context.
    RolledBack { oldest_retained_slot: u64 },
    /// `slot` predates the oldest slot the server still retains --
    /// genuinely out of history, gone regardless of whether it was
    /// ever canonical. Carries that horizon.
    TooOld { oldest_retained_slot: u64 },
}

/// Resolution of a `since`-style reorg-safety check (floe's own
/// `Point` type, see `plan-rpc.md`): `slot`/`hash` are the request's
/// own fields, both optional and independent of each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointCheck {
    /// No `hash` was given -- a bare numeric lower bound (the
    /// request's own `slot`, if given; `None` means no lower bound at
    /// all), with no chain-validity check performed. Entirely the
    /// caller's own responsibility to use only when that's safe.
    NoHashGiven(Option<u64>),
    /// `hash` was given and is still on the canonical chain, at this
    /// slot (which matches the request's own `slot`, if also given).
    Valid(u64),
    /// `hash` was given but is stale. Not an error -- an expected,
    /// valid outcome the caller must handle by finding an earlier
    /// intersection and retrying, not something resolved here.
    Stale(StaleReason),
}

/// Requires running mode (like every other secondary-index-backed
/// lookup): during catch-up, `block` rows only exist per chunk
/// checkpoint, not per block, so a point check made mid-catch-up
/// could misjudge a perfectly canonical hash as stale/rolled-back
/// purely because the fine-grained row for it hasn't been written
/// yet -- a false positive, not a reorg. Rather than try to reason
/// about that gap, this just refuses to run at all until running
/// mode, same `NotReady` every other such lookup reports.
pub fn check_point(
    conn: &Connection,
    slot: Option<u64>,
    hash: Option<[u8; 32]>,
) -> Result<PointCheck> {
    require_running(conn)?;
    let Some(hash) = hash else {
        return Ok(PointCheck::NoHashGiven(slot));
    };
    let found_slot: Option<u64> = conn
        .query_row(
            "SELECT slot FROM block WHERE hash = ?1",
            params![hash.as_slice()],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(found) = found_slot
        && (slot.is_none() || slot == Some(found))
    {
        return Ok(PointCheck::Valid(found));
    }
    // Not found, or found at a slot that disagrees with the one
    // claimed -- try to tell "pruned" from "reorged" using the claimed
    // `slot` (if given) against the oldest slot still retained.
    let Some(claimed_slot) = slot else {
        return Ok(PointCheck::Stale(StaleReason::Unknown));
    };
    let oldest_retained_slot: Option<u64> =
        conn.query_row("SELECT MIN(slot) FROM block", [], |row| row.get(0))?;
    let Some(oldest) = oldest_retained_slot else {
        // No blocks at all yet -- degenerate (a fresh, empty DB that
        // somehow already passed `require_running`); nothing to
        // classify against.
        return Ok(PointCheck::Stale(StaleReason::Unknown));
    };
    Ok(PointCheck::Stale(if claimed_slot < oldest {
        StaleReason::TooOld {
            oldest_retained_slot: oldest,
        }
    } else {
        StaleReason::RolledBack {
            oldest_retained_slot: oldest,
        }
    }))
}

/// Created after `since_slot` and still unspent as of now -- the
/// "newly live" half of a `since`-based change feed. `since_slot:
/// None` means no lower bound (every live row). No `pay_script`
/// filter, like `by_pay_cred_any_script` -- `AddressPattern` has no
/// such flag. Paginated like `by_pay_cred` & co: `after`/`limit`,
/// same `(tx_hash, ix)` keyset cursor.
pub fn created_since_by_pay_cred_any_script(
    conn: &Connection,
    pay_cred: [u8; 28],
    since_slot: Option<u64>,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_cred = ?1 AND utxo.spent_by IS NULL
           AND (?2 IS NULL OR utxo.slot > ?2)
           AND (?3 IS NULL OR (utxo.tx_hash, utxo.ix) > (?3, ?4))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?5"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_cred.as_slice(),
            since_slot,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Like `created_since_by_pay_cred_any_script`, with a `stake_cred`
/// filter too (`IS`, not `=` -- see `by_address`'s doc comment).
pub fn created_since_by_credentials_any_script(
    conn: &Connection,
    pay_cred: [u8; 28],
    stake_cred: Option<[u8; 28]>,
    since_slot: Option<u64>,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_cred = ?1 AND utxo.stake_cred IS ?2 AND utxo.spent_by IS NULL
           AND (?3 IS NULL OR utxo.slot > ?3)
           AND (?4 IS NULL OR (utxo.tx_hash, utxo.ix) > (?4, ?5))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?6"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_cred.as_slice(),
            stake_cred.map(|c| c.to_vec()),
            since_slot,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Like `created_since_by_pay_cred_any_script`, filtered by stake
/// credential alone (see `by_stake_cred`).
pub fn created_since_by_stake_cred(
    conn: &Connection,
    stake_cred: [u8; 28],
    since_slot: Option<u64>,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.stake_cred = ?1 AND utxo.spent_by IS NULL
           AND (?2 IS NULL OR utxo.slot > ?2)
           AND (?3 IS NULL OR (utxo.tx_hash, utxo.ix) > (?3, ?4))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?5"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            stake_cred.as_slice(),
            since_slot,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Live as of `since_slot` (created at or before it) and spent since
/// then -- the "newly gone" half of a `since`-based change feed. A
/// row created *after* `since_slot` and already spent again is
/// deliberately excluded by the `utxo.slot <= ?` half of this filter:
/// a caller who last synced at `since_slot` never saw it live, so its
/// disappearance isn't a change from their point of view. `since_slot:
/// None` (no lower bound at all) makes the `slot <=` half vacuously
/// true for every row, so every ever-spent row matches -- correct:
/// with no prior point, every spend is "new" to the caller. Paginated
/// like `created_since_by_pay_cred_any_script`.
pub fn spent_since_by_pay_cred_any_script(
    conn: &Connection,
    pay_cred: [u8; 28],
    since_slot: Option<u64>,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_cred = ?1 AND utxo.spent_by IS NOT NULL
           AND (?2 IS NULL OR utxo.slot <= ?2)
           AND (?2 IS NULL OR utxo.spent_slot > ?2)
           AND (?3 IS NULL OR (utxo.tx_hash, utxo.ix) > (?3, ?4))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?5"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_cred.as_slice(),
            since_slot,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Like `spent_since_by_pay_cred_any_script`, with a `stake_cred`
/// filter too.
pub fn spent_since_by_credentials_any_script(
    conn: &Connection,
    pay_cred: [u8; 28],
    stake_cred: Option<[u8; 28]>,
    since_slot: Option<u64>,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_cred = ?1 AND utxo.stake_cred IS ?2 AND utxo.spent_by IS NOT NULL
           AND (?3 IS NULL OR utxo.slot <= ?3)
           AND (?3 IS NULL OR utxo.spent_slot > ?3)
           AND (?4 IS NULL OR (utxo.tx_hash, utxo.ix) > (?4, ?5))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?6"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_cred.as_slice(),
            stake_cred.map(|c| c.to_vec()),
            since_slot,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Like `spent_since_by_pay_cred_any_script`, filtered by stake
/// credential alone.
pub fn spent_since_by_stake_cred(
    conn: &Connection,
    stake_cred: [u8; 28],
    since_slot: Option<u64>,
    after: Option<OutputRef>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.stake_cred = ?1 AND utxo.spent_by IS NOT NULL
           AND (?2 IS NULL OR utxo.slot <= ?2)
           AND (?2 IS NULL OR utxo.spent_slot > ?2)
           AND (?3 IS NULL OR (utxo.tx_hash, utxo.ix) > (?3, ?4))
         ORDER BY utxo.tx_hash, utxo.ix LIMIT ?5"
    );
    let (after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            stake_cred.as_slice(),
            since_slot,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// One per request or small batch; don't hold it open (blocks WAL
/// checkpoints).
///
/// `SQLITE_OPEN_URI`: harmless for a plain path (SQLite only treats a
/// filename as a URI if it starts with `file:`), but required for `serve`
/// mode's replicated-DB case, where `db_path` is a `file:...?vfs=<name>`
/// URI selecting a non-default VFS (e.g. a Litestream read replica).
pub fn open_reader(db_path: &str) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )?;
    conn.pragma_update(None, "query_only", true)?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catchup::Engine;
    use crate::decode::{DatumKind, DatumRow, DecodedBlock, ScriptRow, TxRow, Utxo};
    use crate::store::{self, BlockMeta};

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        store::init(&conn).unwrap();
        conn
    }

    fn switch_to_running(conn: &mut Connection) {
        Engine::new(store::tip(conn).unwrap())
            .switch_to_running(conn)
            .unwrap();
    }

    fn utxo(ix_byte: u8) -> Utxo {
        Utxo {
            output_ref: OutputRef {
                tx_hash: [ix_byte; 32],
                ix: 0,
            },
            pay_cred: [0x60; 28],
            pay_script: false,
            stake_cred: None,
            stake_script: None,
            coin: 1,
            assets: None,
            datum_kind: DatumKind::None,
            datum_hash: None,
            script_hash: None,
        }
    }

    #[test]
    fn by_ref_works_in_catchup_but_other_lookups_report_not_ready() {
        let conn = fresh_db();
        assert_eq!(store::get_mode(&conn).unwrap(), Mode::Catchup);

        let r = OutputRef {
            tx_hash: [0x01; 32],
            ix: 0,
        };
        store::insert_block_row(
            &conn,
            &BlockMeta {
                slot: 1,
                hash: [1; 32],
                prev_hash: [0; 32],
                block_no: Some(1),
            },
        )
        .unwrap();
        store::insert_tx(&conn, 1, &[0x01u8; 32]).unwrap();
        store::insert_utxo(&conn, 1, &utxo(0x01)).unwrap();

        assert!(by_ref(&conn, r).unwrap().is_some());

        let err = by_address(&conn, None, false, None, None, 10).unwrap_err();
        assert!(
            err.downcast_ref::<NotReady>().is_some(),
            "expected NotReady, got {err:?}"
        );
    }

    #[test]
    fn resolve_handles_every_datum_and_script_shape() {
        let mut conn = fresh_db();

        let inline_cbor = vec![0xAAu8; 3];
        let inline_hash: [u8; 32] = pallas_crypto::hash::Hasher::<256>::hash(&inline_cbor)
            .as_ref()
            .try_into()
            .unwrap();
        let mut inline_out = utxo(0x01);
        inline_out.datum_kind = DatumKind::Inline;
        inline_out.datum_hash = Some(inline_hash);

        let unresolved_hash = [0xBBu8; 32];
        let mut unresolved_out = utxo(0x02);
        unresolved_out.datum_kind = DatumKind::Hash;
        unresolved_out.datum_hash = Some(unresolved_hash);

        let resolved_hash = [0xCCu8; 32];
        let resolved_cbor = vec![0xDDu8; 2];
        let mut resolved_out = utxo(0x03);
        resolved_out.datum_kind = DatumKind::Hash;
        resolved_out.datum_hash = Some(resolved_hash);

        let script_cbor = vec![0xEEu8; 4];
        let script_hash: [u8; 28] =
            pallas_crypto::hash::Hasher::<224>::hash_tagged(&script_cbor, 0)
                .as_ref()
                .try_into()
                .unwrap();
        let mut script_out = utxo(0x04);
        script_out.script_hash = Some(script_hash);

        let decoded = DecodedBlock {
            txs: [0x01u8, 0x02, 0x03, 0x04]
                .into_iter()
                .map(|b| TxRow { hash: [b; 32] })
                .collect(),
            spent: Default::default(),
            created: vec![inline_out, unresolved_out, resolved_out, script_out],
            datums: vec![
                DatumRow {
                    hash: inline_hash,
                    cbor: inline_cbor.clone(),
                },
                DatumRow {
                    hash: resolved_hash,
                    cbor: resolved_cbor.clone(),
                },
            ],
            scripts: vec![ScriptRow {
                hash: script_hash,
                lang: crate::decode::ScriptLang::Native,
                cbor: script_cbor.clone(),
            }],
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
        switch_to_running(&mut conn);

        let got = |b: u8| {
            by_ref(
                &conn,
                OutputRef {
                    tx_hash: [b; 32],
                    ix: 0,
                },
            )
            .unwrap()
            .unwrap()
        };

        assert_eq!(
            got(0x01).datum,
            ResolvedDatum::Inline(inline_hash, inline_cbor)
        );
        assert_eq!(got(0x02).datum, ResolvedDatum::Hash(unresolved_hash));
        assert_eq!(
            got(0x03).datum,
            ResolvedDatum::HashResolved(resolved_hash, resolved_cbor)
        );
        assert_eq!(
            got(0x04).script,
            Some(ResolvedScript {
                hash: script_hash,
                lang: crate::decode::ScriptLang::Native,
                cbor: script_cbor
            })
        );
    }

    #[test]
    fn by_address_pagination_covers_every_row_exactly_once() {
        let mut conn = fresh_db();
        let pay_cred = [0x61u8; 28];
        let decoded = DecodedBlock {
            txs: (0..5u8).map(|i| TxRow { hash: [i; 32] }).collect(),
            spent: Default::default(),
            created: (0..5u8)
                .map(|i| Utxo {
                    pay_cred,
                    ..utxo(i)
                })
                .collect(),
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
        switch_to_running(&mut conn);

        let mut seen = Vec::new();
        let mut after = None;
        loop {
            let page = by_address(&conn, Some(pay_cred), false, None, after, 2).unwrap();
            if page.is_empty() {
                break;
            }
            after = Some(page.last().unwrap().output_ref);
            seen.extend(page.into_iter().map(|u| u.output_ref));
        }

        let mut want: Vec<OutputRef> = (0..5u8)
            .map(|i| OutputRef {
                tx_hash: [i; 32],
                ix: 0,
            })
            .collect();
        want.sort();
        assert_eq!(seen, want);
    }

    /// The reason `by_address` uses `stake_cred IS ?3`, not `= ?3`: an
    /// enterprise address and a base address sharing the same payment
    /// credential must stay distinguishable without a dedicated address
    /// index, now that `utxo_addr` is gone.
    #[test]
    fn by_address_distinguishes_enterprise_from_base_sharing_a_pay_cred() {
        let mut conn = fresh_db();
        let pay_cred = [0x61u8; 28];
        let enterprise = Utxo {
            pay_cred,
            stake_cred: None,
            stake_script: None,
            ..utxo(0x01)
        };
        let base = Utxo {
            pay_cred,
            stake_cred: Some([0x99u8; 28]),
            stake_script: Some(false),
            ..utxo(0x02)
        };
        let decoded = DecodedBlock {
            txs: vec![TxRow { hash: [0x01; 32] }, TxRow { hash: [0x02; 32] }],
            spent: Default::default(),
            created: vec![enterprise, base],
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
        switch_to_running(&mut conn);

        let enterprise_only = by_address(&conn, Some(pay_cred), false, None, None, 10).unwrap();
        assert_eq!(enterprise_only.len(), 1);
        assert_eq!(enterprise_only[0].output_ref.tx_hash, [0x01; 32]);

        let base_only =
            by_address(&conn, Some(pay_cred), false, Some([0x99u8; 28]), None, 10).unwrap();
        assert_eq!(base_only.len(), 1);
        assert_eq!(base_only[0].output_ref.tx_hash, [0x02; 32]);
    }

    #[test]
    fn credential_and_address_lookups_exclude_spent() {
        let mut conn = fresh_db();
        let mut spent_utxo = utxo(0x01);
        spent_utxo.pay_cred = [0x42; 28];
        spent_utxo.stake_cred = Some([0x43; 28]);
        spent_utxo.stake_script = Some(false);
        let r = spent_utxo.output_ref;

        let create = DecodedBlock {
            txs: vec![TxRow { hash: [0x01; 32] }],
            spent: Default::default(),
            created: vec![spent_utxo],
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
            &create,
        )
        .unwrap();
        let spend = DecodedBlock {
            txs: vec![TxRow { hash: [0xFFu8; 32] }],
            spent: [(r, [0xFFu8; 32])].into_iter().collect(),
            created: vec![],
            datums: vec![],
            scripts: vec![],
        };
        store::apply(
            &mut conn,
            &BlockMeta {
                slot: 2,
                hash: [2; 32],
                prev_hash: [1; 32],
                block_no: Some(2),
            },
            &spend,
        )
        .unwrap();
        switch_to_running(&mut conn);

        assert!(
            by_pay_cred(&conn, [0x42; 28], false, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            by_stake_cred(&conn, [0x43; 28], None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            by_address(&conn, Some([0x42; 28]), false, Some([0x43; 28]), None, 10)
                .unwrap()
                .is_empty()
        );
    }
}
