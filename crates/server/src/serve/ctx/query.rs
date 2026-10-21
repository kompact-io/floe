//! query: read-only lookups against the index.
//!
//! Every lookup except `by_ref` requires running mode: catch-up has no
//! secondary indexes and a mid-flight tip set, so callers get "not ready"
//! instead. Use `open_reader` for a short-lived connection per
//! request/batch — a long-lived reader blocks WAL checkpoints.

use anyhow::Result;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, params};

use crate::ingest::decode::{OutputRef, ScriptLang, TxHash};
use crate::ingest::store::{self, Mode};

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
    /// Always present -- see `decode::Utxo::pay_hash`.
    pub pay_hash: [u8; 28],
    pub stake_hash: Option<[u8; 28]>,
    /// Key-vs-script for both parts, network, and address type, all in
    /// one -- see `decode::Credentials`. The only reason this is
    /// stored at all is to rebuild the exact address later
    /// (`ctx.rs`'s `reconstruct_address`); no lookup here filters on
    /// it -- a hash is cryptographically a key hash or a script hash,
    /// never both, so there's nothing to disambiguate for a query.
    pub address_header: u8,
    pub coin: u64,
    pub assets: Option<Vec<u8>>,
    pub datum: ResolvedDatum,
    pub script: Option<ResolvedScript>,
}

const RESOLVE_SELECT: &str = "
    SELECT utxo.tx_hash, utxo.ix, utxo.spent_slot, utxo.pay_hash, utxo.stake_hash,
           utxo.address_header, utxo.coin, utxo.assets,
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
    let pay_hash: Vec<u8> = row.get(3)?;
    let stake_hash: Option<Vec<u8>> = row.get(4)?;
    let address_header: u8 = row.get(5)?;
    let coin: u64 = row.get(6)?;
    let assets: Option<Vec<u8>> = row.get(7)?;
    let datum_kind: i64 = row.get(8)?;
    let datum_hash: Option<Vec<u8>> = row.get(9)?;
    let script_hash: Option<Vec<u8>> = row.get(10)?;
    let datum_cbor: Option<Vec<u8>> = row.get(11)?;
    let script_cbor: Option<Vec<u8>> = row.get(12)?;
    let script_lang: Option<i64> = row.get(13)?;
    let spent_by: Option<Vec<u8>> = row.get(14)?;
    let slot: u64 = row.get(15)?;

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
        pay_hash: pay_hash
            .try_into()
            .expect("pay_hash column must be 28 bytes"),
        stake_hash: stake_hash.map(|v| v.try_into().expect("stake_hash column must be 28 bytes")),
        address_header,
        coin,
        assets,
        datum,
        script,
    })
}

/// A paginated lookup's resume point: a timestamp slot (meaning
/// depends on the lookup -- creation slot, spend slot, or
/// `COALESCE(spent_slot, slot)`, see each function's own doc comment)
/// plus the row's own key, as a tiebreak for rows sharing a slot.
#[derive(Clone, Copy)]
pub struct Cursor {
    pub slot: u64,
    pub output_ref: OutputRef,
}

impl Cursor {
    /// `<slot>:<tx_hash_hex>:<ix>`, the one opaque wire shape both
    /// `scheme::grpc`'s `SearchUtxosRequest::start_token`/
    /// `SearchUtxosResponse::next_token` and `scheme::http::query`'s
    /// own same-named fields use -- defined once here so the two
    /// schemes' cursors can't drift apart. The `&'static str` error is
    /// generic on purpose; each scheme wraps it in its own status type
    /// (`tonic::Status::invalid_argument`/`ApiError::bad_request`),
    /// typically prefixed with the field name it came from.
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        let mut parts = s.splitn(3, ':');
        let slot: u64 = parts
            .next()
            .ok_or("must be <slot>:<tx_hash_hex>:<ix>")?
            .parse()
            .map_err(|_| "slot must be a number")?;
        let h = parts.next().ok_or("must be <slot>:<tx_hash_hex>:<ix>")?;
        let i = parts.next().ok_or("must be <slot>:<tx_hash_hex>:<ix>")?;
        let tx_hash: TxHash = hex::decode(h)
            .map_err(|_| "tx_hash must be hex")?
            .try_into()
            .map_err(|_| "tx_hash must be 32 bytes")?;
        let ix: u16 = i.parse().map_err(|_| "ix must be a number")?;
        Ok(Cursor {
            slot,
            output_ref: OutputRef { tx_hash, ix },
        })
    }

    pub fn format(self) -> String {
        format!(
            "{}:{}:{}",
            self.slot,
            hex::encode(self.output_ref.tx_hash),
            self.output_ref.ix
        )
    }
}

fn cursor_params(after: Option<Cursor>) -> (Option<u64>, Option<Vec<u8>>, Option<i64>) {
    match after {
        Some(c) => (
            Some(c.slot),
            Some(c.output_ref.tx_hash.to_vec()),
            Some(c.output_ref.ix as i64),
        ),
        None => (None, None, None),
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

/// The parsed form of whatever wire-specific address pattern a caller
/// validated into this (gRPC's `AddressPattern`, http's `pay_hash`/
/// `stake_hash` query params) -- shared by every credential-keyed
/// lookup below.
#[derive(Clone, Copy)]
pub enum AddressFilter {
    PayOnly([u8; 28]),
    PayAndStake([u8; 28], [u8; 28]),
    StakeOnly([u8; 28]),
}

/// Joins each row's origin `BlockPoint` in one pass -- callers get a
/// self-contained result with no further connection needed.
pub fn with_block_points(
    conn: &Connection,
    rows: Vec<ResolvedUtxo>,
) -> Result<Vec<(ResolvedUtxo, Option<BlockPoint>)>> {
    rows.into_iter()
        .map(|u| {
            let block = block_point(conn, u.slot)?;
            Ok((u, block))
        })
        .collect()
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
    /// `tx.required_signers`, unpacked from `store`'s concatenated
    /// 28-byte-chunk `BLOB` encoding. Empty, not an error, for a
    /// seed-inherited row (see `store::insert_tx`'s own doc).
    pub required_signers: Vec<[u8; 28]>,
}

/// Primary-key lookup, available in both modes like `by_ref`. `None`
/// means the hash was never indexed as a tx at all (unknown, or a
/// seed-anchor-era tx predating `tx` rows existing); an indexed tx with
/// no outputs still resolving (e.g. no indexed outputs for other
/// reasons) is `Some` with an empty `outputs`/`inputs` as appropriate,
/// not `None`.
pub fn by_tx_hash(conn: &Connection, hash: TxHash) -> Result<Option<ResolvedTx>> {
    let row: Option<(u64, Option<Vec<u8>>)> = conn
        .query_row(
            "SELECT block_slot, required_signers FROM tx WHERE hash = ?1",
            params![hash.as_slice()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((block_slot, required_signers)) = row else {
        return Ok(None);
    };
    let required_signers = required_signers
        .unwrap_or_default()
        .as_chunks::<28>()
        .0
        .to_vec();

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
        required_signers,
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
/// Tip-set lookup by stake credential (uses `utxo_stk`).
///
/// `since_slot` (`SearchUtxos.since`, floe's own addition): `None` is
/// the tip-set-only predicate (`spent_by IS NULL`); `Some(s)` also
/// includes rows spent after `s`, live or not -- the caller's own job
/// to validate `s` against retained history first (see `check_point`).
///
/// Orders and paginates by `effective_slot = COALESCE(spent_slot,
/// slot)`, not row identity: a row's position in this order is "when
/// it last became relevant to this predicate" -- its creation slot
/// while live, its spend slot once spent. A client polling page by
/// page while the chain advances always finds newly-relevant rows at
/// or past its current cursor, never skipped by landing earlier in an
/// identity-ordered scan.
pub fn by_stake_hash(
    conn: &Connection,
    stake_hash: [u8; 28],
    since_slot: Option<u64>,
    after: Option<Cursor>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.stake_hash = ?1
           AND (utxo.spent_by IS NULL OR (?2 IS NOT NULL AND utxo.spent_slot > ?2))
           AND (?3 IS NULL OR (COALESCE(utxo.spent_slot, utxo.slot), utxo.tx_hash, utxo.ix) > (?3, ?4, ?5))
         ORDER BY COALESCE(utxo.spent_slot, utxo.slot), utxo.tx_hash, utxo.ix LIMIT ?6"
    );
    let (after_slot, after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            stake_hash.as_slice(),
            since_slot,
            after_slot,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Tip-set lookup by payment hash (uses `utxo_pay`). Never
/// distinguishes key-hash from script-hash: UTxORPC's `AddressPattern`
/// has no such flag (`payment_part`/`delegation_part` are bare
/// `bytes`), and a hash is cryptographically one or the other, never
/// both, so there's nothing to disambiguate for a lookup anyway.
///
/// `since_slot`/ordering: see `by_stake_hash`'s doc comment.
pub fn by_pay_hash(
    conn: &Connection,
    pay_hash: [u8; 28],
    since_slot: Option<u64>,
    after: Option<Cursor>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_hash = ?1
           AND (utxo.spent_by IS NULL OR (?2 IS NOT NULL AND utxo.spent_slot > ?2))
           AND (?3 IS NULL OR (COALESCE(utxo.spent_slot, utxo.slot), utxo.tx_hash, utxo.ix) > (?3, ?4, ?5))
         ORDER BY COALESCE(utxo.spent_slot, utxo.slot), utxo.tx_hash, utxo.ix LIMIT ?6"
    );
    let (after_slot, after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_hash.as_slice(),
            since_slot,
            after_slot,
            after_tx_hash,
            after_ix,
            limit
        ],
    )
}

/// Like `by_pay_hash`, with a `stake_hash` filter too (`IS`, not `=`:
/// an enterprise address and a base address sharing the same payment
/// hash must stay distinguishable -- `NULL` only matches `stake_hash
/// IS NULL`, never every row regardless of stake part).
pub fn by_credentials(
    conn: &Connection,
    pay_hash: [u8; 28],
    stake_hash: Option<[u8; 28]>,
    since_slot: Option<u64>,
    after: Option<Cursor>,
    limit: u32,
) -> Result<Vec<ResolvedUtxo>> {
    require_running(conn)?;
    let sql = format!(
        "{RESOLVE_SELECT}
         WHERE utxo.pay_hash = ?1 AND utxo.stake_hash IS ?2
           AND (utxo.spent_by IS NULL OR (?3 IS NOT NULL AND utxo.spent_slot > ?3))
           AND (?4 IS NULL OR (COALESCE(utxo.spent_slot, utxo.slot), utxo.tx_hash, utxo.ix) > (?4, ?5, ?6))
         ORDER BY COALESCE(utxo.spent_slot, utxo.slot), utxo.tx_hash, utxo.ix LIMIT ?7"
    );
    let (after_slot, after_tx_hash, after_ix) = cursor_params(after);
    let mut stmt = conn.prepare(&sql)?;
    collect_resolved(
        &mut stmt,
        params![
            pay_hash.as_slice(),
            stake_hash.map(|c| c.to_vec()),
            since_slot,
            after_slot,
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

/// Wraps a stale `since`/`Point` as an error, for `?`-propagation out
/// of a lookup. `err.downcast_ref::<Stale>()` recovers the
/// `StaleReason` for a scheme to map to its own distinguishable
/// wire-level error (gRPC status code, HTTP status code) -- never a
/// bare string, so a caller can branch on it.
#[derive(Debug, Clone, Copy)]
pub struct Stale(pub StaleReason);

impl std::fmt::Display for Stale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            StaleReason::Unknown => write!(f, "since is stale: hash not found"),
            StaleReason::RolledBack {
                oldest_retained_slot,
            } => write!(
                f,
                "since is stale: rolled back (oldest retained slot {oldest_retained_slot})"
            ),
            StaleReason::TooOld {
                oldest_retained_slot,
            } => write!(
                f,
                "since is stale: too old (oldest retained slot {oldest_retained_slot})"
            ),
        }
    }
}

impl std::error::Error for Stale {}

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

/// One per request or small batch; don't hold it open (blocks WAL
/// checkpoints).
///
/// `SQLITE_OPEN_URI`: harmless for a plain path (SQLite only treats a
/// filename as a URI if it starts with `file:`), but needed if
/// `db_path` is ever a `file:...?vfs=<name>` URI selecting a
/// non-default VFS.
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
    use crate::ingest::catchup::Engine;
    use crate::ingest::decode::{DatumKind, DatumRow, DecodedBlock, ScriptRow, TxRow, Utxo};
    use crate::ingest::store::{self, BlockMeta};

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
        store::insert_tx(&conn, 1, &[0x01u8; 32], &[]).unwrap();
        store::insert_utxo(&conn, 1, &utxo(0x01)).unwrap();

        assert!(by_ref(&conn, r).unwrap().is_some());

        let err = by_pay_hash(&conn, [0u8; 28], None, None, 10).unwrap_err();
        assert!(
            err.downcast_ref::<NotReady>().is_some(),
            "expected NotReady, got {err:?}"
        );
    }

    #[test]
    fn resolve_handles_every_datum_and_script_shape() {
        let mut conn = fresh_db();

        let inline_cbor = vec![0xAAu8; 3];
        let inline_hash: [u8; 32] = amaru_kernel::Hasher::<256>::hash(&inline_cbor)
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
        let script_hash: [u8; 28] = amaru_kernel::Hasher::<224>::hash_tagged(&script_cbor, 0)
            .as_ref()
            .try_into()
            .unwrap();
        let mut script_out = utxo(0x04);
        script_out.script_hash = Some(script_hash);

        let decoded = DecodedBlock {
            txs: [0x01u8, 0x02, 0x03, 0x04]
                .into_iter()
                .map(|b| TxRow {
                    hash: [b; 32],
                    required_signers: vec![],
                })
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
                lang: crate::ingest::decode::ScriptLang::Native,
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
                lang: crate::ingest::decode::ScriptLang::Native,
                cbor: script_cbor
            })
        );
    }

    #[test]
    fn by_pay_hash_pagination_covers_every_row_exactly_once() {
        let mut conn = fresh_db();
        let pay_hash = [0x61u8; 28];
        let decoded = DecodedBlock {
            txs: (0..5u8)
                .map(|i| TxRow {
                    hash: [i; 32],
                    required_signers: vec![],
                })
                .collect(),
            spent: Default::default(),
            created: (0..5u8)
                .map(|i| Utxo {
                    pay_hash,
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
            let page = by_pay_hash(&conn, pay_hash, None, after, 2).unwrap();
            if page.is_empty() {
                break;
            }
            let last = page.last().unwrap();
            after = Some(Cursor {
                slot: last.slot,
                output_ref: last.output_ref,
            });
            seen.extend(page.into_iter().map(|u| u.output_ref));
        }

        let mut want: Vec<OutputRef> = (0..5u8)
            .map(|i| OutputRef {
                tx_hash: [i; 32],
                ix: 0,
            })
            .collect();
        want.sort();
        seen.sort();
        assert_eq!(seen, want);
    }

    /// The reason `by_credentials` uses `stake_hash IS ?2`, not
    /// `= ?2`: an enterprise address and a base address sharing the
    /// same payment hash must stay distinguishable without a
    /// dedicated address index.
    #[test]
    fn by_credentials_distinguishes_enterprise_from_base_sharing_a_pay_hash() {
        let mut conn = fresh_db();
        let pay_hash = [0x61u8; 28];
        let enterprise = Utxo {
            pay_hash,
            stake_hash: None,
            address_header: 0x61,
            ..utxo(0x01)
        };
        let base = Utxo {
            pay_hash,
            stake_hash: Some([0x99u8; 28]),
            address_header: 0x01,
            ..utxo(0x02)
        };
        let decoded = DecodedBlock {
            txs: vec![
                TxRow {
                    hash: [0x01; 32],
                    required_signers: vec![],
                },
                TxRow {
                    hash: [0x02; 32],
                    required_signers: vec![],
                },
            ],
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

        let enterprise_only = by_credentials(&conn, pay_hash, None, None, None, 10).unwrap();
        assert_eq!(enterprise_only.len(), 1);
        assert_eq!(enterprise_only[0].output_ref.tx_hash, [0x01; 32]);

        let base_only =
            by_credentials(&conn, pay_hash, Some([0x99u8; 28]), None, None, 10).unwrap();
        assert_eq!(base_only.len(), 1);
        assert_eq!(base_only[0].output_ref.tx_hash, [0x02; 32]);
    }

    #[test]
    fn credential_and_address_lookups_exclude_spent() {
        let mut conn = fresh_db();
        let mut spent_utxo = utxo(0x01);
        spent_utxo.pay_hash = [0x42; 28];
        spent_utxo.stake_hash = Some([0x43; 28]);
        let r = spent_utxo.output_ref;

        let create = DecodedBlock {
            txs: vec![TxRow {
                hash: [0x01; 32],
                required_signers: vec![],
            }],
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
            txs: vec![TxRow {
                hash: [0xFFu8; 32],
                required_signers: vec![],
            }],
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
            by_pay_hash(&conn, [0x42; 28], None, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            by_stake_hash(&conn, [0x43; 28], None, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            by_credentials(&conn, [0x42; 28], Some([0x43; 28]), None, None, 10)
                .unwrap()
                .is_empty()
        );
    }
}
