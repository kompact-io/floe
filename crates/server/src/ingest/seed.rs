//! seed: streams amaru's bootstrap `tvar` snapshot into the `utxo` table in
//! sorted chunks, then writes one checkpoint `block` row at the anchor.
//! Catch-up resumes at the anchor's next block.
//!
//! The caller locates `tables/tvar` inside the archive and knows the
//! anchor's `(slot, hash)` from its file name; this module only streams
//! the entries.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;

use anyhow::{Result, anyhow, bail};
use pallas_addresses::Address;
use pallas_codec::minicbor;
use pallas_crypto::hash::Hasher;
use pallas_primitives::{AssetName, PolicyId, PositiveCoin};
use rayon::prelude::*;
use rusqlite::Connection;
use tracing::{info, instrument};

use crate::ingest::decode::{self, DatumKind, DatumRow, OutputRef, ScriptLang, ScriptRow, Utxo};
use crate::ingest::mempack::{self, Datum, ScriptRef};
use crate::ingest::store::{self, BlockMeta};
use crate::ingest::tvar;

/// Row-count chunk size for the bulk load. No byte ceiling: unlike live
/// catch-up, a seed `TxOut`'s datum/script payloads are the exception, not
/// routine, so a fixed row count keeps memory bounded well enough. Bumped
/// from 100_000: fewer, larger transactions amortize per-chunk commit
/// overhead further now that decode (the actual bottleneck, see
/// `BATCH_ROWS`) is off the critical path.
const CHUNK_ROWS: usize = 300_000;

/// How many entries' raw (key, value) bytes to buffer before decoding
/// them as a `rayon` parallel batch. `decode_entry` (credential/asset/
/// datum/script extraction, address parsing) is pure CPU work with no
/// shared state per entry -- unlike catch-up's per-block decode (already
/// parallel, see `decode::decode_blocks_parallel`), this streaming loop
/// used to do that work inline on one thread. Independent of
/// `CHUNK_ROWS`: this is a decode-parallelism granularity, not a DB
/// transaction size.
const BATCH_ROWS: usize = 20_000;

#[derive(Debug, Default, Clone, Copy)]
pub struct SeedStats {
    pub entries: u64,
    pub inserted: u64,
    /// Byron-address outputs (not indexed) or an address that failed to
    /// parse — defensive; not expected to fire on a real archive.
    pub skipped: u64,
}

/// Has `run` already completed on this DB (`meta.seed = 'done'`)?
pub fn is_done(conn: &Connection) -> Result<bool> {
    store::is_seed_done(conn)
}

/// Refuses to run again once already done: a crash mid-seed wipes and
/// restarts, but a *completed* seed shouldn't be silently redone.
///
/// `anchor`'s state *includes* that block, so block application must
/// start at the next one.
#[instrument(skip(reader, conn, anchor), fields(anchor_slot = anchor.0, anchor_hash = %hex::encode(anchor.1)))]
pub fn run(reader: impl Read, conn: &mut Connection, anchor: (u64, [u8; 32])) -> Result<SeedStats> {
    if is_done(conn)? {
        bail!("seed already completed (meta.seed = 'done'); refusing to re-run");
    }
    store::wipe_for_reseed(conn)?;

    // Bulk one-time load, not incremental writes: `foreign_keys` is
    // provably satisfied by construction (`Chunk::flush` always inserts
    // a `tx` row before the `utxo` rows referencing it, same
    // transaction), and a crash mid-seed restarts from scratch
    // (`wipe_for_reseed` above, gated on `meta.seed` never having been
    // set) rather than resuming -- there's no partial state to protect
    // with `synchronous=NORMAL`'s extra fsync here. Both are restored
    // below regardless of outcome, since catch-up/running mode do rely
    // on them.
    conn.pragma_update(None, "foreign_keys", false)?;
    conn.pragma_update(None, "synchronous", "OFF")?;
    let result = stream_into(reader, conn, anchor);
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", true)?;
    let stats = result?;

    store::mark_seed_done(conn)?;
    info!(
        entries = stats.entries,
        inserted = stats.inserted,
        skipped = stats.skipped,
        "seed complete"
    );
    Ok(stats)
}

struct Chunk {
    anchor_slot: u64,
    /// Every seeded UTxO's owning tx needs a `tx` row (the new FK on
    /// `utxo.tx_hash`) -- attributed to the anchor checkpoint block,
    /// same simplification `rows`' own `anchor_slot` already makes for
    /// "when was this created".
    txs: HashSet<[u8; 32]>,
    rows: Vec<Utxo>,
    datums: HashMap<[u8; 32], DatumRow>,
    scripts: HashMap<[u8; 28], ScriptRow>,
}

impl Chunk {
    fn new(anchor_slot: u64) -> Self {
        Self {
            anchor_slot,
            txs: HashSet::new(),
            rows: Vec::new(),
            datums: HashMap::new(),
            scripts: HashMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    fn push_datum(&mut self, d: DatumRow) {
        self.datums.entry(d.hash).or_insert(d);
    }

    fn push_script(&mut self, s: ScriptRow) {
        self.scripts.entry(s.hash).or_insert(s);
    }

    /// One transaction: sorted creates, then deduped sorted datum/script
    /// inserts — the same sorted-insert discipline as the live catch-up
    /// chunk, minus tombstones: a seed snapshot has no spends.
    fn flush(&mut self, conn: &mut Connection) -> Result<()> {
        if self.rows.is_empty() && self.datums.is_empty() && self.scripts.is_empty() {
            return Ok(());
        }
        let tx = conn.transaction()?;

        let mut txs: Vec<[u8; 32]> = self.txs.drain().collect();
        txs.sort();
        for hash in &txs {
            store::insert_tx(&tx, self.anchor_slot, hash)?;
        }

        let mut rows = std::mem::take(&mut self.rows);
        rows.sort_by_key(|u| u.output_ref);
        for u in &rows {
            store::insert_utxo(&tx, self.anchor_slot, u)?;
        }

        let mut datums: Vec<DatumRow> = self.datums.drain().map(|(_, d)| d).collect();
        datums.sort_by_key(|d| d.hash);
        for d in &datums {
            store::insert_datum(&tx, self.anchor_slot, d)?;
        }

        let mut scripts: Vec<ScriptRow> = self.scripts.drain().map(|(_, s)| s).collect();
        scripts.sort_by_key(|s| s.hash);
        for s in &scripts {
            store::insert_script(&tx, self.anchor_slot, s)?;
        }

        tx.commit()?;
        info!(
            rows = rows.len(),
            datums = datums.len(),
            scripts = scripts.len(),
            "flushed seed chunk"
        );
        Ok(())
    }
}

/// Does NOT wipe existing state or set `meta.seed` — that's `run`'s job;
/// this part is also convenient to call directly from a test.
#[instrument(skip(reader, conn, anchor), fields(anchor_slot = anchor.0))]
pub fn stream_into(
    reader: impl Read,
    conn: &mut Connection,
    anchor: (u64, [u8; 32]),
) -> Result<SeedStats> {
    let (anchor_slot, anchor_hash) = anchor;
    let mut stats = SeedStats::default();
    let mut chunk = Chunk::new(anchor_slot);

    // Inserted up front, not after streaming: every seeded UTxO's `tx`
    // row (new FK on `utxo.tx_hash`) is attributed to this checkpoint
    // block (`block_slot`), so it must already exist before the first
    // chunk flush, not just before the utxo rows land.
    // prev_hash is informational only; block_no is genuinely unknown here.
    store::insert_block_row(
        conn,
        &BlockMeta {
            slot: anchor_slot,
            hash: anchor_hash,
            prev_hash: [0u8; 32],
            block_no: None,
        },
    )?;

    let mut batch: Vec<([u8; 32], u16, Vec<u8>)> = Vec::with_capacity(BATCH_ROWS);

    tvar::for_each_entry(reader, |key, val| {
        let (txid, ix) = tvar::split_key(key);
        let tx_hash: [u8; 32] = txid.try_into().expect("tvar key's txid is 32 bytes");
        batch.push((tx_hash, ix, val.to_vec()));

        if batch.len() >= BATCH_ROWS {
            flush_batch(&mut batch, conn, &mut chunk, &mut stats)?;
        }
        Ok(true)
    })?;
    flush_batch(&mut batch, conn, &mut chunk, &mut stats)?;

    chunk.flush(conn)?;
    Ok(stats)
}

/// Decodes a buffered batch in parallel (pure CPU work, no shared state
/// per entry -- see `BATCH_ROWS`), then merges results into `chunk` and
/// `stats` on this thread and flushes `chunk` if it's grown past
/// `CHUNK_ROWS`. Clears `batch` either way.
fn flush_batch(
    batch: &mut Vec<([u8; 32], u16, Vec<u8>)>,
    conn: &mut Connection,
    chunk: &mut Chunk,
    stats: &mut SeedStats,
) -> Result<()> {
    let decoded: Vec<Result<Option<SeedEntry>>> = batch
        .par_iter()
        .map(|(tx_hash, ix, val)| decode_entry(*tx_hash, *ix, val))
        .collect();
    stats.entries += batch.len() as u64;
    batch.clear();

    for entry in decoded {
        match entry? {
            Some(e) => {
                chunk.txs.insert(e.utxo.output_ref.tx_hash);
                if let Some(d) = e.datum {
                    chunk.push_datum(d);
                }
                if let Some(s) = e.script {
                    chunk.push_script(s);
                }
                chunk.rows.push(e.utxo);
                stats.inserted += 1;
            }
            None => stats.skipped += 1,
        }
    }

    if chunk.len() >= CHUNK_ROWS {
        chunk.flush(conn)?;
    }
    Ok(())
}

/// One decoded `tvar` entry, pending merge into the shared `Chunk`.
struct SeedEntry {
    utxo: Utxo,
    datum: Option<DatumRow>,
    script: Option<ScriptRow>,
}

/// Converts one `tvar` entry into a `SeedEntry`, or `None` for a
/// Byron-address output (not indexed) or an unparseable address
/// (defensive). Pure -- no DB, no shared state -- so callers can run it
/// across a `rayon` batch.
fn decode_entry(tx_hash: [u8; 32], ix: u16, val: &[u8]) -> Result<Option<SeedEntry>> {
    let tx_out = mempack::decode_tx_out(val)?;

    let address = match Address::from_bytes(&tx_out.address) {
        Ok(a) => a,
        Err(_) => return Ok(None),
    };
    if matches!(address, Address::Byron(_)) {
        return Ok(None);
    }
    let (pay_hash, stake_hash, address_header) = decode::credentials_from_address(&address);
    let pay_hash = pay_hash.expect("Byron (the only no-pay_hash case) was already filtered out");

    let assets = if tx_out.assets.is_empty() {
        None
    } else {
        let mut grouped: BTreeMap<PolicyId, BTreeMap<AssetName, PositiveCoin>> = BTreeMap::new();
        for a in &tx_out.assets {
            let qty = PositiveCoin::try_from(a.qty).map_err(|_| {
                anyhow!(
                    "asset with zero quantity in seed output {}#{ix}",
                    hex::encode(tx_hash)
                )
            })?;
            grouped
                .entry(PolicyId::from(a.policy))
                .or_default()
                .insert(AssetName::from(a.name.clone()), qty);
        }
        Some(minicbor::to_vec(&grouped).expect("infallible"))
    };

    let mut datum_row = None;
    let (datum_kind, datum_hash) = match &tx_out.datum {
        Datum::None => (DatumKind::None, None),
        Datum::Hash(h) => (DatumKind::Hash, Some(*h)),
        Datum::Inline(bytes) => {
            let hash: [u8; 32] = decode::hash_bytes(Hasher::<256>::hash(bytes));
            datum_row = Some(DatumRow {
                hash,
                cbor: bytes.clone(),
            });
            (DatumKind::Inline, Some(hash))
        }
    };

    let mut script_row = None;
    let script_hash = match &tx_out.script {
        None => None,
        Some(script_ref) => {
            let (lang, cbor) = match script_ref {
                ScriptRef::Native(b) => (ScriptLang::Native, b.clone()),
                ScriptRef::PlutusV1(b) => (ScriptLang::PlutusV1, b.clone()),
                ScriptRef::PlutusV2(b) => (ScriptLang::PlutusV2, b.clone()),
                ScriptRef::PlutusV3(b) => (ScriptLang::PlutusV3, b.clone()),
            };
            let hash: [u8; 28] = decode::hash_bytes(Hasher::<224>::hash_tagged(&cbor, lang as u8));
            script_row = Some(ScriptRow { hash, lang, cbor });
            Some(hash)
        }
    };

    Ok(Some(SeedEntry {
        utxo: Utxo {
            output_ref: OutputRef { tx_hash, ix },
            pay_hash,
            stake_hash,
            address_header,
            coin: tx_out.coin,
            assets,
            datum_kind,
            datum_hash,
            script_hash,
        },
        datum: datum_row,
        script: script_row,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::decode::ScriptLang;

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        store::init(&conn).unwrap();
        conn
    }

    /// Hand-encodes a `tvar` snapshot: a plain output (tag 0), a Byron
    /// output (must be skipped), one with a multiasset bundle + hash datum
    /// (tag 1), and one with an inline datum + native ref script (tag 5).
    fn synthetic_tvar() -> Vec<u8> {
        fn cbor_bytes(out: &mut Vec<u8>, b: &[u8]) {
            // CBOR byte string, always via the 1-byte-length form.
            assert!(
                b.len() < 256,
                "test helper only handles byte strings under 256 bytes"
            );
            out.push(0x58);
            out.push(b.len() as u8);
            out.extend_from_slice(b);
        }

        fn varuint(out: &mut Vec<u8>, mut v: u64) {
            // Mempack's own big-endian base-128 varint (see Dec::varuint).
            let mut bytes = vec![(v & 0x7f) as u8];
            v >>= 7;
            while v > 0 {
                bytes.push((v & 0x7f) as u8 | 0x80);
                v >>= 7;
            }
            bytes.reverse();
            out.extend_from_slice(&bytes);
        }

        fn short_bytes(out: &mut Vec<u8>, b: &[u8]) {
            varuint(out, b.len() as u64);
            out.extend_from_slice(b);
        }

        // Entry 1: tag 0, plain 29-byte enterprise-key address, no datum/script.
        let addr1 = {
            let mut a = vec![0x61u8]; // header: enterprise key, testnet
            a.extend(std::iter::repeat_n(0x11u8, 28));
            a
        };
        let mut v1 = vec![0u8]; // tag 0
        short_bytes(&mut v1, &addr1);
        v1.push(0); // compact value tag 0 (coin only)
        varuint(&mut v1, 5_000_000);

        // Entry 2: not a valid address at all (fiddlier to hand-roll a real
        // Byron blob); exercises seed_utxo's parse-error skip path instead.
        let addr2 = vec![0xFFu8; 5];
        let mut v2 = vec![0u8]; // tag 0
        short_bytes(&mut v2, &addr2);
        v2.push(0);
        varuint(&mut v2, 1);

        // Entry 3: tag 1, enterprise-script address, one asset, hash datum.
        let addr3 = {
            let mut a = vec![0x71u8]; // header: enterprise script, testnet
            a.extend(std::iter::repeat_n(0x22u8, 28));
            a
        };
        let policy = [0x33u8; 28];
        let name = b"TOKEN".to_vec();
        let qty = 7u64;
        let mut rep = Vec::new();
        rep.extend_from_slice(&qty.to_le_bytes()); // n=1 quantities
        rep.extend_from_slice(&0u16.to_le_bytes()); // policy offset
        rep.extend_from_slice(&28u16.to_le_bytes()); // name offset
        rep.extend_from_slice(&policy);
        rep.extend_from_slice(&name);
        let mut v3 = vec![1u8]; // tag 1
        short_bytes(&mut v3, &addr3);
        v3.push(1); // compact value tag 1 (coin + multiasset)
        varuint(&mut v3, 2_000_000);
        varuint(&mut v3, 1); // asset count
        short_bytes(&mut v3, &rep);
        v3.extend_from_slice(&[0xABu8; 32]); // hash datum (fixed 32 bytes, no length prefix)

        // Entry 4: tag 5, enterprise-key address, inline datum + native script.
        let addr4 = {
            let mut a = vec![0x61u8];
            a.extend(std::iter::repeat_n(0x44u8, 28));
            a
        };
        let inline_bytes = vec![0xCDu8; 3];
        let native_script = vec![0xEFu8; 4];
        let mut v4 = vec![5u8]; // tag 5
        short_bytes(&mut v4, &addr4);
        v4.push(0);
        varuint(&mut v4, 3_000_000);
        v4.push(2); // datum tag 2: inline
        short_bytes(&mut v4, &inline_bytes);
        v4.push(0); // script tag 0: native
        short_bytes(&mut v4, &native_script);

        let mut txid = [0u8; 32];

        let mut out = vec![0x81]; // array(1)
        out.push(0xa4); // map(4), definite length (simpler than indefinite for this test)
        for (i, v) in [v1, v2, v3, v4].into_iter().enumerate() {
            txid[0] = i as u8 + 1;
            let mut key = [0u8; tvar::KEY_LEN];
            key[..32].copy_from_slice(&txid);
            key[32..].copy_from_slice(&0u16.to_be_bytes());
            cbor_bytes(&mut out, &key);
            cbor_bytes(&mut out, &v);
        }
        out
    }

    #[test]
    fn stream_into_decodes_each_shape_and_skips_unparseable_addresses() {
        let mut conn = fresh_db();
        let anchor = (1_000u64, [0x99u8; 32]);
        let stats = stream_into(synthetic_tvar().as_slice(), &mut conn, anchor).unwrap();

        assert_eq!(stats.entries, 4);
        assert_eq!(
            stats.skipped, 1,
            "the unparseable-address entry must be skipped, not errored"
        );
        assert_eq!(stats.inserted, 3);

        assert_eq!(store::tip(&conn).unwrap(), Some(anchor));

        let count: u64 = conn
            .query_row("SELECT COUNT(*) FROM utxo", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 3);

        // Entry 3: multiasset + hash datum.
        let row: (i64, Option<Vec<u8>>, i64, Option<Vec<u8>>) = conn
            .query_row(
                "SELECT coin, assets, datum_kind, datum_hash FROM utxo WHERE tx_hash = ?1",
                [vec![3u8; 1]
                    .into_iter()
                    .chain(std::iter::repeat_n(0u8, 31))
                    .collect::<Vec<u8>>()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(row.0, 2_000_000);
        assert!(
            row.1.is_some(),
            "multiasset output must carry an assets blob"
        );
        assert_eq!(row.2, DatumKind::Hash as i64);
        assert_eq!(row.3, Some(vec![0xABu8; 32]));

        // Entry 4: inline datum + native reference script, both resolvable.
        let tx4: Vec<u8> = vec![4u8; 1]
            .into_iter()
            .chain(std::iter::repeat_n(0u8, 31))
            .collect();
        let (datum_hash, script_hash): (Vec<u8>, Vec<u8>) = conn
            .query_row(
                "SELECT datum_hash, script_hash FROM utxo WHERE tx_hash = ?1",
                [&tx4],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        let datum_row: Vec<u8> = conn
            .query_row(
                "SELECT cbor FROM datum WHERE hash = ?1",
                [&datum_hash],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(datum_row, vec![0xCDu8; 3]);
        let (script_cbor, script_lang): (Vec<u8>, i64) = conn
            .query_row(
                "SELECT cbor, lang FROM script WHERE hash = ?1",
                [&script_hash],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(script_cbor, vec![0xEFu8; 4]);
        assert_eq!(script_lang, ScriptLang::Native as i64);
    }

    #[test]
    fn run_is_crash_safe_and_refuses_a_second_completed_run() {
        let mut conn = fresh_db();
        let anchor = (1_000u64, [0x99u8; 32]);

        // Simulate a crashed first attempt: some rows landed, but `meta.seed`
        // was never marked done. `utxo.tx_hash` is a real FK now, so the
        // stale row needs a `block`/`tx` row behind it too.
        store::insert_block_row(
            &conn,
            &BlockMeta {
                slot: 1,
                hash: [0u8; 32],
                prev_hash: [0u8; 32],
                block_no: None,
            },
        )
        .unwrap();
        store::insert_tx(&conn, 1, &[0x7fu8; 32]).unwrap();
        store::insert_utxo(
            &conn,
            1,
            &Utxo {
                output_ref: OutputRef {
                    tx_hash: [0x7fu8; 32],
                    ix: 0,
                },
                pay_hash: [0x61; 28],
                stake_hash: None,
                address_header: 0x61,
                coin: 1,
                assets: None,
                datum_kind: DatumKind::None,
                datum_hash: None,
                script_hash: None,
            },
        )
        .unwrap();
        assert!(!is_done(&conn).unwrap());

        let stats = run(synthetic_tvar().as_slice(), &mut conn, anchor).unwrap();
        assert_eq!(stats.inserted, 3);
        assert!(is_done(&conn).unwrap());

        // The stale pre-crash row must be gone, replaced entirely by this run.
        let count: u64 = conn
            .query_row("SELECT COUNT(*) FROM utxo", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 3);

        // A second run must refuse rather than silently redo the wipe.
        assert!(run(synthetic_tvar().as_slice(), &mut conn, anchor).is_err());
    }

    #[test]
    fn running_catchup_engine_continues_correctly_after_a_seed() {
        use crate::ingest::catchup::Engine;

        let mut conn = fresh_db();
        let anchor = (1_000u64, [0x99u8; 32]);
        run(synthetic_tvar().as_slice(), &mut conn, anchor).unwrap();

        // The normal post-seed handoff: a fresh catch-up Engine picks up the
        // seed's checkpoint as its continuity anchor, exactly as
        // `sync::apply_loop` does on every (re)start.
        let mut engine = Engine::new(store::tip(&conn).unwrap());
        let next = BlockMeta {
            slot: 1_001,
            hash: [0x01; 32],
            prev_hash: anchor.1,
            block_no: Some(1_001),
        };
        engine
            .apply_block(next, &crate::ingest::decode::DecodedBlock::default())
            .expect("the block after the anchor must chain onto the seed's checkpoint");

        let bad = BlockMeta {
            slot: 1_001,
            hash: [0x02; 32],
            prev_hash: [0u8; 32],
            block_no: Some(1_001),
        };
        let mut engine2 = Engine::new(store::tip(&conn).unwrap());
        assert!(
            engine2
                .apply_block(bad, &crate::ingest::decode::DecodedBlock::default())
                .is_err()
        );
    }
}
