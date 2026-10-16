//! sync: chainsync + blockfetch client, wired to `decode`, `store` and
//! `catchup`.

use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::{Point, chainsync::NextResponse};
use pallas_traverse::{MultiEraBlock, MultiEraHeader};
use rusqlite::Connection;
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

use crate::catchup::{self, ChunkConfig};
use crate::decode::{self, DecodedBlock};
use crate::health::Health;
use crate::store::{self, BlockMeta, Mode};

/// jemalloc only returns a chunk to the OS once it's entirely free. Catch-up's
/// chunk buffer (see `catchup::ChunkConfig`, up to 256MiB) spreads its
/// allocations across many chunks; running mode's own small long-lived
/// allocations (e.g. per-request SQLite connections) can end up pinning a
/// few slots in each one indefinitely, so none of them are ever fully free
/// and none get returned, even once the live working set has shrunk back
/// down. The catchup -> running switch is the one moment we know a large
/// one-off burst just ended and won't recur at that scale, so it's safe to
/// nudge jemalloc to purge here rather than wait for its own decay/fragmentation
/// heuristics, which may never fire for chunks pinned this way.
fn jemalloc_purge_all_arenas() {
    // `arena.<i>.purge` is a void, write-only mallctl (triggered by the call
    // itself, nothing is actually read or written); `4096` is jemalloc's
    // `MALLCTL_ARENAS_ALL` sentinel, meaning "every arena".
    if let Err(e) = unsafe { tikv_jemalloc_ctl::raw::write(b"arena.4096.purge\0", ()) } {
        warn!(error = %e, "jemalloc arena purge failed");
    }
}

/// `block_no` and `peer_tip_block_no` are what the apply side uses to
/// decide mode per block: `depth = peer_tip.block_no - blk.block_no`.
pub enum Event {
    Apply {
        meta: BlockMeta,
        decoded: Box<DecodedBlock>,
        block_no: u64,
        peer_tip_block_no: u64,
    },
    Rollback {
        slot: u64,
    },
}

/// Catch-up while `depth > k + margin`; mainnet k = 2160. `batch_size`
/// bounds how many headers `fetch_loop` buffers per `fetch_range` call.
/// `prune_interval` is how often (in applied blocks) running mode calls
/// `store::prune(conn, k)`.
#[derive(Debug, Clone, Copy)]
pub struct ModeConfig {
    pub k: u64,
    pub margin: u64,
    pub batch_size: usize,
    pub prune_interval: u64,
}

impl ModeConfig {
    pub fn catchup_depth(&self) -> u64 {
        self.k + self.margin
    }
}

fn depth(peer_tip_block_no: u64, block_no: u64) -> u64 {
    peer_tip_block_no.saturating_sub(block_no)
}

impl Default for ModeConfig {
    fn default() -> Self {
        Self {
            k: 2160,
            margin: 500,
            batch_size: 100,
            prune_interval: 100,
        }
    }
}

/// Decoded just enough to fetch and apply its body later; the blockfetch
/// call is deferred so headers can be buffered into a batch.
struct PendingHeader {
    point: Point,
    meta: BlockMeta,
    block_no: u64,
}

fn decode_header(
    h: pallas_network::miniprotocols::chainsync::HeaderContent,
) -> Result<PendingHeader> {
    let hdr = MultiEraHeader::decode(h.variant, h.byron_prefix.map(|p| p.0), &h.cbor)?;
    let point = Point::Specific(hdr.slot(), hdr.hash().to_vec());
    let prev_hash = hdr
        .previous_hash()
        .ok_or_else(|| anyhow!("header at slot {} has no previous_hash", hdr.slot()))?;
    let meta = BlockMeta {
        slot: hdr.slot(),
        hash: (*hdr.hash().as_ref()).try_into().expect("32-byte hash"),
        prev_hash: (*prev_hash.as_ref()).try_into().expect("32-byte hash"),
        block_no: Some(hdr.number()),
    };
    Ok(PendingHeader {
        point,
        meta,
        block_no: hdr.number(),
    })
}

/// `fetch_single` for one header, `fetch_range` for more; decodes in
/// parallel, then sends one `Event::Apply` per block in order. Returns
/// `true` if `tx`'s receiver is gone and the caller should stop.
#[instrument(skip(peer, headers, tx), fields(n = headers.len(), peer_tip_block_no))]
async fn flush_pending(
    peer: &mut PeerClient,
    headers: Vec<PendingHeader>,
    peer_tip_block_no: u64,
    tx: &mpsc::Sender<Event>,
) -> Result<bool> {
    if headers.is_empty() {
        return Ok(false);
    }

    let bodies: Vec<Vec<u8>> = if headers.len() == 1 {
        vec![
            peer.blockfetch()
                .fetch_single(headers[0].point.clone())
                .await?,
        ]
    } else {
        let first = headers
            .first()
            .expect("checked non-empty above")
            .point
            .clone();
        let last = headers
            .last()
            .expect("checked non-empty above")
            .point
            .clone();
        peer.blockfetch().fetch_range((first, last)).await?
    };
    anyhow::ensure!(
        bodies.len() == headers.len(),
        "blockfetch returned {} bodies for {} requested headers",
        bodies.len(),
        headers.len()
    );

    let blocks: Vec<MultiEraBlock> = bodies
        .iter()
        .map(|b| MultiEraBlock::decode(b))
        .collect::<Result<_, _>>()?;
    let decoded_all = decode::decode_blocks_parallel(&blocks);

    for (h, decoded) in headers.into_iter().zip(decoded_all) {
        let ev = Event::Apply {
            meta: h.meta,
            decoded: Box::new(decoded),
            block_no: h.block_no,
            peer_tip_block_no,
        };
        if tx.send(ev).await.is_err() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `IntersectNotFound` is a valid protocol response (see
/// `pallas_network`'s `IntersectResponse`, `(Option<Point>, Tip)`), not
/// something `find_intersect` itself turns into an `Err` -- it's the
/// routine case right after the peer restarts: unlike a `cardano-node`
/// that's always near its own live tip, amaru persists state and
/// resumes from an earlier point in its own history, then chainsyncs
/// back up to where it left off from its own peers. Our resume point
/// (floe's last checkpoint) can easily be ahead of that for a while.
/// A failed intersect doesn't move the peer's chainsync read pointer,
/// so proceeding past it regardless (as this used to) meant the peer
/// then served whatever its own earliest known point was instead of
/// ours -- for amaru specifically, that's its own bootstrap snapshot
/// (the same archive floe's `--seed` is built from), so the very first
/// block received was from there, mismatching our real tip and
/// tripping `store::apply`'s continuity check.
const INTERSECT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
/// ~2 minutes at `INTERSECT_RETRY_DELAY` -- long enough for a peer to
/// chainsync back up to a reasonably recent point after a restart, not
/// so long that a genuinely wrong/unreachable resume point hangs
/// silently forever.
const MAX_INTERSECT_RETRIES: u32 = 60;

/// Retries `find_intersect` against the given point until the peer
/// confirms it (or gives up -- see `MAX_INTERSECT_RETRIES`'s doc
/// comment, above).
async fn find_intersect_with_retry(peer: &mut PeerClient, point: &Point) -> Result<()> {
    for attempt in 1..=MAX_INTERSECT_RETRIES {
        let (found, _tip) = peer.chainsync().find_intersect(vec![point.clone()]).await?;
        if found.is_some() {
            return Ok(());
        }
        warn!(
            ?point,
            attempt,
            "intersect not found -- peer hasn't caught up to our resume point yet; retrying"
        );
        tokio::time::sleep(INTERSECT_RETRY_DELAY).await;
    }
    bail!(
        "peer never caught up to our resume point {point:?} after {MAX_INTERSECT_RETRIES} attempts"
    );
}

/// Pushes `Event`s into `tx` in chain order until the channel closes or an
/// error occurs. While still deep, headers are batched up to
/// `cfg.batch_size` per `fetch_range` call; near the tip, each block is
/// fetched as soon as its header arrives, with no buffering delay.
#[instrument(skip(point, tx, cfg), fields(addr, magic))]
pub async fn fetch_loop(
    addr: &str,
    magic: u64,
    point: Point,
    tx: mpsc::Sender<Event>,
    cfg: ModeConfig,
) -> Result<()> {
    let mut peer = PeerClient::connect(addr, magic).await?;
    find_intersect_with_retry(&mut peer, &point).await?;

    let mut pending: Vec<PendingHeader> = Vec::new();

    loop {
        match peer.chainsync().request_or_await_next().await? {
            NextResponse::RollBackward(p, _tip) => {
                // Any buffered-but-not-yet-fetched headers are upstream of
                // nothing we've emitted, so they're simply discarded.
                pending.clear();
                let slot = match p {
                    Point::Origin => 0,
                    Point::Specific(slot, _) => slot,
                };
                if tx.send(Event::Rollback { slot }).await.is_err() {
                    return Ok(());
                }
            }
            NextResponse::Await => continue,
            NextResponse::RollForward(h, tip) => {
                let header = decode_header(h)?;
                let depth = depth(tip.1, header.block_no);
                let still_deep = depth > cfg.catchup_depth();

                pending.push(header);
                if !still_deep || pending.len() >= cfg.batch_size {
                    let batch = std::mem::take(&mut pending);
                    if flush_pending(&mut peer, batch, tip.1, &tx).await? {
                        return Ok(());
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ApplyStats {
    pub applied: u64,
    pub rolled_back: u64,
    pub last_block_no: u64,
    pub peer_tip_block_no: u64,
}

/// Routes each block through the catch-up chunk buffer or `store::apply`
/// per current mode, switching over by depth as needed. Owning `rx` means
/// an early return here drops it, which is how `fetch_loop`'s next send
/// fails and lets it return gracefully. A clean stop flushes any buffered
/// catch-up chunk, so only a crash pays for a re-fetch.
#[instrument(skip(conn, rx, health, cfg), fields(max_blocks, k = cfg.k))]
pub fn apply_loop(
    conn: &mut Connection,
    mut rx: mpsc::Receiver<Event>,
    max_blocks: Option<u64>,
    cfg: ModeConfig,
    health: Option<&Health>,
) -> Result<ApplyStats> {
    let mut stats = ApplyStats::default();
    let mut mode = store::get_mode(conn)?;
    let mut engine =
        (mode == Mode::Catchup).then(|| catchup::Engine::new(store::tip(conn).unwrap()));
    let chunk_cfg = ChunkConfig::default();
    let mut blocks_since_prune = 0u64;
    let mut blocks_since_progress_log = 0u64;
    const CATCHUP_PROGRESS_INTERVAL: u64 = 1000;

    while let Some(ev) = rx.blocking_recv() {
        match ev {
            Event::Apply {
                meta,
                decoded,
                block_no,
                peer_tip_block_no,
            } => {
                let depth = depth(peer_tip_block_no, block_no);

                if mode == Mode::Catchup && depth <= cfg.catchup_depth() {
                    let mut eng = engine.take().expect("engine present while mode is Catchup");
                    eng.switch_to_running(conn)?;
                    mode = Mode::Running;
                    info!(
                        slot = meta.slot,
                        depth, "caught up, switched to running mode"
                    );
                    jemalloc_purge_all_arenas();
                }

                match mode {
                    Mode::Catchup => {
                        let eng = engine
                            .as_mut()
                            .expect("engine present while mode is Catchup");
                        eng.apply_block(meta, &decoded)?;
                        if eng.should_flush(&chunk_cfg) {
                            eng.flush(conn)?;
                        }
                        blocks_since_progress_log += 1;
                        if blocks_since_progress_log >= CATCHUP_PROGRESS_INTERVAL {
                            info!(
                                block_no,
                                depth,
                                peer_tip_block_no,
                                pending_blocks = eng.pending_blocks(),
                                pending_bytes = eng.pending_bytes(),
                                "catch-up progress"
                            );
                            blocks_since_progress_log = 0;
                        }
                    }
                    Mode::Running => {
                        store::apply(conn, &meta, &decoded)?;
                        blocks_since_prune += 1;
                        if blocks_since_prune >= cfg.prune_interval {
                            store::prune(conn, cfg.k)?;
                            blocks_since_prune = 0;
                        }
                    }
                }

                stats.applied += 1;
                stats.last_block_no = block_no;
                stats.peer_tip_block_no = peer_tip_block_no;
                if let Some(h) = health {
                    h.record_apply(
                        mode.into(),
                        meta.slot,
                        block_no,
                        meta.hash,
                        peer_tip_block_no,
                        decoded.created.len() as u64,
                    );
                }
            }
            Event::Rollback { slot } => {
                match mode {
                    // Catch-up blocks are already deeper than k+margin, so
                    // no real rollback can reach them; the only
                    // RollBackward catch-up sees is the initial
                    // find_intersect correction.
                    Mode::Catchup => {}
                    Mode::Running => {
                        warn!(to_slot = slot, "rollback");
                        store::rollback(conn, slot)?;
                    }
                }
                stats.rolled_back += 1;
                if let Some(h) = health {
                    h.record_rollback();
                }
            }
        }
        if max_blocks.is_some_and(|max| stats.applied >= max) {
            break;
        }
    }

    if let Some(mut eng) = engine {
        eng.flush(conn)?;
    }
    Ok(stats)
}

/// Runs `fetch_loop` and `apply_loop` concurrently (fetch on a tokio task,
/// apply on a blocking task, connected by a bounded channel) until
/// `max_blocks` applies have happened or the peer connection ends.
#[instrument(skip(point, conn, health, cfg), fields(addr, magic))]
pub async fn run(
    addr: String,
    magic: u64,
    point: Point,
    mut conn: Connection,
    max_blocks: Option<u64>,
    cfg: ModeConfig,
    health: Option<Arc<Health>>,
) -> Result<(ApplyStats, Connection)> {
    let (tx, rx) = mpsc::channel(32);

    let fetch_task = tokio::spawn(async move { fetch_loop(&addr, magic, point, tx, cfg).await });
    let stats = tokio::task::spawn_blocking(move || {
        let stats = apply_loop(&mut conn, rx, max_blocks, cfg, health.as_deref());
        stats.map(|s| (s, conn))
    })
    .await??;

    // Dropping rx above (apply_loop returning) makes fetch_loop's next send
    // fail and return Ok(()); if it errored instead, surface that here.
    fetch_task.await??;
    Ok(stats)
}

/// Returns the number of unspent UTxOs (the tip set).
pub fn utxo_count(conn: &Connection) -> Result<u64> {
    store::utxo_count(conn)
}
