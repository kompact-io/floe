//! sync: chainsync + blockfetch client, wired to `decode`, `store` and
//! `catchup`.
//!
//! Chain-sync and block-fetch alike go entirely through `floe_n2n`'s
//! amaru-protocols/amaru-pure-stage based client (real CIP-0164 pipelining,
//! which pallas-network's lock-step `Client` cannot do -- see
//! `plan-amaru-migration.md`). `pallas-network` is no longer a dependency of
//! `floe-server`'s production path at all: the one place it used to survive,
//! `fetch_header_via_blockfetch` (a one-shot bootstrap used only when
//! there's no cached resume header yet), now goes through
//! `floe_n2n::single_fetch::fetch_block` -- a blockfetch-only dial that
//! skips chain-sync entirely, since chain-sync has no equivalent bootstrap
//! (it needs a `ChainStore` resume point to even start, which is exactly
//! what's missing here).

use std::net::ToSocketAddrs;
use std::sync::Arc;

use amaru_kernel::cardano::block::Block;
use amaru_kernel::cardano::header::Header;
use amaru_kernel::{IsHeader, NetworkMagic, Point};
use anyhow::{Result, anyhow, bail};
use floe_n2n::client::Client;
use floe_n2n::pipeline::HeaderEvent;
use rusqlite::Connection;
use tokio::sync::mpsc;
use tracing::{info, instrument, warn};

use crate::health::Health;
use crate::ingest::catchup::{self, ChunkConfig};
use crate::ingest::decode::{self, DecodedBlock};
use crate::ingest::store::{self, BlockMeta, Mode};

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
        /// This block's own header, re-encoded CBOR -- cached via
        /// `store::set_resume_header` once applied, so a restart can resume
        /// chain-sync from here without `fetch_header_via_blockfetch`.
        header_cbor: Vec<u8>,
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
    header_cbor: Vec<u8>,
}

/// `IntersectNotFound` is a valid protocol response, not something resuming
/// treats as an error -- it's the routine case right after the peer
/// restarts: unlike a `cardano-node` that's always near its own live tip,
/// amaru persists state and resumes from an earlier point in its own
/// history, then chainsyncs back up to where it left off from its own
/// peers. Our resume point (floe's last checkpoint) can easily be ahead of
/// that for a while. `Client::connect_with_retry` (not this module) retries
/// by reconnecting from scratch -- `amaru-protocols`' chain-sync initiator
/// has no "retry `FindIntersect` on this same connection" primitive, unlike
/// `pallas_network`'s `Client`.
const INTERSECT_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);
/// ~2 minutes at `INTERSECT_RETRY_DELAY` -- long enough for a peer to
/// chainsync back up to a reasonably recent point after a restart, not
/// so long that a genuinely wrong/unreachable resume point hangs
/// silently forever.
const MAX_INTERSECT_RETRIES: u32 = 60;

/// One-shot bootstrap: fetch the header at `(slot, hash)` via
/// `floe_n2n::single_fetch` (chain-sync never hands back the intersection
/// point's own content, only blocks after it -- see `floe_n2n`'s
/// `dial_peer` example, which uses the identical pattern). Only called
/// when `store::get_resume_header` found nothing cached.
async fn fetch_header_via_blockfetch(
    addr: &str,
    magic: u64,
    slot: u64,
    hash: [u8; 32],
) -> Result<Header> {
    let socket_addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("could not resolve {addr}"))?;
    let point = Point::Specific(slot.into(), hash.into(), 0u64.into());
    let block =
        floe_n2n::single_fetch::fetch_block(socket_addr, NetworkMagic::from(magic), point).await?;
    Ok(block.header)
}

/// Where to resume chain-sync from, resolved once before the fetch loop
/// starts.
pub(crate) enum Resume {
    Header(Box<Header>),
    Origin,
}

/// Prefers the cached header (`store::get_resume_header`); falls back to
/// `fetch_header_via_blockfetch` using `store::tip`'s `(slot, hash)` if
/// nothing's cached yet; `Resume::Origin` only for a DB with no tip at all (no
/// `--seed`, never synced).
async fn resolve_resume(addr: &str, magic: u64, conn: &Connection) -> Result<Resume> {
    if let Some(bytes) = store::get_resume_header(conn)? {
        let header: Header = amaru_kernel::cbor::decode(&bytes)?;
        return Ok(Resume::Header(Box::new(header)));
    }
    match store::tip(conn)? {
        None => Ok(Resume::Origin),
        Some((slot, hash)) => {
            info!(
                slot,
                hash = hex::encode(hash),
                "no cached resume header -- fetching one via floe_n2n::single_fetch"
            );
            let header = fetch_header_via_blockfetch(addr, magic, slot, hash).await?;
            Ok(Resume::Header(Box::new(header)))
        }
    }
}

/// `fetch_single`/`fetch_range`-equivalent via `Client::fetch_bodies`;
/// decodes in parallel, then sends one `Event::Apply` per block in order.
/// Returns `true` if `tx`'s receiver is gone and the caller should stop.
#[instrument(skip(client, headers, tx), fields(n = headers.len(), peer_tip_block_no))]
async fn flush_pending(
    client: &mut Client,
    headers: Vec<PendingHeader>,
    peer_tip_block_no: u64,
    tx: &mpsc::Sender<Event>,
) -> Result<bool> {
    if headers.is_empty() {
        return Ok(false);
    }

    let from = headers.first().expect("checked non-empty above").point;
    let through = headers.last().expect("checked non-empty above").point;
    let network_blocks = client.fetch_bodies(from, through).await?;
    anyhow::ensure!(
        network_blocks.len() == headers.len(),
        "block-fetch returned {} bodies for {} requested headers",
        network_blocks.len(),
        headers.len()
    );

    let blocks: Vec<Block> = network_blocks
        .iter()
        .map(|b| b.decode_block())
        .collect::<Result<_, _>>()?;
    let decoded_all = decode::decode_blocks_parallel(&blocks);

    for (h, decoded) in headers.into_iter().zip(decoded_all) {
        let ev = Event::Apply {
            meta: h.meta,
            decoded: Box::new(decoded),
            block_no: h.block_no,
            peer_tip_block_no,
            header_cbor: h.header_cbor,
        };
        if tx.send(ev).await.is_err() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Pushes `Event`s into `tx` in chain order until the channel closes or an
/// error occurs. While still deep, headers are batched up to
/// `cfg.batch_size` per `fetch_bodies` call; near the tip, each block is
/// fetched as soon as its header arrives, with no buffering delay.
#[instrument(skip(resume, tx, cfg), fields(addr, magic))]
pub(crate) async fn fetch_loop(
    addr: &str,
    magic: u64,
    resume: Resume,
    tx: mpsc::Sender<Event>,
    cfg: ModeConfig,
) -> Result<()> {
    let resume_header = match &resume {
        Resume::Header(h) => Some(h.as_ref()),
        Resume::Origin => None,
    };
    let socket_addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("could not resolve {addr}"))?;
    let mut client = Client::connect_with_retry(
        socket_addr,
        NetworkMagic::from(magic),
        resume_header,
        INTERSECT_RETRY_DELAY,
        MAX_INTERSECT_RETRIES,
    )
    .await?;

    let mut pending: Vec<PendingHeader> = Vec::new();

    loop {
        match client.next_header().await? {
            HeaderEvent::RollBackward { point, .. } => {
                // Any buffered-but-not-yet-fetched headers are upstream of
                // nothing we've emitted, so they're simply discarded.
                pending.clear();
                let slot = point.slot_or_default().as_u64();
                if tx.send(Event::Rollback { slot }).await.is_err() {
                    return Ok(());
                }
            }
            HeaderEvent::RollForward {
                header,
                tip_block_no,
            } => {
                let point = floe_n2n::resume::point_of(&header);
                let slot = point.slot().as_u64();
                let hash: [u8; 32] = *header.hash();
                let prev_hash: [u8; 32] = *header
                    .parent_hash()
                    .ok_or_else(|| anyhow!("header at slot {slot} has no previous_hash"))?;
                let block_no = header.block_height().as_u64();
                let header_cbor = amaru_kernel::cbor::to_vec(&*header)?;
                let meta = BlockMeta {
                    slot,
                    hash,
                    prev_hash,
                    block_no: Some(block_no),
                };

                let depth = depth(tip_block_no, block_no);
                let still_deep = depth > cfg.catchup_depth();

                pending.push(PendingHeader {
                    point,
                    meta,
                    block_no,
                    header_cbor,
                });
                if !still_deep || pending.len() >= cfg.batch_size {
                    let batch = std::mem::take(&mut pending);
                    if flush_pending(&mut client, batch, tip_block_no, &tx).await? {
                        return Ok(());
                    }
                }
            }
            HeaderEvent::IntersectNotFound { .. } => {
                bail!("Client::connect_with_retry should have already resolved this")
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
                header_cbor,
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
                        eng.apply_block(meta, &decoded, Some(&header_cbor))?;
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
                        store::apply(conn, &meta, &decoded, Some(&header_cbor))?;
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
                        mode,
                        meta.slot,
                        block_no,
                        meta.hash,
                        peer_tip_block_no,
                        decoded.created.len() as u64,
                        decoded.spent.len() as u64,
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
#[instrument(skip(conn, health, cfg), fields(addr, magic))]
pub async fn run(
    addr: String,
    magic: u64,
    mut conn: Connection,
    max_blocks: Option<u64>,
    cfg: ModeConfig,
    health: Option<Arc<Health>>,
) -> Result<(ApplyStats, Connection)> {
    let (tx, rx) = mpsc::channel(32);

    // Resolve the resume point (cached header, bootstrap fetch, or Origin)
    // up front, on `conn`, before it moves into the blocking apply task
    // below -- `fetch_loop` itself never touches the DB.
    let resume = resolve_resume(&addr, magic, &conn).await?;

    // `Client::connect_with_retry`'s future isn't `Send` (amaru-pure-stage
    // internals), so it can't be `tokio::spawn`'d like `fetch_loop` used to
    // be with pallas-network. Run it directly on this task instead, joined
    // with the (separately-spawned, since it's blocking) apply task --
    // dropping `rx` (apply_loop returning) still makes fetch_loop's next
    // send fail and return `Ok(())`, same as before.
    let apply_task = tokio::task::spawn_blocking(move || {
        let stats = apply_loop(&mut conn, rx, max_blocks, cfg, health.as_deref());
        stats.map(|s| (s, conn))
    });
    let fetch_fut = fetch_loop(&addr, magic, resume, tx, cfg);

    let (apply_result, fetch_result) = tokio::join!(apply_task, fetch_fut);
    let stats = apply_result??;
    fetch_result?;
    Ok(stats)
}
