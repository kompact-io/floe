//! Facade over `connection` + `pipeline`: a single-peer client. Caller
//! drives its own fetch policy (batch size, depth) -- see `pipeline`'s
//! docs. Integration point for `floe`'s `ingest/sync.rs`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use amaru_kernel::cardano::network_block::NetworkBlock;
use amaru_kernel::{Header, NetworkMagic, Peer, Point};
use amaru_network::connection::TokioConnections;
use amaru_ouroboros::{ConnectionProvider, ConnectionsResource};
use amaru_protocols::blockfetch::Blocks;
use amaru_protocols::connection::ConnectionMessage;
use amaru_protocols::store_effects::ResourceHeaderStore;
use amaru_pure_stage::tokio::{TokioBuilder, TokioRunning};
use amaru_pure_stage::{Receiver, Sender, StageGraph};
use futures_util::StreamExt;

use crate::Result;
use crate::error::Error;
use crate::pipeline::HeaderEvent;
use crate::resume::ResumeFrom;
use crate::{connection, pipeline, resume};

/// How long one connect-and-intersect attempt may take before giving up on
/// it (not the whole retry budget -- see [`Client::connect_with_retry`]).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Client {
    headers: Receiver<HeaderEvent>,
    pending_first: Option<HeaderEvent>,
    /// Sends `ConnectionMessage::FetchBlocks` straight to `connection`'s
    /// mailbox, not `pipeline`'s -- see `connection`'s module docs for why.
    fetch: Sender<ConnectionMessage>,
    blocks_out: amaru_pure_stage::StageRef<Blocks>,
    blocks: Receiver<Blocks>,
    next_fetch_id: u64,
    _running: TokioRunning,
}

impl Client {
    /// Dial `peer`, resuming chain-sync from `resume_header`'s own point
    /// (see `resume::seed_chain_store`). One attempt, no retry -- see
    /// [`Self::connect_with_retry`] for the retry behavior floe's real sync
    /// loop wants.
    pub async fn connect(peer: Peer, magic: NetworkMagic, resume: ResumeFrom<'_>) -> Result<Self> {
        let chain_store = resume::seed_chain_store(resume)?;
        let connections = Arc::new(TokioConnections::new(64 * 1024));
        let conn_id = connections.connect(peer, CONNECT_TIMEOUT).await?;

        let mut network = TokioBuilder::default();
        network.resources().put::<ConnectionsResource>(connections);
        network.resources().put::<ResourceHeaderStore>(chain_store);

        let (headers_ref, headers_rx) = network.output::<HeaderEvent>("headers", 64);
        let (blocks_ref, blocks_rx) = network.output::<Blocks>("blocks", 64);

        let pipeline_stage = network.stage("pipeline", pipeline::stage);
        let pipeline_ref = network
            .wire_up(pipeline_stage, pipeline::Pipeline::new(headers_ref))
            .without_state();

        let connection_stage = network.stage("connection", connection::stage);
        let connection_ref = network.wire_up(
            connection_stage,
            connection::Connection::new(connection::Params {
                peer,
                conn_id,
                magic,
                pipeline: pipeline_ref,
            }),
        );

        // Kept, not a one-shot send: `fetch_bodies` reuses this for every
        // later `FetchBlocks` too.
        let fetch = network.input(&connection_ref);
        let running = network.run(tokio::runtime::Handle::current());
        fetch
            .send(ConnectionMessage::Initialize)
            .await
            .map_err(|_| Error::StageGone("connection"))?;

        Ok(Self {
            headers: headers_rx,
            pending_first: None,
            fetch,
            blocks_out: blocks_ref,
            blocks: blocks_rx,
            next_fetch_id: 0,
            _running: running,
        })
    }

    /// Like [`Self::connect`], but retries on [`HeaderEvent::IntersectNotFound`]
    /// by reconnecting from scratch -- no "resend `FindIntersect`" primitive
    /// here (unlike `pallas_network`'s `Client`).
    pub async fn connect_with_retry(
        addr: SocketAddr,
        magic: NetworkMagic,
        resume_header: Option<&Header>,
        retry_delay: Duration,
        max_retries: u32,
    ) -> Result<Self> {
        let peer = Peer::try_from(addr)?;
        let resume = || match resume_header {
            Some(h) => ResumeFrom::Header(h),
            None => ResumeFrom::Origin,
        };
        let mut last_tip_block_no = 0;
        for attempt in 1..=max_retries {
            let mut client = Self::connect(peer, magic, resume()).await?;
            match client.next_header().await? {
                HeaderEvent::IntersectNotFound { tip_block_no } => {
                    last_tip_block_no = tip_block_no;
                    tracing::warn!(
                        attempt,
                        tip_block_no,
                        "intersect not found -- peer hasn't caught up to our resume point yet; reconnecting"
                    );
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }
                other => return Ok(client.with_first_header(other)),
            }
        }
        Err(Error::NeverCaughtUp {
            attempts: max_retries,
            peer_tip_block_no: last_tip_block_no,
        })
    }

    /// Re-queues `event` so the next [`Self::next_header`] call returns it --
    /// used by [`Self::connect_with_retry`] to not drop the first real event
    /// it had to peek at to check for `IntersectNotFound`.
    fn with_first_header(mut self, event: HeaderEvent) -> Self {
        self.pending_first = Some(event);
        self
    }

    /// Next chain-sync event. Pipelining (keeping multiple `RequestNext`s
    /// outstanding) happens transparently underneath; this just drains the
    /// buffered results in order.
    pub async fn next_header(&mut self) -> Result<HeaderEvent> {
        if let Some(event) = self.pending_first.take() {
            return Ok(event);
        }
        self.headers.next().await.ok_or(Error::ConnectionClosed)
    }

    /// Request bodies for the (inclusive) point range `from..=through` and
    /// wait for all of them. The caller decides the range -- this crate has
    /// no batching policy of its own.
    pub async fn fetch_bodies(&mut self, from: Point, through: Point) -> Result<Vec<NetworkBlock>> {
        let id = self.next_fetch_id;
        self.next_fetch_id += 1;
        self.fetch
            .send(ConnectionMessage::FetchBlocks {
                from,
                through,
                id,
                cr: self.blocks_out.clone(),
            })
            .await
            .map_err(|_| Error::StageGone("connection"))?;

        let mut blocks = Vec::new();
        loop {
            match self.blocks.next().await.ok_or(Error::ConnectionClosed)? {
                Blocks::Block(got_id, _peer, block) if got_id == id => blocks.push(block),
                Blocks::Done(got_id) if got_id == id => return Ok(blocks),
                Blocks::NoBlocks(got_id, _peer) if got_id == id => {
                    return Err(Error::NoBlocksInRange { from, through });
                }
                Blocks::NoPeersAvailable(got_id) if got_id == id => {
                    return Err(Error::BlockFetchNotReady);
                }
                // Not for this request -- shouldn't happen (Client only has
                // one fetch in flight at a time), but don't silently hang.
                other => tracing::warn!(?other, "ignoring unexpected block-fetch message"),
            }
        }
    }
}
