//! One-shot block fetch at a known point, skipping chain-sync -- used only
//! when there's no cached resume header yet, since
//! [`Client::connect`](crate::client::Client::connect)'s chain-sync
//! initiator needs a `ChainStore` resume point to even start.
//!
//! Replaces floe's former `fetch_header_via_pallas`, the one remaining
//! `pallas-network` call in `floe-server`'s production path.
//!
//! Registers only the block-fetch initiator (mux + handshake, no
//! chain-sync), requests one point, returns its block.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use amaru_kernel::cardano::block::Block;
use amaru_kernel::{NetworkMagic, Peer, Point};
use amaru_network::connection::TokioConnections;
use amaru_ouroboros::{ConnectionId, ConnectionProvider, ConnectionsResource};
use amaru_protocols::blockfetch::{
    BLOCKFETCH_PIPELINE_N, BlockFetchMessage, Blocks, register_blockfetch_initiator,
};
use amaru_protocols::connection::{ChildId, ConnectionMessage};
use amaru_protocols::handshake;
use amaru_protocols::mux::{self, MuxMessage};
use amaru_protocols::protocol::{Inputs, PROTO_HANDSHAKE, Role, ingress_limit};
use amaru_protocols::protocol_messages::handshake::HandshakeResult;
use amaru_protocols::protocol_messages::version_number::VersionNumber;
use amaru_protocols::protocol_messages::version_table::VersionTable;
use amaru_pure_stage::tokio::TokioBuilder;
use amaru_pure_stage::{Effects, StageGraph, StageRef};
use futures_util::StreamExt;

use crate::Result;
use crate::error::Error;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Params {
    peer: Peer,
    conn_id: ConnectionId,
    magic: NetworkMagic,
    point: Point,
    blocks_out: StageRef<Blocks>,
}

/// Same `params`-owning, `mem::replace`-driven pattern as
/// `connection::Connection` -- see its doc for why.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct SingleFetch {
    params: Params,
    phase: Phase,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum Phase {
    Initial,
    Handshaking { muxer: StageRef<MuxMessage> },
    Established,
}

impl SingleFetch {
    fn new(params: Params) -> Self {
        Self {
            params,
            phase: Phase::Initial,
        }
    }

    async fn handle(&mut self, msg: ConnectionMessage, eff: Effects<ConnectionMessage>) {
        // Placeholder for the swap, immediately overwritten below -- no
        // `Done` variant here (unlike `connection::Phase`) to use instead.
        self.phase = match (std::mem::replace(&mut self.phase, Phase::Established), msg) {
            (Phase::Initial, ConnectionMessage::Initialize) => self.initialize(eff).await,
            (Phase::Handshaking { muxer }, ConnectionMessage::Handshake(result)) => {
                self.handshake(muxer, result, eff).await
            }
            (_, ConnectionMessage::ChildDied(child)) => {
                tracing::error!(?child, "single-fetch connection child died, giving up");
                eff.terminate().await
            }
            (phase, _other) => phase,
        };
    }

    async fn initialize(&mut self, eff: Effects<ConnectionMessage>) -> Phase {
        let muxer = eff
            .stage_with_mailbox_size("mux", mux::stage, mux::MUX_MAILBOX_SIZE)
            .await;
        let muxer = eff.supervise(muxer, ConnectionMessage::ChildDied(ChildId::Mux));
        let muxer = eff
            .wire_up(
                muxer,
                mux::State::new(self.params.conn_id, &[], Role::Initiator, self.params.peer),
            )
            .await;

        let handshake_result: StageRef<HandshakeResult> =
            eff.me_ref().contramap(ConnectionMessage::Handshake);
        let hs = eff.stage("handshake", handshake::initiator()).await;
        let hs = eff.supervise(hs, ConnectionMessage::ChildDied(ChildId::Handshake));
        let version_table = VersionTable::v11_through(
            VersionNumber::CURRENT,
            self.params.magic,
            /* initiator_only_diffusion_mode */ true,
            /* advertisable */ false,
        );
        let hs = eff
            .wire_up(
                hs,
                handshake::HandshakeInitiator::new(muxer.clone(), handshake_result, version_table),
            )
            .await;

        let handler = hs.contramap(Inputs::Network);
        eff.send(
            &muxer,
            MuxMessage::Register {
                protocol: PROTO_HANDSHAKE.erase(),
                frame: mux::Frame::OneCborItem,
                handler,
                max_buffer: ingress_limit(PROTO_HANDSHAKE),
            },
        )
        .await;

        Phase::Handshaking { muxer }
    }

    async fn handshake(
        &mut self,
        muxer: StageRef<MuxMessage>,
        result: HandshakeResult,
        eff: Effects<ConnectionMessage>,
    ) -> Phase {
        match result {
            HandshakeResult::Accepted(..) => {}
            HandshakeResult::Refused(reason) => {
                tracing::error!(?reason, "handshake refused");
                eff.terminate().await
            }
            HandshakeResult::Query(table) => {
                tracing::error!(?table, "peer returned a version query instead of accepting");
                eff.terminate().await
            }
        }

        eff.send(
            &muxer,
            MuxMessage::SetSduTimeout(mux::SDU_TIMEOUT_ESTABLISHED),
        )
        .await;

        let blockfetch_ref = register_blockfetch_initiator(
            &muxer,
            self.params.peer,
            BLOCKFETCH_PIPELINE_N,
            &eff,
            ConnectionMessage::ChildDied(ChildId::BlockFetch),
        )
        .await;

        eff.send(
            &blockfetch_ref,
            BlockFetchMessage::RequestRange {
                from: self.params.point,
                through: self.params.point,
                id: 0,
                cr: self.params.blocks_out.clone(),
            },
        )
        .await;

        Phase::Established
    }
}

// This wrapper simply allows us to follow the convention of amaru-protocols.
async fn stage(
    mut state: SingleFetch,
    msg: ConnectionMessage,
    eff: Effects<ConnectionMessage>,
) -> SingleFetch {
    state.handle(msg, eff).await;
    state
}

/// Dials `addr`, fetches the single block at `point`, decoded.
pub async fn fetch_block(addr: SocketAddr, magic: NetworkMagic, point: Point) -> Result<Block> {
    let peer = Peer::try_from(addr)?;
    let connections = Arc::new(TokioConnections::new(64 * 1024));
    let conn_id = connections.connect(peer, CONNECT_TIMEOUT).await?;

    let mut network = TokioBuilder::default();
    network.resources().put::<ConnectionsResource>(connections);

    let (blocks_ref, mut blocks_rx) = network.output::<Blocks>("blocks", 8);

    let stage_handle = network.stage("single-fetch", stage);
    let stage_ref = network.wire_up(
        stage_handle,
        SingleFetch::new(Params {
            peer,
            conn_id,
            magic,
            point,
            blocks_out: blocks_ref,
        }),
    );

    let input = network.input(&stage_ref);
    let _running = network.run(tokio::runtime::Handle::current());
    input
        .send(ConnectionMessage::Initialize)
        .await
        .map_err(|_| Error::StageGone("single-fetch"))?;

    loop {
        match blocks_rx
            .next()
            .await
            .ok_or(Error::ConnectionClosedBeforeReply)?
        {
            Blocks::Block(0, _peer, network_block) => {
                return network_block
                    .decode_block()
                    .map_err(|e| Error::DecodeBlock(e.to_string()));
            }
            Blocks::Done(0) => return Err(Error::NoBlockReturned(point)),
            Blocks::NoBlocks(0, _peer) => return Err(Error::NoBlockAt(point)),
            Blocks::NoPeersAvailable(0) => return Err(Error::BlockFetchNotReady),
            other => tracing::warn!(?other, "ignoring unexpected block-fetch message"),
        }
    }
}
