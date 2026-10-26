//! One-shot block fetch at a known point, skipping chain-sync entirely.
//!
//! This is the replacement for floe's former `fetch_header_via_pallas`
//! (`crates/server/src/ingest/sync.rs`), which dialed
//! `pallas_network::facades::PeerClient` just to call `blockfetch().fetch_single(..)`
//! -- the one place `pallas-network` still appeared in `floe-server`'s
//! production path. [`Client::connect`](crate::client::Client::connect)
//! can't be reused here: its chain-sync initiator needs a `ChainStore`
//! resume point to even start (see [`crate::resume`]), and a `ChainStore`
//! is exactly what's missing when there's no cached header yet -- the
//! whole reason this function gets called.
//!
//! So this registers *only* the block-fetch initiator (mux + handshake +
//! blockfetch, no chain-sync, no `ResourceHeaderStore`), requests exactly
//! one point, and returns its block.

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
use anyhow::{Result, anyhow, bail};
use futures_util::StreamExt;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Params {
    peer: Peer,
    conn_id: ConnectionId,
    magic: NetworkMagic,
    point: Point,
    blocks_out: StageRef<Blocks>,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum State {
    Initial(Params),
    Handshaking {
        params: Params,
        muxer: StageRef<MuxMessage>,
    },
    Established,
}

async fn stage(state: State, msg: ConnectionMessage, eff: Effects<ConnectionMessage>) -> State {
    match (state, msg) {
        (State::Initial(params), ConnectionMessage::Initialize) => do_initialize(params, eff).await,
        (State::Handshaking { params, muxer }, ConnectionMessage::Handshake(result)) => {
            do_handshake(params, muxer, result, eff).await
        }
        (_, ConnectionMessage::ChildDied(child)) => {
            tracing::error!(?child, "single-fetch connection child died, giving up");
            eff.terminate().await
        }
        (state, _other) => state,
    }
}

async fn do_initialize(params: Params, eff: Effects<ConnectionMessage>) -> State {
    let muxer = eff
        .stage_with_mailbox_size("mux", mux::stage, mux::MUX_MAILBOX_SIZE)
        .await;
    let muxer = eff.supervise(muxer, ConnectionMessage::ChildDied(ChildId::Mux));
    let muxer = eff
        .wire_up(
            muxer,
            mux::State::new(params.conn_id, &[], Role::Initiator, params.peer),
        )
        .await;

    let handshake_result: StageRef<HandshakeResult> =
        eff.me_ref().contramap(ConnectionMessage::Handshake);
    let hs = eff.stage("handshake", handshake::initiator()).await;
    let hs = eff.supervise(hs, ConnectionMessage::ChildDied(ChildId::Handshake));
    let version_table = VersionTable::v11_through(
        VersionNumber::CURRENT,
        params.magic,
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

    State::Handshaking { params, muxer }
}

async fn do_handshake(
    params: Params,
    muxer: StageRef<MuxMessage>,
    result: HandshakeResult,
    eff: Effects<ConnectionMessage>,
) -> State {
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
        params.peer,
        BLOCKFETCH_PIPELINE_N,
        &eff,
        ConnectionMessage::ChildDied(ChildId::BlockFetch),
    )
    .await;

    eff.send(
        &blockfetch_ref,
        BlockFetchMessage::RequestRange {
            from: params.point,
            through: params.point,
            id: 0,
            cr: params.blocks_out,
        },
    )
    .await;

    State::Established
}

/// Dials `addr`, fetches the single block at `point`, and returns it
/// decoded. Only ever called when there's no cached resume header (see
/// module docs).
pub async fn fetch_block(addr: SocketAddr, magic: NetworkMagic, point: Point) -> Result<Block> {
    let peer = Peer::try_from(addr).map_err(|e| anyhow!("{e}"))?;
    let connections = Arc::new(TokioConnections::new(64 * 1024));
    let conn_id = connections.connect(peer, CONNECT_TIMEOUT).await?;

    let mut network = TokioBuilder::default();
    network.resources().put::<ConnectionsResource>(connections);

    let (blocks_ref, mut blocks_rx) = network.output::<Blocks>("blocks", 8);

    let stage_handle = network.stage("single-fetch", stage);
    let stage_ref = network.wire_up(
        stage_handle,
        State::Initial(Params {
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
        .map_err(|_| anyhow!("single-fetch stage already gone"))?;

    loop {
        match blocks_rx
            .next()
            .await
            .ok_or_else(|| anyhow!("connection closed before any reply"))?
        {
            Blocks::Block(0, _peer, network_block) => return Ok(network_block.decode_block()?),
            Blocks::Done(0) => bail!("peer returned no block for {point:?}"),
            Blocks::NoBlocks(0, _peer) => bail!("peer has no block at {point:?}"),
            Blocks::NoPeersAvailable(0) => bail!("block-fetch initiator not ready"),
            other => tracing::warn!(?other, "ignoring unexpected block-fetch message"),
        }
    }
}
