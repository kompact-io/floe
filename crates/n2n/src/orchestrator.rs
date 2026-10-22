//! From-scratch, initiator-only replacement for
//! `amaru_protocols::connection::Connection`, scoped to exactly what a
//! single-peer client needs: mux + handshake (declaring
//! `initiator_only_diffusion_mode = true`, so we never claim full-duplex and
//! never risk the peer expecting us to run responders) + the chain-sync and
//! block-fetch initiators. No keepalive, peer-sharing, or tx-submission yet --
//! those are follow-ups once header+body pipelining itself is proven.
//!
//! Reuses `amaru_protocols::connection::ConnectionMessage`/`ChildId` as our
//! message type purely because `register_chainsync_initiator`'s signature is
//! hardcoded to `Effects<ConnectionMessage>` -- not because we want anything
//! else `connection.rs` does.

use amaru_kernel::NetworkMagic;
use amaru_ouroboros::ConnectionId;
use amaru_protocols::{
    blockfetch::{self, BLOCKFETCH_PIPELINE_N},
    chainsync,
    connection::{ChildId, ConnectionMessage},
    handshake,
    mux::{self, MuxMessage},
    protocol::{Inputs, PROTO_HANDSHAKE, Role},
    protocol_messages::{
        handshake::HandshakeResult, version_number::VersionNumber, version_table::VersionTable,
    },
};
use amaru_pure_stage::{Effects, StageRef};
use tracing::{error, info};

use crate::pipeline::PipelineMsg;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Params {
    pub peer: amaru_kernel::Peer,
    pub conn_id: ConnectionId,
    pub magic: NetworkMagic,
    /// The pipeline decides *what* to fetch (headers drive the block-fetch
    /// range); this orchestrator only wires the block-fetch initiator up and
    /// hands the pipeline its `StageRef` once registered.
    pub pipeline: StageRef<PipelineMsg>,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum State {
    Initial(Params),
    Handshaking {
        params: Params,
        muxer: StageRef<MuxMessage>,
    },
    Established,
    Done,
}

pub async fn stage(state: State, msg: ConnectionMessage, eff: Effects<ConnectionMessage>) -> State {
    match (state, msg) {
        (State::Initial(params), ConnectionMessage::Initialize) => do_initialize(params, eff).await,
        (State::Handshaking { params, muxer }, ConnectionMessage::Handshake(result)) => {
            do_handshake(params, muxer, result, eff).await
        }
        (_, ConnectionMessage::ChildDied(child)) => {
            error!(?child, "connection child died, giving up");
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
    // No early mini-protocol buffers: as an initiator we register handshake
    // before the peer has any reason to send us anything else.
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
            max_buffer: amaru_protocols::protocol::ingress_limit(PROTO_HANDSHAKE),
        },
    )
    .await;

    info!(peer = %params.peer, "handshake registered, awaiting result");
    State::Handshaking { params, muxer }
}

async fn do_handshake(
    params: Params,
    muxer: StageRef<MuxMessage>,
    result: HandshakeResult,
    eff: Effects<ConnectionMessage>,
) -> State {
    let (version_number, version_data) = match result {
        HandshakeResult::Accepted(version_number, version_data) => (version_number, version_data),
        HandshakeResult::Refused(reason) => {
            error!(?reason, "handshake refused");
            eff.terminate().await
        }
        HandshakeResult::Query(table) => {
            error!(?table, "peer returned a version query instead of accepting");
            eff.terminate().await
        }
    };
    info!(
        ?version_number,
        full_duplex = version_data.is_full_duplex_capable(),
        "handshake accepted"
    );

    eff.send(
        &muxer,
        MuxMessage::SetSduTimeout(mux::SDU_TIMEOUT_ESTABLISHED),
    )
    .await;

    chainsync::register_chainsync_initiator(
        &muxer,
        params.peer,
        params.conn_id,
        params.pipeline.clone().contramap(PipelineMsg::Chain),
        &eff,
        ConnectionMessage::ChildDied(ChildId::ChainSync),
    )
    .await;
    info!("chainsync initiator registered");

    let blockfetch_ref = blockfetch::register_blockfetch_initiator(
        &muxer,
        params.peer,
        BLOCKFETCH_PIPELINE_N,
        &eff,
        ConnectionMessage::ChildDied(ChildId::BlockFetch),
    )
    .await;
    info!("blockfetch initiator registered");
    eff.send(&params.pipeline, PipelineMsg::SetBlockfetch(blockfetch_ref))
        .await;

    State::Established
}
