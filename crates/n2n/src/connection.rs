//! A simpler connection / connection-manager -- a cut-down
//! [`amaru_protocols::connection::Connection`] / amaru `Connections`, for
//! one fixed peer only. Divergent design goals:
//!
//! - initiator-only: no responders, no full-duplex.
//! - one fixed peer: no redial, no peer registry, no peer-sharing.
//! - no keepalive or tx-submission yet.
//!
//! Message type is `ConnectionMessage` only because
//! `register_chainsync_initiator` hardcodes `Effects<ConnectionMessage>`.
//!
//! `ConnectionMessage::FetchBlocks` is handled directly here, not relayed
//! through `pipeline` as an earlier version did -- a block-fetch request
//! must never queue behind `pipeline`'s own header-relay backpressure.
//! See `fix-pipleline-bug.md` in the repo root for the incident writeup.

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

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Params {
    pub peer: amaru_kernel::Peer,
    pub conn_id: ConnectionId,
    pub magic: NetworkMagic,
    /// Where chain-sync results get relayed to -- see `pipeline`'s module
    /// docs. Block-fetch requests do not go through this; see this
    /// module's own docs for why.
    pub pipeline: StageRef<chainsync::ChainSyncInitiatorMsg>,
}

/// `params` is a permanent field, never reconstructed -- it never changes.
/// `phase` transitions via a consuming match (`mem::replace`, see
/// `handle`), so a handler can only run in its matching phase -- no
/// `unreachable!()` needed.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Connection {
    params: Params,
    phase: Phase,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum Phase {
    Initial,
    Handshaking {
        muxer: StageRef<MuxMessage>,
    },
    Established {
        blockfetch: StageRef<blockfetch::BlockFetchMessage>,
    },
    Done,
}

impl Connection {
    pub fn new(params: Params) -> Self {
        Self {
            params,
            phase: Phase::Initial,
        }
    }

    async fn handle(&mut self, msg: ConnectionMessage, eff: Effects<ConnectionMessage>) {
        self.phase = match (std::mem::replace(&mut self.phase, Phase::Done), msg) {
            (Phase::Initial, ConnectionMessage::Initialize) => self.initialize(eff).await,
            (Phase::Handshaking { muxer }, ConnectionMessage::Handshake(result)) => {
                self.handshake(muxer, result, eff).await
            }
            (
                Phase::Established { blockfetch },
                ConnectionMessage::FetchBlocks {
                    from,
                    through,
                    id,
                    cr,
                },
            ) => {
                eff.send(
                    &blockfetch,
                    blockfetch::BlockFetchMessage::RequestRange {
                        from,
                        through,
                        id,
                        cr,
                    },
                )
                .await;
                Phase::Established { blockfetch }
            }
            (_, ConnectionMessage::ChildDied(child)) => {
                error!(?child, "connection child died, giving up");
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
        // No early mini-protocol buffers: as an initiator we register handshake
        // before the peer has any reason to send us anything else.
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
                max_buffer: amaru_protocols::protocol::ingress_limit(PROTO_HANDSHAKE),
            },
        )
        .await;

        info!(peer = %self.params.peer, "handshake registered, awaiting result");
        Phase::Handshaking { muxer }
    }

    async fn handshake(
        &mut self,
        muxer: StageRef<MuxMessage>,
        result: HandshakeResult,
        eff: Effects<ConnectionMessage>,
    ) -> Phase {
        let (version_number, version_data) = match result {
            HandshakeResult::Accepted(version_number, version_data) => {
                (version_number, version_data)
            }
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
            self.params.peer,
            self.params.conn_id,
            self.params.pipeline.clone(),
            &eff,
            ConnectionMessage::ChildDied(ChildId::ChainSync),
        )
        .await;
        info!("chainsync initiator registered");

        let blockfetch = blockfetch::register_blockfetch_initiator(
            &muxer,
            self.params.peer,
            BLOCKFETCH_PIPELINE_N,
            &eff,
            ConnectionMessage::ChildDied(ChildId::BlockFetch),
        )
        .await;
        info!("blockfetch initiator registered");

        Phase::Established { blockfetch }
    }
}

// This wrapper simply allows us to follow the convention of amaru-protocols.
pub async fn stage(
    mut state: Connection,
    msg: ConnectionMessage,
    eff: Effects<ConnectionMessage>,
) -> Connection {
    state.handle(msg, eff).await;
    state
}
