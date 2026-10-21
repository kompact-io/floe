//! The "consensus pipeline" stage `amaru_protocols::chainsync`'s initiator
//! sends `ChainSyncInitiatorMsg` to. This is where CIP-0164 pipelining
//! actually happens: the chain-sync initiator itself never sends more than
//! one `RequestNext` on its own -- it is driven entirely by messages this
//! stage sends back to `msg.handler`. Keeping [`PIPELINE_DEPTH`] requests
//! outstanding at all times (instead of one at a time, as
//! `pallas_network::miniprotocols::chainsync::Client` forces) is the whole
//! point of this crate.
//!
//! This stage has no fetch policy of its own -- it just forwards every
//! header event out to [`HeaderEvent`]'s output channel and relays
//! [`PipelineMsg::FetchBodies`] requests to the block-fetch initiator once
//! it's registered. Deciding *when* to batch bodies (depth-based, as
//! `floe`'s real `ingest/sync.rs` does) is the caller's job, through
//! [`crate::client::Client`] -- this stage runs forever, it doesn't stop
//! after any fixed count.
//!
//! The block-fetch initiator's `StageRef` only exists after handshake
//! completes (inside `orchestrator`), strictly after this stage is already
//! wired up and receiving `ChainSyncInitiatorMsg` -- so [`PipelineMsg`] wraps
//! that type alongside a `SetBlockfetch` variant `orchestrator` sends once
//! the block-fetch initiator is registered. `register_chainsync_initiator`'s
//! signature requires exactly `StageRef<ChainSyncInitiatorMsg>`, so
//! `orchestrator` adapts a `StageRef<PipelineMsg>` down to that via
//! `.contramap(PipelineMsg::Chain)` rather than this stage's `Msg` type being
//! `ChainSyncInitiatorMsg` directly.

use amaru_kernel::{Header, Point, cbor};
use amaru_protocols::blockfetch::{BlockFetchMessage, Blocks};
use amaru_protocols::chainsync::{
    ChainSyncInitiatorMsg, InitiatorMessage, InitiatorResult, PIPELINE_DEPTH,
};
use amaru_pure_stage::{Effects, StageRef};
use tracing::{error, info, warn};

/// What this stage reports to its caller (via an output channel -- see
/// `StageGraph::output`). `tip_block_no` is the peer's current best block
/// number, as amaru-protocols' chainsync reports it alongside every reply.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum HeaderEvent {
    RollForward {
        header: Box<Header>,
        tip_block_no: u64,
    },
    RollBackward {
        point: Point,
        tip_block_no: u64,
    },
    /// The resume point hasn't been found on the peer's chain (yet).
    /// `amaru-protocols`' chain-sync initiator has no "retry `FindIntersect`
    /// on this same connection" primitive (unlike
    /// `pallas_network::miniprotocols::chainsync::Client`, which allows
    /// resending `FindIntersect`) -- the caller has to reconnect and retry
    /// from scratch. See `client::Client::connect`.
    IntersectNotFound {
        tip_block_no: u64,
    },
}

/// This stage's real message type. See module docs for why `Chain` wraps
/// `ChainSyncInitiatorMsg` rather than this being `ChainSyncInitiatorMsg`
/// itself.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PipelineMsg {
    Chain(ChainSyncInitiatorMsg),
    SetBlockfetch(StageRef<BlockFetchMessage>),
    FetchBodies {
        from: Point,
        through: Point,
        id: u64,
        cr: StageRef<Blocks>,
    },
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct State {
    headers: StageRef<HeaderEvent>,
    blockfetch: Option<StageRef<BlockFetchMessage>>,
}

impl State {
    pub fn new(headers: StageRef<HeaderEvent>) -> Self {
        Self {
            headers,
            blockfetch: None,
        }
    }
}

pub async fn stage(mut state: State, msg: PipelineMsg, eff: Effects<PipelineMsg>) -> State {
    match msg {
        PipelineMsg::SetBlockfetch(blockfetch) => {
            info!("block-fetch initiator registered");
            state.blockfetch = Some(blockfetch);
        }
        PipelineMsg::FetchBodies {
            from,
            through,
            id,
            cr,
        } => match &state.blockfetch {
            Some(blockfetch) => {
                eff.send(
                    blockfetch,
                    BlockFetchMessage::RequestRange {
                        from,
                        through,
                        id,
                        cr,
                    },
                )
                .await;
            }
            None => {
                warn!("fetch_bodies requested before the block-fetch initiator was ready");
                eff.send(&cr, Blocks::NoPeersAvailable(id)).await;
            }
        },
        PipelineMsg::Chain(msg) => {
            let handler = msg.handler;
            match msg.msg {
                InitiatorResult::Initialize => {
                    info!("chainsync initialized, waiting for intersect");
                }
                InitiatorResult::IntersectFound(point, tip) => {
                    info!(
                        ?point,
                        ?tip,
                        depth = PIPELINE_DEPTH,
                        "intersect found, filling pipeline"
                    );
                    for _ in 0..PIPELINE_DEPTH {
                        eff.send(&handler, InitiatorMessage::RequestNext).await;
                    }
                }
                InitiatorResult::IntersectNotFound(tip) => {
                    warn!(
                        ?tip,
                        "intersect not found -- caller must reconnect and retry"
                    );
                    eff.send(&handler, InitiatorMessage::Done).await;
                    eff.send(
                        &state.headers,
                        HeaderEvent::IntersectNotFound {
                            tip_block_no: tip.block_height().as_u64(),
                        },
                    )
                    .await;
                }
                InitiatorResult::RollForward(header, tip) => {
                    match cbor::decode::<Header>(&header.cbor) {
                        Ok(hdr) => {
                            eff.send(
                                &state.headers,
                                HeaderEvent::RollForward {
                                    header: Box::new(hdr),
                                    tip_block_no: tip.block_height().as_u64(),
                                },
                            )
                            .await;
                        }
                        Err(error) => error!(?error, "failed to decode header, dropping it"),
                    }
                    eff.send(&handler, InitiatorMessage::RequestNext).await;
                }
                InitiatorResult::RollBackward(point, tip) => {
                    eff.send(
                        &state.headers,
                        HeaderEvent::RollBackward {
                            point,
                            tip_block_no: tip.block_height().as_u64(),
                        },
                    )
                    .await;
                    eff.send(&handler, InitiatorMessage::RequestNext).await;
                }
                InitiatorResult::Terminated => {
                    info!("chainsync initiator terminated");
                }
            }
        }
    }
    state
}
