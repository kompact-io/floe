//! Receives `ChainSyncInitiatorMsg` and drives CIP-0164 pipelining: the
//! chain-sync initiator never sends more than one `RequestNext` on its
//! own, this stage re-arms it. One job only: relay `RollForward`/
//! `RollBackward` to [`HeaderEvent`]'s output channel and re-arm. Batching
//! policy (depth-based) is the caller's job, through [`crate::client::Client`].
//!
//! Block-fetch requests do not come through here -- see `connection`'s
//! module docs for why. `Msg` is `ChainSyncInitiatorMsg` directly, not a
//! wrapper enum, since relay is this stage's only job.

use amaru_kernel::{Header, Point, cbor};
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
    /// Resume point not found (yet). No "retry `FindIntersect`" primitive
    /// here (unlike `pallas_network`'s `Client`) -- caller reconnects from
    /// scratch. See `client::Client::connect_with_retry`.
    IntersectNotFound {
        tip_block_no: u64,
    },
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Pipeline {
    headers: StageRef<HeaderEvent>,
}

impl Pipeline {
    pub fn new(headers: StageRef<HeaderEvent>) -> Self {
        Self { headers }
    }

    /// `&mut self`: flat data, no phase transitions to guard, so no
    /// exhaustive-match safety gained by consuming `self` instead.
    async fn handle(&mut self, msg: ChainSyncInitiatorMsg, eff: Effects<ChainSyncInitiatorMsg>) {
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
                    &self.headers,
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
                            &self.headers,
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
                    &self.headers,
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

// This wrapper simply allows us to follow the convention of amaru-protocols.
pub async fn stage(
    mut state: Pipeline,
    msg: ChainSyncInitiatorMsg,
    eff: Effects<ChainSyncInitiatorMsg>,
) -> Pipeline {
    state.handle(msg, eff).await;
    state
}
