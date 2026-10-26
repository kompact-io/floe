//! This crate's own error type, replacing `anyhow` -- every public
//! function here returns a typed [`Error`], not an opaque chain of
//! strings, so a caller can match on what actually went wrong instead of
//! only ever logging/propagating it.

use amaru_kernel::{PeerError, Point};
use amaru_ouroboros::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid peer address: {0}")]
    InvalidPeer(#[from] PeerError),

    #[error("failed to seed chain store: {0}")]
    SeedChainStore(#[from] StoreError),

    #[error("failed to connect to peer: {0}")]
    Connect(#[from] std::io::Error),

    /// `{0}`: which stage's mailbox is gone (`"connection"`, `"pipeline"`,
    /// `"single-fetch"`).
    #[error("{0} stage already gone")]
    StageGone(&'static str),

    #[error("connection closed")]
    ConnectionClosed,

    #[error("connection closed before any reply")]
    ConnectionClosedBeforeReply,

    /// `peer_tip_block_no`: the peer's own tip as of the last
    /// `IntersectNotFound` reply -- callers use it to tell "peer hasn't
    /// caught up to our resume point yet" (peer tip still behind it) apart
    /// from a confirmed orphan (peer tip already past it, so no amount of
    /// retrying the same point will ever succeed).
    #[error("peer never caught up to our resume point after {attempts} attempts (peer tip block_no={peer_tip_block_no})")]
    NeverCaughtUp {
        attempts: u32,
        peer_tip_block_no: u64,
    },

    #[error("peer returned no block for {0:?}")]
    NoBlockReturned(Point),

    #[error("peer has no block at {0:?}")]
    NoBlockAt(Point),

    #[error("peer has no blocks in {from:?}..={through:?}")]
    NoBlocksInRange { from: Point, through: Point },

    #[error("block-fetch initiator not ready")]
    BlockFetchNotReady,

    /// Stringified, not `#[from]`: `amaru-kernel` pins a different,
    /// incompatible `minicbor` major version than this workspace's own.
    #[error("failed to decode block: {0}")]
    DecodeBlock(String),
}

pub type Result<T> = std::result::Result<T, Error>;
