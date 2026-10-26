//! Seeds the `ChainStore` resource chain-sync resumes from.
//!
//! amaru-protocols' chainsync initiator takes no "resume from this Point"
//! parameter -- on `Initialize` it samples ancestor points from a local
//! `ChainStore`. So resuming means seeding that store's "best chain tip"
//! with one real header, not passing a point directly.
//!
//! [`ResumeFrom::Header`] takes a [`Header`], not a `Point` + fetch: the
//! real caller (`ingest/sync.rs`) caches its tip's header across restarts
//! to avoid a network round-trip in steady state; that fallback lives
//! there, not here.

use std::sync::Arc;

use amaru_kernel::{Header, IsHeader, NetworkPoint, Point};
use amaru_ouroboros::WriteChainStore;
use amaru_ouroboros_traits::stores::chain_store::InMemoryChainStore;

/// This header's own point: slot + hash + block height.
pub fn point_of(header: &Header) -> Point {
    Point::new(
        NetworkPoint::Specific(header.slot(), header.hash()),
        header.block_height(),
    )
}

/// Where to resume chain-sync from.
pub enum ResumeFrom<'a> {
    /// A real header, elsewhere on the peer's chain. Its own point (via
    /// [`point_of`]) is what's offered to `FindIntersect`.
    Header(&'a Header),
    /// Genesis -- no header needed: the chain-sync initiator special-cases
    /// `Point::Origin` as the sole intersect candidate without a header load.
    Origin,
}

/// Build a `ChainStore` (suitable for `ResourceHeaderStore`) whose "best
/// chain tip" is `resume`'s point -- the minimum a chain-sync initiator
/// needs to successfully intersect and resume from that point.
pub fn seed_chain_store(resume: ResumeFrom<'_>) -> crate::Result<Arc<InMemoryChainStore>> {
    let chain_store = Arc::new(InMemoryChainStore::default());
    let point = match resume {
        ResumeFrom::Header(header) => {
            chain_store.store_header(header)?;
            point_of(header)
        }
        ResumeFrom::Origin => Point::Origin,
    };
    chain_store.set_anchor_point(&point)?;
    chain_store.set_best_chain_tip(&point)?;
    Ok(chain_store)
}
