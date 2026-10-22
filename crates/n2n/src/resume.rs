//! Seeding the `ChainStore` resource `amaru-protocols`' chain-sync initiator
//! needs to resume from a point.
//!
//! There's no "resume from this `Point`" parameter anywhere in
//! `amaru-protocols`' chainsync initiator -- on `Initialize` it samples its
//! own ancestor points to offer in `FindIntersect` from a local `ChainStore`
//! resource (see `orchestrator`/`pipeline` module docs). So "where to resume
//! from" has to be expressed by seeding that store with one real header as
//! its "best chain tip," not passed as a parameter anywhere closer to the
//! connection itself.
//!
//! [`ResumeFrom::Header`] takes a [`Header`] directly -- deliberately not a
//! `Point` plus a fetch -- because the intended caller is `floe`'s own
//! `ingest/sync.rs`, which caches its current tip's header (re-encoded CBOR)
//! across restarts specifically so this doesn't need a network round-trip in
//! steady state. A round-trip is only unavoidable once: after a fresh seed
//! (the snapshot anchor has no header bytes attached, just `<slot>.<hash>` in
//! its filename) or for a pre-existing DB's first run under this cache --
//! `ingest/sync.rs` handles that fallback, this module doesn't.

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
    /// Genesis. No header is needed for this one: amaru-protocols' chain-sync
    /// initiator special-cases `Point::Origin` as the best chain tip and
    /// returns it directly as the sole intersect candidate (see
    /// `sample_ancestor_points`), without ever calling `load_header`.
    Origin,
}

/// Build a `ChainStore` (suitable for `ResourceHeaderStore`) whose "best
/// chain tip" is `resume`'s point -- the minimum a chain-sync initiator
/// needs to successfully intersect and resume from that point.
pub fn seed_chain_store(resume: ResumeFrom<'_>) -> anyhow::Result<Arc<InMemoryChainStore>> {
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
