//! floe-dto-grpc: generated protobuf/gRPC types for floe's own
//! `QueryService` fork (+ its `cardano` dependency), compiled by
//! `build.rs` from `proto/floe/v1/`. Nothing hand-written here --
//! that's `floe-dto-http`'s job for the REST/CBOR wire shapes. Module
//! nesting (`floe::v1::{cardano,query}`) mirrors the proto package
//! path exactly: `prost`/`tonic-build` emit cross-package references
//! (e.g. `query`'s use of `cardano::Block`) as `super::`-relative
//! paths assuming that nesting, so it isn't just a style choice here.
//!
//! This is floe's own spec, not the vendored UTxORPC one -- see
//! `docs/utxorpc-divergences.md` for how the two relate, and
//! `docs/utxorpc-spec/` for the pristine upstream copy these types
//! are diffed against.

pub mod floe;
