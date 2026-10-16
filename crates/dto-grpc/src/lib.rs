//! floe-dto-grpc: generated protobuf/gRPC types for floe's own
//! `QueryService` fork (+ its `cardano` dependency), compiled by
//! `build.rs` from `proto/floe/v1/`. Nothing hand-written here --
//! that's `floe-dto-http`'s job for the REST/CBOR wire shapes. Module
//! nesting (`floe::v1::{cardano,query}`) mirrors the proto package
//! path exactly: `prost`/`tonic-build` emit cross-package references
//! (e.g. `query`'s use of `cardano::Block`) as `super::`-relative
//! paths assuming that nesting, so it isn't just a style choice here.
//!
//! This is floe's own fork of UTxORPC, not the upstream spec --
//! divergences are documented inline on the diverging fields
//! themselves in `proto/floe/v1/*.proto`, not in a separate doc.

pub mod floe;
