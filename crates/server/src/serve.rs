//! serve: store -> response. `Ctx` owns every store-access path --
//! its own `query` submodule is private, reachable from nowhere else
//! in this crate. `scheme` is the set of wire transports built on
//! `Ctx`; `submit` is its supporting piece.

pub mod ctx;
pub mod scheme;
pub mod submit;

pub use ctx::Ctx;
