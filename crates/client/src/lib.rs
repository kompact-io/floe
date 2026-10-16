//! floe-client: a library client for floe-server's QueryService/
//! SubmitService. One `Backend`-implementing `Client` per supported
//! transport+wire-format scheme (see `common::Backend` and each scheme
//! module's own doc comment), plus two scheme-independent helpers
//! (`key_info::key_info`, `self_pay::run`, the latter generic over
//! `Backend`). `src/main.rs` is a thin CLI front end over this
//! library -- see its own doc comment for why it needs `clap`/
//! `serde_json` that this library itself never does.
//!
//! Deliberately depends on nothing from `floe-server` itself (no
//! `rusqlite`/`tikv-jemallocator` in its dependency graph) -- that's
//! what keeps a wasm target for this crate possible without dragging
//! in the server's native-only stack.

mod common;
pub use common::{
    Backend, ChangesParams, Cred, NetworkArg, Paging, TxHash, parse_hex, shelley_creds,
};

pub mod key_info;
pub mod self_pay;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(feature = "http-cbor")]
pub mod http_cbor;
#[cfg(any(feature = "http-json", feature = "http-cbor"))]
mod http_common;
#[cfg(feature = "http-json")]
pub mod http_json;
