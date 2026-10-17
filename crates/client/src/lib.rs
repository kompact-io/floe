//! floe-client: a library client for floe-server's QueryService/
//! SubmitService. One `Backend`-implementing `Client` per supported
//! transport+wire-format scheme (see `common::Backend` and each scheme
//! module's own doc comment); `scheme::{Scheme, SchemeArgs, AnyClient,
//! connect}` is the shared "pick a scheme from flags, build that
//! client" seam both `floe-client` and `floe-smoke` use, instead of
//! each reimplementing it. `key_info` is a pure local helper
//! independent of any scheme (no network call at all).
//!
//! Deliberately depends on nothing from `floe-server` itself (no
//! `rusqlite`/`tikv-jemallocator` in its dependency graph) -- that's
//! what makes the `wasm` feature's `wasm32-unknown-unknown` target
//! possible without dragging in the server's native-only stack.
//! `clap`/`tracing` stay fully optional for the same reason: a plain lib
//! consumer (`default-features = false, features = ["grpc"]`, say) pulls
//! neither in -- see `Cargo.toml`'s own `[features]` doc comment.

mod common;
pub use common::{Backend, Hash28, NetworkArg, TxHash, parse_hex, shelley_hashes};

mod scheme;
pub use scheme::{AnyClient, Scheme, SchemeArgs, connect};

pub mod key_info;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(feature = "http-cbor")]
pub mod http_cbor;
#[cfg(any(feature = "http-json", feature = "http-cbor"))]
mod http_common;
#[cfg(feature = "http-json")]
pub mod http_json;
#[cfg(any(feature = "http-json", feature = "wasm"))]
mod json_wire;

#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
pub mod wasm;
