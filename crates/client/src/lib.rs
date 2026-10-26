//! floe-client: a library client for floe-server's QueryService/
//! SubmitService. One `Backend`-implementing `Client` per scheme
//! (`grpc`, `http::json`, `http::cbor`); `scheme::{Scheme, SchemeArgs,
//! AnyClient, connect}` picks one from flags.

mod error;
pub use error::{Error, Result};

mod common;
pub use common::{Backend, Hash28, NetworkArg, TxHash, address_hashes, parse_hex};

mod scheme;
pub use scheme::{AnyClient, Scheme, SchemeArgs, connect};

pub mod key_info;

#[cfg(feature = "grpc")]
pub mod grpc;

#[cfg(any(feature = "http-json", feature = "http-cbor"))]
pub mod http;

// `wasm::WasmClient` wraps `AnyClient`, picking `http::json`/`http::cbor`
// at JS-side instantiation -- both can be compiled in at once.
#[cfg(all(
    target_arch = "wasm32",
    any(feature = "http-json", feature = "http-cbor")
))]
pub mod wasm;

// `tonic::transport::Channel` needs hyper/tokio TCP; doesn't build for wasm32.
#[cfg(all(target_arch = "wasm32", feature = "grpc"))]
compile_error!("feature \"grpc\" doesn't build for wasm32 -- use --no-default-features");
