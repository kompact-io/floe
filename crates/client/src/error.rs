//! `floe-client`'s error type. `thiserror`, not `anyhow` -- a lib
//! exposes a concrete type; callers (the bins, or any downstream
//! consumer) add their own context/erasure on top, e.g. with `anyhow`
//! at the edge, which converts any `std::error::Error` for free.

use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error("{msg}: {source}")]
    Context { msg: String, source: Box<Error> },

    // `reqwest`/`gloo-net` are only actually dependencies when a
    // `http-*` scheme feature turns them on (native) or unconditionally
    // on wasm32 (see `Cargo.toml`'s target tables) -- same gating here.
    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "http-json", feature = "http-cbor")
    ))]
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[cfg(target_arch = "wasm32")]
    #[error(transparent)]
    Http(#[from] gloo_net::Error),

    #[cfg(any(feature = "http-json", target_arch = "wasm32"))]
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    CborDecode(#[from] minicbor::decode::Error),
    #[error(transparent)]
    CborEncode(#[from] minicbor::encode::Error<std::convert::Infallible>),

    #[cfg(feature = "grpc")]
    #[error(transparent)]
    Grpc(#[from] tonic::Status),
    #[cfg(feature = "grpc")]
    #[error(transparent)]
    GrpcTransport(#[from] tonic::transport::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn msg(s: impl Into<String>) -> Self {
        Self::Message(s.into())
    }
}

/// `anyhow::Context`'s two methods, same signatures -- so call sites
/// that only used `anyhow` for `.context()`/`.with_context()` port over
/// unchanged. Unused with every `http-*` feature off (nothing left to
/// add context to).
#[allow(dead_code)]
pub trait Context<T> {
    fn context(self, msg: impl Into<String>) -> Result<T>;
    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T>;
}

impl<T, E: Into<Error>> Context<T> for std::result::Result<T, E> {
    fn context(self, msg: impl Into<String>) -> Result<T> {
        self.map_err(|e| Error::Context {
            msg: msg.into(),
            source: Box::new(e.into()),
        })
    }

    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T> {
        self.map_err(|e| Error::Context {
            msg: f(),
            source: Box::new(e.into()),
        })
    }
}
