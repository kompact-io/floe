//! http: `floe-server`'s http transport. `query` (the query/submit
//! API, both wire formats sharing one negotiated listener -- see its
//! own doc) and `metrics` (`/health`/`/metrics`) are independent
//! listeners.

pub mod metrics;

#[cfg(any(feature = "http-json", feature = "http-cbor"))]
pub mod query;
