//! scheme: `floe-server`'s transports -- `grpc` (`QueryService`/
//! `SubmitService`/`HealthService`) and `http` (the query/submit API
//! plus `/health`/`/metrics`). Both are reporting/serving apparatus
//! over the crate-root `crate::health`/`crate::metrics`/`crate::serve::query`
//! core: neither scheme computes anything of its own.

#[cfg(feature = "grpc")]
pub mod grpc;
pub mod http;
