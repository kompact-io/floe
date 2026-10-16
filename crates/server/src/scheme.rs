//! scheme: `floe-server`'s transports -- `grpc` (`QueryService`/
//! `SubmitService`/`HealthService`) and `http` (the REST query API
//! plus `/health`/`/metrics`). Both are reporting/serving apparatus
//! over the crate-root `crate::health`/`crate::metrics`/`crate::query`
//! core: neither scheme computes anything of its own.

pub mod grpc;
pub mod http;
