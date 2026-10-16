//! grpc::metrics: reporting apparatus only -- a `tower::Layer` wrapping
//! the whole gRPC server (every service/method, see `super::serve`) that
//! records each request's method path into the crate-root
//! `crate::metrics::Metrics` (`Scheme::Grpc`). No counters of its own.
//!
//! Counts only, no latency/status breakdown: a tonic `Status` surfaces
//! as an *Ok* HTTP/2 response carrying a `grpc-status` trailer, not a
//! transport-level `Err` -- splitting ok/error here would mean
//! buffering/inspecting every response, not worth it for a first cut
//! at throughput (not success-rate).

use std::sync::Arc;
use std::task::{Context, Poll};

use tower::{Layer, Service};

use crate::metrics::{Metrics, Scheme};

#[derive(Clone)]
pub struct MetricsLayer(pub Arc<Metrics>);

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics: Arc<Metrics>,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for MetricsService<S>
where
    S: Service<http::Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // The gRPC method path, e.g. "/floe.v1.query.QueryService/SearchUtxos".
        self.metrics.record(Scheme::Grpc, req.uri().path());
        self.inner.call(req)
    }
}
