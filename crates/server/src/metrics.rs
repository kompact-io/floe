//! metrics: core, scheme-labeled request-count state + `MetricsResponse`
//! snapshot assembly, plus `MetricsLayer`, the tower `Layer` every
//! scheme wraps its server in to record into that state. Zero
//! knowledge of HTTP, gRPC, or wire format beyond the generic `http`
//! crate `Request` tower itself is built on -- every scheme's own
//! handlers just snapshot and render `snapshot()`'s result.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

pub use floe_dto_http::health::Scheme;
use floe_dto_http::health::{MetricsResponse, RequestCount};
use tower::{Layer, Service};

use crate::health::Health;

#[derive(Debug, Default)]
pub struct Metrics {
    counts: Mutex<HashMap<(Scheme, String), u64>>,
}

impl Metrics {
    pub fn record(&self, scheme: Scheme, method: impl Into<String>) {
        *self
            .counts
            .lock()
            .unwrap()
            .entry((scheme, method.into()))
            .or_insert(0) += 1;
    }

    /// Marks `scheme` as live (called once, when `main.rs` actually
    /// starts that scheme's listener) so it's always reported at 0
    /// rather than being absent from `/metrics` until its first
    /// request -- lazily-created Prometheus series can't be told
    /// apart from "no such scheme" by anything scraping this. The
    /// empty `method` is a sentinel, not a real route; real
    /// `record()` calls accumulate in their own `(scheme, method)`
    /// entries alongside it.
    pub fn register(&self, scheme: Scheme) {
        self.counts
            .lock()
            .unwrap()
            .entry((scheme, String::new()))
            .or_insert(0);
    }

    fn request_counts(&self) -> Vec<RequestCount> {
        self.counts
            .lock()
            .unwrap()
            .iter()
            .map(|((scheme, method), count)| RequestCount {
                scheme: *scheme,
                method: method.clone(),
                count: *count,
            })
            .collect()
    }
}

/// `/metrics`/`GetMetrics` payload: `health`'s throughput counters +
/// `requests`' scheme-labeled counts. No DB access at all -- see
/// `floe_dto_http::health::MetricsResponse`'s own doc for why there's
/// no live UTxO-set gauge here anymore.
pub fn snapshot(health: &Health, requests: &Metrics) -> MetricsResponse {
    MetricsResponse {
        applied_total: health.applied_total(),
        rolled_back_total: health.rolled_back_total(),
        utxos_created_total: health.utxos_created_total(),
        utxos_spent_total: health.utxos_spent_total(),
        requests: requests.request_counts(),
    }
}

/// Wraps a whole scheme's server (gRPC's `tonic::transport::Server`,
/// or one http query listener's `axum::Router`) and records every
/// request's path into `Metrics`, labeled with this layer's fixed
/// `Scheme`. Counts only, no latency/status breakdown: a tonic
/// `Status` surfaces as an *Ok* HTTP/2 response carrying a
/// `grpc-status` trailer, not a transport-level `Err`, so splitting
/// ok/error here would mean buffering/inspecting every response --
/// not worth it for a first cut at throughput (not success-rate).
#[derive(Clone)]
pub struct MetricsLayer {
    metrics: Arc<Metrics>,
    scheme: Scheme,
}

impl MetricsLayer {
    pub fn new(metrics: Arc<Metrics>, scheme: Scheme) -> Self {
        Self { metrics, scheme }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            metrics: self.metrics.clone(),
            scheme: self.scheme,
        }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    metrics: Arc<Metrics>,
    scheme: Scheme,
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
        // The request path: a gRPC method
        // ("/floe.v1.query.QueryService/SearchUtxos") or an http route
        // ("/utxos").
        self.metrics.record(self.scheme, req.uri().path());
        self.inner.call(req)
    }
}
