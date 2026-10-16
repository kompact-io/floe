//! metrics: core, scheme-labeled request-count state + `MetricsDto`
//! snapshot assembly. Zero knowledge of HTTP, gRPC, or wire format --
//! `scheme::grpc::metrics`'s tower layer *writes into* this state
//! (`record`); every scheme's handlers just snapshot and render it.

use std::collections::HashMap;
use std::sync::Mutex;

pub use floe_dto_http::Scheme;
use floe_dto_http::{MetricsDto, RequestCount};

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

/// `run` mode's `/metrics`/`GetMetrics` payload: `health`'s throughput
/// counters + `utxo_count` (DB-sourced, fetched by the caller) +
/// `requests`' scheme-labeled counts.
pub fn snapshot(health: &Health, utxo_count: u64, requests: &Metrics) -> MetricsDto {
    MetricsDto {
        applied_total: health.applied_total(),
        rolled_back_total: health.rolled_back_total(),
        utxos_created_total: health.utxos_created_total(),
        utxo_count,
        requests: requests.request_counts(),
    }
}

/// `serve` (read-replica) mode's `/metrics`/`GetMetrics` payload: no
/// local throughput counters (nothing applies blocks here) -- just
/// `utxo_count` + request counts.
pub fn replica_snapshot(utxo_count: u64, requests: &Metrics) -> MetricsDto {
    MetricsDto {
        applied_total: 0,
        rolled_back_total: 0,
        utxos_created_total: 0,
        utxo_count,
        requests: requests.request_counts(),
    }
}
