//! grpc::health: floe's own `HealthService` RPC (`GetHealth`/
//! `GetMetrics`) -- reporting apparatus only, mirroring HTTP's
//! `/health`/`/metrics` method-for-route (see
//! `../http/metrics.rs`). Each handler snapshots the crate-root
//! `crate::health`/`crate::metrics` core (fetching whatever DB values
//! that needs first, for `serve` mode) and converts to proto; all the
//! logic lives in the core, not here.

use std::sync::Arc;

use floe_dto_grpc::floe::v1::health as h;
pub use h::health_service_server::HealthServiceServer;
use floe_dto_http::{HealthResponseDto, MetricsDto, Readiness, RequestCount, Scheme, Stage};
use tonic::{Request, Response, Status};

use crate::health::Health;
use crate::metrics::Metrics;
use crate::{query, store};

pub struct Svc {
    pub db_path: Arc<str>,
    /// `Some` for `run`, `None` for `serve` -- see `super::super`'s module doc.
    pub health: Option<Arc<Health>>,
    pub requests: Arc<Metrics>,
}

fn join_err(e: tokio::task::JoinError) -> Status {
    Status::internal(e.to_string())
}

fn stage_proto(s: Stage) -> i32 {
    match s {
        Stage::Seeding => h::Stage::Seeding as i32,
        Stage::Catchup => h::Stage::Catchup as i32,
        Stage::Running => h::Stage::Running as i32,
    }
}

fn readiness_proto(r: Readiness) -> i32 {
    match r {
        Readiness::Ready => h::Readiness::Ready as i32,
        Readiness::Unavailable => h::Readiness::Unavailable as i32,
    }
}

fn scheme_proto(s: Scheme) -> i32 {
    match s {
        Scheme::Grpc => h::Scheme::Grpc as i32,
        Scheme::HttpJson => h::Scheme::HttpJson as i32,
        Scheme::HttpCbor => h::Scheme::HttpCbor as i32,
    }
}

fn health_response_proto(dto: HealthResponseDto) -> h::HealthResponse {
    let mode = match dto {
        HealthResponseDto::Run(r) => h::health_response::Mode::Run(h::RunHealth {
            stage: stage_proto(r.stage),
            tip_slot: r.tip_slot,
            block_no: r.block_no,
            tip_hash: r.tip_hash.to_vec(),
            peer_tip_block_no: r.peer_tip_block_no,
        }),
        HealthResponseDto::Replica(r) => h::health_response::Mode::Replica(h::ReplicaHealth {
            readiness: readiness_proto(r.readiness),
            tip_slot: r.tip_slot,
            utxo_count: r.utxo_count,
            upstream_mode: r.upstream_mode as u32,
        }),
    };
    h::HealthResponse { mode: Some(mode) }
}

fn metrics_response_proto(dto: MetricsDto) -> h::MetricsResponse {
    h::MetricsResponse {
        applied_total: dto.applied_total,
        rolled_back_total: dto.rolled_back_total,
        utxos_created_total: dto.utxos_created_total,
        utxo_count: dto.utxo_count,
        requests: dto
            .requests
            .into_iter()
            .map(|RequestCount { scheme, method, count }| h::RequestCount {
                scheme: scheme_proto(scheme),
                method,
                count,
            })
            .collect(),
    }
}

#[tonic::async_trait]
impl h::health_service_server::HealthService for Svc {
    async fn get_health(
        &self,
        _request: Request<()>,
    ) -> Result<Response<h::HealthResponse>, Status> {
        let dto = match &self.health {
            Some(health) => HealthResponseDto::Run(health.snapshot()),
            None => {
                let db_path = self.db_path.clone();
                let result = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                    let conn = query::open_reader(&db_path)?;
                    let tip_slot = store::tip(&conn)?.map(|(slot, _)| slot);
                    let utxo_count = store::utxo_count(&conn)?;
                    let upstream_mode = store::get_mode(&conn)?;
                    Ok((tip_slot, utxo_count, upstream_mode))
                })
                .await
                .map_err(join_err)?;
                match result {
                    Ok((tip_slot, utxo_count, upstream_mode)) => {
                        HealthResponseDto::Replica(crate::health::replica_snapshot(
                            Readiness::Ready,
                            tip_slot,
                            utxo_count,
                            upstream_mode,
                        ))
                    }
                    Err(_) => HealthResponseDto::Replica(crate::health::replica_snapshot(
                        Readiness::Unavailable,
                        None,
                        0,
                        store::Mode::Catchup,
                    )),
                }
            }
        };
        Ok(Response::new(health_response_proto(dto)))
    }

    async fn get_metrics(
        &self,
        _request: Request<()>,
    ) -> Result<Response<h::MetricsResponse>, Status> {
        let db_path = self.db_path.clone();
        let utxo_count = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
            let conn = query::open_reader(&db_path)?;
            store::utxo_count(&conn)
        })
        .await
        .map_err(join_err)?
        .map_err(|e| Status::internal(e.to_string()))?;

        let dto = match &self.health {
            Some(health) => crate::metrics::snapshot(health, utxo_count, &self.requests),
            None => crate::metrics::replica_snapshot(utxo_count, &self.requests),
        };
        Ok(Response::new(metrics_response_proto(dto)))
    }
}
