//! grpc::health: floe's own `HealthService` RPC (`GetHealth`/
//! `GetMetrics`) -- reporting apparatus only, mirroring HTTP's
//! `/health`/`/metrics` method-for-route (see
//! `../http/metrics.rs`). Each handler snapshots the crate-root
//! `crate::health`/`crate::metrics` core and converts to proto; all
//! the logic lives in the core, not here.

use floe_dto_grpc::floe::v1::health as h;
use floe_dto_http::health::{HealthResponse, MetricsResponse, RequestCount, Scheme, Stage};
pub use h::health_service_server::HealthServiceServer;
use tonic::{Request, Response, Status};

use crate::serve::ctx::Ctx;

fn stage_proto(s: Stage) -> i32 {
    match s {
        Stage::Seeding => h::Stage::Seeding as i32,
        Stage::Catchup => h::Stage::Catchup as i32,
        Stage::Running => h::Stage::Running as i32,
    }
}

fn scheme_proto(s: Scheme) -> i32 {
    match s {
        Scheme::Grpc => h::Scheme::Grpc as i32,
        Scheme::HttpJson => h::Scheme::HttpJson as i32,
        Scheme::HttpCbor => h::Scheme::HttpCbor as i32,
    }
}

fn health_response_proto(dto: HealthResponse) -> h::HealthResponse {
    let mode = match dto {
        HealthResponse::Run(r) => h::health_response::Mode::Run(h::RunHealth {
            stage: stage_proto(r.stage),
            tip_slot: r.tip_slot,
            block_no: r.block_no,
            tip_hash: r.tip_hash.to_vec(),
            peer_tip_block_no: r.peer_tip_block_no,
        }),
    };
    h::HealthResponse { mode: Some(mode) }
}

fn metrics_response_proto(dto: MetricsResponse) -> h::MetricsResponse {
    h::MetricsResponse {
        applied_total: dto.applied_total,
        rolled_back_total: dto.rolled_back_total,
        utxos_created_total: dto.utxos_created_total,
        utxo_count: dto.utxo_count,
        requests: dto
            .requests
            .into_iter()
            .map(
                |RequestCount {
                     scheme,
                     method,
                     count,
                 }| h::RequestCount {
                    scheme: scheme_proto(scheme),
                    method,
                    count,
                },
            )
            .collect(),
    }
}

#[tonic::async_trait]
impl h::health_service_server::HealthService for Ctx {
    async fn get_health(
        &self,
        _request: Request<()>,
    ) -> Result<Response<h::HealthResponse>, Status> {
        let dto = HealthResponse::Run(self.health.snapshot());
        Ok(Response::new(health_response_proto(dto)))
    }

    async fn get_metrics(
        &self,
        _request: Request<()>,
    ) -> Result<Response<h::MetricsResponse>, Status> {
        let utxo_count = self
            .utxo_count()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let dto = crate::metrics::snapshot(&self.health, utxo_count, &self.requests);
        Ok(Response::new(metrics_response_proto(dto)))
    }
}
