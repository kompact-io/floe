//! grpc::submit: `SubmitService` -- `SubmitTx` translates `Ctx::submit`'s
//! scheme-agnostic `SubmitOutcome` into tonic's wire shape; every other
//! RPC needs mempool visibility floe doesn't have and returns
//! `Status::unimplemented`.

use floe_dto_grpc::floe::v1::submit as s;
use s::submit_service_server::SubmitService;
use tonic::{Request, Response, Status};

use crate::serve::ctx::Ctx;
use crate::serve::submit::SubmitOutcome;

#[tonic::async_trait]
impl SubmitService for Ctx {
    async fn eval_tx(
        &self,
        _request: Request<s::EvalTxRequest>,
    ) -> Result<Response<s::EvalTxResponse>, Status> {
        Err(Status::unimplemented(
            "EvalTx is not supported -- floe has no ledger state to evaluate against",
        ))
    }

    async fn submit_tx(
        &self,
        request: Request<s::SubmitTxRequest>,
    ) -> Result<Response<s::SubmitTxResponse>, Status> {
        let req = request.into_inner();
        let raw = match req.tx.and_then(|tx| tx.r#type) {
            Some(s::any_chain_tx::Type::Raw(bytes)) => bytes,
            None => return Err(Status::invalid_argument("tx.raw is required")),
        };
        match self.submit(&raw).await {
            SubmitOutcome::Accepted { tx_hash } => {
                Ok(Response::new(s::SubmitTxResponse { r#ref: tx_hash }))
            }
            SubmitOutcome::Rejected { reason } => Err(Status::invalid_argument(reason)),
            SubmitOutcome::Unavailable { reason } => Err(Status::unavailable(reason)),
        }
    }

    type WaitForTxStream = tonic::codegen::BoxStream<s::WaitForTxResponse>;

    async fn wait_for_tx(
        &self,
        _request: Request<s::WaitForTxRequest>,
    ) -> Result<Response<Self::WaitForTxStream>, Status> {
        Err(Status::unimplemented(
            "WaitForTx is not supported -- floe has no mempool visibility beyond submit/reject",
        ))
    }

    async fn read_mempool(
        &self,
        _request: Request<s::ReadMempoolRequest>,
    ) -> Result<Response<s::ReadMempoolResponse>, Status> {
        Err(Status::unimplemented(
            "ReadMempool is not supported -- floe has no mempool visibility beyond submit/reject",
        ))
    }

    type WatchMempoolStream = tonic::codegen::BoxStream<s::WatchMempoolResponse>;

    async fn watch_mempool(
        &self,
        _request: Request<s::WatchMempoolRequest>,
    ) -> Result<Response<Self::WatchMempoolStream>, Status> {
        Err(Status::unimplemented(
            "WatchMempool is not supported -- floe has no mempool visibility beyond submit/reject",
        ))
    }
}
