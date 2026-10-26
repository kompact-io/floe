//! JSON scheme: `Accept`/`Content-Type: application/json` over
//! `common::Client`. `wasm::WasmClient` wraps this directly.

use crate::Result;

use super::common;
use super::json_wire::decode;
use crate::common::Backend;
use floe_dto_http::health::{HEALTH_PATH, HealthResponse, METRICS_PATH, MetricsResponse};
use floe_dto_http::query::{
    ReadDataRequest, ReadDataResponse, ReadTxRequest, ReadTxResponse, ReadUtxosRequest,
    ReadUtxosResponse, SearchUtxosRequest, SearchUtxosResponse,
};
use floe_dto_http::submit::SubmitTxResponse;

#[derive(Clone)]
pub struct Client {
    http: common::Client,
    base: String,
}

impl Client {
    /// `async fn(&str) -> Result<Self>` to mirror `grpc::Client::connect`
    /// even though it does no I/O.
    pub async fn connect(server: &str) -> Result<Self> {
        Self::new(server)
    }

    /// Sync counterpart of `connect`, for callers (`wasm::WasmClient`'s
    /// constructor) that can't be `async`.
    pub(crate) fn new(server: &str) -> Result<Self> {
        Ok(Self {
            http: common::Client::new()?,
            base: common::base_url(server),
        })
    }

    async fn fetch<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let (status, body) = self.http.get(&self.base, path, "application/json").await?;
        decode(status, &body)
    }

    async fn request<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        let body = serde_json::to_vec(req)?;
        let (status, body) = self
            .http
            .post(
                &self.base,
                path,
                &body,
                "application/json",
                "application/json",
            )
            .await?;
        decode(status, &body)
    }

    /// 404 -> `Ok(None)`, for `read_tx`/`read_data`.
    async fn request_opt<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Option<Resp>> {
        let body = serde_json::to_vec(req)?;
        let (status, body) = self
            .http
            .post(
                &self.base,
                path,
                &body,
                "application/json",
                "application/json",
            )
            .await?;
        if status == 404 {
            return Ok(None);
        }
        decode(status, &body).map(Some)
    }
}

impl Backend for Client {
    async fn read_utxos(&mut self, req: ReadUtxosRequest) -> Result<ReadUtxosResponse> {
        self.request(floe_dto_http::query::READ_UTXOS_PATH, &req)
            .await
    }

    async fn search_utxos(&mut self, req: SearchUtxosRequest) -> Result<SearchUtxosResponse> {
        self.request(floe_dto_http::query::SEARCH_UTXOS_PATH, &req)
            .await
    }

    async fn read_data(&mut self, req: ReadDataRequest) -> Result<Option<ReadDataResponse>> {
        self.request_opt(floe_dto_http::query::READ_DATA_PATH, &req)
            .await
    }

    async fn read_tx(&mut self, req: ReadTxRequest) -> Result<Option<ReadTxResponse>> {
        self.request_opt(floe_dto_http::query::READ_TX_PATH, &req)
            .await
    }

    async fn submit_tx(&mut self, raw: Vec<u8>) -> Result<SubmitTxResponse> {
        let (status, body) = self
            .http
            .post(
                &self.base,
                floe_dto_http::submit::SUBMIT_TX_PATH,
                &raw,
                "application/cbor",
                "application/json",
            )
            .await?;
        decode(status, &body)
    }

    async fn get_health(&mut self) -> Result<HealthResponse> {
        self.fetch(HEALTH_PATH).await
    }

    async fn get_metrics(&mut self) -> Result<MetricsResponse> {
        self.fetch(METRICS_PATH).await
    }
}
