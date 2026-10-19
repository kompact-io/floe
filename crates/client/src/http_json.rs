//! http_json: the JSON wire format over `http_common`'s pooled
//! `reqwest::Client` -- `Accept: application/json`,
//! `Content-Type: application/json`, `serde_json`/`floe-dto-http`'s
//! `Serialize`/`Deserialize` derives for both the request and the
//! response.

use anyhow::{Context, Result};

use crate::common::Backend;
use crate::http_common;
use crate::json_wire::decode;
use floe_dto_http::health::{HEALTH_PATH, HealthResponse, METRICS_PATH, MetricsResponse};
use floe_dto_http::query::{
    ReadDataRequest, ReadDataResponse, ReadTxRequest, ReadTxResponse, ReadUtxosRequest,
    ReadUtxosResponse, SearchUtxosRequest, SearchUtxosResponse,
};
use floe_dto_http::submit::SubmitTxResponse;

pub struct Client {
    http: reqwest::Client,
    base: String,
}

impl Client {
    /// Mirrors `grpc::Client::connect`'s shape (`async fn(&str) ->
    /// Result<Self>`) even though building a pooled `reqwest::Client`
    /// does no I/O itself -- every scheme's `Client` connects the same
    /// way from `scheme::connect`'s point of view, so a future retry/
    /// readiness wait here (matching gRPC's own) wouldn't change the
    /// call site.
    pub async fn connect(server: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .build()
            .context("building the HTTP client")?;
        Ok(Self {
            http,
            base: http_common::base_url(server),
        })
    }

    async fn fetch<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let (status, body) =
            http_common::get(&self.http, &self.base, path, "application/json").await?;
        decode(status, &body)
    }

    /// POSTs `req` as a JSON body and decodes the response the same
    /// way -- every `/query`/`/submit` RPC's shape (see
    /// `http_common::post`'s own doc).
    async fn request<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        let body = serde_json::to_vec(req)?;
        let (status, body) = http_common::post(
            &self.http,
            &self.base,
            path,
            &body,
            "application/json",
            "application/json",
        )
        .await?;
        decode(status, &body)
    }

    /// Like `request`, but a 404 is `Ok(None)` instead of `Err` -- for
    /// `read_tx`/`read_data`, whose `Backend` signature reports "not
    /// found" that way.
    async fn request_opt<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Option<Resp>> {
        let body = serde_json::to_vec(req)?;
        let (status, body) = http_common::post(
            &self.http,
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
        self.request(floe_dto_http::query::READ_UTXO_PATH, &req)
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
        let (status, body) = http_common::post(
            &self.http,
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
