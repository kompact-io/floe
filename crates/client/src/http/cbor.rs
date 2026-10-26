//! CBOR scheme: `Accept`/`Content-Type: application/cbor` over
//! `common::Client`. Mirror of `json.rs`.

use crate::{Error, Result};

use super::common;
use crate::common::Backend;
use floe_dto_http::ErrorBody;
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
    pub async fn connect(server: &str) -> Result<Self> {
        Self::new(server)
    }

    pub(crate) fn new(server: &str) -> Result<Self> {
        Ok(Self {
            http: common::Client::new()?,
            base: common::base_url(server),
        })
    }

    async fn fetch<T: for<'b> minicbor::Decode<'b, ()>>(&self, path: &str) -> Result<T> {
        let (status, body) = self.http.get(&self.base, path, "application/cbor").await?;
        decode(status, &body)
    }

    async fn request<Req: minicbor::Encode<()>, Resp: for<'b> minicbor::Decode<'b, ()>>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        let mut body = Vec::new();
        minicbor::encode(req, &mut body)?;
        let (status, body) = self
            .http
            .post(
                &self.base,
                path,
                &body,
                "application/cbor",
                "application/cbor",
            )
            .await?;
        decode(status, &body)
    }

    async fn request_opt<Req: minicbor::Encode<()>, Resp: for<'b> minicbor::Decode<'b, ()>>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Option<Resp>> {
        let mut body = Vec::new();
        minicbor::encode(req, &mut body)?;
        let (status, body) = self
            .http
            .post(
                &self.base,
                path,
                &body,
                "application/cbor",
                "application/cbor",
            )
            .await?;
        if status == 404 {
            return Ok(None);
        }
        decode(status, &body).map(Some)
    }
}

fn decode<T: for<'b> minicbor::Decode<'b, ()>>(status: u16, body: &[u8]) -> Result<T> {
    if !(200..300).contains(&status) {
        let msg = minicbor::decode::<ErrorBody>(body)
            .map(|e| e.error)
            .unwrap_or_else(|_| hex::encode(body));
        return Err(Error::msg(format!("http {status}: {msg}")));
    }
    Ok(minicbor::decode(body)?)
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
                "application/cbor",
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
