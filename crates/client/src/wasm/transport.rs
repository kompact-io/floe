//! wasm::transport: a `Backend` over browser `fetch`, replacing
//! `http_common.rs`'s `TcpStream` (unavailable on wasm32). Same JSON
//! wire format as `http_json.rs`, shared via `crate::json_wire`.

use anyhow::{Result, anyhow, bail};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{Headers, Request, RequestInit, Response};

use crate::common::Backend;
use crate::json_wire::decode;
use floe_dto_http::health::{HEALTH_PATH, HealthResponse, METRICS_PATH, MetricsResponse};
use floe_dto_http::query::{
    ReadDataRequest, ReadDataResponse, ReadTxRequest, ReadTxResponse, ReadUtxosRequest,
    ReadUtxosResponse, SearchUtxosRequest, SearchUtxosResponse,
};
use floe_dto_http::submit::SubmitTxResponse;

/// `base` prefixes every path (`""` = same-origin root).
///
/// `Clone`: just a `String`, no connection state -- lets
/// `wasm::WasmClient`'s `&self` methods clone one per call instead of
/// needing `&mut`.
#[derive(Clone)]
pub struct Client {
    base: String,
}

impl Client {
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
        }
    }

    async fn get(&self, path: &str, accept: &str) -> Result<(u16, Vec<u8>)> {
        self.send("GET", path, None, accept).await
    }

    async fn post(&self, path: &str, body: &[u8], accept: &str) -> Result<(u16, Vec<u8>)> {
        self.send("POST", path, Some(body), accept).await
    }

    async fn send(
        &self,
        method: &str,
        path: &str,
        body: Option<&[u8]>,
        accept: &str,
    ) -> Result<(u16, Vec<u8>)> {
        let opts = RequestInit::new();
        opts.set_method(method);
        let headers = Headers::new().map_err(js_err)?;
        headers.set("Accept", accept).map_err(js_err)?;
        if let Some(body) = body {
            headers
                .set("Content-Type", "application/json")
                .map_err(js_err)?;
            let text = str::from_utf8(body).map_err(|e| anyhow!("request body not utf8: {e}"))?;
            opts.set_body(&JsValue::from_str(text));
        }
        opts.set_headers(&headers);

        let url = format!("{}{path}", self.base);
        let request = Request::new_with_str_and_init(&url, &opts).map_err(js_err)?;

        let window = web_sys::window()
            .ok_or_else(|| anyhow!("fetch: no `window` (not running in a browser)"))?;
        let resp_value = JsFuture::from(window.fetch_with_request(&request))
            .await
            .map_err(js_err)?;
        let resp: Response = resp_value
            .dyn_into()
            .map_err(|_| anyhow!("fetch: response wasn't a Response"))?;
        let status = resp.status();
        let text_promise = resp.text().map_err(js_err)?;
        let text = JsFuture::from(text_promise).await.map_err(js_err)?;
        let text = text
            .as_string()
            .ok_or_else(|| anyhow!("fetch: response body wasn't text"))?;
        Ok((status, text.into_bytes()))
    }

    async fn request<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Resp> {
        let body = serde_json::to_vec(req)?;
        let (status, body) = self.post(path, &body, "application/json").await?;
        decode(status, &body)
    }

    /// 404 -> `Ok(None)`, same convention as `http_json::Client`.
    async fn request_opt<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        req: &Req,
    ) -> Result<Option<Resp>> {
        let body = serde_json::to_vec(req)?;
        let (status, body) = self.post(path, &body, "application/json").await?;
        if status == 404 {
            return Ok(None);
        }
        decode(status, &body).map(Some)
    }
}

fn js_err(e: JsValue) -> anyhow::Error {
    anyhow!("{}", e.as_string().unwrap_or_else(|| format!("{e:?}")))
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
        // No submit UI in the demo -- not wired up.
        let _ = raw;
        bail!("submit_tx: not supported over the wasm fetch transport")
    }

    async fn get_health(&mut self) -> Result<HealthResponse> {
        let (status, body) = self.get(HEALTH_PATH, "application/json").await?;
        decode(status, &body)
    }

    async fn get_metrics(&mut self) -> Result<MetricsResponse> {
        let (status, body) = self.get(METRICS_PATH, "application/json").await?;
        decode(status, &body)
    }
}
