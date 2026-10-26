//! JSON/CBOR wire schemes over `common`'s transport (reqwest native,
//! `gloo-net` on wasm32 -- see its module doc). Each independently
//! usable on either target.

#[cfg(feature = "http-cbor")]
pub mod cbor;
mod common;
#[cfg(feature = "http-json")]
pub mod json;
#[cfg(feature = "http-json")]
mod json_wire;

use crate::Backend;

/// Type-erases across the (up to two) HTTP wire schemes compiled in --
/// `wasm::WasmClient`'s runtime pick between them. `scheme::AnyClient`
/// does the analogous job across all three schemes including `grpc`,
/// but wasm never wants that variant, so this is its own, smaller type
/// rather than reusing that one.
#[derive(Clone)]
pub enum AnyHttpClient {
    #[cfg(feature = "http-json")]
    Json(json::Client),
    #[cfg(feature = "http-cbor")]
    Cbor(cbor::Client),
}

macro_rules! delegate {
    ($self:expr, |$c:ident| $call:expr) => {
        match $self {
            #[cfg(feature = "http-json")]
            AnyHttpClient::Json($c) => $call,
            #[cfg(feature = "http-cbor")]
            AnyHttpClient::Cbor($c) => $call,
            #[allow(unreachable_patterns)]
            _ => unreachable!("no http-json/http-cbor feature compiled in"),
        }
    };
}

#[allow(unused_variables)]
impl Backend for AnyHttpClient {
    async fn read_utxos(
        &mut self,
        req: floe_dto_http::query::ReadUtxosRequest,
    ) -> crate::Result<floe_dto_http::query::ReadUtxosResponse> {
        delegate!(self, |c| c.read_utxos(req).await)
    }
    async fn search_utxos(
        &mut self,
        req: floe_dto_http::query::SearchUtxosRequest,
    ) -> crate::Result<floe_dto_http::query::SearchUtxosResponse> {
        delegate!(self, |c| c.search_utxos(req).await)
    }
    async fn read_data(
        &mut self,
        req: floe_dto_http::query::ReadDataRequest,
    ) -> crate::Result<Option<floe_dto_http::query::ReadDataResponse>> {
        delegate!(self, |c| c.read_data(req).await)
    }
    async fn read_tx(
        &mut self,
        req: floe_dto_http::query::ReadTxRequest,
    ) -> crate::Result<Option<floe_dto_http::query::ReadTxResponse>> {
        delegate!(self, |c| c.read_tx(req).await)
    }
    async fn submit_tx(
        &mut self,
        raw: Vec<u8>,
    ) -> crate::Result<floe_dto_http::submit::SubmitTxResponse> {
        delegate!(self, |c| c.submit_tx(raw).await)
    }
    async fn get_health(&mut self) -> crate::Result<floe_dto_http::health::HealthResponse> {
        delegate!(self, |c| c.get_health().await)
    }
    async fn get_metrics(&mut self) -> crate::Result<floe_dto_http::health::MetricsResponse> {
        delegate!(self, |c| c.get_metrics().await)
    }
}
