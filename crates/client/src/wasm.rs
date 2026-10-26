//! wasm: `#[wasm_bindgen]`-facing surface, wasm32 only. Wraps
//! `http::AnyHttpClient`, picked at JS-instantiation time by
//! `WasmScheme` rather than a compile-time choice -- both
//! `http-json`/`http-cbor` can be compiled in at once. The JS-facing
//! methods always take/return JSON strings regardless of which wire
//! format talks to the server.
//!
//! Errors are the `crate::Error`'s message as a JS exception string;
//! fine for a demo console.

use wasm_bindgen::prelude::*;

use crate::Backend;
use crate::http::AnyHttpClient;

/// JS-facing scheme selector for `WasmClient::new` -- a real enum, not
/// a `&str`, so a typo is a JS compile/lint-time type error (TS union),
/// not a runtime one.
#[wasm_bindgen]
#[derive(Clone, Copy, Debug)]
pub enum WasmScheme {
    Json,
    Cbor,
}

/// One per instance, pointed at `base` (`""` = same-origin root).
#[wasm_bindgen]
pub struct WasmClient(AnyHttpClient);

#[wasm_bindgen]
impl WasmClient {
    /// Sync, not `connect` -- a JS `new` can't await (and neither
    /// scheme does any I/O here anyway, see
    /// `http::json::Client::connect`'s doc).
    #[wasm_bindgen(constructor)]
    pub fn new(base: &str, scheme: WasmScheme) -> Result<Self, String> {
        let client = match scheme {
            #[cfg(feature = "http-json")]
            WasmScheme::Json => AnyHttpClient::Json(
                crate::http::json::Client::new(base).map_err(|e| e.to_string())?,
            ),
            #[cfg(feature = "http-cbor")]
            WasmScheme::Cbor => AnyHttpClient::Cbor(
                crate::http::cbor::Client::new(base).map_err(|e| e.to_string())?,
            ),
            #[allow(unreachable_patterns)]
            other => return Err(format!("scheme {other:?} not compiled in")),
        };
        Ok(Self(client))
    }

    #[wasm_bindgen(js_name = readUtxos)]
    pub async fn read_utxos(&self, request_json: String) -> Result<String, String> {
        let req = serde_json::from_str(&request_json).map_err(|e| e.to_string())?;
        let resp = self
            .0
            .clone()
            .read_utxos(req)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::to_string(&resp).map_err(|e| e.to_string())
    }

    #[wasm_bindgen(js_name = searchUtxos)]
    pub async fn search_utxos(&self, request_json: String) -> Result<String, String> {
        let req = serde_json::from_str(&request_json).map_err(|e| e.to_string())?;
        let resp = self
            .0
            .clone()
            .search_utxos(req)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::to_string(&resp).map_err(|e| e.to_string())
    }

    #[wasm_bindgen(js_name = readTx)]
    pub async fn read_tx(&self, request_json: String) -> Result<String, String> {
        let req = serde_json::from_str(&request_json).map_err(|e| e.to_string())?;
        let resp = self
            .0
            .clone()
            .read_tx(req)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::to_string(&resp).map_err(|e| e.to_string())
    }

    #[wasm_bindgen(js_name = readData)]
    pub async fn read_data(&self, request_json: String) -> Result<String, String> {
        let req = serde_json::from_str(&request_json).map_err(|e| e.to_string())?;
        let resp = self
            .0
            .clone()
            .read_data(req)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::to_string(&resp).map_err(|e| e.to_string())
    }
}

#[derive(serde::Serialize)]
struct DecodedAddress {
    #[serde(skip_serializing_if = "Option::is_none")]
    pay_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stake_hash: Option<String>,
}

/// JS counterpart of `--address` on the CLI -- `address_hashes` is
/// shared with native, so a `Stake` address decodes the same way here
/// as it would there.
#[wasm_bindgen(js_name = decodeAddress)]
pub fn decode_address(address: String) -> Result<String, String> {
    let addr: amaru_kernel::address::Address = address
        .parse()
        .map_err(|_| format!("invalid address: {address}"))?;
    let (pay_hash, stake_hash) = crate::address_hashes(&addr).map_err(|e| e.to_string())?;
    let decoded = DecodedAddress {
        pay_hash: pay_hash.map(hex::encode),
        stake_hash: stake_hash.map(hex::encode),
    };
    serde_json::to_string(&decoded).map_err(|e| e.to_string())
}
