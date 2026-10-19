//! wasm: `#[wasm_bindgen]`-facing surface, `wasm32`+`wasm` feature only.
//! Everything wasm-only lives here or under `wasm/`.
//!
//! Methods take/return JSON strings (demo JS does `JSON.stringify`/
//! `parse`), not `serde-wasm-bindgen` `JsValue`s -- same shape as
//! `floe_dto_http::query::*`. Errors are the `anyhow::Error`'s message
//! as a JS exception string; fine for a demo console.

mod transport;

use wasm_bindgen::prelude::*;

use crate::common::Backend;

/// One per instance, pointed at `base` (`""` = same-origin root).
#[wasm_bindgen]
pub struct WasmClient(transport::Client);

#[wasm_bindgen]
impl WasmClient {
    #[wasm_bindgen(constructor)]
    pub fn new(base: &str) -> Self {
        Self(transport::Client::new(base))
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

/// JS counterpart of `--address` on the CLI. `shelley_hashes` only
/// covers payment addresses, so `Stake` is decomposed here instead.
#[wasm_bindgen(js_name = decodeAddress)]
pub fn decode_address(address: String) -> Result<String, String> {
    let addr: pallas_addresses::Address = address.parse().map_err(|e| format!("{e}"))?;
    let decoded = match &addr {
        pallas_addresses::Address::Stake(stake) => DecodedAddress {
            pay_hash: None,
            stake_hash: Some(hex::encode(stake.payload().as_hash())),
        },
        _ => {
            let (pay_hash, stake_hash) = crate::shelley_hashes(&addr).map_err(|e| e.to_string())?;
            DecodedAddress {
                pay_hash: Some(hex::encode(pay_hash)),
                stake_hash: stake_hash.map(hex::encode),
            }
        }
    };
    serde_json::to_string(&decoded).map_err(|e| e.to_string())
}
