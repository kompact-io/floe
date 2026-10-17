//! json_wire: JSON response decode shared by `http_json.rs` (native) and
//! `wasm::transport` (browser `fetch`) -- same wire format, different
//! socket I/O.

use anyhow::{Result, bail};
use floe_dto_http::ErrorBody;

pub fn decode<T: serde::de::DeserializeOwned>(status: u16, body: &[u8]) -> Result<T> {
    if !(200..300).contains(&status) {
        let msg = serde_json::from_slice::<ErrorBody>(body)
            .map(|e| e.error)
            .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned());
        bail!("http {status}: {msg}");
    }
    Ok(serde_json::from_slice(body)?)
}
