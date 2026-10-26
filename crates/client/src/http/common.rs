//! Shared GET/POST transport for `json.rs`/`cbor.rs`: `native::Client`
//! (reqwest) or `browser::Client` (`gloo-net` fetch) -- picked by
//! target, never both compiled in. Same `(status, body)` shape either
//! way, so this module's `Client` is the only name callers need.

#[cfg(target_arch = "wasm32")]
mod browser;
#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(target_arch = "wasm32")]
pub use browser::Client;
#[cfg(not(target_arch = "wasm32"))]
pub use native::Client;

/// Normalizes `--server`'s bare `host:port` (defaults plaintext) and an
/// explicit `http(s)://` URL to the same shape.
pub fn base_url(server: &str) -> String {
    let server = server.trim_end_matches('/');
    if server.starts_with("http://") || server.starts_with("https://") {
        server.to_string()
    } else {
        format!("http://{server}")
    }
}
