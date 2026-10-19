//! http_common: shared http-transport plumbing for `http_json`/
//! `http_cbor` -- one pooled `reqwest::Client` per `Client::new`, plus
//! GET/POST helpers that just set headers/body and hand back
//! `(status, body)`. `http_json.rs`/`http_cbor.rs` each add only their
//! own `Accept`/`Content-Type` string and request/response
//! encode/decode glue on top of this.
//!
//! `reqwest` (built with its `rustls-tls` backend, see
//! `crates/client/Cargo.toml`) picks TLS vs plaintext from the
//! `http://`/`https://` prefix itself -- the same `--http`/`--server`
//! string works against a plaintext dev server or a TLS-fronted
//! deployment alike, matching `wasm::transport`'s browser-`fetch`
//! behavior rather than requiring a second, TLS-only code path.
//!
//! Every RPC under `/query`/`/submit` (`floe_dto_http::query`/
//! `submit`'s `*_PATH` constants) is POST with a body -- the request
//! DTO travels the same way the response does, so there are no
//! query-string builders here. `/health`/`/metrics` are the one
//! exception still served as a bodyless GET (see `get` below).

use anyhow::{Context, Result};

/// Normalizes `--server`'s gRPC-flavored default (a bare `host:port`,
/// defaulting to plaintext) and an explicit `http://`/`https://` URL
/// to the same shape: no trailing slash, scheme always present.
pub fn base_url(server: &str) -> String {
    let server = server.trim_end_matches('/');
    if server.starts_with("http://") || server.starts_with("https://") {
        server.to_string()
    } else {
        format!("http://{server}")
    }
}

/// Bodyless GET -- only `/health`/`/metrics` still use this (see
/// module doc); every RPC under `/query`/`/submit` is `post` below.
pub(crate) async fn get(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    accept: &str,
) -> Result<(u16, Vec<u8>)> {
    let resp = http
        .get(format!("{base}{path}"))
        .header(reqwest::header::ACCEPT, accept)
        .send()
        .await
        .with_context(|| format!("GET {base}{path}"))?;
    let status = resp.status().as_u16();
    let body = resp.bytes().await.context("reading response body")?;
    Ok((status, body.into()))
}

/// Every RPC (`SubmitTx`'s raw tx bytes, or a `*Request` DTO already
/// encoded by the caller in the same format as `accept`) goes through
/// this one POST -- `content_type` is `application/cbor`
/// unconditionally for `SubmitTx` (matching `floe-server`'s own
/// convention talking to amaru, and gRPC's `AnyChainTx.raw`, regardless
/// of wire format), otherwise whatever `http_json`/`http_cbor` are
/// themselves encoding.
pub(crate) async fn post(
    http: &reqwest::Client,
    base: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    accept: &str,
) -> Result<(u16, Vec<u8>)> {
    let resp = http
        .post(format!("{base}{path}"))
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .header(reqwest::header::ACCEPT, accept)
        .body(body.to_vec())
        .send()
        .await
        .with_context(|| format!("POST {base}{path}"))?;
    let status = resp.status().as_u16();
    let body = resp.bytes().await.context("reading response body")?;
    Ok((status, body.into()))
}

// `floe_dto_http::health::{HEALTH_PATH, METRICS_PATH}` are `/health`/
// `/metrics` themselves -- `http_json.rs`/`http_cbor.rs` import those
// directly rather than this module re-exporting them. They live on
// `floe-server`'s own dedicated listener (`--metrics`), not the query
// listener this client's `addr` points at -- these paths only resolve
// when a deployment happens to colocate the two. See
// `Backend::health`'s doc for how a caller should treat that.
