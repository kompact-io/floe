//! http_common: shared http-transport plumbing for `http_json`/
//! `http_cbor` -- just the raw HTTP/1.1 socket I/O, identical
//! regardless of wire format. `http_json.rs`/`http_cbor.rs` each add
//! only their own `Accept`/`Content-Type` string and request/response
//! encode/decode glue on top of this.
//!
//! Every RPC under `/query`/`/submit` (`floe_dto_http::query`/
//! `submit`'s `*_PATH` constants) is POST with a body -- the request
//! DTO travels the same way the response does, so there are no
//! query-string builders here. `/health`/`/metrics` are the one
//! exception still served as a bodyless GET (see `get` below).

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Strips the scheme `--server`'s gRPC-flavored default carries --
/// this client hand-rolls HTTP/1.1 over a raw socket, so it wants bare
/// host:port.
pub fn strip_scheme(server: &str) -> &str {
    server
        .trim_start_matches("http://")
        .trim_start_matches("https://")
}

fn parse_response(resp: &[u8]) -> Result<(u16, Vec<u8>)> {
    let split = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response"))?;
    let header_text = String::from_utf8_lossy(&resp[..split]).into_owned();
    let status: u16 = header_text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP status line: {header_text:?}"))?;
    Ok((status, resp[split + 4..].to_vec()))
}

/// Bodyless GET -- only `/health`/`/metrics` still use this (see
/// module doc); every RPC under `/query`/`/submit` is `post` below.
pub(crate) async fn get(addr: &str, path: &str, accept: &str) -> Result<(u16, Vec<u8>)> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: {accept}\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    parse_response(&resp)
}

/// Every RPC (`SubmitTx`'s raw tx bytes, or a `*Request` DTO already
/// encoded by the caller in the same format as `accept`) goes through
/// this one POST -- `content_type` is `application/cbor` unconditionally
/// for `SubmitTx` (matching `floe-server`'s own convention talking to
/// amaru, and gRPC's `AnyChainTx.raw`, regardless of wire format),
/// otherwise whatever `http_json`/`http_cbor` are themselves encoding.
pub(crate) async fn post(
    addr: &str,
    path: &str,
    body: &[u8],
    content_type: &str,
    accept: &str,
) -> Result<(u16, Vec<u8>)> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: {content_type}\r\nAccept: {accept}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    parse_response(&resp)
}

/// `/health`/`/metrics` live on `floe-server`'s own dedicated listener
/// (`--metrics`), not the query listener this client's `addr` points
/// at -- these paths only resolve when a deployment happens to
/// colocate the two. See `Backend::health`'s doc for how a caller
/// should treat that.
pub(crate) const HEALTH_PATH: &str = "/health";
pub(crate) const METRICS_PATH: &str = "/metrics";
