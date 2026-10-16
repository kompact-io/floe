//! http_common: shared REST-transport plumbing for `http_json`/
//! `http_cbor` -- the raw HTTP/1.1 socket I/O (identical regardless of
//! wire format) and the path/query-string builders (identical business
//! logic regardless of wire format too). `http_json.rs`/`http_cbor.rs`
//! each add only their own `Accept`/`Content-Type` string and
//! encode/decode glue on top of this.

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::common::{ChangesParams, Cred, Paging, TxHash};

/// Strips the scheme `--server`'s gRPC-flavored default carries --
/// REST hand-rolls HTTP/1.1 over a raw socket, so it wants bare
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

/// `POST /tx`'s shape: body is always raw bytes (`application/cbor`,
/// matching `floe-server`'s own convention talking to amaru), only the
/// response is format-negotiated -- same split `scheme::http`'s
/// `submit_handler` makes server-side.
pub(crate) async fn post(addr: &str, path: &str, body: &[u8], accept: &str) -> Result<(u16, Vec<u8>)> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/cbor\r\nAccept: {accept}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(body).await?;
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    parse_response(&resp)
}

fn push_query(qs: &mut Vec<String>, key: &str, value: impl std::fmt::Display) {
    qs.push(format!("{key}={value}"));
}

/// `GET /utxos` for `pay_cred`/`stake_cred` (and, via `ref_path` below,
/// `ref`) -- same query shape `floe-server`'s `scheme::http` parses.
pub(crate) fn utxos_path(pay_cred: Option<Cred>, stake_cred: Option<Cred>, paging: &Paging) -> String {
    let mut qs = Vec::new();
    if let Some(p) = pay_cred {
        push_query(&mut qs, "pay_cred", hex::encode(p));
    }
    if let Some(d) = stake_cred {
        push_query(&mut qs, "stake_cred", hex::encode(d));
    }
    if let Some(after) = &paging.after {
        push_query(&mut qs, "after", after);
    }
    if let Some(limit) = paging.limit {
        push_query(&mut qs, "limit", limit);
    }
    format!("/utxos?{}", qs.join("&"))
}

pub(crate) fn ref_path(tx_hash: TxHash, ix: u32) -> String {
    format!("/utxos?ref={}:{ix}", hex::encode(tx_hash))
}

pub(crate) fn tx_path(tx_hash: TxHash) -> String {
    format!("/tx/{}", hex::encode(tx_hash))
}

pub(crate) fn data_path(hash: TxHash) -> String {
    format!("/data/{}", hex::encode(hash))
}

pub(crate) fn changes_path(params: &ChangesParams) -> String {
    let mut qs = Vec::new();
    if let Some(p) = params.pay_cred {
        push_query(&mut qs, "pay_cred", hex::encode(p));
    }
    if let Some(d) = params.stake_cred {
        push_query(&mut qs, "stake_cred", hex::encode(d));
    }
    if let Some(s) = params.since_slot {
        push_query(&mut qs, "since_slot", s);
    }
    if let Some(h) = &params.since_hash {
        push_query(&mut qs, "since_hash", hex::encode(h));
    }
    if let Some(n) = params.max_items {
        push_query(&mut qs, "limit", n);
    }
    if let Some(c) = &params.created_after {
        push_query(&mut qs, "created_after", c);
    }
    if let Some(c) = &params.spent_after {
        push_query(&mut qs, "spent_after", c);
    }
    format!("/changes?{}", qs.join("&"))
}
