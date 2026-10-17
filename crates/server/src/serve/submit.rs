//! submit: generic tx-submission logic -- decode, and forward raw tx
//! bytes to amaru's HTTP Cardano Submit API at `POST /api/submit/tx`.
//! `Ctx::submit` (in `ctx.rs`) is the one shared operation both
//! schemes' `SubmitTx` call; this module only holds the low-level
//! pieces it's built from and the scheme-agnostic `SubmitOutcome` it
//! returns -- it knows nothing about `Ctx` itself.
//!
//! Stateless on purpose: each request opens its own one-shot
//! connection -- amaru's submit-api is local and this is low-volume
//! by nature (one tx per call).
//!
//! Scheme-specific translation (`scheme::grpc::submit`'s
//! `SubmitService` impl, `scheme::http::query`'s `submit_handler`)
//! lives in each scheme's own module -- they only ever see
//! `SubmitOutcome`, never these pieces directly.

use anyhow::Result;
use pallas_traverse::MultiEraTx;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub(crate) fn decode_tx_bytes(bytes: &[u8]) -> Result<MultiEraTx<'_>> {
    MultiEraTx::decode(bytes).map_err(|e| anyhow::anyhow!("invalid transaction: {e}"))
}

/// Posts raw tx bytes to amaru's submit-api over a plain TCP
/// connection, hand-rolling the HTTP/1.1 request/response -- a
/// one-shot localhost POST has no need for a full HTTP client
/// dependency. Returns amaru's status code and response body
/// verbatim; `Ctx::submit` is the only place that interprets them.
pub(crate) async fn post_tx(addr: &str, tx_bytes: &[u8]) -> Result<(u16, String)> {
    let mut stream = TcpStream::connect(addr).await?;
    let request = format!(
        "POST /api/submit/tx HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Type: application/cbor\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        tx_bytes.len()
    );
    stream.write_all(request.as_bytes()).await?;
    stream.write_all(tx_bytes).await?;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;

    let split = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response from amaru's submit-api"))?;
    let header_text = String::from_utf8_lossy(&resp[..split]).into_owned();
    let body = String::from_utf8_lossy(&resp[split + 4..]).into_owned();
    let status: u16 = header_text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP status line: {header_text:?}"))?;
    Ok((status, body))
}

/// Outcome of `Ctx::submit`, scheme-agnostic -- each scheme maps this
/// to its own wire shape (gRPC: `Status::invalid_argument`/
/// `unavailable`; http: 400/503) at its own translation boundary.
pub enum SubmitOutcome {
    Accepted {
        tx_hash: Vec<u8>,
    },
    /// Covers both "floe couldn't even decode it" and "amaru decoded
    /// it fine but rejected it" -- both are the caller's tx being
    /// wrong, not floe's or amaru's own availability.
    Rejected {
        reason: String,
    },
    /// amaru's submit-api itself is unreachable.
    Unavailable {
        reason: String,
    },
}
