//! submit: `floe-server`'s gRPC `SubmitService` -- `SubmitTx` only.
//! `EvalTx`/`WaitForTx`/`ReadMempool`/`WatchMempool` all need mempool
//! visibility floe doesn't have and return `Status::unimplemented`.
//!
//! Forwards raw tx bytes to amaru's own HTTP Cardano Submit API
//! (`POST /api/submit/tx`) rather than speaking the Ouroboros
//! LocalTxSubmission mini-protocol to a `cardano-node` socket: amaru
//! doesn't expose one (its CLI only has `--submit-api-listen-on`), so
//! the LocalTxSubmission client this used to be built on never had
//! anything to connect to in this stack -- confirmed dead code, not a
//! style preference. floe does no re-validation of its own: a
//! rejection's reason is amaru's own response body, passed through
//! verbatim, not reinterpreted.
//!
//! Stateless on purpose: each request opens its own one-shot
//! connection (no keep-alive, no connection pool) -- amaru's
//! submit-api is local and this is low-volume by nature (one tx per
//! call), so there's no shared mutable state here worth the
//! complexity a persistent connection would need.

use anyhow::Result;
use pallas_traverse::MultiEraTx;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tonic::{Request, Response, Status};

use floe_dto_grpc::floe::v1::submit as s;
use s::submit_service_server::SubmitService;

pub struct Svc {
    amaru_submit_api: String,
}

impl Svc {
    pub fn new(amaru_submit_api: String) -> Self {
        Self { amaru_submit_api }
    }
}

/// `anyhow`, not `tonic::Status` -- shared by both `SubmitTx`
/// transports (this module's own gRPC `Svc` and `http.rs`'s REST
/// handler), each of which maps the error to its own wire shape
/// (`Status::invalid_argument` / HTTP 400).
pub(crate) fn decode_tx_bytes(bytes: &[u8]) -> Result<MultiEraTx<'_>> {
    MultiEraTx::decode(bytes).map_err(|e| anyhow::anyhow!("invalid transaction: {e}"))
}

fn decode_tx(bytes: &[u8]) -> Result<MultiEraTx<'_>, Status> {
    decode_tx_bytes(bytes).map_err(|e| Status::invalid_argument(e.to_string()))
}

/// Posts raw tx bytes to amaru's submit-api over a plain TCP
/// connection, hand-rolling the HTTP/1.1 request/response -- a
/// one-shot localhost POST has no need for a full HTTP client
/// dependency. Returns amaru's status code and response body
/// verbatim; callers (this module's `submit_tx`, `http.rs`'s REST
/// handler) are the only places that interpret them.
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

#[tonic::async_trait]
impl SubmitService for Svc {
    async fn eval_tx(
        &self,
        _request: Request<s::EvalTxRequest>,
    ) -> Result<Response<s::EvalTxResponse>, Status> {
        Err(Status::unimplemented(
            "EvalTx is not supported -- floe has no ledger state to evaluate against",
        ))
    }

    async fn submit_tx(
        &self,
        request: Request<s::SubmitTxRequest>,
    ) -> Result<Response<s::SubmitTxResponse>, Status> {
        let req = request.into_inner();
        let raw = match req.tx.and_then(|tx| tx.r#type) {
            Some(s::any_chain_tx::Type::Raw(bytes)) => bytes,
            None => return Err(Status::invalid_argument("tx.raw is required")),
        };
        // Decoded for its own sake (catches garbage before any network
        // call) and for `tx.hash()` -- the transaction id, computed the
        // same way regardless of which backend accepts it, so there's
        // no need to parse one back out of amaru's response body.
        let tx = decode_tx(&raw)?;
        let tx_hash = tx.hash().to_vec();

        match post_tx(&self.amaru_submit_api, &raw).await {
            Ok((status, _body)) if (200..300).contains(&status) => {
                Ok(Response::new(s::SubmitTxResponse { r#ref: tx_hash }))
            }
            Ok((_status, body)) => Err(Status::invalid_argument(format!(
                "rejected by amaru: {body}"
            ))),
            Err(e) => Err(Status::unavailable(format!(
                "amaru submit-api unavailable: {e}"
            ))),
        }
    }

    type WaitForTxStream = tonic::codegen::BoxStream<s::WaitForTxResponse>;

    async fn wait_for_tx(
        &self,
        _request: Request<s::WaitForTxRequest>,
    ) -> Result<Response<Self::WaitForTxStream>, Status> {
        Err(Status::unimplemented(
            "WaitForTx is not supported -- floe has no mempool visibility beyond submit/reject",
        ))
    }

    async fn read_mempool(
        &self,
        _request: Request<s::ReadMempoolRequest>,
    ) -> Result<Response<s::ReadMempoolResponse>, Status> {
        Err(Status::unimplemented(
            "ReadMempool is not supported -- floe has no mempool visibility beyond submit/reject",
        ))
    }

    type WatchMempoolStream = tonic::codegen::BoxStream<s::WatchMempoolResponse>;

    async fn watch_mempool(
        &self,
        _request: Request<s::WatchMempoolRequest>,
    ) -> Result<Response<Self::WatchMempoolStream>, Status> {
        Err(Status::unimplemented(
            "WatchMempool is not supported -- floe has no mempool visibility beyond submit/reject",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_tx_rejects_garbage() {
        assert_eq!(
            decode_tx(b"not a transaction").unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}
