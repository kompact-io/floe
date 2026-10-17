//! http::metrics: `/health` and `/metrics` -- reporting apparatus
//! only. Every handler here snapshots the crate-root
//! `crate::health`/`crate::metrics` core, then renders per `Accept`.
//! No handler computes anything of its own.
//!
//! Format negotiation: no `Accept`, or `text/plain`, renders
//! Prometheus text (`Display`); `application/json`/`application/cbor`
//! render that format; anything else is a 406, not a silent fallback.

use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use floe_dto_http::health::{HealthResponse, MetricsResponse};
use tracing::info;

use crate::serve::ctx::Ctx;

/// Each request opens its own short-lived read-only DB connection
/// inside `spawn_blocking`, since rusqlite is synchronous -- same
/// policy every other scheme follows.
pub async fn serve(addr: &str, ctx: Ctx) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "metrics server listening");
    axum::serve(listener, build_router(ctx)).await?;
    Ok(())
}

fn build_router(state: Ctx) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

// --- format negotiation --------------------------------------------------

#[derive(Clone, Copy)]
enum Format {
    Text,
    Json,
    Cbor,
}

/// `Err`'s `&'static str` (not a full `Response`) keeps this fn's
/// return type small -- `health_handler`/`metrics_handler` build the
/// actual 406 response at the call site.
fn negotiate(headers: &HeaderMap) -> Result<Format, &'static str> {
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if accept.is_empty() || accept.contains("text/plain") || accept.contains("*/*") {
        Ok(Format::Text)
    } else if accept.contains("application/json") {
        Ok(Format::Json)
    } else if accept.contains("application/cbor") {
        Ok(Format::Cbor)
    } else {
        Err("supported: text/plain, application/json, application/cbor\n")
    }
}

fn render<T>(fmt: Format, value: &T) -> Response
where
    T: std::fmt::Display + serde::Serialize + minicbor::Encode<()>,
{
    match fmt {
        Format::Text => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            value.to_string(),
        )
            .into_response(),
        Format::Json => {
            let body = serde_json::to_vec(value).expect("dto JSON serialization is infallible");
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response()
        }
        Format::Cbor => {
            let mut buf = Vec::new();
            minicbor::encode(value, &mut buf).expect("dto CBOR encoding is infallible");
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/cbor")],
                buf,
            )
                .into_response()
        }
    }
}

fn plaintext_error(status: StatusCode, err: impl std::fmt::Display) -> Response {
    (status, format!("{err}\n")).into_response()
}

// --- /health ---------------------------------------------------------------

async fn health_handler(State(state): State<Ctx>, headers: HeaderMap) -> Response {
    let fmt = match negotiate(&headers) {
        Ok(fmt) => fmt,
        Err(msg) => return (StatusCode::NOT_ACCEPTABLE, msg).into_response(),
    };

    render(fmt, &HealthResponse::Run(state.health.snapshot()))
}

// --- /metrics ----------------------------------------------------------------

async fn metrics_handler(State(state): State<Ctx>, headers: HeaderMap) -> Response {
    let fmt = match negotiate(&headers) {
        Ok(fmt) => fmt,
        Err(msg) => return (StatusCode::NOT_ACCEPTABLE, msg).into_response(),
    };

    let utxo_count = match state.utxo_count().await {
        Ok(n) => n,
        Err(e) => return plaintext_error(StatusCode::INTERNAL_SERVER_ERROR, e),
    };

    let dto: MetricsResponse = crate::metrics::snapshot(&state.health, utxo_count, &state.requests);
    render(fmt, &dto)
}
