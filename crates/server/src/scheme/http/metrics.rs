//! http::metrics: `/health` and `/metrics` -- reporting apparatus only.
//! Every handler here does the same thing: snapshot the crate-root
//! `crate::health`/`crate::metrics` core (fetching whatever DB values
//! that needs first, for `serve` mode), then render per `Accept`. No
//! handler computes anything of its own.
//!
//! Format negotiation: no `Accept`, or `text/plain`, renders
//! Prometheus text (`Display`); `application/json`/`application/cbor`
//! render that format; anything else is a 406, not a silent fallback.

use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use floe_dto_http::{HealthResponseDto, MetricsDto, Readiness};
use tracing::info;

use crate::health::Health;
use crate::metrics::Metrics;
use crate::{query, store};

#[derive(Clone)]
struct AppState {
    /// `Some` for `run` (the writer: tracks its own seed/catchup/running
    /// lifecycle in-memory). `None` for `serve` (a stateless read
    /// replica with no such lifecycle of its own -- the handlers below
    /// read straight from the DB instead, see `crate::health::replica_snapshot`).
    health: Option<Arc<Health>>,
    db_path: Arc<str>,
    requests: Arc<Metrics>,
}

/// Each request opens its own short-lived read-only DB connection
/// inside `spawn_blocking`, since rusqlite is synchronous -- same
/// policy every other scheme follows.
pub async fn serve(
    addr: &str,
    db_path: Arc<str>,
    health: Option<Arc<Health>>,
    requests: Arc<Metrics>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "metrics server listening");
    axum::serve(
        listener,
        build_router(AppState {
            health,
            db_path,
            requests,
        }),
    )
    .await?;
    Ok(())
}

fn build_router(state: AppState) -> Router {
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

async fn health_handler(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let fmt = match negotiate(&headers) {
        Ok(fmt) => fmt,
        Err(msg) => return (StatusCode::NOT_ACCEPTABLE, msg).into_response(),
    };

    let Some(health) = &state.health else {
        // `serve`: readiness is just "can I read the replica right now" --
        // there's no seed/catchup/running lifecycle to report.
        let db_path = state.db_path.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<_> {
            let conn = query::open_reader(&db_path)?;
            let tip_slot = store::tip(&conn)?.map(|(slot, _)| slot);
            let utxo_count = store::utxo_count(&conn)?;
            let upstream_mode = store::get_mode(&conn)?;
            Ok((tip_slot, utxo_count, upstream_mode))
        })
        .await;
        let dto = match result {
            Ok(Ok((tip_slot, utxo_count, upstream_mode))) => {
                HealthResponseDto::Replica(crate::health::replica_snapshot(
                    Readiness::Ready,
                    tip_slot,
                    utxo_count,
                    upstream_mode,
                ))
            }
            Ok(Err(_)) => HealthResponseDto::Replica(crate::health::replica_snapshot(
                Readiness::Unavailable,
                None,
                0,
                store::Mode::Catchup,
            )),
            Err(e) => return plaintext_error(StatusCode::INTERNAL_SERVER_ERROR, e),
        };
        return render(fmt, &dto);
    };

    render(fmt, &HealthResponseDto::Run(health.snapshot()))
}

// --- /metrics ----------------------------------------------------------------

async fn metrics_handler(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let fmt = match negotiate(&headers) {
        Ok(fmt) => fmt,
        Err(msg) => return (StatusCode::NOT_ACCEPTABLE, msg).into_response(),
    };

    let db_path = state.db_path.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<u64> {
        let conn = query::open_reader(&db_path)?;
        store::utxo_count(&conn)
    })
    .await;
    let utxo_count = match result {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return plaintext_error(StatusCode::BAD_REQUEST, e),
        Err(e) => return plaintext_error(StatusCode::INTERNAL_SERVER_ERROR, e),
    };

    let dto: MetricsDto = match &state.health {
        Some(health) => crate::metrics::snapshot(health, utxo_count, &state.requests),
        None => crate::metrics::replica_snapshot(utxo_count, &state.requests),
    };
    render(fmt, &dto)
}
