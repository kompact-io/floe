//! Floe
//!
//! The last chunk of the Cardano chain

use std::fs::File;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};
use floe::health::Health;
use floe::ingest::{seed, store, sync};
#[cfg(feature = "grpc")]
use floe::serve::scheme::grpc;
use pallas_network::miniprotocols::Point;
use rusqlite::Connection;
use tracing::{error, info, instrument};
use tracing_subscriber::EnvFilter;

/// glibc's malloc keeps freed heap pages for reuse rather than returning
/// them to the OS; catch-up's chunk buffer (see `catchup::ChunkConfig`)
/// peaks at up to 256MiB per chunk, and that high-water mark otherwise
/// becomes RSS's permanent floor for the rest of the run. jemalloc decays
/// and returns freed pages instead.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[command(name = "floe", about = "Minimal Cardano UTxO index")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Seed (if given) and sync continuously against a peer.
    Run(RunArgs),
    /// Print the current checkpoint and mode, then exit.
    Tip(TipArgs),
}

#[derive(Args)]
struct RunArgs {
    /// Path to the SQLite index file (created if it doesn't exist).
    #[arg(long)]
    db: String,
    /// Peer address, host:port. Omit to skip the peer connection and
    /// sync loop entirely -- just seed (if given) and serve whatever's
    /// already in `--db`. For standing up query listeners against a
    /// fixed fixture DB (e.g. integration tests) with no live peer to
    /// sync against.
    #[arg(long)]
    peer: Option<String>,
    /// Network magic (mainnet=764824073, preprod=1, preview=2).
    #[arg(long)]
    magic: u64,
    /// Amaru bootstrap snapshot archive (<slot>.<hash>.tar.zst). Skipped if
    /// the DB is already seeded.
    #[arg(long)]
    seed: Option<String>,
    /// Serve the gRPC QueryService/SubmitService on this host:port
    /// (requires the `grpc` feature).
    #[arg(long)]
    grpc: Option<String>,
    /// Serve `/health` and `/metrics` (Prometheus text) on this
    /// host:port, for an orchestrator's readiness probe and a scrape
    /// target.
    #[arg(long)]
    metrics: Option<String>,
    /// Serve the http query/submit API on this host:port. JSON vs CBOR
    /// is negotiated per request from `Accept`, not fixed per listener
    /// -- `--http-json`/`--http-cbor` just pick which formats this one
    /// listener accepts at all (at least one required).
    #[arg(long)]
    http: Option<String>,
    /// Accept `application/json` on `--http` (requires the `http-json`
    /// feature; requires `--http`).
    #[arg(long, requires = "http")]
    http_json: bool,
    /// Accept `application/cbor` on `--http` (requires the `http-cbor`
    /// feature; requires `--http`).
    #[arg(long, requires = "http")]
    http_cbor: bool,
    /// amaru's HTTP Cardano Submit API, host:port (its own
    /// `--submit-api-listen-on`). `SubmitTx` is always mounted on
    /// every scheme that's serving (`--grpc`/`--http-json`/
    /// `--http-cbor`) -- a request fails with 503 if amaru's
    /// submit-api itself is unreachable, not if this flag is absent
    /// -- see `submit.rs`'s module doc for why this isn't a
    /// `cardano-node` Unix socket.
    #[arg(long)]
    amaru_submit_api: String,
    /// Hard ceiling on `SearchUtxos` page size -- a request asking for
    /// more just gets clamped, not rejected.
    #[arg(long, default_value_t = 1000)]
    max_page_size: u32,
    /// Reorg depth, and how much history `prune` retains. Default is
    /// ~1.5 epochs' worth of blocks (15 * mainnet/preprod's own
    /// consensus k=2160, average 10k blocks/epoch at epochLength=432000,
    /// activeSlotsCoeff=0.05) rather than the bare consensus minimum --
    /// `block_ref` on a UTxO/tx older than this goes to the "unknown"
    /// `ChainPoint` sentinel once pruned (see query.proto), so the wider
    /// the margin here, the longer that stays resolvable for callers.
    #[arg(long, default_value_t = 32_400)]
    k: u64,
    /// Margin added to k before switching out of catch-up.
    #[arg(long, default_value_t = 500)]
    margin: u64,
    /// How often (in applied blocks) running mode prunes utxo rows spent
    /// more than k blocks ago and old block checkpoint rows.
    #[arg(long, default_value_t = 100)]
    prune_interval: u64,
}

#[derive(Args)]
struct TipArgs {
    /// Path to the SQLite index file.
    #[arg(long)]
    db: String,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().command {
        Command::Run(args) => run(args),
        Command::Tip(args) => tip(args),
    }
}

#[instrument(skip(args), fields(db = %args.db))]
fn tip(args: TipArgs) -> Result<()> {
    let conn = Connection::open(&args.db)?;
    let mode = store::get_mode(&conn)?;
    match store::tip(&conn)? {
        Some((slot, hash)) => println!("slot={slot} hash={} mode={mode:?}", hex::encode(hash)),
        None => println!("no checkpoint yet mode={mode:?}"),
    }
    Ok(())
}

fn anchor_from_filename(archive: &str) -> Result<(u64, [u8; 32])> {
    let fname = std::path::Path::new(archive)
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_default();
    fname
        .strip_suffix(".tar.zst")
        .and_then(|stem| {
            let (s, h) = stem.split_once('.')?;
            let slot = s.parse().ok()?;
            let hash: [u8; 32] = hex::decode(h).ok()?.try_into().ok()?;
            Some((slot, hash))
        })
        .ok_or_else(|| anyhow!("archive must be named <slot>.<hash>.tar.zst"))
}

#[instrument(skip(args), fields(db = %args.db, peer = args.peer.as_deref(), magic = args.magic))]
fn run(args: RunArgs) -> Result<()> {
    let mut conn = Connection::open(&args.db)?;
    store::init(&conn)?;

    let health = Arc::new(Health::default());
    health.set_magic(args.magic);
    let cfg = sync::ModeConfig {
        k: args.k,
        margin: args.margin,
        prune_interval: args.prune_interval,
        ..sync::ModeConfig::default()
    };

    let requests = Arc::new(floe::metrics::Metrics::default());
    let ctx = floe::serve::Ctx {
        db_path: Arc::from(args.db.as_str()),
        max_page_size: args.max_page_size,
        amaru_submit_api: args.amaru_submit_api.clone(),
        health: health.clone(),
        requests: requests.clone(),
    };

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        if let Some(grpc_addr) = args.grpc {
            #[cfg(feature = "grpc")]
            {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(e) = grpc::serve(&grpc_addr, ctx).await {
                        error!(error = %e, "grpc server stopped");
                    }
                });
            }
            #[cfg(not(feature = "grpc"))]
            bail!("--grpc {grpc_addr}: not compiled in (missing the `grpc` feature)");
        }

        if let Some(http_addr) = args.http {
            #[cfg(not(feature = "http-json"))]
            if args.http_json {
                bail!("--http-json: not compiled in (missing the `http-json` feature)");
            }
            #[cfg(not(feature = "http-cbor"))]
            if args.http_cbor {
                bail!("--http-cbor: not compiled in (missing the `http-cbor` feature)");
            }
            if !args.http_json && !args.http_cbor {
                bail!("--http {http_addr}: need --http-json and/or --http-cbor to pick a wire format");
            }

            #[cfg(any(feature = "http-json", feature = "http-cbor"))]
            {
                let ctx = ctx.clone();
                let formats = floe::serve::scheme::http::query::HttpFormats {
                    json: args.http_json,
                    cbor: args.http_cbor,
                };
                tokio::spawn(async move {
                    if let Err(e) = floe::serve::scheme::http::query::serve(&http_addr, ctx, formats).await
                    {
                        error!(error = %e, "http server stopped");
                    }
                });
            }
            #[cfg(not(any(feature = "http-json", feature = "http-cbor")))]
            bail!(
                "--http {http_addr}: not compiled in (missing both the `http-json`/`http-cbor` features)"
            );
        }

        if let Some(metrics_addr) = args.metrics {
            let ctx = ctx.clone();
            tokio::spawn(async move {
                if let Err(e) = floe::serve::scheme::http::metrics::serve(&metrics_addr, ctx).await
                {
                    error!(error = %e, "metrics server stopped");
                }
            });
        }

        // Off the async executor: seeding is blocking IO/CPU and can take
        // minutes on a large snapshot.
        if let Some(archive) = args.seed {
            conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
                if seed::is_done(&conn)? {
                    info!("seed already done, skipping --seed");
                } else {
                    let anchor = anchor_from_filename(&archive)?;
                    info!(archive, "seeding");
                    let zr = zstd::stream::read::Decoder::new(File::open(&archive)?)?;
                    let mut ar = tar::Archive::new(zr);
                    let mut found = false;
                    for entry in ar.entries()? {
                        let mut e = entry?;
                        if e.path()?.to_string_lossy().ends_with("/tables/tvar") {
                            seed::run(&mut e, &mut conn, anchor)?;
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        bail!("no tables/tvar found in {archive}");
                    }
                }
                Ok(conn)
            })
            .await??;
        }
        health.set_stage(store::get_mode(&conn)?.into());

        let Some(peer) = args.peer else {
            info!(mode = ?store::get_mode(&conn)?, "no --peer: serving without a sync loop");
            std::future::pending::<()>().await;
            unreachable!("pending future never resolves");
        };

        let point = match store::tip(&conn)? {
            Some((slot, hash)) => Point::Specific(slot, hash.to_vec()),
            None => Point::Origin,
        };

        info!(mode = ?store::get_mode(&conn)?, "connecting");
        let (stats, _conn) =
            sync::run(peer, args.magic, point, conn, None, cfg, Some(health)).await?;
        info!(
            applied = stats.applied,
            rolled_back = stats.rolled_back,
            "sync loop ended"
        );
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_from_filename_parses_slot_and_hash_ignoring_directory() {
        let hash = [0xABu8; 32];
        let name = format!("snapshots/preprod/130982398.{}.tar.zst", hex::encode(hash));
        assert_eq!(anchor_from_filename(&name).unwrap(), (130982398, hash));
    }

    #[test]
    fn anchor_from_filename_rejects_wrong_suffix() {
        assert!(anchor_from_filename("130982398.abcd.tar.gz").is_err());
    }

    #[test]
    fn anchor_from_filename_rejects_non_32_byte_hash() {
        assert!(anchor_from_filename("130982398.abcd.tar.zst").is_err());
    }

    #[test]
    fn anchor_from_filename_rejects_non_numeric_slot() {
        let name = format!("notaslot.{}.tar.zst", hex::encode([0u8; 32]));
        assert!(anchor_from_filename(&name).is_err());
    }
}
