//! floe: minimal Cardano UTxO index. CLI entry point.
//!
//! `run` seeds (if given a snapshot archive and not already seeded) and
//! syncs continuously against a peer, optionally serving the gRPC
//! `QueryService` (`grpc`). `tip` prints the current checkpoint and mode,
//! then exits. `serve` is `run` without the writer half: no peer, no seed,
//! no sync loop -- just the gRPC query interface against an existing DB
//! (e.g. a read-only replica kept current by something else, like
//! Litestream, outside this process entirely).
//!
//! `--metrics` optionally serves `/health` and `/metrics` (see
//! `floe::metrics`) alongside the gRPC port. The old REST query API
//! (`http.rs`) is still gone (dropped going gRPC-first, see
//! `plan-rpc.md`), but its `/health`/`/metrics` routes were always a
//! separate concern and are back.

use std::fs::File;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};
use floe::health::Health;
use floe::scheme::grpc;
use floe::{seed, store, sync};
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
    /// Serve the gRPC QueryService against an existing DB, with no peer
    /// connection and no sync loop -- for a read replica kept current by
    /// something else outside this process.
    Serve(ServeArgs),
    /// Print the current checkpoint and mode, then exit.
    Tip(TipArgs),
}

#[derive(Args)]
struct RunArgs {
    /// Path to the SQLite index file (created if it doesn't exist).
    #[arg(long)]
    db: String,
    /// Peer address, host:port.
    #[arg(long)]
    peer: String,
    /// Network magic (mainnet=764824073, preprod=1, preview=2).
    #[arg(long)]
    magic: u64,
    /// Amaru bootstrap snapshot archive (<slot>.<hash>.tar.zst). Skipped if
    /// the DB is already seeded.
    #[arg(long)]
    seed: Option<String>,
    /// Serve the gRPC QueryService on this host:port.
    #[arg(long)]
    grpc: Option<String>,
    /// Serve `/health` and `/metrics` (Prometheus text) on this
    /// host:port, for an orchestrator's readiness probe and a scrape
    /// target.
    #[arg(long)]
    metrics: Option<String>,
    /// Serve the REST transport (`http::serve` -- mirrors `QueryService`/
    /// `SubmitService`, see `../../plan-dto-http.md`) on this host:port.
    /// Independent of `--grpc`/`--metrics`: its own listener, own
    /// routes, same underlying DB.
    #[arg(long)]
    http: Option<String>,
    /// amaru's HTTP Cardano Submit API, host:port (its own
    /// `--submit-api-listen-on`). When given alongside `--grpc`
    /// and/or `--http`, also mounts `SubmitTx` on each -- see
    /// `submit.rs`'s module doc for why this isn't a `cardano-node`
    /// Unix socket.
    #[arg(long)]
    amaru_submit_api: Option<String>,
    /// Hard ceiling on `SearchUtxos`/`UtxoChanges` page sizes -- a
    /// request asking for more just gets clamped, not rejected.
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
struct ServeArgs {
    /// Path to the SQLite index file, or a `file:...?vfs=<name>` URI
    /// selecting a non-default VFS (e.g. a Litestream read replica) --
    /// anything `rusqlite::Connection::open_with_flags` with
    /// `SQLITE_OPEN_URI` accepts.
    #[arg(long)]
    db: String,
    /// Network magic (mainnet=764824073, preprod=1, preview=2) -- needed
    /// to render/validate bech32 addresses, same as `run`.
    #[arg(long)]
    magic: u64,
    /// Serve the gRPC QueryService on this host:port.
    #[arg(long)]
    grpc: String,
    /// Serve `/health` and `/metrics` (Prometheus text) on this
    /// host:port, for an orchestrator's readiness probe and a scrape
    /// target.
    #[arg(long)]
    metrics: Option<String>,
    /// Serve the REST transport on this host:port -- see `RunArgs::http`'s
    /// doc comment.
    #[arg(long)]
    http: Option<String>,
    /// amaru's HTTP Cardano Submit API, host:port. When given, also
    /// mounts `SubmitTx` on `--grpc` and/or `--http` -- see
    /// `submit.rs`'s module doc.
    #[arg(long)]
    amaru_submit_api: Option<String>,
    /// Hard ceiling on `SearchUtxos`/`UtxoChanges` page sizes -- a
    /// request asking for more just gets clamped, not rejected.
    #[arg(long, default_value_t = 1000)]
    max_page_size: u32,
    /// Shared library implementing the DB URI's `vfs=<name>`, loaded once
    /// at startup (e.g. Litestream's `litestream-vfs` build). Registering
    /// a VFS is process-wide, not per-connection, so this only needs
    /// loading once even though every request opens its own connection.
    /// Omit if `db` doesn't need one (a plain path, or a VFS already
    /// registered some other way).
    #[arg(long)]
    vfs_extension: Option<String>,
    /// The VFS extension's entry point, if it isn't the SQLite default
    /// (`sqlite3_<libname>_init`). Litestream's is `sqlite3_litestreamvfs_init`.
    #[arg(long)]
    vfs_entry_point: Option<String>,
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
        Command::Serve(args) => serve(args),
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

#[instrument(skip(args), fields(db = %args.db, peer = %args.peer, magic = args.magic))]
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
    let db_path: Arc<str> = Arc::from(args.db.as_str());
    // Cloned out once, up front: `--grpc` and `--http` can both mount
    // `SubmitTx`, so neither branch below can simply move
    // `args.amaru_submit_api` out from under the other.
    let amaru_submit_api = args.amaru_submit_api.clone();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        if let Some(grpc_addr) = args.grpc {
            let network = floe::domain::network_from_magic(args.magic);
            let amaru_submit_api = amaru_submit_api.clone();
            let max_page_size = args.max_page_size;
            let db_path = db_path.clone();
            let health = health.clone();
            let requests = requests.clone();
            tokio::spawn(async move {
                if let Err(e) = grpc::serve(
                    &grpc_addr,
                    db_path,
                    network,
                    max_page_size,
                    amaru_submit_api,
                    Some(health),
                    requests,
                )
                .await
                {
                    error!(error = %e, "grpc server stopped");
                }
            });
        }

        if let Some(http_addr) = args.http {
            let network = floe::domain::network_from_magic(args.magic);
            let amaru_submit_api = amaru_submit_api.clone();
            let max_page_size = args.max_page_size;
            let db_path = db_path.clone();
            tokio::spawn(async move {
                if let Err(e) = floe::scheme::http::serve(
                    &http_addr,
                    db_path,
                    network,
                    max_page_size,
                    amaru_submit_api,
                )
                .await
                {
                    error!(error = %e, "http server stopped");
                }
            });
        }

        if let Some(metrics_addr) = args.metrics {
            let db_path = db_path.clone();
            let health = health.clone();
            let requests = requests.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    floe::scheme::http::metrics::serve(&metrics_addr, db_path, Some(health), requests)
                        .await
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

        let point = match store::tip(&conn)? {
            Some((slot, hash)) => Point::Specific(slot, hash.to_vec()),
            None => Point::Origin,
        };

        info!(mode = ?store::get_mode(&conn)?, "connecting");
        let (stats, _conn) =
            sync::run(args.peer, args.magic, point, conn, None, cfg, Some(health)).await?;
        info!(
            applied = stats.applied,
            rolled_back = stats.rolled_back,
            "sync loop ended"
        );
        Ok(())
    })
}

/// No peer, no seed, no sync loop -- just `grpc::serve` against
/// whatever's at `args.db` right now. Loads the VFS extension once up
/// front (a VFS is registered process-wide, not per-connection) and
/// does one sanity read before binding, so a broken replica fails
/// fast at startup rather than on the first real request.
#[instrument(skip(args), fields(db = %args.db, magic = args.magic))]
fn serve(args: ServeArgs) -> Result<()> {
    if let Some(ext) = &args.vfs_extension {
        // SAFETY: `vfs_extension` is an operator-supplied path (a CLI
        // flag, not request input), same trust model as the `--db` path
        // itself. No SQL runs on this connection while loading is enabled.
        let conn = Connection::open_in_memory()?;
        unsafe {
            let _guard = rusqlite::LoadExtensionGuard::new(&conn)?;
            conn.load_extension(ext, args.vfs_entry_point.as_deref())?;
        }
        info!(extension = %ext, "loaded VFS extension");
    }

    {
        let conn = store::query::open_reader(&args.db)?;
        info!(tip = ?store::tip(&conn)?, mode = ?store::get_mode(&conn)?, "replica readable");
    }

    let network = floe::domain::network_from_magic(args.magic);
    let db_path: Arc<str> = Arc::from(args.db.as_str());
    let requests = Arc::new(floe::metrics::Metrics::default());
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        if let Some(http_addr) = args.http {
            let db_path = db_path.clone();
            let amaru_submit_api = args.amaru_submit_api.clone();
            tokio::spawn(async move {
                if let Err(e) = floe::scheme::http::serve(
                    &http_addr,
                    db_path,
                    network,
                    args.max_page_size,
                    amaru_submit_api,
                )
                .await
                {
                    error!(error = %e, "http server stopped");
                }
            });
        }

        if let Some(metrics_addr) = args.metrics {
            let db_path = db_path.clone();
            let requests = requests.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    floe::scheme::http::metrics::serve(&metrics_addr, db_path, None, requests).await
                {
                    error!(error = %e, "metrics server stopped");
                }
            });
        }

        grpc::serve(
            &args.grpc,
            db_path,
            network,
            args.max_page_size,
            args.amaru_submit_api,
            None,
            requests,
        )
        .await
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
