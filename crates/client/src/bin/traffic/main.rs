//! floe-traffic: generates ongoing, varied read traffic against a
//! running `floe-server` deployment -- not a load-testing tool (no
//! ramping, no latency histograms, just `--concurrency` independent
//! workers, each holding its own connection per scheme and
//! round-robining through every selected scheme and endpoint
//! (`SearchUtxos`/`ReadUtxos`/`ReadTx`/`ReadData`/`GetHealth`/
//! `GetMetrics`) on its own `--interval-secs` clock. `SearchUtxos`/
//! `ReadTx` chain into each other rather than hammering one fixed
//! address forever -- see `SchemeState`'s own doc comment for the
//! walk. Useful for keeping a dashboard's panels non-empty with
//! realistic-looking traffic, or for eyeballing that every enabled
//! scheme is actually getting hit under some concurrent load (see
//! `floe-smoke`'s own `GetMetrics`-reflects-this-run check, which this
//! doesn't do -- `floe-smoke` is the pass/fail tool, this is just
//! noise generation).
//!
//! Runs until Ctrl-C, or `--duration-secs` if given, then prints a
//! per-scheme/per-endpoint ok/fail/latency tally, pooled across every
//! worker.
//!
//! usage:
//! ```text
//! cargo run -p floe-client --bin floe-traffic -- --http 127.0.0.1:8080 --http-json --http-cbor \
//!     --address addr_test1... --concurrency 8 --interval-secs 1 --duration-secs 300
//! cargo run -p floe-client --bin floe-traffic -- --grpc 127.0.0.1:50051 --skey "$FLOE_SMOKE_SKEY"
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use amaru_kernel::address::Address;
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

use floe_client::{AnyClient, Backend, Hash28, NetworkArg, Scheme, SchemeArgs, TxHash, parse_hex};
use floe_dto_http::query::{
    ReadDataRequest, ReadTxRequest, ReadUtxosRequest, SearchUtxosRequest, TxoRef,
};

#[derive(Parser)]
#[command(
    name = "floe-traffic",
    about = "Generate basic, ongoing read traffic against a running floe-server deployment"
)]
struct Cli {
    #[command(flatten)]
    scheme: SchemeArgs,

    /// Retry window before giving up on a scheme connecting at all.
    #[arg(long, default_value_t = 30)]
    ready_timeout_secs: u64,

    /// ed25519 secret key, hex (32 bytes). Derives the search hash
    /// when `--pay-hash`/`--address` are absent.
    #[arg(long, env = "FLOE_SMOKE_SKEY", value_parser = parse_hex::<32>)]
    skey: Option<[u8; 32]>,

    /// Bech32 network prefix for an address derived from `--skey`.
    #[arg(long, default_value = "mainnet")]
    network: NetworkArg,

    /// Address (bech32) to search. Overrides `--skey`'s derived one.
    #[arg(long)]
    address: Option<String>,

    /// Payment part hash to search, hex (28 bytes). Overrides
    /// `--address`/`--skey`.
    #[arg(long, value_parser = parse_hex::<28>)]
    pay_hash: Option<Hash28>,

    /// Also round-robin `ReadData` on this hash.
    #[arg(long, value_parser = parse_hex::<32>)]
    datum_hash: Option<TxHash>,

    /// Independent workers, each cycling through every selected
    /// scheme on its own clock -- the "how much concurrent traffic"
    /// knob. Each gets its own connection per scheme, so they don't
    /// serialize behind one another.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// Seconds between request ticks, per worker (one request per
    /// scheme, per tick, per worker).
    #[arg(long, default_value_t = 1.0)]
    interval_secs: f64,

    /// Stop after this long. Omit to run until Ctrl-C.
    #[arg(long)]
    duration_secs: Option<u64>,
}

/// `--pay-hash` wins outright; otherwise `--address`/`--skey`'s
/// address is decomposed via `shelley_hashes`. Mirrors `floe-smoke`'s
/// own `resolve_hash` -- not shared, since each bin compiles
/// independently.
fn resolve_hash(cli: &Cli) -> Result<Hash28> {
    if let Some(pay_hash) = cli.pay_hash {
        return Ok(pay_hash);
    }
    let address: Address = if let Some(addr) = &cli.address {
        addr.parse()
            .map_err(|_| anyhow::anyhow!("--address is not a valid address"))?
    } else if let Some(skey) = cli.skey {
        let info = floe_client::key_info::key_info(skey, cli.network.0)?;
        info!(address = %info.address, "derived address from --skey");
        info.address
            .parse()
            .expect("key_info always returns a valid address")
    } else {
        anyhow::bail!("need one of --pay-hash, --address, or --skey to know what to search for");
    };
    floe_client::shelley_hashes(&address).map(|(pay, _stake)| pay)
}

/// Round-robins through the basic read endpoints. `ReadUtxos`/`ReadTx`
/// need a target discovered from a prior `SearchUtxos` -- skipped
/// (not failed) until one's cached.
#[derive(Clone, Copy)]
enum Endpoint {
    SearchUtxos,
    ReadUtxos,
    ReadTx,
    ReadData,
    GetHealth,
    GetMetrics,
}
const ENDPOINTS: [Endpoint; 6] = [
    Endpoint::SearchUtxos,
    Endpoint::ReadUtxos,
    Endpoint::ReadTx,
    Endpoint::ReadData,
    Endpoint::GetHealth,
    Endpoint::GetMetrics,
];
impl Endpoint {
    fn name(self) -> &'static str {
        match self {
            Endpoint::SearchUtxos => "SearchUtxos",
            Endpoint::ReadUtxos => "ReadUtxos",
            Endpoint::ReadTx => "ReadTx",
            Endpoint::ReadData => "ReadData",
            Endpoint::GetHealth => "GetHealth",
            Endpoint::GetMetrics => "GetMetrics",
        }
    }
}

/// One worker's connection to one scheme, plus the walk state its own
/// `ReadUtxos`/`ReadTx` ticks read from -- each worker gets its own
/// `Client` (and so its own walk) rather than sharing one across
/// workers, so concurrent workers never serialize behind the same
/// connection, and each wanders the chain independently.
///
/// `SearchUtxos`/`ReadTx` chain into each other rather than hammering
/// one fixed address: `SearchUtxos(walk_pay_hash)` finds a UTxO,
/// `ReadTx` on its creating tx looks at *that tx's inputs*, and the
/// first input's address becomes the next `walk_pay_hash` -- so the
/// walk wanders backward through real spends instead of
/// re-querying the same starting point forever. `origin_pay_hash` is
/// where it restarts from once a walk runs out of road (no items, no
/// inputs, or an input address with no payment hash to extract).
struct SchemeState {
    scheme: Scheme,
    client: AnyClient,
    origin_pay_hash: Hash28,
    walk_pay_hash: Hash28,
    cached_ref: Option<TxoRef>,
    cached_tx_hash: Option<TxHash>,
}

/// Running ok/fail counts and latency (sum/min/max, for a mean and a
/// crude spread) per (scheme, endpoint), pooled across every worker.
#[derive(Default, Clone, Copy)]
struct Stats {
    ok: u64,
    fail: u64,
    total_latency: Duration,
    min_latency: Option<Duration>,
    max_latency: Option<Duration>,
}
type Tally = HashMap<(Scheme, &'static str), Stats>;

fn record(tally: &Mutex<Tally>, scheme: Scheme, endpoint: Endpoint, ok: bool, latency: Duration) {
    // A plain `std::sync::Mutex` is fine here: the critical section is
    // a few field updates, never held across an `.await`, so it can't
    // block the executor for more than that.
    let mut tally = tally.lock().expect("tally mutex poisoned");
    let entry = tally.entry((scheme, endpoint.name())).or_default();
    if ok {
        entry.ok += 1;
    } else {
        entry.fail += 1;
    }
    entry.total_latency += latency;
    entry.min_latency = Some(entry.min_latency.map_or(latency, |m| m.min(latency)));
    entry.max_latency = Some(entry.max_latency.map_or(latency, |m| m.max(latency)));
}

/// One tick: fires `endpoint` for this scheme, logs at debug (success)
/// or warn (failure -- not fatal, traffic generation keeps going
/// regardless), times it, and refreshes `state`'s cache/walk position
/// on a successful `SearchUtxos`/`ReadTx`.
async fn fire(
    state: &mut SchemeState,
    endpoint: Endpoint,
    datum_hash: Option<TxHash>,
    tally: &Mutex<Tally>,
) {
    let scheme = state.scheme;
    let start = Instant::now();
    let report = |ok: bool| record(tally, scheme, endpoint, ok, start.elapsed());

    match endpoint {
        Endpoint::SearchUtxos => {
            match state
                .client
                .search_utxos(SearchUtxosRequest {
                    pay_hash: Some(state.walk_pay_hash),
                    stake_hash: None,
                    since_slot: None,
                    since_hash: None,
                    start_token: None,
                    max_items: Some(1),
                })
                .await
            {
                Ok(page) => {
                    if let Some(item) = page.items.first() {
                        state.cached_ref = Some(item.txo_ref);
                        state.cached_tx_hash = Some(item.txo_ref.hash);
                    } else {
                        // Dead end (nothing left at this address) --
                        // restart the walk rather than searching the
                        // same empty address forever.
                        state.walk_pay_hash = state.origin_pay_hash;
                    }
                    debug!(%scheme, endpoint = endpoint.name(), pay_hash = %hex::encode(state.walk_pay_hash), items = page.items.len(), "ok");
                    report(true);
                }
                Err(e) => {
                    warn!(%scheme, endpoint = endpoint.name(), error = %e, "failed");
                    report(false);
                }
            }
        }
        Endpoint::ReadUtxos => {
            let Some(key) = state.cached_ref else {
                debug!(%scheme, endpoint = endpoint.name(), "skipped, nothing cached yet");
                return;
            };
            match state
                .client
                .read_utxos(ReadUtxosRequest { keys: vec![key] })
                .await
            {
                Ok(resp) => {
                    debug!(%scheme, endpoint = endpoint.name(), items = resp.items.len(), "ok");
                    report(true);
                }
                Err(e) => {
                    warn!(%scheme, endpoint = endpoint.name(), error = %e, "failed");
                    report(false);
                }
            }
        }
        Endpoint::ReadTx => {
            let Some(hash) = state.cached_tx_hash else {
                debug!(%scheme, endpoint = endpoint.name(), "skipped, nothing cached yet");
                return;
            };
            match state.client.read_tx(ReadTxRequest { hash }).await {
                Ok(Some(resp)) => {
                    // Walk backward: the first input's address becomes
                    // the next `SearchUtxos` target, so the chain gets
                    // traversed rather than re-probed at one address --
                    // see `SchemeState`'s own doc. Falls back to
                    // restarting the walk if this tx has no inputs
                    // (impossible for a real tx, but genesis-ish rows
                    // might report none) or the input's address has no
                    // payment credential to extract (Byron, pointer
                    // address).
                    state.walk_pay_hash = resp
                        .tx
                        .inputs
                        .first()
                        .and_then(|input| {
                            floe_client::shelley_hashes(&input.as_output.address.0).ok()
                        })
                        .map(|(pay, _stake)| pay)
                        .unwrap_or(state.origin_pay_hash);
                    debug!(%scheme, endpoint = endpoint.name(), next_pay_hash = %hex::encode(state.walk_pay_hash), "ok");
                    report(true);
                }
                Ok(None) => {
                    debug!(%scheme, endpoint = endpoint.name(), "not found");
                    report(true);
                }
                Err(e) => {
                    warn!(%scheme, endpoint = endpoint.name(), error = %e, "failed");
                    report(false);
                }
            }
        }
        Endpoint::ReadData => {
            let Some(key) = datum_hash else {
                debug!(%scheme, endpoint = endpoint.name(), "skipped, no --datum-hash given");
                return;
            };
            match state.client.read_data(ReadDataRequest { key }).await {
                Ok(_) => {
                    debug!(%scheme, endpoint = endpoint.name(), "ok");
                    report(true);
                }
                Err(e) => {
                    warn!(%scheme, endpoint = endpoint.name(), error = %e, "failed");
                    report(false);
                }
            }
        }
        Endpoint::GetHealth => match state.client.get_health().await {
            Ok(_) => {
                debug!(%scheme, endpoint = endpoint.name(), "ok");
                report(true);
            }
            Err(e) => {
                debug!(%scheme, endpoint = endpoint.name(), error = %e, "unavailable over this scheme");
                report(false);
            }
        },
        Endpoint::GetMetrics => match state.client.get_metrics().await {
            Ok(_) => {
                debug!(%scheme, endpoint = endpoint.name(), "ok");
                report(true);
            }
            Err(e) => {
                debug!(%scheme, endpoint = endpoint.name(), error = %e, "unavailable over this scheme");
                report(false);
            }
        },
    }
}

/// One worker: its own connection per scheme, round-robining through
/// every scheme and (independently) every endpoint until `stop` flips
/// -- checked between ticks, not pre-empted mid-request.
async fn run_worker(
    worker_id: usize,
    mut states: Vec<SchemeState>,
    interval: Duration,
    datum_hash: Option<TxHash>,
    tally: Arc<Mutex<Tally>>,
    stop: Arc<AtomicBool>,
) {
    let mut ticker = tokio::time::interval(interval);
    let mut scheme_tick = 0usize;
    let mut endpoint_tick = 0usize;
    while !stop.load(Ordering::Relaxed) {
        ticker.tick().await;
        let n = states.len();
        let state = &mut states[scheme_tick % n];
        let endpoint = ENDPOINTS[endpoint_tick % ENDPOINTS.len()];
        fire(state, endpoint, datum_hash, &tally).await;
        scheme_tick += 1;
        // Every scheme gets every endpoint eventually without the two
        // counters staying in lockstep (which would, with
        // `schemes.len() == ENDPOINTS.len()`, pin each scheme to one
        // endpoint forever).
        if scheme_tick.is_multiple_of(n) {
            endpoint_tick += 1;
        }
    }
    debug!(worker_id, "stopped");
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let pay_hash = resolve_hash(&cli)?;

    let schemes = cli.scheme.selected();
    if schemes.is_empty() {
        anyhow::bail!(
            "need --grpc and/or --http (with --http-json/--http-cbor) to know what to hit"
        );
    }
    if cli.concurrency == 0 {
        anyhow::bail!("--concurrency must be at least 1");
    }

    // Each worker gets its own connection per scheme -- `concurrency`
    // independent clients per scheme in total, so workers genuinely
    // run concurrently instead of serializing behind a shared one.
    let mut workers_states = Vec::with_capacity(cli.concurrency);
    for worker_id in 0..cli.concurrency {
        let mut states = Vec::with_capacity(schemes.len());
        for &scheme in &schemes {
            let client = floe_client::connect(
                scheme,
                &cli.scheme,
                Duration::from_secs(cli.ready_timeout_secs),
            )
            .await?;
            info!(%scheme, worker_id, "connected");
            states.push(SchemeState {
                scheme,
                client,
                origin_pay_hash: pay_hash,
                walk_pay_hash: pay_hash,
                cached_ref: None,
                cached_tx_hash: None,
            });
        }
        workers_states.push(states);
    }

    let interval = Duration::from_secs_f64(cli.interval_secs.max(0.01));
    let tally = Arc::new(Mutex::new(Tally::new()));
    let stop = Arc::new(AtomicBool::new(false));

    info!(
        schemes = schemes.len(),
        concurrency = cli.concurrency,
        interval = ?interval,
        duration = ?cli.duration_secs,
        "traffic started, Ctrl-C to stop"
    );

    let handles: Vec<_> = workers_states
        .into_iter()
        .enumerate()
        .map(|(worker_id, states)| {
            tokio::spawn(run_worker(
                worker_id,
                states,
                interval,
                cli.datum_hash,
                tally.clone(),
                stop.clone(),
            ))
        })
        .collect();

    // Whichever comes first -- `--duration-secs` elapsing or Ctrl-C --
    // flips `stop`; workers notice within one tick and wind down.
    match cli.duration_secs {
        Some(secs) => {
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(secs)) => {
                    info!("--duration-secs elapsed, stopping");
                }
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl-C, stopping");
                }
            }
        }
        None => {
            let _ = tokio::signal::ctrl_c().await;
            info!("Ctrl-C, stopping");
        }
    }
    stop.store(true, Ordering::Relaxed);

    for handle in handles {
        let _ = handle.await;
    }

    let tally = Arc::try_unwrap(tally)
        .unwrap_or_else(|_| unreachable!("all workers joined above"))
        .into_inner()
        .expect("tally mutex poisoned");
    let mut rows: Vec<_> = tally.into_iter().collect();
    rows.sort_by_key(|((scheme, endpoint), _)| (scheme.to_string(), *endpoint));
    for ((scheme, endpoint), stats) in rows {
        let total = stats.ok + stats.fail;
        let mean_latency = if total > 0 {
            stats.total_latency / total as u32
        } else {
            Duration::ZERO
        };
        info!(
            %scheme,
            endpoint,
            ok = stats.ok,
            fail = stats.fail,
            mean_latency = ?mean_latency,
            min_latency = ?stats.min_latency.unwrap_or_default(),
            max_latency = ?stats.max_latency.unwrap_or_default(),
            "tally"
        );
    }

    Ok(())
}
