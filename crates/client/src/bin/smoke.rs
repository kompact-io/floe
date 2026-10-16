//! floe-smoke: deployment smoke test against a running `floe-server`
//! (not a correctness suite -- that's `floe-integration`). Exercises
//! the whole `Backend` contract (search by pay/stake cred, `ReadUtxos`,
//! `ReadTx`, `ReadData`, `UtxoChanges`, opt-in `SubmitTx` via
//! `--submit`) and fails on wiring problems, not absent data.
//!
//! One address flag per scheme (`--grpc`/`--http-json`/`--http-cbor`,
//! host:port); whichever are given run. With two or more, their
//! (ledger-tip-normalized) responses are cross-checked for agreement.
//!
//! Logging is `tracing`, `RUST_LOG`-filtered (default `info`); the
//! ready/connect retry loop stays at `debug`.
//!
//! usage:
//! ```text
//! cargo run -p floe-client --bin floe-smoke -- --grpc 127.0.0.1:50051 --skey "$FLOE_SMOKE_SKEY"
//! cargo run -p floe-client --bin floe-smoke -- --http-json 127.0.0.1:8080 --pay-cred <cred_hex>
//! cargo run -p floe-client --bin floe-smoke -- --http-cbor 127.0.0.1:8080 --address <bech32> --datum-hash <hash_hex>
//! cargo run -p floe-client --bin floe-smoke -- --grpc 127.0.0.1:50051 \
//!     --http-json 127.0.0.1:8080 --http-cbor 127.0.0.1:8080 --skey "$FLOE_SMOKE_SKEY" --submit
//! ```

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use pallas_addresses::Address;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use floe_client::{Backend, ChangesParams, Cred, NetworkArg, Paging, TxHash, parse_hex};
use floe_dto_http::{
    ChainPointDto, ChangesResponseDto, DatumResponseDto, PageDto, TxResponseDto, UtxoDto,
};

/// A wire scheme being smoke-tested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Scheme {
    Grpc,
    HttpJson,
    HttpCbor,
}

impl std::fmt::Display for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Scheme::Grpc => "grpc",
            Scheme::HttpJson => "http-json",
            Scheme::HttpCbor => "http-cbor",
        })
    }
}

#[derive(Parser)]
#[command(
    name = "floe-smoke",
    about = "Smoke-test a running floe-server deployment"
)]
struct Cli {
    /// gRPC address, host:port. Bare `host:port` gets `http://`
    /// prepended. One of `--grpc`/`--http-json`/`--http-cbor` required.
    #[arg(long)]
    grpc: Option<String>,

    /// `http-json` address, host:port.
    #[arg(long)]
    http_json: Option<String>,

    /// `http-cbor` address, host:port.
    #[arg(long)]
    http_cbor: Option<String>,

    /// Retry window before giving up on the server answering at all.
    #[arg(long, default_value_t = 30)]
    ready_timeout_secs: u64,

    /// ed25519 secret key, hex (32 bytes). Derives the search
    /// credential when `--pay-cred`/`--address` are absent; always
    /// signs `--submit`'s self-payment.
    #[arg(long, env = "FLOE_SMOKE_SKEY", value_parser = parse_hex::<32>)]
    skey: Option<[u8; 32]>,

    /// Bech32 network prefix for an address derived from `--skey`.
    #[arg(long, default_value = "mainnet")]
    network: NetworkArg,

    /// Address (bech32) to search. Overrides `--skey`'s derived one.
    #[arg(long)]
    address: Option<String>,

    /// Payment credential to search, hex (28 bytes). Overrides
    /// `--address`/`--skey`.
    #[arg(long, value_parser = parse_hex::<28>)]
    pay_cred: Option<Cred>,

    /// Stake credential alongside `--pay-cred` (ignored otherwise).
    #[arg(long, value_parser = parse_hex::<28>)]
    stake_cred: Option<Cred>,

    /// `ReadTx` this hash instead of the found UTxO's own tx.
    #[arg(long, value_parser = parse_hex::<32>)]
    tx_hash: Option<TxHash>,

    /// Also check `ReadData` on this hash.
    #[arg(long, value_parser = parse_hex::<32>)]
    datum_hash: Option<TxHash>,

    /// Also broadcast a self-payment from `--skey`. Mutates chain
    /// state and spends real test ADA; runs once, against the first
    /// scheme. Requires `--skey`.
    #[arg(long)]
    submit: bool,
}

/// `Fail` sinks the run's exit code; `Warn` doesn't (absent data, not
/// a wiring problem).
enum Step {
    Ok(String),
    Warn(String),
    Fail(String),
}

/// One-line UTxO summary, for cross-referencing against an independent
/// source.
fn precis_item(u: &UtxoDto) -> String {
    format!("{}:{} coin={}", hex::encode(u.tx_hash), u.ix, u.coin)
}

fn report(step: &Step) {
    match step {
        Step::Ok(msg) => info!("[ok]   {msg}"),
        Step::Warn(msg) => warn!("[warn] {msg}"),
        Step::Fail(msg) => error!("[fail] {msg}"),
    }
}

/// `--pay-cred` wins outright; otherwise `--address`/`--skey`'s
/// address is decomposed via `shelley_creds`.
fn resolve_credential(cli: &Cli) -> Result<(Cred, Option<Cred>)> {
    if let Some(pay_cred) = cli.pay_cred {
        return Ok((pay_cred, cli.stake_cred));
    }
    let address: Address = if let Some(addr) = &cli.address {
        addr.parse().context("--address is not a valid address")?
    } else if let Some(skey) = cli.skey {
        let info = floe_client::key_info::key_info(skey, cli.network.0)?;
        info!(address = %info.address, "derived address from --skey");
        info.address
            .parse()
            .expect("key_info always returns a valid address")
    } else {
        anyhow::bail!("need one of --pay-cred, --address, or --skey to know what to search for");
    };
    floe_client::shelley_creds(&address)
}

/// One scheme's results, for cross-scheme comparison in `main`.
#[derive(Default)]
struct SmokeOutcome {
    ok: bool,
    page: Option<PageDto>,
    reread: Option<UtxoDto>,
    stake_page: Option<PageDto>,
    tx: Option<TxResponseDto>,
    datum: Option<DatumResponseDto>,
    changes: Option<ChangesResponseDto>,
}

/// Scheme-agnostic smoke flow. `do_submit` gates `SubmitTx`.
async fn run_smoke<B: Backend>(
    client: &mut B,
    cli: &Cli,
    scheme: Scheme,
    do_submit: bool,
) -> Result<SmokeOutcome> {
    let (pay_cred, stake_cred) = resolve_credential(cli)?;
    info!(
        scheme = %scheme,
        pay_cred = %hex::encode(pay_cred),
        stake_cred = ?stake_cred.map(hex::encode),
        "searching"
    );

    // "Ready" = first successful response (no dedicated health check).
    let timeout = Duration::from_secs(cli.ready_timeout_secs);
    let start = Instant::now();
    loop {
        match client
            .pay_cred(
                pay_cred,
                Paging {
                    limit: Some(1),
                    after: None,
                },
            )
            .await
        {
            Ok(_) => {
                info!(scheme = %scheme, elapsed = ?start.elapsed(), "server is answering");
                break;
            }
            Err(e) if start.elapsed() < timeout => {
                debug!(scheme = %scheme, error = %e, "not ready yet, retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => anyhow::bail!("[{scheme}] server never answered within {timeout:?}: {e}"),
        }
    }

    let mut outcome = SmokeOutcome {
        ok: true,
        ..Default::default()
    };
    let mut record = |step: Step| {
        if matches!(step, Step::Fail(_)) {
            outcome.ok = false;
        }
        report(&step);
    };

    let page = match client
        .address(pay_cred, stake_cred, Paging::default())
        .await
    {
        Ok(page) => {
            let first = page.items.first().map(precis_item);
            record(Step::Ok(format!(
                "[{scheme}] SearchUtxos(pay_cred={}, stake_cred={:?}): {} item(s), first={:?}",
                hex::encode(pay_cred),
                stake_cred.map(hex::encode),
                page.items.len(),
                first
            )));
            Some(page)
        }
        Err(e) => {
            record(Step::Fail(format!("[{scheme}] SearchUtxos failed: {e}")));
            None
        }
    };
    let first = page.as_ref().and_then(|p| p.items.first()).cloned();
    outcome.page = page;

    let mut derived_tx_hash = None;
    if let Some(item) = &first {
        derived_tx_hash = Some(item.tx_hash);
        match client.read_ref(item.tx_hash, item.ix as u32).await {
            Ok(Some(reread)) if reread.coin == item.coin => {
                record(Step::Ok(format!(
                    "[{scheme}] ReadUtxos: {}:{} re-read matches (coin={})",
                    hex::encode(item.tx_hash),
                    item.ix,
                    item.coin
                )));
                outcome.reread = Some(reread);
            }
            Ok(Some(reread)) => record(Step::Fail(format!(
                "[{scheme}] ReadUtxos: {}:{} re-read disagrees with SearchUtxos (coin {} vs {})",
                hex::encode(item.tx_hash),
                item.ix,
                reread.coin,
                item.coin
            ))),
            Ok(None) => record(Step::Fail(format!(
                "[{scheme}] ReadUtxos: {}:{} not found, but SearchUtxos just returned it",
                hex::encode(item.tx_hash),
                item.ix
            ))),
            Err(e) => record(Step::Fail(format!("[{scheme}] ReadUtxos failed: {e}"))),
        }
    } else {
        record(Step::Warn(format!(
            "[{scheme}] no UTxOs at this credential -- nothing to re-read"
        )));
    }

    let fmt_tx_found = |scheme: Scheme, hash: TxHash, tx: &floe_dto_http::TxResponseDto| {
        format!(
            "[{scheme}] ReadTx {}: found, {} input(s) {} output(s), first_output={:?}",
            hex::encode(hash),
            tx.tx.inputs.len(),
            tx.tx.outputs.len(),
            tx.tx.outputs.first().map(precis_item)
        )
    };
    match (cli.tx_hash, derived_tx_hash) {
        (Some(hash), _) => match client.tx(hash).await {
            Ok(Some(tx)) => {
                record(Step::Ok(fmt_tx_found(scheme, hash, &tx)));
                outcome.tx = Some(tx);
            }
            Ok(None) => record(Step::Warn(format!(
                "[{scheme}] ReadTx {}: not found (explicit --tx-hash, not derived)",
                hex::encode(hash)
            ))),
            Err(e) => record(Step::Fail(format!("[{scheme}] ReadTx failed: {e}"))),
        },
        (None, Some(hash)) => match client.tx(hash).await {
            Ok(Some(tx)) => {
                record(Step::Ok(fmt_tx_found(scheme, hash, &tx)));
                outcome.tx = Some(tx);
            }
            Ok(None) => record(Step::Fail(format!(
                "[{scheme}] ReadTx {}: not found, but it created a UTxO SearchUtxos just returned",
                hex::encode(hash)
            ))),
            Err(e) => record(Step::Fail(format!("[{scheme}] ReadTx failed: {e}"))),
        },
        (None, None) => record(Step::Warn(format!(
            "[{scheme}] no tx to check (no --tx-hash, no UTxO found)"
        ))),
    }

    if let Some(hash) = cli.datum_hash {
        match client.datum(hash).await {
            Ok(Some(datum)) => {
                record(Step::Ok(format!(
                    "[{scheme}] ReadData {}: found, {} byte(s), native_bytes[..8]={}",
                    hex::encode(hash),
                    datum.native_bytes.len(),
                    hex::encode(&datum.native_bytes[..datum.native_bytes.len().min(8)])
                )));
                outcome.datum = Some(datum);
            }
            Ok(None) => record(Step::Warn(format!(
                "[{scheme}] ReadData {}: not found",
                hex::encode(hash)
            ))),
            Err(e) => record(Step::Fail(format!("[{scheme}] ReadData failed: {e}"))),
        }
    }

    if let Some(cred) = stake_cred {
        match client
            .stake_cred(
                cred,
                Paging {
                    limit: Some(1),
                    after: None,
                },
            )
            .await
        {
            Ok(page) => {
                let first = page.items.first().map(precis_item);
                record(Step::Ok(format!(
                    "[{scheme}] SearchUtxos(stake_cred={}): {} item(s), first={:?}",
                    hex::encode(cred),
                    page.items.len(),
                    first
                )));
                outcome.stake_page = Some(page);
            }
            Err(e) => record(Step::Fail(format!(
                "[{scheme}] SearchUtxos (stake_cred) failed: {e}"
            ))),
        }
    }

    match client
        .changes(ChangesParams {
            pay_cred: Some(pay_cred),
            stake_cred,
            ..Default::default()
        })
        .await
    {
        Ok(resp) => {
            match &resp {
                ChangesResponseDto::Changes(c) => record(Step::Ok(format!(
                    "[{scheme}] UtxoChanges: {} created (first={:?}), {} spent (first={:?})",
                    c.created.len(),
                    c.created.first().map(precis_item),
                    c.spent.len(),
                    c.spent.first().map(precis_item)
                ))),
                ChangesResponseDto::Stale(s) => record(Step::Warn(format!(
                    "[{scheme}] UtxoChanges: stale ({:?})",
                    s.reason
                ))),
            }
            outcome.changes = Some(resp);
        }
        Err(e) => record(Step::Fail(format!("[{scheme}] UtxoChanges failed: {e}"))),
    }

    if do_submit {
        let skey = cli.skey.expect("main validated --submit requires --skey");
        match floe_client::self_pay::run(client, skey, true).await {
            Ok(floe_client::self_pay::SelfPayResult::Submitted { r#ref: tx_ref }) => {
                record(Step::Ok(format!(
                    "[{scheme}] SubmitTx: submitted, ref={tx_ref}"
                )));
            }
            Ok(floe_client::self_pay::SelfPayResult::Built { .. }) => {
                unreachable!("self_pay::run(.., submit: true) always returns Submitted or Err")
            }
            Err(floe_client::self_pay::SelfPayError::NoFunds) => {
                record(Step::Warn(format!(
                    "[{scheme}] SubmitTx: no funds at --skey's address, skipped"
                )));
            }
            Err(floe_client::self_pay::SelfPayError::InsufficientFunds) => {
                record(Step::Warn(format!(
                    "[{scheme}] SubmitTx: best UTxO can't cover fee + margin, skipped"
                )));
            }
            Err(floe_client::self_pay::SelfPayError::Other(e)) => {
                record(Step::Fail(format!("[{scheme}] SubmitTx failed: {e}")));
            }
        }
    }

    Ok(outcome)
}

/// gRPC connect fails outright if the port isn't listening yet, unlike
/// HTTP's `Client::new` -- needs its own retry loop before `run_smoke`.
#[cfg(feature = "grpc")]
async fn connect_grpc_with_retry(
    server: &str,
    ready_timeout_secs: u64,
) -> Result<floe_client::grpc::Client> {
    let timeout = Duration::from_secs(ready_timeout_secs);
    let start = Instant::now();
    loop {
        match floe_client::grpc::Client::connect(server).await {
            Ok(client) => return Ok(client),
            Err(e) if start.elapsed() < timeout => {
                debug!(error = %e, "not connected yet, retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => anyhow::bail!("never connected within {timeout:?}: {e}"),
        }
    }
}

/// Prepends `http://` to a bare `host:port`; passes an already-schemed
/// value through.
fn grpc_url(addr: &str) -> String {
    if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// Dispatches to the concrete `Client` for `scheme` (each gated by its
/// own compile-time feature).
async fn run_scheme(scheme: Scheme, cli: &Cli, do_submit: bool) -> Result<SmokeOutcome> {
    match scheme {
        Scheme::Grpc => {
            #[cfg(feature = "grpc")]
            {
                let server = grpc_url(
                    cli.grpc
                        .as_deref()
                        .expect("Scheme::Grpc only chosen when --grpc is given"),
                );
                let mut client = connect_grpc_with_retry(&server, cli.ready_timeout_secs).await?;
                run_smoke(&mut client, cli, scheme, do_submit).await
            }
            #[cfg(not(feature = "grpc"))]
            anyhow::bail!("--grpc: not compiled in (missing the `grpc` feature)");
        }
        Scheme::HttpJson => {
            #[cfg(feature = "http-json")]
            {
                let server = cli
                    .http_json
                    .as_deref()
                    .expect("Scheme::HttpJson only chosen when --http-json is given");
                let mut client = floe_client::http_json::Client::new(server);
                run_smoke(&mut client, cli, scheme, do_submit).await
            }
            #[cfg(not(feature = "http-json"))]
            anyhow::bail!("--http-json: not compiled in (missing the `http-json` feature)");
        }
        Scheme::HttpCbor => {
            #[cfg(feature = "http-cbor")]
            {
                let server = cli
                    .http_cbor
                    .as_deref()
                    .expect("Scheme::HttpCbor only chosen when --http-cbor is given");
                let mut client = floe_client::http_cbor::Client::new(server);
                run_smoke(&mut client, cli, scheme, do_submit).await
            }
            #[cfg(not(feature = "http-cbor"))]
            anyhow::bail!("--http-cbor: not compiled in (missing the `http-cbor` feature)");
        }
    }
}

/// The ledger tip can advance between two schemes' calls; that's not a
/// disagreement.
fn normalize_tip(tip: &mut ChainPointDto) {
    *tip = ChainPointDto::default();
}

fn fingerprint_page(page: &PageDto) -> String {
    let mut page = page.clone();
    normalize_tip(&mut page.ledger_tip);
    format!("{page:?}")
}

fn fingerprint_tx(tx: &TxResponseDto) -> String {
    let mut tx = tx.clone();
    normalize_tip(&mut tx.ledger_tip);
    format!("{tx:?}")
}

fn fingerprint_datum(datum: &DatumResponseDto) -> String {
    let mut datum = datum.clone();
    normalize_tip(&mut datum.ledger_tip);
    format!("{datum:?}")
}

fn fingerprint_changes(changes: &ChangesResponseDto) -> String {
    let mut changes = changes.clone();
    if let ChangesResponseDto::Changes(c) = &mut changes {
        normalize_tip(&mut c.ledger_tip);
    }
    format!("{changes:?}")
}

/// Compares one field of a baseline scheme's outcome against every
/// other scheme's, one step per field (not per pair).
fn cross_check<T>(
    field: &str,
    baseline: (Scheme, &Option<T>),
    others: &[(Scheme, Option<T>)],
    fingerprint: impl Fn(&T) -> String,
) -> Step {
    let (base_scheme, base_val) = baseline;
    let Some(base_val) = base_val else {
        return Step::Warn(format!(
            "{field}: {base_scheme} has nothing to compare (absent data)"
        ));
    };
    let base_fp = fingerprint(base_val);
    let mut mismatches = Vec::new();
    for (scheme, val) in others {
        match val {
            Some(v) if fingerprint(v) == base_fp => {}
            Some(_) => mismatches.push(format!("{scheme} disagrees with {base_scheme}")),
            None => mismatches.push(format!(
                "{scheme} returned nothing but {base_scheme} didn't"
            )),
        }
    }
    if mismatches.is_empty() {
        Step::Ok(format!(
            "cross-scheme {field}: all {} scheme(s) agree",
            others.len() + 1
        ))
    } else {
        Step::Fail(format!("cross-scheme {field}: {}", mismatches.join("; ")))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    if cli.submit && cli.skey.is_none() {
        anyhow::bail!("--submit requires --skey (or $FLOE_SMOKE_SKEY)");
    }
    if cli.grpc.is_none() && cli.http_json.is_none() && cli.http_cbor.is_none() {
        anyhow::bail!(
            "need at least one of --grpc/--http-json/--http-cbor to know what to smoke-test"
        );
    }

    let mut schemes = Vec::new();
    if cli.grpc.is_some() {
        schemes.push(Scheme::Grpc);
    }
    if cli.http_json.is_some() {
        schemes.push(Scheme::HttpJson);
    }
    if cli.http_cbor.is_some() {
        schemes.push(Scheme::HttpCbor);
    }

    let mut outcomes = Vec::with_capacity(schemes.len());
    let mut ok = true;
    for (i, &scheme) in schemes.iter().enumerate() {
        let do_submit = cli.submit && i == 0;
        match run_scheme(scheme, &cli, do_submit).await {
            Ok(outcome) => {
                ok &= outcome.ok;
                outcomes.push((scheme, outcome));
            }
            Err(e) => {
                error!("[fail] [{scheme}] {e}");
                ok = false;
            }
        }
    }

    if outcomes.len() > 1 {
        let (base_scheme, base) = &outcomes[0];
        let others: Vec<(Scheme, &SmokeOutcome)> =
            outcomes[1..].iter().map(|(s, o)| (*s, o)).collect();

        macro_rules! check {
            ($field:ident, $fp:expr) => {
                let others_field: Vec<(Scheme, Option<_>)> =
                    others.iter().map(|(s, o)| (*s, o.$field.clone())).collect();
                let step = cross_check(
                    stringify!($field),
                    (*base_scheme, &base.$field),
                    &others_field,
                    $fp,
                );
                if matches!(step, Step::Fail(_)) {
                    ok = false;
                }
                report(&step);
            };
        }
        check!(page, fingerprint_page);
        check!(reread, |u: &UtxoDto| format!("{u:?}"));
        check!(stake_page, fingerprint_page);
        check!(tx, fingerprint_tx);
        check!(datum, fingerprint_datum);
        check!(changes, fingerprint_changes);
    }

    if ok {
        info!("SMOKE OK");
        Ok(())
    } else {
        error!("SMOKE FAILED");
        std::process::exit(1);
    }
}
