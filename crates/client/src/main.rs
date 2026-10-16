//! floe-client: CLI front end over the `floe_client` library -- parses
//! args, builds whichever scheme's `Client` the flags ask for, calls
//! one generic `dispatch<B: Backend>`, prints the result as JSON.
//! Needs `clap` (arg parsing) and `serde_json` (printing) that the
//! library itself never does -- see `floe_client`'s own doc comment.
//!
//! usage:
//! ```text
//! cargo run -p floe-client -- [--server <url>] <command> ...
//! cargo run -p floe-client -- ref <tx_hash_hex> <ix>
//! cargo run -p floe-client -- address <address_bech32_or_hex> [--after <cursor>] [--limit <n>]
//! cargo run -p floe-client -- pay-cred <cred_hex> [--after ...] [--limit ...]
//! cargo run -p floe-client -- stake-cred <cred_hex> [--after ...] [--limit ...]
//! cargo run -p floe-client -- tx <tx_hash_hex>
//! cargo run -p floe-client -- datum <datum_hash_hex>
//! cargo run -p floe-client -- changes --pay-cred <cred_hex> [--stake-cred <cred_hex>] [--since-slot <n>] [--since-hash <block_hash_hex>] [--max-items <n>] [--created-after <cursor>] [--spent-after <cursor>]
//! cargo run -p floe-client -- submit <tx_cbor_hex>
//! cargo run -p floe-client -- selfpay --skey <ed25519_skey_hex> [--submit]
//! cargo run -p floe-client -- key-info --skey <ed25519_skey_hex> [--network <network>] [--output-json]
//! cargo run -p floe-client -- --transport http --format cbor pay-cred <cred_hex> --limit 1
//! ```

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use pallas_addresses::Address;

use floe_client::{Backend, ChangesParams, Cred, NetworkArg, Paging, TxHash, parse_hex};

/// Which transport a command goes over. `selfpay` now works over
/// either (generic over `Backend`, see `floe_client::self_pay`) --
/// unlike the old gRPC-only client, there's no restriction here
/// anymore.
#[derive(Debug, Clone, Copy, ValueEnum, Default, PartialEq, Eq)]
enum Transport {
    #[default]
    Grpc,
    Http,
}

/// Wire format for `--transport http` (ignored otherwise).
#[derive(Debug, Clone, Copy, ValueEnum, Default, PartialEq, Eq)]
enum Format {
    #[default]
    Json,
    Cbor,
}

#[derive(Parser)]
#[command(name = "floe-client", about = "Query a running floe server")]
struct Cli {
    /// Base URL/address of the floe server: a full URL for
    /// `--transport grpc`, host:port for `--transport http` (any
    /// `http://`/`https://` prefix is stripped -- REST hand-rolls
    /// HTTP/1.1 over a raw socket, same as `floe-server`'s
    /// `submit.rs`).
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    server: String,

    /// gRPC or REST.
    #[arg(long, value_enum, default_value_t = Transport::Grpc)]
    transport: Transport,

    /// Wire format for `--transport http` (ignored otherwise).
    #[arg(long, value_enum, default_value_t = Format::Json)]
    format: Format,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// One UTxO by its reference (`ReadUtxos`).
    Ref {
        #[arg(value_parser = parse_hex::<32>)]
        tx_hash: TxHash,
        ix: u32,
    },
    /// UTxOs at an address, tip set only (`SearchUtxos`). Accepts bech32
    /// or hex; decomposed into payment/delegation credentials client-side
    /// (`floe_client::shelley_creds`) -- same as the server's own storage
    /// -- since no scheme's address-pattern wire shape has a raw-address
    /// field floe supports. No network check against the server here --
    /// a credential from the wrong network just won't match anything.
    Address {
        address: String,
        #[command(flatten)]
        paging: Paging,
    },
    /// UTxOs by payment credential, tip set only, key-or-script.
    PayCred {
        #[arg(value_parser = parse_hex::<28>)]
        cred: Cred,
        #[command(flatten)]
        paging: Paging,
    },
    /// UTxOs by stake credential, tip set only.
    StakeCred {
        #[arg(value_parser = parse_hex::<28>)]
        cred: Cred,
        #[command(flatten)]
        paging: Paging,
    },
    /// A transaction by hash, hydrated from indexed UTxOs (`ReadTx`) --
    /// in/outputs only, no certs/mint/fee/collateral/withdrawals/
    /// auxiliary_data/redeemer (see `floe-server`'s `scheme::grpc`).
    Tx {
        #[arg(value_parser = parse_hex::<32>)]
        tx_hash: TxHash,
    },
    /// A datum by hash (`ReadData`) -- raw bytes only, no parsed
    /// `PlutusData` (not implemented yet).
    Datum {
        #[arg(value_parser = parse_hex::<32>)]
        datum_hash: TxHash,
    },
    /// What changed (created or spent) at an address since a `Point`
    /// (`UtxoChanges`, floe's own addition). At least one of
    /// `--pay-cred`/`--stake-cred` is required. With neither
    /// `--since-slot` nor `--since-hash`, reports everything from
    /// genesis. `--since-hash` alone is the reorg-safe form (the
    /// server verifies it's still on the canonical chain); giving
    /// both and having them disagree, or giving a hash the server
    /// doesn't recognize, is reported as "stale" -- this client's job
    /// ends there, finding an earlier intersection and retrying is the
    /// caller's own responsibility.
    Changes {
        #[arg(long, value_parser = parse_hex::<28>)]
        pay_cred: Option<Cred>,
        #[arg(long, value_parser = parse_hex::<28>)]
        stake_cred: Option<Cred>,
        #[arg(long)]
        since_slot: Option<u64>,
        #[arg(long, value_parser = parse_hex::<32>)]
        since_hash: Option<TxHash>,
        /// Max rows per list (`created`/`spent` paginate
        /// independently -- server caps this, see `--max-page-size`
        /// on `floe-server`).
        #[arg(long)]
        max_items: Option<i32>,
        /// Resume cursor for `created`, from a previous response's
        /// `created_next_token`.
        #[arg(long)]
        created_after: Option<String>,
        /// Resume cursor for `spent`, from a previous response's
        /// `spent_next_token`.
        #[arg(long)]
        spent_after: Option<String>,
    },
    /// Submit a transaction (`SubmitTx`) -- hex-encoded raw tx CBOR,
    /// e.g. `selfpay`'s own output. Only works if `floe-server` was
    /// started with `--amaru-submit-api`.
    Submit { tx_hex: String },
    /// Builds a minimal self-payment (one input, one output back to
    /// the same address minus a flat fee guess) from a raw ed25519
    /// signing key: queries this server for a live UTxO at the key's
    /// own enterprise address, hand-rolls and signs the tx. The
    /// address is derived from `--skey`, not given separately, so the
    /// two can't disagree.
    ///
    /// Exits 2 with "no funds, cannot test submit" if the address has
    /// no UTxOs, or 3 with "insufficient funds, cannot test submit" if
    /// its best one can't cover the fee plus a safety margin --
    /// distinct, scriptable outcomes for `floe-smoke`, not just a
    /// generic failure.
    Selfpay {
        /// Raw ed25519 secret key, hex (32 bytes).
        #[arg(long, env = "FLOE_CLIENT_SKEY", value_parser = parse_hex::<32>)]
        skey: [u8; 32],
        /// Submit the built tx immediately (`SubmitTx`) instead of
        /// just printing its hex.
        #[arg(long)]
        submit: bool,
    },
    /// Prints a signing key's payment credential (blake2b-224 of its
    /// ed25519 public key) and the address it derives (enterprise, no
    /// delegation part), under whichever network's bech32 prefix
    /// `--network` picks. No network call; doesn't need `--server`.
    KeyInfo {
        /// Raw ed25519 secret key, hex (32 bytes).
        #[arg(long, env = "FLOE_CLIENT_SKEY", value_parser = parse_hex::<32>)]
        skey: [u8; 32],
        /// Network whose bech32 prefix the derived address uses,
        /// case-insensitive (`mainnet`/`testnet`/`preprod`/`preview`).
        #[arg(long, env = "FLOE_CLIENT_NETWORK", default_value = "mainnet")]
        network: NetworkArg,
        /// Print `address=`/`vkh=` as a single JSON object instead of
        /// `key=value` lines.
        #[arg(long)]
        output_json: bool,
    },
}

/// Whichever DTO (or `self_pay`'s own result) a `dispatch` arm
/// produced -- the single print call site in `main` serializes
/// whichever variant came back.
#[derive(serde::Serialize)]
#[serde(untagged)]
enum Output {
    Utxo(floe_dto_http::UtxoDto),
    Page(floe_dto_http::PageDto),
    Tx(floe_dto_http::TxResponseDto),
    Datum(floe_dto_http::DatumResponseDto),
    Changes(floe_dto_http::ChangesDto),
    Submit(floe_dto_http::SubmitTxResponseDto),
    SelfPay(floe_client::self_pay::SelfPayResult),
}

/// The one dispatch function, generic over `B: Backend` -- this is
/// what keeps `main` from tripling its match arms per scheme:
/// genericity does the sharing, not an enum + manual delegation. Each
/// arm's content-dependent outcome (not found, stale, `selfpay`'s
/// no-funds/insufficient-funds, a rejected submit) prints its own
/// stderr message and exits directly; only the success path returns,
/// for `main`'s single print call site.
async fn dispatch<B: Backend>(client: &mut B, cmd: Command) -> Result<Output> {
    match cmd {
        Command::Ref { tx_hash, ix } => match client.read_ref(tx_hash, ix).await? {
            Some(utxo) => Ok(Output::Utxo(utxo)),
            None => {
                eprintln!("not found");
                std::process::exit(1);
            }
        },
        Command::Address { address, paging } => {
            let addr: Address = address.parse()?;
            let (pay_cred, stake_cred) = floe_client::shelley_creds(&addr)?;
            Ok(Output::Page(
                client.address(pay_cred, stake_cred, paging).await?,
            ))
        }
        Command::PayCred { cred, paging } => Ok(Output::Page(client.pay_cred(cred, paging).await?)),
        Command::StakeCred { cred, paging } => {
            Ok(Output::Page(client.stake_cred(cred, paging).await?))
        }
        Command::Tx { tx_hash } => match client.tx(tx_hash).await? {
            Some(tx) => Ok(Output::Tx(tx)),
            None => {
                eprintln!("not found");
                std::process::exit(1);
            }
        },
        Command::Datum { datum_hash } => match client.datum(datum_hash).await? {
            Some(d) => Ok(Output::Datum(d)),
            None => {
                eprintln!("not found");
                std::process::exit(1);
            }
        },
        Command::Changes {
            pay_cred,
            stake_cred,
            since_slot,
            since_hash,
            max_items,
            created_after,
            spent_after,
        } => {
            if pay_cred.is_none() && stake_cred.is_none() {
                anyhow::bail!("changes needs at least one of --pay-cred/--stake-cred");
            }
            let params = ChangesParams {
                pay_cred,
                stake_cred,
                since_slot,
                since_hash,
                max_items,
                created_after,
                spent_after,
            };
            match client.changes(params).await? {
                floe_dto_http::ChangesResponseDto::Changes(c) => Ok(Output::Changes(c)),
                floe_dto_http::ChangesResponseDto::Stale(stale) => {
                    eprintln!(
                        "stale ({:?}, oldest retained slot: {:?}): since is not usable -- \
                         find an earlier intersection and retry",
                        stale.reason, stale.oldest_retained_slot
                    );
                    std::process::exit(1);
                }
            }
        }
        Command::Submit { tx_hex } => {
            let raw = hex::decode(&tx_hex)?;
            match client.submit(raw).await {
                Ok(resp) => Ok(Output::Submit(resp)),
                Err(e) => {
                    eprintln!("rejected: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::Selfpay { skey, submit } => {
            match floe_client::self_pay::run(client, skey, submit).await {
                Ok(result) => Ok(Output::SelfPay(result)),
                Err(floe_client::self_pay::SelfPayError::NoFunds) => {
                    eprintln!("no funds, cannot test submit");
                    std::process::exit(2);
                }
                Err(floe_client::self_pay::SelfPayError::InsufficientFunds) => {
                    eprintln!("insufficient funds, cannot test submit");
                    std::process::exit(3);
                }
                Err(floe_client::self_pay::SelfPayError::Other(e)) => Err(e),
            }
        }
        Command::KeyInfo { .. } => unreachable!("main() handles KeyInfo before dispatch"),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Pure local computation, no server/transport involved -- handled
    // before any client is constructed, so it works with no
    // `floe-server` running.
    if let Command::KeyInfo {
        skey,
        network,
        output_json,
    } = &cli.command
    {
        let info = floe_client::key_info::key_info(*skey, network.0)?;
        if *output_json {
            println!("{}", serde_json::to_string(&info)?);
        } else {
            println!("address={}", info.address);
            println!("vkh={}", info.vkh);
        }
        return Ok(());
    }

    let output = match (cli.transport, cli.format) {
        (Transport::Grpc, _) => {
            #[cfg(feature = "grpc")]
            {
                let mut client = floe_client::grpc::Client::connect(&cli.server).await?;
                dispatch(&mut client, cli.command).await?
            }
            #[cfg(not(feature = "grpc"))]
            anyhow::bail!("--transport grpc: not compiled in (missing the `grpc` feature)");
        }
        (Transport::Http, Format::Json) => {
            #[cfg(feature = "http-json")]
            {
                let mut client = floe_client::http_json::Client::new(&cli.server);
                dispatch(&mut client, cli.command).await?
            }
            #[cfg(not(feature = "http-json"))]
            anyhow::bail!(
                "--transport http --format json: not compiled in (missing the `http-json` feature)"
            );
        }
        (Transport::Http, Format::Cbor) => {
            #[cfg(feature = "http-cbor")]
            {
                let mut client = floe_client::http_cbor::Client::new(&cli.server);
                dispatch(&mut client, cli.command).await?
            }
            #[cfg(not(feature = "http-cbor"))]
            anyhow::bail!(
                "--transport http --format cbor: not compiled in (missing the `http-cbor` feature)"
            );
        }
    };

    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
