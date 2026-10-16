//! floe-integration: integration tests between `floe-server` and
//! `floe-client` -- a real `floe serve` subprocess, driven two ways.
//! Not literally end-to-end (no real Cardano peer; see
//! `floe-server`'s own `sync.rs`/`catchup.rs` for the piece that talks
//! to one) -- just the server/client wire contract, which is what this
//! crate was always actually testing despite its old name (`floe-e2e`).
//!
//! Data round-trips (`ref`/`address`/`pay-cred`/`tx`/`datum`/`changes`)
//! go through `floe_client`'s *library* (`Backend` impls, called
//! in-process, asserting on typed `floe_dto_http` DTOs) rather than
//! spawning the `floe-client` binary and scraping its stdout -- once a
//! real library target existed, a subprocess-plus-string-matching
//! round trip was only ever a weaker proxy for exactly what the
//! library itself can assert on directly. The server half stays a real
//! subprocess either way (`ServerProc`): that's what still catches
//! "the client actually speaking gRPC/HTTP to the server's actual
//! listener," which an in-process router (`floe-server`'s own
//! `scheme::grpc::tests`) can't.
//!
//! `assert_full_round_trip`, below, is the one fixture-aware assertion
//! body, generic over `B: Backend` -- `grpc_backend_full_round_trip`/
//! `http_json_backend_full_round_trip`/`http_cbor_backend_full_round_trip`
//! each just construct their own scheme's `Client` and call it against
//! their own spawned server. This is "full coverage" in the sense of
//! every `Backend` method exercised against every compiled scheme at
//! least once -- not an exhaustive cross product of every fixture shape
//! against every scheme against every pagination edge case (that finer
//! -grained coverage already exists, cheaper, in `floe-server`'s own
//! unit tests, e.g. `scheme::grpc::tests`' `utxo_changes_*` cases and
//! `store::query::tests::resolve_handles_every_datum_and_script_shape`).
//!
//! CLI-specific behavior -- exit codes, stderr messages on a miss --
//! has no library-level equivalent (`Backend::tx`/`datum` just return
//! `Ok(None)`, `Backend::changes` returns `ChangesResponseDto::Stale`;
//! turning those into a particular exit code and message is
//! `main.rs`'s own dispatch logic). Those cases still spawn the real
//! `floe-client` binary (`run_client`), the only way to exercise that
//! layer at all -- kept to the default (gRPC) scheme, since the
//! exit-code logic itself is `dispatch`'s one generic code path, not
//! per-scheme.
//!
//! Rewritten when `floe-server` dropped REST for gRPC (see
//! `plan-rpc.md`), then again when `floe-client` gained a library
//! target (see `../../plan-floe-client-as-lib.md`), then renamed
//! `floe-e2e` -> `floe-integration` to match what it actually tests.
//!
//! Finding the `floe`/`floe-client` binaries (still needed for the
//! CLI-specific tests) is hand-rolled (`target/<profile>/<bin>`
//! relative to the workspace root) rather than pulling in a
//! binary-discovery crate like `escargot`/`assert_cmd` -- known to be a
//! little brittle (a custom `--target-dir` beyond `CARGO_TARGET_DIR`, or
//! cross-compilation, would break it), accepted in exchange for zero new
//! dependencies. Running `cargo test --workspace` (what CI does) always
//! builds both binaries before this test binary runs, since they're
//! workspace members too; running `cargo test -p floe-integration` in
//! isolation needs them already built.
//!
//! Each test gets its own fixture DB and its own `floe serve` process
//! on its own OS-assigned ports -- independent, parallel-safe, no
//! shared state.

use std::collections::BTreeMap;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::time::Duration;

use floe::catchup::Engine;
use floe::decode::{DatumKind, DatumRow, DecodedBlock, OutputRef, ScriptRow, TxRow, Utxo};
use floe::store::{self, BlockMeta};
use floe_client::Backend;
use floe_dto_http::{ChangesResponseDto, ScriptLang};
use pallas_addresses::Address;
use pallas_codec::utils::{Bytes, PositiveCoin};
use pallas_crypto::hash::Hash;
use rusqlite::Connection;

struct TempDb(PathBuf);

impl TempDb {
    fn create() -> Self {
        let path = std::env::temp_dir().join(format!(
            "floe_integration_{}_{}.sqlite",
            std::process::id(),
            rand_suffix()
        ));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// `61`-repeated is this whole suite's one fixture pay credential
/// (every seeded UTxO's header byte `0x61`, mainnet/enterprise/
/// key-hash).
const PAY_CRED: [u8; 28] = [0x61u8; 28];

const TX0: [u8; 32] = [0u8; 32];
const TX1: [u8; 32] = [2u8; 32];
const TX2: [u8; 32] = [3u8; 32];
const DATUM_HASH: [u8; 32] = [0x42u8; 32];
const DATUM_CBOR: [u8; 4] = [1, 2, 3, 4];
const ASSET_POLICY: [u8; 28] = [0xAAu8; 28];
const ASSET_NAME: &[u8] = b"TOKEN";
const ASSET_QTY: u64 = 5;
const SCRIPT_HASH: [u8; 28] = [0xBBu8; 28];
const SCRIPT_CBOR: [u8; 3] = [9, 9, 9];

/// Raw CBOR of a one-policy, one-asset `Multiasset<PositiveCoin>` map
/// -- the same shape `floe-server`'s `decode.rs` stores and
/// `scheme::grpc`/`scheme::http` decode back out (`decode_assets`/
/// `asset_dtos`).
fn asset_cbor() -> Vec<u8> {
    let mut assets = BTreeMap::new();
    assets.insert(
        Bytes::from(ASSET_NAME.to_vec()),
        PositiveCoin::try_from(ASSET_QTY).unwrap(),
    );
    let mut multiasset = BTreeMap::new();
    multiasset.insert(Hash::<28>::from(ASSET_POLICY), assets);
    pallas_codec::minicbor::to_vec(&multiasset).expect("infallible")
}

/// Three UTxOs at the same pay credential, one block: a plain one
/// (`TX0`, coin 7 -- the suite's simplest case, used by `ref`'s exact-
/// key lookup), one with an asset and a reference script (`TX1`, coin
/// 11), and one with a resolved hash-kind datum (`TX2`, coin 13).
/// Enough shape variety to exercise `UtxoDto`'s whole surface across
/// every scheme without a combinatorial fixture set -- see this
/// module's doc comment.
fn seed(path: &Path) {
    let mut conn = Connection::open(path).unwrap();
    store::init(&conn).unwrap();
    let plain = Utxo {
        output_ref: OutputRef {
            tx_hash: TX0,
            ix: 0,
        },
        pay_cred: PAY_CRED,
        pay_script: false,
        stake_cred: None,
        stake_script: None,
        coin: 7,
        assets: None,
        datum_kind: DatumKind::None,
        datum_hash: None,
        script_hash: None,
    };
    let with_asset_and_script = Utxo {
        output_ref: OutputRef {
            tx_hash: TX1,
            ix: 0,
        },
        pay_cred: PAY_CRED,
        pay_script: false,
        stake_cred: None,
        stake_script: None,
        coin: 11,
        assets: Some(asset_cbor()),
        datum_kind: DatumKind::None,
        datum_hash: None,
        script_hash: Some(SCRIPT_HASH),
    };
    let with_datum = Utxo {
        output_ref: OutputRef {
            tx_hash: TX2,
            ix: 0,
        },
        pay_cred: PAY_CRED,
        pay_script: false,
        stake_cred: None,
        stake_script: None,
        coin: 13,
        assets: None,
        datum_kind: DatumKind::Hash,
        datum_hash: Some(DATUM_HASH),
        script_hash: None,
    };
    let decoded = DecodedBlock {
        txs: [TX0, TX1, TX2]
            .into_iter()
            .map(|hash| TxRow { hash })
            .collect(),
        spent: Default::default(),
        created: vec![plain, with_asset_and_script, with_datum],
        datums: vec![DatumRow {
            hash: DATUM_HASH,
            cbor: DATUM_CBOR.to_vec(),
        }],
        scripts: vec![ScriptRow {
            hash: SCRIPT_HASH,
            lang: ScriptLang::PlutusV2,
            cbor: SCRIPT_CBOR.to_vec(),
        }],
    };
    store::apply(
        &mut conn,
        &BlockMeta {
            slot: 1,
            hash: [1; 32],
            prev_hash: [0; 32],
            block_no: Some(1),
        },
        &decoded,
    )
    .unwrap();
    Engine::new(store::tip(&conn).unwrap())
        .switch_to_running(&mut conn)
        .unwrap();
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            // crates/integration -> crates -> workspace root
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .join("target")
        })
}

fn profile_dir() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn bin_path(name: &str) -> PathBuf {
    let path = target_dir().join(profile_dir()).join(name);
    assert!(
        path.exists(),
        "{name} binary not found at {path:?} -- run `cargo build --workspace` \
         (matching whatever profile these tests are running in) first"
    );
    path
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_port(addr: &str) {
    for _ in 0..200 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("floe serve never came up on {addr}");
}

struct ServerProc {
    child: Child,
    addr: String,
    http_addr: String,
}

impl ServerProc {
    /// Binds both `--grpc` and `--http`: most tests only use one, but
    /// a couple (parity checks) want both schemes live against the
    /// same DB at once.
    fn spawn(db_path: &Path) -> Self {
        let addr = format!("127.0.0.1:{}", free_port());
        let http_addr = format!("127.0.0.1:{}", free_port());
        let mut cmd = Command::new(bin_path("floe"));
        cmd.args([
            "serve",
            "--db",
            db_path.to_str().unwrap(),
            "--magic",
            "1",
            "--grpc",
            &addr,
            "--http",
            &http_addr,
        ]);
        let child = cmd.spawn().expect("spawn `floe serve`");
        wait_for_port(&addr);
        wait_for_port(&http_addr);
        Self {
            child,
            addr,
            http_addr,
        }
    }
}

impl Drop for ServerProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn run_client(server_addr: &str, args: &[&str]) -> Output {
    let mut cmd = Command::new(bin_path("floe-client"));
    cmd.args(["--server", &format!("http://{server_addr}")]);
    cmd.args(args);
    cmd.output().expect("spawn `floe-client`")
}

// --- the one fixture-aware assertion body, generic over `Backend` -----

/// Exercises every `Backend` method against `seed`'s fixture --
/// `grpc_backend_full_round_trip`/`http_json_backend_full_round_trip`/
/// `http_cbor_backend_full_round_trip` each just supply a different
/// concrete `client`.
async fn assert_full_round_trip<B: Backend>(client: &mut B) {
    // read_ref: the plain UTxO, and a miss.
    let utxo = client
        .read_ref(TX0, 0)
        .await
        .unwrap()
        .expect("TX0 was seeded");
    assert_eq!(utxo.coin, 7);
    assert!(client.read_ref([0xffu8; 32], 0).await.unwrap().is_none());

    // pay_cred: all three rows, tip order, with their asset/script/datum intact.
    let page = client
        .pay_cred(PAY_CRED, floe_client::Paging::default())
        .await
        .unwrap();
    assert_eq!(page.items.len(), 3);
    assert_eq!(page.items[0].coin, 7);
    assert_eq!(page.items[1].coin, 11);
    assert_eq!(page.items[1].assets.len(), 1);
    assert_eq!(page.items[1].assets[0].policy_id, ASSET_POLICY);
    assert_eq!(page.items[1].assets[0].assets[0].name, ASSET_NAME);
    assert_eq!(page.items[1].assets[0].assets[0].quantity, ASSET_QTY);
    let script = page.items[1]
        .script
        .as_ref()
        .expect("TX1 has a reference script");
    assert_eq!(script.lang, ScriptLang::PlutusV2);
    assert_eq!(script.cbor, SCRIPT_CBOR);
    assert_eq!(page.items[2].coin, 13);
    let datum = page.items[2]
        .datum
        .as_ref()
        .expect("TX2 has a resolved datum");
    assert_eq!(datum.hash, DATUM_HASH);
    assert_eq!(datum.original_cbor.as_deref(), Some(&DATUM_CBOR[..]));

    // address: same fixture, reached via bech32 + client-side credential decomposition.
    let bech32 = Address::from_bytes(&[&[0x61u8][..], &PAY_CRED].concat())
        .unwrap()
        .to_bech32()
        .unwrap();
    let addr: Address = bech32.parse().unwrap();
    let (pay_cred, stake_cred) = floe_client::shelley_creds(&addr).unwrap();
    assert_eq!(pay_cred, PAY_CRED);
    assert_eq!(stake_cred, None);
    let page = client
        .address(pay_cred, stake_cred, floe_client::Paging::default())
        .await
        .unwrap();
    assert_eq!(page.items.len(), 3);

    // stake_cred: none of this fixture's rows carry one, so a search for
    // an arbitrary one comes back empty rather than erroring.
    let page = client
        .stake_cred([0u8; 28], floe_client::Paging::default())
        .await
        .unwrap();
    assert!(page.items.is_empty());

    // tx: hydrated inputs/outputs, and a miss. `block`/`spent_by` are
    // the shared "unknown"/`None` shape every scheme's `ReadTx`
    // equivalent reports (see `floe-dto-http`'s `TxDto` doc comment) --
    // asserted here as a floor, and cross-scheme agreement is this
    // file's own `tx_lookup_agrees_across_schemes` test.
    let tx = client.tx(TX0).await.unwrap().expect("TX0 was seeded");
    assert_eq!(tx.tx.outputs.len(), 1);
    assert_eq!(tx.tx.outputs[0].coin, 7);
    assert!(client.tx([0xffu8; 32]).await.unwrap().is_none());

    // datum: the resolved hash-kind datum, and a miss.
    let datum = client
        .datum(DATUM_HASH)
        .await
        .unwrap()
        .expect("seeded above");
    assert_eq!(datum.native_bytes, DATUM_CBOR);
    assert!(client.datum([0xffu8; 32]).await.unwrap().is_none());

    // changes: everything reported as created from genesis.
    let params = floe_client::ChangesParams {
        pay_cred: Some(PAY_CRED),
        since_slot: Some(0),
        ..Default::default()
    };
    match client.changes(params).await.unwrap() {
        ChangesResponseDto::Changes(c) => assert_eq!(c.created.len(), 3),
        ChangesResponseDto::Stale(_) => panic!("since_slot=0 is never stale"),
    }
}

#[tokio::test]
async fn grpc_backend_full_round_trip() {
    let db = TempDb::create();
    seed(db.path());
    let server = ServerProc::spawn(db.path());
    let mut client = floe_client::grpc::Client::connect(&format!("http://{}", server.addr))
        .await
        .unwrap();
    assert_full_round_trip(&mut client).await;
}

#[tokio::test]
async fn http_json_backend_full_round_trip() {
    let db = TempDb::create();
    seed(db.path());
    let server = ServerProc::spawn(db.path());
    let mut client = floe_client::http_json::Client::new(&server.http_addr);
    assert_full_round_trip(&mut client).await;
}

#[tokio::test]
async fn http_cbor_backend_full_round_trip() {
    let db = TempDb::create();
    seed(db.path());
    let server = ServerProc::spawn(db.path());
    let mut client = floe_client::http_cbor::Client::new(&server.http_addr);
    assert_full_round_trip(&mut client).await;
}

/// `--transport grpc` and `--transport http` must report byte-for-byte
/// identical `tx` content -- not just each individually matching the
/// fixture (`assert_full_round_trip`'s `tx` checks), but matching each
/// other, including the shared "unknown block, no `spent_by`" shape
/// (see `floe-dto-http`'s `TxDto` doc comment).
#[tokio::test]
async fn tx_lookup_agrees_across_schemes() {
    let db = TempDb::create();
    seed(db.path());
    let server = ServerProc::spawn(db.path());

    let mut grpc = floe_client::grpc::Client::connect(&format!("http://{}", server.addr))
        .await
        .unwrap();
    let mut http = floe_client::http_json::Client::new(&server.http_addr);

    let via_grpc = grpc.tx(TX0).await.unwrap().expect("TX0 was seeded");
    let via_http = http.tx(TX0).await.unwrap().expect("TX0 was seeded");

    assert_eq!(via_grpc.tx.outputs[0].coin, via_http.tx.outputs[0].coin);
    assert_eq!(via_grpc.tx.outputs[0].block, via_http.tx.outputs[0].block);
    assert_eq!(
        via_grpc.tx.outputs[0].spent_by,
        via_http.tx.outputs[0].spent_by
    );
    assert_eq!(
        via_grpc.tx.outputs[0].block,
        floe_dto_http::ChainPointDto::default()
    );
    assert_eq!(via_grpc.tx.outputs[0].spent_by, None);
}

// --- CLI-specific behavior: exit codes and stderr messages --------------
//
// No library-level equivalent (see this module's doc comment) --
// spawns the real `floe-client` binary. Kept to the default (gRPC)
// scheme: the exit-code logic is `dispatch`'s one generic code path,
// not per-scheme, so there's nothing scheme-specific left to cover here.

#[test]
fn ref_lookup_reports_not_found_for_a_missing_key() {
    let db = TempDb::create();
    seed(db.path());
    let server = ServerProc::spawn(db.path());

    let out = run_client(&server.addr, &["ref", &"ff".repeat(32), "0"]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("not found"));
}

#[test]
fn tx_lookup_reports_not_found_for_a_missing_hash() {
    let db = TempDb::create();
    seed(db.path());
    let server = ServerProc::spawn(db.path());

    let out = run_client(&server.addr, &["tx", &"ff".repeat(32)]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("not found"));
}

#[test]
fn datum_lookup_reports_not_found_for_a_missing_hash() {
    let db = TempDb::create();
    seed(db.path());
    let server = ServerProc::spawn(db.path());

    let out = run_client(&server.addr, &["datum", &"ff".repeat(32)]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("not found"));
}

#[test]
fn changes_reports_stale_for_an_unknown_since_hash() {
    let db = TempDb::create();
    seed(db.path());
    let server = ServerProc::spawn(db.path());

    let out = run_client(
        &server.addr,
        &[
            "changes",
            "--pay-cred",
            &hex::encode(PAY_CRED),
            "--since-hash",
            &"ff".repeat(32),
        ],
    );
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("stale"));
}
