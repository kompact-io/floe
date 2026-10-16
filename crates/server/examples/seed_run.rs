//! seed_run: seed a real SQLite DB from an amaru bootstrap snapshot archive
//! via `floe::seed::run`, then (optionally) hand off to `sync::run` against
//! a live peer to prove the post-seed continuity actually works.
//!
//! usage:
//!   cargo run --release --example seed_run -- <snapshot.tar.zst> <db_path> \
//!       [--peer host:port --magic N [--blocks N]]
//!
//! The archive is `snapshots/<net>/<slot>.<hash>.tar.zst`; its state includes
//! the anchor block, so sync must (and does) start at the next one.

use std::fs::File;

use anyhow::{Result, anyhow, bail};
use floe::catchup::Engine;
use floe::{seed, store, sync};
use pallas_network::miniprotocols::Point;
use rusqlite::Connection;

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let archive = args.next().ok_or_else(|| anyhow!("usage: <snapshot.tar.zst> <db_path> [--switch] [--peer host:port --magic N [--blocks N]]"))?;
    let db_path = args.next().ok_or_else(|| anyhow!("missing db_path"))?;

    let (mut peer_addr, mut magic, mut blocks, mut force_switch) = (None, None, 1000u64, false);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--peer" => peer_addr = args.next(),
            "--magic" => {
                magic = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--magic N"))?
                        .parse::<u64>()?,
                )
            }
            "--blocks" => blocks = args.next().ok_or_else(|| anyhow!("--blocks N"))?.parse()?,
            "--switch" => force_switch = true,
            other => bail!("unknown arg {other}"),
        }
    }

    let fname = std::path::Path::new(&archive)
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_default();
    let (aslot, ahash): (u64, [u8; 32]) = fname
        .strip_suffix(".tar.zst")
        .and_then(|stem| {
            let (s, h) = stem.split_once('.')?;
            let slot = s.parse().ok()?;
            let hash: [u8; 32] = hex::decode(h).ok()?.try_into().ok()?;
            Some((slot, hash))
        })
        .ok_or_else(|| anyhow!("archive must be named <slot>.<hash>.tar.zst"))?;
    println!("anchor: slot={aslot} hash={}", hex::encode(ahash));

    let mut conn = Connection::open(&db_path)?;
    store::init(&conn)?;

    let t0 = std::time::Instant::now();
    let zr = zstd::stream::read::Decoder::new(File::open(&archive)?)?;
    let mut ar = tar::Archive::new(zr);
    let mut seen_tvar = false;
    let mut stats = None;
    for entry in ar.entries()? {
        let mut e = entry?;
        let path = e.path()?.to_string_lossy().into_owned();
        if path.ends_with("/tables/tvar") {
            seen_tvar = true;
            stats = Some(seed::run(&mut e, &mut conn, (aslot, ahash))?);
            break;
        }
    }
    if !seen_tvar {
        bail!("no tables/tvar found in archive");
    }
    let stats = stats.expect("set when seen_tvar is true");
    println!(
        "seeded entries={} inserted={} skipped={} in {:.1?}",
        stats.entries,
        stats.inserted,
        stats.skipped,
        t0.elapsed()
    );
    println!(
        "tip set after seed: {} unspent UTxOs",
        sync::utxo_count(&conn)?
    );

    if force_switch {
        let t1 = std::time::Instant::now();
        let mut engine = Engine::new(store::tip(&conn)?);
        engine.switch_to_running(&mut conn)?;
        println!(
            "forced switch_to_running (indexes + sweep + mode flip) in {:.1?}",
            t1.elapsed()
        );
        println!("mode is now {:?}", store::get_mode(&conn)?);
    }

    if let Some(addr) = peer_addr {
        let magic = magic.ok_or_else(|| anyhow!("--peer needs --magic"))?;
        println!(
            "\nhanding off to sync: connecting to {addr} (magic {magic}), applying up to {blocks} blocks"
        );
        let point = Point::Specific(aslot, ahash.to_vec());
        let rt = tokio::runtime::Runtime::new()?;
        let (stats, conn) = rt.block_on(sync::run(
            addr,
            magic,
            point,
            conn,
            Some(blocks),
            sync::ModeConfig::default(),
            None,
        ))?;
        println!(
            "applied={} rolled_back={} last_block_slot={} peer_tip_block_no={} mode={:?}",
            stats.applied,
            stats.rolled_back,
            stats.last_block_no,
            stats.peer_tip_block_no,
            store::get_mode(&conn)?,
        );
        println!(
            "tip set after sync: {} unspent UTxOs",
            sync::utxo_count(&conn)?
        );
    }

    Ok(())
}
