//! sync_run: drive sync::run against a live peer in running mode, applying
//! into a SQLite file, then report the resulting tip UTxO count.
//!
//! Note: without the seed (PLAN.md Milestone 6/"Seed" — not wired in yet),
//! starting from the anchor means this count is NOT the real total live
//! UTxO set; it only reflects outputs created after the intersect point
//! that remain unspent. It still exercises the real sync+decode+store path
//! end to end against a live peer.
//!
//! usage: cargo run --release --example sync_run -- <host:port> <magic> <db_path> \
//!     --point <slot>/<hash-hex> [--blocks N]

use anyhow::{Result, anyhow};
use floe::ingest::store;
use floe::ingest::sync;
use pallas_network::miniprotocols::Point;
use rusqlite::Connection;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().ok_or_else(|| {
        anyhow!("usage: <host:port> <magic> <db_path> --point slot/hash [--blocks N]")
    })?;
    let magic: u64 = args
        .next()
        .ok_or_else(|| anyhow!("missing magic"))?
        .parse()?;
    let db_path = args.next().ok_or_else(|| anyhow!("missing db_path"))?;

    let mut point = Point::Origin;
    let mut blocks = 1000u64;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--point" => {
                let v = args
                    .next()
                    .ok_or_else(|| anyhow!("--point needs slot/hash"))?;
                let (s, h) = v
                    .split_once('/')
                    .ok_or_else(|| anyhow!("--point format: slot/hash"))?;
                point = Point::Specific(s.parse()?, hex::decode(h)?);
            }
            "--blocks" => blocks = args.next().ok_or_else(|| anyhow!("--blocks N"))?.parse()?,
            other => return Err(anyhow!("unknown arg {other}")),
        }
    }

    let conn = Connection::open(&db_path)?;
    store::init(&conn)?;

    println!("connecting to {addr} (magic {magic}), applying up to {blocks} blocks into {db_path}");
    let (stats, conn) = sync::run(
        addr,
        magic,
        point,
        conn,
        Some(blocks),
        sync::ModeConfig::default(),
        None,
    )
    .await?;

    println!(
        "applied={} rolled_back={} last_block_slot={} peer_tip_block_no={} mode={:?}",
        stats.applied,
        stats.rolled_back,
        stats.last_block_no,
        stats.peer_tip_block_no,
        floe::ingest::store::get_mode(&conn)?,
    );
    println!(
        "utxo count at tip (post-intersect only, no seed): {}",
        sync::utxo_count(&conn)?
    );

    Ok(())
}
