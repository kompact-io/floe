//! Manual probe for `floe_n2n::single_fetch::fetch_block`: dial a real peer
//! and blockfetch exactly one known point, with no chain-sync involved.
//! Mirrors the bootstrap path `crates/server/src/ingest/sync.rs`'s
//! `fetch_header_via_blockfetch` takes when there's no cached resume
//! header yet.
//!
//! usage: cargo run -p floe-n2n --example single_fetch_probe -- <host:port> <magic> <slot> <hash-hex>

use std::net::ToSocketAddrs;

use amaru_kernel::{IsHeader, NetworkMagic, Point};
use anyhow::{Result, anyhow};
use floe_n2n::single_fetch;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let mut args = std::env::args().skip(1);
    let addr_str = args
        .next()
        .ok_or_else(|| anyhow!("usage: <host:port> <magic> <slot> <hash-hex>"))?;
    let magic: u64 = args
        .next()
        .ok_or_else(|| anyhow!("missing magic"))?
        .parse()?;
    let slot: u64 = args
        .next()
        .ok_or_else(|| anyhow!("missing slot"))?
        .parse()?;
    let hash_hex = args.next().ok_or_else(|| anyhow!("missing hash"))?;
    let hash_bytes = hex::decode(&hash_hex)?;
    let hash: [u8; 32] = hash_bytes
        .try_into()
        .map_err(|_| anyhow!("hash must be 32 bytes"))?;

    let socket_addr = addr_str
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("could not resolve {addr_str}"))?;
    let point = Point::Specific(slot.into(), hash.into(), 0u64.into());

    println!("dialing {socket_addr} (blockfetch-only, no chain-sync)...");
    let block = single_fetch::fetch_block(socket_addr, NetworkMagic::from(magic), point).await?;
    println!(
        "ok: block_no={} header_hash={} slot={}",
        block.header.block_height().as_u64(),
        hex::encode(block.header.hash()),
        block.header.slot().as_u64(),
    );
    Ok(())
}
