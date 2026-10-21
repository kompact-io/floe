//! Spike: dial a real Cardano n2n peer and pull headers + bodies with real
//! CIP-0164 pipelining, via `floe_n2n::client::Client`.
//!
//! usage: cargo run -p floe-n2n --example dial_peer -- <host:port> <magic> --point <slot>/<hash-hex> [--count N]

use std::net::ToSocketAddrs;

use amaru_kernel::cardano::header::Header;
use amaru_kernel::{IsHeader, NetworkMagic};
use anyhow::{Result, anyhow};
use floe_n2n::client::Client;
use floe_n2n::pipeline::HeaderEvent;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let mut args = std::env::args().skip(1);
    let addr_str = args
        .next()
        .ok_or_else(|| anyhow!("usage: <host:port> <magic> --point slot/hash [--count N]"))?;
    let magic: u64 = args
        .next()
        .ok_or_else(|| anyhow!("missing magic"))?
        .parse()?;
    let mut point: Option<(u64, Vec<u8>)> = None;
    let mut count = 200u64;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--point" => {
                let v = args
                    .next()
                    .ok_or_else(|| anyhow!("--point needs slot/hash"))?;
                let (s, h) = v
                    .split_once('/')
                    .ok_or_else(|| anyhow!("--point format: slot/hash"))?;
                point = Some((s.parse()?, hex::decode(h)?));
            }
            "--count" => {
                count = args
                    .next()
                    .ok_or_else(|| anyhow!("--count needs N"))?
                    .parse()?
            }
            other => return Err(anyhow!("unknown arg {other}")),
        }
    }
    let (seed_slot, seed_hash) = point.ok_or_else(|| {
        anyhow!("--point slot/hash is required (no Origin bootstrap for the seed fetch)")
    })?;

    let socket_addr = addr_str
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("could not resolve {addr_str}"))?;

    println!(
        "fetching seed header at {seed_slot}/{} via pallas-network",
        hex::encode(&seed_hash)
    );
    let seed_header = fetch_seed_header(&addr_str, magic, seed_slot, seed_hash).await?;
    println!(
        "seed header ok: block_no={}",
        seed_header.block_height().as_u64()
    );

    println!("dialing {socket_addr} via floe-n2n...");
    let mut client = Client::connect_with_retry(
        socket_addr,
        NetworkMagic::from(magic),
        Some(&seed_header),
        std::time::Duration::from_secs(2),
        5,
    )
    .await?;
    println!("connected and intersected");

    let mut headers_seen = 0u64;
    let mut first_point = None;
    #[allow(unused_assignments)]
    let mut last_point = None;
    loop {
        match client.next_header().await? {
            HeaderEvent::RollForward {
                header,
                tip_block_no,
            } => {
                headers_seen += 1;
                let point = floe_n2n::resume::point_of(&header);
                first_point.get_or_insert(point);
                last_point = Some(point);
                println!("roll forward total={headers_seen} tip_block_no={tip_block_no}");
                if headers_seen >= count {
                    break;
                }
            }
            HeaderEvent::RollBackward {
                point,
                tip_block_no,
            } => {
                println!("roll backward point={point:?} tip_block_no={tip_block_no}");
            }
            HeaderEvent::IntersectNotFound { .. } => {
                unreachable!("connect_with_retry already handled this")
            }
        }
    }

    if let (Some(from), Some(through)) = (first_point, last_point) {
        println!("fetching bodies for {from:?}..={through:?}");
        let blocks = client.fetch_bodies(from, through).await?;
        println!("got {} block(s)", blocks.len());
    }

    Ok(())
}

async fn fetch_seed_header(addr: &str, magic: u64, slot: u64, hash: Vec<u8>) -> Result<Header> {
    use amaru_kernel::RawBlock;
    use amaru_kernel::cardano::network_block::NetworkBlock;
    use pallas_network::facades::PeerClient;
    use pallas_network::miniprotocols::Point as PallasPoint;

    let mut client = PeerClient::connect(addr, magic).await?;
    let point = PallasPoint::Specific(slot, hash);
    let bytes = client.blockfetch().fetch_single(point).await?;
    let network_block = NetworkBlock::try_from(RawBlock::from(bytes.as_slice()))?;
    Ok(network_block.decode_block()?.header)
}
