//! chainsync_probe: connect to a Cardano N2N peer (e.g. amaru), find an intersect,
//! print the first N headers, and fetch the first block body.
//!
//! usage: cargo run --release --example chainsync_probe -- <host:port> <magic> [--point <slot>/<hash-hex>] [--count N]
//!   default intersect: Origin.  magic: mainnet=764824073 preprod=1 preview=2

use amaru_kernel::cardano::era_name::EraName;
use amaru_kernel::cardano::header::Header;
use amaru_kernel::cardano::network_block::NetworkBlock;
use amaru_kernel::{IsHeader, RawBlock};
use anyhow::{Result, anyhow};
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::{Point, chainsync::NextResponse};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args
        .next()
        .ok_or_else(|| anyhow!("usage: <host:port> <magic> [--point slot/hash] [--count N]"))?;
    let magic: u64 = args
        .next()
        .ok_or_else(|| anyhow!("missing magic"))?
        .parse()?;

    let mut point = Point::Origin;
    let mut count = 10usize;
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
            "--count" => {
                count = args
                    .next()
                    .ok_or_else(|| anyhow!("--count needs N"))?
                    .parse()?
            }
            other => return Err(anyhow!("unknown arg {other}")),
        }
    }

    println!("connecting to {addr} (magic {magic})");
    let mut peer = PeerClient::connect(&addr, magic).await?;
    println!("handshake ok");

    // intersect
    let (found, tip) = peer.chainsync().find_intersect(vec![point.clone()]).await?;
    println!("requested intersect: {point:?}");
    println!("intersect found:     {found:?}");
    println!("peer tip:            {:?} block_no={}", tip.0, tip.1);
    if found.is_none() {
        println!("=> peer has no intersection with that point (chain does not reach it)");
        return Ok(());
    }

    // headers
    let mut first: Option<Point> = None;
    let mut seen = 0usize;
    while seen < count {
        match peer.chainsync().request_or_await_next().await? {
            NextResponse::RollForward(h, tip) => {
                if EraName::from_header_variant(h.variant)? == EraName::Byron {
                    return Err(anyhow!("Byron-era headers are not supported"));
                }
                let hdr: Header = amaru_kernel::cbor::decode(&h.cbor)?;
                let slot = hdr.slot().as_u64();
                let hash: [u8; 32] = *hdr.hash();
                println!(
                    "fwd  slot={:<10} block_no={:<8} hash={}  (tip block_no={})",
                    slot,
                    hdr.block_height().as_u64(),
                    hex::encode(hash),
                    tip.1
                );
                if first.is_none() {
                    first = Some(Point::Specific(slot, hash.to_vec()));
                }
                seen += 1;
            }
            NextResponse::RollBackward(p, tip) => {
                println!("back to {p:?} (tip block_no={})", tip.1)
            }
            NextResponse::Await => {
                println!("await (at tip)");
                break;
            }
        }
    }

    // body of the first header
    if let Some(p) = first {
        println!("blockfetch {p:?}");
        match peer.blockfetch().fetch_single(p).await {
            Ok(bytes) => {
                let network_block = NetworkBlock::try_from(RawBlock::from(bytes.as_slice()))?;
                let blk = network_block.decode_block()?;
                println!(
                    "body ok: {} bytes, era={:?}, txs={}",
                    bytes.len(),
                    network_block.era_tag(),
                    blk.transaction_bodies.len()
                );
            }
            Err(e) => println!("blockfetch failed: {e}"),
        }
    }
    Ok(())
}
