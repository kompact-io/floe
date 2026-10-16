//! peer_tip: ask a peer's chainsync tip directly — no floe state
//! involved. `FindIntersect` always returns the peer's current tip as
//! part of its reply, so one handshake + one round trip is enough.
//!
//! Useful for checking whether the peer itself is behind the real
//! network, independent of (and before blaming) floe's own indexed tip.
//!
//! usage: cargo run --release --example peer_tip -- --peer <host:port> --magic <n>

use anyhow::Result;
use clap::Parser;
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::Point;

#[derive(Parser)]
struct Cli {
    /// Peer address, host:port.
    #[arg(long)]
    peer: String,
    /// Network magic (mainnet=764824073, preprod=1, preview=2).
    #[arg(long)]
    magic: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let mut peer = PeerClient::connect(&args.peer, args.magic).await?;
    let (_, tip) = peer.chainsync().find_intersect(vec![Point::Origin]).await?;

    match tip.0 {
        Point::Origin => println!("peer tip: origin, block_no={}", tip.1),
        Point::Specific(slot, hash) => {
            println!(
                "peer tip: slot={slot} hash={} block_no={}",
                hex::encode(hash),
                tip.1
            );
        }
    }
    Ok(())
}
