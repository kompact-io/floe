//! Confidence test: drives the real `floe-n2n` client (`orchestrator` +
//! `pipeline`, via `client::Client`) against an in-process mock peer
//! (pallas-network's server-side chainsync + blockfetch, already a
//! well-understood floe dependency) serving a synthetic, deterministic
//! chain. No real network or remote host required -- this is what CI can
//! actually run, unlike the `dial_peer` example which needs a live Cardano
//! peer.
//!
//! This exercises the exact thing the spike set out to prove: real CIP-0164
//! pipelining (multiple outstanding `RequestNext`s) against
//! `amaru-protocols`/`amaru-pure-stage`, via `floe_n2n::orchestrator`'s
//! from-scratch initiator-only connection setup -- for both headers
//! (chain-sync) and bodies (block-fetch).

use std::net::SocketAddr;
use std::time::Duration;

use amaru_kernel::cardano::header::make_header;
use amaru_kernel::cardano::network_block::make_encoded_chain;
use amaru_kernel::{IsHeader, NetworkMagic, PREPROD_ERA_HISTORY};
use floe_n2n::client::Client;
use floe_n2n::pipeline::HeaderEvent;
use pallas_network::facades::PeerServer;
use pallas_network::miniprotocols::Point as PallasPoint;
use pallas_network::miniprotocols::chainsync::{ClientRequest, HeaderContent, Tip as PallasTip};
use tokio::net::TcpListener;

// Comfortably more than HEADER_LIMIT + PIPELINE_DEPTH (amaru's chainsync
// initiator keeps that many requests outstanding, which can land past
// HEADER_LIMIT before this stage's Done takes effect) -- otherwise the mock
// peer runs out of chain and hits its `send_await_reply` path, which this
// mock's simple loop doesn't implement.
const CHAIN_LEN: usize = 60;
const HEADER_LIMIT: u64 = 20;
const MAGIC: u64 = 2;

/// Mutually-consistent (header, body) pairs, chained by `prev_hash`, built via
/// amaru-kernel's own test helper rather than hand-rolled headers -- so the
/// block bodies served over blockfetch actually decode as the `NetworkBlock`
/// the real wire format carries, not an ad hoc blob.
fn build_chain() -> Vec<amaru_kernel::cardano::network_block::EncodedTestBlock> {
    let seeds = (0..CHAIN_LEN as u64).map(|i| make_header(1_000_000 + i, 2_000_000 + i, None));
    make_encoded_chain(seeds, &PREPROD_ERA_HISTORY)
}

fn pallas_point_of(h: &amaru_kernel::Header) -> PallasPoint {
    PallasPoint::Specific(h.slot().as_u64(), h.hash().to_vec())
}

async fn run_mock_peer(
    listener: TcpListener,
    chain: Vec<amaru_kernel::cardano::network_block::EncodedTestBlock>,
) -> anyhow::Result<()> {
    let server = PeerServer::accept(&listener, MAGIC).await?;
    let PeerServer {
        mut chainsync,
        mut blockfetch,
        ..
    } = server;

    let root_point = pallas_point_of(&chain[0].header);
    let tip = PallasTip(
        pallas_point_of(&chain.last().expect("non-empty chain").header),
        chain.len() as u64,
    );

    let chainsync_chain = chain.clone();
    let chainsync_task = tokio::spawn(async move {
        let mut rolled_back = false;
        let mut cursor = 1usize;
        loop {
            match chainsync.recv_while_idle().await? {
                None => break,
                Some(ClientRequest::Intersect(points)) => {
                    if points.contains(&root_point) {
                        chainsync
                            .send_intersect_found(root_point.clone(), tip.clone())
                            .await?;
                    } else {
                        chainsync.send_intersect_not_found(tip.clone()).await?;
                    }
                }
                Some(ClientRequest::RequestNext) => {
                    if !rolled_back {
                        chainsync
                            .send_roll_backward(root_point.clone(), tip.clone())
                            .await?;
                        rolled_back = true;
                    } else if cursor < chainsync_chain.len() {
                        let variant =
                            amaru_kernel::cardano::era_name::EraName::Conway.header_variant();
                        let content = HeaderContent {
                            variant,
                            byron_prefix: None,
                            cbor: amaru_kernel::cardano::raw_block::extract_block_header_cbor(
                                &chainsync_chain[cursor].raw,
                            )?
                            .to_vec(),
                        };
                        chainsync.send_roll_forward(content, tip.clone()).await?;
                        cursor += 1;
                    } else {
                        chainsync.send_await_reply().await?;
                    }
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let blockfetch_block = chain[0].raw.clone();
    let blockfetch_task = tokio::spawn(async move {
        loop {
            match blockfetch.recv_while_idle().await? {
                None => break,
                Some(_request) => {
                    blockfetch.send_start_batch().await?;
                    blockfetch.send_block(blockfetch_block.to_vec()).await?;
                    blockfetch.send_batch_done().await?;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    });

    let (cs, bf) = tokio::join!(chainsync_task, blockfetch_task);
    cs??;
    bf??;
    Ok(())
}

#[tokio::test]
async fn pipelined_chainsync_and_blockfetch_pull_from_mock_peer() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let chain = build_chain();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr: SocketAddr = listener.local_addr()?;

    let mock_chain = chain.clone();
    tokio::spawn(async move {
        if let Err(e) = run_mock_peer(listener, mock_chain).await {
            eprintln!("mock peer error: {e:#}");
        }
    });

    let root = &chain[0].header;

    let mut client = tokio::time::timeout(
        Duration::from_secs(10),
        Client::connect_with_retry(
            addr,
            NetworkMagic::from(MAGIC),
            Some(root),
            Duration::from_millis(200),
            3,
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out connecting"))??;

    let mut headers_seen = 0u64;
    let mut rollbacks_seen = 0u64;
    let mut first_point = None;
    let mut last_point = None;
    loop {
        match tokio::time::timeout(Duration::from_secs(10), client.next_header())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting for a header"))??
        {
            HeaderEvent::RollForward { header, .. } => {
                headers_seen += 1;
                let point = floe_n2n::resume::point_of(&header);
                first_point.get_or_insert(point);
                last_point = Some(point);
                if headers_seen >= HEADER_LIMIT {
                    break;
                }
            }
            HeaderEvent::RollBackward { .. } => rollbacks_seen += 1,
            HeaderEvent::IntersectNotFound { .. } => {
                panic!("connect_with_retry already handled this")
            }
        }
    }

    assert_eq!(
        headers_seen, HEADER_LIMIT,
        "should have pulled exactly header_limit headers"
    );
    // The mock peer always sends exactly one RollBackward (to the intersect
    // point) before any RollForward, matching real chain-sync semantics.
    assert_eq!(rollbacks_seen, 1);

    let (from, through) = (
        first_point.expect("saw headers"),
        last_point.expect("saw headers"),
    );
    let blocks = tokio::time::timeout(Duration::from_secs(10), client.fetch_bodies(from, through))
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for a blockfetch result"))??;
    assert!(!blocks.is_empty(), "expected at least one block body");

    Ok(())
}
