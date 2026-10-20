//! fixture_scan: walk blocks from a chainsync intersect point looking for blocks
//! that exercise each case decode.rs needs fixture coverage for, and dump the
//! raw block CBOR (hex) for the first match of each to tests/data/.
//!
//! usage: cargo run --release --example fixture_scan -- <host:port> <magic> \
//!     --point <slot>/<hash-hex> [--blocks N]

use std::collections::HashSet;
use std::fs;

use amaru_kernel::cardano::era_name::EraName;
use amaru_kernel::cardano::header::Header;
use amaru_kernel::cardano::memoized::MemoizedDatum;
use amaru_kernel::cardano::network_block::NetworkBlock;
use amaru_kernel::{IsHeader, RawBlock};
use anyhow::{Result, anyhow};
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::{Point, chainsync::NextResponse};

#[derive(Default)]
struct Found {
    invalid_tx: bool,
    same_block_chain: bool,
    inline_datum: bool,
    hash_datum: bool,
    witness_datum: bool,
    ref_script: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args
        .next()
        .ok_or_else(|| anyhow!("usage: <host:port> <magic> --point slot/hash [--blocks N]"))?;
    let magic: u64 = args
        .next()
        .ok_or_else(|| anyhow!("missing magic"))?
        .parse()?;

    let mut point = Point::Origin;
    let mut blocks = 5000usize;
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

    fs::create_dir_all("tests/data")?;
    let mut found = Found::default();
    let mut n = 0usize;
    let mut retries = 0u32;

    'reconnect: loop {
        println!("connecting to {addr} (magic {magic}), resuming at {point:?}");
        let mut peer = match PeerClient::connect(&addr, magic).await {
            Ok(p) => p,
            Err(e) if retries < 10 => {
                retries += 1;
                eprintln!("connect failed ({e}), retrying ({retries}/10)");
                continue 'reconnect;
            }
            Err(e) => return Err(e.into()),
        };
        let (isect, tip) = match peer.chainsync().find_intersect(vec![point.clone()]).await {
            Ok(v) => v,
            Err(e) if retries < 10 => {
                retries += 1;
                eprintln!("intersect failed ({e}), retrying ({retries}/10)");
                continue 'reconnect;
            }
            Err(e) => return Err(e.into()),
        };
        println!("intersect: {isect:?} peer tip block_no={}", tip.1);

        while n < blocks {
            let next = peer.chainsync().request_or_await_next().await;
            let next = match next {
                Ok(v) => v,
                Err(e) if retries < 10 => {
                    retries += 1;
                    eprintln!("chainsync error at block {n} ({e}), reconnecting ({retries}/10)");
                    continue 'reconnect;
                }
                Err(e) => return Err(e.into()),
            };
            match next {
                NextResponse::RollBackward(p, _) => {
                    println!("rollback to {p:?} (expected once, to the intersect)")
                }
                NextResponse::Await => {
                    println!("reached tip at block {n}");
                    break 'reconnect;
                }
                NextResponse::RollForward(h, _) => {
                    if EraName::from_header_variant(h.variant)? == EraName::Byron {
                        return Err(anyhow!("Byron-era headers are not supported"));
                    }
                    let hdr: Header = amaru_kernel::cbor::decode(&h.cbor)?;
                    let slot = hdr.slot().as_u64();
                    let hash: [u8; 32] = *hdr.hash();
                    let pt = Point::Specific(slot, hash.to_vec());
                    let bytes = match peer.blockfetch().fetch_single(pt.clone()).await {
                        Ok(v) => v,
                        Err(e) if retries < 10 => {
                            retries += 1;
                            eprintln!(
                                "blockfetch error at block {n} ({e}), reconnecting ({retries}/10)"
                            );
                            continue 'reconnect;
                        }
                        Err(e) => return Err(e.into()),
                    };
                    let network_block = NetworkBlock::try_from(RawBlock::from(bytes.as_slice()))?;
                    let blk = network_block.decode_block()?;
                    point = pt;

                    let mut created_here: HashSet<([u8; 32], u64)> = HashSet::new();
                    let mut spent_here: Vec<([u8; 32], u64)> = Vec::new();
                    let mut has_invalid = false;
                    let mut has_inline = false;
                    let mut has_hash_datum = false;
                    let mut has_witness_datum = false;
                    let mut has_ref_script = false;

                    for (_ix, tx) in &blk {
                        let tx_hash: [u8; 32] = *tx.body.id();
                        if !tx.is_expected_valid {
                            has_invalid = has_invalid || tx.body.collateral_return.is_some();
                        } else if tx.witnesses.plutus_data.is_some() {
                            has_witness_datum = true;
                        }
                        let consumed = if tx.is_expected_valid {
                            tx.body.inputs.iter().collect::<Vec<_>>()
                        } else {
                            tx.body
                                .collateral
                                .as_ref()
                                .map(|c| c.iter().collect())
                                .unwrap_or_default()
                        };
                        for i in consumed {
                            spent_here.push((*i.transaction_id, i.index as u64));
                        }
                        let produced: Vec<(u64, &amaru_kernel::cardano::memoized::MemoizedTransactionOutput)> =
                            if tx.is_expected_valid {
                                tx.body.outputs.iter().enumerate().map(|(i, o)| (i as u64, o)).collect()
                            } else {
                                tx.body
                                    .collateral_return
                                    .as_deref()
                                    .map(|o| vec![(tx.body.outputs.len() as u64, o)])
                                    .unwrap_or_default()
                            };
                        for (ix, o) in produced {
                            created_here.insert((tx_hash, ix));
                            match &o.datum {
                                MemoizedDatum::Inline(_) => has_inline = true,
                                MemoizedDatum::Hash(_) => has_hash_datum = true,
                                MemoizedDatum::None => {}
                            }
                            if o.script.is_some() {
                                has_ref_script = true;
                            }
                        }
                    }
                    let has_same_block_chain = spent_here.iter().any(|k| created_here.contains(k));

                    let save = |name: &str, flag: &mut bool, cond: bool| -> Result<()> {
                        if cond && !*flag {
                            *flag = true;
                            let path = format!("tests/data/fixture_{name}.block.hex");
                            fs::write(&path, hex::encode(&bytes))?;
                            println!(
                                "block {n} slot={} block_no={} -> {path} ({} txs)",
                                slot,
                                blk.header.body().block_number,
                                blk.transaction_bodies.len()
                            );
                        }
                        Ok(())
                    };
                    save("invalid_tx", &mut found.invalid_tx, has_invalid)?;
                    save(
                        "same_block_chain",
                        &mut found.same_block_chain,
                        has_same_block_chain,
                    )?;
                    save("inline_datum", &mut found.inline_datum, has_inline)?;
                    save("hash_datum", &mut found.hash_datum, has_hash_datum)?;
                    save("witness_datum", &mut found.witness_datum, has_witness_datum)?;
                    save("ref_script", &mut found.ref_script, has_ref_script)?;

                    n += 1;
                    if n.is_multiple_of(500) {
                        eprintln!("... scanned {n} blocks");
                    }
                    if found.invalid_tx
                        && found.same_block_chain
                        && found.inline_datum
                        && found.hash_datum
                        && found.witness_datum
                        && found.ref_script
                    {
                        println!("all cases found after {n} blocks");
                        break 'reconnect;
                    }
                }
            }
        }
        break 'reconnect;
    }

    println!(
        "done: invalid_tx={} same_block_chain={} inline_datum={} hash_datum={} witness_datum={} ref_script={}",
        found.invalid_tx,
        found.same_block_chain,
        found.inline_datum,
        found.hash_datum,
        found.witness_datum,
        found.ref_script
    );

    Ok(())
}
