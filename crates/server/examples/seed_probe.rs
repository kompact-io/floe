//! seed_probe: stream the UTxO set out of an amaru bootstrap snapshot archive,
//! decode it, print stats, and (optionally) verify it against the chain:
//!  1. every input spent by the first N blocks after the anchor resolves in the seed;
//!  2. for spent inputs with a key-hash payment credential, the credential in the
//!     DECODED address equals the hash of one of the spending tx's vkey witnesses.
//!     (2) validates address reconstruction, which lookups by key cannot.
//!
//! usage:
//!   cargo run --release --example seed_probe -- <snapshot.tar.zst> \
//!       [--peer host:port --magic N [--blocks N]]
//!
//! The archive is `snapshots/<net>/<slot>.<hash>.tar.zst`; its state includes the
//! anchor block `<slot>.<hash>`, so applying must start at the NEXT block.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Read;
use std::rc::Rc;

use amaru_kernel::cardano::era_name::EraName;
use amaru_kernel::cardano::header::Header;
use amaru_kernel::cardano::network_block::NetworkBlock;
use amaru_kernel::{Hasher, IsHeader, RawBlock};
use anyhow::{Result, anyhow, bail};
use floe::ingest::mempack::{Datum, ScriptRef, TxOut, decode_tx_out};
use floe::ingest::tvar::{self, KEY_LEN};
use pallas_network::facades::PeerClient;
use pallas_network::miniprotocols::{Point, chainsync::NextResponse};

#[derive(Default)]
struct Stats {
    entries: u64,
    errors: u64,
    unsorted: u64,
    by_tag: [u64; 6],
    by_addr_type: [u64; 16],
    multiasset: u64,
    datum_hash: u64,
    datum_inline: u64,
    script_ref: u64,
    total_coin: u128,
    max_value_len: usize,
    sum_value_len: u64,
}

fn key_of(txid: &[u8], ix: u64) -> [u8; KEY_LEN] {
    let mut k = [0u8; KEY_LEN];
    k[..32].copy_from_slice(txid);
    k[32..].copy_from_slice(&(ix as u16).to_be_bytes());
    k
}

fn describe(txid: &[u8], ix: u16, o: &TxOut) -> String {
    let datum = match &o.datum {
        Datum::None => "none".to_string(),
        Datum::Hash(h) => format!("hash:{}", hex::encode(h)),
        Datum::Inline(b) => format!("inline:{}B", b.len()),
    };
    let script = match &o.script {
        None => "-".to_string(),
        Some(ScriptRef::Native(b)) => format!("native:{}B", b.len()),
        Some(ScriptRef::PlutusV1(b)) => format!("v1:{}B", b.len()),
        Some(ScriptRef::PlutusV2(b)) => format!("v2:{}B", b.len()),
        Some(ScriptRef::PlutusV3(b)) => format!("v3:{}B", b.len()),
    };
    format!(
        "{}#{} tag={} addr={} coin={} assets={} datum={} script={}",
        hex::encode(txid),
        ix,
        o.tag,
        hex::encode(&o.address),
        o.coin,
        o.assets.len(),
        datum,
        script
    )
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let archive = args.next().ok_or_else(|| {
        anyhow!("usage: <snapshot.tar.zst> [--peer host:port --magic N [--blocks N]]")
    })?;
    let (mut peer, mut magic, mut blocks) = (None, None, 200usize);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--peer" => peer = args.next(),
            "--magic" => {
                magic = Some(
                    args.next()
                        .ok_or_else(|| anyhow!("--magic N"))?
                        .parse::<u64>()?,
                )
            }
            "--blocks" => blocks = args.next().ok_or_else(|| anyhow!("--blocks N"))?.parse()?,
            o => bail!("unknown arg {o}"),
        }
    }

    // ---- anchor: from the file name `<slot>.<hash>.tar.zst`
    let fname = std::path::Path::new(&archive)
        .file_name()
        .map(|f| f.to_string_lossy().into_owned())
        .unwrap_or_default();
    let anchor: Option<(u64, Vec<u8>)> = fname.strip_suffix(".tar.zst").and_then(|stem| {
        let (s, h) = stem.split_once('.')?;
        Some((s.parse().ok()?, hex::decode(h).ok()?))
    });

    // ---- optional: walk the chain after the anchor first (cheap), remember what it spends
    let mut needs: HashMap<[u8; KEY_LEN], Rc<Vec<[u8; 28]>>> = HashMap::new();
    if let Some(addr) = &peer {
        let magic = magic.ok_or_else(|| anyhow!("--peer needs --magic"))?;
        let (aslot, ahash) = anchor
            .clone()
            .ok_or_else(|| anyhow!("--peer needs an archive named <slot>.<hash>.tar.zst"))?;
        let rt = tokio::runtime::Runtime::new()?;
        needs = rt.block_on(collect(addr, magic, aslot, ahash, blocks))?;
    }
    let n_needed = needs.len();

    // ---- seed
    let t0 = std::time::Instant::now();
    let zr = zstd::stream::read::Decoder::new(File::open(&archive)?)?;
    let mut ar = tar::Archive::new(zr);

    let mut st = Stats::default();
    let mut seen_tvar = false;
    // address-check: [compact-address tags 0,1,4,5 | packed tags 2,3] -> (checked, matched)
    let mut wit = [(0u64, 0u64); 2];
    let mut wit_bad: Vec<String> = Vec::new();

    for entry in ar.entries()? {
        let mut e = entry?;
        let path = e.path()?.to_string_lossy().into_owned();
        if path.ends_with("/meta") {
            let mut m = String::new();
            e.read_to_string(&mut m)?;
            println!("meta: {m}");
        } else if path.ends_with("/tables/tvar") {
            seen_tvar = true;
            let mut prev = [0u8; KEY_LEN];
            let mut shown = 0;
            let mut shown_kinds = [false; 4]; // inline datum, script ref, datum hash, addr28 form
            tvar::for_each_entry(&mut e, |key, val| {
                st.entries += 1;
                st.max_value_len = st.max_value_len.max(val.len());
                st.sum_value_len += val.len() as u64;
                if st.entries > 1 && *key <= prev {
                    st.unsorted += 1;
                }
                prev = *key;
                let (txid, ix) = tvar::split_key(key);
                match decode_tx_out(val) {
                    Ok(o) => {
                        st.by_tag[o.tag as usize] += 1;
                        st.by_addr_type[(o.address.first().copied().unwrap_or(0) >> 4) as usize] +=
                            1;
                        st.total_coin += u128::from(o.coin);
                        if !o.assets.is_empty() {
                            st.multiasset += 1;
                        }
                        let mut interesting = false;
                        match &o.datum {
                            Datum::Hash(_) => {
                                st.datum_hash += 1;
                                interesting |= !std::mem::replace(&mut shown_kinds[2], true);
                            }
                            Datum::Inline(_) => {
                                st.datum_inline += 1;
                                interesting |= !std::mem::replace(&mut shown_kinds[0], true);
                            }
                            Datum::None => {}
                        }
                        if o.script.is_some() {
                            st.script_ref += 1;
                            interesting |= !std::mem::replace(&mut shown_kinds[1], true);
                        }
                        if o.tag == 2 || o.tag == 3 {
                            interesting |= !std::mem::replace(&mut shown_kinds[3], true);
                        }
                        if shown < 3 || interesting {
                            println!("  sample {}", describe(txid, ix, &o));
                            shown += 1;
                        }
                        // resolve what the walked blocks spend
                        if let Some(hashes) = needs.remove(key) {
                            let hdr = o.address.first().copied().unwrap_or(0) >> 4;
                            let key_payment = matches!(hdr, 0 | 2 | 4 | 6) && o.address.len() >= 29;
                            if key_payment {
                                let g = usize::from(o.tag == 2 || o.tag == 3);
                                wit[g].0 += 1;
                                let hash: [u8; 28] = o.address[1..29].try_into().unwrap();
                                if hashes.contains(&hash) {
                                    wit[g].1 += 1;
                                } else if wit_bad.len() < 5 {
                                    wit_bad.push(describe(txid, ix, &o));
                                }
                            }
                        }
                    }
                    Err(err) => {
                        st.errors += 1;
                        if st.errors <= 5 {
                            println!(
                                "  DECODE ERROR {}#{}: {err}  value={}",
                                hex::encode(txid),
                                ix,
                                hex::encode(val)
                            );
                        }
                    }
                }
                if st.entries % 1_000_000 == 0 {
                    eprintln!("  ... {} entries ({:.0?})", st.entries, t0.elapsed());
                }
                Ok(true)
            })?;
            break; // nothing after tvar is needed
        }
    }
    if !seen_tvar {
        bail!("no tables/tvar found in archive");
    }

    if let Some((aslot, ahash)) = &anchor {
        println!(
            "\nanchor (state includes this block): slot={aslot} hash={}",
            hex::encode(ahash)
        );
    }
    println!(
        "entries={} decode_errors={} keys_not_ascending={}",
        st.entries, st.errors, st.unsorted
    );
    println!("by mempack tag [0..5]: {:?}", st.by_tag);
    println!(
        "by address type (hi nibble): base0-3={:?} ptr4-5={:?} ent6-7={:?} byron8={} other={}",
        &st.by_addr_type[0..4],
        &st.by_addr_type[4..6],
        &st.by_addr_type[6..8],
        st.by_addr_type[8],
        st.by_addr_type[9..].iter().sum::<u64>()
    );
    println!(
        "multiasset={} datum_hash={} datum_inline={} script_ref={}",
        st.multiasset, st.datum_hash, st.datum_inline, st.script_ref
    );
    println!(
        "total coin={} lovelace  avg value bytes={}  max={}",
        st.total_coin,
        st.sum_value_len / st.entries.max(1),
        st.max_value_len
    );
    println!("seed time: {:.1?}", t0.elapsed());

    if peer.is_some() {
        println!("\n== verification against the chain after the anchor ==");
        println!(
            "inputs needed from the seed: {n_needed}   NOT FOUND in seed: {}",
            needs.len()
        );
        for (k, _) in needs.iter().take(5) {
            let (t, i) = tvar::split_key(k);
            println!("  MISSING {}#{}", hex::encode(t), i);
        }
        println!(
            "key-credential vs vkey-witness match:  compact-address outputs {}/{}   packed (tag 2/3) outputs {}/{}",
            wit[0].1, wit[0].0, wit[1].1, wit[1].0
        );
        for b in &wit_bad {
            println!("  NO WITNESS MATCH {b}");
        }
        if needs.is_empty() && wit[0].0 == wit[0].1 && wit[1].0 == wit[1].1 {
            println!("OK");
        }
    }
    Ok(())
}

/// Walk `blocks` blocks after the anchor. Returns, for every spent input that was NOT
/// created inside the walk, the vkey-witness hashes of the spending tx.
async fn collect(
    addr: &str,
    magic: u64,
    slot: u64,
    hash: Vec<u8>,
    blocks: usize,
) -> Result<HashMap<[u8; KEY_LEN], Rc<Vec<[u8; 28]>>>> {
    println!("connecting to {addr} (magic {magic})");
    let mut peer = PeerClient::connect(addr, magic).await?;
    let (found, tip) = peer
        .chainsync()
        .find_intersect(vec![Point::Specific(slot, hash)])
        .await?;
    println!("intersect: {found:?}  peer tip block_no={}", tip.1);

    let mut created: HashSet<[u8; KEY_LEN]> = HashSet::new();
    let mut needs: HashMap<[u8; KEY_LEN], Rc<Vec<[u8; 28]>>> = HashMap::new();
    let (mut nblocks, mut ntx, mut ninvalid, mut ninputs) = (0usize, 0u64, 0u64, 0u64);

    while nblocks < blocks {
        match peer.chainsync().request_or_await_next().await? {
            NextResponse::RollBackward(p, _) => {
                println!("rollback to {p:?} (expected once, to the anchor)")
            }
            NextResponse::Await => {
                println!("reached tip");
                break;
            }
            NextResponse::RollForward(h, _) => {
                if EraName::from_header_variant(h.variant)? == EraName::Byron {
                    bail!("Byron-era headers are not supported");
                }
                let hdr: Header = amaru_kernel::cbor::decode(&h.cbor)?;
                let slot = hdr.slot().as_u64();
                let hash: [u8; 32] = *hdr.hash();
                let pt = Point::Specific(slot, hash.to_vec());
                let bytes = peer.blockfetch().fetch_single(pt).await?;
                let network_block = NetworkBlock::try_from(RawBlock::from(bytes.as_slice()))?;
                let blk = network_block.decode_block()?;
                if nblocks == 0 {
                    println!(
                        "first block after anchor: slot={} block_no={} (apply starts here)",
                        slot,
                        blk.header.body().block_number
                    );
                }
                for (_ix, tx) in &blk {
                    ntx += 1;
                    let valid = tx.is_expected_valid;
                    if !valid {
                        ninvalid += 1;
                    }
                    let tx_hash: [u8; 32] = *tx.body.id();
                    let wits: Rc<Vec<[u8; 28]>> = Rc::new(
                        tx.witnesses
                            .verification_key_witness
                            .iter()
                            .flat_map(|ws| ws.iter())
                            .map(|w| {
                                let h: [u8; 28] =
                                    *Hasher::<224>::hash(w.verification_key.as_slice());
                                h
                            })
                            .collect(),
                    );
                    let spent: Vec<&amaru_kernel::cardano::transaction_input::TransactionInput> =
                        if valid {
                            tx.body.inputs.iter().collect()
                        } else {
                            tx.body
                                .collateral
                                .as_ref()
                                .map(|c| c.iter().collect())
                                .unwrap_or_default()
                        };
                    for i in spent {
                        ninputs += 1;
                        let k = key_of(i.transaction_id.as_ref(), i.index as u64);
                        if !created.remove(&k) {
                            needs.insert(k, wits.clone());
                        }
                    }
                    if valid {
                        for ix in 0..tx.body.outputs.len() {
                            created.insert(key_of(&tx_hash, ix as u64));
                        }
                    } else if tx.body.collateral_return.is_some() {
                        created.insert(key_of(&tx_hash, tx.body.outputs.len() as u64));
                    }
                }
                nblocks += 1;
            }
        }
    }
    println!("walked blocks={nblocks} txs={ntx} (invalid={ninvalid}) inputs={ninputs}");
    Ok(needs)
}
