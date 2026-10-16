//! Streaming reader for the UTxO-HD in-memory `tables/tvar` file.
//!
//! Observed layout: CBOR `array(1)` containing a map (indefinite-length in current
//! nodes, definite-length in some) of `bytes(34) -> bytes(n)`:
//!  - key   = 32-byte tx id ++ 2-byte big-endian output index
//!  - value = MemPack-encoded TxOut, see [`crate::mempack`]

use std::io::{BufReader, Read};

use anyhow::{Result, bail};

pub const KEY_LEN: usize = 34;
const MAX_VALUE_LEN: usize = 1 << 24;

pub fn split_key(key: &[u8; KEY_LEN]) -> (&[u8], u16) {
    (&key[..32], u16::from_be_bytes([key[32], key[33]]))
}

fn byte<R: Read>(r: &mut R) -> Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

fn arg<R: Read>(r: &mut R, ai: u8) -> Result<u64> {
    Ok(match ai {
        0..=23 => u64::from(ai),
        24 => u64::from(byte(r)?),
        25 => {
            let mut b = [0u8; 2];
            r.read_exact(&mut b)?;
            u64::from(u16::from_be_bytes(b))
        }
        26 => {
            let mut b = [0u8; 4];
            r.read_exact(&mut b)?;
            u64::from(u32::from_be_bytes(b))
        }
        27 => {
            let mut b = [0u8; 8];
            r.read_exact(&mut b)?;
            u64::from_be_bytes(b)
        }
        _ => bail!("unsupported CBOR additional info {ai}"),
    })
}

/// Length of a definite-length CBOR byte string whose first byte is `first`.
fn bytes_len<R: Read>(r: &mut R, first: u8) -> Result<usize> {
    if first >> 5 != 2 {
        bail!("expected CBOR byte string, got initial byte {first:#04x}");
    }
    Ok(usize::try_from(arg(r, first & 0x1f)?)?)
}

/// Streams every entry to `f`. `f` returns `Ok(false)` to stop early.
/// Returns the number of entries visited.
pub fn for_each_entry<R, F>(r: R, mut f: F) -> Result<u64>
where
    R: Read,
    F: FnMut(&[u8; KEY_LEN], &[u8]) -> Result<bool>,
{
    let mut r = BufReader::with_capacity(1 << 20, r);

    let first = byte(&mut r)?;
    if first != 0x81 {
        bail!("expected CBOR array(1) at start of tvar, got {first:#04x}");
    }
    let m = byte(&mut r)?;
    let mut remaining: Option<u64> = match m {
        0xbf => None,
        0xa0..=0xbb => Some(arg(&mut r, m & 0x1f)?),
        _ => bail!("expected CBOR map, got {m:#04x}"),
    };

    let mut key = [0u8; KEY_LEN];
    let mut val: Vec<u8> = Vec::new();
    let mut n = 0u64;

    loop {
        let kfirst = match remaining {
            Some(0) => break,
            Some(ref mut k) => {
                *k -= 1;
                byte(&mut r)?
            }
            None => {
                let b = byte(&mut r)?;
                if b == 0xff {
                    break;
                }
                b
            }
        };
        let klen = bytes_len(&mut r, kfirst)?;
        if klen != KEY_LEN {
            bail!("unexpected key length {klen} at entry {n}");
        }
        r.read_exact(&mut key)?;

        let vfirst = byte(&mut r)?;
        let vlen = bytes_len(&mut r, vfirst)?;
        if vlen > MAX_VALUE_LEN {
            bail!("value length {vlen} too large at entry {n}");
        }
        val.resize(vlen, 0);
        r.read_exact(&mut val)?;

        n += 1;
        if !f(&key, &val)? {
            break;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempack::{Datum, decode_tx_out};

    /// First 400 bytes of a real preprod `tables/tvar` (epoch 306 snapshot):
    /// the whole first entry, then a truncated second one.
    const HEAD: &[u8] = include_bytes!("../tests/data/tvar_head.bin");

    #[test]
    fn first_real_entry() {
        let mut got = None;
        let _ = for_each_entry(HEAD, |k, v| {
            got = Some((*k, v.to_vec()));
            Ok(false)
        })
        .unwrap();
        let (key, val) = got.unwrap();

        let (txid, ix) = split_key(&key);
        assert_eq!(
            hex::encode(txid),
            "00000c0cf6fe6389492dd7fe7c8ff3040d70d11b3356093cf651ac876c6f66d9"
        );
        assert_eq!(ix, 0);
        assert_eq!(val.len(), 274);

        let o = decode_tx_out(&val).unwrap();
        assert_eq!(o.tag, 0);
        assert_eq!(o.address.len(), 57);
        assert_eq!(o.address[0], 0x00); // base, key/key, testnet
        assert_eq!(o.coin, 1_710_000);
        assert_eq!(o.datum, Datum::None);
        assert!(o.script.is_none());

        assert_eq!(o.assets.len(), 10);
        let policy = "f1efa1875fc86249b86bdd726dc72f63ec94e15ba1b1285559bb1d25";
        let mut names: Vec<String> = Vec::new();
        for a in &o.assets {
            assert_eq!(hex::encode(a.policy), policy);
            assert_eq!(a.qty, 1);
            names.push(String::from_utf8(a.name.clone()).unwrap());
        }
        names.sort();
        let expect: Vec<String> = (90..=99).map(|i| format!("NFT_{i}")).collect();
        assert_eq!(names, expect);
    }

    #[test]
    fn second_entry_key_is_same_tx_ix1() {
        // Entry 1's value ends at 0x13b (0x29 + 274); entry 2 = `58 22` + 34-byte key.
        assert_eq!(&HEAD[0x13b..0x13d], &[0x58, 0x22]);
        let k = &HEAD[0x13d..0x13d + 34];
        assert_eq!(&k[..32], &HEAD[4..36]);
        assert_eq!(u16::from_be_bytes([k[32], k[33]]), 1);
    }
}
