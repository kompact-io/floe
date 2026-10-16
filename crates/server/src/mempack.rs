//! Decoder for the node's `MemPack`-encoded Babbage/Conway `TxOut`, as stored in
//! UTxO-HD `tables/tvar` snapshots.
//!
//! The layout follows `amaru-bootstrap/src/cardano_node/mempack.rs`
//! (Apache-2.0, (c) PRAGMA), reimplemented here without amaru types.
//!
//! Addresses are returned as raw bytes. Pointer addresses with out-of-range
//! pointers are NOT normalised (amaru rewrites them to (0,0,0)); irrelevant for
//! credential indexing.

use anyhow::{Result, anyhow, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Datum {
    None,
    Hash([u8; 32]),
    /// Raw CBOR bytes of the inline datum.
    Inline(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptRef {
    /// Raw CBOR bytes of the native script.
    Native(Vec<u8>),
    PlutusV1(Vec<u8>),
    PlutusV2(Vec<u8>),
    PlutusV3(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asset {
    pub policy: [u8; 28],
    pub name: Vec<u8>,
    pub qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxOut {
    /// MemPack constructor tag (0..=5), useful for stats.
    pub tag: u8,
    pub address: Vec<u8>,
    pub coin: u64,
    pub assets: Vec<Asset>,
    pub datum: Datum,
    pub script: Option<ScriptRef>,
}

const MAX_VARUINT_BYTES: usize = 10;

struct Dec<'a> {
    b: &'a [u8],
    off: usize,
}

impl<'a> Dec<'a> {
    fn new(b: &'a [u8]) -> Self {
        Self { b, off: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .off
            .checked_add(n)
            .ok_or_else(|| anyhow!("offset overflow"))?;
        if end > self.b.len() {
            bail!("unexpected end at {} reading {} bytes", self.off, n);
        }
        let s = &self.b[self.off..end];
        self.off = end;
        Ok(s)
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    /// Big-endian base-128 varint (high bit = continuation).
    fn varuint(&mut self) -> Result<u64> {
        let mut v = 0u64;
        for i in 0..MAX_VARUINT_BYTES {
            let b = self.byte()?;
            v = v
                .checked_mul(0x80)
                .and_then(|v| v.checked_add(u64::from(b & 0x7f)))
                .ok_or_else(|| anyhow!("varuint overflows u64"))?;
            if b & 0x80 == 0 {
                return Ok(v);
            }
            if i + 1 == MAX_VARUINT_BYTES {
                bail!("varuint exceeds {MAX_VARUINT_BYTES} bytes");
            }
        }
        unreachable!()
    }

    /// varuint length followed by that many bytes.
    fn short_bytes(&mut self) -> Result<&'a [u8]> {
        let n = usize::try_from(self.varuint()?).map_err(|_| anyhow!("length overflow"))?;
        self.take(n)
    }
}

/// Decode one value of the `tvar` map.
pub fn decode_tx_out(bytes: &[u8]) -> Result<TxOut> {
    let mut d = Dec::new(bytes);
    let tag = d.byte()?;

    let mut out = TxOut {
        tag,
        address: Vec::new(),
        coin: 0,
        assets: Vec::new(),
        datum: Datum::None,
        script: None,
    };

    match tag {
        0 | 1 | 4 | 5 => {
            out.address = d.short_bytes()?.to_vec();
            compact_value(&mut d, &mut out)?;
            match tag {
                0 => {}
                1 => out.datum = Datum::Hash(d.array()?),
                4 => out.datum = Datum::Inline(d.short_bytes()?.to_vec()),
                _ => {
                    out.datum = datum(&mut d)?;
                    out.script = Some(script(&mut d)?);
                }
            }
        }
        2 | 3 => {
            let stake = stake_credential(&mut d)?;
            out.address = address28(&mut d, stake)?;
            if d.byte()? != 0 {
                bail!("unsupported compact coin tag");
            }
            out.coin = d.varuint()?;
            if tag == 3 {
                out.datum = Datum::Hash(packed_hash32(&mut d)?);
            }
        }
        t => bail!("unsupported BabbageTxOut mempack tag {t}"),
    }

    if d.off != bytes.len() {
        bail!(
            "{} trailing bytes after tag {tag} output",
            bytes.len() - d.off
        );
    }
    Ok(out)
}

fn compact_value(d: &mut Dec, out: &mut TxOut) -> Result<()> {
    match d.byte()? {
        0 => out.coin = d.varuint()?,
        1 => {
            out.coin = d.varuint()?;
            let n = usize::try_from(d.varuint()?).map_err(|_| anyhow!("asset count overflow"))?;
            let rep = d.short_bytes()?;
            out.assets = multiasset_rep(rep, n)?;
        }
        t => bail!("unsupported compact value tag {t}"),
    }
    Ok(())
}

fn datum(d: &mut Dec) -> Result<Datum> {
    Ok(match d.byte()? {
        0 => Datum::None,
        1 => Datum::Hash(d.array()?),
        2 => Datum::Inline(d.short_bytes()?.to_vec()),
        t => bail!("unsupported datum tag {t}"),
    })
}

fn script(d: &mut Dec) -> Result<ScriptRef> {
    Ok(match d.byte()? {
        0 => ScriptRef::Native(d.short_bytes()?.to_vec()),
        1 => {
            let lang = d.byte()?;
            let b = d.short_bytes()?.to_vec();
            match lang {
                0 => ScriptRef::PlutusV1(b),
                1 => ScriptRef::PlutusV2(b),
                2 => ScriptRef::PlutusV3(b),
                l => bail!("unsupported plutus script tag {l}"),
            }
        }
        t => bail!("unsupported script tag {t}"),
    })
}

/// (tag, 28-byte hash). tag 0 = script, 1 = key.
fn stake_credential(d: &mut Dec) -> Result<(u8, [u8; 28])> {
    let tag = d.byte()?;
    let h = d.array::<28>()?;
    match tag {
        0 | 1 => Ok((tag, h)),
        t => bail!("unsupported stake credential tag {t}"),
    }
}

/// Rebuild a base address from the "Addr28Extra" packing: four little-endian
/// words holding the 28-byte payment hash and flags.
fn address28(d: &mut Dec, stake: (u8, [u8; 28])) -> Result<Vec<u8>> {
    let extra = d.array::<32>()?;
    let flags = u32::from_le_bytes(extra[24..28].try_into().unwrap());

    let mut pay = [0u8; 28];
    for (i, chunk) in extra[..24].chunks(8).enumerate() {
        pay[i * 8..(i + 1) * 8].copy_from_slice(chunk);
        pay[i * 8..(i + 1) * 8].reverse();
    }
    pay[24..].copy_from_slice(&extra[28..32]);
    pay[24..].reverse();

    let mainnet = flags & 0b10 != 0;
    let pay_is_key = flags & 0b1 != 0;
    let stake_is_script = stake.0 == 0;

    // header: bit4 = payment is script, bit5 = stake is script, low nibble = network
    let header =
        (u8::from(!pay_is_key) << 4) | (u8::from(stake_is_script) << 5) | u8::from(mainnet);

    let mut a = Vec::with_capacity(57);
    a.push(header);
    a.extend_from_slice(&pay);
    a.extend_from_slice(&stake.1);
    Ok(a)
}

fn packed_hash32(d: &mut Dec) -> Result<[u8; 32]> {
    let raw = d.array::<32>()?;
    let mut h = [0u8; 32];
    for (i, chunk) in raw.chunks(8).enumerate() {
        h[i * 8..(i + 1) * 8].copy_from_slice(chunk);
        h[i * 8..(i + 1) * 8].reverse();
    }
    Ok(h)
}

/// rep = [n x u64 LE quantity][n x u16 LE policy offset][n x u16 LE name offset][data]
fn multiasset_rep(rep: &[u8], n: usize) -> Result<Vec<Asset>> {
    if n > rep.len() {
        bail!(
            "asset count {n} exceeds representation length {}",
            rep.len()
        );
    }
    let q_end = n * 8;
    let p_end = q_end + n * 2;
    let a_end = p_end + n * 2;
    if rep.len() < a_end {
        bail!("multiasset representation is truncated");
    }

    let u16_at = |at: usize| u16::from_le_bytes([rep[at], rep[at + 1]]) as usize;

    let mut triples = Vec::with_capacity(n);
    for i in 0..n {
        let qty = u64::from_le_bytes(rep[i * 8..i * 8 + 8].try_into().unwrap());
        triples.push((u16_at(q_end + i * 2), u16_at(p_end + i * 2), qty));
    }

    // Name lengths are implied by consecutive distinct offsets; the last runs to the end.
    let mut ordered: Vec<usize> = Vec::new();
    for &(_, a, _) in &triples {
        if ordered.last() != Some(&a) {
            ordered.push(a);
        }
    }
    let mut lens = std::collections::BTreeMap::new();
    for (i, &off) in ordered.iter().enumerate() {
        let next = ordered.get(i + 1).copied().unwrap_or(rep.len());
        if off > next || next > rep.len() {
            bail!("invalid asset-name offsets");
        }
        lens.insert(off, next - off);
    }

    let mut out = Vec::with_capacity(n);
    for (po, ao, qty) in triples {
        if po + 28 > rep.len() {
            bail!("policy offset out of bounds");
        }
        let len = *lens
            .get(&ao)
            .ok_or_else(|| anyhow!("missing asset-name length"))?;
        if ao + len > rep.len() {
            bail!("asset-name offset out of bounds");
        }
        let mut policy = [0u8; 28];
        policy.copy_from_slice(&rep[po..po + 28]);
        out.push(Asset {
            policy,
            name: rep[ao..ao + len].to_vec(),
            qty,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_address_value_no_datum() {
        let addr = vec![0x61u8; 29];
        let mut b = vec![0u8, addr.len() as u8];
        b.extend_from_slice(&addr);
        b.extend_from_slice(&[0, 42]); // value tag 0, coin 42
        let o = decode_tx_out(&b).unwrap();
        assert_eq!(o.tag, 0);
        assert_eq!(o.address, addr);
        assert_eq!(o.coin, 42);
        assert_eq!(o.datum, Datum::None);
        assert!(o.assets.is_empty() && o.script.is_none());
    }

    #[test]
    fn inline_datum_tag4() {
        let addr = vec![0x61u8; 29];
        let mut b = vec![4u8, addr.len() as u8];
        b.extend_from_slice(&addr);
        b.extend_from_slice(&[0, 99]); // coin 99
        b.extend_from_slice(&[1, 0]); // datum bytes: len 1, 0x00
        let o = decode_tx_out(&b).unwrap();
        assert_eq!(o.coin, 99);
        assert_eq!(o.datum, Datum::Inline(vec![0]));
    }

    #[test]
    fn rejects_trailing_bytes_and_overlong_varuint() {
        let mut b = vec![0u8, 1, 0x61, 0, 1, 0xff];
        assert!(decode_tx_out(&b).is_err());
        b = vec![
            0u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
        ];
        assert!(decode_tx_out(&b).is_err());
    }
}
