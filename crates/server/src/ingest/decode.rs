//! decode: pure per-block extraction of floe's UTxO events from a pallas
//! `MultiEraBlock`. No I/O, no DB; `store` consumes a `DecodedBlock`.

use std::collections::BTreeMap;

use pallas_addresses::{Address, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
use pallas_codec::minicbor;
use pallas_primitives::conway::{DatumOption, ScriptRef as PallasScriptRef, Value};
use pallas_traverse::{
    ComputeHash, MultiEraBlock, MultiEraOutput, MultiEraSigners, MultiEraTx, OriginalHash,
};

pub type TxHash = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutputRef {
    pub tx_hash: TxHash,
    pub ix: u16,
}

/// A real Cardano protocol constant (its discriminant is also the
/// blake2b-224 `lang_tag` byte prepended to the script bytes before
/// hashing, used below), not a value unique to this crate's own
/// modeling -- defined in `floe-dto-http` (which both `floe-server` and
/// `floe-client` depend on) and re-exported here so every existing
/// `crate::ingest::decode::ScriptLang` reference in this crate keeps working
/// unchanged.
pub use floe_dto_http::cardano::ScriptLang;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatumKind {
    None = 0,
    Hash = 1,
    Inline = 2,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Utxo {
    pub output_ref: OutputRef,
    /// Always present: Byron outputs (the only address kind with no
    /// payment hash) are filtered out before a `Utxo` is ever built --
    /// see `decode_output`/`seed_utxo`.
    pub pay_hash: [u8; 28],
    pub stake_hash: Option<[u8; 28]>,
    /// The address's own header byte -- key-vs-script for both parts,
    /// network, and address type, all in one (see
    /// `credentials_from_address`). The only thing that lets
    /// `reconstruct_address` rebuild the exact original address from
    /// `pay_hash`/`stake_hash` alone.
    pub address_header: u8,
    pub coin: u64,
    /// Raw CBOR of the multiasset map, `None` if the output carries ADA only.
    pub assets: Option<Vec<u8>>,
    pub datum_kind: DatumKind,
    pub datum_hash: Option<[u8; 32]>,
    pub script_hash: Option<[u8; 28]>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DatumRow {
    pub hash: [u8; 32],
    /// Raw CBOR of the original datum bytes (never re-encoded).
    pub cbor: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScriptRow {
    pub hash: [u8; 28],
    pub lang: ScriptLang,
    /// Raw script bytes: the CBOR-encoded native script, or the raw Plutus bytecode.
    pub cbor: Vec<u8>,
}

/// A tx's existence and parent block, plus `required_signers` -- no
/// other body fields (no certs, mint, fee, ...). See
/// `store::BASE_SCHEMA_SQL`'s `tx` table doc: this stays minimal by
/// design, `required_signers` included deliberately rather than as a
/// first step towards storing the rest of the tx body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxRow {
    pub hash: TxHash,
    /// Empty for a tx with no `required_signers` field, or one
    /// inherited from a seed snapshot (which carries no tx-body data
    /// at all, not just this field -- see `seed.rs`).
    pub required_signers: Vec<[u8; 28]>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct DecodedBlock {
    pub txs: Vec<TxRow>,
    /// Refs consumed by valid txs' inputs, or by invalid txs' collateral,
    /// keyed to the hash of the tx that spent them.
    pub spent: BTreeMap<OutputRef, TxHash>,
    pub created: Vec<Utxo>,
    pub datums: Vec<DatumRow>,
    pub scripts: Vec<ScriptRow>,
}

pub(crate) fn hash_bytes<const N: usize>(h: impl AsRef<[u8]>) -> [u8; N] {
    h.as_ref().try_into().expect("wrong hash length")
}

/// `tx.required_signers()` across every era: `NotApplicable` (Byron/
/// Shelley/Allegra/Mary, no such CBOR field at all) and `Empty` (field
/// present but empty) both collapse to `vec![]` -- `TxRow` has no use
/// for the distinction, same as every other "absent" case here.
fn required_signers(tx: &MultiEraTx) -> Vec<[u8; 28]> {
    match tx.required_signers() {
        MultiEraSigners::AlonzoCompatible(signers) => signers.iter().map(hash_bytes).collect(),
        _ => vec![],
    }
}

/// `(pay_hash, stake_hash, address_header)`. `pay_hash` is `None` only
/// for a Byron or reward/stake address (neither has a payment part).
/// `address_header` is the header byte of the *effective* address
/// this returns hashes for -- see below for why that's not always the
/// address's own original header.
///
/// Pointer addresses are not supported: `Pointer` collapses into the
/// same no-stake-part shape as `Null` (enterprise) -- not an
/// approximation to revisit, since floe doesn't store raw address
/// bytes to fall back on (see `store::BASE_SCHEMA_SQL`'s `utxo` table
/// comment). `address_header` reflects this collapse (built from the
/// collapsed parts, not read off the original bytes): a pointer
/// address's own header would claim pointer payload bytes follow,
/// which floe never stores, so reconstructing from that header would
/// misparse.
pub type Credentials = (Option<[u8; 28]>, Option<[u8; 28]>, u8);

/// Shared by the live decode path and `seed`'s snapshot decode. A
/// reward/stake address isn't a UTxO owner and, like Byron, has no
/// payment part.
pub fn credentials_from_address(address: &Address) -> Credentials {
    match address {
        Address::Shelley(a) => {
            let payment = a.payment().clone();
            let pay_hash = hash_bytes(match &payment {
                ShelleyPaymentPart::Key(h) => h,
                ShelleyPaymentPart::Script(h) => h,
            });
            let delegation = match a.delegation() {
                ShelleyDelegationPart::Key(h) => ShelleyDelegationPart::Key(*h),
                ShelleyDelegationPart::Script(h) => ShelleyDelegationPart::Script(*h),
                ShelleyDelegationPart::Pointer(_) | ShelleyDelegationPart::Null => {
                    ShelleyDelegationPart::Null
                }
            };
            let stake_hash = match &delegation {
                ShelleyDelegationPart::Key(h) | ShelleyDelegationPart::Script(h) => {
                    Some(hash_bytes(h))
                }
                _ => None,
            };
            let header = ShelleyAddress::new(a.network(), payment, delegation).to_header();
            (Some(pay_hash), stake_hash, header)
        }
        Address::Byron(_) | Address::Stake(_) => (None, None, 0),
    }
}

pub fn decode_block(blk: &MultiEraBlock) -> DecodedBlock {
    let mut out = DecodedBlock::default();
    for tx in blk.txs() {
        decode_tx(&tx, &mut out);
    }
    out
}

/// `decode_block` is pure and per-block, so a batch just `par_iter().map()`s
/// over it; order of `blks` is preserved in the result so callers can zip
/// it back against the originating headers.
pub fn decode_blocks_parallel(blks: &[MultiEraBlock]) -> Vec<DecodedBlock> {
    use rayon::prelude::*;
    blks.par_iter().map(decode_block).collect()
}

fn decode_tx(tx: &MultiEraTx, out: &mut DecodedBlock) {
    let tx_hash: TxHash = hash_bytes(tx.hash());
    out.txs.push(TxRow {
        hash: tx_hash,
        required_signers: required_signers(tx),
    });

    for input in tx.consumes() {
        out.spent.insert(
            OutputRef {
                tx_hash: hash_bytes(input.hash()),
                ix: input.index() as u16,
            },
            tx_hash,
        );
    }

    // Witness datums of an invalid tx are skipped.
    if tx.is_valid() {
        for d in tx.plutus_data() {
            out.datums.push(DatumRow {
                hash: hash_bytes(d.original_hash()),
                cbor: d.raw_cbor().to_vec(),
            });
        }
    }

    for (ix, output) in tx.produces() {
        if let Some(utxo) = decode_output(tx_hash, ix as u16, &output, out) {
            out.created.push(utxo);
        }
    }
}

/// Returns `None` for Byron-address outputs: not indexed. Outputs created
/// after floe's seed anchor are always Shelley+, so this is defensive,
/// not expected to trigger on the live sync path.
fn decode_output(
    tx_hash: TxHash,
    ix: u16,
    output: &MultiEraOutput,
    out: &mut DecodedBlock,
) -> Option<Utxo> {
    let address = output.address().ok()?;
    if matches!(address, Address::Byron(_)) {
        return None;
    }

    let (pay_hash, stake_hash, address_header) = credentials_from_address(&address);
    let pay_hash = pay_hash.expect("Byron (the only no-pay_hash case) was already filtered out");

    let value = output.value();
    let coin = value.coin();
    let assets = match value.into_conway() {
        Value::Coin(_) => None,
        Value::Multiasset(_, assets) => Some(minicbor::to_vec(&assets).expect("infallible")),
    };

    let (datum_kind, datum_hash) = match output.datum() {
        None => (DatumKind::None, None),
        Some(DatumOption::Hash(h)) => (DatumKind::Hash, Some(hash_bytes(h))),
        Some(DatumOption::Data(d)) => {
            let hash = hash_bytes(d.original_hash());
            out.datums.push(DatumRow {
                hash,
                cbor: d.raw_cbor().to_vec(),
            });
            (DatumKind::Inline, Some(hash))
        }
    };

    let script_hash = output.script_ref().map(|script_ref| {
        let (lang, hash, cbor) = match script_ref {
            PallasScriptRef::NativeScript(ns) => (
                ScriptLang::Native,
                hash_bytes(ns.original_hash()),
                ns.raw_cbor().to_vec(),
            ),
            PallasScriptRef::PlutusV1Script(s) => (
                ScriptLang::PlutusV1,
                hash_bytes(s.compute_hash()),
                s.0.to_vec(),
            ),
            PallasScriptRef::PlutusV2Script(s) => (
                ScriptLang::PlutusV2,
                hash_bytes(s.compute_hash()),
                s.0.to_vec(),
            ),
            PallasScriptRef::PlutusV3Script(s) => (
                ScriptLang::PlutusV3,
                hash_bytes(s.compute_hash()),
                s.0.to_vec(),
            ),
        };
        out.scripts.push(ScriptRow { hash, lang, cbor });
        hash
    });

    Some(Utxo {
        output_ref: OutputRef { tx_hash, ix },
        pay_hash,
        stake_hash,
        address_header,
        coin,
        assets,
        datum_kind,
        datum_hash,
        script_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_crypto::hash::Hasher;

    /// Fixtures are raw block CBOR (hex) captured from a live preprod peer.
    fn decode_fixture(hex_str: &str) -> DecodedBlock {
        let bytes = hex::decode(hex_str.trim()).expect("fixture is valid hex");
        let blk = MultiEraBlock::decode(&bytes).expect("fixture is a valid block");
        decode_block(&blk)
    }

    #[test]
    fn inline_datum_is_ingested_and_hashed_over_original_bytes() {
        let decoded = decode_fixture(include_str!(
            "../../tests/data/fixture_inline_datum.block.hex"
        ));
        assert!(
            !decoded.created.is_empty(),
            "expected at least one created output (valid tx)"
        );

        let inline = decoded
            .created
            .iter()
            .find(|u| matches!(u.datum_kind, DatumKind::Inline))
            .expect("fixture should contain an inline-datum output");
        let hash = inline.datum_hash.expect("inline datum must carry a hash");

        let row = decoded
            .datums
            .iter()
            .find(|d| d.hash == hash)
            .expect("inline datum hash must have a matching datum row");

        // Recompute independently of decode.rs's own OriginalHash call.
        let recomputed = Hasher::<256>::hash(&row.cbor);
        assert_eq!(recomputed.as_ref(), hash.as_slice());
    }

    #[test]
    fn same_block_create_and_spend_are_both_reported() {
        let decoded = decode_fixture(include_str!(
            "../../tests/data/fixture_same_block_chain.block.hex"
        ));
        assert!(!decoded.created.is_empty());
        assert!(!decoded.spent.is_empty());

        // decode.rs reports raw events; cancelling a same-block create+spend
        // pair is the catch-up chunk buffer's job, not decode's.
        let chained = decoded
            .created
            .iter()
            .any(|u| decoded.spent.contains_key(&u.output_ref));
        assert!(
            chained,
            "fixture should contain an output created and spent within the same block"
        );
    }

    #[test]
    fn reference_script_is_ingested_and_hashed_with_correct_lang_tag() {
        let decoded = decode_fixture(include_str!(
            "../../tests/data/fixture_ref_script.block.hex"
        ));

        let with_script = decoded
            .created
            .iter()
            .find(|u| u.script_hash.is_some())
            .expect("fixture should contain a reference-script output");
        let hash = with_script.script_hash.unwrap();

        let row = decoded
            .scripts
            .iter()
            .find(|s| s.hash == hash)
            .expect("script hash must have a matching script row");

        // Recompute independently of decode.rs's own ComputeHash/OriginalHash call.
        let recomputed = Hasher::<224>::hash_tagged(&row.cbor, row.lang as u8);
        assert_eq!(recomputed.as_ref(), hash.as_slice());
    }

    /// Hand-built via `examples/synth_fixtures.rs`: tx0 is valid (hash-kind
    /// datum output, witness-set datum tag 1); tx1 is invalid (regular
    /// input/output that must be ignored, collateral input/return that must
    /// not, witness-set datum tag 2 that must be skipped).
    fn synthetic() -> DecodedBlock {
        decode_fixture(include_str!("../../tests/data/fixture_synthetic.block.hex"))
    }

    fn has_addr_tail(u: &Utxo, tail: u8) -> bool {
        u.pay_hash == [tail; 28]
    }

    #[test]
    fn hash_datum_is_recorded_unresolved() {
        let decoded = synthetic();

        let hashed = decoded
            .created
            .iter()
            .find(|u| has_addr_tail(u, 0x11))
            .expect("tx0's first output should be present");
        assert!(matches!(hashed.datum_kind, DatumKind::Hash));
        assert_eq!(hashed.datum_hash, Some([0xABu8; 32]));

        // No witness in this fixture reveals the preimage, so it must stay
        // unresolved.
        assert!(!decoded.datums.iter().any(|d| d.hash == [0xABu8; 32]));
    }

    #[test]
    fn invalid_tx_ingests_only_collateral() {
        let decoded = synthetic();

        // tx1's regular input/output must be ignored...
        assert!(!decoded.spent.keys().any(|r| r.tx_hash == [0x02u8; 32]));
        assert!(!decoded.created.iter().any(|u| has_addr_tail(u, 0x13)));
        // ...while its collateral input/return must be ingested.
        assert!(
            decoded
                .spent
                .keys()
                .any(|r| r.tx_hash == [0x03u8; 32] && r.ix == 0)
        );
        let collateral_return = decoded
            .created
            .iter()
            .find(|u| has_addr_tail(u, 0x14))
            .expect("tx1's collateral_return output should be present");
        assert_eq!(collateral_return.coin, 100_000);

        // tx0 (valid) is unaffected.
        assert!(
            decoded
                .spent
                .keys()
                .any(|r| r.tx_hash == [0x01u8; 32] && r.ix == 0)
        );
        assert!(decoded.created.iter().any(|u| has_addr_tail(u, 0x11)));
        assert!(decoded.created.iter().any(|u| has_addr_tail(u, 0x12)));
    }

    #[test]
    fn parallel_decode_matches_sequential_decode() {
        let fixtures = [
            include_str!("../../tests/data/fixture_inline_datum.block.hex"),
            include_str!("../../tests/data/fixture_ref_script.block.hex"),
            include_str!("../../tests/data/fixture_same_block_chain.block.hex"),
            include_str!("../../tests/data/fixture_synthetic.block.hex"),
        ];
        let blocks: Vec<MultiEraBlock> = fixtures
            .iter()
            .map(|hex_str| {
                let bytes = hex::decode(hex_str.trim()).unwrap();
                // Deliberately leaked: MultiEraBlock borrows from the decoded
                // bytes, and this is a short-lived test fixture, not a service.
                MultiEraBlock::decode(Box::leak(bytes.into_boxed_slice())).unwrap()
            })
            .collect();

        let sequential: Vec<DecodedBlock> = blocks.iter().map(decode_block).collect();
        let parallel = decode_blocks_parallel(&blocks);
        assert_eq!(sequential, parallel);
    }

    #[test]
    fn witness_datum_ingested_only_for_valid_tx() {
        let decoded = synthetic();

        // tx0's witness datum (tag 1) is ingested; tx1's (tag 2) is skipped
        // because tx1 is invalid.
        assert_eq!(
            decoded.datums.len(),
            1,
            "exactly one witness datum should survive: {:?}",
            decoded.datums
        );
    }
}
