//! synth_fixtures: hand-construct a minimal Conway block covering decode.rs
//! fixture cases that are rare/unreliable to find by scanning the live chain:
//! an invalid (collateral-only) tx, a hash-kind datum, and a witness-set datum.
//!
//! amaru-kernel's own validity derivation (`Transaction::is_expected_valid`,
//! `Block`'s `invalid_transactions` index list) never executes Plutus or
//! checks signatures -- a syntactically valid block with dummy crypto fields
//! is sufficient; no real wallet, script, or chain submission is needed.
//!
//! `amaru_kernel::cardano::transaction_body::TransactionBody` and
//! `WitnessSet`'s `plutus_data` (`PlutusDataSet`) both keep their
//! original-bytes bookkeeping fields private -- there's no public
//! struct-literal constructor for either from outside the crate. Both are
//! built here by hand-writing their CBOR (the exact field-number maps their
//! own `Decode` impls expect -- see `TransactionBody::decode`/
//! `WitnessSet::decode` in the amaru-kernel source) and decoding that back
//! through amaru-kernel's own `Decode` impl, rather than constructing the
//! Rust value directly.
//!
//! usage: cargo run --release --example synth_fixtures
//! writes: tests/data/fixture_synthetic.block.hex

use amaru_kernel::MemoizedPlutusData;
use amaru_kernel::address::Address;
use amaru_kernel::cardano::block::Block;
use amaru_kernel::cardano::era_name::EraName;
use amaru_kernel::cardano::fixed_bytes::FixedBytes;
use amaru_kernel::cardano::header::{Header, KesSignature};
use amaru_kernel::cardano::header_body::HeaderBody;
use amaru_kernel::cardano::memoized::{MemoizedDatum, MemoizedTransactionOutput};
use amaru_kernel::cardano::operational_cert::OperationalCert;
use amaru_kernel::cardano::plutus_data::{BigInt, PlutusData};
use amaru_kernel::cardano::protocol_version::ProtocolVersion;
use amaru_kernel::cardano::transaction::Transaction;
use amaru_kernel::cardano::transaction_body::TransactionBody;
use amaru_kernel::cardano::transaction_input::TransactionInput;
use amaru_kernel::cardano::value::Value;
use amaru_kernel::cardano::vrf_cert::{VRF_PROOF, VrfCert};
use amaru_kernel::cardano::witness_set::WitnessSet;
use amaru_kernel::{
    Bytes, Ed25519Signature, Hash, Int, KesPeriod, NonEmptyVec, VerificationKey, cbor,
};

fn address(tail: u8) -> Address {
    // Enterprise key address (type id 0b0110), testnet network id 0: header
    // byte 0x60 + 28-byte payment key hash. Decodes fine; the hash value and
    // network are arbitrary for decode.rs's purposes.
    let mut bytes = vec![0x60u8];
    bytes.extend(std::iter::repeat_n(tail, 28));
    Address::from_bytes(&bytes).expect("well-formed enterprise-key address")
}

fn plain_output(tail: u8, coin: u64) -> MemoizedTransactionOutput {
    MemoizedTransactionOutput::new(address(tail), Value::Coin(coin), MemoizedDatum::None, None)
}

fn input(tail: u8, ix: u16) -> TransactionInput {
    TransactionInput {
        transaction_id: Hash::from([tail; 32]),
        index: ix,
    }
}

/// Hand-writes a `WitnessSet` map carrying a single floating `plutus_data`
/// entry (map key 4) -- see this module's own doc for why `PlutusDataSet`
/// can't be built directly.
fn witness_set_with_plutus_data(tag: i64) -> WitnessSet {
    let datum = PlutusData::int(BigInt::Int(Int::from(tag)));
    let datums: NonEmptyVec<MemoizedPlutusData> =
        NonEmptyVec::singleton(MemoizedPlutusData::new(datum));

    let mut buf = Vec::new();
    let mut ctx = ();
    {
        let mut e = cbor::Encoder::new(&mut buf);
        e.begin_map().unwrap();
        e.u8(4).unwrap().encode_with(&datums, &mut ctx).unwrap();
        e.end().unwrap();
    }
    cbor::decode(&buf).expect("valid witness set cbor")
}

/// Hand-writes a `TransactionBody` map with only the fields this fixture
/// needs set (required fields 0/1/2, plus collateral fields 13/16/17 when
/// given) -- see this module's own doc for why `TransactionBody` can't be
/// built directly.
fn transaction_body(
    inputs: Vec<TransactionInput>,
    outputs: Vec<MemoizedTransactionOutput>,
    fee: u64,
    collateral: Option<Vec<TransactionInput>>,
    collateral_return: Option<MemoizedTransactionOutput>,
    total_collateral: Option<u64>,
) -> TransactionBody {
    let inputs: amaru_kernel::Set<TransactionInput> = inputs.into_iter().collect();

    let mut buf = Vec::new();
    let mut ctx = ();
    {
        let mut e = cbor::Encoder::new(&mut buf);
        e.begin_map().unwrap();
        e.u8(0).unwrap().encode_with(&inputs, &mut ctx).unwrap();
        e.u8(1).unwrap().encode_with(&outputs, &mut ctx).unwrap();
        e.u8(2).unwrap().encode_with(fee, &mut ctx).unwrap();
        if let Some(collateral) = &collateral {
            let collateral: amaru_kernel::VecSet<TransactionInput> = collateral
                .clone()
                .try_into()
                .expect("collateral has no duplicates");
            e.u8(13)
                .unwrap()
                .encode_with(&collateral, &mut ctx)
                .unwrap();
        }
        if let Some(collateral_return) = &collateral_return {
            e.u8(16)
                .unwrap()
                .encode_with(collateral_return, &mut ctx)
                .unwrap();
        }
        if let Some(total_collateral) = total_collateral {
            e.u8(17)
                .unwrap()
                .encode_with(total_collateral, &mut ctx)
                .unwrap();
        }
        e.end().unwrap();
    }
    cbor::decode(&buf).expect("valid transaction body cbor")
}

fn main() -> anyhow::Result<()> {
    // tx0: valid. Output 0 carries a hash-kind datum (DatumOption::Hash);
    // the witness set carries a floating plutus_data (the normal case per
    // PLAN.md: "a hash datum's preimage usually appears in the spending tx").
    let hash_datum_output = MemoizedTransactionOutput::new(
        address(0x11),
        Value::Coin(1_500_000),
        MemoizedDatum::from(Hash::<32>::from([0xABu8; 32])),
        None,
    );
    let body0 = transaction_body(
        vec![input(0x01, 0)],
        vec![hash_datum_output, plain_output(0x12, 500_000)],
        200_000,
        None,
        None,
        None,
    );
    let wit0 = witness_set_with_plutus_data(1);
    let tx0 = Transaction {
        body: body0,
        witnesses: cbor::WithSize::new(wit0.clone(), cbor::to_cbor_with(&wit0, &mut ()).len()),
        is_expected_valid: true,
        auxiliary_data: None,
    };

    // tx1: invalid (listed in the block's invalid_transactions). Only the
    // collateral input and collateral_return output are meant to be ingested;
    // its regular inputs/outputs and witness-set datum must be skipped
    // (PLAN.md ingest rule 4).
    let body1 = transaction_body(
        vec![input(0x02, 0)],
        vec![plain_output(0x13, 999_000_000)],
        300_000,
        Some(vec![input(0x03, 0)]),
        Some(plain_output(0x14, 100_000)),
        Some(300_000),
    );
    let wit1 = witness_set_with_plutus_data(2);
    let tx1 = Transaction {
        body: body1,
        witnesses: cbor::WithSize::new(wit1.clone(), cbor::to_cbor_with(&wit1, &mut ()).len()),
        is_expected_valid: false,
        auxiliary_data: None,
    };

    let (block_body_hash, block_body_size) = Block::body_commitment(&[tx0.clone(), tx1.clone()])?;

    let header_body = HeaderBody {
        block_number: 1,
        slot: 1,
        prev_hash: None,
        issuer_verification_key: VerificationKey::zeroes(),
        vrf_verification_key: VerificationKey::zeroes(),
        vrf_result: VrfCert {
            output: Bytes::default(),
            proof: FixedBytes::<VRF_PROOF>::zeroes(),
        },
        block_body_size,
        block_body_hash,
        operational_cert: OperationalCert {
            operational_cert_hot_verification_key: VerificationKey::zeroes(),
            operational_cert_sequence_number: 0,
            operational_cert_kes_period: KesPeriod::from(0),
            operational_cert_sigma: Ed25519Signature::zeroes(),
        },
        protocol_version: ProtocolVersion::new(10, 0),
    };
    let header = Header::new(header_body, KesSignature::zeroes());

    let block = Block::from_transactions(header, vec![tx0, tx1])?;

    // Same wire shape blockfetch delivers: `[era_tag, block_term]` (see
    // `amaru_kernel::cardano::network_block::NetworkBlock`), era tag hardcoded
    // to Conway regardless of the dummy slot (same as the old pallas-era
    // tuple `(7u8, block)` this replaces).
    let bytes = cbor::to_cbor(&(EraName::Conway, block));

    // Round-trip through the real decode path, exactly like a block fetched
    // from the wire, so this is genuinely well-formed, not just assumed so.
    let network_block = amaru_kernel::cardano::network_block::NetworkBlock::try_from(
        amaru_kernel::RawBlock::from(bytes.as_slice()),
    )?;
    let decoded = network_block.decode_block()?;
    assert_eq!(decoded.transaction_bodies.len(), 2);
    let tx_count = (&decoded).into_iter().count();
    assert_eq!(tx_count, 2);
    let valid_flags: Vec<bool> = (&decoded)
        .into_iter()
        .map(|(_, tx)| tx.is_expected_valid)
        .collect();
    assert_eq!(valid_flags, vec![true, false]);

    std::fs::create_dir_all("tests/data")?;
    std::fs::write(
        "tests/data/fixture_synthetic.block.hex",
        hex::encode(&bytes),
    )?;
    println!(
        "wrote tests/data/fixture_synthetic.block.hex ({} bytes)",
        bytes.len()
    );

    Ok(())
}
