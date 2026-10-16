//! synth_fixtures: hand-construct a minimal Conway block covering decode.rs
//! fixture cases that are rare/unreliable to find by scanning the live chain:
//! an invalid (collateral-only) tx, a hash-kind datum, and a witness-set datum.
//!
//! pallas's `is_valid()` / `.txs()` derive validity purely from the block-level
//! `invalid_transactions` index list (see pallas-primitives conway::Block) — it
//! never executes Plutus or checks signatures — so a syntactically valid block
//! with dummy crypto fields is sufficient; no real wallet, script, or chain
//! submission is needed.
//!
//! usage: cargo run --release --example synth_fixtures
//! writes: tests/data/fixture_synthetic.block.hex

use std::collections::BTreeMap;

use pallas_codec::minicbor;
use pallas_codec::utils::{Bytes, Int, KeepRaw, NonEmptySet, Set};
use pallas_crypto::hash::Hash;
use pallas_primitives::conway::{
    Block, DatumOption, Header, HeaderBody, OperationalCert, PostAlonzoTransactionOutput,
    TransactionBody, TransactionOutput, Value, WitnessSet,
};
use pallas_primitives::{BigInt, PlutusData, TransactionInput, VrfCert};
use pallas_traverse::MultiEraBlock;

fn addr(tail: u8) -> Bytes {
    // Enterprise key address (type id 0b0110), testnet network id 0: header
    // byte 0x60 + 28-byte payment key hash. Decodes fine; the hash value and
    // network are arbitrary for decode.rs's purposes.
    let mut a = vec![0x60u8];
    a.extend(std::iter::repeat_n(tail, 28));
    Bytes::from(a)
}

fn plain_output(tail: u8, coin: u64) -> TransactionOutput<'static> {
    TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
        address: addr(tail),
        value: Value::Coin(coin),
        datum_option: None,
        script_ref: None,
    }))
}

fn input(tail: u8, ix: u64) -> TransactionInput {
    TransactionInput {
        transaction_id: Hash::from([tail; 32]),
        index: ix,
    }
}

fn witness_with_plutus_data(tag: i64) -> WitnessSet<'static> {
    let datum = PlutusData::BigInt(BigInt::Int(Int::from(tag)));
    WitnessSet {
        vkeywitness: None,
        native_script: None,
        bootstrap_witness: None,
        plutus_v1_script: None,
        plutus_data: Some(KeepRaw::from(
            NonEmptySet::try_from(vec![KeepRaw::from(datum)]).unwrap(),
        )),
        redeemer: None,
        plutus_v2_script: None,
        plutus_v3_script: None,
    }
}

fn main() -> anyhow::Result<()> {
    // tx0: valid. Output 0 carries a hash-kind datum (DatumOption::Hash);
    // the witness set carries a floating plutus_data (the normal case per
    // PLAN.md: "a hash datum's preimage usually appears in the spending tx").
    let hash_datum_output =
        TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
            address: addr(0x11),
            value: Value::Coin(1_500_000),
            datum_option: Some(KeepRaw::from(DatumOption::Hash(Hash::from([0xABu8; 32])))),
            script_ref: None,
        }));
    let body0 = TransactionBody {
        inputs: Set::from(vec![input(0x01, 0)]),
        outputs: vec![hash_datum_output, plain_output(0x12, 500_000)],
        fee: 200_000,
        ttl: None,
        certificates: None,
        withdrawals: None,
        auxiliary_data_hash: None,
        validity_interval_start: None,
        mint: None,
        script_data_hash: None,
        collateral: None,
        required_signers: None,
        network_id: None,
        collateral_return: None,
        total_collateral: None,
        reference_inputs: None,
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    };
    let wit0 = witness_with_plutus_data(1);

    // tx1: invalid (listed in the block's invalid_transactions). Only the
    // collateral input and collateral_return output are meant to be ingested;
    // its regular inputs/outputs and witness-set datum must be skipped
    // (PLAN.md ingest rule 4).
    let body1 = TransactionBody {
        inputs: Set::from(vec![input(0x02, 0)]),
        outputs: vec![plain_output(0x13, 999_000_000)],
        fee: 300_000,
        ttl: None,
        certificates: None,
        withdrawals: None,
        auxiliary_data_hash: None,
        validity_interval_start: None,
        mint: None,
        script_data_hash: None,
        collateral: Some(NonEmptySet::try_from(vec![input(0x03, 0)]).unwrap()),
        required_signers: None,
        network_id: None,
        collateral_return: Some(plain_output(0x14, 100_000)),
        total_collateral: Some(300_000),
        reference_inputs: None,
        voting_procedures: None,
        proposal_procedures: None,
        treasury_value: None,
        donation: None,
    };
    let wit1 = witness_with_plutus_data(2);

    let header = Header {
        header_body: HeaderBody {
            block_number: 1,
            slot: 1,
            prev_hash: None,
            issuer_vkey: Bytes::from(vec![0u8; 32]),
            vrf_vkey: Bytes::from(vec![0u8; 32]),
            vrf_result: VrfCert(Bytes::from(vec![0u8; 80]), Bytes::from(vec![0u8; 80])),
            block_body_size: 0,
            block_body_hash: Hash::from([0u8; 32]),
            operational_cert: OperationalCert {
                operational_cert_hot_vkey: Bytes::from(vec![0u8; 32]),
                operational_cert_sequence_number: 0,
                operational_cert_kes_period: 0,
                operational_cert_sigma: Bytes::from(vec![0u8; 64]),
            },
            protocol_version: (10, 0),
        },
        body_signature: Bytes::from(vec![0u8; 64]),
    };

    let block = Block {
        header: KeepRaw::from(header),
        transaction_bodies: vec![KeepRaw::from(body0), KeepRaw::from(body1)],
        transaction_witness_sets: vec![KeepRaw::from(wit0), KeepRaw::from(wit1)],
        auxiliary_data_set: BTreeMap::new(),
        invalid_transactions: Some(vec![1]),
    };

    let bytes = minicbor::to_vec(&(7u8, block))?;

    // Round-trip through the real decode path, exactly like a block fetched
    // from the wire, so every KeepRaw field's raw-byte tracking is genuine.
    let decoded = MultiEraBlock::decode(&bytes)?;
    assert_eq!(decoded.txs().len(), 2);
    assert!(decoded.txs()[0].is_valid());
    assert!(!decoded.txs()[1].is_valid());

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
