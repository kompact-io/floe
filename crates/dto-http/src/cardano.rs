//! Mirrors `floe_dto_grpc::floe::v1::cardano`: same names, same field
//! order, for every message floe has a use for. Each dropped field or
//! struct carries a one-line comment saying why.
//!
//! This is `query::Utxo`/`query::Tx`'s chain-specific payload --
//! `TxInput`/`TxOutput` is the distinction those two actually need: an
//! input is a prior output plus where it was spent from, an output is
//! just itself.

#[cfg(feature = "cbor")]
use minicbor::{Decode, Encode};
#[cfg(feature = "schema")]
use schemars::JsonSchema;
#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};

use crate::TxHash;
use crate::query::Address;

/// The discriminant is also the blake2b-224 `lang_tag` byte prepended
/// to script bytes before hashing -- a real Cardano protocol constant.
/// `floe-server`'s `decode.rs` re-exports this type so both crates
/// share one discriminant. `Debug`/`Clone`/`Copy`/`Eq` stay
/// unconditional since `store::query::ResolvedScript` uses this type
/// regardless of which wire-format features are enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
#[cfg_attr(feature = "json", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "cbor", cbor(index_only))]
pub enum ScriptLang {
    #[cfg_attr(feature = "cbor", n(0))]
    Native = 0,
    #[cfg_attr(feature = "cbor", n(1))]
    PlutusV1 = 1,
    #[cfg_attr(feature = "cbor", n(2))]
    PlutusV2 = 2,
    #[cfg_attr(feature = "cbor", n(3))]
    PlutusV3 = 3,
    /// Doesn't exist on any network yet -- kept for parity with
    /// `cardano.Script`'s own `plutus_v4`, not because floe has ever
    /// indexed one.
    #[cfg_attr(feature = "cbor", n(4))]
    PlutusV4 = 4,
}

/// `cardano.Asset`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct Asset {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_vec"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub name: Vec<u8>,
    /// `cardano.Asset.quantity` is a signed `BigInt` (also covers a
    /// mint/burn delta); a live output's held quantity is always
    /// non-negative, so this is a plain `u64`.
    #[cfg_attr(feature = "cbor", n(1))]
    pub quantity: u64,
}

/// `cardano.Multiasset`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct Multiasset {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_vec"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub policy_id: Vec<u8>,
    #[cfg_attr(feature = "cbor", n(1))]
    pub assets: Vec<Asset>,
}

/// `cardano.Datum`, minus `payload` (parsed `PlutusData`, unsupported).
/// A `None`-kind datum is the *containing* `Option<Datum>` being
/// `None`, not a variant here -- a `Datum` always has a hash.
/// `original_cbor` is `None` for an unresolved hash-kind datum, `Some`
/// once resolved (inline or witnessed).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct Datum {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_array"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub hash: [u8; 32],
    // payload (parsed PlutusData): unsupported, no parser for it.
    #[cfg_attr(
        feature = "json",
        serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::ser_hex_opt",
            deserialize_with = "crate::de_hex_vec_opt",
            default
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub original_cbor: Option<Vec<u8>>,
}

/// `cardano.Script`'s `oneof {native, plutus_v1..v4}` collapsed to a
/// tag + raw bytes, lossless either way: `cbor` is the raw script
/// bytes regardless of `lang`, with no recursive `NativeScript`
/// decode.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct Script {
    #[cfg_attr(feature = "cbor", n(0))]
    pub lang: ScriptLang,
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_vec"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub cbor: Vec<u8>,
}

/// `cardano.TxOutput`, minus `original_cbor` (the output's own raw
/// CBOR as seen on-chain -- floe doesn't retain it). `address` renders
/// bech32, where gRPC's own field is raw `bytes` (see `query::Address`'s
/// own doc).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct TxOutput {
    #[cfg_attr(feature = "cbor", n(0))]
    pub address: Address,
    #[cfg_attr(feature = "cbor", n(1))]
    pub coin: u64,
    #[cfg_attr(feature = "cbor", n(2))]
    pub assets: Vec<Multiasset>,
    #[cfg_attr(feature = "cbor", n(3))]
    pub datum: Option<Datum>,
    #[cfg_attr(feature = "cbor", n(4))]
    pub script: Option<Script>,
    // original_cbor: not retained.
}

/// `cardano.TxInput`, minus `redeemer` (floe tracks no redeemer data).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct TxInput {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_array"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub tx_hash: TxHash,
    #[cfg_attr(feature = "cbor", n(1))]
    pub output_index: u16,
    #[cfg_attr(feature = "cbor", n(2))]
    pub as_output: TxOutput,
    // redeemer: unsupported.
}
