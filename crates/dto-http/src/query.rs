//! Mirrors `floe_dto_grpc::floe::v1::query`: same names, same field
//! layout, for every message floe has an equivalent of. `Utxo` and
//! `Tx` inline `AnyUtxoData`/`AnyChainTx`'s own fields directly around
//! a `cardano::TxOutput`/`cardano::TxInput`, in place of gRPC's `oneof
//! chain`/`AnyChain*` envelope, since there's only one chain to pick
//! between.
//!
//! Each RPC's request/response pair sits together here, its own
//! `*_PATH` route constant right above it -- the one place that
//! boundary is defined, so `floe-server` (encodes responses/decodes
//! requests) and `floe-client` (encodes requests/decodes responses)
//! both depend on it rather than on each other. Every route is POST:
//! the request travels as a body in the same negotiated wire format
//! (JSON/CBOR) as the response, picked per request from `Accept`
//! (see `floe-server`'s `scheme::http::query`), not GET+query-string --
//! that also means a `*Request` gets real CBOR, not hex-in-text, on
//! the CBOR wire.
//!
//! Field masks and `UtxoPredicate`'s general predicate tree have no
//! equivalent here at all -- `SearchUtxosRequest` flattens to a single
//! `pay_hash`/`stake_hash` address filter (floe's `SearchUtxos` itself
//! only ever supports that one pattern, over either transport -- see
//! `scheme::grpc`'s `parse_predicate`), matching `AddressFilter`
//! (`floe-server`'s own internal type, shared by both schemes)
//! one-for-one. Everything else mirrors the gRPC request field-for-
//! field, including names (e.g. `ReadUtxosRequest.keys`,
//! `ReadDataRequest.key` vs. `ReadTxRequest.hash` -- that asymmetry is
//! the gRPC proto's own, not introduced here).

#[cfg(feature = "cbor")]
use minicbor::decode::{Decoder, Error as DecodeError};
#[cfg(feature = "cbor")]
use minicbor::encode::{Encoder, Error as EncodeError, Write as CborWrite};
#[cfg(feature = "cbor")]
use minicbor::{Decode, Encode};
use pallas_addresses::Address as PallasAddress;
#[cfg(feature = "schema")]
use schemars::JsonSchema;
#[cfg(feature = "json")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::TxHash;
use crate::cardano::{TxInput, TxOutput};

/// Mirrors `query::ChainPoint`: zero-valued (slot 0, empty hash, height
/// 0, timestamp 0) is the "unknown" sentinel -- a pruned or
/// seed-anchor-predating origin block. `timestamp` is always 0
/// (deriving a real one needs genesis/era data floe doesn't track).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ChainPoint {
    #[cfg_attr(feature = "cbor", n(0))]
    pub slot: u64,
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_vec"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub hash: Vec<u8>,
    #[cfg_attr(feature = "cbor", n(2))]
    pub height: u64,
    #[cfg_attr(feature = "cbor", n(3))]
    pub timestamp: u64,
}

/// Mirrors `query::TxoRef`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct TxoRef {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_array"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub hash: TxHash,
    #[cfg_attr(feature = "cbor", n(1))]
    pub index: u16,
}

/// Renders the server-reconstructed `pallas_addresses::Address` as
/// bech32, where gRPC's own field is raw `bytes`. Encode-only --
/// `Debug`/`Clone` only, no `Eq`.
#[derive(Debug, Clone)]
pub struct Address(pub PallasAddress);

#[cfg(feature = "json")]
impl Serialize for Address {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_bech32().map_err(serde::ser::Error::custom)?)
    }
}

#[cfg(feature = "json")]
impl<'de> Deserialize<'de> for Address {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(Address(s.parse().map_err(serde::de::Error::custom)?))
    }
}

#[cfg(feature = "cbor")]
impl<Ctx> Encode<Ctx> for Address {
    fn encode<W: CborWrite>(
        &self,
        e: &mut Encoder<W>,
        _ctx: &mut Ctx,
    ) -> Result<(), EncodeError<W::Error>> {
        let s = self.0.to_bech32().map_err(EncodeError::message)?;
        e.str(&s)?;
        Ok(())
    }
}

#[cfg(feature = "cbor")]
impl<'b, Ctx> Decode<'b, Ctx> for Address {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut Ctx) -> Result<Self, DecodeError> {
        let addr = d.str()?.parse().map_err(DecodeError::message)?;
        Ok(Address(addr))
    }
}

/// `pallas_addresses::Address` has no `JsonSchema` impl of its own, so
/// this mirrors the `Serialize` impl above by hand: the wire shape is
/// a bech32 string, same as `String`'s own schema.
#[cfg(feature = "schema")]
impl JsonSchema for Address {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Address".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

/// `query.AnyUtxoData`'s own fields, around a `cardano::TxOutput`
/// where gRPC has a `oneof parsed_state { cardano: TxOutput }` (no
/// chain to pick between, so no oneof needed -- see module doc).
/// `block_ref` is the creation point, zero-valued when unknown (see
/// `ChainPoint`). `spent_by` is `None` iff confirmedly unspent -- the
/// spending tx's own point is a `ReadTx` away, same design as gRPC's
/// `AnyUtxoData.spent_by`; no separate spent-slot field for that
/// reason.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct Utxo {
    #[cfg_attr(feature = "cbor", n(0))]
    pub txo_ref: TxoRef,
    #[cfg_attr(feature = "cbor", n(1))]
    pub output: TxOutput,
    #[cfg_attr(feature = "cbor", n(2))]
    pub block_ref: ChainPoint,
    #[cfg_attr(
        feature = "json",
        serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::ser_hex_opt",
            deserialize_with = "crate::de_hex_array_opt",
            default
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    #[cfg_attr(feature = "cbor", n(3), cbor(with = "minicbor::bytes"))]
    pub spent_by: Option<TxHash>,
}

/// `ReadUtxos`'s own route: an exact key lookup, never paginated --
/// see `ReadUtxosRequest`/`ReadUtxosResponse`.
pub const READ_UTXO_PATH: &str = "/query/read-utxos";

/// Mirrors `query.ReadUtxosRequest`, minus `field_mask` (see module
/// doc). `keys` is the exact name gRPC uses here too.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ReadUtxosRequest {
    #[cfg_attr(feature = "json", serde(default))]
    #[cfg_attr(feature = "cbor", n(0))]
    pub keys: Vec<TxoRef>,
}

/// `next_token` is an opaque `<slot>:<tx_hash_hex>:<ix>` cursor
/// (`scheme::http::query`'s `format_cursor`/`parse_cursor` shape),
/// round-tripped through `SearchUtxosRequest::start_token`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ReadUtxosResponse {
    #[cfg_attr(feature = "cbor", n(0))]
    pub items: Vec<Utxo>,
    #[cfg_attr(feature = "cbor", n(1))]
    pub ledger_tip: ChainPoint,
}

/// `SearchUtxos`'s own route -- see `SearchUtxosRequest`/
/// `SearchUtxosResponse`.
pub const SEARCH_UTXOS_PATH: &str = "/query/search-utxos";

/// Mirrors `query.SearchUtxosRequest`, minus `field_mask` and
/// `predicate` (flattened to `pay_hash`/`stake_hash` -- see module
/// doc) and `since` (flattened to `since_slot`/`since_hash`, same
/// floe addition `SearchUtxosResponse`'s own doc on gRPC's side
/// describes). `start_token`/`max_items` keep gRPC's exact names.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct SearchUtxosRequest {
    #[cfg_attr(
        feature = "json",
        serde(
            default,
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::ser_hex_opt",
            deserialize_with = "crate::de_hex_array_opt"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub pay_hash: Option<[u8; 28]>,
    #[cfg_attr(
        feature = "json",
        serde(
            default,
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::ser_hex_opt",
            deserialize_with = "crate::de_hex_array_opt"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub stake_hash: Option<[u8; 28]>,
    #[cfg_attr(
        feature = "json",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "cbor", n(2))]
    pub since_slot: Option<u64>,
    #[cfg_attr(
        feature = "json",
        serde(
            default,
            skip_serializing_if = "Option::is_none",
            serialize_with = "crate::ser_hex_opt",
            deserialize_with = "crate::de_hex_array_opt"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "Option<String>"))]
    #[cfg_attr(feature = "cbor", n(3), cbor(with = "minicbor::bytes"))]
    pub since_hash: Option<TxHash>,
    #[cfg_attr(
        feature = "json",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "cbor", n(4))]
    pub start_token: Option<String>,
    #[cfg_attr(
        feature = "json",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    #[cfg_attr(feature = "cbor", n(5))]
    pub max_items: Option<i32>,
}

/// Mirrors `query.SearchUtxosResponse`. `next_token` is absent once
/// exhausted -- `ReadUtxosResponse` has no equivalent field at all
/// (exact-key lookup, never paginated), which is why these are two
/// distinct types rather than one shared "page" shape.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct SearchUtxosResponse {
    #[cfg_attr(feature = "cbor", n(0))]
    pub items: Vec<Utxo>,
    #[cfg_attr(feature = "cbor", n(1))]
    pub ledger_tip: ChainPoint,
    #[cfg_attr(
        feature = "json",
        serde(skip_serializing_if = "Option::is_none", default)
    )]
    #[cfg_attr(feature = "cbor", n(2))]
    pub next_token: Option<String>,
}

/// Mirrors `query.AnyChainDatum`, minus `parsed_state` (parsed
/// `PlutusData`, unsupported -- same gap as `Datum.payload`).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct AnyChainDatum {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_vec"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub native_bytes: Vec<u8>,
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_array"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub key: [u8; 32],
}

/// `ReadData`'s own route -- see `ReadDataRequest`/`ReadDataResponse`.
pub const READ_DATA_PATH: &str = "/query/read-data";

/// Mirrors `query.ReadDataRequest`, minus `field_mask`. `key`, not
/// `hash` -- that's gRPC's own naming (`ReadTxRequest` below calls its
/// equivalent field `hash`; the asymmetry is the proto's, not
/// introduced here).
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ReadDataRequest {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_array"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub key: TxHash,
}

/// Mirrors `query.ReadDataResponse`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ReadDataResponse {
    #[cfg_attr(feature = "cbor", n(0))]
    pub value: AnyChainDatum,
    #[cfg_attr(feature = "cbor", n(1))]
    pub ledger_tip: ChainPoint,
}

/// `query.AnyChainTx`'s own fields, around a `cardano.Tx` (no `oneof
/// chain`, same reasoning as `Utxo`). `successful` is hardcoded `true`
/// -- a real tracked gap (no `valid` column on `tx`), carried over
/// unchanged rather than hidden.
///
/// `inputs`/`outputs` are `cardano::TxInput`/`cardano::TxOutput`
/// directly, matching gRPC's own `cardano.Tx` exactly -- neither type
/// has a `block_ref`/`spent_by` field at all, so (unlike an earlier
/// version of this type) there's no leftover "doesn't apply here"
/// field for `ReadTx` to leave unset.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct Tx {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_array"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub hash: TxHash,
    #[cfg_attr(feature = "cbor", n(1))]
    pub block_ref: ChainPoint,
    #[cfg_attr(feature = "cbor", n(2))]
    pub inputs: Vec<TxInput>,
    #[cfg_attr(feature = "cbor", n(3))]
    pub outputs: Vec<TxOutput>,
    #[cfg_attr(feature = "cbor", n(4))]
    pub successful: bool,
}

/// `ReadTx`'s own route -- see `ReadTxRequest`/`ReadTxResponse`.
pub const READ_TX_PATH: &str = "/query/read-tx";

/// Mirrors `query.ReadTxRequest`, minus `field_mask`.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ReadTxRequest {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_array"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub hash: TxHash,
}

/// Mirrors `query.ReadTxResponse`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ReadTxResponse {
    #[cfg_attr(feature = "cbor", n(0))]
    pub tx: Tx,
    #[cfg_attr(feature = "cbor", n(1))]
    pub ledger_tip: ChainPoint,
}
