//! floe-dto-http: the wire schema `floe-server`'s http transport
//! (`http.rs`) encodes and `floe-client` decodes.
//!
//! Namespaced like `floe-dto-grpc` (`query`/`submit`/`health`, mirroring
//! that crate's `floe::v1::{query,submit,health}`) -- a type's module
//! says what it is. Names and field layout follow `floe-dto-grpc`'s
//! generated types one-for-one wherever floe supports the equivalent
//! gRPC message; `query::Utxo` inlines what gRPC splits across
//! `AnyUtxoData` + `cardano::TxOutput`, since there's only one chain to
//! pick between. Requests travel as query params/path segments, so
//! gRPC's predicate/field-mask/`ReadParams`/`ReadGenesis`/`ReadState`
//! messages have no counterpart here at all.
//!
//! `query::Script` stays `{lang, cbor}` raw rather than recursively
//! decoding `NativeScript`, matching `cardano.Script`'s bytes without
//! its UTxORPC-spec tree structure.
//!
//! `json`/`cbor` feature flags gate independent dependencies (serde vs.
//! minicbor), so a format-specific consumer pulls only what it needs.
//! Byte fields are hex text under `json`, raw CBOR byte strings
//! (`#[cbor(with = "minicbor::bytes")]`) under `cbor`.

#[cfg(feature = "json")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub mod cardano;
#[cfg(feature = "metrics")]
pub mod health;
pub mod query;
pub mod submit;

pub type TxHash = [u8; 32];

#[cfg(feature = "json")]
pub(crate) fn ser_hex<S: Serializer, T: AsRef<[u8]>>(bytes: &T, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&hex::encode(bytes.as_ref()))
}

#[cfg(feature = "json")]
pub(crate) fn de_hex_array<'de, D: Deserializer<'de>, const N: usize>(
    d: D,
) -> Result<[u8; N], D::Error> {
    let s = String::deserialize(d)?;
    let v = hex::decode(&s).map_err(serde::de::Error::custom)?;
    v.try_into().map_err(|v: Vec<u8>| {
        serde::de::Error::custom(format!("expected {N} bytes, got {}", v.len()))
    })
}

#[cfg(feature = "json")]
pub(crate) fn de_hex_vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    let s = String::deserialize(d)?;
    hex::decode(&s).map_err(serde::de::Error::custom)
}

#[cfg(feature = "json")]
pub(crate) fn ser_hex_opt<S: Serializer, T: AsRef<[u8]>>(
    bytes: &Option<T>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match bytes {
        Some(b) => s.serialize_some(&hex::encode(b.as_ref())),
        None => s.serialize_none(),
    }
}

#[cfg(feature = "json")]
pub(crate) fn de_hex_vec_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
    let opt: Option<String> = Option::deserialize(d)?;
    opt.map(|s| hex::decode(&s).map_err(serde::de::Error::custom))
        .transpose()
}

#[cfg(feature = "json")]
pub(crate) fn de_hex_array_opt<'de, D: Deserializer<'de>, const N: usize>(
    d: D,
) -> Result<Option<[u8; N]>, D::Error> {
    let opt: Option<String> = Option::deserialize(d)?;
    opt.map(|s| {
        let v = hex::decode(&s).map_err(serde::de::Error::custom)?;
        v.try_into().map_err(|v: Vec<u8>| {
            serde::de::Error::custom(format!("expected {N} bytes, got {}", v.len()))
        })
    })
    .transpose()
}

/// Like `ser_hex`, for a list of byte strings (e.g.
/// `query::Tx.required_signers`) -- each element its own hex string,
/// not one hex string over the whole list.
#[cfg(feature = "json")]
pub(crate) fn ser_hex_list<S: Serializer, T: AsRef<[u8]>>(
    items: &[T],
    s: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeSeq;
    let mut seq = s.serialize_seq(Some(items.len()))?;
    for item in items {
        seq.serialize_element(&hex::encode(item.as_ref()))?;
    }
    seq.end()
}

#[cfg(feature = "json")]
pub(crate) fn de_hex_vec_list<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Vec<u8>>, D::Error> {
    let items: Vec<String> = Vec::deserialize(d)?;
    items
        .into_iter()
        .map(|s| hex::decode(&s).map_err(serde::de::Error::custom))
        .collect()
}

/// gRPC carries its errors as `tonic::Status` instead.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "cbor", derive(minicbor::Encode, minicbor::Decode))]
pub struct ErrorBody {
    #[cfg_attr(feature = "cbor", n(0))]
    pub error: String,
}
