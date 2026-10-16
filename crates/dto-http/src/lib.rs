//! floe-dto-http: the wire schema `floe-server`'s REST transport
//! (`http.rs`) encodes and `floe-client` decodes. Rewritten from the
//! pre-gRPC `UtxoJson`/`Page`/`DatumJson`/`ScriptJson`/`AddressJson`,
//! which had drifted from gRPC's current shape (no `block_ref`,
//! wrong `spent_by`, no `ledger_tip`) -- see ../../plan-dto-http.md.
//! Each DTO here mirrors `grpc.rs`'s `any_utxo_data`/`any_chain_tx`
//! field for field, except `ScriptDto`, which stays `{lang, cbor}`
//! raw rather than recursively decoding `NativeScript` -- that tree
//! is UTxORPC-spec fidelity gRPC owes upstream, not something REST
//! needs to match. Assets *do* get the structured
//! `Vec<MultiassetDto>` treatment (a flat decode, see
//! `decode_assets` in `grpc.rs`, same source bytes).
//!
//! `json`/`cbor` feature flags gate independent dependencies (serde
//! vs. minicbor), so a format-specific consumer pulls only what it
//! needs. Byte fields are hex text under `json`, raw CBOR byte
//! strings (`#[cbor(with = "minicbor::bytes")]`) under `cbor`.
//! `AddressDto` renders a server-reconstructed
//! `pallas_addresses::Address` as bech32 on both wire formats --
//! encode-only, so no hex fallback to model.
//!
//! Request parsing (`Accept` negotiation, query params, pagination)
//! stays in `floe-server`'s `http.rs`, which builds these from
//! `store::query`'s `Resolved*` types.

#[cfg(feature = "cbor")]
use minicbor::decode::{Decoder, Error as DecodeError};
#[cfg(feature = "cbor")]
use minicbor::encode::{Encoder, Error as EncodeError, Write as CborWrite};
#[cfg(feature = "cbor")]
use minicbor::{Decode, Encode};
use pallas_addresses::Address;
#[cfg(feature = "json")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[cfg(feature = "metrics")]
mod metrics;
#[cfg(feature = "metrics")]
pub use metrics::{
    HealthDto, HealthResponseDto, MetricsDto, Readiness, ReplicaHealthDto, RequestCount, Scheme,
    Stage,
};

pub type TxHash = [u8; 32];

#[cfg(feature = "json")]
fn ser_hex<S: Serializer, T: AsRef<[u8]>>(bytes: &T, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&hex::encode(bytes.as_ref()))
}

#[cfg(feature = "json")]
fn de_hex_array<'de, D: Deserializer<'de>, const N: usize>(d: D) -> Result<[u8; N], D::Error> {
    let s = String::deserialize(d)?;
    let v = hex::decode(&s).map_err(serde::de::Error::custom)?;
    v.try_into().map_err(|v: Vec<u8>| {
        serde::de::Error::custom(format!("expected {N} bytes, got {}", v.len()))
    })
}

#[cfg(feature = "json")]
fn de_hex_vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    let s = String::deserialize(d)?;
    hex::decode(&s).map_err(serde::de::Error::custom)
}

#[cfg(feature = "json")]
fn ser_hex_opt<S: Serializer, T: AsRef<[u8]>>(bytes: &Option<T>, s: S) -> Result<S::Ok, S::Error> {
    match bytes {
        Some(b) => s.serialize_some(&hex::encode(b.as_ref())),
        None => s.serialize_none(),
    }
}

#[cfg(feature = "json")]
fn de_hex_vec_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
    let opt: Option<String> = Option::deserialize(d)?;
    opt.map(|s| hex::decode(&s).map_err(serde::de::Error::custom))
        .transpose()
}

#[cfg(feature = "json")]
fn de_hex_array_opt<'de, D: Deserializer<'de>, const N: usize>(
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

/// The discriminant is also the blake2b-224 `lang_tag` byte prepended
/// to script bytes before hashing -- a real Cardano protocol constant.
/// `floe-server`'s `decode.rs` re-exports this type so both crates
/// share one discriminant. `Debug`/`Clone`/`Copy`/`Eq` stay
/// unconditional since `store::query::ResolvedScript` uses this type
/// regardless of which wire-format features are enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
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
    /// `floe-dto-grpc`'s own `ScriptLang` (added there for UTxORPC's
    /// `plutus_v4`), not because floe has ever indexed one.
    #[cfg_attr(feature = "cbor", n(4))]
    PlutusV4 = 4,
}

/// Mirrors `q::ChainPoint`: zero-valued (slot 0, empty hash, height 0,
/// timestamp 0) is the "unknown" sentinel `grpc.rs`'s
/// `chain_point(None)` encodes -- a pruned or seed-anchor-predating
/// origin block. `timestamp` is always 0 (deriving a real one needs
/// genesis/era data floe doesn't track, see `TODO.md`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ChainPointDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub slot: u64,
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_vec")
    )]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub hash: Vec<u8>,
    #[cfg_attr(feature = "cbor", n(2))]
    pub height: u64,
    #[cfg_attr(feature = "cbor", n(3))]
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct AssetDto {
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_vec")
    )]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub name: Vec<u8>,
    /// A live output's held quantity (`PositiveCoin`, same as
    /// `grpc.rs`'s `decode_assets`), never a mint/burn delta -- always
    /// non-negative.
    #[cfg_attr(feature = "cbor", n(1))]
    pub quantity: u64,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct MultiassetDto {
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_vec")
    )]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub policy_id: Vec<u8>,
    #[cfg_attr(feature = "cbor", n(1))]
    pub assets: Vec<AssetDto>,
}

/// Mirrors `grpc.rs`'s `datum()`: a `None`-kind datum is the
/// *containing* `Option<DatumDto>` being `None`, not a variant here --
/// a `DatumDto` always has a hash. `original_cbor` is `None` for an
/// unresolved hash-kind datum, `Some` once resolved (inline or
/// witnessed) -- mirrors `ResolvedDatum::Hash` vs.
/// `HashResolved`/`Inline`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct DatumDto {
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_array")
    )]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub hash: [u8; 32],
    #[cfg_attr(
        feature = "json",
        serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "ser_hex_opt",
            deserialize_with = "de_hex_vec_opt",
            default
        )
    )]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub original_cbor: Option<Vec<u8>>,
}

/// `cbor` is raw bytes regardless of `lang` -- no recursive
/// `NativeScript` decode (see module doc); lossless either way.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ScriptDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub lang: ScriptLang,
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_vec")
    )]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub cbor: Vec<u8>,
}

/// A server-reconstructed `pallas_addresses::Address`, rendered bech32
/// on both wire formats. Encode-only -- `Debug`/`Clone` only, no `Eq`.
#[derive(Debug, Clone)]
pub struct AddressDto(pub Address);

#[cfg(feature = "json")]
impl Serialize for AddressDto {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_bech32().map_err(serde::ser::Error::custom)?)
    }
}

#[cfg(feature = "json")]
impl<'de> Deserialize<'de> for AddressDto {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Ok(AddressDto(s.parse().map_err(serde::de::Error::custom)?))
    }
}

#[cfg(feature = "cbor")]
impl<Ctx> Encode<Ctx> for AddressDto {
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
impl<'b, Ctx> Decode<'b, Ctx> for AddressDto {
    fn decode(d: &mut Decoder<'b>, _ctx: &mut Ctx) -> Result<Self, DecodeError> {
        let addr = d.str()?.parse().map_err(DecodeError::message)?;
        Ok(AddressDto(addr))
    }
}

/// Mirrors `q::AnyUtxoData`'s populated fields. `block` is the
/// creation point, zero-valued when unknown (see `ChainPointDto`).
/// `spent_by` is `None` iff confirmedly unspent -- the spending tx's
/// own point is a `ReadTx` away, same design as `query.proto`'s
/// `AnyUtxoData.spent_by`; no separate spent-slot field for that
/// reason.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct UtxoDto {
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_array")
    )]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub tx_hash: TxHash,
    #[cfg_attr(feature = "cbor", n(1))]
    pub ix: u16,
    #[cfg_attr(feature = "cbor", n(2))]
    pub block: ChainPointDto,
    #[cfg_attr(
        feature = "json",
        serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "ser_hex_opt",
            deserialize_with = "de_hex_array_opt",
            default
        )
    )]
    #[cfg_attr(feature = "cbor", n(3), cbor(with = "minicbor::bytes"))]
    pub spent_by: Option<TxHash>,
    #[cfg_attr(feature = "cbor", n(4))]
    pub address: AddressDto,
    #[cfg_attr(feature = "cbor", n(5))]
    pub coin: u64,
    #[cfg_attr(feature = "cbor", n(6))]
    pub assets: Vec<MultiassetDto>,
    #[cfg_attr(feature = "cbor", n(7))]
    pub datum: Option<DatumDto>,
    #[cfg_attr(feature = "cbor", n(8))]
    pub script: Option<ScriptDto>,
}

/// `next` is an opaque `<tx_hash_hex>:<ix>` cursor (`grpc.rs`'s
/// `format_cursor`/`parse_cursor` shape), round-tripped through the
/// next request's query param. `None` for `ReadUtxos` (not
/// paginated); for `SearchUtxos`, absent once exhausted.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct PageDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub items: Vec<UtxoDto>,
    #[cfg_attr(feature = "cbor", n(1))]
    pub ledger_tip: ChainPointDto,
    #[cfg_attr(
        feature = "json",
        serde(skip_serializing_if = "Option::is_none", default)
    )]
    #[cfg_attr(feature = "cbor", n(2))]
    pub next: Option<String>,
}

/// Mirrors `q::AnyChainTx`'s populated Cardano fields: in/outputs
/// only. `successful` is hardcoded `true` -- a real tracked gap (no
/// `valid` column on `tx`, see `TODO.md`), carried over unchanged
/// rather than hidden.
///
/// `inputs`/`outputs` are each a `UtxoDto`, but a deliberately leaner
/// one than `ReadUtxos`/`SearchUtxos`/`UtxoChanges` return: `block` is
/// always the zero-valued "unknown" sentinel and `spent_by` is always
/// `None` (which here does *not* mean "confirmedly unspent" the way it
/// does everywhere else `UtxoDto` is used -- just "`ReadTx` doesn't
/// say"). Both servers' `ReadTx` implementations report this
/// identically, by design: gRPC's wire shape (`cardano.proto`'s
/// `TxOutput`/`TxInput`) has no field for either one, so matching that
/// here keeps `ReadTx`'s content the same across schemes rather than
/// letting REST say more just because its own domain layer happens to
/// have the data on hand.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct TxDto {
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_array")
    )]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub hash: TxHash,
    #[cfg_attr(feature = "cbor", n(1))]
    pub block: ChainPointDto,
    #[cfg_attr(feature = "cbor", n(2))]
    pub inputs: Vec<UtxoDto>,
    #[cfg_attr(feature = "cbor", n(3))]
    pub outputs: Vec<UtxoDto>,
    #[cfg_attr(feature = "cbor", n(4))]
    pub successful: bool,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct TxResponseDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub tx: TxDto,
    #[cfg_attr(feature = "cbor", n(1))]
    pub ledger_tip: ChainPointDto,
}

/// Mirrors `q::AnyChainDatum`: raw `native_bytes` only, no parsed
/// `PlutusData`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct DatumResponseDto {
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_array")
    )]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub key: [u8; 32],
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_vec")
    )]
    #[cfg_attr(feature = "cbor", n(1), cbor(with = "minicbor::bytes"))]
    pub native_bytes: Vec<u8>,
    #[cfg_attr(feature = "cbor", n(2))]
    pub ledger_tip: ChainPointDto,
}

/// Mirrors `q::Changes`: `created`/`spent` each paginate
/// independently via their own cursor (`PageDto::next`'s shape).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ChangesDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub created: Vec<UtxoDto>,
    #[cfg_attr(feature = "cbor", n(1))]
    pub spent: Vec<UtxoDto>,
    #[cfg_attr(feature = "cbor", n(2))]
    pub ledger_tip: ChainPointDto,
    #[cfg_attr(
        feature = "json",
        serde(skip_serializing_if = "Option::is_none", default)
    )]
    #[cfg_attr(feature = "cbor", n(3))]
    pub created_next: Option<String>,
    #[cfg_attr(
        feature = "json",
        serde(skip_serializing_if = "Option::is_none", default)
    )]
    #[cfg_attr(feature = "cbor", n(4))]
    pub spent_next: Option<String>,
}

/// Mirrors `query::StaleReason`'s three cases as a flat tag;
/// `RolledBack`/`TooOld`'s `oldest_retained_slot` moves to the
/// containing `StaleDto`, matching `q::Stale`'s shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
#[cfg_attr(feature = "json", serde(rename_all = "snake_case"))]
#[cfg_attr(feature = "cbor", cbor(index_only))]
pub enum StaleReasonDto {
    #[cfg_attr(feature = "cbor", n(0))]
    Unknown,
    #[cfg_attr(feature = "cbor", n(1))]
    RolledBack,
    #[cfg_attr(feature = "cbor", n(2))]
    TooOld,
}

/// Why `since` didn't check out -- see `UtxoChangesRequest`'s doc
/// comment for the strict/permissive/none semantics behind this.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct StaleDto {
    #[cfg_attr(feature = "cbor", n(0))]
    pub reason: StaleReasonDto,
    #[cfg_attr(
        feature = "json",
        serde(skip_serializing_if = "Option::is_none", default)
    )]
    #[cfg_attr(feature = "cbor", n(1))]
    pub oldest_retained_slot: Option<u64>,
}

/// Mirrors `q::UtxoChangesResponse`'s oneof. JSON is an
/// externally-tagged enum (`{"changes": {...}}`/`{"stale": {...}}`);
/// `#[cbor(flat)]` puts the tag and its single field in one array
/// instead of nesting a second one.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
#[cfg_attr(feature = "cbor", cbor(flat))]
pub enum ChangesResponseDto {
    #[cfg_attr(feature = "cbor", n(0))]
    Changes(#[cfg_attr(feature = "cbor", n(0))] ChangesDto),
    #[cfg_attr(feature = "cbor", n(1))]
    Stale(#[cfg_attr(feature = "cbor", n(0))] StaleDto),
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct SubmitTxResponseDto {
    #[cfg_attr(
        feature = "json",
        serde(serialize_with = "ser_hex", deserialize_with = "de_hex_vec")
    )]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub r#ref: Vec<u8>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct ErrorBody {
    #[cfg_attr(feature = "cbor", n(0))]
    pub error: String,
}
