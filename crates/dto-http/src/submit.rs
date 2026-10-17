//! Mirrors `floe_dto_grpc::floe::v1::submit`. floe only ever implements
//! `SubmitTx` (see `submit.proto`'s own doc on why) -- `SubmitTxResponse`
//! is the only message that needs an equivalent here.

#[cfg(feature = "cbor")]
use minicbor::{Decode, Encode};
#[cfg(feature = "schema")]
use schemars::JsonSchema;
#[cfg(feature = "json")]
use serde::{Deserialize, Serialize};

/// `SubmitTx`'s own route.
pub const SUBMIT_TX_PATH: &str = "/submit/submit-tx";

/// Mirrors `submit.SubmitTxResponse`.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "json", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "schema", derive(JsonSchema))]
#[cfg_attr(feature = "cbor", derive(Encode, Decode))]
pub struct SubmitTxResponse {
    #[cfg_attr(
        feature = "json",
        serde(
            serialize_with = "crate::ser_hex",
            deserialize_with = "crate::de_hex_vec"
        )
    )]
    #[cfg_attr(feature = "schema", schemars(with = "String"))]
    #[cfg_attr(feature = "cbor", n(0), cbor(with = "minicbor::bytes"))]
    pub r#ref: Vec<u8>,
}
