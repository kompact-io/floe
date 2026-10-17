//! Dumps machine-readable JSON Schema for every public JSON type in
//! this crate, keyed by type name. Published to the docs site as
//! `schema.json` by the Docs CI workflow, generated straight from
//! these structs/enums via `schemars` -- a type drifting from what
//! `#[derive(JsonSchema)]` can see fails this example's own compile,
//! and CI with it.
//!
//! Requires both `schema` (the derive) and `metrics` (gates
//! `health::RunHealth` and friends) features.

use floe_dto_http::ErrorBody;
use floe_dto_http::cardano::{Asset, Datum, Multiasset, Script, ScriptLang, TxInput, TxOutput};
use floe_dto_http::health::{HealthResponse, MetricsResponse, RequestCount, RunHealth};
use floe_dto_http::query::{
    Address, AnyChainDatum, ChainPoint, ReadDataRequest, ReadDataResponse, ReadTxRequest,
    ReadTxResponse, ReadUtxosRequest, ReadUtxosResponse, SearchUtxosRequest, SearchUtxosResponse,
    Tx, TxoRef, Utxo,
};
use floe_dto_http::submit::SubmitTxResponse;
use schemars::schema_for;

macro_rules! entry {
    ($map:expr, $ty:ty) => {
        $map.insert(
            stringify!($ty).to_string(),
            serde_json::to_value(schema_for!($ty)).expect("Schema is always representable as JSON"),
        );
    };
}

fn main() {
    let mut types = serde_json::Map::new();

    entry!(types, ChainPoint);
    entry!(types, TxoRef);
    entry!(types, Asset);
    entry!(types, Multiasset);
    entry!(types, ScriptLang);
    entry!(types, Script);
    entry!(types, Address);
    entry!(types, Datum);
    entry!(types, TxOutput);
    entry!(types, TxInput);
    entry!(types, Utxo);
    entry!(types, ReadUtxosRequest);
    entry!(types, ReadUtxosResponse);
    entry!(types, SearchUtxosRequest);
    entry!(types, SearchUtxosResponse);
    entry!(types, Tx);
    entry!(types, ReadTxRequest);
    entry!(types, ReadTxResponse);
    entry!(types, AnyChainDatum);
    entry!(types, ReadDataRequest);
    entry!(types, ReadDataResponse);
    entry!(types, SubmitTxResponse);
    entry!(types, ErrorBody);
    entry!(types, RunHealth);
    entry!(types, HealthResponse);
    entry!(types, RequestCount);
    entry!(types, MetricsResponse);

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::Value::Object(types)).unwrap()
    );
}
