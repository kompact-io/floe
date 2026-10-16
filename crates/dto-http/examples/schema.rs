//! Dumps machine-readable JSON Schema for every public REST/JSON DTO
//! in this crate, keyed by type name. Published to the docs site as
//! `schema.json` by the Docs CI workflow -- generated straight from
//! these structs/enums via `schemars`, not a hand-maintained spec, so
//! it fails to compile (and CI fails loudly) the moment a DTO drifts
//! from what `#[derive(JsonSchema)]` can see.
//!
//! Requires both `schema` (the derive) and `metrics` (`HealthDto` and
//! friends, otherwise not part of this crate's public API) features.

use floe_dto_http::{
    AddressDto, AssetDto, ChainPointDto, ChangesDto, ChangesResponseDto, DatumDto,
    DatumResponseDto, ErrorBody, HealthDto, HealthResponseDto, MetricsDto, MultiassetDto, PageDto,
    ReplicaHealthDto, RequestCount, ScriptDto, ScriptLang, StaleDto, StaleReasonDto,
    SubmitTxResponseDto, TxDto, TxResponseDto, UtxoDto,
};
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

    entry!(types, ChainPointDto);
    entry!(types, AssetDto);
    entry!(types, MultiassetDto);
    entry!(types, ScriptLang);
    entry!(types, ScriptDto);
    entry!(types, AddressDto);
    entry!(types, DatumDto);
    entry!(types, UtxoDto);
    entry!(types, PageDto);
    entry!(types, TxDto);
    entry!(types, TxResponseDto);
    entry!(types, DatumResponseDto);
    entry!(types, StaleReasonDto);
    entry!(types, StaleDto);
    entry!(types, ChangesDto);
    entry!(types, ChangesResponseDto);
    entry!(types, SubmitTxResponseDto);
    entry!(types, ErrorBody);
    entry!(types, HealthDto);
    entry!(types, ReplicaHealthDto);
    entry!(types, HealthResponseDto);
    entry!(types, RequestCount);
    entry!(types, MetricsDto);

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::Value::Object(types)).unwrap()
    );
}
