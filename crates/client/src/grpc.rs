//! grpc: the gRPC scheme -- `Client` wraps both `QueryServiceClient`
//! and `SubmitServiceClient` (`floe-server` always serves both on the
//! same address, so `connect` eagerly connects both rather than
//! lazily). `impl Backend` is the one place with genuinely new logic
//! in this crate: converting `floe-dto-grpc`'s prost types into the
//! same `floe-dto-http` DTOs every scheme returns (see
//! `../../plan-floe-client-as-lib.md`'s "A `Backend` trait is the
//! shared contract").

use anyhow::Result;
use tonic::transport::Channel;

use floe_dto_grpc::floe::v1::cardano as c;
use floe_dto_grpc::floe::v1::query as q;
use floe_dto_grpc::floe::v1::query::query_service_client::QueryServiceClient;
use floe_dto_grpc::floe::v1::submit::submit_service_client::SubmitServiceClient;
use floe_dto_grpc::floe::v1::submit::{AnyChainTx, SubmitTxRequest, any_chain_tx};
use floe_dto_http::{
    AddressDto, AssetDto, ChainPointDto, ChangesDto, ChangesResponseDto, DatumDto,
    DatumResponseDto, MultiassetDto, PageDto, ScriptDto, StaleDto, StaleReasonDto,
    SubmitTxResponseDto, TxDto, TxResponseDto, UtxoDto,
};

use crate::common::{Backend, ChangesParams, Cred, Paging, TxHash};

pub struct Client {
    query: QueryServiceClient<Channel>,
    submit: SubmitServiceClient<Channel>,
}

impl Client {
    pub async fn connect(server: &str) -> Result<Self> {
        let channel = Channel::from_shared(server.to_string())?.connect().await?;
        Ok(Self {
            query: QueryServiceClient::new(channel.clone()),
            submit: SubmitServiceClient::new(channel),
        })
    }
}

// --- proto -> DTO conversion -------------------------------------------
//
// Mirrors `floe-server`'s own `any_utxo_data`/`any_chain_tx` field for
// field, in reverse -- same source shapes, opposite direction.

fn chain_point_dto(p: Option<q::ChainPoint>) -> ChainPointDto {
    match p {
        None => ChainPointDto::default(),
        Some(p) => ChainPointDto {
            slot: p.slot,
            hash: p.hash,
            height: p.height,
            timestamp: p.timestamp,
        },
    }
}

fn big_int_u64(b: Option<c::BigInt>) -> u64 {
    match b.and_then(|b| b.big_int) {
        Some(c::big_int::BigInt::Int(i)) => i as u64,
        Some(c::big_int::BigInt::BigUInt(bytes)) => {
            let mut buf = [0u8; 8];
            let n = bytes.len().min(8);
            buf[8 - n..].copy_from_slice(&bytes[bytes.len() - n..]);
            u64::from_be_bytes(buf)
        }
        _ => 0,
    }
}

fn assets_dto(assets: Vec<c::Multiasset>) -> Vec<MultiassetDto> {
    assets
        .into_iter()
        .map(|ma| MultiassetDto {
            policy_id: ma.policy_id,
            assets: ma
                .assets
                .into_iter()
                .map(|a| AssetDto {
                    name: a.name,
                    quantity: big_int_u64(a.quantity),
                })
                .collect(),
        })
        .collect()
}

fn datum_dto(d: Option<c::Datum>) -> Result<Option<DatumDto>> {
    d.map(|d| -> Result<DatumDto> {
        Ok(DatumDto {
            hash: d
                .hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("Datum.hash must be 32 bytes"))?,
            original_cbor: d.original_cbor,
        })
    })
    .transpose()
}

/// Plutus scripts carry their raw bytes on the wire (`bytes
/// plutus_v{1,2,3,4}`), round-tripped losslessly into `ScriptDto.cbor`.
/// A native script does not: `floe-server`'s `script()` sends the
/// *structured* `NativeScript` tree, not the raw CBOR that produced it
/// (see `floe-dto-grpc`'s `cardano.proto`), and re-encoding that tree
/// wouldn't reproduce the original bytes anyway (a different CBOR
/// encoding is an equally valid encoding of the same tree, but not a
/// byte-identical one) -- `ScriptDto` requires raw `cbor`, so a native
/// script's own `ReadTx`/`ReadUtxos`/`SearchUtxos` result through this
/// scheme carries `script: None` instead. `--transport http` doesn't
/// have this gap (the server's REST routes send raw script bytes
/// regardless of lang).
fn script_dto(s: Option<c::Script>) -> Option<ScriptDto> {
    match s?.script? {
        c::script::Script::Native(_) => None,
        c::script::Script::PlutusV1(cbor) => Some(ScriptDto {
            lang: floe_dto_http::ScriptLang::PlutusV1,
            cbor,
        }),
        c::script::Script::PlutusV2(cbor) => Some(ScriptDto {
            lang: floe_dto_http::ScriptLang::PlutusV2,
            cbor,
        }),
        c::script::Script::PlutusV3(cbor) => Some(ScriptDto {
            lang: floe_dto_http::ScriptLang::PlutusV3,
            cbor,
        }),
        c::script::Script::PlutusV4(cbor) => Some(ScriptDto {
            lang: floe_dto_http::ScriptLang::PlutusV4,
            cbor,
        }),
    }
}

fn address_dto(bytes: &[u8]) -> Result<AddressDto> {
    Ok(AddressDto(pallas_addresses::Address::from_bytes(bytes)?))
}

fn utxo_dto(item: q::AnyUtxoData) -> Result<UtxoDto> {
    let txo_ref = item
        .txo_ref
        .ok_or_else(|| anyhow::anyhow!("AnyUtxoData has no txo_ref"))?;
    let tx_hash: TxHash = txo_ref
        .hash
        .try_into()
        .map_err(|_| anyhow::anyhow!("TxoRef.hash must be 32 bytes"))?;
    let q::any_utxo_data::ParsedState::Cardano(out) = item
        .parsed_state
        .ok_or_else(|| anyhow::anyhow!("AnyUtxoData has no parsed_state"))?;
    let spent_by = item
        .spent_by
        .map(|h| -> Result<TxHash> {
            h.try_into()
                .map_err(|_| anyhow::anyhow!("spent_by must be 32 bytes"))
        })
        .transpose()?;
    Ok(UtxoDto {
        tx_hash,
        ix: txo_ref.index as u16,
        block: chain_point_dto(item.block_ref),
        spent_by,
        address: address_dto(&out.address)?,
        coin: big_int_u64(out.coin),
        assets: assets_dto(out.assets),
        datum: datum_dto(out.datum)?,
        script: script_dto(out.script),
    })
}

/// `ReadTx`'s inputs/outputs are plain `TxOutput`/`TxInput` -- no
/// per-row `block_ref` or `spent_by` the way `AnyUtxoData`
/// (`ReadUtxos`/`SearchUtxos`/`UtxoChanges`) carries (see
/// `cardano.proto`'s `TxOutput`/`TxInput` vs. `query.proto`'s
/// `AnyUtxoData`). `block` is therefore always the "unknown" sentinel
/// here, and `spent_by` is always `None` -- not "confirmedly unspent"
/// the way `None` means for `AnyUtxoData`, just "`ReadTx` doesn't say."
/// This isn't a gRPC-only shortfall: `--transport http`'s `ReadTx`
/// equivalent reports the same way for the same reason, by design, see
/// `floe-server`'s `scheme::http::tx_row_dto`. (An input's `spent_by`
/// *would* be trivially knowable -- it's this tx itself, by definition
/// of being listed under `inputs` -- but that's not information
/// `ReadTx` is recovering, so it's left `None` here too rather than
/// singled out as a special case.)
fn tx_output_to_utxo_dto(tx_hash: TxHash, ix: u16, out: c::TxOutput) -> Result<UtxoDto> {
    Ok(UtxoDto {
        tx_hash,
        ix,
        block: ChainPointDto::default(),
        spent_by: None,
        address: address_dto(&out.address)?,
        coin: big_int_u64(out.coin),
        assets: assets_dto(out.assets),
        datum: datum_dto(out.datum)?,
        script: script_dto(out.script),
    })
}

fn tx_dto(tx_hash: TxHash, block: Option<q::ChainPoint>, t: c::Tx) -> Result<TxDto> {
    let inputs = t
        .inputs
        .into_iter()
        .map(|i| -> Result<UtxoDto> {
            let prev_hash: TxHash = i
                .tx_hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("TxInput.tx_hash must be 32 bytes"))?;
            let out = i
                .as_output
                .ok_or_else(|| anyhow::anyhow!("TxInput has no as_output"))?;
            tx_output_to_utxo_dto(prev_hash, i.output_index as u16, out)
        })
        .collect::<Result<Vec<_>>>()?;
    let outputs = t
        .outputs
        .into_iter()
        .enumerate()
        .map(|(ix, out)| tx_output_to_utxo_dto(tx_hash, ix as u16, out))
        .collect::<Result<Vec<_>>>()?;
    Ok(TxDto {
        hash: tx_hash,
        block: chain_point_dto(block),
        inputs,
        outputs,
        successful: t.successful,
    })
}

fn stale_dto(s: q::Stale) -> StaleDto {
    let reason = match q::StaleReason::try_from(s.reason) {
        Ok(q::StaleReason::RolledBack) => StaleReasonDto::RolledBack,
        Ok(q::StaleReason::TooOld) => StaleReasonDto::TooOld,
        _ => StaleReasonDto::Unknown,
    };
    StaleDto {
        reason,
        oldest_retained_slot: s.oldest_retained_slot,
    }
}

fn cred_predicate(
    payment_part: Option<Vec<u8>>,
    delegation_part: Option<Vec<u8>>,
) -> q::UtxoPredicate {
    q::UtxoPredicate {
        r#match: Some(q::AnyUtxoPattern {
            utxo_pattern: Some(q::any_utxo_pattern::UtxoPattern::Cardano(
                c::TxOutputPattern {
                    address: Some(c::AddressPattern {
                        exact_address: None,
                        payment_part,
                        delegation_part,
                    }),
                    asset: None,
                },
            )),
        }),
        not: vec![],
        all_of: vec![],
        any_of: vec![],
    }
}

fn address_pattern(
    payment_part: Option<Vec<u8>>,
    delegation_part: Option<Vec<u8>>,
) -> c::AddressPattern {
    c::AddressPattern {
        exact_address: None,
        payment_part,
        delegation_part,
    }
}

impl Client {
    async fn search(&mut self, predicate: q::UtxoPredicate, paging: Paging) -> Result<PageDto> {
        let resp = self
            .query
            .search_utxos(q::SearchUtxosRequest {
                predicate: Some(predicate),
                field_mask: None,
                max_items: paging.limit,
                start_token: paging.after,
            })
            .await?
            .into_inner();
        Ok(PageDto {
            items: resp
                .items
                .into_iter()
                .map(utxo_dto)
                .collect::<Result<_>>()?,
            ledger_tip: chain_point_dto(resp.ledger_tip),
            next: resp.next_token,
        })
    }
}

impl Backend for Client {
    async fn read_ref(&mut self, tx_hash: TxHash, ix: u32) -> Result<Option<UtxoDto>> {
        let resp = self
            .query
            .read_utxos(q::ReadUtxosRequest {
                keys: vec![q::TxoRef {
                    hash: tx_hash.to_vec(),
                    index: ix,
                }],
                field_mask: None,
            })
            .await?
            .into_inner();
        resp.items.into_iter().next().map(utxo_dto).transpose()
    }

    async fn address(
        &mut self,
        pay_cred: Cred,
        stake_cred: Option<Cred>,
        paging: Paging,
    ) -> Result<PageDto> {
        let predicate = cred_predicate(Some(pay_cred.to_vec()), stake_cred.map(|c| c.to_vec()));
        self.search(predicate, paging).await
    }

    async fn pay_cred(&mut self, cred: Cred, paging: Paging) -> Result<PageDto> {
        self.search(cred_predicate(Some(cred.to_vec()), None), paging)
            .await
    }

    async fn stake_cred(&mut self, cred: Cred, paging: Paging) -> Result<PageDto> {
        self.search(cred_predicate(None, Some(cred.to_vec())), paging)
            .await
    }

    async fn tx(&mut self, tx_hash: TxHash) -> Result<Option<TxResponseDto>> {
        match self
            .query
            .read_tx(q::ReadTxRequest {
                hash: tx_hash.to_vec(),
                field_mask: None,
            })
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                let Some(tx) = resp.tx else { return Ok(None) };
                let q::any_chain_tx::Chain::Cardano(t) = tx
                    .chain
                    .ok_or_else(|| anyhow::anyhow!("AnyChainTx has no chain"))?;
                Ok(Some(TxResponseDto {
                    tx: tx_dto(tx_hash, tx.block_ref, t)?,
                    ledger_tip: chain_point_dto(resp.ledger_tip),
                }))
            }
            Err(e) if e.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn datum(&mut self, hash: TxHash) -> Result<Option<DatumResponseDto>> {
        match self
            .query
            .read_data(q::ReadDataRequest {
                key: hash.to_vec(),
                field_mask: None,
            })
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                let Some(v) = resp.value else { return Ok(None) };
                Ok(Some(DatumResponseDto {
                    key: v
                        .key
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("AnyChainDatum.key must be 32 bytes"))?,
                    native_bytes: v.native_bytes,
                    ledger_tip: chain_point_dto(resp.ledger_tip),
                }))
            }
            Err(e) if e.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn changes(&mut self, params: ChangesParams) -> Result<ChangesResponseDto> {
        let since = if params.since_slot.is_none() && params.since_hash.is_none() {
            None
        } else {
            Some(q::Point {
                slot: params.since_slot,
                hash: params.since_hash.map(|h| h.to_vec()),
            })
        };
        let resp = self
            .query
            .utxo_changes(q::UtxoChangesRequest {
                address: Some(address_pattern(
                    params.pay_cred.map(|c| c.to_vec()),
                    params.stake_cred.map(|c| c.to_vec()),
                )),
                since,
                max_items: params.max_items,
                created_start_token: params.created_after,
                spent_start_token: params.spent_after,
            })
            .await?
            .into_inner();
        match resp
            .result
            .ok_or_else(|| anyhow::anyhow!("UtxoChangesResponse has no result"))?
        {
            q::utxo_changes_response::Result::Changes(c) => {
                Ok(ChangesResponseDto::Changes(ChangesDto {
                    created: c.created.into_iter().map(utxo_dto).collect::<Result<_>>()?,
                    spent: c.spent.into_iter().map(utxo_dto).collect::<Result<_>>()?,
                    ledger_tip: chain_point_dto(c.ledger_tip),
                    created_next: c.created_next_token,
                    spent_next: c.spent_next_token,
                }))
            }
            q::utxo_changes_response::Result::Stale(s) => {
                Ok(ChangesResponseDto::Stale(stale_dto(s)))
            }
        }
    }

    async fn submit(&mut self, raw: Vec<u8>) -> Result<SubmitTxResponseDto> {
        let resp = self
            .submit
            .submit_tx(SubmitTxRequest {
                tx: Some(AnyChainTx {
                    r#type: Some(any_chain_tx::Type::Raw(raw)),
                }),
            })
            .await?
            .into_inner();
        Ok(SubmitTxResponseDto { r#ref: resp.r#ref })
    }
}
