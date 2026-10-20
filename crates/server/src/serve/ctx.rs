//! Ctx: the one shared, scheme-agnostic bundle every transport builds
//! its request handling on. Built once in `main.rs`, cloned into each
//! spawned scheme's `serve()`.
//!
//! `Ctx` owns the store: `query` (below) is private, reachable from
//! nowhere outside this module -- no scheme, anywhere in the crate,
//! opens a `Connection` or calls `query::` itself. Every method here
//! does the full job (open a reader, run the lookup, join each row's
//! `BlockPoint`/reconstructed `Address`) and hands back a
//! self-contained, connection-free result; a scheme's own code is
//! pure formatting from there -- the actual wire-translation
//! boundary, and the only thing that differs between them.
//!
//! Replaces what used to be separate, near-identical structs per
//! scheme (`scheme::grpc`'s `Svc`, `scheme::grpc::health`'s `Svc`,
//! `scheme::http::query`'s `AppState`, `scheme::http::metrics`'s
//! `AppState`, `submit::Svc`) -- each was just a different subset of
//! these same fields.

mod query;

use std::sync::Arc;

use anyhow::Result;
use pallas_addresses::Address;
use rusqlite::Connection;

use crate::health::Health;
use crate::ingest::decode::{OutputRef, TxHash};
use crate::metrics::Metrics;
use crate::serve::submit::{self, SubmitOutcome};

pub use query::{
    AddressFilter, BlockPoint, Cursor, DatumRecord, NotReady, ResolvedDatum, ResolvedScript, Stale,
    StaleReason,
};

#[derive(Clone)]
pub struct Ctx {
    /// A path, not a pooled connection -- every method below opens its
    /// own short-lived reader from this. No DB pooling; performance
    /// impact unknown.
    pub db_path: Arc<str>,
    /// Hard ceiling on `SearchUtxos.max_items` -- a request asking for
    /// more just gets clamped, not rejected. Operator-configurable via
    /// `--max-page-size`.
    pub max_page_size: u32,
    pub amaru_submit_api: String,
    pub health: Arc<Health>,
    pub requests: Arc<Metrics>,
}

// --- submit --------------------------------------------------------

impl Ctx {
    pub async fn submit(&self, raw: &[u8]) -> SubmitOutcome {
        let tx = match submit::decode_tx_bytes(raw) {
            Ok(tx) => tx,
            Err(e) => {
                return SubmitOutcome::Rejected {
                    reason: e.to_string(),
                };
            }
        };
        // `tx.hash()` is the transaction id, computed the same way
        // regardless of which backend accepts it -- no need to parse
        // one back out of amaru's response body.
        let tx_hash = tx.hash().to_vec();

        match submit::post_tx(&self.amaru_submit_api, raw).await {
            Ok((status, _body)) if (200..300).contains(&status) => {
                SubmitOutcome::Accepted { tx_hash }
            }
            Ok((_status, body)) => SubmitOutcome::Rejected {
                reason: format!("rejected by amaru: {body}"),
            },
            Err(e) => SubmitOutcome::Unavailable {
                reason: format!("amaru submit-api unavailable: {e}"),
            },
        }
    }
}

// --- read: resolved row shapes --------------------------------------

/// A fully-resolved UTxO -- address already reconstructed, origin
/// `BlockPoint` already joined. No connection needed to finish it;
/// every scheme's own conversion from here is pure formatting.
///
/// `block`/`spent_by` are meaningless for `ReadTx`'s inputs/outputs
/// (see `Ctx::read_tx`) -- that's a wire-contract choice each scheme
/// makes in its own leaner conversion, not a reason for a second
/// resolved-row type here.
pub struct Utxo {
    pub output_ref: OutputRef,
    pub address: Address,
    pub spent_by: Option<TxHash>,
    pub coin: u64,
    /// Raw CBOR multiasset bytes -- decoding into a wire shape
    /// (gRPC's `cardano::Multiasset`, `floe_dto_http::cardano::Multiasset`)
    /// stays scheme-specific.
    pub assets: Option<Vec<u8>>,
    pub datum: ResolvedDatum,
    pub script: Option<ResolvedScript>,
    pub block: Option<BlockPoint>,
}

pub struct Tx {
    pub hash: TxHash,
    pub inputs: Vec<Utxo>,
    pub outputs: Vec<Utxo>,
    pub required_signers: Vec<[u8; 28]>,
}

/// Rebuilds the address from `(address_header, pay_hash, stake_hash)`
/// -- `utxo` doesn't store raw address bytes at all. Byte-exact, not
/// a rebuilt equivalent: `address_header` is the real header
/// `decode.rs` saw, so this is the original address, parsed back
/// through `pallas_addresses` rather than hand-assembled. Lossless for
/// every address floe indexes: Byron is never indexed and pointer
/// addresses aren't supported (not approximated -- `decode::Credentials`
/// already collapses `Pointer` into the same shape as enterprise, so
/// there's nothing this would get wrong that wasn't already true).
fn reconstruct_address(u: &query::ResolvedUtxo) -> Address {
    let mut bytes = Vec::with_capacity(1 + 28 + 28);
    bytes.push(u.address_header);
    bytes.extend_from_slice(&u.pay_hash);
    if let Some(stake_hash) = u.stake_hash {
        bytes.extend_from_slice(&stake_hash);
    }
    Address::from_bytes(&bytes)
        .expect("address_header/pay_hash/stake_hash always encode a valid, supported address")
}

fn resolve_utxo(u: query::ResolvedUtxo, block: Option<BlockPoint>) -> Utxo {
    Utxo {
        address: reconstruct_address(&u),
        output_ref: u.output_ref,
        spent_by: u.spent_by,
        coin: u.coin,
        assets: u.assets,
        datum: u.datum,
        script: u.script,
        block,
    }
}

// --- read: Ctx methods -----------------------------------------------

impl Ctx {
    /// Every read opens its own short-lived reader (see `db_path`'s
    /// doc) inside `spawn_blocking`, since rusqlite is synchronous.
    /// "Pretends" there's a connection pool behind `db_path`: this is
    /// the one seam a real pool would slot into later, with no caller
    /// anywhere else needing to change.
    async fn blocking<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = query::open_reader(&db_path)?;
            f(&mut conn)
        })
        .await
        .map_err(|e| anyhow::anyhow!("blocking task panicked: {e}"))?
    }

    fn clamp_limit(&self, requested: Option<i32>) -> u32 {
        requested
            .unwrap_or(100)
            .clamp(1, self.max_page_size.min(i32::MAX as u32) as i32) as u32
    }

    /// Every lookup except `read_utxos` (by-ref) gates on this --
    /// `Health::is_ready` (not just `Mode::Running`, see its own doc).
    fn require_ready(&self) -> Result<()> {
        if !self.health.is_ready() {
            return Err(query::NotReady.into());
        }
        Ok(())
    }

    /// `ReadUtxos`: by exact `TxoRef`/`<tx_hash_hex>:<ix>` keys.
    pub async fn read_utxos(
        &self,
        keys: Vec<OutputRef>,
    ) -> Result<(Vec<Utxo>, Option<BlockPoint>)> {
        self.blocking(move |conn| {
            let mut rows = Vec::new();
            for r in keys {
                if let Some(u) = query::by_ref(conn, r)? {
                    rows.push(u);
                }
            }
            let rows = query::with_block_points(conn, rows)?;
            let tip = query::tip_point(conn)?;
            let items = rows.into_iter().map(|(u, b)| resolve_utxo(u, b)).collect();
            Ok((items, tip))
        })
        .await
    }

    /// `SearchUtxos`, reached via http's `?pay_hash=&stake_hash=`: a
    /// single address-pattern predicate, paginated.
    ///
    /// `since_slot`/`since_hash` (floe's own addition): absent, this
    /// is the tip set only (today's behavior). Given, `items` also
    /// includes matching rows spent after that point -- `spent_by`
    /// tells them apart -- and a stale point (too old, or rolled back)
    /// fails the call with `Stale` rather than returning an
    /// incomplete result.
    pub async fn search_utxos(
        &self,
        filter: AddressFilter,
        since_slot: Option<u64>,
        since_hash: Option<[u8; 32]>,
        after: Option<Cursor>,
        limit: Option<i32>,
    ) -> Result<(Vec<Utxo>, Option<Cursor>, Option<BlockPoint>)> {
        self.require_ready()?;
        let limit = self.clamp_limit(limit);
        self.blocking(move |conn| {
            let since_slot = match query::check_point(conn, since_slot, since_hash)? {
                query::PointCheck::Stale(reason) => return Err(query::Stale(reason).into()),
                query::PointCheck::NoHashGiven(s) => s,
                query::PointCheck::Valid(s) => Some(s),
            };
            let rows = match filter {
                AddressFilter::PayOnly(p) => query::by_pay_hash(conn, p, since_slot, after, limit)?,
                AddressFilter::PayAndStake(p, d) => {
                    query::by_credentials(conn, p, Some(d), since_slot, after, limit)?
                }
                AddressFilter::StakeOnly(d) => {
                    query::by_stake_hash(conn, d, since_slot, after, limit)?
                }
            };
            let last = rows.last().map(|u| Cursor {
                slot: u.spent_slot.unwrap_or(u.slot),
                output_ref: u.output_ref,
            });
            let tip = query::tip_point(conn)?;
            let rows = query::with_block_points(conn, rows)?;
            let items = rows.into_iter().map(|(u, b)| resolve_utxo(u, b)).collect();
            Ok((items, last, tip))
        })
        .await
    }

    /// `ReadData`: a datum by its own hash, independent of any UTxO.
    pub async fn read_data(
        &self,
        hash: [u8; 32],
    ) -> Result<(Option<DatumRecord>, Option<BlockPoint>)> {
        self.require_ready()?;
        self.blocking(move |conn| {
            let datum = query::datum_by_hash(conn, hash)?;
            let tip = query::tip_point(conn)?;
            Ok((datum, tip))
        })
        .await
    }

    /// `ReadTx`: a tx by hash, hydrated entirely from `utxo` rows (see
    /// `query::ResolvedTx`'s doc comment). `inputs`/`outputs`' own
    /// `block`/`spent_by` are left unresolved (`None`/whatever the row
    /// has) -- neither scheme's `ReadTx` shape uses them, only the
    /// tx's own origin `BlockPoint` (returned alongside) does.
    pub async fn read_tx(
        &self,
        hash: TxHash,
    ) -> Result<(Option<(Tx, Option<BlockPoint>)>, Option<BlockPoint>)> {
        self.require_ready()?;
        self.blocking(move |conn| {
            let tx = query::by_tx_hash(conn, hash)?;
            let tx = match tx {
                Some(t) => {
                    let block = query::block_point(conn, t.block_slot)?;
                    let row = |u| resolve_utxo(u, None);
                    Some((
                        Tx {
                            hash: t.hash,
                            inputs: t.inputs.into_iter().map(row).collect(),
                            outputs: t.outputs.into_iter().map(row).collect(),
                            required_signers: t.required_signers,
                        },
                        block,
                    ))
                }
                None => None,
            };
            let tip = query::tip_point(conn)?;
            Ok((tx, tip))
        })
        .await
    }

    pub async fn tip(&self) -> Result<Option<(u64, [u8; 32])>> {
        self.blocking(|conn| crate::ingest::store::tip(conn)).await
    }

    pub async fn mode(&self) -> Result<crate::ingest::store::Mode> {
        self.blocking(|conn| crate::ingest::store::get_mode(conn))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn submit_rejects_undecodable_bytes_without_dialing_amaru() {
        let ctx = Ctx {
            db_path: Arc::from(""),
            max_page_size: 1000,
            // Never dialed: decode fails first.
            amaru_submit_api: "127.0.0.1:1".to_string(),
            health: Arc::new(Health::default()),
            requests: Arc::new(Metrics::default()),
        };
        match ctx.submit(b"not a transaction").await {
            SubmitOutcome::Rejected { .. } => {}
            _ => panic!("expected Rejected"),
        }
    }
}
