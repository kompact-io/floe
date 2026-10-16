//! http_json: the JSON wire format over `http_common`'s shared socket
//! I/O and path builders -- `Accept: application/json`,
//! `serde_json`/`floe-dto-http`'s `Serialize`/`Deserialize` derives.

use anyhow::{Result, bail};

use crate::common::{Backend, ChangesParams, Cred, Paging, TxHash};
use crate::http_common;
use floe_dto_http::{
    ChangesResponseDto, DatumResponseDto, ErrorBody, PageDto, SubmitTxResponseDto, TxResponseDto,
};

pub struct Client {
    addr: String,
}

impl Client {
    pub fn new(server: &str) -> Self {
        Self {
            addr: http_common::strip_scheme(server).to_string(),
        }
    }

    async fn fetch<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let (status, body) = http_common::get(&self.addr, path, "application/json").await?;
        decode(status, &body)
    }

    /// Like `fetch`, but a 404 is `Ok(None)` instead of `Err` -- for
    /// `tx`/`datum`, whose `Backend` signature reports "not found" that
    /// way.
    async fn fetch_opt<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        let (status, body) = http_common::get(&self.addr, path, "application/json").await?;
        if status == 404 {
            return Ok(None);
        }
        decode(status, &body).map(Some)
    }

    async fn post(&self, path: &str, raw: &[u8]) -> Result<SubmitTxResponseDto> {
        let (status, body) = http_common::post(&self.addr, path, raw, "application/json").await?;
        decode(status, &body)
    }
}

fn decode<T: serde::de::DeserializeOwned>(status: u16, body: &[u8]) -> Result<T> {
    if !(200..300).contains(&status) {
        let msg = serde_json::from_slice::<ErrorBody>(body)
            .map(|e| e.error)
            .unwrap_or_else(|_| String::from_utf8_lossy(body).into_owned());
        bail!("http {status}: {msg}");
    }
    Ok(serde_json::from_slice(body)?)
}

impl Backend for Client {
    async fn read_ref(
        &mut self,
        tx_hash: TxHash,
        ix: u32,
    ) -> Result<Option<floe_dto_http::UtxoDto>> {
        let page: PageDto = self.fetch(&http_common::ref_path(tx_hash, ix)).await?;
        Ok(page.items.into_iter().next())
    }

    async fn address(
        &mut self,
        pay_cred: Cred,
        stake_cred: Option<Cred>,
        paging: Paging,
    ) -> Result<PageDto> {
        self.fetch(&http_common::utxos_path(
            Some(pay_cred),
            stake_cred,
            &paging,
        ))
        .await
    }

    async fn pay_cred(&mut self, cred: Cred, paging: Paging) -> Result<PageDto> {
        self.fetch(&http_common::utxos_path(Some(cred), None, &paging))
            .await
    }

    async fn stake_cred(&mut self, cred: Cred, paging: Paging) -> Result<PageDto> {
        self.fetch(&http_common::utxos_path(None, Some(cred), &paging))
            .await
    }

    async fn tx(&mut self, tx_hash: TxHash) -> Result<Option<TxResponseDto>> {
        self.fetch_opt(&http_common::tx_path(tx_hash)).await
    }

    async fn datum(&mut self, hash: TxHash) -> Result<Option<DatumResponseDto>> {
        self.fetch_opt(&http_common::data_path(hash)).await
    }

    async fn changes(&mut self, params: ChangesParams) -> Result<ChangesResponseDto> {
        self.fetch(&http_common::changes_path(&params)).await
    }

    async fn submit(&mut self, raw: Vec<u8>) -> Result<SubmitTxResponseDto> {
        self.post("/tx", &raw).await
    }
}
