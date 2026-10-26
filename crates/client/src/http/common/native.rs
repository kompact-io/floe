//! reqwest-backed `Client`.

use crate::error::{Context, Result};

#[derive(Clone)]
pub struct Client {
    inner: reqwest::Client,
}

impl Client {
    pub fn new() -> Result<Self> {
        let inner = reqwest::Client::builder()
            .build()
            .context("building the HTTP client")?;
        Ok(Self { inner })
    }

    /// Bodyless -- only `/health`/`/metrics` use this; every other RPC
    /// is `post`.
    pub(crate) async fn get(&self, base: &str, path: &str, accept: &str) -> Result<(u16, Vec<u8>)> {
        let resp = self
            .inner
            .get(format!("{base}{path}"))
            .header(reqwest::header::ACCEPT, accept)
            .send()
            .await
            .with_context(|| format!("GET {base}{path}"))?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await.context("reading response body")?;
        Ok((status, body.into()))
    }

    pub(crate) async fn post(
        &self,
        base: &str,
        path: &str,
        body: &[u8],
        content_type: &str,
        accept: &str,
    ) -> Result<(u16, Vec<u8>)> {
        let resp = self
            .inner
            .post(format!("{base}{path}"))
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .header(reqwest::header::ACCEPT, accept)
            .body(body.to_vec())
            .send()
            .await
            .with_context(|| format!("POST {base}{path}"))?;
        let status = resp.status().as_u16();
        let body = resp.bytes().await.context("reading response body")?;
        Ok((status, body.into()))
    }
}
