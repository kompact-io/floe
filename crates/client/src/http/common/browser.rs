//! `gloo-net` fetch-backed `Client`.

use crate::error::{Context, Result};

#[derive(Clone)]
pub struct Client;

impl Client {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }

    pub(crate) async fn get(&self, base: &str, path: &str, accept: &str) -> Result<(u16, Vec<u8>)> {
        let resp = gloo_net::http::Request::get(&format!("{base}{path}"))
            .header("Accept", accept)
            .send()
            .await
            .with_context(|| format!("GET {base}{path}"))?;
        let status = resp.status();
        let body = resp.binary().await.context("reading response body")?;
        Ok((status, body))
    }

    pub(crate) async fn post(
        &self,
        base: &str,
        path: &str,
        body: &[u8],
        content_type: &str,
        accept: &str,
    ) -> Result<(u16, Vec<u8>)> {
        let request = gloo_net::http::Request::post(&format!("{base}{path}"))
            .header("Content-Type", content_type)
            .header("Accept", accept)
            .body(js_sys::Uint8Array::from(body))
            .context("building request body")?;
        let resp = request
            .send()
            .await
            .with_context(|| format!("POST {base}{path}"))?;
        let status = resp.status();
        let body = resp.binary().await.context("reading response body")?;
        Ok((status, body))
    }
}
