//! common: always-compiled pieces shared by every scheme -- hex/network
//! CLI value parsers, the address decomposition `SearchUtxos` callers
//! need, and the `Backend` trait itself.

pub type TxHash = [u8; 32];

/// Used for address parts
pub type Hash28 = [u8; 28];

/// Hex-string clap value parser, coercing straight into a fixed-size
/// byte array -- also just a plain hex-decode-and-check helper when
/// `clap` isn't compiled in.
pub fn parse_hex<const N: usize>(s: &str) -> Result<[u8; N], String> {
    let bytes = hex::decode(s).map_err(|e| e.to_string())?;
    bytes
        .try_into()
        .map_err(|v: Vec<u8>| format!("expected {N} bytes, got {}", v.len()))
}

/// `--network`'s value: parsed case-insensitively, with the three
/// non-mainnet CIP-5-visible names (`testnet`/`preprod`/`preview`) all
/// mapping to `pallas_addresses::Network::Testnet` -- that's the only
/// distinction the bech32 HRP (and this client) can see.
#[derive(Debug, Clone, Copy)]
pub struct NetworkArg(pub pallas_addresses::Network);

impl std::str::FromStr for NetworkArg {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "mainnet" => Ok(Self(pallas_addresses::Network::Mainnet)),
            "testnet" | "preprod" | "preview" => Ok(Self(pallas_addresses::Network::Testnet)),
            other => Err(format!(
                "unknown network {other:?} (expected mainnet/testnet/preprod/preview)"
            )),
        }
    }
}

/// Decomposes a `Shelley` address into its parts.
pub fn shelley_hashes(
    addr: &pallas_addresses::Address,
) -> anyhow::Result<(Hash28, Option<Hash28>)> {
    use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};

    let Address::Shelley(s) = addr else {
        anyhow::bail!("address has no payment hash floe could have indexed (Byron/Stake)");
    };
    let hash28 = |h: &pallas_crypto::hash::Hash<28>| -> Hash28 {
        h.as_ref().try_into().expect("Hash<28> is 28 bytes")
    };
    let pay_hash = match s.payment() {
        ShelleyPaymentPart::Key(h) | ShelleyPaymentPart::Script(h) => hash28(h),
    };
    let stake_hash = match s.delegation() {
        ShelleyDelegationPart::Key(h) | ShelleyDelegationPart::Script(h) => Some(hash28(h)),
        ShelleyDelegationPart::Pointer(_) | ShelleyDelegationPart::Null => None,
    };
    Ok((pay_hash, stake_hash))
}

/// Surface of `Floe`: one method per RPC, taking/returning the exact
/// `floe_dto_http` request/response DTO `floe-server`'s own handlers
/// take/return -- no bespoke per-scheme parameter shapes, so this
/// can't drift from what the wire actually carries.
#[allow(async_fn_in_trait)]
pub trait Backend {
    async fn read_utxos(
        &mut self,
        req: floe_dto_http::query::ReadUtxosRequest,
    ) -> anyhow::Result<floe_dto_http::query::ReadUtxosResponse>;

    async fn search_utxos(
        &mut self,
        req: floe_dto_http::query::SearchUtxosRequest,
    ) -> anyhow::Result<floe_dto_http::query::SearchUtxosResponse>;

    /// `None` on a 404/`NotFound` -- absent, not a wiring failure.
    async fn read_data(
        &mut self,
        req: floe_dto_http::query::ReadDataRequest,
    ) -> anyhow::Result<Option<floe_dto_http::query::ReadDataResponse>>;

    /// Same not-found convention as `read_data`.
    async fn read_tx(
        &mut self,
        req: floe_dto_http::query::ReadTxRequest,
    ) -> anyhow::Result<Option<floe_dto_http::query::ReadTxResponse>>;

    async fn submit_tx(
        &mut self,
        raw: Vec<u8>,
    ) -> anyhow::Result<floe_dto_http::submit::SubmitTxResponse>;

    /// Not every deployment exposes this over every scheme (an
    /// `http-json`/`http-cbor` client only sees it if the query
    /// listener and the dedicated `/health` listener happen to share
    /// an address) -- callers that want to probe for support, not
    /// require it, should treat `Err` as "unsupported here", not a
    /// wiring failure.
    async fn get_health(&mut self) -> anyhow::Result<floe_dto_http::health::HealthResponse>;

    /// Same availability caveat as `get_health`.
    async fn get_metrics(&mut self) -> anyhow::Result<floe_dto_http::health::MetricsResponse>;
}
