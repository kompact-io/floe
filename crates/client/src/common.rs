//! common: always-compiled pieces shared by every scheme (and by
//! `main.rs`, when the `clap`/`serde_json` bin features are on) --
//! hex/network CLI value parsers, paging, the address/credential
//! decomposition every scheme's `address`/`Address` case needs, and
//! the `Backend` trait itself (see `../../plan-floe-client-as-lib.md`'s
//! "A `Backend` trait is the shared contract" for why this exists
//! instead of three independent `Client` APIs).

pub type TxHash = [u8; 32];
pub type Cred = [u8; 28];

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

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "clap", derive(clap::Args))]
pub struct Paging {
    /// Resume cursor, taken verbatim from a previous page's `next_token`.
    #[cfg_attr(feature = "clap", arg(long))]
    pub after: Option<String>,
    /// Max rows in this page (server caps at 1000, default 100).
    #[cfg_attr(feature = "clap", arg(long))]
    pub limit: Option<i32>,
}

/// `changes`'s seven arguments, named instead of positional -- shared
/// by every scheme's `Backend::changes` impl.
#[derive(Debug, Clone, Default)]
pub struct ChangesParams {
    pub pay_cred: Option<Cred>,
    pub stake_cred: Option<Cred>,
    pub since_slot: Option<u64>,
    pub since_hash: Option<TxHash>,
    pub max_items: Option<i32>,
    pub created_after: Option<String>,
    pub spent_after: Option<String>,
}

/// Decomposes a `Shelley` address into its payment/delegation
/// credentials -- the same decomposition every scheme's `address`
/// case needs (no scheme's wire predicate has a raw-address field
/// floe supports; see `floe-server`'s `decode::credentials_from_address`
/// for the server-side mirror of this). A bare `Byron`/`Stake`
/// address has no payment credential floe could have indexed.
pub fn shelley_creds(addr: &pallas_addresses::Address) -> anyhow::Result<(Cred, Option<Cred>)> {
    use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};

    let Address::Shelley(s) = addr else {
        anyhow::bail!("address has no payment credential floe could have indexed (Byron/Stake)");
    };
    let hash28 = |h: &pallas_crypto::hash::Hash<28>| -> Cred {
        h.as_ref().try_into().expect("Hash<28> is 28 bytes")
    };
    let pay_cred = match s.payment() {
        ShelleyPaymentPart::Key(h) | ShelleyPaymentPart::Script(h) => hash28(h),
    };
    let stake_cred = match s.delegation() {
        ShelleyDelegationPart::Key(h) | ShelleyDelegationPart::Script(h) => Some(hash28(h)),
        ShelleyDelegationPart::Pointer(_) | ShelleyDelegationPart::Null => None,
    };
    Ok((pay_cred, stake_cred))
}

/// The actual public surface of every scheme -- `grpc::Client`,
/// `http_json::Client`, `http_cbor::Client` each implement this, and
/// `self_pay::run`/`main.rs`'s dispatch are generic over it, never
/// mentioning a concrete scheme. "Not found" is `Ok(None)`;
/// wrong-network/malformed input is `Err`; `changes`'s stale-cursor
/// case is `ChangesResponseDto::Stale`, not a separate error.
///
/// `address` takes already-decomposed credentials (not a
/// `pallas_addresses::Address`) -- `shelley_creds`, above, does that
/// decomposition once at the call site (`main.rs`'s dispatch); every
/// scheme's own address-pattern wire shape already distinguishes
/// pay-only/pay-and-stake/stake-only exactly this way (see
/// `floe-server`'s `AddressFilter`), so there's nothing scheme-specific
/// left for `address` to do with a richer input type.
///
/// Native `async fn` in traits (stable, no `async-trait` crate needed)
/// is deliberate here: nothing needs `dyn Backend` or to send a
/// `Backend` call's future across threads (every call site is either
/// generic over `B: Backend` or a concrete type picked once in
/// `main.rs`, awaited in place) -- `#[allow]` suppresses the
/// auto-trait-bounds lint this otherwise triggers.
#[allow(async_fn_in_trait)]
pub trait Backend {
    async fn read_ref(
        &mut self,
        tx_hash: TxHash,
        ix: u32,
    ) -> anyhow::Result<Option<floe_dto_http::UtxoDto>>;

    async fn address(
        &mut self,
        pay_cred: Cred,
        stake_cred: Option<Cred>,
        paging: Paging,
    ) -> anyhow::Result<floe_dto_http::PageDto>;

    async fn pay_cred(
        &mut self,
        cred: Cred,
        paging: Paging,
    ) -> anyhow::Result<floe_dto_http::PageDto>;

    async fn stake_cred(
        &mut self,
        cred: Cred,
        paging: Paging,
    ) -> anyhow::Result<floe_dto_http::PageDto>;

    async fn tx(&mut self, tx_hash: TxHash) -> anyhow::Result<Option<floe_dto_http::TxResponseDto>>;

    async fn datum(
        &mut self,
        hash: TxHash,
    ) -> anyhow::Result<Option<floe_dto_http::DatumResponseDto>>;

    async fn changes(
        &mut self,
        params: ChangesParams,
    ) -> anyhow::Result<floe_dto_http::ChangesResponseDto>;

    async fn submit(&mut self, raw: Vec<u8>) -> anyhow::Result<floe_dto_http::SubmitTxResponseDto>;
}
