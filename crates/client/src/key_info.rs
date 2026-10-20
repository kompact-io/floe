//! key_info: pure local crypto -- an ed25519 signing key's payment
//! credential and the address it derives (enterprise, no delegation
//! part). No `Backend`, no client, no network call: works with no
//! `floe-server` running at all.

use amaru_kernel::address::{Address, ShelleyAddress};
use amaru_kernel::ed25519::SigningKey;
use amaru_kernel::{Credential, Hasher};

#[derive(Debug, Clone, serde::Serialize)]
pub struct KeyInfo {
    pub address: String,
    pub vkh: String,
}

pub fn key_info(skey: [u8; 32], network: amaru_kernel::network::Network) -> anyhow::Result<KeyInfo> {
    let vk = SigningKey::from_bytes(&skey).verifying_key();
    let pay_hash = Hasher::<224>::hash(vk.as_bytes());
    let address = Address::Shelley(ShelleyAddress::new(
        network,
        Credential::KeyHash(pay_hash),
        None,
    ));
    Ok(KeyInfo {
        address: address
            .to_bech32()
            .ok_or_else(|| anyhow::anyhow!("address has no bech32 representation"))?,
        vkh: hex::encode(pay_hash),
    })
}
