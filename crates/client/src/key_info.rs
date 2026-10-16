//! key_info: pure local crypto -- an ed25519 signing key's payment
//! credential and the address it derives (enterprise, no delegation
//! part). No `Backend`, no client, no network call: works with no
//! `floe-server` running at all.

use pallas_addresses::{Address, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart};
use pallas_crypto::hash::Hasher;
use pallas_crypto::key::ed25519::SecretKey;

#[derive(Debug, Clone, serde::Serialize)]
pub struct KeyInfo {
    pub address: String,
    pub vkh: String,
}

pub fn key_info(skey: [u8; 32], network: pallas_addresses::Network) -> anyhow::Result<KeyInfo> {
    let vk = SecretKey::from(skey).public_key();
    let pay_cred = Hasher::<224>::hash(vk.as_ref());
    let address = Address::Shelley(ShelleyAddress::new(
        network,
        ShelleyPaymentPart::key_hash(pay_cred),
        ShelleyDelegationPart::Null,
    ));
    Ok(KeyInfo {
        address: address.to_bech32()?,
        vkh: hex::encode(pay_cred),
    })
}
