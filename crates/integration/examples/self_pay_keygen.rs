//! Generates a fresh preprod ed25519 payment key (enterprise address,
//! no stake part), printed as `KEY=value` dotenv lines for `floe-smoke
//! --skey "$FLOE_SMOKE_SKEY" --submit`. Fund `FLOE_TEST_ADDRESS` via
//! the preprod faucet; it's printed for that step only, never read
//! back.

use pallas_addresses::{
    Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
};
use pallas_crypto::hash::Hasher;
use pallas_crypto::key::ed25519::SecretKey;

fn main() {
    let sk = SecretKey::new(rand::rng());
    let vk = sk.public_key();
    let pay_hash = Hasher::<224>::hash(vk.as_ref());

    let address = Address::Shelley(ShelleyAddress::new(
        Network::Testnet,
        ShelleyPaymentPart::key_hash(pay_hash),
        ShelleyDelegationPart::Null,
    ))
    .to_bech32()
    .expect("a freshly-built Shelley address always encodes");

    // SAFETY: printed once, straight into a gitignored dotenv file --
    // not retained or logged anywhere else.
    let sk_bytes = unsafe { SecretKey::leak_into_bytes(sk) };

    let skey_hex = hex::encode(sk_bytes);
    println!("FLOE_TEST_SKEY={skey_hex}");
    println!("FLOE_SMOKE_SKEY={skey_hex}");
    println!("FLOE_TEST_ADDRESS={address}");
}
