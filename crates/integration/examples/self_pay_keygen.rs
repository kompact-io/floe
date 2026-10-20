//! Generates a fresh preprod ed25519 payment key (enterprise address,
//! no stake part), printed as `KEY=value` dotenv lines for `floe-smoke
//! --skey "$FLOE_SMOKE_SKEY" --submit`. Fund `FLOE_TEST_ADDRESS` via
//! the preprod faucet; it's printed for that step only, never read
//! back.

use amaru_kernel::address::{Address, ShelleyAddress};
use amaru_kernel::ed25519::SigningKey;
use amaru_kernel::network::Network;
use amaru_kernel::{Credential, Hasher};

fn main() {
    use rand::RngExt;
    let mut seed = [0u8; 32];
    rand::rng().fill(&mut seed);
    let sk = SigningKey::from_bytes(&seed);
    let vk = sk.verifying_key();
    let pay_hash = Hasher::<224>::hash(vk.as_bytes());

    let address = Address::Shelley(ShelleyAddress::new(
        Network::Testnet,
        Credential::KeyHash(pay_hash),
        None,
    ))
    .to_bech32()
    .expect("a freshly-built Shelley address always encodes");

    // Printed once, straight into a gitignored dotenv file -- not
    // retained or logged anywhere else.
    let sk_bytes = sk.to_bytes();

    let skey_hex = hex::encode(sk_bytes);
    println!("FLOE_TEST_SKEY={skey_hex}");
    println!("FLOE_SMOKE_SKEY={skey_hex}");
    println!("FLOE_TEST_ADDRESS={address}");
}
