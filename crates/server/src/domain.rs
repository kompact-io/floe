//! domain: small pieces of business logic shared regardless of
//! transport -- network-magic mapping and rebuilding an address from
//! stored credentials (`reconstruct_address`, used by `scheme::grpc`'s
//! and `scheme::http`'s domain-to-DTO conversions). Health/metrics
//! reporting used to live here too; split out into their own
//! `health`/`metrics` core modules once those became cross-scheme (read
//! from both the HTTP and gRPC health handlers), see
//! `../../plan-server-health-and-metrics.md`.

use pallas_addresses::{
    Address, Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
};

use crate::query::ResolvedUtxo;

/// All public Cardano testnets (preprod, preview, …) share address network
/// id 0; only mainnet's magic maps to `Network::Mainnet`.
pub fn network_from_magic(magic: u64) -> Network {
    if magic == 764824073 {
        Network::Mainnet
    } else {
        Network::Testnet
    }
}

/// Rebuilds the address from `(pay_cred, pay_script, stake_cred,
/// stake_script)` -- `utxo` doesn't store raw address bytes at all
/// (see `store::BASE_SCHEMA_SQL`'s `utxo` table comment). Lossless for
/// every address `floe` indexes: Byron is never indexed and pointer
/// addresses aren't supported (not approximated -- `decode::Credentials`
/// already collapses `Pointer` into the same shape as enterprise, so
/// there's nothing this would get wrong that wasn't already true).
pub(crate) fn reconstruct_address(u: &ResolvedUtxo, network: Network) -> Address {
    let payment = if u.pay_script {
        ShelleyPaymentPart::script_hash(u.pay_cred.into())
    } else {
        ShelleyPaymentPart::key_hash(u.pay_cred.into())
    };
    let delegation = match (u.stake_cred, u.stake_script) {
        (Some(h), Some(true)) => ShelleyDelegationPart::script_hash(h.into()),
        (Some(h), Some(false)) => ShelleyDelegationPart::key_hash(h.into()),
        (Some(_), None) => unreachable!("stake_script is Some iff stake_cred is"),
        (None, _) => ShelleyDelegationPart::Null,
    };
    Address::Shelley(ShelleyAddress::new(network, payment, delegation))
}
