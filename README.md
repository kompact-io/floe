# Floe

> The last chunk of the Cardano chain

Aimed at supporting dapps that require utxos at tip + some depth, 
and meeting this with minimal overhead.

Built N2N, but expects amaru (specifically, expects to consume amaru snapshots). 

## Design

We aimed at being utxo-rpc compat. 
However there seemed no way to recover "depth", 
which is something required in apps which need
to assess the risk / exposure, that may be incurred during rollbacks. 

Floe does:

- speak grpc, http/json, and http/cbor.
- querying tip and recent history of the Utxo set
- tx submission

Floe does not:

- support byron or pointer addresses (and associated utxos)
- support chain history with depth
- we dont handle `is_valid = false` properly
- query by assets
- anything about staking, governance, _etc_

Floe does not (yet):

- support protocol params

## TODO

### Protocol params

Not present in amaru's bootstrap snapshot (`state` is unused even by amaru's
own bootstrap). amaru instead hardcodes a per-network baseline `PParams` at a
known epoch and updates it by replaying enacted `ParameterChangeAction`s.
Replicating this reliably means also tracking governance quorum/threshold
(ratification) state, not just the param values themselves.

## Code

The repo uses cargo + nix.

