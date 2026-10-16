Vendored from https://github.com/utxorpc/spec.git
ref: 8a7a350edfdfc0059a12c10e91a6bedb966be5d6 (resolved: 8a7a350edfdfc0059a12c10e91a6bedb966be5d6)
label: v1
packages: cardano query submit

Pristine upstream copy -- not compiled, not depended on by any crate.
Kept purely as the diff baseline for `docs/utxorpc-divergences.md`:
`crates/dto-grpc/proto/floe/v1/` is floe's own fork of `query`/`cardano`
(repackaged `floe.v1.*`, diverging on purpose), and a diff against this
tree (after normalizing the package line) is the actual divergence
documentation.

Regenerate with scripts/fetch-utxorpc-protos.sh (edit UTXORPC_SPEC_REF
at the top of that script to upgrade). This file is pristine again
after that -- no patch to re-apply.
