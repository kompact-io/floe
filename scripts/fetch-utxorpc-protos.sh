#!/usr/bin/env bash
# Vendors the utxorpc .proto files floe's gRPC layer builds against.
#
# Not run at build time (keeps the Nix build hermetic, no network fetch
# from flake.nix/crane) -- run by hand when bumping to a newer spec
# commit, and the result (docs/utxorpc-spec/) is committed like any
# other source file. This is a pristine, uncompiled reference copy only
# -- it's the diff baseline for docs/utxorpc-divergences.md, not an
# input to any build. floe's own fork (crates/dto-grpc/proto/floe/v1/)
# is maintained separately and isn't touched by this script. To
# upgrade: bump UTXORPC_SPEC_REF (and UTXORPC_SPEC_VERSION for the
# human-readable label) below, re-run, review the diff against floe's
# fork, fix up whatever's needed there, commit both.
#
# Takes no arguments -- it ignores and runs the same way regardless, so
# don't pass it one expecting different behavior (e.g. `--help`).
if [ "$#" -gt 0 ]; then
    echo "error: this script takes no arguments (got: $*)" >&2
    exit 1
fi
set -euo pipefail

UTXORPC_SPEC_REPO="https://github.com/utxorpc/spec.git"
UTXORPC_SPEC_REF="8a7a350edfdfc0059a12c10e91a6bedb966be5d6"
UTXORPC_SPEC_VERSION="v1"     # human label only, not passed to git

# Only what floe's Query service needs: the chain-agnostic `query`
# package, its `cardano` dependency, and `submit` (vendored now so it's
# ready when Submit support lands, even though nothing generates from
# it yet -- see plan-rpc.md).
PACKAGES=(cardano query submit)

dest_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/docs/utxorpc-spec/utxorpc/v1"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

echo "fetching utxorpc/spec @ ${UTXORPC_SPEC_REF} (${UTXORPC_SPEC_VERSION})"
git clone --quiet "$UTXORPC_SPEC_REPO" "$work_dir"
git -C "$work_dir" checkout --quiet "$UTXORPC_SPEC_REF"
resolved_sha="$(git -C "$work_dir" rev-parse HEAD)"

for pkg in "${PACKAGES[@]}"; do
    src="$work_dir/proto/utxorpc/v1/$pkg"
    if [ ! -d "$src" ]; then
        echo "error: $src not found at ${UTXORPC_SPEC_REF} -- spec layout may have changed" >&2
        exit 1
    fi
    mkdir -p "$dest_root/$pkg"
    cp "$src"/*.proto "$dest_root/$pkg/"
done

cat > "$dest_root/VENDORED.md" <<EOF
Vendored from $UTXORPC_SPEC_REPO
ref: $UTXORPC_SPEC_REF (resolved: $resolved_sha)
label: $UTXORPC_SPEC_VERSION
packages: ${PACKAGES[*]}

Regenerate with scripts/fetch-utxorpc-protos.sh (edit UTXORPC_SPEC_REF
at the top of that script to upgrade).
EOF

echo "vendored ${PACKAGES[*]} into $dest_root (resolved $resolved_sha)"
