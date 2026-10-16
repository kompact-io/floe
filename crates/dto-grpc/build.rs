//! Compiles floe's own proto fork (`proto/floe/v1/`, see
//! `docs/utxorpc-divergences.md` for how it relates to the pristine
//! upstream copy under `docs/utxorpc-spec/`) into Rust via
//! `tonic-prost-build`.
//!
//! Uses `protox` (a pure-Rust protobuf parser) instead of shelling out
//! to a system `protoc` binary: there's no `protoc` on `PATH` in this
//! repo's dev/CI/Nix environments, and a pure-Rust build-dependency
//! keeps `nix build` hermetic without needing to plumb a protoc
//! derivation through `flake.nix`.
//!
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = Path::new("proto");
    let files = [
        proto_root.join("floe/v1/cardano.proto"),
        proto_root.join("floe/v1/query.proto"),
        proto_root.join("floe/v1/submit.proto"),
        proto_root.join("floe/v1/health.proto"),
    ];

    let fds = protox::compile(&files, [proto_root])?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // `GovernanceAction`'s largest variant (680 bytes) dwarfs its
        // second-largest (104 bytes) -- box it so the enum's own size
        // doesn't balloon to fit a variant every other caller pays for
        // but rarely uses (clippy::large_enum_variant).
        .boxed(".floe.v1.cardano.GovernanceAction.governance_action.parameter_change_action")
        .compile_fds(fds)?;

    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
    Ok(())
}
