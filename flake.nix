{
  description = "The last chunk of the Cardano chain";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
    # Only needed for wasm32-unknown-unknown; nixpkgs' own rustc only ships
    # the host's target, so that std has to come from somewhere that lets
    # you pick targets a la carte.
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, crane, flake-utils, fenix }:
    (flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};

        # Plain `cleanCargoSource` drops `tests/data/*.hex`/`*.bin` --
        # non-Rust, non-TOML, so its default filter doesn't know
        # `decode.rs`'s/`tvar.rs`'s `include_bytes!`/`include_str!` need
        # them. Only matters for `--all-targets` (clippy, `cargo test`
        # without `--bin`/`--example`); plain `--bin floe` doesn't touch
        # those tests so the gap was invisible until clippy ran as a nix
        # build rather than directly via `cargo clippy` in the dev shell.
        # `commonCargoSources` itself already covers the whole workspace
        # (every member's Cargo.toml + src, from the root Cargo.toml/
        # Cargo.lock) -- only these non-Rust inputs need listing by hand:
        # the fixture directory above, and `dto-grpc`'s `.proto` sources
        # (`build.rs` reads them at compile time via `protox`).
        src = nixpkgs.lib.fileset.toSource {
          root = ./.;
          fileset = nixpkgs.lib.fileset.unions [
            ((crane.mkLib pkgs).fileset.commonCargoSources ./.)
            ./crates/server/tests/data
            ./crates/dto-grpc/proto
          ];
        };

        # One crate set (floe + the two probe examples + their checks),
        # parameterized by craneLib so the same definitions serve both the
        # host toolchain here and the wasm one below -- nothing about
        # *what* gets built differs, only *which compiler* builds it.
        mkCrateSet = { craneLib, extraArgs ? { } }:
          let
            commonArgs = {
              inherit src;
              strictDeps = true;
              # buildInputs = [ pkgs.sqlite ];  # once rusqlite is linked non-bundled
              # Root `Cargo.toml` is now the virtual workspace manifest
              # (no `[package]` table), so crane can't infer a name/
              # version from it the way it does for a single package --
              # this is just a label for the shared deps-only cache
              # artifact below, not tied to any one member.
              pname = "floe-workspace";
              version = "0.1.0";
              cargoToml = ./Cargo.toml;
              cargoLock = ./Cargo.lock;
            } // extraArgs;
            cargoArtifacts = craneLib.buildDepsOnly commonArgs;
            mkPkg = { pname, cargoExtraArgs }:
              craneLib.buildPackage (commonArgs // { inherit pname cargoArtifacts cargoExtraArgs; version = "0.1.0"; });
          in
          rec {
            inherit commonArgs cargoArtifacts craneLib;
            chainsync-probe = mkPkg { pname = "chainsync-probe"; cargoExtraArgs = "--locked -p floe-server --example chainsync_probe"; };
            seed-probe = mkPkg { pname = "seed-probe"; cargoExtraArgs = "--locked -p floe-server --example seed_probe"; };
            floe = mkPkg { pname = "floe"; cargoExtraArgs = "--locked -p floe-server --bin floe"; };
            floe-client = mkPkg { pname = "floe-client"; cargoExtraArgs = "--locked -p floe-client --bin floe-client"; };
            floe-smoke = mkPkg { pname = "floe-smoke"; cargoExtraArgs = "--locked -p floe-client --bin floe-smoke"; };
            checks = {
              inherit chainsync-probe seed-probe floe floe-client floe-smoke;
              clippy = craneLib.cargoClippy (commonArgs // {
                inherit cargoArtifacts;
                cargoClippyExtraArgs = "--all-targets -- --deny warnings";
              });
            };
          };

        native = mkCrateSet { craneLib = crane.mkLib pkgs; };

        # wasm32-unknown-unknown: no C toolchain needed (no native deps),
        # just fenix's std for the target.
        wasmCraneLib = (crane.mkLib pkgs).overrideToolchain (fenix.packages.${system}.combine [
          fenix.packages.${system}.stable.toolchain
          fenix.packages.${system}.targets.wasm32-unknown-unknown.stable.rust-std
        ]);
        wasmCommonArgs = native.commonArgs // {
          pname = "floe-client-wasm";
          version = "0.1.0";
          CARGO_BUILD_TARGET = "wasm32-unknown-unknown";
          cargoExtraArgs = "--locked -p floe-client --lib --no-default-features --features wasm";
          # Default checkPhase runs the test binary on the build host --
          # impossible for a wasm32 artifact (no runtime here).
          doCheck = false;
        };
        wasmCargoArtifacts = wasmCraneLib.buildDepsOnly wasmCommonArgs;
        # Default install phase picks up the cdylib fine, at
        # `$out/lib/floe_client.wasm`.
        floe-client-wasm-raw = wasmCraneLib.buildPackage (wasmCommonArgs // {
          cargoArtifacts = wasmCargoArtifacts;
        });
        # Must match `wasm-bindgen`'s version exactly (see Cargo.toml).
        floe-client-wasm = pkgs.runCommand "floe-client-wasm" { nativeBuildInputs = [ pkgs.wasm-bindgen-cli ]; } ''
          wasm-bindgen --target web --out-dir "$out" "${floe-client-wasm-raw}/lib/floe_client.wasm"
        '';

        floe-web-dashboard = pkgs.runCommand "floe-web-dashboard" { } ''
          mkdir -p "$out"
          cp ${./web/shared.css} "$out/shared.css"
          cp -r ${./web/dashboard}/. "$out/"
        '';
        floe-web-demo = pkgs.runCommand "floe-web-demo" { } ''
          mkdir -p "$out/pkg"
          cp ${./web/shared.css} "$out/shared.css"
          cp -r ${./web/demo}/. "$out/"
          cp -r ${floe-client-wasm}/. "$out/pkg/"
        '';
      in
      {
        packages = {
          default = native.floe;
          inherit (native) chainsync-probe seed-probe floe floe-client floe-smoke;
          inherit floe-client-wasm floe-web-dashboard floe-web-demo;
        };

        apps.default = {
          type = "app";
          program = "${native.floe}/bin/floe";
        };

        checks = native.checks // { fmt = (crane.mkLib pkgs).cargoFmt { inherit src; }; };

        devShells.default = native.craneLib.devShell {
          checks = self.checks.${system};
          packages = [ pkgs.rust-analyzer pkgs.sqlite  pkgs.wasm-bindgen-cli ];
          # jemalloc's bundled `configure` probes compile at -O0 (debug
          # profile) under `-Werror`; glibc's features.h treats
          # _FORTIFY_SOURCE at -O0 as a hard warning, which then aborts
          # every configure check (first symptom: "cannot determine
          # return type of strerror_r"). Nix's cc-wrapper injects
          # _FORTIFY_SOURCE unconditionally via NIX_HARDENING_ENABLE, so
          # disable just that flag here -- `nix build`'s release profile
          # compiles at -O3 and never hits this, so it's left alone.
          hardeningDisable = [ "fortify" "fortify3" ];
        };
      }))
    # Not per-system: a plain function (lib.mkArgs, ./nix/lib.nix) and a
    # NixOS module (nixosModules.default, ./nix/modules/floe.nix —
    # services.floe.*). A process-compose or other orchestrator builds
    # argv via lib.mkArgs directly, against its own process schema.
    // {
      lib.mkArgs = (import ./nix/lib.nix { inherit (nixpkgs) lib; }).mkArgs;
      lib.mkSmokeArgs = (import ./nix/lib.nix { inherit (nixpkgs) lib; }).mkSmokeArgs;
      nixosModules.default = import ./nix/modules/floe.nix;
      nixosModules.floe = self.nixosModules.default;
    };
}
