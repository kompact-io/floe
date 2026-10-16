# NixOS module: `services.floe`, syncing against a sibling amaru node as
# its `peer`. Assumes a TLS-terminating reverse proxy (e.g. Caddy) sits in
# front of the gRPC port. For process-compose or another orchestrator,
# build argv via `lib.mkArgs` (../lib.nix) directly against its own
# process schema.
{ lib, config, pkgs, ... }:
let
  inherit (lib) mkOption mkEnableOption mkIf types;
  cfg = config.services.floe;
  mkArgs = (import ../lib.nix { inherit lib; }).mkArgs;
  exe = lib.getExe' cfg.package "floe";
in
{
  options.services.floe = {
    enable = mkEnableOption "floe Cardano UTxO index";
    package = mkOption { type = types.package; description = "The floe package (this flake's `packages.floe`)."; };
    db = mkOption { type = types.str; description = "Path to the SQLite index file (created if it doesn't exist)."; };
    peer = mkOption { type = types.str; description = "Upstream peer to sync from, host:port (the amaru node this deploys alongside)."; };
    magic = mkOption { type = types.ints.unsigned; description = "Network magic (mainnet=764824073, preprod=1, preview=2)."; };
    seed = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "Amaru bootstrap snapshot archive (<slot>.<hash>.tar.zst). Skipped once already seeded.";
    };
    grpc = mkOption {
      type = types.nullOr types.str;
      default = "127.0.0.1:50051";
      description = "host:port to serve the gRPC QueryService on, or null to disable it.";
    };
    http = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "host:port to serve the REST transport (QueryService/SubmitService over HTTP) on, independent of grpc/metrics.";
    };
    metrics = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "host:port to serve /health and /metrics (Prometheus text) on.";
    };
    amaruSubmitApi = mkOption {
      type = types.nullOr types.str;
      default = null;
      description = "amaru's HTTP Cardano Submit API, host:port. When set, also mounts SubmitTx on grpc and/or http.";
    };
    maxPageSize = mkOption { type = types.ints.positive; default = 1000; description = "Hard ceiling on SearchUtxos/UtxoChanges page sizes."; };
    k = mkOption { type = types.ints.positive; default = 2160; description = "Reorg depth."; };
    margin = mkOption { type = types.ints.unsigned; default = 500; description = "Margin added to k before switching out of catch-up."; };
    pruneInterval = mkOption { type = types.ints.positive; default = 100; description = "Blocks between running-mode prunes."; };
    log = mkOption { type = types.str; default = "info"; description = "RUST_LOG filter."; };
    user = mkOption { type = types.str; default = "floe"; };
    group = mkOption { type = types.str; default = "floe"; };

    smoke = {
      enable = mkEnableOption "a floe-smoke oneshot service (crates/client/src/bin/smoke.rs), for deploy tooling to trigger via `systemctl start floe-smoke` -- not wanted by any target, since it asserts against *this* deployment's live data rather than running continuously";
      package = mkOption {
        type = types.package;
        description = "The floe-smoke package (this flake's `packages.floe-smoke`).";
      };
      grpc = mkOption {
        type = types.nullOr types.str;
        default = cfg.grpc;
        description = "floe-smoke's --grpc. Defaults to services.floe.grpc.";
      };
      httpJson = mkOption {
        type = types.nullOr types.str;
        default = cfg.http;
        description = "floe-smoke's --http-json. Defaults to services.floe.http.";
      };
      httpCbor = mkOption {
        type = types.nullOr types.str;
        default = cfg.http;
        description = "floe-smoke's --http-cbor. Defaults to services.floe.http.";
      };
      skeyFile = mkOption {
        type = types.nullOr types.path;
        default = null;
        description = "EnvironmentFile providing FLOE_SMOKE_SKEY.";
      };
      extraArgs = mkOption {
        type = types.listOf types.str;
        default = [ ];
        description = "Extra floe-smoke flags, e.g. [ \"--pay-cred\" \"...\" \"--ready-timeout-secs\" \"60\" ].";
      };
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = !cfg.smoke.enable || cfg.smoke.grpc != null || cfg.smoke.httpJson != null || cfg.smoke.httpCbor != null;
        message = "services.floe.smoke.enable requires grpc and/or http to be set (to derive smoke.grpc/smoke.httpJson/smoke.httpCbor), or one of those set explicitly.";
      }
    ];

    users.users.${cfg.user} = mkIf (cfg.user == "floe") {
      isSystemUser = true;
      group = cfg.group;
    };
    users.groups.${cfg.group} = mkIf (cfg.group == "floe") { };

    systemd.services.floe = {
      description = "floe: minimal Cardano UTxO index";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      environment.RUST_LOG = cfg.log;
      preStart = ''mkdir -p "$(dirname ${lib.escapeShellArg cfg.db})"'';
      serviceConfig = {
        User = cfg.user;
        Group = cfg.group;
        ExecStart = "${exe} ${lib.escapeShellArgs (mkArgs {
          inherit (cfg) db peer magic seed grpc http metrics amaruSubmitApi k margin pruneInterval;
          maxPageSize = cfg.maxPageSize;
        })}";
        Restart = "on-failure";
        RestartSec = 5;
      };
    };

    systemd.services.floe-smoke = mkIf cfg.smoke.enable {
      description = "floe-smoke: deployment smoke test against services.floe";
      after = [ "floe.service" ];
      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        EnvironmentFile = mkIf (cfg.smoke.skeyFile != null) cfg.smoke.skeyFile;
        ExecStart = "${lib.getExe' cfg.smoke.package "floe-smoke"} ${lib.escapeShellArgs (
          lib.optionals (cfg.smoke.grpc != null) [ "--grpc" cfg.smoke.grpc ]
          ++ lib.optionals (cfg.smoke.httpJson != null) [ "--http-json" cfg.smoke.httpJson ]
          ++ lib.optionals (cfg.smoke.httpCbor != null) [ "--http-cbor" cfg.smoke.httpCbor ]
          ++ cfg.smoke.extraArgs
        )}";
      };
    };
  };
}
