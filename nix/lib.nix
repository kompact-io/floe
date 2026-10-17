# Pure: given floe's/floe-smoke's own config, returns its CLI argv
# (excluding the binary) as a list of strings. `mkArgs` mirrors `RunArgs`
# in crates/server/src/main.rs; `mkSmokeArgs` mirrors floe-smoke's own
# `Cli` (crates/client/src/bin/smoke/main.rs), which deliberately uses
# the identical `--grpc`/`--http`+`--http-json`/`--http-cbor` shape.
# Shared by the systemd module (./modules/floe.nix) and any
# process-compose wiring, so both build the same argv shape -- that's
# also why this stays a plain function returning argv, not anything
# systemd-specific (`EnvironmentFile`, `extraArgs`'s own free-form
# flags, etc. stay the module's job).
{ lib }:
{
  mkArgs =
    { db
    , peer
    , magic
    , seed ? null
    , grpc ? null
    # `http`: the one host:port the http transport listens on --
    # `httpJson`/`httpCbor` don't take addresses of their own (unlike
    # `grpc`); they just pick which wire format(s) that one listener
    # accepts, negotiated per request via `Accept` (see
    # `serve::scheme::http::query`'s own doc). At least one of
    # `httpJson`/`httpCbor` must be true when `http` is set.
    , http ? null
    , httpJson ? false
    , httpCbor ? false
    , metrics ? null
    , amaruSubmitApi ? null
    , maxPageSize ? 1000
    , k ? 32400 # ~1.5 epochs' worth of blocks -- see `RunArgs::k`'s own doc comment.
    , margin ? 500
    , pruneInterval ? 100
    }:
    assert http != null -> (httpJson || httpCbor);
    [ "run" "--db" db "--peer" peer "--magic" (toString magic) ]
    ++ lib.optionals (seed != null) [ "--seed" seed ]
    ++ lib.optionals (grpc != null) [ "--grpc" grpc ]
    ++ lib.optionals (http != null) [ "--http" http ]
    ++ lib.optionals (http != null && httpJson) [ "--http-json" ]
    ++ lib.optionals (http != null && httpCbor) [ "--http-cbor" ]
    ++ lib.optionals (metrics != null) [ "--metrics" metrics ]
    ++ lib.optionals (amaruSubmitApi != null) [ "--amaru-submit-api" amaruSubmitApi ]
    ++ [
      "--max-page-size"
      (toString maxPageSize)
      "--k"
      (toString k)
      "--margin"
      (toString margin)
      "--prune-interval"
      (toString pruneInterval)
    ];

  # `floe-smoke`'s own `--grpc`/`--http`+`--http-json`/`--http-cbor` --
  # same flags, same meaning, as `mkArgs`' above. `extraArgs` is where
  # `--skey`/`--ready-timeout-secs`/etc. and the smoke-specific search
  # flags (`--pay-hash`, `--submit`, ...) go -- orthogonal to which
  # scheme(s) get dialed.
  mkSmokeArgs =
    { grpc ? null
    , http ? null
    , httpJson ? false
    , httpCbor ? false
    , extraArgs ? [ ]
    }:
    assert http != null -> (httpJson || httpCbor);
    lib.optionals (grpc != null) [ "--grpc" grpc ]
    ++ lib.optionals (http != null) [ "--http" http ]
    ++ lib.optionals (http != null && httpJson) [ "--http-json" ]
    ++ lib.optionals (http != null && httpCbor) [ "--http-cbor" ]
    ++ extraArgs;
}
