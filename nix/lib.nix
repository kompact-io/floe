# Pure: given floe's `run` config, returns its CLI argv (excluding the
# binary) as a list of strings. Mirrors `RunArgs` in
# crates/server/src/main.rs. Shared by the systemd module
# (./modules/floe.nix) and any process-compose wiring, so both build the
# same argv shape.
{ lib }:
{
  mkArgs =
    { db
    , peer
    , magic
    , seed ? null
    , grpc ? null
    , http ? null # the REST transport (`floe::http::serve`), see ../plan-dto-http.md
    , metrics ? null
    , amaruSubmitApi ? null
    , maxPageSize ? 1000
    , k ? 32400 # ~1.5 epochs' worth of blocks -- see `RunArgs::k`'s own doc comment.
    , margin ? 500
    , pruneInterval ? 100
    }:
    [ "run" "--db" db "--peer" peer "--magic" (toString magic) ]
    ++ lib.optionals (seed != null) [ "--seed" seed ]
    ++ lib.optionals (grpc != null) [ "--grpc" grpc ]
    ++ lib.optionals (http != null) [ "--http" http ]
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
}
