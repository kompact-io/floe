# Divergences from UTxORPC

floe maintains its own fork of UTxORPC's `query`/`cardano`/`submit`
proto packages (`crates/dto-grpc/proto/floe/v1/{cardano,query,submit}.proto`,
packages `floe.v1.{cardano,query,submit}`) rather than building against
the spec unmodified. It stays as close to upstream's shapes as floe's
needs allow, diverging on purpose where they genuinely differ.

## The diff is the documentation

`docs/utxorpc-spec/utxorpc/v1/` is a pristine, uncompiled copy of
upstream at a pinned commit (see its `VENDORED.md`; regenerate with
`scripts/fetch-utxorpc-protos.sh`). It is never built and nothing
depends on it -- it exists only as the diff baseline for floe's own
fork. To see the actual, current, field-by-field divergence against
that pinned ref:

```sh
diff -u docs/utxorpc-spec/utxorpc/v1/cardano/cardano.proto \
        crates/dto-grpc/proto/floe/v1/cardano.proto
diff -u docs/utxorpc-spec/utxorpc/v1/query/query.proto \
        crates/dto-grpc/proto/floe/v1/query.proto
diff -u docs/utxorpc-spec/utxorpc/v1/submit/submit.proto \
        crates/dto-grpc/proto/floe/v1/submit.proto
```

(The `package` line and the `import` path for `cardano.proto` always
differ -- that's the repackaging, not a divergence. Everything else in
the diff is a real one.)

## Curated summary (as of this fork)

- **`cardano.proto`**: no divergence yet. Textually identical to
  upstream apart from the package line -- the chain-specific message
  set floe converts into hasn't needed to change.
- **`query.proto`**:
  - `AnyUtxoData` gains `optional bytes spent_by = 5` -- see the
    comment on that field for why (not a boolean, no separate
    `spent_slot`, never "unknown"). This used to be shipped as a
    `>= 900` wire-compatible field number against the unmodified
    upstream message (`crates/dto-grpc/floe-extensions.patch`, now
    deleted) under the theory that floe would track upstream exactly
    except for additive, ignorable extensions. That convention is
    reverted: floe is its own fork now, so the field just takes the
    next real field number.
  - Everything else in `query.proto` is currently unmodified. Partial
    support (`ReadTx` has no certs/mint/fee/collateral/withdrawals/
    auxiliary_data; `SearchUtxos`'s asset/cert branches and
    `exact_address` aren't supported; `ReadParams`/`ReadGenesis`/
    `ReadEraSummary`/`ReadState` aren't supported) is a server-side
    behavior difference, not a proto-shape one -- those RPCs and
    messages are declared exactly as upstream has them, and
    unsupported requests get a structured "not supported" error at
    call time rather than being absent from the schema. See the
    crate-level doc comment on `floe-server`'s `grpc.rs` for the
    authoritative list, and `TODO.md` for what's still open.
  - Two further floe-only additions: a `Point` message (reorg-safe
    "since X" filtering -- optional
    `slot`/`hash`, strict chain-validity check when `hash` is given,
    bare numeric filter otherwise) and `UtxoChanges`, a new RPC built
    on it (one-shot "what changed at this address since this `Point`"
    diff, `address` sharing `SearchUtxos`'s restrictions exactly).
    **Both landed** -- `message Point`, `message UtxoChangesRequest`,
    `message Changes`, `message UtxoChangesResponse`, `enum
    StaleReason`, `message Stale`, and the `rpc UtxoChanges` line on
    `QueryService`, all new relative to upstream (no equivalent
    concept exists in UTxORPC's `query.proto` at all). Implemented in
    `floe-server`'s `grpc.rs`/`store::query.rs`.
    - `UtxoChangesRequest`/`Changes` paginate `created`/`spent`
      independently (`max_items`/`created_start_token`/
      `spent_start_token`, `created_next_token`/`spent_next_token`),
      and a new `--max-page-size` flag on `floe-server` caps
      `max_items` on both `SearchUtxos` and `UtxoChanges` (previously
      an unconfigurable `1000` on `SearchUtxos` alone).
    - `UtxoChangesResponse.stale` is `message Stale{reason,
      oldest_retained_slot}`, not a bare `bool` -- `reason` is
      `ROLLED_BACK` (within retained history, hash just doesn't
      match -- a real reorg), `TOO_OLD` (predates the oldest retained
      block -- gone regardless of fork status), or `UNSPECIFIED`
      (only `hash` was given, nothing to classify against).
    - Redeemer support is still not implemented (see `TODO.md`).

- **`submit.proto`**: forked (`crates/dto-grpc/proto/floe/v1/submit.proto`,
  package `floe.v1.submit`). Textually identical to upstream apart
  from the package/import repackaging -- the message and RPC set
  itself hasn't diverged. `floe-server`'s `SubmitService` impl
  (`crates/server/src/submit.rs`) implements `SubmitTx` only;
  `EvalTx`/`WaitForTx`/`ReadMempool`/`WatchMempool` return
  `UNIMPLEMENTED` -- a server-side behavior difference, not a
  proto-shape one, same convention as `QueryService`'s partial
  support above.
