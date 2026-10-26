#!/usr/bin/env bash
# On-demand repro for the catchup->running phase-boundary stall (floe went
# silent on mindex-test right after "switched to running mode" on
# 2026-10-26; systemd showed it alive but every thread parked, and the
# chainsync/blockfetch socket to amaru had gone dead at that exact
# timestamp). Not wired into CI or `cargo test` -- this needs a real
# deployed amaru peer and SSH access to a NixOS test host, so it only
# ever runs when a human invokes it.
#
# What it does:
#   1. `nix build .#floe` locally, for whatever's currently checked out.
#   2. `nix copy` that closure to the remote host over SSH.
#   3. Takes a fresh copy of a pre-captured "fixture" DB that's deep in
#      Catchup mode, a few thousand blocks short of the k+margin
#      boundary (see "Making a new fixture" below) -- so each run only
#      has to chew through a short, fast tail of real catch-up before
#      crossing into Running, rather than reseeding from scratch.
#   4. Launches `floe run` against that copy with NO --seed (it resumes
#      from the fixture's own cached header) as a transient systemd unit.
#   5. Watches the journal for the mode switch, then watches for further
#      progress for a while afterward -- PASS if it keeps applying
#      blocks/moving on the wire past the switch, FAIL if it goes silent
#      the way it did in production.
#
# Usage:
#   scripts/repro-catchup-boundary.sh [--watch-secs N]
#
# Config (env vars, all optional):
#   FLOE_TEST_HOST       default: 185.42.223.27
#   FLOE_TEST_SSH_KEY    default: <repo>/.ssh/claude
#   FLOE_TEST_PEER       default: 127.0.0.1:3001   (amaru on the test host)
#   FLOE_TEST_MAGIC      default: 1                (preprod)
#   FLOE_TEST_SUBMIT_API default: 127.0.0.1:8090
#   FLOE_TEST_FIXTURE    default: /var/lib/floe-fixtures/near-boundary.sqlite
#
# Making a new fixture (the current one was captured 2026-10-26, will go
# stale as the chain moves on -- `floe tip` on it will show a `depth`
# that keeps growing, which just means more real catch-up per run, not
# that it's broken):
#   1. Run a normal seeded ad-hoc instance against the test peer.
#   2. Watch its `catch-up progress` logs; once `depth` is a few
#      thousand above k+margin (2660 by default), `systemctl stop` it.
#   3. Confirm with `floe tip --db <that db>` that `mode=Catchup`.
#   4. Copy it to FLOE_TEST_FIXTURE (chmod 444 so a slipped write can't
#      silently advance it) and clean up the ad-hoc DB.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

HOST="${FLOE_TEST_HOST:-185.42.223.27}"
SSH_KEY="${FLOE_TEST_SSH_KEY:-$REPO_ROOT/.ssh/claude}"
PEER="${FLOE_TEST_PEER:-127.0.0.1:3001}"
MAGIC="${FLOE_TEST_MAGIC:-1}"
SUBMIT_API="${FLOE_TEST_SUBMIT_API:-127.0.0.1:8090}"
FIXTURE="${FLOE_TEST_FIXTURE:-/var/lib/floe-fixtures/near-boundary.sqlite}"
WATCH_SECS=180

while [[ $# -gt 0 ]]; do
  case "$1" in
    --watch-secs) WATCH_SECS="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

SSH_OPTS=(-i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/tmp/known_hosts_floe_repro -o ConnectTimeout=8)
ssh_remote() { ssh "${SSH_OPTS[@]}" "root@$HOST" "$@"; }

RUN_ID="floe-repro-$(date +%s)"
REMOTE_DB="/var/lib/floe-adhoc/${RUN_ID}.sqlite"

echo "==> building floe (nix build .#floe)"
cd "$REPO_ROOT"
STORE_PATH="$(nix build .#floe --extra-experimental-features 'nix-command flakes' --print-out-paths --no-link)"
echo "    $STORE_PATH"

echo "==> copying closure to root@$HOST"
NIX_SSHOPTS="-i $SSH_KEY -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/tmp/known_hosts_floe_repro" \
  nix copy --extra-experimental-features 'nix-command flakes' --to "ssh://root@$HOST" "$STORE_PATH"

echo "==> staging fixture copy at $REMOTE_DB"
ssh_remote "mkdir -p /var/lib/floe-adhoc && cp '$FIXTURE' '$REMOTE_DB'"

FIXTURE_TIP="$(ssh_remote "$STORE_PATH/bin/floe tip --db '$REMOTE_DB'")"
echo "    fixture tip: $FIXTURE_TIP"
if [[ "$FIXTURE_TIP" != *"mode=Catchup"* ]]; then
  echo "!! fixture is not in Catchup mode -- it's stale or already crossed the boundary." >&2
  echo "!! see the 'Making a new fixture' section in this script's header." >&2
  ssh_remote "rm -f '$REMOTE_DB'*"
  exit 1
fi

echo "==> launching $RUN_ID"
ssh_remote "systemd-run --unit='$RUN_ID' --setenv=RUST_LOG=info \
  '$STORE_PATH/bin/floe' run \
  --db '$REMOTE_DB' \
  --peer '$PEER' --magic '$MAGIC' \
  --amaru-submit-api '$SUBMIT_API' \
  --max-page-size 1000 --k 2160 --margin 500 --prune-interval 100"

cleanup() {
  echo "==> cleaning up ($RUN_ID)"
  # `*` also catches SQLite's `-wal`/`-shm` sidecar files, which a bare
  # `rm -f "$REMOTE_DB"` leaves behind as orphans.
  ssh_remote "systemctl stop '$RUN_ID' 2>/dev/null; systemctl reset-failed '$RUN_ID' 2>/dev/null; rm -f '$REMOTE_DB'*" || true
}
trap cleanup EXIT

echo "==> waiting for the catchup->running switch (journalctl -u $RUN_ID -f)"
# Two footguns here under `set -eo pipefail`: (1) a bare pipeline statement
# aborts the whole script immediately on a non-zero exit, before any `$?`
# capture on the next line ever runs -- only the condition of an `if` is
# exempt; (2) `grep -m1` matching and closing the pipe early SIGPIPEs the
# ssh/journalctl side, which pipefail would otherwise mistake for failure
# even though grep itself found the line. Disabling pipefail makes the
# pipeline's status just grep's own, and putting it in the `if` keeps
# that from killing the script either way.
set +o pipefail
if ! timeout 120 ssh "${SSH_OPTS[@]}" "root@$HOST" "journalctl -u '$RUN_ID' -f --no-pager" | grep -q -m1 "switched to running mode"; then
  set -o pipefail
  echo "FAIL: never reached the mode switch within 120s" >&2
  exit 1
fi
set -o pipefail
echo "    crossed into Running mode"

SWITCH_AT="$(date +%s)"
echo "==> watching for ${WATCH_SECS}s past the switch for continued progress"
sleep "$WATCH_SECS"

MAIN_PID="$(ssh_remote "systemctl show -p MainPID --value '$RUN_ID'")"
# Scope strictly to this run's own PID -- the real production floe.service
# (still stalled from the 2026-10-26 incident, untouched by this script)
# holds its own long-idle socket to the same peer address, which a bare
# `grep "$PEER"` would also match and could be mistaken for this run's.
SOCK_IDLE="$(ssh_remote "ss -tinp 2>/dev/null | grep -A1 \"pid=$MAIN_PID,\" | grep -oP 'lastrcv:\\K[0-9]+' | sort -n | head -1 || true")"

echo "    main pid: $MAIN_PID"
if [[ -n "$SOCK_IDLE" ]]; then
  echo "    socket to peer: lastrcv ${SOCK_IDLE}ms ago"
fi

ELAPSED_MS=$(( (WATCH_SECS) * 1000 ))
if [[ -n "$SOCK_IDLE" && "$SOCK_IDLE" -gt "$ELAPSED_MS" ]]; then
  echo "FAIL: socket to peer has been idle longer than this whole watch window -- reproduced the stall." >&2
  echo "      unit left running as '$RUN_ID' for inspection; re-run with cleanup disabled by commenting out the trap if you want to attach gdb." >&2
  exit 1
fi

echo "PASS: still exchanging data with the peer ${WATCH_SECS}s after crossing into Running mode."
