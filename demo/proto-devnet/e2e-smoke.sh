#!/usr/bin/env bash
# Automated end-to-end smoke run of the two-lane dynamic-pricing prototype.
#
# Boots a fully isolated proto-devnet (own working dir, own demo dir, own
# ports — it can run NEXT TO a live demo), drives it with the actor feeder,
# and asserts the four behaviours that make the mechanism real:
#
#   1. the chain advances            (blocks forge, the tailer follows)
#   2. prices move                   (the urgent quote leaves its genesis rate)
#   3. an endorser block certifies   (the patient lane's payments land)
#   4. the fee split executes        (treasury donations AND refunds are
#                                     measured non-zero in the ledger's pots)
#
# Exit 0 on PASS, 1 on FAIL/timeout. Tunables: E2E_BLOCKS (default 8),
# E2E_TIMEOUT_S (default 600).
set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WS="$(cd "$SOURCE_DIR/../../../.." && pwd)"

: "${E2E_BLOCKS:=8}"
: "${E2E_TIMEOUT_S:=600}"

# The locally built node and feeder, exactly as launch-demo.sh wires them.
NODE_DIR="$WS/repos/cardano-node/dist-newstyle/build/aarch64-osx/ghc-9.6.7/cardano-node-11.0.1.164/x/cardano-node/build/cardano-node"
CLI_DIR=/nix/store/669b6xwwyiggbh3pghwdl9filspdqlvn-cardano-cli-exe-cardano-cli-11.0.0.0/bin
FEEDER="$WS/repos/cardano-node/dist-newstyle/build/aarch64-osx/ghc-9.6.7/tx-generator-2.16/x/dijkstra-lane-feeder/build/dijkstra-lane-feeder/dijkstra-lane-feeder"

# Everything isolated: a live demo on the default ports is left untouched.
WORKING_DIR="${TMPDIR:-/tmp}/dijkstra-e2e-$$"
DEMO_DIR="$(mktemp -d)"
HTTP_PORT=8791
LIVE="$DEMO_DIR/live.ndjson"
INCENTIVES="$DEMO_DIR/incentives.ndjson"

supervisor_pid=""
cleanup() {
  trap - INT TERM EXIT
  if [ -n "$supervisor_pid" ] && kill -0 "$supervisor_pid" 2>/dev/null; then
    kill -INT "$supervisor_pid" 2>/dev/null || true
    for _ in $(seq 1 20); do kill -0 "$supervisor_pid" 2>/dev/null || break; sleep 1; done
    kill -TERM "$supervisor_pid" 2>/dev/null || true
  fi
  rm -rf "$WORKING_DIR" "$DEMO_DIR"
}
trap cleanup INT TERM EXIT

echo "e2e: booting an isolated devnet (workdir $WORKING_DIR, ports 4001-4003, http $HTTP_PORT)"
(
  cd "$SOURCE_DIR"
  WORKING_DIR="$WORKING_DIR" DEMO_DIR="$DEMO_DIR" HTTP_PORT="$HTTP_PORT" \
  PORT_NODE1=4001 PORT_NODE2=4002 PORT_NODE3=4003 \
  METRICS_PORT_NODE1=13901 METRICS_PORT_NODE2=13902 METRICS_PORT_NODE3=13903 \
  OPEN_BROWSER=0 PC_DISABLE_TUI=1 LANE_FEEDER="$FEEDER" \
  exec nix shell nixpkgs#process-compose nixpkgs#yq-go --command bash -c "
    export PATH=\"$NODE_DIR:$CLI_DIR:\$PATH\"
    exec bash run-dijkstra-live-demo.sh
  "
) >"${WORKING_DIR}.e2e.log" 2>&1 &
supervisor_pid=$!

blocks=no; repriced=no; certified=no; split=no
deadline=$(( $(date +%s) + E2E_TIMEOUT_S ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  sleep 10
  kill -0 "$supervisor_pid" 2>/dev/null || { echo "e2e: FAIL — the devnet died (see ${WORKING_DIR}.e2e.log)"; exit 1; }
  [ -s "$LIVE" ] || continue
  verdict=$(python3 - "$LIVE" "$INCENTIVES" "$E2E_BLOCKS" <<'PY'
import json, sys
live_path, inc_path, want = sys.argv[1], sys.argv[2], int(sys.argv[3])
def rows(path):
    try:
        return [json.loads(l) for l in open(path) if l.strip()]
    except (FileNotFoundError, json.JSONDecodeError):
        return []
live, inc = rows(live_path), rows(inc_path)
blocks = bool(live) and live[-1].get("i", 0) >= want
repriced = any(r.get("urgent") not in (None, 16 * 44) for r in live)
certified = any(r.get("certIn") for r in live)
split = bool(inc) and inc[-1].get("donation", 0) > 0 and inc[-1].get("refundsDelivered", 0) > 0
print(f"{'y' if blocks else 'n'}{'y' if repriced else 'n'}{'y' if certified else 'n'}{'y' if split else 'n'}",
      live[-1].get("i", 0) if live else 0)
PY
)
  flags="${verdict%% *}"; tip="${verdict##* }"
  [ "${flags:0:1}" = y ] && blocks=yes
  [ "${flags:1:1}" = y ] && repriced=yes
  [ "${flags:2:1}" = y ] && certified=yes
  [ "${flags:3:1}" = y ] && split=yes
  echo "e2e: block $tip — chain:$blocks reprice:$repriced certified:$certified fee-split:$split"
  if [ "$blocks$repriced$certified$split" = "yesyesyesyes" ]; then
    echo "e2e: PASS — chain advanced to block $tip, prices moved, an endorser block certified, and the fee split showed up in the measured pots."
    exit 0
  fi
done

echo "e2e: FAIL — after ${E2E_TIMEOUT_S}s: chain:$blocks reprice:$repriced certified:$certified fee-split:$split (log: ${WORKING_DIR}.e2e.log)"
exit 1
