#!/usr/bin/env bash
# Live, never-ending two-lane dynamic-pricing demo on a real 3-node Dijkstra
# proto-devnet.
#
#   * starts the devnet (node1 is the one we observe),
#   * keeps two feeders running forever — one chains optimistic txs from the
#     first genesis UTxO, the other chains urgent txs from the second, so both
#     lanes stay congested at the same time,
#   * tails node1's forge traces into a live NDJSON stream,
#   * serves the dashboard, which polls that stream and updates as blocks forge.
#
# Press Ctrl-C to stop: the devnet, both feeders, the tailer and the web server
# are all torn down together.
#
# Tunables (all optional, sensible defaults):
#   FEE             flat lovelace fee each tx pays. This is the PRICE CEILING:
#                   a lane's quote can only climb until quote*txSize reaches the
#                   fee, so a bigger fee lets prices climb higher. Default 10 ADA.
#   METADATA_BYTES  payload size per tx. Smaller => more txs per block.
#   DELAY_MS        pause between submissions per feeder (0 = as fast as the
#                   mempool drains). Raise it to soften the load.
#   HTTP_PORT       dashboard port (default 8780).

set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${WORKING_DIR:=${TMPDIR:-/tmp}/dijkstra-live-demo}"
: "${LANE_FEEDER:=dijkstra-lane-feeder}"
: "${NETWORK_MAGIC:=164}"
: "${FEE:=10000000}"
: "${METADATA_BYTES:=2000}"
: "${DELAY_MS:=0}"
: "${CYCLES:=2000000}"
: "${DEMO_DIR:=${SOURCE_DIR}/../../../../organisation/06_prototype/demo}"
: "${HTTP_PORT:=8780}"
: "${STARTUP_TIMEOUT:=120}"
: "${PC_DISABLE_TUI:=1}"
: "${OPEN_BROWSER:=1}"
: "${ACTOR_MODE:=1}"
: "${ACTOR_BUCKET:=120}"
# CONFLICT_MODE=1 replaces the load with the cross-lane conflict generator: each
# cycle spends one input with both lanes, so the optimistic tx is evicted
# (AllInputsAreSpent). An aggregator turns the feeder's submission results into the
# dashboard's evictions.ndjson — real, classified drops.
: "${CONFLICT_MODE:=0}"
# INDEPENDENT_FUNDING=1 splits one UTxO into a pool and floods independent urgent
# txs (no chaining). A rising quote evicts the queued ones by re-validation, which
# the node traces (Mempool.RemoveTxs) — the tailer captures it into evictions.ndjson.
# Pair with a modest FEE (bid) and larger METADATA_BYTES so the RB saturates.
: "${INDEPENDENT_FUNDING:=0}"

RUN_LOG="${WORKING_DIR}.live-demo.log"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_command cardano-cli
require_command jq
require_command python3

if ! command -v "$LANE_FEEDER" >/dev/null 2>&1 && [ ! -x "$LANE_FEEDER" ]; then
  echo "Missing lane feeder executable: $LANE_FEEDER" >&2
  echo "Set LANE_FEEDER=/path/to/dijkstra-lane-feeder." >&2
  exit 1
fi

DEMO_DIR="$(cd "$DEMO_DIR" && pwd)"
LIVE_STREAM="${DEMO_DIR}/live.ndjson"
ACTOR_STREAM="${DEMO_DIR}/actors.ndjson"
EVICT_STREAM="${DEMO_DIR}/evictions.ndjson"
QUOTES_FILE="${DEMO_DIR}/latest-quotes.json"
ACTOR_CONFIG="${DEMO_DIR}/actor-config.json"
EVICTION_CONTROL="${DEMO_DIR}/eviction-control.json"
RUN_CONFIG="${DEMO_DIR}/run-config.json"

devnet_pid=""
tailer_pid=""
http_pid=""
optimistic_feeder_pid=""
urgent_feeder_pid=""
actor_feeder_pid=""
aggregator_pid=""
conflict_feeder_pid=""
evict_aggregator_pid=""
eviction_controller_pid=""
watchdog_pid=""
http_public_pid=""

compose_file="${WORKING_DIR}/process-compose.no-tx-centrifuge.yaml"

find_devnet_process_compose() {
  ps -axo pid=,command= |
    awk -v compose_file="$compose_file" '
      index($0, "process-compose --no-server") && index($0, compose_file) { print $1 }
    '
}

stop_everything() {
  trap - INT TERM EXIT
  echo ""
  echo "Stopping live demo (feeders, tailer, web server, devnet)..."
  for pid in "$optimistic_feeder_pid" "$urgent_feeder_pid" "$actor_feeder_pid" "$aggregator_pid" "$conflict_feeder_pid" "$evict_aggregator_pid" "$eviction_controller_pid" "$watchdog_pid" "$tailer_pid" "$http_pid" "${http_public_pid:-}"; do
    [ -n "$pid" ] && kill "$pid" >/dev/null 2>&1 || true
  done
  # The dashboard-controlled eviction generators run under the controller subshell;
  # their pids are recorded in files.
  local pid_file
  rm -f "$WORKING_DIR/withhold-votes.flag" 2>/dev/null || true
  for pid_file in "$WORKING_DIR"/evgen.pid "$WORKING_DIR"/evgen-aggregator.pid; do
    if [ -f "$pid_file" ]; then
      kill "$(cat "$pid_file")" >/dev/null 2>&1 || true
    fi
  done
  local pid
  for pid in $(find_devnet_process_compose); do
    kill -INT "$pid" >/dev/null 2>&1 || true
  done
  if [ -n "$devnet_pid" ] && kill -0 "$devnet_pid" >/dev/null 2>&1; then
    kill -INT "$devnet_pid" >/dev/null 2>&1 || true
  fi
  for _ in 1 2 3 4 5; do
    if [ -n "$(find_devnet_process_compose)" ]; then sleep 1; else break; fi
  done
  for pid in $(find_devnet_process_compose); do
    kill -TERM "$pid" >/dev/null 2>&1 || true
  done
  wait "$devnet_pid" >/dev/null 2>&1 || true
  echo "Stopped."
}

trap stop_everything INT TERM EXIT

if [ -d "$WORKING_DIR" ]; then
  chmod -R u+w "$WORKING_DIR" 2>/dev/null || true
  rm -rf "$WORKING_DIR"
fi
rm -f "$RUN_LOG"
: >"$LIVE_STREAM"
: >"$ACTOR_STREAM"
: >"$EVICT_STREAM"
: >"${DEMO_DIR}/removed-txs.ndjson"
: >"${DEMO_DIR}/evicted-txs.ndjson"
rm -f "$QUOTES_FILE"
rm -f "${DEMO_DIR}/leios-status.json"
rm -f "${DEMO_DIR}/lifecycle.json"
# Start from a default actor population (the dashboard rewrites this live).
cat >"$ACTOR_CONFIG" <<'JSON'
{"honest":60,"patient":20,"impatient":20,"valueMinAda":1,"valueMaxAda":30,"urgencyMin":0.02,"urgencyMax":0.35,"urgentLatency":1,"optimisticLatency":4,"reservationMultiple":1,"feeBuffer":1.2}
JSON

# Demo control: the patched node withholds its Leios votes while this flag file
# exists (LEIOS_WITHHOLD_VOTES_FILE). All three nodes inherit it, so raising the
# flag makes every EB miss its certification quorum until it is removed.
WITHHOLD_FLAG="$WORKING_DIR/withhold-votes.flag"
export LEIOS_WITHHOLD_VOTES_FILE="$WITHHOLD_FLAG"

echo "Starting proto-devnet in $WORKING_DIR"
(
  cd "$SOURCE_DIR"
  TX_CENTRIFUGE=0 \
    XRAY=0 \
    TC=0 \
    PC_DISABLE_TUI="$PC_DISABLE_TUI" \
    WORKING_DIR="$WORKING_DIR" \
    ./run.sh
) >"$RUN_LOG" 2>&1 &
devnet_pid=$!

socket="$WORKING_DIR/node1/node.socket"
deadline=$((SECONDS + STARTUP_TIMEOUT))
while [ ! -S "$socket" ]; do
  if ! kill -0 "$devnet_pid" >/dev/null 2>&1; then
    echo "proto-devnet exited before node socket was ready" >&2
    tail -100 "$RUN_LOG" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "Timed out waiting for node socket: $socket" >&2
    tail -100 "$RUN_LOG" >&2 || true
    exit 1
  fi
  sleep 1
done

export CARDANO_NODE_SOCKET_PATH="$socket"
deadline=$((SECONDS + STARTUP_TIMEOUT))
while ! cardano-cli query tip --testnet-magic "$NETWORK_MAGIC" 2>/dev/null | jq -e '.era == "Dijkstra"' >/dev/null; do
  if ! kill -0 "$devnet_pid" >/dev/null 2>&1; then
    echo "proto-devnet exited before node1 reached Dijkstra era" >&2
    tail -100 "$RUN_LOG" >&2 || true
    exit 1
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "Timed out waiting for Dijkstra era on node1" >&2
    cardano-cli query tip --testnet-magic "$NETWORK_MAGIC" >&2 || true
    exit 1
  fi
  sleep 1
done
echo "node1 reached the Dijkstra era."

# Defensive: clear any tailer/aggregator left over from a run that did not tear
# down cleanly, so we never get two writers appending to the same stream.
pkill -f "live-trace-tailer.py" >/dev/null 2>&1 || true
pkill -f "actor-aggregator.py" >/dev/null 2>&1 || true
pkill -f "eviction-aggregator.py" >/dev/null 2>&1 || true
pkill -f "demo-server.py" >/dev/null 2>&1 || true
# Nothing else may sit on the dashboard port: a stray file server would keep
# serving the page while silently swallowing every command (read-only trap).
lsof -ti tcp:"$HTTP_PORT" 2>/dev/null | xargs kill 2>/dev/null || true
sleep 1
: >"$LIVE_STREAM"
: >"$ACTOR_STREAM"
: >"${DEMO_DIR}/removed-txs.ndjson"
: >"${DEMO_DIR}/evicted-txs.ndjson"

# Stream every node's forge traces into the dashboard's live feed. Each block is
# forged by exactly one node, so merging the three logs gives the full sequence.
# Always capture real re-validation evictions (Mempool.RemoveTxs) into
# evictions.ndjson for the "Live drops" panel — the dashboard can switch a price
# flood on at any time, and the trace costs nothing when nothing is evicted.
python3 "$SOURCE_DIR/live-trace-tailer.py" --evictions "$EVICT_STREAM" "$LIVE_STREAM" \
  "$WORKING_DIR/node1/node.log" \
  "$WORKING_DIR/node2/node.log" \
  "$WORKING_DIR/node3/node.log" &
tailer_pid=$!

# Reset the dashboard-controlled eviction switch (the dashboard POSTs
# {"mode":"off"|"type1"|"type2"} to /eviction-control; the controller loop below
# starts/stops the matching generator).
printf '{"mode":"off"}\n' >"$EVICTION_CONTROL"

# Serve the dashboard. live.ndjson lives in the same directory, so the page can
# poll it from the same origin.
python3 "$SOURCE_DIR/demo-server.py" "$HTTP_PORT" "$DEMO_DIR" "$ACTOR_CONFIG" "$WORKING_DIR" >/dev/null 2>&1 &
http_pid=$!

# The audience copy: same dashboard, same live streams, commands refused —
# the page shows "watching mode". Tunnel THIS port to share the demo;
# the presenter keeps driving through $HTTP_PORT.
: "${HTTP_PUBLIC_PORT:=8781}"
lsof -ti tcp:"$HTTP_PUBLIC_PORT" 2>/dev/null | xargs kill 2>/dev/null || true
python3 "$SOURCE_DIR/demo-server.py" --read-only "$HTTP_PUBLIC_PORT" "$DEMO_DIR" "$ACTOR_CONFIG" "$WORKING_DIR" >/dev/null 2>&1 &
http_public_pid=$!

run_feeder_forever() {
  local label fund_index optimistic_first urgent_after feeder_log
  label="$1"
  fund_index="$2"
  optimistic_first="$3"
  urgent_after="$4"
  feeder_log="$WORKING_DIR/${label}-feeder.log"
  "$LANE_FEEDER" \
    --socket "$socket" \
    --funds "$WORKING_DIR/funds.json" \
    --network-magic "$NETWORK_MAGIC" \
    --fee "$FEE" \
    --metadata-bytes "$METADATA_BYTES" \
    --optimistic-first "$optimistic_first" \
    --urgent-after "$urgent_after" \
    --cycles "$CYCLES" \
    --fund-index "$fund_index" \
    --delay-ms "$DELAY_MS" \
    >"$feeder_log" 2>&1 &
}

if [ "$CONFLICT_MODE" = "1" ]; then
  FEED_MODE="conflict"
  echo "Starting cross-lane conflict generator (fee=${FEE} lovelace, metadata=${METADATA_BYTES} bytes, delay=${DELAY_MS}ms)"
  echo "  each cycle spends one input with both lanes; first-come: the optimistic tx is admitted and protected, the late urgent tx is rejected (AllInputsAreSpent)"
  conflict_log="$WORKING_DIR/conflict-feeder.log"
  "$LANE_FEEDER" \
    --socket "$socket" \
    --funds "$WORKING_DIR/funds.json" \
    --network-magic "$NETWORK_MAGIC" \
    --fee "$FEE" \
    --metadata-bytes "$METADATA_BYTES" \
    --cycles "$CYCLES" \
    --delay-ms "$DELAY_MS" \
    --conflict-mode \
    --fund-index 0 \
    >"$conflict_log" 2>&1 &
  conflict_feeder_pid=$!
  # The node returns admission rejections to the submitter but does not trace them,
  # so the aggregator reads the feeder's own results to fill the dashboard feed.
  python3 "$SOURCE_DIR/eviction-aggregator.py" "$conflict_log" "$EVICT_STREAM" &
  evict_aggregator_pid=$!
elif [ "$INDEPENDENT_FUNDING" = "1" ]; then
  FEED_MODE="independent"
  echo "Starting independent-funding load (bid=${FEE} lovelace, metadata=${METADATA_BYTES} bytes, delay=${DELAY_MS}ms)"
  echo "  splits one UTxO into a pool, then floods independent urgent txs; a rising quote evicts the queued ones (real Mempool.RemoveTxs)"
  "$LANE_FEEDER" \
    --socket "$socket" \
    --funds "$WORKING_DIR/funds.json" \
    --network-magic "$NETWORK_MAGIC" \
    --fee "$FEE" \
    --metadata-bytes "$METADATA_BYTES" \
    --cycles "$CYCLES" \
    --delay-ms "$DELAY_MS" \
    --independent-funding \
    --fund-index 0 \
    >"$WORKING_DIR/independent-feeder.log" 2>&1 &
  conflict_feeder_pid=$!
elif [ "$ACTOR_MODE" = "1" ]; then
  FEED_MODE="actor"
  echo "Starting actor-driven load (fee ceiling=${FEE} lovelace, metadata=${METADATA_BYTES} bytes, delay=${DELAY_MS}ms)"
  echo "  each actor buys urgent or optimistic by retained value, or walks away when neither lane pays off"
  start_actor_feeder() {
    # Extra args: --initial-txin/--initial-value (fund 0, optimistic chain) and
    # --initial-txin-2/--initial-value-2 (fund 1, urgent chain) on restarts.
    "$LANE_FEEDER" \
      --socket "$socket" \
      --funds "$WORKING_DIR/funds.json" \
      --network-magic "$NETWORK_MAGIC" \
      --fee "$FEE" \
      --metadata-bytes "$METADATA_BYTES" \
      --cycles "$CYCLES" \
      --delay-ms "$DELAY_MS" \
      --actor-mode \
      --fanout "${FANOUT:-12}" \
      --quotes-file "$QUOTES_FILE" \
      --actor-config "$ACTOR_CONFIG" \
      "$@" \
      >>"$WORKING_DIR/actor-feeder.log" 2>&1 &
    actor_feeder_pid=$!
  }
  start_actor_feeder
  python3 "$SOURCE_DIR/actor-aggregator.py" "$WORKING_DIR/actor-feeder.log" "$ACTOR_STREAM" "$ACTOR_BUCKET" \
    --removed "$DEMO_DIR/removed-txs.ndjson" --lifecycle "$DEMO_DIR/lifecycle.json" &
  aggregator_pid=$!
else
  FEED_MODE="fixed"
  echo "Starting sustained two-lane load (fee=${FEE} lovelace, metadata=${METADATA_BYTES} bytes, delay=${DELAY_MS}ms)"
  run_feeder_forever "optimistic" 0 1 0
  optimistic_feeder_pid=$!
  run_feeder_forever "urgent" 1 0 1
  urgent_feeder_pid=$!
fi

# ---- dashboard-controlled eviction generators --------------------------------
# The dashboard flips /eviction-control between off | type1 | type2; this loop
# starts/stops the matching generator NEXT TO the main feed, on its own genesis
# fund (index 1), so the story stays visible while evictions run:
#   type1  price flood: independent urgent txs with a modest bid; the rising
#          quote overtakes their bid and the node evicts the admitted backlog
#          (real Mempool.RemoveTxs -> tailer -> evictions.ndjson, stage "evicted").
#   type2  cross-lane conflicts: an optimistic tx is admitted and holds a coin,
#          then an urgent tx tries the same coin and is rejected (first-come;
#          feeder log -> aggregator -> evictions.ndjson, stage "rejected").
# Generators restart cleanly: before each start we ask the node for the fund's
# current largest UTxO and hand it to the feeder (--initial-txin/--initial-value).
: "${T1_BID:=auto}"   # auto = 1.8x the live urgent cost at scenario start
: "${T1_METADATA:=10000}"
: "${T1_DELAY_MS:=50}"
: "${T2_FEE:=10000000}"
: "${T2_METADATA:=2000}"
: "${T2_DELAY_MS:=300}"
EVGEN_FUND_INDEX=2

write_run_config() {
  # What the dashboard's "what is running" explainer reads. t1Bid/t1TxBytes
  # describe the CURRENT squeeze burst (0 when none runs) so the dashboard can
  # say at which quote the burst txs get priced out.
  printf '{"feed":"%s","feeLovelace":%s,"metadataBytes":%s,"delayMs":%s,"generator":"%s","t1Bid":%s,"t1TxBytes":%s}\n' \
    "$FEED_MODE" "$FEE" "$METADATA_BYTES" "$DELAY_MS" "${1:-off}" "${CURRENT_T1_BID:-0}" "$((T1_METADATA + 300))" >"${RUN_CONFIG}.tmp"
  mv "${RUN_CONFIG}.tmp" "$RUN_CONFIG"
}

fund_largest_utxo() {
  # $1 = utxo key number (1-based: utxo1, utxo2, ...).
  # -> "txid#ix lovelace" of the biggest UTxO at that fund's payment address.
  local n="$1" skey vkey addr json
  skey="$WORKING_DIR/utxo-keys/utxo${n}/utxo.skey"
  vkey="$WORKING_DIR/utxo-keys/utxo${n}/utxo.query.vkey"
  [ -f "$vkey" ] || cardano-cli key verification-key \
    --signing-key-file "$skey" --verification-key-file "$vkey" 2>/dev/null || return 1
  addr=$(cardano-cli address build --payment-verification-key-file "$vkey" \
    --testnet-magic "$NETWORK_MAGIC" 2>/dev/null) || return 1
  json=$(cardano-cli query utxo --address "$addr" \
    --testnet-magic "$NETWORK_MAGIC" --output-json 2>/dev/null) || return 1
  printf '%s\n' "$json" |
    jq -r 'to_entries
           | map({k: .key, v: (.value.value.lovelace // 0)})
           | sort_by(-.v) | .[0] // empty | "\(.k) \(.v)"'
}

evgen_largest_utxo() {
  fund_largest_utxo $((EVGEN_FUND_INDEX + 1))
}

eviction_controller() {
  local current="off" pid="" agg_pid="" want line stall_count=0 gen_log gen_lines
  local initial_args=()
  while :; do
    want=$(jq -r '.mode // "off"' "$EVICTION_CONTROL" 2>/dev/null || echo off)
    case "$want" in type1|type2|type3) ;; *) want="off" ;; esac
    if [ "$want" != "$current" ]; then
      echo "(eviction generator: $current -> $want)"
      [ -n "$pid" ] && kill "$pid" >/dev/null 2>&1 || true
      [ -n "$agg_pid" ] && kill "$agg_pid" >/dev/null 2>&1 || true
      pid=""; agg_pid=""
      rm -f "$WORKING_DIR/evgen.pid" "$WORKING_DIR/evgen-aggregator.pid"
      rm -f "$WITHHOLD_FLAG"
      if [ "$want" = "type3" ]; then
        # No feeder: raising the flag silences every node's Leios vote, so EBs
        # keep being forged but never reach their certification quorum.
        touch "$WITHHOLD_FLAG"
      elif [ "$want" != "off" ]; then
        sleep 15  # let in-flight generator txs land so the UTxO query is stable
        initial_args=()
        if line=$(evgen_largest_utxo) && [ -n "$line" ]; then
          initial_args=(--initial-txin "${line%% *}" --initial-value "${line##* }")
        fi
        if [ "$want" = "type1" ]; then
          # Calibrate the burst bid from the LIVE urgent quote: high enough to
          # be admitted now, low enough to be priced out fast — 1.35x today's
          # cost is crossed on the second +25% step (1.25^2 = 1.56), so the
          # burst's own full blocks price it out within ~2 blocks. A fixed
          # bid only works for one price regime; this works in all of them.
          t1_bid="$T1_BID"
          # A silent restart of the SAME scenario keeps its original bid: the
          # burst's own full blocks push the quote up, and recomputing 1.35x at
          # the climbed quote every relaunch would ratchet the bid upward and
          # never let the crossing happen.
          if [ "$t1_bid" = "auto" ] && [ "${CURRENT_T1_BID:-0}" != "0" ] && [ "${RELAUNCH_SAME:-0}" = "1" ]; then
            t1_bid="$CURRENT_T1_BID"
          elif [ "$t1_bid" = "auto" ]; then
            t1_bid=$(python3 -c "
import json, sys
try:
    quote = json.load(open('$QUOTES_FILE'))['urgent']
except Exception:
    quote = 704
size = $T1_METADATA + 300
print(max(1500000, quote * size * 27 // 20))
")
          fi
          echo "type1 burst: bid ${t1_bid} lovelace (~1.35x the live urgent cost)"
          CURRENT_T1_BID="$t1_bid"
          "$LANE_FEEDER" --socket "$socket" --funds "$WORKING_DIR/funds.json" \
            --network-magic "$NETWORK_MAGIC" --fee "$t1_bid" \
            --metadata-bytes "$T1_METADATA" --cycles "$CYCLES" \
            --delay-ms "$T1_DELAY_MS" --independent-funding \
            --fund-index "$EVGEN_FUND_INDEX" "${initial_args[@]}" \
            >"$WORKING_DIR/evgen-type1.log" 2>&1 &
          pid=$!
          # Door rejections (once the quote already tops the burst bid) only
          # show in the generator's own log — stream them too.
          python3 "$SOURCE_DIR/eviction-aggregator.py" \
            "$WORKING_DIR/evgen-type1.log" "$EVICT_STREAM" &
          agg_pid=$!
          echo "$agg_pid" >"$WORKING_DIR/evgen-aggregator.pid"
        else
          "$LANE_FEEDER" --socket "$socket" --funds "$WORKING_DIR/funds.json" \
            --network-magic "$NETWORK_MAGIC" --fee "$T2_FEE" \
            --metadata-bytes "$T2_METADATA" --cycles "$CYCLES" \
            --delay-ms "$T2_DELAY_MS" --conflict-mode \
            --fund-index "$EVGEN_FUND_INDEX" "${initial_args[@]}" \
            >"$WORKING_DIR/evgen-type2.log" 2>&1 &
          pid=$!
          python3 "$SOURCE_DIR/eviction-aggregator.py" \
            "$WORKING_DIR/evgen-type2.log" "$EVICT_STREAM" &
          agg_pid=$!
          echo "$agg_pid" >"$WORKING_DIR/evgen-aggregator.pid"
        fi
        echo "$pid" >"$WORKING_DIR/evgen.pid"
      fi
      current="$want"
      [ "$current" = "type1" ] || CURRENT_T1_BID=0
      RELAUNCH_SAME=0
      write_run_config "$current"
    elif [ "$current" = "type3" ]; then
      : # flag-file scenario: nothing to babysit
    elif [ -n "$pid" ]; then
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        # The generator died (e.g. its first tx raced an in-flight one). Relaunch
        # from the fund's current UTxO on the next pass.
        echo "(eviction generator $current stopped — relaunching)"
        RELAUNCH_SAME=1
        current="off"
      else
        # Progress watchdog: a generator stuck retrying a doomed first submission
        # stays alive but silent. If its log has not grown past the banner after
        # ~30s, restart it from a freshly queried UTxO.
        gen_log="$WORKING_DIR/evgen-${current}.log"
        gen_lines=$(wc -l <"$gen_log" 2>/dev/null || echo 0)
        if [ "${gen_lines:-0}" -le 2 ]; then
          stall_count=$((stall_count + 1))
          if [ "$stall_count" -ge 15 ]; then
            echo "(eviction generator $current silent for 30s — restarting)"
            kill "$pid" >/dev/null 2>&1 || true
            [ -n "$agg_pid" ] && kill "$agg_pid" >/dev/null 2>&1 || true
            pid=""; agg_pid=""; stall_count=0
            RELAUNCH_SAME=1
            current="off"
          fi
        else
          stall_count=0
        fi
      fi
    fi
    sleep 2
  done
}

write_run_config "off"
# Watchdog: the demo's plumbing must outlive its own crashes. The actor feeder
# already restarts itself; give the tailer, the web server and the actor
# aggregator the same courtesy — a silently dead one freezes half the page.
plumbing_watchdog() {
  while :; do
    sleep 10
    if ! kill -0 "$tailer_pid" >/dev/null 2>&1; then
      echo "(tailer died — restarting, resuming block numbering)"
      python3 "$SOURCE_DIR/live-trace-tailer.py" --from-now --evictions "$EVICT_STREAM" "$LIVE_STREAM" \
        "$WORKING_DIR/node1/node.log" "$WORKING_DIR/node2/node.log" "$WORKING_DIR/node3/node.log" &
      tailer_pid=$!
    fi
    if ! kill -0 "$http_pid" >/dev/null 2>&1; then
      echo "(demo server died — restarting)"
      python3 "$SOURCE_DIR/demo-server.py" "$HTTP_PORT" "$DEMO_DIR" "$ACTOR_CONFIG" "$WORKING_DIR" >/dev/null 2>&1 &
      http_pid=$!
    fi
    if [ -n "${http_public_pid:-}" ] && ! kill -0 "$http_public_pid" >/dev/null 2>&1; then
      echo "(audience server died — restarting)"
      python3 "$SOURCE_DIR/demo-server.py" --read-only "$HTTP_PUBLIC_PORT" "$DEMO_DIR" "$ACTOR_CONFIG" "$WORKING_DIR" >/dev/null 2>&1 &
      http_public_pid=$!
    fi
    if [ -n "$aggregator_pid" ] && ! kill -0 "$aggregator_pid" >/dev/null 2>&1; then
      echo "(actor aggregator died — restarting)"
      python3 "$SOURCE_DIR/actor-aggregator.py" "$WORKING_DIR/actor-feeder.log" "$ACTOR_STREAM" &
      aggregator_pid=$!
    fi
  done
}
plumbing_watchdog &
watchdog_pid=$!

eviction_controller &
eviction_controller_pid=$!

url="http://localhost:${HTTP_PORT}"
echo ""
echo "============================================================"
echo "  Live dashboard:  $url"
echo "  Live stream:     $LIVE_STREAM"
echo "  Press Ctrl-C here to stop the whole demo."
echo "============================================================"
echo ""

if [ "$OPEN_BROWSER" = "1" ]; then
  ( sleep 1; open -a "Google Chrome" "$url" >/dev/null 2>&1 || open "$url" >/dev/null 2>&1 || true ) &
fi

# Hold here until Ctrl-C; if a feeder dies, keep the devnet (prices simply drain
# back to the floor) and tell the user.
actor_last_restart=0
while kill -0 "$devnet_pid" >/dev/null 2>&1; do
  for name in optimistic urgent actor; do
    pid_var="${name}_feeder_pid"
    pid="${!pid_var}"
    if [ -n "$pid" ] && ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "(${name} feeder stopped — see $WORKING_DIR/${name}-feeder.log)"
      eval "${pid_var}=''"
    fi
  done
  # Self-heal the actor feeder: a dependent chain can break (a fork, an evicted
  # link -> AllInputsAreSpent) and kill it. Re-anchor both chains on the funds'
  # current on-chain UTxOs and relaunch. If leftover queued txs still hold the
  # UTxO the restart loses first-come and dies again — the backoff retries
  # until the queue has drained enough for the anchor to be spendable.
  if [ "$FEED_MODE" = "actor" ] && [ -z "$actor_feeder_pid" ]; then
    now=$SECONDS
    if [ $((now - actor_last_restart)) -ge 30 ]; then
      actor_last_restart=$now
      restart_args=()
      if line=$(fund_largest_utxo 1) && [ -n "$line" ]; then
        restart_args+=(--initial-txin "${line%% *}" --initial-value "${line##* }")
      fi
      if line=$(fund_largest_utxo 2) && [ -n "$line" ]; then
        restart_args+=(--initial-txin-2 "${line%% *}" --initial-value-2 "${line##* }")
      fi
      # Rapid death loop = the old incarnation's in-flight txs are wedged in
      # the patient pool (min-fill keeps them pooling, they lock the fund's
      # heads, every new chain dies at the door). Clear that lane once after
      # a few consecutive fast deaths, then re-anchor as usual.
      feeder_deaths=$(( ${feeder_deaths:-0} + 1 ))
      if [ "$feeder_deaths" -ge 3 ]; then
        echo "(feeder died $feeder_deaths times in a row — clearing the patient lane to unwedge its old txs)"
        for n in node1 node2 node3; do
          printf 'optimistic' >"$WORKING_DIR/$n/flush-lane.tmp" 2>/dev/null \
            && mv "$WORKING_DIR/$n/flush-lane.tmp" "$WORKING_DIR/$n/flush-lane" 2>/dev/null || true
        done
        feeder_deaths=0
        sleep 5
      fi
      echo "(actor feeder: re-anchoring on current UTxOs and restarting)"
      start_actor_feeder "${restart_args[@]}"
    fi
  fi
  sleep 5
done
