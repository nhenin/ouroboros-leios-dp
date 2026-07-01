#!/usr/bin/env bash
set -euo pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${WORKING_DIR:=${TMPDIR:-/tmp}/dijkstra-lane-pressure}"
: "${LANE_FEEDER:=dijkstra-lane-feeder}"
: "${NETWORK_MAGIC:=164}"
: "${FEE:=10000000}"
: "${METADATA_BYTES:=16000}"
: "${OPTIMISTIC_FIRST:=40}"
: "${URGENT_AFTER:=16}"
: "${CYCLES:=3}"
: "${PHASE1_OPTIMISTIC_FIRST:=4}"
: "${PHASE1_URGENT_AFTER:=2}"
: "${PHASE1_METADATA_BYTES:=0}"
: "${PHASE1_CYCLES:=1}"
: "${PHASE2_OPTIMISTIC_FIRST:=$OPTIMISTIC_FIRST}"
: "${PHASE2_URGENT_AFTER:=$URGENT_AFTER}"
: "${PHASE2_METADATA_BYTES:=$METADATA_BYTES}"
: "${PHASE2_CYCLES:=$CYCLES}"
: "${PHASE2_ITERATIONS:=4}"
: "${PHASE2_SLEEP:=1}"
: "${DEMO_HOLD_SECONDS:=0}"
: "${STARTUP_TIMEOUT:=90}"
: "${FORGE_TIMEOUT:=180}"
: "${PRICE_TIMEOUT:=180}"
: "${KEEP_DEVNET:=0}"
: "${PC_DISABLE_TUI:=1}"
: "${PRICE_LINE_REGEX:=([Pp]ublished[ -]?[Pp]rices|publishedPrices|[Pp]ricing|[Pp]rices|[Qq]uotes?)}"

RUN_LOG="${WORKING_DIR}.pressure-run.log"
summary_regex='forge lanes: RB urgent=[1-9][0-9]*, EB optimistic=[1-9][0-9]*'
blocker_regex='InvalidBlock|HeaderProtVerTooHigh|Dijkstra era is not active|StoreButDontChange'
matching_summary=""

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_command cardano-cli
require_command jq

if ! command -v "$LANE_FEEDER" >/dev/null 2>&1 && [ ! -x "$LANE_FEEDER" ]; then
  echo "Missing lane feeder executable: $LANE_FEEDER" >&2
  echo "Set LANE_FEEDER=/path/to/dijkstra-lane-feeder." >&2
  exit 1
fi

grep_node_logs() {
  grep -Eh "$1" "$WORKING_DIR"/node*/node.log 2>/dev/null || true
}

count_matching_summaries() {
  grep_node_logs "$summary_regex" | wc -l | tr -d ' '
}

latest_matching_summary() {
  grep_node_logs "$summary_regex" | tail -1
}

assert_no_blockers() {
  local blocker_file

  blocker_file="$WORKING_DIR/dijkstra-lane-pressure-blockers.log"
  grep_node_logs "$blocker_regex" >"$blocker_file"
  if [ -s "$blocker_file" ]; then
    echo "Found blocker signals in node logs:" >&2
    cat "$blocker_file" >&2
    exit 1
  fi
}

assert_no_filler() {
  local filler_file

  filler_file="$WORKING_DIR/dijkstra-lane-pressure-filler.log"
  grep_node_logs 'forge lanes:.*optimistic-filler=' >"$filler_file"
  if [ -s "$filler_file" ]; then
    echo "Found obsolete optimistic filler in forge trace:" >&2
    cat "$filler_file" >&2
    exit 1
  fi
}

assert_no_optimistic_in_rb() {
  local rb_optimistic_file

  rb_optimistic_file="$WORKING_DIR/dijkstra-lane-pressure-rb-optimistic.log"
  grep_node_logs 'forge lanes:.*RB[^,]*optimistic=[1-9][0-9]*' >"$rb_optimistic_file"
  if [ -s "$rb_optimistic_file" ]; then
    echo "Found optimistic transactions forged in RB under no-filler policy:" >&2
    cat "$rb_optimistic_file" >&2
    exit 1
  fi
}

snapshot_mempool() {
  local label

  label="$1"
  cardano-cli query tx-mempool info --testnet-magic "$NETWORK_MAGIC" \
    >"$WORKING_DIR/mempool-${label}.json" \
    2>"$WORKING_DIR/mempool-${label}.stderr" || true
}

run_feeder() {
  local phase iteration metadata_bytes optimistic_first urgent_after cycles feeder_log

  phase="$1"
  iteration="$2"
  metadata_bytes="$3"
  optimistic_first="$4"
  urgent_after="$5"
  cycles="$6"
  feeder_log="$WORKING_DIR/${phase}-feeder-${iteration}.log"

  echo "Submitting ${phase} feeder ${iteration}: optimistic-first=${optimistic_first}, urgent-after=${urgent_after}, metadata-bytes=${metadata_bytes}, cycles=${cycles}"
  "$LANE_FEEDER" \
    --socket "$socket" \
    --funds "$WORKING_DIR/funds.json" \
    --network-magic "$NETWORK_MAGIC" \
    --fee "$FEE" \
    --metadata-bytes "$metadata_bytes" \
    --optimistic-first "$optimistic_first" \
    --urgent-after "$urgent_after" \
    --cycles "$cycles" \
    --independent-lanes \
    >"$feeder_log" \
    2>&1
}

wait_for_new_lane_summary() {
  local label start_count deadline current_count

  label="$1"
  start_count="$2"
  deadline=$((SECONDS + FORGE_TIMEOUT))

  while [ "$SECONDS" -lt "$deadline" ]; do
    assert_no_blockers
    assert_no_filler
    assert_no_optimistic_in_rb

    current_count="$(count_matching_summaries)"
    if [ "$current_count" -gt "$start_count" ]; then
      matching_summary="$(latest_matching_summary)"
      return 0
    fi

    sleep 1
  done

  echo "Timed out waiting for ${label} forge lane split matching:" >&2
  echo "  $summary_regex" >&2
  echo "Last lane summaries:" >&2
  grep_node_logs 'forge lanes:' | tail -20 >&2
  exit 1
}

read_published_prices() {
  local line urgent optimistic

  line="$(
    grep_node_logs "$PRICE_LINE_REGEX" |
      grep -Ei 'urgent' |
      grep -Ei 'optimistic' |
      tail -1 || true
  )"
  if [ -z "$line" ]; then
    return 1
  fi

  urgent="$(printf '%s\n' "$line" | sed -E 's/.*[Uu]rgent[^0-9]*([0-9]+).*/\1/')"
  optimistic="$(printf '%s\n' "$line" | sed -E 's/.*[Oo]ptimistic[^0-9]*([0-9]+).*/\1/')"
  if ! [[ "$urgent" =~ ^[0-9]+$ && "$optimistic" =~ ^[0-9]+$ ]]; then
    return 1
  fi

  printf '%s %s %s\n' "$urgent" "$optimistic" "$line"
}

print_recent_price_traces() {
  grep_node_logs "$PRICE_LINE_REGEX" | tail -20 >&2
}

wait_for_price_increase() {
  local baseline_urgent baseline_optimistic deadline price_snapshot urgent rest optimistic line

  baseline_urgent="$1"
  baseline_optimistic="$2"
  deadline=$((SECONDS + PRICE_TIMEOUT))

  while [ "$SECONDS" -lt "$deadline" ]; do
    assert_no_blockers
    assert_no_filler
    assert_no_optimistic_in_rb

    if price_snapshot="$(read_published_prices)"; then
      urgent="${price_snapshot%% *}"
      rest="${price_snapshot#* }"
      optimistic="${rest%% *}"
      line="${rest#* }"
      if [ "$urgent" -gt "$baseline_urgent" ] && [ "$optimistic" -gt "$baseline_optimistic" ]; then
        echo "Observed price increase: urgent ${baseline_urgent}->${urgent}, optimistic ${baseline_optimistic}->${optimistic}"
        echo "Price trace: $line"
        return 0
      fi
    fi

    sleep 1
  done

  echo "Timed out waiting for urgent and optimistic prices to rise from urgent=${baseline_urgent}, optimistic=${baseline_optimistic}" >&2
  echo "Recent pricing traces:" >&2
  print_recent_price_traces
  exit 1
}

if [ -d "$WORKING_DIR" ]; then
  chmod -R u+w "$WORKING_DIR" 2>/dev/null || true
  rm -rf "$WORKING_DIR"
fi
rm -f "$RUN_LOG"

devnet_pid=""
compose_file="${WORKING_DIR}/process-compose.no-tx-centrifuge.yaml"

find_devnet_process_compose() {
  ps -axo pid=,command= |
    awk -v compose_file="$compose_file" '
      index($0, "process-compose --no-server") && index($0, compose_file) { print $1 }
    '
}

stop_devnet() {
  local pid
  for pid in $(find_devnet_process_compose); do
    kill -INT "$pid" >/dev/null 2>&1 || true
  done
  if [ -n "$devnet_pid" ] && kill -0 "$devnet_pid" >/dev/null 2>&1; then
    kill -INT "$devnet_pid" >/dev/null 2>&1 || true
  fi
  for _ in 1 2 3 4 5; do
    if [ -n "$devnet_pid" ] && kill -0 "$devnet_pid" >/dev/null 2>&1; then
      sleep 1
    elif [ -n "$(find_devnet_process_compose)" ]; then
      sleep 1
    else
      break
    fi
  done
  for pid in $(find_devnet_process_compose); do
    kill -TERM "$pid" >/dev/null 2>&1 || true
  done
  if [ -n "$devnet_pid" ] && kill -0 "$devnet_pid" >/dev/null 2>&1; then
    kill -TERM "$devnet_pid" >/dev/null 2>&1 || true
  fi
  wait "$devnet_pid" >/dev/null 2>&1 || true
}

if [ "$KEEP_DEVNET" != "1" ]; then
  trap stop_devnet EXIT
fi

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

echo "Capturing baseline published prices (floor, before any congestion)"
# Retry: a node may have reached the Dijkstra era but not yet forged a block
# emitting a 'forge prices:' trace, so reading once races the first forge.
baseline_deadline=$((SECONDS + 120))
until price_snapshot="$(read_published_prices)"; do
  if [ "$SECONDS" -ge "$baseline_deadline" ]; then
    echo "Could not read published urgent/optimistic prices (timed out)." >&2
    echo "Set PRICE_LINE_REGEX if the node trace label changed." >&2
    echo "Recent pricing traces:" >&2
    print_recent_price_traces
    exit 1
  fi
  sleep 2
done
baseline_urgent="${price_snapshot%% *}"
price_rest="${price_snapshot#* }"
baseline_optimistic="${price_rest%% *}"
baseline_price_line="${price_rest#* }"
echo "Baseline prices: urgent=${baseline_urgent}, optimistic=${baseline_optimistic}"
echo "Baseline price trace: $baseline_price_line"

# A single sustained feeder run from the genesis funds. The proto-devnet has only
# two genesis UTxOs, and the feeder spends them, so a separate light "sanity"
# phase cannot be followed by a heavy one (the heavy one hits AllInputsAreSpent).
# One congestion run from the fresh genesis proves BOTH the no-filler lane split
# and the price response under load.
echo "Sustained congestion: single feeder run from genesis funds"
congestion_start_count="$(count_matching_summaries)"
run_feeder "congestion" "1" "$PHASE2_METADATA_BYTES" "$PHASE2_OPTIMISTIC_FIRST" "$PHASE2_URGENT_AFTER" "$PHASE2_CYCLES"
snapshot_mempool "congestion-after-submit"
wait_for_new_lane_summary "congestion" "$congestion_start_count"
phase2_summary="$matching_summary"
assert_no_blockers
assert_no_filler
assert_no_optimistic_in_rb
wait_for_price_increase "$baseline_urgent" "$baseline_optimistic"
snapshot_mempool "congestion-final"
assert_no_blockers
assert_no_filler
assert_no_optimistic_in_rb

echo "Congestion proof observed:"
echo "$phase2_summary"
echo "Feeder logs: $WORKING_DIR/phase*-feeder-*.log"
echo "Mempool snapshots:"
echo "$WORKING_DIR/mempool-phase1-after-submit.json"
echo "$WORKING_DIR/mempool-phase2-final.json"
echo "Run log: $RUN_LOG"

if [ "$DEMO_HOLD_SECONDS" -gt 0 ]; then
  echo "Holding devnet ${DEMO_HOLD_SECONDS}s to capture the full price trajectory for the demo..."
  sleep "$DEMO_HOLD_SECONDS"
fi

if [ "$KEEP_DEVNET" = "1" ]; then
  echo "Proto-devnet left running with PID $devnet_pid"
fi
