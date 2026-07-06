#!/usr/bin/env python3
"""Aggregate the actor feeder's decision log into NDJSON buckets for the dashboard,
and track each cockpit command's transactions through their lifecycle.

The actor feeder logs one line per demand it weighs:

    actor decision: n=<k> type=<t> lane=urgent|optimistic|shed value=<v> urgency=<u> size=<s> quoteUrgent=<qu> quoteOptimistic=<qo> bid=<b> [gen=<label>]

and one line per accepted submission:

    <k>: accepted urgent|optimistic "<64-hex txid>"

Bucket stream (unchanged): one record per fixed-size bucket of decisions, so the
dashboard can show how the urgent / optimistic / walk-away split shifts.

Lifecycle (new): every decision belongs to a "generation" — the cockpit command
label active when it was made (the feeder stamps gen=<label>). We join:
  - decisions        -> sent counts per generation (and lane / walk-away split)
  - accepted lines   -> txid prefix -> generation
  - removed-txs.ndjson (from live-trace-tailer) -> per-tx "cleared" (the tx
    landed on-chain; its mempool copy was housekept away) or "evicted" (priced
    out) — attributed back to the generation via the txid prefix
and periodically snapshot lifecycle.json: per generation, how many txs were
sent, accepted, are still waiting in the mempool, landed in a block, or were
dropped. That is what lets the dashboard show the latency between a cockpit
command and its visible effect.

Usage: actor-aggregator.py <feeder.log> <out.ndjson> [bucket-size]
                           [--removed <removed-txs.ndjson>] [--lifecycle <lifecycle.json>]
"""

import json
import os
import re
import sys
import time

DECISION_RE = re.compile(
    r"actor decision: n=(\d+).*?lane=(\w+).*?quoteUrgent=(\d+).*?quoteOptimistic=(\d+)"
    r"(?:.*?gen=(\S+))?(?:.*?\st=(\d+))?"
)
ACCEPTED_RE = re.compile(r"^(\d+): accepted (urgent|optimistic) \"([0-9a-f]{64})\"")


def tail_lines(path, state):
    """Non-blocking: yield only COMPLETE new lines (never waits). A partial
    line caught mid-write is buffered until its newline arrives, so a decision
    cut before its gen= field is never mis-booked into the wrong generation."""
    handle = state.get(path)
    if handle is None:
        try:
            handle = state[path] = open(path, "r")
        except FileNotFoundError:
            return
    if os.path.exists(path) and os.stat(path).st_size < handle.tell():
        handle.close()
        handle = state[path] = open(path, "r")
        state[path + "#buf"] = ""
    buf = state.get(path + "#buf", "")
    while True:
        chunk = handle.readline()
        if not chunk:
            state[path + "#buf"] = buf
            return
        buf += chunk
        if buf.endswith("\n"):
            line, buf = buf, ""
            yield line


def main():
    args = sys.argv[1:]
    removed_path = None
    lifecycle_path = None
    if "--removed" in args:
        i = args.index("--removed"); removed_path = args[i + 1]; del args[i:i + 2]
    if "--lifecycle" in args:
        i = args.index("--lifecycle"); lifecycle_path = args[i + 1]; del args[i:i + 2]
    if len(args) < 2:
        sys.exit("usage: actor-aggregator.py <feeder.log> <out.ndjson> [bucket-size] [--removed f] [--lifecycle f]")
    log_path, out_path = args[0], args[1]
    bucket_size = int(args[2]) if len(args) > 2 else 120

    counts = {"urgent": 0, "optimistic": 0, "shed": 0}
    last_qu, last_qo = 704, 44
    seen = 0
    bucket = 0

    # generation label -> lifecycle counters (insertion order = command order)
    generations = {}
    pending_gen = {}   # decision n -> generation (until its accepted line shows up)
    tx_gen = {}        # txid 8-hex prefix -> generation
    last_snapshot = 0.0

    def gen_bucket(label):
        if label not in generations:
            generations[label] = {
                "label": label, "sent": 0, "urgent": 0, "optimistic": 0, "shed": 0,
                "accepted": 0, "forged": 0, "evicted": 0, "orphaned": 0,
                "acceptedUrgent": 0, "acceptedOptimistic": 0,
                "doneUrgent": 0, "doneOptimistic": 0,
                "firstSeen": time.time(), "lastSeen": time.time(),
            }
        return generations[label]

    def snapshot():
        if not lifecycle_path:
            return
        gens = list(generations.values())[-15:]
        for g in gens:
            g["waiting"] = max(0, g["accepted"] - g["forged"] - g["evicted"] - g.get("orphaned", 0))
            g["waitingUrgent"] = max(0, g.get("acceptedUrgent", 0) - g.get("doneUrgent", 0))
            g["waitingOptimistic"] = max(0, g.get("acceptedOptimistic", 0) - g.get("doneOptimistic", 0))
        tmp = lifecycle_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"generations": gens, "updated": time.time()}, f)
        os.replace(tmp, lifecycle_path)

    handles = {}
    while True:
        progressed = False
        for line in tail_lines(log_path, handles):
            progressed = True
            match = DECISION_RE.search(line)
            if match:
                n, lane = int(match.group(1)), match.group(2)
                last_qu, last_qo = int(match.group(3)), int(match.group(4))
                label = match.group(5) or "warmup"
                # Prefer the line's own wall-clock stamp: it survives a log
                # replay, so generation time ranges stay real after a restart.
                ts = float(match.group(6)) if match.group(6) else time.time()
                g = gen_bucket(label)
                if g["sent"] == 0:
                    g["firstSeen"] = ts
                g["sent"] += 1
                g["lastSeen"] = ts
                if lane in ("urgent", "optimistic", "shed"):
                    g[lane] += 1
                if lane in ("urgent", "optimistic"):
                    pending_gen[n] = label
                    if len(pending_gen) > 10000:   # accepted line never came (chain drop)
                        for k in sorted(pending_gen)[:1000]:
                            del pending_gen[k]
                if lane in counts:
                    counts[lane] += 1
                seen += 1
                if seen >= bucket_size:
                    record = {
                        "i": bucket,
                        "urgent": counts["urgent"],
                        "optimistic": counts["optimistic"],
                        "shed": counts["shed"],
                        "qu": last_qu,
                        "qo": last_qo,
                    }
                    with open(out_path, "a") as out:
                        out.write(json.dumps(record) + "\n")
                        out.flush()
                    bucket += 1
                    seen = 0
                    counts = {"urgent": 0, "optimistic": 0, "shed": 0}
                continue
            match = ACCEPTED_RE.match(line)
            if match:
                n, lane, txid = int(match.group(1)), match.group(2), match.group(3)
                label = pending_gen.pop(n, "warmup")
                g = gen_bucket(label)
                g["accepted"] += 1
                if lane == "urgent":
                    g["acceptedUrgent"] += 1
                else:
                    g["acceptedOptimistic"] += 1
                tx_gen[txid[:8]] = label
                if len(tx_gen) > 300000:
                    for k in list(tx_gen)[:50000]:
                        del tx_gen[k]
        if removed_path:
            for line in tail_lines(removed_path, handles):
                progressed = True
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                label = tx_gen.pop(rec.get("txid", ""), None)
                if label is None:
                    continue   # not an actor tx (scenario generators, other feeders)
                g = gen_bucket(label)
                stage = rec.get("stage")
                if stage == "evicted":
                    g["evicted"] += 1
                elif stage == "orphaned":
                    # chained descendant of an evicted tx: dropped, never forged
                    g["orphaned"] += 1
                else:
                    g["forged"] += 1
                if rec.get("lane") == "urgent":
                    g["doneUrgent"] += 1
                else:
                    g["doneOptimistic"] += 1
        now = time.time()
        if now - last_snapshot > 1.0:
            snapshot()
            last_snapshot = now
        if not progressed:
            time.sleep(0.3)


if __name__ == "__main__":
    main()
