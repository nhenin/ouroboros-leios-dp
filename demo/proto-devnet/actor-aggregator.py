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
  - accepted lines   -> txid prefix -> generation AND lane (the feeder's own
    record — authoritative even when a removal line can't tell the lane)
  - removed-txs.ndjson (from live-trace-tailer) -> per-tx stage: "cleared" (the
    tx landed on-chain; its mempool copy was housekept away), "riding" (stripped
    for an announced endorser block whose certificate landed — settled on-chain
    through the optimistic pipeline, counted forged), "evicted" (priced out
    while waiting), "orphaned" (chained behind an evicted tx), "flushed"
    (dropped by hand via the presenter's flush command) or "stranded" (stripped
    for an endorser block that was superseded uncertified — a real drop) —
    attributed back to the generation via the txid prefix. Every drop kind is
    counted separately and split per lane, so the journal can say WHY a tx
    never made it, not just that it didn't. "returned" (and "flushed") are
    PROVISIONAL verdicts: the tailer can still pair a lagging strip and settle
    the tx as "riding". When that happens the drop is taken back — the counters
    move to forged/rode and a correction line (the settlement record plus
    "corrects": <old stage>) is appended to dropped-txs.ndjson so downstream
    joins can undo the fossilized verdict too.
and periodically snapshot lifecycle.json: per generation, how many txs were
sent, accepted, are still waiting in the mempool, landed in a block, or were
dropped (and how). That is what lets the dashboard show the latency between a
cockpit command and its visible effect. An accepted line whose decision entry
the overflow sweep already pruned is never credited to a generation (that
would book an accepted without its sent); it lands in the snapshot's
top-level "unattributedAccepted" counter instead.

Usage: actor-aggregator.py <feeder.log> <out.ndjson> [bucket-size]
                           [--removed <removed-txs.ndjson>] [--lifecycle <lifecycle.json>]

--removed and --lifecycle default to <out dir>/removed-txs.ndjson and
<out dir>/lifecycle.json — the whole plumbing writes into one demo dir, so a
bare respawn (watchdog) keeps the full lifecycle alive.
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
    marker = path + ".truncated"
    if handle is None:
        try:
            handle = state[path] = open(path, "r")
            try:
                state[path + "#truncate_mtime"] = os.stat(marker).st_mtime_ns
            except FileNotFoundError:
                state[path + "#truncate_mtime"] = 0
        except FileNotFoundError:
            return
    seen = state.get(path + "#truncate_mtime", 0)
    try:
        marker_mtime = os.stat(marker).st_mtime_ns
    except FileNotFoundError:
        marker_mtime = seen
    if marker_mtime > seen or (
        os.path.exists(path) and os.stat(path).st_size < handle.tell()
    ):
        handle.close()
        handle = state[path] = open(path, "r")
        state[path + "#buf"] = ""
        state[path + "#truncate_mtime"] = marker_mtime
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
    demo_dir = os.path.dirname(os.path.abspath(out_path))
    if removed_path is None:
        removed_path = os.path.join(demo_dir, "removed-txs.ndjson")
    if lifecycle_path is None:
        lifecycle_path = os.path.join(demo_dir, "lifecycle.json")
    # Every dropped tx, attributed: txid -> generation + why. The dashboard
    # joins this against the tailer's evicted-txs detail stream to tell each
    # generation's drop story EXACTLY (time windows alone kidnap neighbours'
    # evictions — a sweep near a boundary belongs to whoever sent the txs).
    dropped_path = os.path.join(demo_dir, "dropped-txs.ndjson")
    # This process replays its inputs from the start on every (re)launch, so
    # its append-streams must start empty — a respawn would otherwise double
    # every bucket and every drop.
    open(out_path, "w").close()
    open(dropped_path, "w").close()

    counts = {"urgent": 0, "optimistic": 0, "shed": 0}
    last_qu, last_qo = 88, 44
    seen = 0
    bucket = 0

    # generation label -> lifecycle counters (insertion order = command order)
    generations = {}
    pending_gen = {}   # decision n -> generation (until its accepted line shows up)
    tx_gen = {}        # txid 8-hex prefix -> (generation, lane)
    # Provisional drop verdicts whose tx may yet settle: "returned" and
    # "flushed" only say the tx left the MEMPOOL — an in-flight endorser
    # block can still carry it to a certificate, and the tailer then pairs
    # the lagging strip as "riding" AFTER the drop was booked. Popping
    # tx_gen on the first record would fossilize the wrong story, so these
    # verdicts leave a tombstone behind for the late settlement to recall.
    tombstones = {}    # txid 8-hex prefix -> (generation, lane, stage)
    # Accepted lines whose decision entry the overflow sweep already pruned.
    # Kept OUT of every generation: crediting one would book an accepted
    # without its sent — the one path to journal totals exceeding "sent".
    unattributed = 0
    last_snapshot = 0.0

    def gen_bucket(label):
        if label not in generations:
            generations[label] = {
                "label": label, "sent": 0, "urgent": 0, "optimistic": 0, "shed": 0,
                "accepted": 0, "forged": 0, "evicted": 0, "orphaned": 0, "flushed": 0,
                "rode": 0, "stranded": 0, "returned": 0,
                "acceptedUrgent": 0, "acceptedOptimistic": 0,
                "evictedUrgent": 0, "evictedOptimistic": 0,
                "orphanedUrgent": 0, "orphanedOptimistic": 0,
                "flushedUrgent": 0, "flushedOptimistic": 0,
                "rodeUrgent": 0, "rodeOptimistic": 0,
                "strandedUrgent": 0, "strandedOptimistic": 0,
                "returnedUrgent": 0, "returnedOptimistic": 0,
                "doneUrgent": 0, "doneOptimistic": 0,
                "firstSeen": time.time(), "lastSeen": time.time(),
            }
        return generations[label]

    def snapshot():
        if not lifecycle_path:
            return
        gens = list(generations.values())[-15:]
        for g in gens:
            g["waiting"] = max(0, g["accepted"] - g["forged"] - g["evicted"]
                               - g.get("orphaned", 0) - g.get("flushed", 0)
                               - g.get("stranded", 0) - g.get("returned", 0))
            g["waitingUrgent"] = max(0, g.get("acceptedUrgent", 0) - g.get("doneUrgent", 0))
            g["waitingOptimistic"] = max(0, g.get("acceptedOptimistic", 0) - g.get("doneOptimistic", 0))
        tmp = lifecycle_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"generations": gens, "unattributedAccepted": unattributed,
                       "updated": time.time()}, f)
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
                    # Overflow sweep. Most swept entries are dead (the chain
                    # dropped the submission, no accepted line will come), but
                    # the feeder logs the decision BEFORE offering to its
                    # bounded lane queue, so under backpressure a LIVE entry's
                    # accepted line can trail by minutes and outlive the sweep
                    # — such late accepts land in `unattributed` below.
                    if len(pending_gen) > 10000:
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
                label = pending_gen.pop(n, None)
                if label is None:
                    # Its decision entry was pruned (see the overflow sweep):
                    # this accept belongs to a generation we can no longer
                    # name. Booking it into "warmup" — the old fallback — made
                    # warmup show accepted > sent. Count it aside instead; its
                    # txid is dropped too, so the removal join skips it like
                    # any non-actor tx rather than minting a ghost generation.
                    unattributed += 1
                    continue
                g = gen_bucket(label)
                g["accepted"] += 1
                if lane == "urgent":
                    g["acceptedUrgent"] += 1
                else:
                    g["acceptedOptimistic"] += 1
                tx_gen[txid[:8]] = (label, lane)
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
                txid = rec.get("txid", "")
                stage = rec.get("stage")
                entry = tx_gen.pop(txid, None)
                if entry is None:
                    # A second record for a tx already judged. Only one
                    # follow-up matters: a settlement landing on a tombstoned
                    # provisional verdict — the endorser block made it after
                    # all, its re-strip just lagged the grace window.
                    tomb = tombstones.get(txid)
                    if tomb is None or stage in (
                            "evicted", "orphaned", "flushed", "stranded", "returned"):
                        continue   # not an actor tx, or the drop verdict stands
                    del tombstones[txid]
                    label, lane, verdict = tomb
                    suffix = "Urgent" if lane == "urgent" else "Optimistic"
                    g = gen_bucket(label)
                    # Take the drop back and credit the settlement. "done" was
                    # already counted with the provisional verdict — the tx is
                    # still done, just for the happy reason now — so the
                    # waiting maths need no touch-up.
                    g[verdict] -= 1
                    g[verdict + suffix] -= 1
                    g["forged"] += 1
                    if stage == "riding":
                        g["rode"] += 1
                        g["rode" + suffix] += 1
                    # The drop line is already in the journal stream: append a
                    # correction (same shape + "corrects") so downstream joins
                    # can undo the fossilized verdict instead of re-counting.
                    with open(dropped_path, "a") as out:
                        out.write(json.dumps({"txid": txid, "gen": label,
                                              "stage": stage, "lane": lane,
                                              "corrects": verdict}) + "\n")
                    continue
                label, lane = entry
                # The feeder's own record of the lane is authoritative — a
                # flushed-tx trace line only carries "?" for the lane.
                if lane not in ("urgent", "optimistic"):
                    lane = "urgent" if rec.get("lane") == "urgent" else "optimistic"
                suffix = "Urgent" if lane == "urgent" else "Optimistic"
                g = gen_bucket(label)
                if stage == "evicted":
                    g["evicted"] += 1
                    g["evicted" + suffix] += 1
                elif stage == "orphaned":
                    # chained descendant of an evicted tx: dropped, never forged
                    g["orphaned"] += 1
                    g["orphaned" + suffix] += 1
                elif stage == "flushed":
                    # dropped by hand (the presenter's flush command) — a drop,
                    # never a forge: selling it as "forged" broke the story
                    g["flushed"] += 1
                    g["flushed" + suffix] += 1
                elif stage == "riding":
                    # rode an endorser block to its certificate — settled
                    # on-chain, a forge through the optimistic pipeline
                    g["forged"] += 1
                    g["rode"] += 1
                    g["rode" + suffix] += 1
                elif stage == "stranded":
                    # stripped for an endorser block superseded uncertified —
                    # gone from every mempool, never settled
                    g["stranded"] += 1
                    g["stranded" + suffix] += 1
                elif stage == "returned":
                    # its endorser block missed certification and the mempool
                    # refused it on return: the quote had climbed past its max
                    # fee while the block was failing — a price verdict, the
                    # same one eviction would have delivered
                    g["returned"] += 1
                    g["returned" + suffix] += 1
                else:
                    g["forged"] += 1
                g["done" + suffix] += 1
                if stage in ("returned", "flushed"):
                    tombstones[txid] = (label, lane, stage)
                    if len(tombstones) > 50000:   # bounded like tx_gen
                        for k in list(tombstones)[:10000]:
                            del tombstones[k]
                if stage in ("evicted", "orphaned", "flushed", "stranded", "returned"):
                    with open(dropped_path, "a") as out:
                        out.write(json.dumps({"txid": txid, "gen": label,
                                              "stage": stage, "lane": lane}) + "\n")
        now = time.time()
        if now - last_snapshot > 1.0:
            snapshot()
            last_snapshot = now
        if not progressed:
            time.sleep(0.3)


if __name__ == "__main__":
    main()
