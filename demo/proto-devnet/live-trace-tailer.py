#!/usr/bin/env python3
"""Follow the Dijkstra node logs and turn their forge traces into NDJSON for the
live dashboard.

Every forged block emits a trio of traces (see NodeKernel.hs), in this order,
all from the node that forged it:

    forge lanes: RB urgent=<n>, EB optimistic=<m>
    forge queue: urgent=<qu>, optimistic=<qo>
    forge prices: urgent=<u>, optimistic=<o>

In a multi-node devnet each block is forged by exactly one node, so we follow
every node log and merge their trios into a single block sequence, appending one
JSON record per forged block to the output file (which the dashboard polls):

    {"i": <i>, "urgent": <u>, "optimistic": <o>, "rb": <n>, "eb": <m>, "qu": <qu>, "qo": <qo>}

where rb/eb are the txs forged into each lane's block and qu/qo are the txs still
waiting in each lane (the queue depth) at forge time. We read one line per log
per pass (round-robin), so emission order tracks real forge order, and each log
keeps its own lanes/queue accumulator so interleaving across nodes never
mis-pairs a trio.

Usage: live-trace-tailer.py <out.ndjson> <node.log> [<node.log> ...]
"""

import json
import os
import re
import sys
import time
from datetime import datetime

SPEND_INPUT_RE = re.compile(r'dtbrSpendInputs = fromList \[TxIn \(TxId \{unTxId = SafeHash \\"([0-9a-f]{64})')
LANE_RE = re.compile(
    r"forge lanes:.*?RB[^0-9]*urgent=(\d+).*?EB[^0-9]*optimistic=(\d+)"
    r"(?:.*?rbBytes=(\d+))?(?:.*?ebBytes=(\d+))?"
)
QUEUE_RE = re.compile(r"forge queue:.*?urgent=(\d+).*?optimistic=(\d+)")
PRICE_RE = re.compile(r"forge prices:.*?urgent=(\d+).*?optimistic=(\d+)")


# The ledger's own verdict inside a BidBelowQuote removal: what the tx offered
# (supplied) vs what the lane charges for it at the crossing (expected).
MISMATCH_RE = re.compile(r"supplied: Coin (\d+), expected: Coin (\d+)")


def main():
    args = sys.argv[1:]
    # Optional: also capture real re-validation evictions (Mempool.RemoveTxs) into a
    # stream the dashboard polls. Used in independent-funding mode; in conflict mode
    # the drops are admission rejections (untraced) handled by eviction-aggregator.py.
    evictions_path = None
    if "--evictions" in args:
        i = args.index("--evictions")
        evictions_path = args[i + 1]
        del args[i : i + 2]
    if len(args) < 2:
        sys.exit("usage: live-trace-tailer.py [--evictions <path>] <out.ndjson> <node.log> [<node.log> ...]")
    out_path = args[0]
    log_paths = args[1:]

    handles = {p: None for p in log_paths}
    state = {p: {"rb": 0, "eb": 0, "qu": 0, "qo": 0} for p in log_paths}
    block_index = 0
    ev_index = 0
    node1 = log_paths[0]  # count one node's mempool to avoid triple-counting
    # NOTE: the stream files are truncated once by the run script at launch;
    # the tailer only appends, so a late (re)start never wipes history.

    # The actor feeder reads the latest quotes from here, next to the stream.
    quotes_path = os.path.join(os.path.dirname(os.path.abspath(out_path)), "latest-quotes.json")

    # EB lifecycle: forged (any node) -> certified (any node). What never
    # certifies is the "awaiting votes" backlog the dashboard shows during the
    # certification-miss scenario.
    leios_status_path = os.path.join(os.path.dirname(os.path.abspath(out_path)), "leios-status.json")
    eb_forged = {}     # ebHash -> {"slot", "numTxs"}
    eb_certified = set()

    def write_leios_status():
        # "Stalled" = uncertified AND newer than the last certified EB. An older
        # uncertified EB was superseded (a later one certified; its txs rode
        # again) — counting those forever would inflate the number all run long.
        last_cert_slot = max(
            (eb_forged[h]["slot"] for h in eb_certified if h in eb_forged), default=-1
        )
        pending = [
            h for h in eb_forged
            if h not in eb_certified and eb_forged[h]["slot"] > last_cert_slot
        ]
        recent = sorted(eb_forged.items(), key=lambda kv: kv[1]["slot"])[-120:]
        status = {
            "forged": len(eb_forged),
            "certified": len(eb_certified),
            "pendingEbs": len(pending),
            "pendingTxs": sum(eb_forged[h].get("numTxs", 0) for h in pending),
            "lastForgedSlot": max((eb_forged[h]["slot"] for h in eb_forged), default=None),
            # Per-EB status, newest last — the dashboard joins this against each
            # block record's ebHash to colour blocks by certification state.
            "ebs": [
                {"hash": h[:8], "slot": v["slot"], "numTxs": v.get("numTxs", 0),
                 "certified": h in eb_certified}
                for h, v in recent
            ],
        }
        tmp = leios_status_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(status, f)
        os.replace(tmp, leios_status_path)

    def flush_pending(p_):
        pending = state[p_].pop("pending", None)
        if pending is not None:
            emit(pending)

    def emit_eviction(record):
        with open(evictions_path, "a") as out:
            out.write(json.dumps(record) + "\n")
            out.flush()

    # Per-tx removal stream (short txid prefix + stage + lane) so the actor
    # aggregator can attribute each removal to the cockpit command ("generation")
    # that sent the tx. Same dir as the block stream the dashboard polls.
    removed_path = os.path.join(os.path.dirname(os.path.abspath(out_path)), "removed-txs.ndjson")

    # Live mempool size (node1's view, the submission point): every AddedTx /
    # RemoveTxs trace carries mempoolSize, so the dashboard can move between
    # forges instead of freezing until the next block. Throttled to 2 Hz.
    mempool_live_path = os.path.join(os.path.dirname(os.path.abspath(out_path)), "mempool-live.json")
    mempool_live_last = [0.0]

    def write_mempool_live(size, at):
        now = time.time()
        if now - mempool_live_last[0] < 0.25:
            return
        mempool_live_last[0] = now
        tmp = mempool_live_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"numTxs": size.get("numTxs"), "bytes": size.get("bytes"), "at": at}, f)
        os.replace(tmp, mempool_live_path)

    def emit_removed(records):
        if not records:
            return
        with open(removed_path, "a") as out:
            for r in records:
                out.write(json.dumps(r) + "\n")
            out.flush()

    # Per-eviction DETAIL stream: one line per priced-out tx with the exact
    # numbers the ledger judged (its bid vs what the lane now charges for it)
    # and how long it waited (admission time joined by txid prefix). The
    # dashboard's pressure journal opens this to tell each tx's full story.
    evicted_detail_path = os.path.join(os.path.dirname(os.path.abspath(out_path)), "evicted-txs.ndjson")
    added_at = {}  # txid prefix -> ISO admission time (node1, bounded)

    def note_added(txid, at):
        if txid and at:
            if len(added_at) > 60000:
                for k in list(added_at)[:30000]:
                    added_at.pop(k, None)
            added_at[txid] = at

    def emit_evicted_detail(records):
        if not records:
            return
        with open(evicted_detail_path, "a") as out:
            for r in records:
                out.write(json.dumps(r) + "\n")
            out.flush()

    def emit(record):
        with open(out_path, "a") as out:
            out.write(json.dumps(record) + "\n")
            out.flush()
        # Publish the latest quotes atomically (write + rename) so a concurrent
        # reader never sees a half-written file.
        tmp = quotes_path + ".tmp"
        with open(tmp, "w") as q:
            json.dump({"urgent": record["urgent"], "optimistic": record["optimistic"]}, q)
        os.replace(tmp, quotes_path)

    while True:
        progressed = False
        for p in log_paths:
            handle = handles[p]
            if handle is None:
                try:
                    handle = handles[p] = open(p, "r")
                except FileNotFoundError:
                    continue
            # Survive log rotation/truncation.
            if os.path.exists(p) and os.stat(p).st_size < handle.tell():
                handle.close()
                handle = handles[p] = open(p, "r")
            line = handle.readline()
            if not line:
                continue
            progressed = True
            if "LeiosBlockForged" in line or "LeiosBlockCertified" in line:
                try:
                    inner = json.loads(json.loads(line)["message"])
                    data = inner.get("data", {})
                    kind = data.get("kind", "")
                    if kind == "LeiosBlockForged":
                        eb_forged.setdefault(
                            data.get("hash", ""),
                            {"slot": data.get("slot", 0), "numTxs": data.get("numTxs", 0)},
                        )
                        pending = state[p].get("pending")
                        if pending is not None:
                            pending["ebHash"] = data.get("hash", "")[:8]
                            emit(pending)
                            state[p]["pending"] = None
                        else:
                            state[p]["ebHash"] = data.get("hash", "")[:8]
                        write_leios_status()
                    elif kind == "LeiosBlockCertified":
                        eb_certified.add(data.get("ebHash", ""))
                        write_leios_status()
                    continue
                except Exception:
                    pass
            if p == node1 and "Mempool.AddedTx" in line:
                try:
                    inner = json.loads(json.loads(line)["message"])
                    data = inner.get("data", {})
                    size = data.get("mempoolSize") or {}
                    if size:
                        write_mempool_live(size, inner.get("at"))
                    note_added((data.get("tx") or {}).get("txid"), inner.get("at"))
                except Exception:
                    pass
                continue
            if evictions_path and p == node1 and "Mempool.RemoveTxs" in line:
                handled = False
                try:
                    inner = json.loads(json.loads(line)["message"])
                    if inner.get("ns", "").endswith("RemoveTxs"):
                        data = inner.get("data", {})
                        txs = data.get("txs", [])
                        mempool_txs = (data.get("mempoolSize") or {}).get("numTxs")
                        if data.get("mempoolSize"):
                            write_mempool_live(data["mempoolSize"], inner.get("at"))
                        # Classify per tx. A BidBelowQuote removal is a REAL price
                        # eviction (the quote overtook the bid while waiting). An
                        # AllInputsAreSpent removal is housekeeping: the tx's coin
                        # was just used on-chain — typically the tx itself landed in
                        # a block and its mempool copy is cleared. Emit them as
                        # separate records so the dashboard never sells housekeeping
                        # as drops.
                        buckets = {}
                        entries = []
                        for tx in txs:
                            blob = json.dumps(tx)
                            priced = "BidBelowQuote" in blob
                            stage = "evicted" if priced else "cleared"
                            short = (tx.get("tx") or {}).get("txid") if isinstance(tx, dict) else None
                            bid_required = MISMATCH_RE.search(blob) if priced else None
                            # the tx's (single) input parent, for cascade detection
                            parent = None
                            pm = SPEND_INPUT_RE.search(blob)
                            if pm:
                                parent = pm.group(1)[:8]
                            if short:
                                entries.append({
                                    "txid": short,
                                    "stage": stage,
                                    "parent": parent,
                                    "lane": "urgent" if "dtbrInclusion = Urgent" in blob
                                            else "optimistic" if "dtbrInclusion = Optimistic" in blob
                                            else "?",
                                    "bid": int(bid_required.group(1)) if bid_required else None,
                                    "required": int(bid_required.group(2)) if bid_required else None,
                                })
                            b = buckets.setdefault(
                                stage,
                                {"n": 0, "urgent": 0, "optimistic": 0, "bid": 0, "spent": 0},
                            )
                            b["n"] += 1
                            b["bid"] += 1 if priced else 0
                            b["spent"] += 1 if "AllInputsAreSpent" in blob else 0
                            if "dtbrInclusion = Urgent" in blob:
                                b["urgent"] += 1
                            elif "dtbrInclusion = Optimistic" in blob:
                                b["optimistic"] += 1
                        for stage, b in buckets.items():
                            emit_eviction({
                                "i": ev_index, "t": inner.get("at"), "stage": stage,
                                "n": b["n"],
                                "urgent": b["urgent"], "optimistic": b["optimistic"],
                                "bidBelowQuote": b["bid"], "allInputsAreSpent": b["spent"],
                                "other": max(0, b["n"] - b["bid"] - b["spent"]),
                                "mempoolTxs": mempool_txs,
                            })
                            ev_index += 1
                        # A chained descendant of an evicted tx is removed in the
                        # same sweep as AllInputsAreSpent — it never reached a
                        # block. Walk the in-batch parent links from each evicted
                        # tx and reclassify those "cleared" as "orphaned" so the
                        # lifecycle never sells a dropped cascade as forged.
                        by_id = {e["txid"]: e for e in entries}
                        dropped = {e["txid"] for e in entries if e["stage"] == "evicted"}
                        changed = True
                        while changed:
                            changed = False
                            for e in entries:
                                if (e["stage"] == "cleared" and e["parent"] in dropped
                                        and e["txid"] not in dropped):
                                    e["stage"] = "orphaned"
                                    dropped.add(e["txid"])
                                    changed = True
                        emit_removed([{k: e[k] for k in ("txid", "stage", "lane")} for e in entries])
                        removed_at = inner.get("at")
                        details = []
                        for e in entries:
                            if e["stage"] not in ("evicted", "orphaned"):
                                continue
                            seen = added_at.pop(e["txid"], None)
                            waited = None
                            if seen and removed_at:
                                try:
                                    t0 = datetime.fromisoformat(seen.replace("Z", "+00:00"))
                                    t1 = datetime.fromisoformat(removed_at.replace("Z", "+00:00"))
                                    waited = round((t1 - t0).total_seconds(), 1)
                                except Exception:
                                    pass
                            details.append({
                                "txid": e["txid"], "stage": e["stage"], "lane": e["lane"],
                                "t": removed_at, "addedAt": seen, "waitedS": waited,
                                "bid": e.get("bid"), "required": e.get("required"),
                            })
                        emit_evicted_detail(details)
                        handled = True
                except Exception:
                    pass
                if handled:
                    continue
            lane = LANE_RE.search(line)
            if lane:
                flush_pending(p)   # a new round starts: whatever was pending is final
                state[p]["rb"], state[p]["eb"] = int(lane.group(1)), int(lane.group(2))
                state[p]["rbBytes"] = int(lane.group(3)) if lane.group(3) else None
                state[p]["ebBytes"] = int(lane.group(4)) if lane.group(4) else None
                continue
            queue = QUEUE_RE.search(line)
            if queue:
                state[p]["qu"], state[p]["qo"] = int(queue.group(1)), int(queue.group(2))
                continue
            price = PRICE_RE.search(line)
            if price:
                record = {
                    "i": block_index,
                    "urgent": int(price.group(1)),
                    "optimistic": int(price.group(2)),
                    "rb": state[p]["rb"],
                    "eb": state[p]["eb"],
                    # bytes each lane occupies — the block's TRUE fullness
                    # (capacity is a byte budget; tx counts mislead when fat
                    # and thin txs mix)
                    "rbBytes": state[p].get("rbBytes"),
                    "ebBytes": state[p].get("ebBytes"),
                    "qu": state[p]["qu"],
                    "qo": state[p]["qo"],
                    # the EB forged in this round, if any — joins against
                    # leios-status.json's per-EB certification list
                    # the EB currently being filled on this log (sticky until a
                    # new one is forged): several rounds can share one EB
                    "ebHash": state[p].get("ebHash"),
                    # which node forged this block (the log that produced the trio)
                    "node": os.path.basename(os.path.dirname(p)),
                }
                block_index += 1
                if record["eb"] > 0 and record["ebHash"] is None:
                    # The LeiosBlockForged trace for this round has not been read
                    # yet (trace ordering varies). Hold the record briefly so the
                    # EB hash can be attached — the dashboard joins certification
                    # state by that hash.
                    state[p]["pending"] = record
                else:
                    emit(record)
        if not progressed:
            for p_ in log_paths:
                flush_pending(p_)
            time.sleep(0.25)


if __name__ == "__main__":
    main()
