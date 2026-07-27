#!/usr/bin/env python3
"""Follow the Dijkstra node logs and turn their forge traces into NDJSON for the
live dashboard.

Every forged block emits a trio of traces (see NodeKernel.hs), in this order,
all from the node that forged it:

    forge lanes: RB urgent=<n>, EB optimistic=<m>, ..., EB urgent=<r>
    forge queue: urgent=<qu>, optimistic=<qo>
    forge prices: urgent=<u>, optimistic=<o>

In a multi-node devnet we follow every node log, merge each forger's trio with
its TraceForgedBlock identity, and use node1's ChainDB selection events to keep
only the canonical candidate after Praos tie-breaking. The output file polled by
the dashboard therefore contains one JSON record per selected chain block:

    {"i": <i>, "urgent": <u>, "optimistic": <o>, "rb": <n>, "eb": <m>, "qu": <qu>, "qo": <qo>}

where rb/eb are the txs forged into each lane's block and qu/qo are the txs still
waiting in each lane (the queue depth) at forge time. Each log keeps its own
lanes/queue accumulator so interleaving across nodes never mis-pairs a trio.

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
    r"(?:.*?rbBytes=(\d+))?(?:.*?ebBytes=(\d+))?(?:.*?ebHeld=(true|false))?"
    r"(?:.*?EB urgent=(\d+))?"
)
QUEUE_RE = re.compile(
    r"forge queue:.*?urgent=(\d+).*?optimistic=(\d+)"
    r"(?:.*?quBytes=(\d+).*?quCap=(\d+).*?quSecs=([\d.eE-]+)"
    r".*?qoBytes=(\d+).*?qoCap=(\d+).*?qoSecs=([\d.eE-]+))?"
)

# Each lane's diffusion-time budget: mirrors the devnet config's
# MempoolTimeoutCapacity (15 s) split by laneTimeoutCapacity (urgent 1/9 — a
# short window, urgent traffic must not queue for hours — patient 8/9).
MEMPOOL_TIME_BUDGET_S = 60.0
URGENT_TIME_SHARE = 1.0 / 4.0
PRICE_RE = re.compile(r"forge prices:.*?urgent=(\d+).*?optimistic=(\d+)")


# The ledger's own verdict inside a BidBelowQuote removal: what the tx offered
# (supplied) vs what the lane charges for it at the crossing (expected).
MISMATCH_RE = re.compile(r"supplied: Coin (\d+), expected: Coin (\d+)")

# Python 3.9's fromisoformat only takes fractional seconds of exactly 3 or 6
# digits; the node emits variable-length fractions. Normalise to 6.
ISO_FRAC_RE = re.compile(r"\.(\d+)")


def parse_iso(ts):
    ts = ts.replace("Z", "+00:00")
    ts = ISO_FRAC_RE.sub(lambda m: "." + (m.group(1) + "000000")[:6], ts, count=1)
    return datetime.fromisoformat(ts)


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
    # --from-now: start at the logs' current end instead of replaying them —
    # lets a tailer restart mid-run without re-emitting the whole history.
    from_now = "--from-now" in args
    if from_now:
        args.remove("--from-now")
    if len(args) < 2:
        sys.exit("usage: live-trace-tailer.py [--evictions <path>] <out.ndjson> <node.log> [<node.log> ...]")
    out_path = args[0]
    log_paths = args[1:]

    handles = {p: None for p in log_paths}
    truncate_markers = {p: p + ".truncated" for p in log_paths}
    truncate_seen = {}
    for p, marker in truncate_markers.items():
        try:
            truncate_seen[p] = os.stat(marker).st_mtime_ns
        except FileNotFoundError:
            truncate_seen[p] = 0
    state = {p: {"rb": 0, "eb": 0, "qu": 0, "qo": 0} for p in log_paths}
    block_index = 0
    last_emitted_block_no = -1
    if from_now:
        # Resuming mid-run: continue the block numbering where the stream
        # left off (NEXT index, the emit site post-increments), and never let
        # one torn line abort the scan and reset the numbering.
        try:
            with open(out_path) as prior:
                for line in prior:
                    if not line.strip():
                        continue
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue  # torn/partial line - skip, keep scanning
                    block_index = max(block_index, rec.get("i", -1) + 1)
                    last_emitted_block_no = max(
                        last_emitted_block_no, rec.get("blockNo", -1)
                    )
        except FileNotFoundError:
            pass
    ev_index = 0
    node1 = log_paths[0]  # count one node's mempool to avoid triple-counting
    # A forge trace is only a candidate. Node1's ChainDB tells us which hash
    # actually became the tip after Praos tie-breaking. Hold that choice briefly
    # because AddedToCurrentChain can be followed by SwitchedToAFork for the same
    # block number less than a second later.
    candidates_by_hash = {}
    canonical_by_block_no = {}
    CANONICAL_SETTLE_S = 2.5
    # The run script bounds source logs in place. A sidecar marker makes a
    # truncation observable even if the source regrows beyond our prior offset
    # before this loop next checks its size.

    # The actor feeder reads the latest quotes from here, next to the stream.
    quotes_path = os.path.join(os.path.dirname(os.path.abspath(out_path)), "latest-quotes.json")

    # EB lifecycle: forged (any node) -> certified (any node). What never
    # certifies is the "awaiting votes" backlog the dashboard shows during the
    # certification-miss scenario.
    leios_status_path = os.path.join(os.path.dirname(os.path.abspath(out_path)), "leios-status.json")
    eb_forged = {}     # ebHash -> {"slot", "numTxs"}
    eb_certified = set()
    # Certificate landings keyed by the certifying block's slot (atSlot from the
    # trace). The LeiosBlockCertified trace fires a beat AFTER that block's own
    # forge-prices, so the block's record is held pending and stamped from here —
    # certification shows on the block that actually certified, not the next one.
    cert_by_slot = {}

    def certificate_info(eb_hash):
        forged = eb_forged.get(eb_hash, {})
        return {
            "hash": eb_hash[:8],
            "numTxs": forged.get("numTxs", 0),
            "optimistic": forged.get("optimistic", 0),
            "urgent": forged.get("urgent", 0),
            "bytes": forged.get("bytes", 0),
        }

    def apply_certificate(record, info):
        """Project the applied block, not the pre-forge mempool selection."""
        record["certIn"] = info
        # The current prototype selects the certifying RB against the pre-cargo
        # state, so Forge deliberately leaves its own payload empty. The price
        # sample and delivered mass come from the previously announced EB.
        record["rb"] = 0
        record["rbBytes"] = 0
        record["eb"] = info.get("optimistic", 0)
        record["ebUrgent"] = info.get("urgent", 0)
        record["ebBytes"] = info.get("bytes", 0)
        record["ebHash"] = info.get("hash")
    # Strips vs hand flushes: the announced-EB mempool strip and the presenter's
    # flush both remove through removeTxsEvenIfValid, so both trace
    # ManuallyRemovedTxs. The strip traces its own LeiosMempoolStripped (ebHash
    # + txCount) right after its removal — pair each removal batch with it by
    # exact count (the kernel tracer's queue lags the mempool tracer's by
    # seconds, so batches wait). A batch no strip trace claims within the
    # window was a hand flush. Claimed txs are riders: held per EB and emitted
    # as "riding" once its certificate lands — only then did they settle.
    strip_pending = []   # [{"txids", "read"}] batches awaiting a strip trace
    riders_by_eb = {}    # ebHash -> {"slot", "txids"} awaiting a certificate
    FLUSH_AFTER_S = 15.0

    # A superseded EB's riders are given back to the mempool (the node's
    # readmission). The ones that re-enter get re-stripped under a later EB;
    # whatever never reappears could not re-enter — the quote climbed past
    # its max fee while its block was failing. That is a price verdict, not
    # a system failure: stage "returned", not "stranded". The clock starts
    # only when the node's own LeiosMempoolReadmitted trace confirms the
    # readmission ran (readmission happens at the NEXT body arrival, which
    # can be minutes away — a wall clock from supersession races it), and a
    # readmission that accepted nothing flushes the batch immediately. The
    # long fallback only guards a lost trace (log rotation).
    returned_pending = []   # [{"ebHash", "txids": set, "seen": t, "at": None|t}]
    RETURN_GRACE_S = 120.0
    RETURN_FALLBACK_S = 600.0

    def flush_returned():
        now = time.time()
        keep = []
        for batch in returned_pending:
            started = batch["at"]
            expired = (started is not None and now - started > RETURN_GRACE_S) \
                or (started is None and now - batch["seen"] > RETURN_FALLBACK_S)
            if not expired:
                keep.append(batch)
                continue
            if batch["txids"]:
                emit_removed([{"txid": t, "stage": "returned", "lane": "?"}
                              for t in sorted(batch["txids"])])
        returned_pending[:] = keep

    def expire_hand_flushes():
        flush_returned()
        now = time.time()
        while strip_pending and now - strip_pending[0]["read"] > FLUSH_AFTER_S:
            batch = strip_pending.pop(0)
            emit_removed([{"txid": t, "stage": "flushed", "lane": "?"}
                          for t in batch["txids"]])

    def write_leios_status():
        # "Stalled" = uncertified AND newer than the last certified EB. An older
        # uncertified EB was superseded — since the announced-EB strip its txs
        # left every mempool and went through readmission (stages "riding" or
        # "returned"), so counting them forever would inflate the number.
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

    def discard_incomplete_pending(p_):
        # A complete forge always receives TraceForgedBlock immediately after
        # forge-prices. If another round starts first, this partial record has no
        # consensus identity and must not become dashboard history.
        state[p_].pop("pending", None)

    def stage_candidate(p_, data):
        pending = state[p_].pop("pending", None)
        block_hash = data.get("block")
        if pending is None or not block_hash:
            return
        pending["slot"] = data.get("slot", pending.get("slot"))
        pending["blockNo"] = data.get("blockNo")
        pending["blockHash"] = block_hash
        pending["blockPrev"] = data.get("blockPrev")
        candidates_by_hash[block_hash] = pending

    def select_canonical(data):
        view = data.get("newSuffixSelectView") or {}
        block_no = view.get("blockNo")
        new_tip = data.get("newtip") or ""
        block_hash = new_tip.split("@", 1)[0]
        if block_no is None or not block_hash:
            return
        canonical_by_block_no[int(block_no)] = {
            "hash": block_hash,
            "changed": time.time(),
        }

    def flush_canonical():
        nonlocal block_index, last_emitted_block_no
        now = time.time()
        while True:
            # Never overtake a selected block whose forger trace has not reached
            # us yet. The node logs are consumed independently, so node1 can
            # select block N before the forging node's candidate for N has been
            # staged here. Emitting N+1 in that interval would create a fake
            # multi-step price jump in History.
            block_no = last_emitted_block_no + 1
            selection = canonical_by_block_no.get(block_no)
            if selection is None:
                return
            if now - selection["changed"] < CANONICAL_SETTLE_S:
                return
            record = candidates_by_hash.get(selection["hash"])
            if record is None:
                return
            info = record.get("certIn")
            if info is None:
                info = cert_by_slot.pop(record.get("slot"), None)
            if info is not None:
                apply_certificate(record, info)
            record["i"] = block_index
            emit(record)
            block_index += 1
            last_emitted_block_no = block_no
            for h, candidate in list(candidates_by_hash.items()):
                candidate_no = candidate.get("blockNo")
                if candidate_no is not None and candidate_no <= block_no:
                    del candidates_by_hash[h]
            for old_no in list(canonical_by_block_no):
                if old_no <= block_no:
                    del canonical_by_block_no[old_no]

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

    # The squeeze's own bid, read live from run-config.json (same dir): lets
    # the streams tag which priced-out txs are the burst's and which are the
    # crowd's collateral — the journal must not sell one as the other.
    run_config_path = os.path.join(os.path.dirname(os.path.abspath(out_path)), "run-config.json")
    t1_bid_cache = {"bid": 0, "read": 0.0}

    def current_t1_bid():
        now = time.time()
        if now - t1_bid_cache["read"] > 5:
            t1_bid_cache["read"] = now
            try:
                with open(run_config_path) as f:
                    t1_bid_cache["bid"] = int(json.load(f).get("t1Bid") or 0)
            except Exception:
                pass
        return t1_bid_cache["bid"]

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

    def publish_quotes(urgent, optimistic):
        # Quote publication is intentionally independent from canonical-history
        # settlement: the actor feeder must react to every current ledger quote,
        # not wait for the dashboard's fork filter.
        tmp = quotes_path + ".tmp"
        with open(tmp, "w") as q:
            json.dump({"urgent": urgent, "optimistic": optimistic}, q)
        os.replace(tmp, quotes_path)

    while True:
        progressed = False
        for p in log_paths:
            handle = handles[p]
            if handle is None:
                try:
                    handle = handles[p] = open(p, "r")
                    if from_now:
                        handle.seek(0, 2)
                except FileNotFoundError:
                    continue
            marker_changed = False
            try:
                marker_mtime = os.stat(truncate_markers[p]).st_mtime_ns
                marker_changed = marker_mtime > truncate_seen[p]
            except FileNotFoundError:
                marker_mtime = truncate_seen[p]
            # Survive both slow and fast in-place truncation. The size check
            # catches manual truncation; the marker catches truncate-and-regrow.
            if marker_changed or (
                os.path.exists(p) and os.stat(p).st_size < handle.tell()
            ):
                handle.close()
                handle = handles[p] = open(p, "r")
                truncate_seen[p] = marker_mtime
            line_start = handle.tell()
            line = handle.readline()
            if not line:
                continue
            # These are regular files that process-compose is still appending to.
            # readline() may therefore return the current, unterminated tail of a
            # JSON record. Keep the offset before it and retry once the writer has
            # completed the line; consuming the fragment can otherwise lose the
            # only TraceForgedBlock for a height and stall canonical publication.
            if not line.endswith("\n"):
                handle.seek(line_start)
                continue
            progressed = True
            if ("LeiosBlockForged" in line or "LeiosBlockCertified" in line
                    or "LeiosMempoolStripped" in line
                    or "LeiosMempoolReadmitted" in line):
                try:
                    inner = json.loads(json.loads(line)["message"])
                    data = inner.get("data", {})
                    kind = data.get("kind", "")
                    if kind == "LeiosBlockForged":
                        eb_forged.setdefault(
                            data.get("hash", ""),
                            {
                                "slot": data.get("slot", 0),
                                "numTxs": data.get("numTxs", 0),
                                "optimistic": state[p].get("eb", 0),
                                "urgent": state[p].get("ebUrgent", 0),
                                "bytes": state[p].get("ebBytes", 0),
                            },
                        )
                        pending = state[p].get("pending")
                        if pending is not None:
                            pending["ebHash"] = data.get("hash", "")[:8]
                        else:
                            state[p]["ebHash"] = data.get("hash", "")[:8]
                        write_leios_status()
                    elif kind == "LeiosBlockCertified":
                        h = data.get("ebHash", "")
                        at_slot = data.get("atSlot")
                        if h and h not in eb_certified:
                            info = certificate_info(h)
                            # This certifying block's record is held pending on
                            # this node (its forge-prices came a beat earlier) —
                            # stamp and release it now, so certification lands on
                            # the very block that certified.
                            pending = state[p].get("pending")
                            if pending is not None and pending.get("slot") == at_slot:
                                apply_certificate(pending, info)
                            else:
                                cert_by_slot[at_slot] = info
                                if len(cert_by_slot) > 50:  # unmatched — don't leak
                                    for k in sorted(cert_by_slot)[:25]:
                                        del cert_by_slot[k]
                            # The certificate settles the EB's riders on-chain.
                            riders = riders_by_eb.pop(h, None)
                            if riders:
                                emit_removed([{"txid": t, "stage": "riding", "lane": "?"}
                                              for t in riders["txids"]])
                            # An older EB still holding riders was superseded:
                            # its txs left every mempool for a block that never
                            # got its certificate.
                            eb_slot = eb_forged.get(h, {}).get("slot")
                            if eb_slot is not None:
                                stale = [k for k, v in riders_by_eb.items()
                                         if v["slot"] is not None and v["slot"] < eb_slot]
                                for k in stale:
                                    returned_pending.append(
                                        {"ebHash": k,
                                         "txids": set(riders_by_eb.pop(k)["txids"]),
                                         "seen": time.time(), "at": None})
                        eb_certified.add(h)
                        write_leios_status()
                    elif kind == "LeiosMempoolStripped" and p == node1:
                        n = data.get("txCount")
                        matches = [i for i, b in enumerate(strip_pending)
                                   if len(b["txids"]) == n]
                        if matches:
                            # Prefer the most recent batch: an old equal-count
                            # hand flush must not steal a fresh strip's claim.
                            batch = strip_pending.pop(matches[-1])
                            claimed = set(batch["txids"])
                            for rp in returned_pending:
                                rp["txids"] -= claimed
                            # A tx rides at most one live EB: a re-strip
                            # supersedes every older rider membership.
                            strip_slot = data.get("ebSlot")
                            for v in riders_by_eb.values():
                                if strip_slot is None or (v["slot"] is not None and v["slot"] < strip_slot):
                                    v["txids"] = [t for t in v["txids"] if t not in claimed]
                            h = data.get("ebHash", "")
                            if h in eb_certified:
                                # certificate already landed (queue lag)
                                emit_removed([{"txid": t, "stage": "riding", "lane": "?"}
                                              for t in batch["txids"]])
                            else:
                                r = riders_by_eb.setdefault(
                                    h, {"slot": data.get("ebSlot"), "txids": []})
                                r["txids"] += batch["txids"]
                    if kind == "LeiosMempoolReadmitted" and p == node1:
                        h = data.get("ebHash", "")
                        accepted = data.get("txCount") or 0
                        for batch in returned_pending:
                            if batch.get("ebHash") == h and batch["at"] is None:
                                batch["at"] = 0 if accepted == 0 else time.time()
                                break
                    continue
                except Exception:
                    pass
            if "TraceForgedBlock" in line:
                try:
                    inner = json.loads(json.loads(line)["message"])
                    data = inner.get("data") or {}
                    if data.get("kind") == "TraceForgedBlock":
                        stage_candidate(p, data)
                except Exception:
                    pass
                continue
            if p == node1 and (
                "ChainDB.AddBlockEvent.AddedToCurrentChain" in line
                or "ChainDB.AddBlockEvent.SwitchedToAFork" in line
            ):
                try:
                    inner = json.loads(json.loads(line)["message"])
                    data = inner.get("data") or {}
                    if data.get("kind") in (
                        "AddedToCurrentChain",
                        "TraceAddBlockEvent.SwitchedToAFork",
                    ):
                        select_canonical(data)
                except Exception:
                    pass
                continue
            if p == node1 and "ManuallyRemovedTxs" in line:
                # Both the announced-EB strip and the lane-flush control land
                # here. Hold the batch: the strip's own trace claims it by
                # count (-> riders of that EB); an unclaimed batch was a hand
                # flush. Short txids only.
                try:
                    inner = json.loads(json.loads(line)["message"])
                    data = inner.get("data") or {}
                    txs = [(t or "")[:8] for t in (data.get("txsRemoved") or []) if t]
                    if txs:
                        strip_pending.append({"txids": txs, "read": time.time()})
                    size = data.get("mempoolSize") or {}
                    if size:
                        write_mempool_live(size, inner.get("at"))
                except Exception:
                    pass
                continue
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
                                {"n": 0, "urgent": 0, "optimistic": 0, "bid": 0, "spent": 0, "burst": 0},
                            )
                            b["n"] += 1
                            b["bid"] += 1 if priced else 0
                            if priced and bid_required and current_t1_bid() \
                                    and int(bid_required.group(1)) == current_t1_bid():
                                b["burst"] += 1
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
                                # the squeeze's OWN txs among the priced-out —
                                # the rest is the crowd's collateral
                                "burstBidBelowQuote": b.get("burst", 0),
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
                                    waited = round((parse_iso(removed_at) - parse_iso(seen)).total_seconds(), 1)
                                except Exception:
                                    pass
                            t1 = current_t1_bid()
                            details.append({
                                "txid": e["txid"], "stage": e["stage"], "lane": e["lane"],
                                "t": removed_at, "addedAt": seen, "waitedS": waited,
                                "bid": e.get("bid"), "required": e.get("required"),
                                "burst": bool(t1) and e.get("bid") == t1,
                            })
                        emit_evicted_detail(details)
                        handled = True
                except Exception:
                    pass
                if handled:
                    continue
            if "NodeIsLeader" in line:
                # The forging slot for THIS round: NodeIsLeader fires with the
                # slot just before the forge-lanes/prices traces, so it is set
                # by the time this round's block record is emitted. The dashboard
                # differences consecutive slots to show the gap between blocks
                # (which the 10-slot certification gap is measured against).
                try:
                    inner = json.loads(json.loads(line)["message"])
                    sl = (inner.get("data") or {}).get("slot")
                    if sl is not None:
                        state[p]["leaderSlot"] = int(sl)
                except Exception:
                    pass
                continue
            lane = LANE_RE.search(line)
            if lane:
                discard_incomplete_pending(p)
                state[p]["rb"], state[p]["eb"] = int(lane.group(1)), int(lane.group(2))
                state[p]["rbBytes"] = int(lane.group(3)) if lane.group(3) else None
                state[p]["ebBytes"] = int(lane.group(4)) if lane.group(4) else None
                state[p]["ebHeld"] = lane.group(5) == "true" if lane.group(5) else None
                # urgent riders merged into the EB (absent on pre-rider builds)
                state[p]["ebUrgent"] = int(lane.group(6)) if lane.group(6) else None
                continue
            queue = QUEUE_RE.search(line)
            if queue:
                state[p]["qu"], state[p]["qo"] = int(queue.group(1)), int(queue.group(2))
                if queue.group(3) is not None:
                    qu_bytes, qu_cap = int(queue.group(3)), int(queue.group(4))
                    qo_bytes, qo_cap = int(queue.group(6)), int(queue.group(7))
                    qu_secs, qo_secs = float(queue.group(5)), float(queue.group(8))
                    qu_time_budget = MEMPOOL_TIME_BUDGET_S * URGENT_TIME_SHARE
                    qo_time_budget = MEMPOOL_TIME_BUDGET_S * (1 - URGENT_TIME_SHARE)
                    state[p]["pool"] = {
                        "quBytePct": round(100 * qu_bytes / qu_cap, 1) if qu_cap else None,
                        "quTimePct": round(100 * qu_secs / qu_time_budget, 1),
                        "qoBytePct": round(100 * qo_bytes / qo_cap, 1) if qo_cap else None,
                        "qoTimePct": round(100 * qo_secs / qo_time_budget, 1),
                        # raw values so the dashboard can print the actual
                        # limits, not just percentages
                        "quBytes": qu_bytes, "quCap": qu_cap,
                        "quSecs": round(qu_secs, 2), "quBudgetS": qu_time_budget,
                        "qoBytes": qo_bytes, "qoCap": qo_cap,
                        "qoSecs": round(qo_secs, 2), "qoBudgetS": qo_time_budget,
                    }
                continue
            price = PRICE_RE.search(line)
            if price:
                urgent = int(price.group(1))
                optimistic = int(price.group(2))
                publish_quotes(urgent, optimistic)
                record = {
                    # the slot this block was forged in (from NodeIsLeader); the
                    # dashboard differences consecutive slots for the gap between
                    # blocks — what the 10-slot certification gap is measured on
                    "slot": state[p].get("leaderSlot"),
                    "urgent": urgent,
                    "optimistic": optimistic,
                    "rb": state[p]["rb"],
                    "eb": state[p]["eb"],
                    # bytes each lane occupies — the block's TRUE fullness
                    # (capacity is a byte budget; tx counts mislead when fat
                    # and thin txs mix)
                    "rbBytes": state[p].get("rbBytes"),
                    "ebBytes": state[p].get("ebBytes"),
                    "ebHeld": state[p].get("ebHeld"),
                    # urgent riders the EB carries (the lanes' FIFO merge)
                    "ebUrgent": state[p].get("ebUrgent"),
                    # the certificate this block counts (stamped by slot when the
                    # LeiosBlockCertified trace lands; usually attached while this
                    # record is held pending, just below)
                    "certIn": cert_by_slot.pop(state[p].get("leaderSlot"), None),
                    "pool": state[p].get("pool"),
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
                if record["certIn"] is not None:
                    apply_certificate(record, record["certIn"])
                # TraceForgedBlock follows with the candidate's hash, parent and
                # block number. ChainDB then decides whether that candidate is the
                # canonical tip; only that selected hash is emitted.
                state[p]["pending"] = record
        expire_hand_flushes()
        flush_canonical()
        if not progressed:
            time.sleep(0.25)


if __name__ == "__main__":
    main()
