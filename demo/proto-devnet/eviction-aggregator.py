#!/usr/bin/env python3
"""Turn the conflict feeder's submission results into evictions.ndjson for the dashboard.

The node returns admission rejections to the submitter — AllInputsAreSpent for a
cross-lane conflict (an urgent tx already took the input), BidBelowQuote for an
underpriced bid — but it does NOT emit a mempool trace for them (only successful
adds and the rare re-validation removal are traced). So the ground truth for "what
got dropped, and why" is the feeder's own log, which reports every submission result.

The conflict feeder logs one line per losing tx, e.g.:

    7: optimistic <txid> (conflicting) -> rejected: ... (AllInputsAreSpent :| [])

We follow that log and append one record per drop (which the dashboard polls):

    {"i", "t", "stage", "n", "bidBelowQuote", "allInputsAreSpent", "other", "mempoolTxs"}

Usage: eviction-aggregator.py <feeder.log> <out.ndjson>
"""

import json
import os
import sys
import time
from datetime import datetime, timezone


def classify(line):
    bid = 1 if "BidBelowQuote" in line else 0
    spent = 1 if "AllInputsAreSpent" in line else 0
    # An optimistic tx that was admitted then evicted when the RB applied still
    # loses to a cross-lane conflict, so count it the same way.
    if not (bid or spent) and "accepted; evicted" in line:
        spent = 1
    other = 0 if (bid or spent) else 1
    return bid, spent, other


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: eviction-aggregator.py <feeder.log> <out.ndjson>")
    log_path, out_path = sys.argv[1], sys.argv[2]
    # The run script truncates the stream once at launch; append-only here so a
    # mid-run (re)start never wipes what other writers appended.
    index = 0
    handle = None

    while True:
        if handle is None:
            try:
                handle = open(log_path, "r")
            except FileNotFoundError:
                time.sleep(0.3)
                continue
        # Survive log rotation/truncation.
        if os.path.exists(log_path) and os.stat(log_path).st_size < handle.tell():
            handle.close()
            handle = open(log_path, "r")
        line = handle.readline()
        if not line:
            time.sleep(0.25)
            continue
        if "(conflicting) ->" not in line:
            continue
        bid, spent, other = classify(line)
        # The feeder logs which lane the losing tx bought ("N: urgent ..." /
        # "N: optimistic ..."), so the dashboard can say what kind of tx lost.
        record = {
            "i": index,
            "t": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "stage": "rejected",
            "n": 1,
            "urgent": 1 if ": urgent " in line else 0,
            "optimistic": 1 if ": optimistic " in line else 0,
            "bidBelowQuote": bid,
            "allInputsAreSpent": spent,
            "other": other,
            "mempoolTxs": None,
        }
        with open(out_path, "a") as out:
            out.write(json.dumps(record) + "\n")
            out.flush()
        index += 1


if __name__ == "__main__":
    main()
