#!/usr/bin/env python3
"""Aggregate the actor feeder's decision log into NDJSON buckets for the dashboard.

The actor feeder logs one line per demand it weighs:

    actor decision: n=<k> lane=urgent|optimistic|shed value=<v> urgency=<u> size=<s> quoteUrgent=<qu> quoteOptimistic=<qo> bid=<b>

We follow that log and emit one record per fixed-size bucket of decisions:

    {"i": <bucket>, "urgent": <n>, "optimistic": <n>, "shed": <n>, "qu": <qu>, "qo": <qo>}

so the dashboard can show how the urgent / optimistic / walk-away split shifts as
the published quotes move — i.e. what the actors are actually doing.

Usage: actor-aggregator.py <feeder.log> <out.ndjson> [bucket-size]
"""

import json
import os
import re
import sys
import time

DECISION_RE = re.compile(
    r"actor decision:.*?lane=(\w+).*?quoteUrgent=(\d+).*?quoteOptimistic=(\d+)"
)


def follow(path):
    handle = None
    while handle is None:
        try:
            handle = open(path, "r")
        except FileNotFoundError:
            time.sleep(0.5)
    while True:
        line = handle.readline()
        if line:
            yield line
            continue
        if os.path.exists(path) and os.stat(path).st_size < handle.tell():
            handle.close()
            handle = open(path, "r")
        else:
            time.sleep(0.3)


def main():
    if len(sys.argv) < 3:
        sys.exit("usage: actor-aggregator.py <feeder.log> <out.ndjson> [bucket-size]")
    log_path, out_path = sys.argv[1], sys.argv[2]
    bucket_size = int(sys.argv[3]) if len(sys.argv) > 3 else 120

    counts = {"urgent": 0, "optimistic": 0, "shed": 0}
    last_qu, last_qo = 704, 44
    seen = 0
    bucket = 0
    for line in follow(log_path):
        match = DECISION_RE.search(line)
        if not match:
            continue
        lane, last_qu, last_qo = match.group(1), int(match.group(2)), int(match.group(3))
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


if __name__ == "__main__":
    main()
