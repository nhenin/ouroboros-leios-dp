#!/usr/bin/env python3
"""Poll the ledger's fee-split pots into an NDJSON stream for the dashboard.

One record per observed block:

    {"i": ..., "slot": ..., "t": ..., "fees": ..., "donation": ...,
     "pendingRefunds": ..., "refundsDelivered": ..., "treasury": ...}

All values are MEASURED from the node's own ledger state (lovelace,
cumulative since genesis), not recomputed from prices:

    fees             what the fee pot kept (base, plus the whole bid of any
                     tx that named no refund account)
    donation         the premiums moved to the treasury-donation pot
    pendingRefunds   refunds recorded but not yet credited (an unregistered
                     account would park here forever)
    refundsDelivered the refund account's balance — refunds actually credited

`cardano-cli latest query ledger-state` for the Dijkstra era prints an
annotated-hex CBOR dump (the era has no ToJSON wired in the CLI), so this
script decodes the CBOR itself and reads the pots at fixed paths in
NewEpochState. The paths were pinned empirically against the running devnet
(the published prices of the pricing state matched the live stream's, block
for block) and hold for the pinned node/ledger versions of this prototype:

    fees      = nes[3][1][1][2]        (UTxOState.utxosFees)
    donation  = nes[3][1][1][5]        (UTxOState.utxosDonation)
    pricing   = nes[3][1][1][6]        ([urgent, optimistic, usage, pendingRefunds, signalWindows])
    treasury  = nes[3][0][0]           (ChainAccountState)
    accounts  = nes[3][1][0][2][0]     (map [0, stakeKeyhash] -> [balance, ...])

Usage:
  incentives-poller.py <out.ndjson> <live.ndjson> <refund-stake.vkey>
      --socket PATH --network-magic N [--interval SECONDS]
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time


# ---------------------------------------------------------------- CBOR


def cbor_decode(b, i=0):
    ib = b[i]
    mt, ai = ib >> 5, ib & 0x1F
    i += 1
    if ai < 24:
        n = ai
    elif ai == 24:
        n = b[i]
        i += 1
    elif ai == 25:
        n = int.from_bytes(b[i : i + 2], "big")
        i += 2
    elif ai == 26:
        n = int.from_bytes(b[i : i + 4], "big")
        i += 4
    elif ai == 27:
        n = int.from_bytes(b[i : i + 8], "big")
        i += 8
    elif ai == 31:
        n = None
    else:
        raise ValueError(f"cbor: additional info {ai}")
    if mt == 0:
        return n, i
    if mt == 1:
        return -1 - n, i
    if mt == 2:
        return ("bytes", b[i : i + n].hex()), i + n
    if mt == 3:
        return ("text", b[i : i + n].decode("utf8", "replace")), i + n
    if mt == 4:
        out = []
        if n is None:
            while b[i] != 0xFF:
                v, i = cbor_decode(b, i)
                out.append(v)
            return out, i + 1
        for _ in range(n):
            v, i = cbor_decode(b, i)
            out.append(v)
        return out, i
    if mt == 5:
        out = []
        if n is None:
            while b[i] != 0xFF:
                k, i = cbor_decode(b, i)
                v, i = cbor_decode(b, i)
                out.append((k, v))
            return ("map", out), i + 1
        for _ in range(n):
            k, i = cbor_decode(b, i)
            v, i = cbor_decode(b, i)
            out.append((k, v))
        return ("map", out), i
    if mt == 6:
        v, i = cbor_decode(b, i)
        return ("tag", n, v), i
    # mt == 7: simple values — none appear on the paths we read
    if ai == 20:
        return False, i
    if ai == 21:
        return True, i
    if ai == 22:
        return None, i
    return ("simple", n), i


def at(v, path):
    """Follow a fixed index path through the decoded tree."""
    for p in path:
        if isinstance(v, tuple) and v and v[0] == "tag":
            v = v[2]
        if isinstance(v, list):
            v = v[p]
        else:
            raise KeyError(f"unexpected node at {p}: {str(v)[:40]}")
    return v


# ---------------------------------------------------------------- pots


def stake_keyhash(vkey_path):
    """blake2b-224 of the key payload in a staking vkey text envelope."""
    envelope = json.load(open(vkey_path))
    raw = bytes.fromhex(envelope["cborHex"])
    if raw[:2] != bytes.fromhex("5820"):
        raise ValueError(f"{vkey_path}: not a 32-byte key envelope")
    return hashlib.blake2b(raw[2:], digest_size=28).hexdigest()


def query_pots(cli_args, refund_keyhash):
    diag = subprocess.run(
        ["cardano-cli", "latest", "query", "ledger-state", *cli_args],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if diag.returncode != 0:
        raise RuntimeError(diag.stderr.strip()[:200])
    hexstr = re.sub(r"[^0-9a-fA-F]", "", re.sub(r"#.*", "", diag.stdout))
    nes, _ = cbor_decode(bytes.fromhex(hexstr))

    pricing = at(nes, [3, 1, 1, 6])
    pending = pricing[3][1] if isinstance(pricing[3], tuple) else []
    delivered = 0
    for key, entry in at(nes, [3, 1, 0, 2, 0])[1]:
        # key = [0, stakeKeyhash] for key-hash credentials
        if isinstance(key, list) and len(key) == 2 and key[1][1] == refund_keyhash:
            delivered = entry[0]
            break
    return {
        "fees": at(nes, [3, 1, 1, 2]),
        "donation": at(nes, [3, 1, 1, 5]),
        "pendingRefunds": sum(coin for _, coin in pending),
        "refundsDelivered": delivered,
        "treasury": at(nes, [3, 0, 0]),
    }


def last_block(live_path):
    """(i, slot) of the newest record in live.ndjson, or (None, None)."""
    try:
        with open(live_path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - 4096))
            lines = handle.read().decode("utf8", "replace").strip().splitlines()
        record = json.loads(lines[-1])
        return record.get("i"), record.get("slot")
    except Exception:
        return None, None


# ---------------------------------------------------------------- main


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("out")
    parser.add_argument("live")
    parser.add_argument("refund_vkey")
    parser.add_argument("--socket", required=True)
    parser.add_argument("--network-magic", required=True)
    parser.add_argument("--interval", type=float, default=5.0)
    args = parser.parse_args()

    refund_keyhash = stake_keyhash(args.refund_vkey)
    cli_args = [
        "--testnet-magic",
        args.network_magic,
        "--socket-path",
        args.socket,
    ]
    print(f"incentives poller: refund account keyhash {refund_keyhash[:16]}…", flush=True)

    emitted_i = None
    while True:
        time.sleep(args.interval)
        i, slot = last_block(args.live)
        if i is None or i == emitted_i:
            continue
        try:
            pots = query_pots(cli_args, refund_keyhash)
        except Exception as exc:  # node busy / mid-restart: retry next tick
            print(f"(pots query failed: {exc})", flush=True)
            continue
        record = {"i": i, "slot": slot, "t": round(time.time(), 1), **pots}
        with open(args.out, "a") as handle:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
        emitted_i = i


if __name__ == "__main__":
    main()
