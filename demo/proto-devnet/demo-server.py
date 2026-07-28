#!/usr/bin/env python3
"""Static file server for the demo dir + a tiny write endpoint for the actor config.

  GET  /<file>          serves files from the demo dir (like `http.server`)
  GET  /actor-config    returns the current actor-config.json (or {})
  POST /actor-config    body = JSON; writes actor-config.json atomically. The live
                        actor feeder re-reads it on its next decision, so changing
                        the population from the dashboard takes effect within a
                        moment, no restart.

Usage: demo-server.py <port> <dir> <config-path>
"""

import json
import os
import subprocess
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer


def main():
    args = sys.argv[1:]
    # --read-only: the audience copy. Serves every live stream but refuses
    # every command with an explicit marker, so the page shows "watching
    # mode" instead of failing quietly. One presenter drives (full server),
    # everyone else watches this one.
    read_only = "--read-only" in args
    if read_only:
        args.remove("--read-only")
    if len(args) not in (3, 4):
        sys.exit("usage: demo-server.py [--read-only] <port> <dir> <config-path> [<devnet-working-dir>]")
    port = int(args[0])
    directory = args[1]
    config_path = args[2]
    working_dir = args[3] if len(args) == 4 else "/tmp/dijkstra-live-demo"

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=directory, **kwargs)

        def log_message(self, *args):
            pass  # keep the demo console quiet

        def _send_json(self, code, body):
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path.rstrip("/") == "/actor-config":
                body = b"{}"
                try:
                    with open(config_path, "rb") as handle:
                        body = handle.read()
                except FileNotFoundError:
                    pass
                self._send_json(200, body)
                return
            return super().do_GET()

        def do_POST(self):
            if read_only:
                self._send_json(403, b'{"error":"read-only","readOnly":true}')
                return
            if self.path.rstrip("/") == "/restart-network":
                # Full restart: fresh chain, every component relaunched. The
                # launcher survives this server's own death (new session) and
                # owns the orderly shutdown of the current supervisor. The
                # enclosing launch script supplies its own portable path.
                launcher = os.environ.get("DEMO_LAUNCHER")
                if not launcher or not os.path.isfile(launcher):
                    self._send_json(
                        409,
                        b'{"error":"restart unavailable: DEMO_LAUNCHER is not configured"}',
                    )
                    return
                self._send_json(200, b'{"ok":true,"restarting":true}')
                restart_command = """
                    sleep 1
                    exec bash "$1"
                """
                subprocess.Popen(["/bin/sh", "-c", restart_command, "restart-demo", launcher],
                                 stdout=open("/tmp/demo-restart.log", "w"),
                                 stderr=subprocess.STDOUT,
                                 start_new_session=True)
                return
            if self.path.rstrip("/") == "/flush-queues":
                # Flush a chosen lane, or both. One lane: raise each node's
                # flag file — the node's watcher removes that lane's waiting
                # txs within a couple of seconds, no restart. Both: kill the
                # three cardano-node processes; process-compose (restart:
                # always) brings them back within seconds with EMPTY
                # mempools. Either way the chain on disk is untouched.
                length = int(self.headers.get("Content-Length", "0"))
                lane = "all"
                if length:
                    try:
                        lane = json.loads(self.rfile.read(length)).get("lane", "all")
                    except Exception:
                        pass
                if lane in ("urgent", "optimistic"):
                    for node in ("node1", "node2", "node3"):
                        try:
                            flag = os.path.join(working_dir, node, "flush-lane")
                            tmp = flag + ".tmp"
                            with open(tmp, "w") as handle:
                                handle.write(lane)
                            os.replace(tmp, flag)
                        except OSError:
                            pass  # node dir not there (devnet down) - flush what exists
                else:
                    subprocess.run(["pkill", "-f", "cardano-node run"], check=False)
                self._send_json(200, b'{"ok":true}')
                return
            # Writable endpoints: the actor population, and the eviction-generator
            # switch (the run script's controller loop watches the latter and
            # starts/stops the matching load generator).
            targets = {
                "/actor-config": config_path,
                "/eviction-control": os.path.join(directory, "eviction-control.json"),
            }
            target = targets.get(self.path.rstrip("/"))
            if target is not None:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
                try:
                    config = json.loads(raw)
                    if target.endswith("eviction-control.json"):
                        if config.get("mode") not in ("off", "type1", "type2", "type3"):
                            raise ValueError("mode must be off | type1 | type2 | type3")
                    else:
                        # Merge into the existing config instead of replacing it,
                        # so a tab that never touched a control cannot wipe what
                        # another tab set. Posting null for a key deletes it
                        # (e.g. laneMix: null = hand the lane choice back to the
                        # simulated senders).
                        current = {}
                        try:
                            with open(target) as handle:
                                current = json.load(handle)
                        except (FileNotFoundError, ValueError):
                            pass
                        current.update(config)
                        config = {k: v for k, v in current.items() if v is not None}
                except Exception as exc:  # noqa: BLE001 - report any parse error to the caller
                    self._send_json(400, json.dumps({"error": str(exc)}).encode())
                    return
                tmp = "%s.%d.tmp" % (target, os.getpid())  # unique per writer: two tabs can't tear one tmp
                with open(tmp, "w") as handle:
                    json.dump(config, handle)
                os.replace(tmp, target)
                self._send_json(200, b'{"ok":true}')
                return
            self.send_response(404)
            self.end_headers()

    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
