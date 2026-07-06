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
    if len(sys.argv) != 4:
        sys.exit("usage: demo-server.py <port> <dir> <config-path>")
    port = int(sys.argv[1])
    directory = sys.argv[2]
    config_path = sys.argv[3]

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
            if self.path.rstrip("/") == "/flush-queues":
                # Flush = restart the nodes' short-term memory: kill the three
                # cardano-node processes; process-compose (restart: always)
                # brings them back within seconds with EMPTY mempools. The
                # chain on disk is untouched — only waiting txs are forgotten.
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
                tmp = target + ".tmp"
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
