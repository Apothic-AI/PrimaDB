#!/usr/bin/env python3
from __future__ import annotations

import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
HOST = os.environ.get("PRIMADB_PACKAGE_HOST", "127.0.0.1")
PORT = int(os.environ.get("PRIMADB_PACKAGE_PORT", "4181"))


class PrimadbPackageHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self) -> None:
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Origin-Agent-Cluster", "?1")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), PrimadbPackageHandler)
    print(f"Serving {ROOT} at http://{HOST}:{PORT}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
