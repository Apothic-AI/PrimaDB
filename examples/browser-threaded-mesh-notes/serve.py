#!/usr/bin/env python3
import os
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
HOST = os.environ.get("PRIMADB_HOST", "127.0.0.1")
PORT = int(os.environ.get("PRIMADB_PORT", "4175"))


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


class CoopCoepHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_headers(self):
        self.send_header("Cross-Origin-Opener-Policy", "same-origin")
        self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()


if __name__ == "__main__":
    server = ReusableThreadingHTTPServer((HOST, PORT), CoopCoepHandler)
    print(f"Serving {ROOT} at http://{HOST}:{PORT}/")
    server.serve_forever()
