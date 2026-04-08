# Browser Threaded Mesh Notes

`browser-threaded-mesh-notes` combines the opt-in `wasm-threads` Primadb build with the browser
peer-to-peer mesh path.

It demonstrates:

- `wasm-bindgen-rayon` thread-pool initialization before Primadb startup
- `SharedArrayBuffer`-based threaded WASM under COOP/COEP headers
- peer discovery over `BroadcastChannel`
- direct browser sync over `RTCPeerConnection` data channels
- a threaded query workload over the same shared note graph

## Run

1. Build the threaded WASM package:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-threaded-mesh-notes/build.sh
```

2. Serve the repo root with COOP/COEP headers:

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-threaded-mesh-notes/serve.sh
```

3. Open `http://127.0.0.1:4175/examples/browser-threaded-mesh-notes/` in two tabs.

4. Use `Seed Shared Load` in one tab, then run `Run Parallel Query` in either tab.

The tabs should discover each other automatically, sync the seeded notes over WebRTC, and report
that the threaded query path is active.
