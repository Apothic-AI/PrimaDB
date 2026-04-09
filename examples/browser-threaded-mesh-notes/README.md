# Browser Threaded Mesh Notes

`browser-threaded-mesh-notes` combines the opt-in `wasm-threads` Primadb build with the browser
peer-to-peer mesh path.

It demonstrates:

- `wasm-bindgen-rayon` thread-pool initialization before Primadb startup
- `SharedArrayBuffer`-based threaded WASM under COOP/COEP headers
- relay-backed peer discovery and signaling by default
- direct browser sync over `RTCPeerConnection` data channels
- configurable ICE servers with built-in STUN defaults
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

3. Start the relay:

```bash
cd /home/bitnom/Code/gunport/primadb
cargo run --example ws_relay_server -- 127.0.0.1:9010
```

4. Open `http://127.0.0.1:4175/examples/browser-threaded-mesh-notes/` in two browsers or tabs.

5. Use `Seed Shared Load` in one browser, then run `Run Parallel Query` in either browser.

The browsers should discover each other through the relay, sync the notes over direct WebRTC data
channels, and report that the threaded query path is active.

By default the demo uses relay signaling at `ws://<current-host>:9010`. Append
`?signal=broadcast` to force the old same-browser `BroadcastChannel` signaling path instead.

## Automated Check

Run the two-page threaded P2P smoke test:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/browser-threaded-mesh-notes/test-two-page-smoke.sh
```

The script builds the threaded package if needed, starts the COOP/COEP server if needed, opens
two Playwright pages in the same room, starts the relay if needed, confirms the `wasm-threads`
build is active, waits for a live WebRTC peer connection, checks that a note replicates without
reload, seeds the shared load, and verifies the parallel query output.

Run the cross-browser smoke test:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/browser-threaded-mesh-notes/test-cross-browser-smoke.sh
```

That script launches Chromium and Firefox against the same relay-backed room and confirms live
cross-browser replication over WebRTC.
