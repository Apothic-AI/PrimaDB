# browser-mesh-notes

`browser-mesh-notes` is the default browser WebRTC mesh example for Primadb.

It demonstrates:

- Primadb compiled to WebAssembly.
- Automatic IndexedDB persistence hooks.
- Relay-backed signaling by default for cross-browser and cross-machine peers.
- Optional `BroadcastChannel` signaling with `?signal=broadcast`.
- Direct browser sync over `RTCPeerConnection` data channels.
- Routed sync frames over Primadb's mesh envelope layer.

## Run

1. Build the WASM package:

```bash
./examples/browser-mesh-notes/build.sh
```

2. Start the relay:

```bash
cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010
```

3. Serve the repo root:

```bash
./examples/browser-mesh-notes/serve.sh
```

4. Open `http://127.0.0.1:4173/examples/browser-mesh-notes/` in two tabs or browsers.

5. For isolated sessions, append `?room=my-room` to the URL. The room name also
   scopes the example's browser storage.

6. To force browser-local signaling instead of the relay, append
   `?signal=broadcast`.

Peers should discover each other automatically and exchange changes over WebRTC.

## Automated Check

Run the two-page default P2P smoke test:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/browser-mesh-notes/test-two-page-smoke.sh
```

The script builds the package if needed, starts the static server if needed,
starts the relay if needed, opens two pages in a fresh room, waits for the WebRTC peer connection, and
confirms that a note replicates live without a reload.

Run the browser/native mixed-host smoke test:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/browser-mesh-notes/test-browser-native-smoke.sh
```
