# browser-mesh-notes

`browser-mesh-notes` is a browser-to-browser Primadb example with no relay server.

It demonstrates:

- Primadb compiled to WebAssembly.
- Automatic IndexedDB persistence hooks.
- Peer discovery over `BroadcastChannel`.
- Direct browser sync over `RTCPeerConnection` data channels.
- Routed sync frames over Primadb's mesh envelope layer.

## Run

1. Build the WASM package:

```bash
./examples/browser-mesh-notes/build.sh
```

2. Serve the repo root:

```bash
./examples/browser-mesh-notes/serve.sh
```

3. Open `http://127.0.0.1:4173/examples/browser-mesh-notes/` in two tabs.

4. For isolated sessions, append `?room=my-room` to the URL. The room name also
   scopes the example's browser storage.

The tabs should discover each other automatically and exchange changes over WebRTC.

## Automated Check

Run the two-page default P2P smoke test:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/browser-mesh-notes/test-two-page-smoke.sh
```

The script builds the package if needed, starts the static server if needed,
opens two pages in a fresh room, waits for the WebRTC peer connection, and
confirms that a note replicates live without a reload.
