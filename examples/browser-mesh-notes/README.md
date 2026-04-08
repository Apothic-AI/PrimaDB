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

The tabs should discover each other automatically and exchange changes over WebRTC.
