# browser-relay-notes

`browser-relay-notes` is a browser Primadb example that syncs through a real WebSocket relay server.

It demonstrates:

- Primadb compiled to WebAssembly.
- Automatic IndexedDB persistence hooks.
- Primadb's shared `connectRelay(...)` client facade with `WebSocketSync` ack/retry/requeue behavior.
- Peer recommendations delivered by the relay.
- Remote `get`, `query`, `lex`, and `snapshot` requests over the routed wire protocol.
- Remote live watch flows through `watchRemoteGet(...)` and `watchRemoteQuery(...)`.
- Chunked query/snapshot reply assembly in the browser.
- A shared note list replicated between multiple browsers.
- Query filters and reactive UI rendering.

## Run

1. Build the WASM package:

```bash
./examples/browser-relay-notes/build.sh
```

2. Start the relay server:

```bash
cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010
```

3. Serve the frontend:

```bash
./examples/browser-relay-notes/serve.sh
```

4. Open `http://127.0.0.1:4173/examples/browser-relay-notes/` in multiple tabs or browsers.

## Notes

- Use the “Seed 90 notes” and “Probe remote peer” controls to force large remote query/snapshot replies and verify chunked response assembly.
- Archiving removes the note from the underlying Primadb set membership.

## Automated Check

Run the browser/native relay smoke test:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/browser-relay-notes/test-browser-native-smoke.sh
```

That script proves native -> browser live relay sync against the real relay server.

Run the browser relay watch smoke test:

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/browser-relay-notes/test-watch-smoke.sh
```
