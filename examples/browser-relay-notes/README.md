# browser-relay-notes

`browser-relay-notes` is a browser Primadb example that syncs through a real WebSocket relay server.

It demonstrates:

- Primadb compiled to WebAssembly.
- Automatic IndexedDB persistence hooks.
- Primadb's built-in `WebSocketSync` ack/retry/requeue behavior.
- A shared note list replicated between multiple browsers.
- Query filters and reactive UI rendering.

## Run

1. Build the WASM package:

```bash
./examples/browser-relay-notes/build.sh
```

2. Start the relay server:

```bash
cargo run --example ws_relay_server -- 127.0.0.1:9010
```

3. Serve the frontend:

```bash
./examples/browser-relay-notes/serve.sh
```

4. Open `http://127.0.0.1:4173/examples/browser-relay-notes/` in multiple tabs or browsers.

## Notes

- The relay is intentionally dumb: it forwards text frames and relies on Primadb's sync framing to handle idempotence and acknowledgments.
- Deletes are soft deletes (`archived: true`) because Primadb does not yet support removing a member from a set.
