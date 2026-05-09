# Threaded Mesh

This is a Vite browser example for the `primadb/threads` entrypoint.

It demonstrates:

- `bootstrapPrimadbThreads(...)`
- live query work on the threaded build
- cross-tab mesh sync in a shared room
- local relay signaling by default, with optional `BroadcastChannel` fallback
- an in-page JavaScript REPL with syntax highlighting against the live `db`, `cards`, and `mesh`
- a log console you can write to with `log(...)` or `threadedPackageDemo.log(...)`

By default it tries to connect to a local relay at `ws://127.0.0.1:9010`. Start the Primadb
relay before opening the page:

```bash
cd /path/to/primadb
cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010
```

If you want same-browser tab sync without a relay, override the signaling mode with
`?signal=broadcast`.

If the relay is not running, the page still stays usable locally, shows `relay=waiting`, and
retries the relay connection on the configured interval in the background.

## Run

```bash
cd /path/to/primadb/packages/primadb/examples
pnpm install
pnpm run dev
```

Open:

```text
http://127.0.0.1:4181/threaded-mesh/
```

Explicit local relay mode:

```text
http://127.0.0.1:4181/threaded-mesh/?signal=relay&relay=ws://127.0.0.1:9010
```

BroadcastChannel fallback mode:

```text
http://127.0.0.1:4181/threaded-mesh/?signal=broadcast
```

## REPL Notes

The page now includes a mesh REPL panel. The editor is preloaded with a few basic queries and runs
against the current live objects:

- `db`
- `cards`
- `mesh`
- `session`
- `log(...)`
- `clearLogs()`
- `persistNow()`

Use `Ctrl+Enter` or `Cmd+Enter` to execute the editor contents. If your script returns a value, it
is appended to the log panel automatically. `persistNow()` forces an immediate IndexedDB segment
flush for the current room, which is useful if you want to experiment and then reload immediately.

## Smoke Test

The example includes a browser smoke that verifies the threaded build boots, opens IndexedDB
segment persistence successfully, and reloads persisted cards:

```bash
cd /path/to/primadb/packages/primadb/examples
bash ./threaded-mesh/test-smoke.sh
```

This example uses `stun:stun.cloudflare.com:3478` by default. To override it from the URL, repeat
`ice=` with either a bare STUN/TURN URL or an encoded JSON object:

```text
http://127.0.0.1:4181/threaded-mesh/?signal=relay&relay=ws://127.0.0.1:9010&ice=stun:stun.l.google.com:19302
```
