# Threaded Mesh

This is a standalone browser project for the `primadb/threads` entrypoint.

It demonstrates:

- `bootstrapPrimadbThreads(...)`
- live query work on the threaded build
- cross-tab mesh sync in a shared room
- optional relay signaling for cross-browser peers
- an in-page JavaScript REPL with syntax highlighting against the live `db`, `cards`, and `mesh`
- a log console you can write to with `log(...)` or `threadedPackageDemo.log(...)`

By default it uses `BroadcastChannel` signaling so multiple tabs in the same browser connect
without any extra infrastructure.

For cross-browser peers, start the Primadb relay and open the page with `?signal=relay`:

```bash
cd /home/bitnom/Code/gunport/primadb
cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010
```

## Run

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
pnpm install
pnpm run build
./examples/serve.sh
```

Open:

```text
http://127.0.0.1:4181/examples/threaded-mesh/
```

Cross-browser relay mode:

```text
http://127.0.0.1:4181/examples/threaded-mesh/?signal=relay&relay=ws://127.0.0.1:9010
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

Use `Ctrl+Enter` or `Cmd+Enter` to execute the editor contents. If your script returns a value, it
is appended to the log panel automatically.

This example uses `stun:stun.cloudflare.com:3478` by default. To override it from the URL, repeat
`ice=` with either a bare STUN/TURN URL or an encoded JSON object:

```text
http://127.0.0.1:4181/examples/threaded-mesh/?signal=relay&relay=ws://127.0.0.1:9010&ice=stun:stun.l.google.com:19302
```
