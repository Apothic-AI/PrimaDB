# Mesh Peer

This example joins a shared Primadb mesh room through the native Node addon.

It uses relay-backed signaling, so you can run the same room from:

- multiple Node terminals
- browser Primadb examples
- the Python package examples

The example is offline-first: if the relay is down, it still starts, keeps local durable state
available, and retries the relay connection in the background. You only need the relay once you
want peer discovery/signaling.

## Start A Relay

```bash
cd /home/bitnom/Code/gunport/primadb
cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010
```

## Run One Peer

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
pnpm install
pnpm run build
node ./examples/mesh-peer/index.mjs --room package-mesh --name node-a --message "hello from node"
```

Run the same command in a second terminal with a different `--name` to watch replication.
By default, the example keeps running until you stop it with `Ctrl+C`.

For a bounded run, add `--duration-ms`:

```bash
node ./examples/mesh-peer/index.mjs --room package-mesh --name node-a --duration-ms 15000
```

This example uses `stun:stun.cloudflare.com:3478` by default. To override it, repeat `--ice-server`.
Each value can be either a bare
STUN/TURN URL or a JSON object with `urls`, `username`, and `credential`:

```bash
node ./examples/mesh-peer/index.mjs \
  --room package-mesh \
  --ice-server stun:stun.l.google.com:19302 \
  --ice-server '{"urls":"turn:turn.example.com:3478","username":"user","credential":"pass"}'
```
