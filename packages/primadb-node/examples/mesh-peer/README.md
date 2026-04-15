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
cargo run --example ws_relay_server -- 127.0.0.1:9010
```

## Run One Peer

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
npm install
npm run build
node ./examples/mesh-peer/index.mjs --room package-mesh --name node-a --message "hello from node"
```

Run the same command in a second terminal with a different `--name` to watch replication.
