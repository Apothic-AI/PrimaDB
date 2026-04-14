# Mesh Peer

This example joins a shared Primadb mesh room through the native Python extension.

It uses relay-backed signaling, so you can run the same room from:

- multiple Python terminals
- browser Primadb examples
- the Node package examples

## Start A Relay

```bash
cd /home/bitnom/Code/gunport/primadb
cargo run --example ws_relay_server -- 127.0.0.1:9010
```

## Run One Peer

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python/examples/mesh_peer
uv sync
uv run python main.py --room package-mesh --name py-a --message "hello from python"
```

Run the same command in a second terminal with a different `--name` to watch replication.
