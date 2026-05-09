# Full Node

This example runs a Python mesh peer together with a colocated relay hosted through the
`primadb-python` package itself.

Use it as an anchor node: start one full node, then point browser, Node, Python, or Rust mesh peers
at its relay URL.

## Run It

```bash
cd /path/to/primadb/packages/primadb-python/examples/full_node
uv sync
uv run python main.py --room package-full-node --name py-anchor --message "hello from the anchor node"
```

By default this starts a local relay on `127.0.0.1:9010`, opens durable storage under
`examples/full_node/.data/full_node/<name>`, joins the mesh through that relay, and keeps running
until you stop it with `Ctrl+C`.

For a bounded run:

```bash
uv run python main.py --room package-full-node --name py-anchor --duration-ms 15000
```

If you want the relay to bind to a different address:

```bash
uv run python main.py --relay-bind 0.0.0.0:9010 --relay-url ws://127.0.0.1:9010
```

`--relay-bind` controls where the relay listens. `--relay-url` controls what URL the local mesh peer
uses and advertises. If you bind to `0.0.0.0`, set `--relay-url` to a real host/URL that peers can
use.

This example uses `stun:stun.cloudflare.com:3478` by default. To override it, repeat
`--ice-server`. Each value can be either a bare STUN/TURN URL or a JSON object with `urls`,
`username`, and `credential`:

```bash
uv run python main.py \
  --room package-full-node \
  --ice-server stun:stun.l.google.com:19302 \
  --ice-server '{"urls":"turn:turn.example.com:3478","username":"user","credential":"pass"}'
```

## Join It From Another Peer

For example, from the Python mesh peer example:

```bash
cd /path/to/primadb/packages/primadb-python/examples/mesh_peer
uv sync
uv run python main.py --room package-full-node --relay ws://127.0.0.1:9010 --name py-leaf
```
