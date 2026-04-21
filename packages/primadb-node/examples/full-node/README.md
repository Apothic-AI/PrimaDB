# Full Node

This example runs a Node mesh peer together with a colocated relay hosted through the
`primadb-node` package itself.

Use it as an anchor node: start one full node, then point browser, Node, Python, or Rust mesh peers
at its relay URL.

## Build The Package

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
pnpm install
pnpm run build
```

## Run One Full Node

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
node ./examples/full-node/index.mjs --room package-full-node --name node-anchor --message "hello from the anchor node"
```

By default this starts a local relay on `127.0.0.1:9010`, opens durable storage under
`examples/full-node/.data/full-node/<name>`, joins the mesh through that relay, and keeps running
until you stop it with `Ctrl+C`.

For a bounded run:

```bash
node ./examples/full-node/index.mjs --room package-full-node --name node-anchor --duration-ms 15000
```

If you want the relay to bind to a different address:

```bash
node ./examples/full-node/index.mjs --relay-bind 0.0.0.0:9010 --relay-url ws://127.0.0.1:9010
```

`--relay-bind` controls where the relay listens. `--relay-url` controls what URL the local mesh peer
uses and advertises. If you bind to `0.0.0.0`, set `--relay-url` to a real host/URL that peers can
use.

This example uses `stun:stun.cloudflare.com:3478` by default. To override it, repeat
`--ice-server`. Each value can be either a bare STUN/TURN URL or a JSON object with `urls`,
`username`, and `credential`:

```bash
node ./examples/full-node/index.mjs \
  --room package-full-node \
  --ice-server stun:stun.l.google.com:19302 \
  --ice-server '{"urls":"turn:turn.example.com:3478","username":"user","credential":"pass"}'
```

## Join It From Another Peer

For example, from the Node mesh peer example:

```bash
node ./examples/mesh-peer/index.mjs --room package-full-node --relay ws://127.0.0.1:9010 --name node-leaf
```
