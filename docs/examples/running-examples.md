---
title: Running Examples
sidebar_position: 2
---

## Standard Browser Build

```bash
cd /home/bitnom/Code/gunport/primadb
./build-wasm.sh
```

## Threaded Browser Build

```bash
cd /home/bitnom/Code/gunport/primadb
./build-wasm-threads.sh
```

## Relay Server

Many relay and mesh examples assume the included relay server:

```bash
cd /home/bitnom/Code/gunport/primadb
cargo run --features native-websocket --example ws_relay_server -- 127.0.0.1:9010
```

## Rust Full Node

The anchor-node example runs the relay and a local mesh peer together in one process:

```bash
cd /home/bitnom/Code/gunport/primadb
cargo run --features native-webrtc --example full_node -- --relay-bind 127.0.0.1:9010 --room demo --message "hello from the anchor node"
```

## Browser Relay Example

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-relay-notes/build.sh
./examples/browser-relay-notes/serve.sh
```

Open:

```text
http://127.0.0.1:4173/examples/browser-relay-notes/
```

## Browser Mesh Example

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-mesh-notes/build.sh
./examples/browser-mesh-notes/serve.sh
```

Open:

```text
http://127.0.0.1:4173/examples/browser-mesh-notes/
```

## Threaded Browser Mesh Example

```bash
cd /home/bitnom/Code/gunport/primadb
./examples/browser-threaded-mesh-notes/build.sh
./examples/browser-threaded-mesh-notes/serve.sh
```

Open:

```text
http://127.0.0.1:4175/examples/browser-threaded-mesh-notes/
```

## Package Consumer Browser App

```bash
cd /home/bitnom/Code/gunport/primadb/examples/browser-package-notes-vite
pnpm install
pnpm run dev
```

Open:

```text
http://127.0.0.1:4182/
```

## Browser Package Examples

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb/examples
pnpm install
pnpm run dev
```

Open:

```text
http://127.0.0.1:4181/
```

The package examples include the binary stream room at `/binary-stream-room/`.

## Full Cross-Target End-To-End Suite

```bash
cd /home/bitnom/Code/gunport/primadb
bash examples/test-all-targets-mesh-e2e.sh
```
