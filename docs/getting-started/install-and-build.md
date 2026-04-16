---
title: Install And Build
sidebar_position: 1
---

PrimaDB has four main consumption paths:

- Rust crate
- browser WASM runtime
- TypeScript package
- native Node package
- native Python package

The build story is intentionally split by host environment. The core model is shared, but the
toolchains are not.

## Repo Prerequisites

At the repo level, expect these tools:

- Rust toolchain
- `npm` for the browser and Node package flows
- `uv` for the Python package flow
- a modern browser for the browser examples and browser package

## Core Rust Build

```bash
cd /home/bitnom/Code/gunport/primadb
cargo test --features "crypto native-websocket native-webrtc"
```

That covers the current richest native feature set.

## Default Browser WASM Build

```bash
cd /home/bitnom/Code/gunport/primadb
./build-wasm.sh
```

This is the stable browser path. It does not require the threaded WebAssembly toolchain setup.

## Threaded Browser WASM Build

```bash
cd /home/bitnom/Code/gunport/primadb
./build-wasm-threads.sh
```

Use this only when you want the opt-in `wasm-threads` build. It has stricter requirements:

- nightly Rust
- `-Z build-std`
- shared-memory linker flags
- `SharedArrayBuffer`
- COOP/COEP at runtime

## TypeScript Package

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb
npm install
npm run build
```

This packages the browser WASM build into a browser-first npm surface.

## Native Node Package

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-node
npm install
npm run build
```

This builds the native addon-backed Node surface.

## Native Python Package

```bash
cd /home/bitnom/Code/gunport/primadb/packages/primadb-python
uv sync
```

Run package commands through `uv run`.

## Docs Site

The docs site itself lives in [website/](https://github.com/Apothic-AI/PrimaDB/tree/master/website),
but all authored docs live in the repo’s top-level [docs/](https://github.com/Apothic-AI/PrimaDB/tree/master/docs)
directory.

```bash
cd /home/bitnom/Code/gunport/primadb/website
npm install
npm run start
```
