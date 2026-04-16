---
title: Build Targets
sidebar_position: 1
---

PrimaDB deliberately has multiple build targets because the deployment constraints are genuinely
different.

## Rust Core

- canonical semantics
- native storage
- native relay
- native mesh

## Default Browser WASM

- stable browser-first target
- no threaded WebAssembly requirements
- best default for compatibility

Build:

```bash
./build-wasm.sh
```

## Threaded Browser WASM

- opt-in performance profile
- Rayon-backed parallel work in the browser
- requires nightly plus COOP/COEP

Build:

```bash
./build-wasm-threads.sh
```

## TypeScript Package

Packages the browser runtime:

- `primadb`
- `primadb/threads`
- `primadb/gun`

## Native Packages

- `primadb-node`
- `primadb-python`

These are native bindings over the Rust runtime, not browser-WASM-in-Node/Python shims.
