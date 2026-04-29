---
title: API Reference
sidebar_position: 1
---

PrimaDB now ships both guide-style docs and reference-style docs.

The pages in this section are generated from the public package declarations, the browser WASM
export layer, and the Rust crate surface. They are meant to answer "what exactly is exported here?"
rather than "how should I learn the product?"

## What Is Covered

- `primadb` browser package entrypoints and hook helpers
- browser WASM runtime classes and top-level functions
- `primadb/threads`
- `primadb/gun`
- `primadb-node`
- `primadb-python`
- the Rust crate, with bundled rustdoc

The generated references include the current strict consistency surface: `Scope`, `ScopePolicy`,
transaction step payloads, transaction reports, provisional proposals, and relay
`remoteTransaction(...)` / `remote_transaction(...)` submission APIs where those bindings expose
them.

## How To Use This Section

- Start with the SDK guide pages if you need concepts or setup.
- Use this section when you need method signatures, class members, hook contracts, or the exact
  public type surface.
- For Rust, use the bundled rustdoc when you need the full crate browser instead of the summarized
  re-export map.
