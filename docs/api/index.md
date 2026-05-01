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
- `primadb/moq` browser helper entrypoint
- browser WASM runtime classes and top-level functions
- `primadb/threads`
- `primadb/gun`
- `primadb-node`
- `primadb-node/moq`
- `primadb-python`
- Python MoQ helpers through the public Python stub
- the Rust crate, with bundled rustdoc

The generated references include the current strict consistency surface: `Scope`, `ScopePolicy`,
transaction step payloads, transaction reports, provisional proposals, and relay
`remoteTransaction(...)` / `remote_transaction(...)` submission APIs where those bindings expose
them.

They also include the current crypto/auth package surface, including identity generation,
password-derived keys, snapshot/transport encryption key setters, signed writes, and authenticated
session config types.

## Source Of Truth

The API pages are generated from current source files during `pnpm --dir website run generate:api`:

- browser package declarations: `packages/primadb/index.ts`, `packages/primadb/moq.ts`, and `packages/primadb/hooks.ts`
- browser runtime exports: `src/wasm.rs`
- threaded browser entrypoint: `packages/primadb/threads.ts`
- Gun-compatible runtime types: `packages/primadb/gun.ts` and `packages/primadb/runtime/primadb-gun.ts`
- Node package declarations: `packages/primadb-node/index.d.ts` and `packages/primadb-node/moq.d.ts`
- Python package declarations: `packages/primadb-python/python/primadb/__init__.pyi`
- Rust crate reference: `src/lib.rs` plus bundled rustdoc under `/rust-api/primadb/`

If a new public API is added, update the relevant declaration/source file first, then regenerate
the docs. For Rust-only details, bundled rustdoc is the complete reference; the `rust-crate` page is
a summarized re-export map.

## How To Use This Section

- Start with the SDK guide pages if you need concepts or setup.
- Use this section when you need method signatures, class members, hook contracts, or the exact
  public type surface.
- For Rust, use the bundled rustdoc when you need the full crate browser instead of the summarized
  re-export map.
