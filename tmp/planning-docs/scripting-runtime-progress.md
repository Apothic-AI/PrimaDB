# Scripting Runtime Progress

## Completed

- Created `feature/scripting-runtime`.
- Added initial plan for explicit, capability-scoped node scripting.
- Added the Rust `scripting` feature using a Rhai-backed runtime abstraction.
- Added node script attachment, listing, removal, and explicit execution APIs.
- Added local capability grants for read, query, traverse, write, and transaction operations.
- Added a script DB facade with read/query/traverse and transaction-step-producing write helpers.
- Added chunked internal graph storage for script manifests to avoid oversized segment-index filenames.
- Exposed scripting methods through browser WASM, Node, and Python package bindings.
- Added package smoke coverage for Node and Python scripting.
- Added concept and guide docs for scripting.

## Verification

- `cargo test --lib --features scripting`
- `cargo test --lib`
- `cargo check --target wasm32-unknown-unknown --features scripting`
- `cargo check --target wasm32-unknown-unknown --features crypto,scripting`
- `cargo check --all-features`
- `cargo check --manifest-path packages/primadb-node/Cargo.toml`
- `cargo check --manifest-path packages/primadb-python/Cargo.toml`
- `pnpm --dir packages/primadb-node exec tsc --noEmit --allowJs false index.d.ts`
- `uv run python -m py_compile packages/primadb-python/python/primadb/__init__.pyi`
- `pnpm --dir packages/primadb run build`
- `pnpm --dir packages/primadb run smoke`
- `pnpm --dir packages/primadb-node run smoke:core`
- `uv run --directory packages/primadb-python maturin develop --quiet`
- `uv run --directory packages/primadb-python python scripts/smoke_core.py`
- `cargo test --all-features`
- `pnpm --dir packages/primadb run pack:check`
- `pnpm --dir packages/primadb-node run pack:check`
- `uv run --directory packages/primadb-python python scripts/pack_check.py`
- `pnpm --dir website build`

## Notes

- The first durable-storage smoke found that a single large script-manifest scalar could create a
  filename-too-long error in segment index storage. Script manifests now store as short base64url
  chunks under the internal script attachment root.
- Browser, Node, and Python smoke tests execute scripts end to end. Node and Python additionally
  exercise durable segment storage during scripting.
