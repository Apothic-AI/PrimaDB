# OPFS Backend Progress

## 2026-05-02

- Created `feature/opfs-backend` from `staging`.
- Verified `web-sys` exposes OPFS directory/file handles and sync access handles.
- Chose direct `web-sys` implementation over the `opfs` crate to preserve performance headroom.
- Added the first Rust/WASM OPFS segment backend surface and verified `cargo check --target wasm32-unknown-unknown --features crypto,scripting` compiles.
- Added a Vite OPFS segment regression example and smoke test.
- Fixed segment auto-persist hooks to ignore pending-only subscription events, preventing a race against the explicit initial full flush.
- Verified OPFS and IndexedDB segment smoke tests report zero failed writes and bounded incremental writes.
- Reworked OPFS full flush to write current files before pruning stale files, avoiding delete-before-write data loss on full replacement failures.
- Extended the OPFS smoke to restore the same OPFS namespace into a fresh `Primadb` instance and verify the latest checkpoint value.
- Verified:
  - `cargo test --lib`
  - `cargo check --target wasm32-unknown-unknown --features crypto,scripting`
  - `pnpm --dir packages/primadb run build`
  - `pnpm --dir packages/primadb run typecheck`
  - `pnpm --dir packages/primadb/examples run build`
  - `pnpm --dir packages/primadb/examples run smoke:opfs`
  - `pnpm --dir packages/primadb/examples run smoke:indexeddb`
  - `pnpm --dir packages/primadb/examples run smoke:default`
  - `pnpm --dir website run build`
