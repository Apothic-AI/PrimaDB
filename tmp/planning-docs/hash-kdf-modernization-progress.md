# Hash / KDF Modernization Progress

## Completed

- Replace SHA-256 blob IDs with BLAKE3.
- Replace 64-bit FNV-style stable content hash with BLAKE3.
- Derive SEA secret-box keys with HKDF-SHA256 after X25519.
- Update docs and lockfiles after implementation.
- Fix all-feature test/example `RelayClientConfig` literals to include `session_auth`.

## Verification

- `cargo test --lib --features crypto`
- `cargo test --lib`
- `cargo check --all-features`
- `cargo check --target wasm32-unknown-unknown --features crypto`
- `cargo test --all-features`
- `cargo check --examples --features native-websocket`
- `pnpm --dir packages/primadb run build`
- `pnpm --dir packages/primadb run smoke`
- `pnpm --dir packages/primadb-node run build`
- `pnpm --dir packages/primadb-node run smoke:core`
- `uv run --directory packages/primadb-python maturin develop --quiet`
- `uv run --directory packages/primadb-python python scripts/pack_check.py`
- `uv run --directory packages/primadb-python python scripts/smoke_core.py`
- `pnpm --dir website build`

## Notes

- Python editable install emitted a non-fatal `patchelf` warning, but the ABI3 wheel built and installed successfully.
- Node and Python smoke tests both returned `blake3:` blob references for the binary blob round trip.
