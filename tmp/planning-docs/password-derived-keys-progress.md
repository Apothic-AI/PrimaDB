# Password-Derived Keys Progress

## Completed

- Added Argon2id password-derived key support in the core `crypto` feature.
- Added bounded password KDF params, random salt generation, and structured derived-key output.
- Exposed `derivePasswordKey(...)` in browser and Node packages.
- Exposed `derive_password_key(...)` in the Python package.
- Exposed native package encryption key setters so derived keys can be used for snapshot and transport encryption.
- Updated the browser Gun compatibility runtime so `SEA.work(...)` uses Argon2id instead of PBKDF2/SHA-256.
- Removed silent SHA-256 normalization of arbitrary strings in browser `SEA.encrypt(...)` / `SEA.decrypt(...)`; callers must pass a 32-byte base64url key or a derived-key result.
- Updated package READMEs, concept docs, and generated API docs.

## Verification

- `cargo test --lib --features crypto`
- `cargo check --target wasm32-unknown-unknown --features crypto`
- `cargo test --all-features`
- `cargo test --lib`
- `cargo check --all-features`
- `cargo check --manifest-path packages/primadb-node/Cargo.toml`
- `cargo check --manifest-path packages/primadb-python/Cargo.toml`
- `pnpm --dir packages/primadb run build`
- `pnpm --dir packages/primadb run typecheck`
- `pnpm --dir packages/primadb run smoke`
- `pnpm --dir packages/primadb-node run build`
- `pnpm --dir packages/primadb-node exec tsc --noEmit --allowJs false index.d.ts`
- `pnpm --dir packages/primadb-node run smoke:core`
- `pnpm --dir packages/primadb-node run pack:check`
- `uv run python -m py_compile packages/primadb-python/python/primadb/__init__.pyi`
- `uv run --directory packages/primadb-python maturin develop --quiet`
- `uv run --directory packages/primadb-python python scripts/smoke_core.py`
- `uv run --directory packages/primadb-python python scripts/pack_check.py`
- `pnpm --dir website build`

## Notes

- Python editable install emitted the existing non-fatal `patchelf` warning.
- Node and Python core smokes both derive Argon2id keys with fixed test parameters and use them through the new encryption-key setters.
