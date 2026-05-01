# Password-Derived Keys Plan

## Goals

- Add memory-hard password-derived secret-box keys to the existing crypto feature.
- Keep password-derived keys separate from fast content hashes such as BLAKE3.
- Expose the same derivation shape across Rust, browser WASM, Node, and Python.
- Make derived keys usable with the existing `SecretBoxKey` / `seaEncrypt` / `seaDecrypt` path.

## Decisions

- Use Argon2id version 1.3.
- Produce 32-byte keys suitable for XChaCha20-Poly1305.
- Generate 16-byte random salts when callers do not provide one.
- Return `algorithm`, `keyBase64`, `saltBase64`, and concrete `params`.
- Use bounded parameter validation so accidental huge settings do not exhaust memory.

## API Shape

- Rust core:
  - `PasswordKeyDerivationParams`
  - `PasswordKeyDerivationOptions`
  - `PasswordDerivedKey`
  - `SecretBoxKey::derive_from_password(...)`
  - `derive_password_key(...)`
- Browser / Node:
  - `derivePasswordKey(password, options?)`
- Python:
  - `derive_password_key(password, options=None)`

## Non-Goals

- No password login or account recovery system.
- No automatic encryption policy changes.
- No password hashing/verifier storage API yet; this derives encryption keys, not login verifiers.

## Verification

- Same password, salt, and params must derive the same key.
- Different password or salt must derive different keys.
- Returned key must decrypt payloads encrypted through the existing secret-box path.
- Browser, Node, and Python packages must build and expose the new API.
