# Hash / KDF Modernization Plan

## Goals

- Use a faster cryptographic content hash for blob IDs and routing/watch dedupe.
- Stop using non-cryptographic stable hashes for transport dedupe.
- Stop using raw X25519 shared-secret bytes directly as XChaCha20-Poly1305 keys.
- Avoid adding password-derived crypto until Primadb has a first-class password-key feature.

## Decisions

- Blob references use BLAKE3 and include an explicit `blake3:` algorithm prefix.
- `stable_content_hash(...)` uses BLAKE3 over canonical JSON bytes and returns an explicit `blake3:` digest string.
- SEA-style shared-secret derivation uses X25519 followed by HKDF-SHA256.
- HKDF salt is bound to a Primadb context string plus the two X25519 public keys in canonical order.
- HKDF info is bound to the intended output use: Primadb SEA secret-box JSON payload encryption.

## Non-Goals

- No backwards compatibility shim for previous `sha256:` blob IDs.
- No migration path for persisted local development data.
- No password KDF implementation unless/until password-derived user keys are added.

## Verification

- Rust crypto tests should prove peers derive the same secret box key.
- Rust crypto tests should prove the derived secret-box key is not the raw X25519 shared secret.
- Blob tests should prove BLAKE3-prefixed content IDs are deterministic and content-sensitive.
- Sync tests should prove content hashes are BLAKE3-prefixed and deterministic.
- Package builds should regenerate lockfiles where package-local Cargo locks exist.
