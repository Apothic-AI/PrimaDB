---
title: Auth, Encryption, And Password Keys
sidebar_position: 1
---

Use this guide when an application needs signed authorship, encrypted data, authenticated peers, or
password-derived encryption keys.

## Choose The Right Primitive

- Use signed values when readers should verify who wrote a field.
- Use delegated write certificates when one owner should authorize another writer for a path.
- Use encryption when data should remain private even if another peer or relay stores or forwards it.
- Use session identity when hooks need to know which peer is connected.
- Use strict scopes when a bounded graph root needs authority-gated write ordering.

These primitives compose. They are not replacements for each other.

## Algorithms

The `crypto` feature uses:

- Ed25519 for signatures and session challenge/response transcripts
- X25519 plus HKDF-SHA256 for SEA-style peer shared secrets
- Argon2id v1.3 for password-derived secret-box keys
- XChaCha20-Poly1305 for authenticated encryption
- BLAKE3 for content IDs and route/watch content hashes

Password-derived keys are encryption keys, not login verifier records. Store the returned
`saltBase64` and `params` next to the encrypted data if the same password must rederive the key
later.

## Browser Password Key

```ts
import { Primadb, derivePasswordKey, initPrimadb } from "primadb";

await initPrimadb();

const derived = derivePasswordKey("correct horse battery staple", {
  memoryCostKiB: 64 * 1024,
  timeCost: 3,
  parallelism: 1,
});

const db = new Primadb("browser-a");
db.setSnapshotEncryptionKey(derived.keyBase64);
db.setTransportEncryptionKey(derived.keyBase64);
```

## Node Password Key

```ts
import { Primadb, derivePasswordKey } from "primadb-node";

const db = new Primadb("node-a");
const derived = derivePasswordKey("correct horse battery staple", {
  memoryCostKiB: 64 * 1024,
  timeCost: 3,
  parallelism: 1,
});

db.setSnapshotEncryptionKey(derived.keyBase64);
db.setTransportEncryptionKey(derived.keyBase64);
```

## Python Password Key

```python
from primadb import Primadb, derive_password_key

db = Primadb("py-a")
derived = derive_password_key(
    "correct horse battery staple",
    {"memoryCostKiB": 64 * 1024, "timeCost": 3, "parallelism": 1},
)

db.set_snapshot_encryption_key(derived["keyBase64"])
db.set_transport_encryption_key(derived["keyBase64"])
```

## Authenticated Session Identity

Relay and mesh configs accept `sessionAuth`. A peer with an authenticated local user advertises a
public key in presence, then proves ownership with a nonce challenge/response. Hooks receive
`verifiedIdentity` only after that proof succeeds.

```ts
const identity = generateIdentity();
db.authenticateLocalUser("alice", identity.secretKey, [{ root: "*", read: true, write: true }]);

const sync = await db.connectRelay({
  url: "ws://127.0.0.1:9010",
  sessionAuth: {
    requireAuthenticatedPeers: true,
    trustedAliases: ["relay", "alice"],
  },
});
```

Strict session mode controls what this peer serves to unauthenticated connections. It does not
automatically make graph values private. Use encryption for read privacy.

## Read Privacy And Hooks

PrimaDB intentionally does not implement deep built-in read ACLs across the entire graph. The
recommended model is:

- encrypt sensitive values or snapshots
- use signed values/certificates for authorship and write delegation
- use hooks for operational gating at the network boundary
- use `verifiedIdentity` in hooks when peer identity matters

See also:

- [Auth and privacy](../concepts/auth-and-privacy)
- [Network hooks](../reference/network-hooks)
- [API reference](../api)
