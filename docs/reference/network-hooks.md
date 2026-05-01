---
title: Network Hooks
sidebar_position: 2
---

PrimaDB exposes optional network-boundary hooks instead of deep built-in graph read ACLs.

## Hook Surface

- `onConnect`
- `onJoinRoom`
- `onPull`
- `onWatch`
- `onServeResult`

Rust uses trait methods on `NetworkHooks`. Browser, Node, and Python expose matching callback
registration on the SDK surface.

Every hook context can include a verified session identity:

- TypeScript/Node/browser: `context.verifiedIdentity`
- Python: `context["verifiedIdentity"]`
- Rust: `context.verified_identity`

This field is `null` / `None` until relay or mesh session challenge/response has verified the peer
signature. A public key advertised in presence is visible as `peer.identity`, but it is not trusted
until it becomes `verifiedIdentity`.

## What They Are For

- connection gating
- room gating
- request denial
- request rewrite
- served-result redaction or reshape
- authenticated peer allowlists based on verified public keys or aliases

## What They Are Not

They are not:

- a full graph ACL system
- encrypted-query infrastructure
- a replacement for value encryption

## Decision Semantics

Across browser, Node, and Python, the current semantics are aligned:

- `null` / `undefined` / `None` => allow unchanged
- `true` => allow unchanged
- `false` => deny
- string => deny with message
- wrapper object/dict => allow or deny with optional rewritten request/result

## Strict Session Mode

Relay and mesh configs accept a `sessionAuth` object. Set
`requireAuthenticatedPeers: true` to avoid serving pull/watch/sync traffic to peers that have not
completed the nonce challenge/response handshake. Optional `trustedPublicKeys` and
`trustedAliases` narrow verified peers to a local allowlist.

If a peer has no local authenticated user or the crypto feature is unavailable, the handshake cannot
complete and strict session mode will deny serving remote data to that peer.

## Why The Boundary Is Important

This keeps the core graph/storage/index/watch model simpler while still giving applications a place
to enforce operational policy.
