---
title: Query And Watch
sidebar_position: 4
---

PrimaDB has both local subscriptions and remote live watches.

## Local Subscriptions

Local subscriptions are the basic reactive primitive. They emit an initial snapshot and then emit
updated snapshots when the relevant path changes.

The current storage/query work narrowed invalidation so unrelated writes do not trigger unrelated
subscriptions.

## Remote Watches

Relay and mesh transports support remote live interests for:

- `get`
- `map`
- `query`
- `lex`
- `snapshot`

Each watch starts with an initial snapshot and then streams updates. Large results are chunked.

Strict-scope `remoteTransaction(...)` / `remote_transaction(...)` calls are request/response pull
operations, not live watches.

## Performance Model

PrimaDB now tracks touched logical paths and coalesces bursts before recomputing watch results. That
improves the hot path without weakening correctness.

## Query Layer

Current query support includes:

- `eq`, `ne`
- `gt`, `gte`, `lt`, `lte`
- `prefix`, `contains`
- `exists`
- ordering
- limits
- lexical traversal

Nested scalar indexing and bounded direct-index scans are already in place for the current storage
engine.
