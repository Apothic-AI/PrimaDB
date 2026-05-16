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

Keyed records also have local scan watches through `watch_records(...)` / `watchRecords(...)`. A
record watch emits the current `RecordScanResult` first, then emits updated scan results only when
the touched logical record keys overlap the scan prefix/range.

## Remote Watches

Relay and mesh transports support remote live interests for:

- `get`
- `map`
- `query`
- `lex`
- `records`
- `text_search`
- `node`
- `snapshot`

Transport handles expose peer-agnostic helpers for the common connected/meshed case. Use
`watchQuery(...)`, `watchRecords(...)`, etc. to let the transport select a connected peer. Pass an
optional `RemoteInterestPolicy` only when the caller needs to constrain selection, for example
`{ target: "peer", peerId: "native:ledger" }` or `{ target: "peers", peers: ["a", "b"] }`.
The explicit `watchRemoteQuery(peerId, ...)` / `watchRemoteRecords(peerId, ...)` shape remains
available for tests, debugging, and authority-targeted reads.

Each watch starts with an initial snapshot and then streams updates. Large results are chunked.

Record watches intentionally use the same `PullRequestKind::Records { scan }` request shape for
local serving, relay pulls, and relay/mesh watches. That keeps the model close to Gun's single
interest pipeline: transport adapters move the same interest/result messages rather than defining
separate local and remote query semantics.

Text search watches follow the same path through
`PullRequestKind::TextSearch { source, query, spec }`. Collection search uses collection-scoped
BM25 statistics; graph-query and record-scan sources rank the materialized candidate set and expose
candidate/truncation metadata so paginated sources are not mistaken for global top-k search.
Use `textSearchFanIn(...)` / `text_search_fan_in(...)` and
`watchTextSearchFanIn(...)` / `watch_text_search_fan_in(...)` when a caller needs source-tagged
text results from every policy-matching peer. Fan-in merged text results report `scoreScope =
"peer_local"` because BM25 scores from different peers are diagnostic, not a single global corpus.

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
