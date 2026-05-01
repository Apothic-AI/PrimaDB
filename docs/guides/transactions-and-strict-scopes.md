---
title: Transactions And Strict Scopes
sidebar_position: 4
---

PrimaDB is eventual and local-first by default. Use transactions and strict scopes only for bounded
graph roots that need stronger write semantics.

## Local Transactions

Local transactions are atomic on one replica and work offline.

```ts
const report = db.transaction([
  {
    kind: "put",
    path: { anchor: "notes", segments: ["welcome"] },
    value: { title: "Welcome" },
  },
  {
    kind: "increment",
    path: { anchor: "metrics", segments: ["noteCount"] },
    by: 1,
  },
]);
```

The write batch commits together, indexes update together, and watches notify after the grouped
commit.

## Local Transactional Scopes

Use `local_transactional` when a root should always be modified through transaction boundaries but
does not need network coordination.

```ts
db.scope("drafts").configure({
  consistency: "local_transactional",
});
```

## Coordinated Scopes

Use `coordinated` when a single authority peer should accept canonical writes for a root.

```ts
db.scope("ledger").configure({
  consistency: "coordinated",
  authority: { kind: "full_node", peerId: "native:ledger" },
  offlineWrites: "reject",
});
```

Non-authority peers cannot commit canonical writes directly inside that scope. They either fail
immediately or store a provisional proposal, depending on `offlineWrites`.

## Remote Submission To Authority

Relay clients can submit a strict-scope transaction to the authority peer:

```ts
const sync = await db.connectRelay({ url: "ws://127.0.0.1:9010" });

const report = await sync.remoteTransaction("native:ledger", "ledger", [
  {
    kind: "increment",
    path: { anchor: "alice", segments: ["balance"] },
    by: 10,
  },
]);
```

Python uses `remote_transaction(...)`; Rust exposes the underlying transaction and scope APIs plus
the same transport semantics.

## Offline Behavior

With `offlineWrites: "reject"`:

- the write fails
- graph state does not change
- normal reads and watches do not show a fake result

With `offlineWrites: "queue_provisional"`:

- the proposal is stored durably
- canonical graph state does not change
- applications inspect proposals through `scope.proposals()`

Current coordinated scopes are single-authority. Quorum consensus and distributed multi-scope
transactions are not implemented yet.

See also:

- [Strict consistency](../concepts/strict-consistency)
- [Replication and convergence](../concepts/replication)
- [API reference](../api)
