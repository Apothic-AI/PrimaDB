---
title: Strict Consistency
sidebar_position: 3
---

PrimaDB is eventual and local-first by default. Strict consistency is opt-in and scoped to a graph
root when an application needs stronger correctness for a bounded part of the graph.

## Default Model

Normal writes remain available offline. They are accepted locally, become explicit operations, and
converge with peers when relay or mesh transports exchange accepted operations.

This is the right mode for collaborative notes, cached profiles, chat history, replicated document
state, and other data where availability matters more than global serial ordering.

## Local Transactions

Local transactions provide atomic multi-step commits on one replica:

- all steps commit together or none commit
- indexes and relationship metadata update with the commit
- watches are notified once after the commit
- durable storage receives one grouped transaction
- the transaction works offline

Browser, Node, and Python use step arrays:

```ts
db.transaction([
  { kind: "assert_revision", path: { anchor: "accounts", segments: ["alice", "balance"] }, revision },
  { kind: "increment", path: { anchor: "accounts", segments: ["alice", "balance"] }, by: -10 },
  { kind: "increment", path: { anchor: "accounts", segments: ["bob", "balance"] }, by: 10 },
]);
```

Rust also exposes closure transactions:

```rust
db.transaction(|tx| {
    tx.root("accounts").field("alice").field("balance").increment(-10.0)?;
    tx.root("accounts").field("bob").field("balance").increment(10.0)?;
    Ok(())
})?;
```

## Scope Policies

A scope is a root/path boundary:

```ts
db.scope("accounts").configure({
  consistency: "coordinated",
  authority: { kind: "full_node", peerId: "native:ledger" },
  offlineWrites: "reject",
});
```

Supported consistency modes:

- `eventual`: the default behavior
- `local_transactional`: a local transaction boundary without network coordination
- `coordinated`: canonical writes require the configured authority

Strict transaction validation rejects accidental transactions that mix strict scoped paths with
unscoped eventual paths. It also rejects transactions that span multiple strict scopes.

## Coordinated Scopes

Coordinated scopes are for data that needs a single sequencer today. A full node or peer configured
as the authority can commit canonical writes for that scope. Non-authority peers cannot directly
commit canonical writes inside the scope.

Relay clients can submit a coordinated transaction to the authority:

```ts
await sync.remoteTransaction("native:ledger", "accounts", [
  { kind: "increment", path: { anchor: "alice", segments: ["balance"] }, by: -10 },
  { kind: "increment", path: { anchor: "bob", segments: ["balance"] }, by: 10 },
]);
```

The authority applies the transaction through the normal scope machinery and returns a transaction
report. Accepted operations then replicate normally.

## Offline Behavior

Coordinated scopes trade write availability for correctness.

With `offlineWrites: "reject"`, a non-authority peer fails immediately:

- no canonical graph state changes
- no local operation is committed
- normal reads and watches do not emit a fake update

With `offlineWrites: "queue_provisional"`, a non-authority peer stores a durable proposal:

- the proposal survives durable storage restore
- the proposal is not canonical graph state
- normal reads and watches exclude the proposal
- remote snapshots do not distribute another peer's provisional UI state
- applications can inspect pending proposals with `scope.proposals()`

This lets a UI show pending work without weakening strict scope semantics.

## Current Limits

The current coordinated implementation is a single-authority path. The policy model already has
types for quorum and authority read modes, but these are not full consensus features yet.

Not implemented yet:

- quorum consensus
- authority sequence certificates
- distributed multi-scope transactions
- strict read routing through `scope.get(...)`
- treating provisional writes as an overlay in normal reads

Use explicit remote reads such as `remoteGet(...)` / `remote_get(...)` when an application needs to
ask an authority peer for current data.
