# Strict Consistency And ACID Scopes Plan

PrimaDB is local-first and eventually consistent by default. That should remain the default model.
Strict consistency should be an explicit opt-in for graph regions where developers need stronger
correctness than CRDT-style merge can provide.

The key design principle is:

- eventual consistency by default
- local ACID where possible
- optimistic conditional writes for common invariants
- coordinated ACID scopes only where the application explicitly trades availability for correctness

## Current State

PrimaDB currently exposes root/path graph APIs:

- Rust: `db.root(...)`
- browser, Node, Python: `db.chain(...)`

There is no `db.scope(...)` API yet.

The durable segment store already has useful foundations for local atomicity:

- writes are represented as operations
- storage persists transactions
- indexes are maintained from operation application
- watches are notified after changes

However, the mesh model is intentionally eventually consistent. A peer that can write while offline
cannot simultaneously provide global serializability for those writes.

## Product Direction

Strict consistency should be a scope/path policy, not a value property.

Avoid this model:

```ts
db.chain("accounts").field("alice").field("balance").put(10, {
  acid: true,
});
```

That is misleading because most real invariants span multiple values:

- transfer debits and credits
- uniqueness constraints
- inventory claims
- membership limits
- edge/index consistency
- auth and ownership metadata

Prefer this model:

```ts
db.scope("accounts").configure({
  consistency: "coordinated",
  authority: "full-node:ledger",
});

await db.scope("accounts").transaction(async (tx) => {
  const alice = await tx.get("alice/balance");
  const bob = await tx.get("bob/balance");
  tx.assert(alice >= 10);
  tx.put("alice/balance", alice - 10);
  tx.put("bob/balance", bob + 10);
});
```

In this model, ACID guarantees apply to a transaction executed within a configured scope.

## Consistency Tiers

### Tier 1: Local Transactions

Local transactions should be the first milestone.

Properties:

- atomic multi-operation commit on one replica
- rollback before commit if a transaction builder fails
- one durable storage transaction
- one watch notification batch after commit
- relationship indexes and scalar indexes update atomically with the commit
- works offline

This does not provide global serializability. It provides local atomicity, consistency, isolation, and
durability.

API sketch:

```ts
await db.transaction((tx) => {
  tx.chain("docs").field("a").put({ title: "A" });
  tx.chain("docs").field("b").put({ title: "B" });
});
```

Rust sketch:

```rust
db.transaction(|tx| {
    tx.root("docs").field("a").put(json!({"title": "A"}))?;
    tx.root("docs").field("b").put(json!({"title": "B"}))?;
    Ok(())
})?;
```

### Tier 2: Optimistic Conditional Writes

Conditional writes should handle common correctness needs without coordination.

Properties:

- compare-and-set by node/field revision
- assert field exists/does not exist
- assert path hash/content hash
- assert unique index claim is unowned
- fail fast with a retryable conflict error
- still compatible with offline-first workflows when the invariant is local

API sketch:

```ts
await db.transaction((tx) => {
  const profile = tx.chain("users").field("alice").once();
  tx.assertRevision("users/alice", profile.$revision);
  tx.chain("users").field("alice").field("displayName").put("Alice");
});
```

This tier is useful for:

- optimistic editing
- unique usernames
- idempotent claims
- leases
- application-level retries

### Tier 3: Coordinated Strict Scopes

Coordinated scopes provide the strongest semantics, but they trade away offline writes for that
scope.

Properties:

- a scope is a root/path prefix such as `accounts/**`
- the scope declares a consistency policy
- accepted writes must be sequenced by an authority
- disconnected peers may read cached data but cannot accept strict writes locally
- clients may queue provisional writes, but those writes are not committed until accepted by the
  authority
- writes outside the strict scope remain local-first/eventual

API sketch:

```ts
await db.scope("accounts").configure({
  consistency: "coordinated",
  authority: {
    kind: "full-node",
    peerId: "native:ledger",
  },
  isolation: "serializable",
  offlineWrites: "queue_provisional",
});

await db.scope("accounts").transaction(async (tx) => {
  tx.assert("alice/balance", "gte", 10);
  tx.increment("alice/balance", -10);
  tx.increment("bob/balance", 10);
});
```

## Scope Policy Model

A scope policy should be stored as graph metadata, but enforced by the runtime and transport layer.

Possible policy shape:

```ts
type ConsistencyPolicy =
  | { consistency: "eventual" }
  | { consistency: "local_transactional" }
  | {
      consistency: "coordinated";
      authority: { kind: "peer"; peerId: string } | { kind: "quorum"; peers: string[]; threshold: number };
      isolation: "serializable";
      readMode?: "cached" | "authority" | "quorum";
      offlineWrites?: "reject" | "queue_provisional";
    };
```

Initial implementation should support a single authoritative full node before quorum.

## Offline Write Policy

Coordinated scopes need explicit offline behavior. The default should be safe:

```ts
offlineWrites: "reject"
```

With `reject`, an offline write to a coordinated scope fails immediately:

- no canonical graph state is changed
- no local operation is committed
- normal watches do not emit a fake committed update
- the caller receives a clear strict-scope availability error

The optional local-first UX mode is:

```ts
offlineWrites: "queue_provisional"
```

With `queue_provisional`, the write is stored locally as a transaction proposal, not as graph truth.

Required semantics:

- provisional writes are durable local proposals
- provisional writes are not canonical graph state
- normal reads exclude provisional writes by default
- normal watches exclude provisional writes by default
- UI-oriented reads/watches may opt into a provisional overlay
- proposals are submitted to the authority when reachable
- accepted proposals become sequenced authoritative operations and then update the graph normally
- rejected proposals become explicit rejection/conflict events
- rejected proposals must not leave partial canonical graph state behind

API sketch:

```ts
await db.scope("accounts").transaction(
  async (tx) => {
    tx.increment("alice/balance", -10);
    tx.increment("bob/balance", 10);
  },
  {
    offline: "queue_provisional",
  },
);

const pending = db.scope("accounts").proposals();
```

Overlay read sketch:

```ts
const view = db.scope("accounts").get("alice/balance", {
  includeProvisional: true,
});
```

This preserves local-first UX without weakening strict consistency. The application can show pending
state, but PrimaDB does not treat that state as committed until the authority accepts it.

## Path And Transaction Rules

Strict scopes need clear rules:

- A transaction entirely inside one strict scope uses that scope's coordinator.
- A transaction entirely outside strict scopes uses normal local/eventual semantics.
- A transaction spanning strict and eventual paths should fail by default.
- A transaction spanning multiple strict scopes should fail until multi-scope coordination exists.
- Links from eventual paths into strict paths are allowed, but writes to the strict target still require
  authority.
- Links from strict paths into eventual paths are allowed only if the transaction does not depend on
  eventual data for strict invariants.

This avoids accidental distributed transactions.

## Read Semantics

Strict writes are only half the problem. Reads also need explicit semantics.

Read modes:

- `cached`: return local data, may be stale
- `authority`: ask the authority/full node
- `quorum`: future mode for quorum-backed scopes

Default reads can remain cached to preserve local-first behavior. Developers who need strict reads
should request them explicitly:

```ts
const balance = await db.scope("accounts").get("alice/balance", {
  consistency: "authority",
});
```

## Transport And Mesh Behavior

Coordinated scopes require protocol support:

- advertise scope policies in peer presence or a policy discovery request
- route strict transaction proposals to the authority
- return accepted sequenced operations or a conflict/rejection
- replicate accepted operations normally after sequencing
- reject or quarantine unsequenced writes that target strict scopes

Relay/full-node behavior:

- a full node can act as an authority for one or more scopes
- relays that are not authorities should route proposals, not accept them
- authorities should persist an ordered scope log
- accepted operations should include authority sequence metadata

Mesh behavior:

- peers can discover the authority through normal peer discovery
- direct peer connections may route proposals directly when available
- relay fallback remains valid
- offline peers can keep reading cached strict-scope data but cannot commit strict writes

## Storage And Operation Model

Strict scopes probably need operation metadata:

```rust
struct Operation {
    // existing fields
    scope_sequence: Option<ScopeSequence>,
    transaction_id: Option<String>,
}

struct ScopeSequence {
    scope: String,
    authority: String,
    sequence: u64,
}
```

Local transactions need grouped operation application:

- stage operations in memory
- validate limits and preconditions
- apply to a cloned or transactional inner state
- persist as one storage transaction
- notify watches once

Coordinated transactions need:

- transaction proposal type
- precondition set
- read set if serializable validation requires it
- write set
- authority signature or certificate over accepted sequence

## Hooks, Auth, And SEA

Strict consistency should compose with existing hooks and SEA-like cryptographic behavior.

Requirements:

- transaction proposals pass through connection/room/pull/serve hooks where relevant
- authorities can require signed proposals
- accepted scope sequences should be signed by the authority
- encrypted payloads remain private; coordination does not imply plaintext access unless the
  application gives the authority that access
- hooks can reject strict-scope proposals before sequencing

Important distinction:

- encryption controls who can read data
- signatures/certificates control who can author data
- strict scopes control ordering and invariant enforcement

These are related but not interchangeable.

## Performance Requirements

Strict scopes should not slow down the default eventual path.

Design constraints:

- no global locks for normal writes
- no scope-policy lookup on every eventual write unless the path could match a strict prefix
- cache scope policy resolution by root/path prefix
- batch transaction watch notifications
- keep local transactions allocation-conscious
- avoid recursive graph materialization during validation
- push conflict detection into revision/path metadata where possible

The common case must remain fast:

- normal `.put` / `.set` outside strict scopes should stay on the existing path
- traversal and watch invalidation should not degrade because strict scopes exist
- strict coordination overhead should only be paid inside strict scopes

## Initial Implementation Plan

1. Add `Scope` API aliases over root/path prefixes.
2. Add local transaction builder and atomic local commit.
3. Batch watch/index/storage updates for local transactions.
4. Add precondition and CAS primitives.
5. Add unique claim/index helper built on preconditions.
6. Add scope policy registry and path-prefix lookup.
7. Add coordinated scope proposal/accept protocol for a single authority full node.
8. Add authority sequence metadata and validation.
9. Add strict read mode against authority.
10. Expose all APIs consistently across Rust, browser, Node, and Python.

## Verification Plan

- local transaction commits all writes or none
- local transaction emits one watch batch
- indexes and traversal relationships are correct after commit
- local transaction survives durable storage restore
- CAS write succeeds with matching revision and fails with stale revision
- uniqueness claim rejects concurrent duplicate claims
- strict scope write fails offline when `offlineWrites` is `reject`
- strict scope write queues but does not commit when `offlineWrites` is `queue_provisional`
- authority accepts and sequences valid transaction proposals
- authority rejects stale/conflicting proposals
- unauthorized peer cannot forge accepted strict-scope operations
- eventual paths are not slowed down or behaviorally changed by strict-scope support

## Non-Goals For The First Pass

- distributed multi-scope transactions
- quorum consensus
- global serializability across the whole graph
- read authorization beyond existing encryption/hook model
- replacing the default eventual consistency model

## Recommended First Milestone

Implement local transactions and preconditions first.

That gives developers immediate value, improves internal correctness, and creates the staging/commit
machinery needed for strict scopes without prematurely adding distributed coordination complexity.
