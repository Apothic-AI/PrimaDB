---
title: Replication And Convergence
sidebar_position: 2
---

PrimaDB is eventually consistent across replicas by default. More precisely, the normal graph path
is an asynchronously replicated, convergent, local-first datastore.

## Core Replication Contract

1. local writes become explicit operations
2. operations are versioned with hybrid logical revisions
3. transports exchange sync frames, pull responses, watch events, and acknowledgements
4. replicas merge those operations deterministically

That makes convergence inspectable instead of hidden behind opaque transport behavior.

## What The Default Model Is Not

The default eventual path is not:

- strongly consistent
- linearizable
- consensus-based

A local replica sees its own accepted writes immediately. Cross-replica convergence is eventual and
depends on peers eventually exchanging the relevant accepted operations.

## Strict Scope Exception

PrimaDB also has opt-in strict scope APIs for bounded graph roots. Local transactions provide
atomic multi-step commits on one replica, and coordinated scopes can require a configured authority
before canonical writes are accepted for that scope.

That does not change the default replication contract. Writes outside coordinated scopes remain
local-first and eventual. Current coordinated scopes are single-authority, not quorum consensus.

See [Strict consistency](strict-consistency) for the exact scope and transaction semantics.

## Why This Model Was Chosen

Compared to Gun’s HAM-style graph merge, PrimaDB’s explicit operation model is a better fit for:

- set membership semantics
- bytes and blob references
- signed values and certificates
- cross-language SDKs
- pull/watch transport features

## Local-First Behavior

A relay or mesh outage does not prevent normal local reads, writes, subscriptions, or durable
storage from working. Network layers reconnect and replay as connectivity returns.

For coordinated strict scopes, non-authority offline writes either fail immediately or become
durable provisional proposals, depending on that scope's `offlineWrites` policy.
