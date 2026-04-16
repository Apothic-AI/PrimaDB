---
title: Replication And Convergence
sidebar_position: 2
---

PrimaDB is eventually consistent across replicas. More precisely, it is an asynchronously
replicated, convergent, local-first datastore.

## Core Replication Contract

1. local writes become explicit operations
2. operations are versioned with hybrid logical revisions
3. transports exchange sync frames, pull responses, watch events, and acknowledgements
4. replicas merge those operations deterministically

That makes convergence inspectable instead of hidden behind opaque transport behavior.

## What This Is Not

PrimaDB is not:

- strongly consistent
- linearizable
- consensus-based

A local replica sees its own accepted writes immediately. Cross-replica convergence is eventual and
depends on peers eventually exchanging the relevant accepted operations.

## Why This Model Was Chosen

Compared to Gun’s HAM-style graph merge, PrimaDB’s explicit operation model is a better fit for:

- set membership semantics
- bytes and blob references
- signed values and certificates
- cross-language SDKs
- pull/watch transport features

## Local-First Behavior

A relay or mesh outage does not prevent local reads, writes, subscriptions, or durable storage from
working. Network layers reconnect and replay as connectivity returns.
