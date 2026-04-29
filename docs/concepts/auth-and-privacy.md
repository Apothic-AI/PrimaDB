---
title: Auth And Privacy
sidebar_position: 6
---

PrimaDB has an SEA-like crypto/auth layer, but it does not try to turn the core graph into a full
read-ACL engine.

## What Exists Today

- identities and trusted users
- signed values
- delegated write certificates
- signed sync
- encrypted sync
- encrypted snapshots
- browser SEA-style primitives

## Write Enforcement

Authenticated write restrictions are already real. Owned paths and delegated certificates are
checked in the core database and sync policy layers.

## Read Privacy

Read privacy is primarily handled through encryption, not deep built-in read ACLs. That is the
current recommended model because:

- it keeps the core simpler
- it avoids ACL complexity across storage, indexing, watches, and replay
- it matches local-first replication better

## Optional Network Hooks

If an application wants operational gating, PrimaDB now exposes optional network-boundary hooks for:

- connection gating
- room gating
- pull/watch denial or rewrite
- served-result redaction

Those hooks are intentionally lighter than universal graph read authorization.

## Relation To Strict Scopes

Strict scopes control write ordering and canonical commit authority for selected graph roots. They
do not replace encryption or signed write certificates. Use encryption for read privacy, SEA-style
signatures/certificates for authorship, and [strict consistency](strict-consistency) when a scoped
part of the graph needs authority-gated ordering.
