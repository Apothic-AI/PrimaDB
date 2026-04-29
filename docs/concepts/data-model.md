---
title: Data Model
sidebar_position: 1
---

PrimaDB is a graph database with path traversal, links, set membership, and explicit binary/blob
support.

## Core Shapes

- scalar JSON values
- link references
- set membership
- first-class byte fields
- blob references stored in-graph
- scope policies attached to graph roots for opt-in strict transaction behavior

## Graph Markers

- `{"$link": "node-id"}` sets a field to a graph link
- `{"#": "node-id"}` is accepted as a Gun-compatible link marker
- `{"$set": [ ... ]}` sets a field to a membership set
- `{"$bytes": "..."}` carries bytes in marker form
- `{"$blob": {...}}` carries a blob reference

Materialized nodes include `"$id"`. Cycles are represented as `{"$ref": "node-id"}`.

## Why It Is Not Gun’s Internal Model

Gun primarily ships graph state fragments plus per-field state metadata. PrimaDB turns writes into
explicit operations and merges them deterministically against field/set version markers.

That tradeoff makes the system easier to reason about across:

- Rust
- browser WASM
- Node
- Python
- relay and mesh transports

## Hypergraph Terminology

It is reasonable to describe PrimaDB as capable of modeling hypergraph-like relationships, because
links plus set membership let one node relate to many others cleanly. The more exact label is
“local-first graph database with link and set relations.”
