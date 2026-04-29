---
title: Storage And Durability
sidebar_position: 7
---

PrimaDB’s current storage path is incremental and segment-backed rather than “always load the full
snapshot first.”

## Supported Durable Storage Paths

Native:

- segment-backed durable storage
- file-backed blobs
- explicit vacuum and blob GC

Browser:

- `localStorage`
- IndexedDB persistence helpers
- IndexedDB blob storage

## Why This Matters

The storage engine now supports:

- lazy node restore
- canonical node/index records
- nested scalar indexes
- bounded journal retention
- explicit vacuum/GC

That closes a meaningful gap relative to the older snapshot-centered design.

## What Is Deferred

PrimaDB does not currently implement Gun’s `Book`, and that is intentional. The storage direction is
closer to a PrimaDB-native segment/index engine than to a direct port of Gun’s experimental string-
packed page format.
