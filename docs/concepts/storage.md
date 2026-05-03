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
- OPFS segment persistence
- IndexedDB blob storage

## Why This Matters

The storage engine now supports:

- lazy node restore
- canonical node/index records
- nested scalar indexes
- bounded journal retention
- explicit vacuum/GC
- BLAKE3-prefixed content-addressed blob references
- bounded incremental browser segment writes for IndexedDB and OPFS

That closes a meaningful gap relative to the older snapshot-centered design.

## Browser Backend Choice

Use OPFS segments for large or high-churn browser-local data when available. OPFS stores segment
records as browser-private files and avoids IndexedDB's structured-clone overhead for large opaque
values. IndexedDB segments remain the compatibility path for browsers without OPFS.

## What Is Deferred

PrimaDB does not currently implement Gun’s `Book`, and that is intentional. The storage direction is
closer to a PrimaDB-native segment/index engine than to a direct port of Gun’s experimental string-
packed page format.
