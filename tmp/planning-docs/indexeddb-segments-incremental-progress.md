# IndexedDB Segments Incremental Persistence Progress

## 2026-05-02

- Confirmed the reported behavior in `src/wasm.rs`.
- `openDurableStorage` returns `incremental: true` for `IndexedDbSegments`.
- `enable_indexed_db_segment_persistence` currently snapshots the full database on every coalesced change.
- `save_segment_transaction_indexed_db` currently deletes every key under the namespace prefix and rewrites all current transaction entries.
- Native segment persistence already has an incremental primitive: `build_storage_transaction_from_ops(...)` over `unflushed_ops`.
- Current direction: reuse that primitive in the browser auto-persist path and reserve full namespace replacement for explicit full flush/save.
- Browser segment persistence now omits unused direct-index records and transport pending-op payloads from IndexedDB, keeping it focused on durable current graph state instead of sync retry queues.
