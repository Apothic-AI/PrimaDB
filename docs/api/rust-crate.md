---
title: Rust Crate API
sidebar_position: 8
---

This page covers the public Rust crate surface. The site also serves the full bundled rustdoc so Rust consumers can browse the real crate API directly.

> The full crate rustdoc is bundled into this site from `src/lib.rs`.

## Full crate reference

- <a href="/rust-api/primadb/" target="_blank" rel="noopener noreferrer">Open bundled rustdoc</a>
- <a href="/rust-api/primadb/index.html" target="_blank" rel="noopener noreferrer">Open crate root page</a>

## Re-export map

### `auth`

- `AuthClaims`
- `AuthenticatedSyncFrame`
- `DataCertificate`
- `EncryptedSyncFrame`
- `inspect_signed_field_value`
- `InspectedSignedFieldValue`
- `LocalUser`
- `owner_public_key_for_path`
- `SecureSyncFrame`
- `SecurityState`
- `SignedValueClaims`
- `StoredSnapshot`
- `UserGrant`
- `UserRecord`

### `binary`

- `BinaryBytes`

### `blob`

- `blob_ref_for_data`
- `BlobRef`
- `BlobStorageBinding`
- `BlobStorageConfig`
- `BlobStore`
- `FileBlobStore`
- `FileBlobStoreOptions`
- `MemoryBlobStore`
- `StoredBlob`

### `clock`

- `HybridClock`
- `Revision`
- `VersionMarker`

### `compat`

- `Gun`
- `GunChain`
- `GunCompatOptions`

### `consistency`

- `ProvisionalTransaction`
- `ScopeAuthority`
- `ScopeConsistency`
- `ScopeIsolation`
- `ScopeOfflineWrites`
- `ScopePolicy`
- `ScopeReadMode`
- `TransactionOptions`
- `TransactionReport`
- `TransactionStatus`
- `TransactionStep`

### `crypto`

- `derive_password_key`
- `EncryptedPayload`
- `Identity`
- `PasswordDerivedKey`
- `PasswordKeyDerivationOptions`
- `PasswordKeyDerivationParams`
- `PublicIdentity`
- `SeaPair`
- `SecretBoxKey`
- `SignedPayload`

### `db`

- `Chain`
- `ChangeEvent`
- `ChangeSubscription`
- `LexBuilder`
- `MapEntry`
- `NodeFetchScheduler`
- `Primadb`
- `QueryBuilder`
- `RecordWatchSubscription`
- `Scope`
- `Subscription`
- `Transaction`
- `TransactionChain`
- `TraversalSubscription`
- `VacuumReport`

### `durable`

- `DurableStorageBinding`
- `DurableStorageConfig`
- `SegmentDurability`
- `SegmentFileStoreOptions`
- `SegmentLockMode`

### `engine`

- `AuthNodeMeta`
- `build_storage_metadata`
- `build_storage_transaction`
- `build_storage_transaction_from_ops`
- `direct_index_encode_prefix`
- `direct_index_key`
- `DirectIndexScan`
- `DirectScalarIndexEntry`
- `encode_component`
- `IncrementalStore`
- `is_record_node_id`
- `node_matches_root`
- `NodeIndexManifest`
- `operation_matches_root`
- `record_entry_from_node_state`
- `record_key_from_node_state`
- `record_node_id`
- `SegmentFileStore`
- `sortable_scalar_key`
- `StorageMetadata`
- `StorageRecoveryReport`
- `StorageSyncReport`
- `StorageTransaction`
- `StorageVacuumReport`
- `StoredAuthFieldMeta`
- `touched_nodes`
- `touched_storage_nodes`

### `error`

- `PrimadbError`
- `Result`

### `hardening`

- `PrimadbLimits`
- `PrimadbStats`

### `hooks`

- `ConnectHookContext`
- `HookDecision`
- `HookTransport`
- `NetworkHooks`
- `parse_request_hook_json`
- `parse_result_hook_json`
- `parse_void_hook_json`
- `RoomHookContext`
- `ServeRequestContext`
- `ServeResultContext`

### `native_mesh`

- `NativeWebRtcMesh`

### `native_relay_server`

- `NativeRelayServer`

### `native_sync`

- `NativeWebSocketSync`

### `net`

- `IceServerConfig`
- `IceServerUrls`
- `MeshConfig`
- `MeshSignal`
- `MeshSignalingMode`
- `RelayClientConfig`
- `RelayServerConfig`

### `operation`

- `Operation`
- `OperationAction`
- `OperationValue`

### `parallel`

- `parallel_enabled`
- `parallel_thread_count`

### `query`

- `LexEntry`
- `LexSpec`
- `QueryDirection`
- `QueryFilter`
- `QueryOrder`
- `QuerySpec`

### `record`

- `RecordBatch`
- `RecordBatchReport`
- `RecordEntry`
- `RecordMutation`
- `RecordPrecondition`
- `RecordScan`
- `RecordScanResult`
- `RecordValue`

### `router`

- `PeerPresence`
- `PeerRecommendation`
- `RouteBatchItem`
- `RouteDecision`
- `RouteEnvelope`
- `RoutePayload`
- `Router`
- `RouterConfig`
- `RouterStats`
- `RouteTarget`

### `scripting`

- `NodeScript`
- `ScriptCapabilities`
- `ScriptExecutionContext`
- `ScriptExecutionOptions`
- `ScriptExecutionResult`
- `ScriptLimits`
- `ScriptPathGrant`
- `ScriptRuntime`

### `session_auth`

- `AuthChallenge`
- `AuthResponse`
- `AuthTranscript`
- `IdentityTrust`
- `PresenceIdentity`
- `SessionAuthConfig`
- `VerifiedIdentity`

### `snapshot`

- `DatabaseSnapshot`

### `storage`

- `MemoryStorageAdapter`
- `SnapshotFileAdapter`
- `SnapshotLogFileAdapter`
- `StorageAdapter`
- `StorageReport`

### `sync`

- `error_pull_response`
- `error_watch_event`
- `PullChunk`
- `PullRequest`
- `PullRequestKind`
- `PullResponse`
- `PullResponseBody`
- `RemoteInterestPolicy`
- `RemoteInterestTarget`
- `RemotePath`
- `RemoteResult`
- `RemoteWatchMessage`
- `RemoteWatchSubscription`
- `stable_content_hash`
- `SyncEnvelope`
- `SyncFrame`
- `WatchEvent`
- `WatchRequest`
- `WatchRequestKind`

### `traversal`

- `TraversalDirection`
- `TraversalEdge`
- `TraversalEdgeKind`
- `TraversalEntry`
- `TraversalResult`
- `TraversalSpec`
- `TraversalStrategy`

### `value`

- `FieldState`
- `FieldValue`
- `NodeId`
- `NodeState`
- `SetState`

## Strict consistency and transactions

PrimaDB is eventual/local-first by default. Strict consistency APIs are opt-in and scoped to a graph root.

- `Primadb::transaction(...)` runs a closure transaction atomically on the local replica.
- `Primadb::apply_transaction_steps(...)` applies serializable step payloads used by SDKs and transports.
- `Primadb::scope(root).configure(...)` stores a `ScopePolicy` for that root.
- `Scope::transaction(...)` runs a Rust closure transaction inside the scope.
- `Scope::transaction_steps(...)` runs step payloads inside the scope and can queue provisional proposals when configured.
- `ScopeConsistency::LocalTransactional` marks a transaction boundary without network coordination.
- `ScopeConsistency::Coordinated` requires the configured authority for canonical writes.

The current coordinated implementation is a single-authority path. Quorum policy types exist, but quorum consensus, authority sequence certificates, and distributed multi-scope transactions are not implemented yet.

## Traversal semantics

`Chain::traverse` is local-first and bounded. With active relay or mesh transports, missing linked nodes are scheduled for bounded background fetch.

`Chain::watch_traverse` receives updated results when fetched nodes merge into the local graph.

The `fetched` field on `TraversalResult` is the number of background node fetches scheduled by that evaluation.
