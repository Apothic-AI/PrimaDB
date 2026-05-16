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

### `app_route`

- `ApplicationRouteAuthStatus`
- `ApplicationRouteContext`
- `ApplicationRouteEvent`
- `ApplicationRouteFilter`
- `ApplicationRouteMessage`
- `ApplicationRouteSubscription`

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
- `TextWatchSubscription`
- `Transaction`
- `TransactionChain`
- `TraversalSubscription`
- `VacuumReport`
- `VectorWatchSubscription`

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

### `native_moq`

- `NativeMoqRouteClient`
- `NativeMoqRouteClientBackend`

### `native_moq_draft07`

- `NativeDraft07MoqRouteClient`

### `native_moq_ietf`

- `NativeIetfMoqRouteClient`

### `native_relay_server`

- `NativeRelayServer`

### `native_sync`

- `NativeMoqSync`
- `NativeWebSocketSync`

### `net`

- `IceServerConfig`
- `IceServerUrls`
- `MeshConfig`
- `MeshSignal`
- `MeshSignalingMode`
- `MoqDraft`
- `MoqRelayClientConfig`
- `RelayClientConfig`
- `RelayEndpointConfig`
- `RelayServerConfig`

### `operation`

- `Operation`
- `OperationAction`
- `OperationValue`

### `overlay`

- `APPLICATION_STREAM_NAMESPACE`
- `APPLICATION_STREAM_PROTOCOL_V1`
- `ApplicationStreamAssembler`
- `ApplicationStreamEvent`
- `ApplicationStreamFrame`
- `ApplicationStreamFrameKind`
- `ApplicationStreamSendOptions`
- `ApplicationStreamSendReport`
- `RouteOverlayDeliveryAttempt`
- `RouteOverlayPolicy`
- `RouteOverlayPumpReport`
- `RouteOverlaySendMode`
- `RouteOverlaySendReport`
- `RouteOverlaySession`
- `RouteOverlayUnderlayHandle`
- `RouteOverlayUnderlayInfo`

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
- `merge_remote_records_fan_in`
- `merge_remote_text_search_fan_in`
- `PullChunk`
- `PullRequest`
- `PullRequestKind`
- `PullResponse`
- `PullResponseBody`
- `RemoteFanInWatch`
- `RemoteFanInWatchEvent`
- `RemoteInterestPolicy`
- `RemoteInterestTarget`
- `RemotePath`
- `RemotePeerFailure`
- `RemotePeerRecords`
- `RemotePeerTextSearch`
- `RemoteRecordConflict`
- `RemoteRecordConflictSource`
- `RemoteRecordsFanIn`
- `RemoteResult`
- `RemoteTextSearchFanIn`
- `RemoteWatchMessage`
- `RemoteWatchSubscription`
- `stable_content_hash`
- `SyncEnvelope`
- `SyncFrame`
- `WatchEvent`
- `WatchRequest`
- `WatchRequestKind`

### `text_search`

- `SearchStalePolicy`
- `TextAnalyzerConfig`
- `TextAnalyzerKind`
- `TextCacheFiles`
- `TextCacheManifest`
- `TextCandidatePolicy`
- `TextCollectionConfig`
- `TextDocument`
- `TextFieldConfig`
- `TextFieldHit`
- `TextIndexState`
- `TextIndexStats`
- `TextScoreScope`
- `TextSearchBackend`
- `TextSearchMatch`
- `TextSearchMode`
- `TextSearchResult`
- `TextSearchSource`
- `TextSearchSourceSummary`
- `TextSearchSpec`
- `TextSnippet`

### `transport`

- `InMemoryRouteHub`
- `InMemoryRouteSession`
- `RouteRelayCore`
- `RouteRelayForward`
- `RouteSessionInfo`
- `RouteTransportKind`

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

### `vector`

- `VectorBackendKind`
- `VectorCacheFiles`
- `VectorCacheKeyRecord`
- `VectorCacheManifest`
- `VectorChunkHeader`
- `VectorChunkingConfig`
- `VectorCollectionConfig`
- `VectorEntry`
- `VectorFilter`
- `VectorHnswConfig`
- `VectorIndexStats`
- `VectorItemMeta`
- `VectorManagerState`
- `VectorMatch`
- `VectorMetadataFilter`
- `VectorMetric`
- `VectorSearchResult`
- `VectorSearchSpec`
- `VectorStalePolicy`

## Application routes

Rust relay, MoQ, and mesh handles expose RouteEnvelope-level application messages through `publish_application(...)`, `send_application(...)`, and `subscribe_applications(...)`.

The public types are re-exported from the crate root and include `ApplicationRouteMessage`, `ApplicationRouteEvent`, `ApplicationRouteFilter`, and `ApplicationRouteSubscription`.

Application routes preserve the surrounding route id, source peer, target, TTL, dedupe, and transport metadata instead of embedding transport-specific socket handles.

## Record fan-in

Rust relay, MoQ, and mesh handles expose `records_fan_in(...)` and `watch_records_fan_in(...)` for source-tagged multi-peer record scans and watches.

The public types are re-exported from the crate root and include `RemoteRecordsFanIn`, `RemotePeerRecords`, `RemotePeerFailure`, `RemoteRecordConflict`, `RemoteFanInWatch`, and `RemoteFanInWatchEvent`.

Fan-in results include deterministic merged records, conflict metadata, and partial failures while preserving per-peer source metadata.

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
