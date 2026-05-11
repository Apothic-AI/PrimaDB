#[cfg(feature = "crypto")]
mod auth;
mod binary;
mod blob;
mod clock;
mod compat;
mod consistency;
#[cfg(feature = "crypto")]
mod crypto;
mod db;
mod durable;
mod engine;
mod error;
mod hardening;
mod hooks;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-webrtc"))]
mod native_mesh;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
mod native_moq;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
mod native_moq_draft07;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
mod native_moq_ietf;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
mod native_relay_server;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
mod native_sync;
mod net;
mod operation;
mod parallel;
mod persistence;
mod query;
mod record;
mod router;
#[cfg(feature = "scripting")]
mod scripting;
mod session_auth;
mod snapshot;
mod storage;
mod sync;
mod transport;
mod traversal;
mod value;
mod vector;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
mod wasm_opfs;

#[cfg(feature = "crypto")]
pub use auth::{
    AuthClaims, AuthenticatedSyncFrame, DataCertificate, EncryptedSyncFrame,
    InspectedSignedFieldValue, LocalUser, SecureSyncFrame, SecurityState, SignedValueClaims,
    StoredSnapshot, UserGrant, UserRecord, inspect_signed_field_value, owner_public_key_for_path,
};
pub use binary::BinaryBytes;
pub use blob::{
    BlobRef, BlobStorageBinding, BlobStorageConfig, BlobStore, MemoryBlobStore, StoredBlob,
    blob_ref_for_data,
};
#[cfg(not(target_arch = "wasm32"))]
pub use blob::{FileBlobStore, FileBlobStoreOptions};
pub use clock::{HybridClock, Revision, VersionMarker};
pub use compat::{Gun, GunChain, GunCompatOptions};
pub use consistency::{
    ProvisionalTransaction, ScopeAuthority, ScopeConsistency, ScopeIsolation, ScopeOfflineWrites,
    ScopePolicy, ScopeReadMode, TransactionOptions, TransactionReport, TransactionStatus,
    TransactionStep,
};
#[cfg(feature = "crypto")]
pub use crypto::{
    EncryptedPayload, Identity, PasswordDerivedKey, PasswordKeyDerivationOptions,
    PasswordKeyDerivationParams, PublicIdentity, SeaPair, SecretBoxKey, SignedPayload,
    derive_password_key,
};
pub use db::{
    Chain, ChangeEvent, ChangeSubscription, LexBuilder, MapEntry, NodeFetchScheduler, Primadb,
    QueryBuilder, RecordWatchSubscription, Scope, Subscription, Transaction, TransactionChain,
    TraversalSubscription, VacuumReport, VectorWatchSubscription,
};
pub use durable::{
    DurableStorageBinding, DurableStorageConfig, SegmentDurability, SegmentFileStoreOptions,
    SegmentLockMode,
};
#[cfg(not(target_arch = "wasm32"))]
pub use engine::SegmentFileStore;
pub use engine::{
    AuthNodeMeta, DirectIndexScan, DirectScalarIndexEntry, IncrementalStore, NodeIndexManifest,
    StorageMetadata, StorageRecoveryReport, StorageSyncReport, StorageTransaction,
    StorageVacuumReport, StoredAuthFieldMeta, build_storage_metadata, build_storage_transaction,
    build_storage_transaction_from_ops, direct_index_encode_prefix, direct_index_key,
    encode_component, is_record_node_id, node_matches_root, operation_matches_root,
    record_entry_from_node_state, record_key_from_node_state, record_node_id, sortable_scalar_key,
    touched_nodes, touched_storage_nodes,
};
pub use error::{PrimadbError, Result};
pub use hardening::{PrimadbLimits, PrimadbStats};
pub use hooks::{
    ConnectHookContext, HookDecision, HookTransport, NetworkHooks, RoomHookContext,
    ServeRequestContext, ServeResultContext, parse_request_hook_json, parse_result_hook_json,
    parse_void_hook_json,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-webrtc"))]
pub use native_mesh::NativeWebRtcMesh;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
pub use native_moq::{NativeMoqRouteClient, NativeMoqRouteClientBackend};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
pub use native_moq_draft07::NativeDraft07MoqRouteClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
pub use native_moq_ietf::NativeIetfMoqRouteClient;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
pub use native_relay_server::NativeRelayServer;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
pub use native_sync::NativeMoqSync;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
pub use native_sync::NativeWebSocketSync;
pub use net::{
    IceServerConfig, IceServerUrls, MeshConfig, MeshSignal, MeshSignalingMode, MoqDraft,
    MoqRelayClientConfig, RelayClientConfig, RelayEndpointConfig, RelayServerConfig,
};
pub use operation::{Operation, OperationAction, OperationValue};
pub use parallel::{parallel_enabled, parallel_thread_count};
pub use query::{LexEntry, LexSpec, QueryDirection, QueryFilter, QueryOrder, QuerySpec};
pub use record::{
    RecordBatch, RecordBatchReport, RecordEntry, RecordMutation, RecordPrecondition, RecordScan,
    RecordScanResult, RecordValue,
};
pub use router::{
    PeerPresence, PeerRecommendation, RouteBatchItem, RouteDecision, RouteEnvelope, RoutePayload,
    RouteTarget, Router, RouterConfig, RouterStats,
};
#[cfg(feature = "scripting")]
pub use scripting::{
    NodeScript, ScriptCapabilities, ScriptExecutionContext, ScriptExecutionOptions,
    ScriptExecutionResult, ScriptLimits, ScriptPathGrant, ScriptRuntime,
};
pub use session_auth::{
    AuthChallenge, AuthResponse, AuthTranscript, IdentityTrust, PresenceIdentity,
    SessionAuthConfig, VerifiedIdentity,
};
pub use snapshot::DatabaseSnapshot;
pub use storage::{MemoryStorageAdapter, StorageAdapter, StorageReport};
#[cfg(not(target_arch = "wasm32"))]
pub use storage::{SnapshotFileAdapter, SnapshotLogFileAdapter};
pub use sync::{
    PullChunk, PullRequest, PullRequestKind, PullResponse, PullResponseBody, RemoteInterestPolicy,
    RemoteInterestTarget, RemotePath, RemoteResult, RemoteWatchMessage, RemoteWatchSubscription,
    SyncEnvelope, SyncFrame, WatchEvent, WatchRequest, WatchRequestKind, error_pull_response,
    error_watch_event, stable_content_hash,
};
pub use transport::{
    InMemoryRouteHub, InMemoryRouteSession, RouteRelayCore, RouteRelayForward, RouteSessionInfo,
    RouteTransportKind,
};
pub use traversal::{
    TraversalDirection, TraversalEdge, TraversalEdgeKind, TraversalEntry, TraversalResult,
    TraversalSpec, TraversalStrategy,
};
pub use value::{FieldState, FieldValue, NodeId, NodeState, SetState};
#[cfg(any(
    target_arch = "wasm32",
    feature = "native-websocket",
    feature = "native-webrtc"
))]
pub(crate) use vector::vector_collection_from_record_key;
pub use vector::{
    VectorBackendKind, VectorCacheFiles, VectorCacheKeyRecord, VectorCacheManifest,
    VectorChunkHeader, VectorChunkingConfig, VectorCollectionConfig, VectorEntry, VectorFilter,
    VectorHnswConfig, VectorIndexStats, VectorItemMeta, VectorManagerState, VectorMatch,
    VectorMetadataFilter, VectorMetric, VectorSearchResult, VectorSearchSpec, VectorStalePolicy,
};
