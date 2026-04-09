#[cfg(feature = "crypto")]
mod auth;
mod clock;
mod compat;
#[cfg(feature = "crypto")]
mod crypto;
mod db;
mod engine;
mod error;
mod hardening;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
mod native_sync;
mod operation;
mod parallel;
mod persistence;
mod query;
mod router;
mod snapshot;
mod storage;
mod sync;
mod value;

#[cfg(target_arch = "wasm32")]
mod wasm;

#[cfg(feature = "crypto")]
pub use auth::{
    AuthClaims, AuthenticatedSyncFrame, DataCertificate, EncryptedSyncFrame, LocalUser,
    SecureSyncFrame, SecurityState, SignedValueClaims, StoredSnapshot, UserGrant, UserRecord,
    inspect_signed_field_value, owner_public_key_for_path, InspectedSignedFieldValue,
};
pub use clock::{HybridClock, Revision, VersionMarker};
pub use compat::{Gun, GunChain, GunCompatOptions};
#[cfg(feature = "crypto")]
pub use crypto::{
    EncryptedPayload, Identity, PublicIdentity, SeaPair, SecretBoxKey, SignedPayload,
};
pub use db::{
    Chain, ChangeEvent, ChangeSubscription, LexBuilder, MapEntry, Primadb, QueryBuilder,
    Subscription,
};
pub use engine::{
    AuthNodeMeta, DirectScalarIndexEntry, IncrementalStore, NodeIndexManifest, StorageMetadata,
    StorageTransaction, StoredAuthFieldMeta, build_storage_metadata,
    build_storage_transaction, build_storage_transaction_from_ops, direct_index_key,
    encode_component, node_matches_root, operation_matches_root, sortable_scalar_key,
    touched_nodes,
};
pub use error::{PrimadbError, Result};
pub use hardening::{PrimadbLimits, PrimadbStats};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
pub use native_sync::NativeWebSocketSync;
pub use operation::{Operation, OperationAction, OperationValue};
pub use parallel::{parallel_enabled, parallel_thread_count};
pub use query::{LexEntry, LexSpec, QueryDirection, QueryFilter, QueryOrder, QuerySpec};
pub use router::{
    PeerPresence, PeerRecommendation, RouteBatchItem, RouteDecision, RouteEnvelope, RoutePayload,
    RouteTarget, Router, RouterConfig, RouterStats,
};
pub use snapshot::DatabaseSnapshot;
pub use storage::{MemoryStorageAdapter, StorageAdapter, StorageReport};
#[cfg(not(target_arch = "wasm32"))]
pub use storage::{RadiskFileAdapter, SnapshotFileAdapter};
#[cfg(not(target_arch = "wasm32"))]
pub use engine::SegmentFileStore;
pub use sync::{
    PullChunk, PullRequest, PullRequestKind, PullResponse, PullResponseBody, RemotePath,
    RemoteResult, SyncEnvelope, SyncFrame, stable_content_hash,
};
pub use value::{FieldState, FieldValue, NodeId, NodeState, SetState};
