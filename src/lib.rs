mod clock;
#[cfg(feature = "crypto")]
mod auth;
mod compat;
#[cfg(feature = "crypto")]
mod crypto;
mod db;
mod error;
mod hardening;
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
mod native_sync;
mod operation;
mod persistence;
mod query;
mod router;
mod snapshot;
mod storage;
mod sync;
mod value;

#[cfg(target_arch = "wasm32")]
mod wasm;

pub use clock::{HybridClock, Revision, VersionMarker};
#[cfg(feature = "crypto")]
pub use auth::{
    AuthClaims, AuthenticatedSyncFrame, DataCertificate, EncryptedSyncFrame, LocalUser,
    SecureSyncFrame, SecurityState, SignedValueClaims, StoredSnapshot, UserGrant, UserRecord,
    owner_public_key_for_path,
};
pub use compat::{Gun, GunChain, GunCompatOptions};
#[cfg(feature = "crypto")]
pub use crypto::{EncryptedPayload, Identity, PublicIdentity, SeaPair, SecretBoxKey, SignedPayload};
pub use db::{
    Chain, ChangeEvent, ChangeSubscription, LexBuilder, MapEntry, Primadb, QueryBuilder,
    Subscription,
};
pub use error::{PrimadbError, Result};
pub use hardening::{PrimadbLimits, PrimadbStats};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
pub use native_sync::NativeWebSocketSync;
pub use operation::{Operation, OperationAction, OperationValue};
pub use query::{LexEntry, LexSpec, QueryDirection, QueryFilter, QueryOrder, QuerySpec};
pub use router::{
    PeerPresence, PeerRecommendation, RouteBatchItem, RouteDecision, RouteEnvelope, RoutePayload,
    RouteTarget, Router, RouterConfig, RouterStats,
};
pub use snapshot::DatabaseSnapshot;
pub use storage::{MemoryStorageAdapter, StorageAdapter, StorageReport};
#[cfg(not(target_arch = "wasm32"))]
pub use storage::{RadiskFileAdapter, SnapshotFileAdapter};
pub use sync::{
    PullChunk, PullRequest, PullRequestKind, PullResponse, PullResponseBody, RemotePath,
    RemoteResult, SyncEnvelope, SyncFrame, stable_content_hash,
};
pub use value::{FieldState, FieldValue, NodeId, NodeState, SetState};
