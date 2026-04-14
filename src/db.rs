use crate::binary::BinaryBytes;
use crate::blob::{
    BlobRef, BlobStorageBinding, BlobStorageConfig, BlobStore, MemoryBlobStore, StoredBlob,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::blob::FileBlobStore;
use crate::clock::{HybridClock, Revision, VersionMarker};
use crate::engine::{
    IncrementalStore, build_storage_metadata, build_storage_transaction,
    build_storage_transaction_from_ops,
};
use crate::error::{PrimadbError, Result};
use crate::hardening::{PrimadbLimits, PrimadbStats};
use crate::operation::{Operation, OperationAction, OperationValue};
use crate::persistence::{PersistenceTarget, load_snapshot_payload, store_snapshot_payload};
use crate::query::{LexEntry, LexSpec, QueryDirection, QueryFilter, QuerySpec};
use crate::snapshot::DatabaseSnapshot;
use crate::storage::StorageAdapter;
use crate::sync::{
    PullChunk, PullRequest, PullRequestKind, PullResponse, PullResponseBody, RemotePath,
    RemoteResult, SyncEnvelope, SyncFrame,
};
use crate::value::{FieldState, FieldValue, NodeId, NodeState, SetState};
#[cfg(feature = "crypto")]
use crate::{
    Identity, PublicIdentity, SecretBoxKey, SecureSyncFrame, SecurityState, StoredSnapshot,
    UserGrant, owner_public_key_for_path,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::durable::{DurableStorageBinding, DurableStorageConfig};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-webrtc"))]
use crate::{MeshConfig, NativeWebRtcMesh};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
use crate::{NativeWebSocketSync, RelayClientConfig};
use async_channel::{Receiver, Sender};
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
const PARALLEL_QUERY_MIN_LEN: usize = 256;
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
const PARALLEL_CHUNK_MIN_LEN: usize = 512;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MapEntry {
    pub key: String,
    pub value: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeEvent {
    pub revision: u64,
    pub pending_ops: usize,
    pub data_changed: bool,
}

#[derive(Debug, Clone)]
pub struct Primadb {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Clone)]
pub struct Chain {
    db: Primadb,
    anchor: NodeId,
    segments: Vec<String>,
}

pub struct Subscription {
    inner: Arc<SubscriptionInner>,
}

pub struct ChangeSubscription {
    inner: Arc<ChangeSubscriptionInner>,
}

#[derive(Debug, Clone)]
pub struct QueryBuilder {
    chain: Chain,
    spec: QuerySpec,
}

#[derive(Debug, Clone)]
pub struct LexBuilder {
    chain: Chain,
    spec: LexSpec,
}

struct SubscriptionInner {
    id: u64,
    db: Weak<Mutex<Inner>>,
    receiver: Receiver<Option<JsonValue>>,
}

struct ChangeSubscriptionInner {
    id: u64,
    db: Weak<Mutex<Inner>>,
    receiver: Receiver<ChangeEvent>,
}

#[derive(Debug)]
struct Inner {
    clock: HybridClock,
    nodes: std::collections::BTreeMap<NodeId, NodeState>,
    pending_ops: Vec<Operation>,
    unflushed_ops: Vec<Operation>,
    subscriptions: std::collections::BTreeMap<u64, Watcher>,
    change_subscriptions: std::collections::BTreeMap<u64, ChangeWatcher>,
    next_subscription_id: u64,
    next_change_subscription_id: u64,
    change_revision: u64,
    persistence: Option<PersistenceTarget>,
    storage_adapter: Option<Arc<dyn StorageAdapter>>,
    storage_engine: Option<Arc<dyn IncrementalStore>>,
    blob_store: Option<Arc<dyn BlobStore>>,
    missing_nodes: BTreeSet<NodeId>,
    next_storage_tx_id: u64,
    limits: PrimadbLimits,
    #[cfg(feature = "crypto")]
    security: SecurityState,
}

#[derive(Debug, Clone)]
struct Watcher {
    anchor: NodeId,
    segments: Vec<String>,
    sender: Sender<Option<JsonValue>>,
}

#[derive(Debug, Clone)]
struct ChangeWatcher {
    sender: Sender<ChangeEvent>,
}

#[derive(Debug, Clone)]
enum Cursor {
    Node(NodeId),
    Field { node: NodeId, field: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationOrigin {
    Local,
    Remote,
}

enum ParsedInput {
    Scalar(JsonValue),
    Bytes(BinaryBytes),
    Blob(BlobRef),
    Link(NodeId),
    Set(Vec<SetMember>),
    Object(Map<String, JsonValue>),
}

enum SetMember {
    Link(NodeId),
    Object(Map<String, JsonValue>),
}

impl Primadb {
    pub fn with_replica_id(replica_id: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                clock: HybridClock::with_actor(replica_id),
                nodes: Default::default(),
                pending_ops: Vec::new(),
                unflushed_ops: Vec::new(),
                subscriptions: Default::default(),
                change_subscriptions: Default::default(),
                next_subscription_id: 0,
                next_change_subscription_id: 0,
                change_revision: 0,
                persistence: None,
                storage_adapter: None,
                storage_engine: None,
                blob_store: None,
                missing_nodes: BTreeSet::new(),
                next_storage_tx_id: 1,
                limits: PrimadbLimits::default(),
                #[cfg(feature = "crypto")]
                security: SecurityState::default(),
            })),
        }
    }

    pub fn replica_id(&self) -> String {
        self.inner.lock().unwrap().clock.actor().to_owned()
    }

    pub fn root(&self, node: impl Into<String>) -> Chain {
        Chain {
            db: self.clone(),
            anchor: node.into(),
            segments: Vec::new(),
        }
    }

    pub fn snapshot(&self) -> DatabaseSnapshot {
        let (engine, clock, pending_ops, nodes) = {
            let inner = self.inner.lock().unwrap();
            (
                inner.storage_engine.clone(),
                inner.clock.clone(),
                inner.pending_ops.clone(),
                inner.nodes.clone(),
            )
        };
        if let Some(engine) = engine {
            if let Ok(mut snapshot) = engine.export_snapshot(None) {
                snapshot.clock = clock;
                snapshot.pending_ops = pending_ops;
                for (node_id, node_state) in nodes {
                    snapshot.nodes.insert(node_id, node_state);
                }
                return snapshot;
            }
        }
        DatabaseSnapshot {
            clock,
            nodes,
            pending_ops,
        }
    }

    pub fn export_snapshot_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(&self.snapshot())?)
    }

    pub fn export_persisted_snapshot_json(&self) -> Result<String> {
        #[cfg(feature = "crypto")]
        {
            let snapshot = self.snapshot();
            let security = self.inner.lock().unwrap().security.clone();
            let stored = security.encode_snapshot(snapshot)?;
            return Ok(match stored {
                StoredSnapshot::Plain(snapshot) => serde_json::to_string_pretty(&snapshot)?,
                stored => serde_json::to_string_pretty(&stored)?,
            });
        }

        #[cfg(not(feature = "crypto"))]
        {
            self.export_snapshot_json()
        }
    }

    pub fn import_snapshot_json(&self, payload: &str) -> Result<()> {
        let snapshot = serde_json::from_str(payload)?;
        self.load_snapshot(snapshot)
    }

    pub fn merge_snapshot_json(&self, payload: &str) -> Result<()> {
        let snapshot = serde_json::from_str(payload)?;
        self.merge_snapshot(snapshot)
    }

    pub fn import_persisted_snapshot_json(&self, payload: &str) -> Result<()> {
        if let Ok(snapshot) = serde_json::from_str::<DatabaseSnapshot>(payload) {
            return self.load_persisted_snapshot(snapshot);
        }

        #[cfg(feature = "crypto")]
        {
            let stored: StoredSnapshot = serde_json::from_str(payload)?;
            let snapshot = {
                let inner = self.inner.lock().unwrap();
                inner.security.decode_snapshot(stored)?
            };
            self.load_persisted_snapshot(snapshot)
        }

        #[cfg(not(feature = "crypto"))]
        {
            Err(PrimadbError::Message(
                "persisted snapshot payload is not a plain snapshot".to_owned(),
            ))
        }
    }

    pub fn load_snapshot(&self, snapshot: DatabaseSnapshot) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.clock = snapshot.clock;
            inner.nodes = snapshot.nodes;
            inner.pending_ops = snapshot.pending_ops;
            inner.unflushed_ops.clear();
            inner.missing_nodes.clear();
        }
        self.finalize_change(true)
    }

    pub fn merge_snapshot(&self, snapshot: DatabaseSnapshot) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            merge_snapshot_into_inner(&mut inner, snapshot);
        }
        self.finalize_change(true)
    }

    fn load_persisted_snapshot(&self, snapshot: DatabaseSnapshot) -> Result<()> {
        let local_actor = self.replica_id();
        let keep_pending = snapshot.clock.actor() == local_actor;
        {
            let mut inner = self.inner.lock().unwrap();
            inner.clock = snapshot.clock.rebased_with_actor(local_actor);
            inner.nodes = snapshot.nodes;
            inner.pending_ops = if keep_pending {
                snapshot.pending_ops
            } else {
                Vec::new()
            };
            inner.unflushed_ops.clear();
            inner.missing_nodes.clear();
        }
        self.finalize_change(true)
    }

    pub fn pending_operations(&self) -> Vec<Operation> {
        self.inner.lock().unwrap().pending_ops.clone()
    }

    pub fn change_revision(&self) -> u64 {
        self.inner.lock().unwrap().change_revision
    }

    pub fn sync_envelope(&self) -> SyncEnvelope {
        SyncEnvelope {
            from: self.replica_id(),
            ops: self.pending_operations(),
        }
    }

    pub fn drain_pending_operations(&self) -> Result<Vec<Operation>> {
        let ops = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::take(&mut inner.pending_ops)
        };
        if !ops.is_empty() {
            self.finalize_change(false)?;
        }
        Ok(ops)
    }

    pub fn requeue_pending_operations<I>(&self, ops: I) -> Result<usize>
    where
        I: IntoIterator<Item = Operation>,
    {
        let mut count = 0;
        {
            let mut inner = self.inner.lock().unwrap();
            for op in ops {
                inner.pending_ops.push(op);
                count += 1;
            }
        }
        if count > 0 {
            self.finalize_change(false)?;
        }
        Ok(count)
    }

    pub fn drain_sync_envelope(&self) -> Result<SyncEnvelope> {
        Ok(SyncEnvelope {
            from: self.replica_id(),
            ops: self.drain_pending_operations()?,
        })
    }

    pub fn export_pending_operations_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.sync_envelope())?)
    }

    pub fn drain_pending_operations_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.drain_sync_envelope()?)?)
    }

    pub fn apply_operation(&self, op: Operation) -> Result<bool> {
        self.apply_operations(std::iter::once(op))
            .map(|count| count == 1)
    }

    pub fn apply_operations<I>(&self, ops: I) -> Result<usize>
    where
        I: IntoIterator<Item = Operation>,
    {
        let mut applied = 0;
        {
            let mut inner = self.inner.lock().unwrap();
            for op in ops {
                if inner.apply_operation_internal(op, OperationOrigin::Remote) {
                    applied += 1;
                }
            }
        }
        if applied > 0 {
            self.finalize_change(true)?;
        }
        Ok(applied)
    }

    pub fn apply_sync_envelope(&self, envelope: SyncEnvelope) -> Result<usize> {
        self.apply_operations(envelope.ops)
    }

    pub fn apply_operations_json(&self, payload: &str) -> Result<usize> {
        match serde_json::from_str::<SyncFrame>(payload) {
            Ok(frame) => match frame {
                SyncFrame::Sync {
                    from,
                    message_id: _,
                    ops,
                } => self.apply_sync_envelope(SyncEnvelope { from, ops }),
                SyncFrame::Ack { .. } => Ok(0),
            },
            Err(_) => {
                #[cfg(feature = "crypto")]
                if let Ok(frame) = serde_json::from_str::<SecureSyncFrame>(payload) {
                    return self.apply_secure_sync_frame(frame);
                }
                let envelope: SyncEnvelope = serde_json::from_str(payload)?;
                self.apply_sync_envelope(envelope)
            }
        }
    }

    #[cfg(feature = "crypto")]
    pub fn secure_sync_frame(&self, frame: SyncFrame) -> Result<SecureSyncFrame> {
        let roots = crate::auth::roots_for_frame(&frame);
        let inner = self.inner.lock().unwrap();
        inner
            .security
            .encode_sync_frame(inner.clock.actor(), roots, frame)
    }

    #[cfg(feature = "crypto")]
    pub fn secure_sync_frame_json(&self, frame: SyncFrame) -> Result<String> {
        Ok(serde_json::to_string(&self.secure_sync_frame(frame)?)?)
    }

    #[cfg(feature = "crypto")]
    pub fn apply_secure_sync_frame(&self, frame: SecureSyncFrame) -> Result<usize> {
        let decoded = self.decode_secure_sync_frame(frame)?;
        match decoded {
            SyncFrame::Sync {
                from,
                message_id: _,
                ops,
            } => self.apply_sync_envelope(SyncEnvelope { from, ops }),
            SyncFrame::Ack { .. } => Ok(0),
        }
    }

    #[cfg(feature = "crypto")]
    pub(crate) fn decode_secure_sync_frame(&self, frame: SecureSyncFrame) -> Result<SyncFrame> {
        let inner = self.inner.lock().unwrap();
        inner.security.decode_sync_frame(frame)
    }

    pub fn get_path(&self, path: &RemotePath) -> Result<Option<JsonValue>> {
        self.materialize(&path.anchor, &path.segments)
    }

    pub fn map_path(&self, path: &RemotePath) -> Result<Vec<MapEntry>> {
        self.map_at(&path.anchor, &path.segments)
    }

    pub fn query_path(&self, path: &RemotePath, spec: &QuerySpec) -> Result<Vec<MapEntry>> {
        self.query_at(&path.anchor, &path.segments, spec)
    }

    pub fn lex_path(&self, path: &RemotePath, spec: &LexSpec) -> Result<Vec<LexEntry>> {
        self.scan_at(&path.anchor, &path.segments, spec)
    }

    pub fn snapshot_for_root(&self, root: Option<&str>) -> DatabaseSnapshot {
        let (engine, clock, pending_ops, loaded_nodes) = {
            let inner = self.inner.lock().unwrap();
            (
                inner.storage_engine.clone(),
                inner.clock.clone(),
                inner.pending_ops.clone(),
                inner.nodes.clone(),
            )
        };

        let mut snapshot = if let Some(engine) = engine {
            engine.export_snapshot(None).unwrap_or(DatabaseSnapshot {
                clock: clock.clone(),
                nodes: BTreeMap::new(),
                pending_ops: Vec::new(),
            })
        } else {
            DatabaseSnapshot {
                clock: clock.clone(),
                nodes: BTreeMap::new(),
                pending_ops: Vec::new(),
            }
        };

        snapshot.clock = clock;
        for (node_id, node_state) in loaded_nodes {
            snapshot.nodes.insert(node_id, node_state);
        }

        if let Some(root) = root {
            let reachable = collect_snapshot_root_closure(&snapshot.nodes, root);
            snapshot.nodes.retain(|node_id, _| reachable.contains(node_id));
            snapshot.pending_ops = pending_ops
                .into_iter()
                .filter(|op| operation_matches_snapshot_nodes(op, &reachable))
                .collect();
        } else {
            snapshot.pending_ops = pending_ops;
        }

        snapshot
    }

    pub fn execute_pull_request(&self, request: &PullRequest) -> Result<RemoteResult> {
        match &request.request {
            PullRequestKind::Get { path } => Ok(RemoteResult::Get {
                value: self.get_path(path)?,
            }),
            PullRequestKind::Map { path } => Ok(RemoteResult::Map {
                entries: self.map_path(path)?,
            }),
            PullRequestKind::Query { path, spec } => Ok(RemoteResult::Query {
                entries: self.query_path(path, spec)?,
            }),
            PullRequestKind::Lex { path, spec } => Ok(RemoteResult::Lex {
                entries: self.lex_path(path, spec)?,
            }),
            PullRequestKind::Snapshot { root } => Ok(RemoteResult::Snapshot {
                snapshot: self.snapshot_for_root(root.as_deref()),
            }),
        }
    }

    pub fn chunk_remote_result(&self, request_id: &str, result: RemoteResult) -> Vec<PullResponse> {
        build_pull_responses(request_id, result, &self.limits())
    }

    pub fn subscribe_changes(&self) -> ChangeSubscription {
        let (sender, receiver) = async_channel::unbounded();
        let (id, event) = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_change_subscription_id = inner.next_change_subscription_id.saturating_add(1);
            let id = inner.next_change_subscription_id;
            let event = ChangeEvent {
                revision: inner.change_revision,
                pending_ops: inner.pending_ops.len(),
                data_changed: false,
            };
            inner.change_subscriptions.insert(
                id,
                ChangeWatcher {
                    sender: sender.clone(),
                },
            );
            (id, event)
        };
        let _ = sender.try_send(event);
        ChangeSubscription {
            inner: Arc::new(ChangeSubscriptionInner {
                id,
                db: Arc::downgrade(&self.inner),
                receiver,
            }),
        }
    }

    pub fn stats(&self) -> PrimadbStats {
        let inner = self.inner.lock().unwrap();
        PrimadbStats {
            replica_id: inner.clock.actor().to_owned(),
            nodes: inner.nodes.len(),
            pending_ops: inner.pending_ops.len(),
            subscriptions: inner.subscriptions.len(),
            change_subscriptions: inner.change_subscriptions.len(),
            unflushed_ops: inner.unflushed_ops.len(),
        }
    }

    pub fn limits(&self) -> PrimadbLimits {
        self.inner.lock().unwrap().limits.clone()
    }

    pub fn set_limits(&self, limits: PrimadbLimits) {
        self.inner.lock().unwrap().limits = limits;
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn use_file_persistence(&self, path: impl Into<std::path::PathBuf>) -> Result<bool> {
        let target = PersistenceTarget::File(path.into());
        self.configure_persistence(target)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn use_radisk_storage(
        &self,
        directory: impl Into<std::path::PathBuf>,
        compaction_threshold: usize,
    ) -> Result<bool> {
        let store = crate::SegmentFileStore::new(directory, compaction_threshold);
        self.attach_incremental_store(Arc::new(store))
    }

    #[cfg(target_arch = "wasm32")]
    pub fn use_browser_storage(&self, key: impl Into<String>) -> Result<bool> {
        let target = PersistenceTarget::BrowserStorage(key.into());
        self.configure_persistence(target)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_durable_storage(&self, config: DurableStorageConfig) -> Result<DurableStorageBinding> {
        match config {
            DurableStorageConfig::SnapshotFile { path } => {
                let loaded = self.use_file_persistence(path)?;
                Ok(DurableStorageBinding {
                    backend: "snapshot_file".to_owned(),
                    incremental: false,
                    loaded_existing: loaded,
                    auto_persist: true,
                })
            }
            DurableStorageConfig::SegmentFiles {
                directory,
                journal_retention,
            } => {
                let loaded = self.use_radisk_storage(directory, journal_retention)?;
                Ok(DurableStorageBinding {
                    backend: "segment_file".to_owned(),
                    incremental: true,
                    loaded_existing: loaded,
                    auto_persist: true,
                })
            }
            DurableStorageConfig::BrowserStorage { .. }
            | DurableStorageConfig::IndexedDbSnapshots { .. }
            | DurableStorageConfig::IndexedDbSegments { .. } => Err(PrimadbError::Message(
                "browser durable storage config is not available on native targets".to_owned(),
            )),
        }
    }

    pub fn attach_blob_store(&self, store: Arc<dyn BlobStore>) {
        self.inner.lock().unwrap().blob_store = Some(store);
    }

    pub fn open_blob_storage(&self, config: BlobStorageConfig) -> Result<BlobStorageBinding> {
        match config {
            BlobStorageConfig::Memory => {
                self.attach_blob_store(Arc::new(MemoryBlobStore::new()));
                Ok(BlobStorageBinding {
                    backend: "memory".to_owned(),
                    content_addressed: true,
                })
            }
            #[cfg(not(target_arch = "wasm32"))]
            BlobStorageConfig::Files { directory } => {
                self.attach_blob_store(Arc::new(FileBlobStore::new(directory)));
                Ok(BlobStorageBinding {
                    backend: "files".to_owned(),
                    content_addressed: true,
                })
            }
            #[cfg(target_arch = "wasm32")]
            BlobStorageConfig::IndexedDb { .. } => Err(PrimadbError::Message(
                "browser blob storage config must be opened through the wasm bindings".to_owned(),
            )),
        }
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
    pub async fn connect_relay(&self, config: RelayClientConfig) -> Result<NativeWebSocketSync> {
        NativeWebSocketSync::connect_with_config(self.clone(), config).await
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "native-webrtc"))]
    pub async fn connect_mesh(&self, config: MeshConfig) -> Result<NativeWebRtcMesh> {
        NativeWebRtcMesh::connect_with_config(self.clone(), config).await
    }

    pub fn store_blob(&self, data: impl AsRef<[u8]>, media_type: Option<&str>) -> Result<BlobRef> {
        let store = self
            .inner
            .lock()
            .unwrap()
            .blob_store
            .clone()
            .ok_or(PrimadbError::BlobStoreUnavailable)?;
        store.put_blob(data.as_ref(), media_type)
    }

    pub fn get_blob(&self, blob_id: &str) -> Result<Option<StoredBlob>> {
        let store = self
            .inner
            .lock()
            .unwrap()
            .blob_store
            .clone()
            .ok_or(PrimadbError::BlobStoreUnavailable)?;
        store.get_blob(blob_id)
    }

    pub fn has_blob(&self, blob_id: &str) -> Result<bool> {
        let store = self
            .inner
            .lock()
            .unwrap()
            .blob_store
            .clone()
            .ok_or(PrimadbError::BlobStoreUnavailable)?;
        store.has_blob(blob_id)
    }

    pub fn attach_storage_adapter(&self, adapter: Arc<dyn StorageAdapter>) -> Result<bool> {
        let loaded = match adapter.load_snapshot()? {
            Some(snapshot) => {
                self.load_snapshot(snapshot)?;
                true
            }
            None => false,
        };

        {
            let mut inner = self.inner.lock().unwrap();
            inner.storage_adapter = Some(adapter);
        }
        self.persist_if_needed()?;
        Ok(loaded)
    }

    pub fn attach_incremental_store(&self, store: Arc<dyn IncrementalStore>) -> Result<bool> {
        let metadata = store.load_metadata()?;
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(metadata) = metadata.clone() {
                inner.clock = metadata.clock;
                inner.pending_ops = metadata.pending_ops;
                inner.unflushed_ops.clear();
                inner.nodes.clear();
                inner.missing_nodes.clear();
                inner.next_storage_tx_id = metadata.next_tx_id.max(1);
            } else {
                inner.next_storage_tx_id = 1;
                inner.missing_nodes.clear();
            }
            inner.storage_engine = Some(store);
        }
        self.persist_if_needed()?;
        Ok(metadata.is_some())
    }

    #[cfg(feature = "crypto")]
    pub fn set_require_signed_sync(&self, required: bool) {
        self.inner.lock().unwrap().security.require_signed_sync = required;
    }

    #[cfg(feature = "crypto")]
    pub fn register_user(
        &self,
        alias: impl Into<String>,
        public_identity: PublicIdentity,
        grants: Vec<UserGrant>,
    ) -> Result<()> {
        let alias = alias.into();
        {
            let mut inner = self.inner.lock().unwrap();
            inner
                .security
                .register_user(alias.clone(), &public_identity, grants);
        }
        let public_key = public_identity.to_base64();
        self.root(format!("~@{alias}")).put(serde_json::json!({
            "alias": alias,
            "pub": public_key,
        }))
    }

    #[cfg(feature = "crypto")]
    pub fn authenticate_local_user(
        &self,
        alias: impl Into<String>,
        identity: Identity,
        grants: Vec<UserGrant>,
    ) -> Result<()> {
        let alias = alias.into();
        let public_key = identity.public_key_base64();
        {
            let mut inner = self.inner.lock().unwrap();
            inner
                .security
                .set_local_user(alias.clone(), identity, grants.clone())?;
        }
        self.root(format!("~@{alias}")).put(serde_json::json!({
            "alias": alias,
            "pub": public_key,
        }))?;
        self.root(format!("~{public_key}")).put_signed(
            serde_json::json!({
                "alias": alias,
                "pub": public_key,
            }),
            None,
        )
    }

    #[cfg(feature = "crypto")]
    pub fn set_snapshot_encryption_key(&self, key: SecretBoxKey) {
        self.inner
            .lock()
            .unwrap()
            .security
            .set_snapshot_encryption_key(key);
    }

    #[cfg(feature = "crypto")]
    pub fn set_transport_encryption_key(&self, key: SecretBoxKey) {
        self.inner
            .lock()
            .unwrap()
            .security
            .set_transport_encryption_key(key);
    }

    #[cfg(feature = "crypto")]
    pub fn create_write_certificate(
        &self,
        certificants: Vec<String>,
        write_policy: JsonValue,
        expires_at_millis: Option<u64>,
        write_block: Option<JsonValue>,
    ) -> Result<String> {
        let inner = self.inner.lock().unwrap();
        inner
            .security
            .certify_write(certificants, write_policy, expires_at_millis, write_block)
    }

    fn configure_persistence(&self, target: PersistenceTarget) -> Result<bool> {
        let loaded = match load_snapshot_payload(&target)? {
            Some(payload) => {
                self.import_persisted_snapshot_json(&payload)?;
                true
            }
            None => false,
        };

        {
            let mut inner = self.inner.lock().unwrap();
            inner.persistence = Some(target);
        }

        self.persist_if_needed()?;
        Ok(loaded)
    }

    fn persist_if_needed(&self) -> Result<()> {
        let (
            target,
            adapter,
            engine,
            snapshot,
            unflushed_ops,
            next_storage_tx_id,
            clock,
            pending_ops,
            nodes,
        ) = {
            let inner = self.inner.lock().unwrap();
            (
                inner.persistence.clone(),
                inner.storage_adapter.clone(),
                inner.storage_engine.clone(),
                DatabaseSnapshot {
                    clock: inner.clock.clone(),
                    nodes: inner.nodes.clone(),
                    pending_ops: inner.pending_ops.clone(),
                },
                inner.unflushed_ops.clone(),
                inner.next_storage_tx_id,
                inner.clock.clone(),
                inner.pending_ops.clone(),
                inner.nodes.clone(),
            )
        };

        if let Some(target) = target {
            store_snapshot_payload(&target, &self.export_persisted_snapshot_json()?)?;
        }

        if let Some(adapter) = adapter {
            adapter.flush(&unflushed_ops, &snapshot)?;
        }

        if let Some(engine) = engine {
            let metadata = build_storage_metadata(clock, pending_ops, next_storage_tx_id + 1);
            let transaction = if unflushed_ops.is_empty() {
                build_storage_transaction(next_storage_tx_id, metadata, nodes)
            } else {
                build_storage_transaction_from_ops(next_storage_tx_id, metadata, &nodes, &unflushed_ops)
            };
            engine.apply_transaction(&transaction)?;
            self.inner.lock().unwrap().next_storage_tx_id = next_storage_tx_id + 1;
        }

        if !unflushed_ops.is_empty() {
            self.inner.lock().unwrap().unflushed_ops.clear();
        }

        Ok(())
    }

    fn finalize_change(&self, data_changed: bool) -> Result<()> {
        let revision = {
            let mut inner = self.inner.lock().unwrap();
            inner.change_revision = inner.change_revision.saturating_add(1);
            inner.change_revision
        };
        self.persist_if_needed()?;
        if data_changed {
            self.notify_subscribers()?;
        }
        self.notify_change_subscribers(revision, data_changed)?;
        Ok(())
    }

    fn materialize(&self, anchor: &str, segments: &[String]) -> Result<Option<JsonValue>> {
        let mut inner = self.inner.lock().unwrap();
        match inner.resolve_cursor(anchor, segments)? {
            Some(Cursor::Node(node)) => Ok(Some(inner.materialize_node(
                &node,
                &node,
                &mut BTreeSet::new(),
            ))),
            Some(Cursor::Field { node, field }) => {
                let value = match inner
                    .nodes
                    .get(&node)
                    .and_then(|node_state| node_state.fields.get(&field))
                    .map(|field_state| field_state.value.clone())
                {
                    Some(value) => value,
                    None => return Ok(None),
                };
                Ok(Some(inner.materialize_field(&node, &field, &value, &mut BTreeSet::new())))
            }
            None => Ok(None),
        }
    }

    fn map_at(&self, anchor: &str, segments: &[String]) -> Result<Vec<MapEntry>> {
        let mut inner = self.inner.lock().unwrap();
        match inner.resolve_cursor(anchor, segments)? {
            Some(Cursor::Node(node)) => Ok(inner.map_node(&node)),
            Some(Cursor::Field { node, field }) => {
                let Some(value) = inner
                    .nodes
                    .get(&node)
                    .and_then(|node_state| node_state.fields.get(&field))
                    .map(|field_state| field_state.value.clone())
                else {
                    return Ok(Vec::new());
                };
                match &value {
                    FieldValue::Link(target) => Ok(inner.map_node(target)),
                    FieldValue::Set(set) => Ok(set
                        .members
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .into_iter()
                        .map(|member| MapEntry {
                            key: member.clone(),
                            value: inner.materialize_node(&member, &member, &mut BTreeSet::new()),
                        })
                        .collect()),
                    FieldValue::Scalar(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {
                        Ok(Vec::new())
                    }
                }
            }
            None => Ok(Vec::new()),
        }
    }

    fn query_at(
        &self,
        anchor: &str,
        segments: &[String],
        spec: &QuerySpec,
    ) -> Result<Vec<MapEntry>> {
        if let Some(entries) = self.query_with_storage_indexes(anchor, segments, spec)? {
            return Ok(entries);
        }

        let mut entries = filter_query_entries(self.map_at(anchor, segments)?, spec);

        if let Some(order) = &spec.order {
            sort_query_entries(&mut entries, order);
        }

        let offset = spec.offset.min(entries.len());
        if offset > 0 {
            entries.drain(0..offset);
        }
        if let Some(limit) = spec.limit {
            entries.truncate(limit);
        }

        Ok(entries)
    }

    fn query_with_storage_indexes(
        &self,
        anchor: &str,
        segments: &[String],
        spec: &QuerySpec,
    ) -> Result<Option<Vec<MapEntry>>> {
        let mut inner = self.inner.lock().unwrap();
        let Some(engine) = inner.storage_engine.clone() else {
            return Ok(None);
        };

        let Some(Cursor::Field { node, field }) = inner.resolve_cursor(anchor, segments)? else {
            return Ok(None);
        };
        let Some(node_state) = inner.nodes.get(&node).cloned() else {
            return Ok(None);
        };
        let Some(field_state) = node_state.fields.get(&field) else {
            return Ok(None);
        };
        let FieldValue::Set(set) = &field_state.value else {
            return Ok(None);
        };

        let indexed_path = spec
            .filters
            .iter()
            .find_map(indexed_filter_path)
            .or_else(|| spec.order.as_ref().and_then(indexed_order_path));
        let Some(indexed_path) = indexed_path else {
            return Ok(None);
        };

        let direction = spec
            .order
            .as_ref()
            .filter(|order| order.path == indexed_path)
            .map(|order| order.direction)
            .unwrap_or(QueryDirection::Asc);

        let candidate_ids: BTreeSet<_> = set.members.keys().cloned().collect();
        let indexed_filters: Vec<_> = spec
            .filters
            .iter()
            .filter(|filter| indexed_filter_path(filter).as_deref() == Some(indexed_path.as_str()))
            .collect();

        let mut ordered_member_ids = Vec::new();
        let mut seen = BTreeSet::new();
        for entry in engine.list_direct_index_entries(&indexed_path, direction)? {
            if !candidate_ids.contains(&entry.node_id) || !seen.insert(entry.node_id.clone()) {
                continue;
            }
            if indexed_filters
                .iter()
                .all(|filter| filter_matches_index_entry(filter, &entry.value))
            {
                ordered_member_ids.push(entry.node_id);
            }
        }

        let mut entries = Vec::new();
        for member_id in ordered_member_ids {
            entries.push(MapEntry {
                key: member_id.clone(),
                value: inner.materialize_node(&member_id, &member_id, &mut BTreeSet::new()),
            });
        }

        entries = filter_query_entries(entries, spec);
        if let Some(order) = &spec.order {
            if order.path != indexed_path {
                sort_query_entries(&mut entries, order);
            }
        }

        let offset = spec.offset.min(entries.len());
        if offset > 0 {
            entries.drain(0..offset);
        }
        if let Some(limit) = spec.limit {
            entries.truncate(limit);
        }

        Ok(Some(entries))
    }

    fn scan_at(&self, anchor: &str, segments: &[String], spec: &LexSpec) -> Result<Vec<LexEntry>> {
        let mut inner = self.inner.lock().unwrap();
        let mut entries = match inner.resolve_cursor(anchor, segments)? {
            Some(Cursor::Node(node)) => {
                let mut output = Vec::new();
                inner.collect_lex_from_node(
                    &node,
                    &display_path(anchor, segments),
                    spec,
                    spec.depth.max(1),
                    &mut output,
                );
                output
            }
            Some(Cursor::Field { node, field }) => {
                let mut output = Vec::new();
                inner.collect_lex_from_field(
                    &node,
                    &field,
                    &display_path(anchor, segments),
                    spec,
                    spec.depth.max(1),
                    &mut output,
                );
                output
            }
            None => Vec::new(),
        };

        if spec.reverse {
            entries.reverse();
        }
        if let Some(limit) = spec.limit {
            entries.truncate(limit);
        }
        Ok(entries)
    }

    fn subscribe_to(&self, anchor: &str, segments: &[String]) -> Result<Subscription> {
        let _ = {
            let mut inner = self.inner.lock().unwrap();
            inner.resolve_cursor(anchor, segments)?
        };

        let (sender, receiver) = async_channel::unbounded();
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_subscription_id = inner.next_subscription_id.saturating_add(1);
            let id = inner.next_subscription_id;
            inner.subscriptions.insert(
                id,
                Watcher {
                    anchor: anchor.to_owned(),
                    segments: segments.to_vec(),
                    sender: sender.clone(),
                },
            );
            id
        };

        let snapshot = self.materialize(anchor, segments)?;
        let _ = sender.try_send(snapshot);

        Ok(Subscription {
            inner: Arc::new(SubscriptionInner {
                id,
                db: Arc::downgrade(&self.inner),
                receiver,
            }),
        })
    }

    fn notify_subscribers(&self) -> Result<()> {
        let watchers: Vec<(u64, Watcher)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .subscriptions
                .iter()
                .map(|(id, watcher)| (*id, watcher.clone()))
                .collect()
        };

        let mut stale = Vec::new();
        for (id, watcher) in watchers {
            let snapshot = self
                .materialize(&watcher.anchor, &watcher.segments)
                .unwrap_or(None);
            if watcher.sender.try_send(snapshot).is_err() {
                stale.push(id);
            }
        }

        if !stale.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for id in stale {
                inner.subscriptions.remove(&id);
            }
        }

        Ok(())
    }

    fn notify_change_subscribers(&self, revision: u64, data_changed: bool) -> Result<()> {
        let (watchers, pending_ops): (Vec<(u64, ChangeWatcher)>, usize) = {
            let inner = self.inner.lock().unwrap();
            (
                inner
                    .change_subscriptions
                    .iter()
                    .map(|(id, watcher)| (*id, watcher.clone()))
                    .collect(),
                inner.pending_ops.len(),
            )
        };

        let mut stale = Vec::new();
        let event = ChangeEvent {
            revision,
            pending_ops,
            data_changed,
        };
        for (id, watcher) in watchers {
            if watcher.sender.try_send(event.clone()).is_err() {
                stale.push(id);
            }
        }

        if !stale.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for id in stale {
                inner.change_subscriptions.remove(&id);
            }
        }

        Ok(())
    }

    fn put_json(&self, anchor: &str, segments: &[String], value: JsonValue) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            if segments.is_empty() {
                let ParsedInput::Object(object) =
                    parse_input(value, &display_path(anchor, segments))?
                else {
                    return Err(PrimadbError::ExpectedObject {
                        path: display_path(anchor, segments),
                    });
                };
                inner.write_object_to_node(anchor, object, &display_path(anchor, segments))?;
            } else {
                let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)?
                else {
                    return Err(PrimadbError::ExpectedFieldPath {
                        path: display_path(anchor, segments),
                    });
                };
                inner.write_value_to_field(
                    &node,
                    &field,
                    value,
                    &display_path(anchor, segments),
                )?;
            }
        }
        self.finalize_change(true)
    }

    #[cfg(feature = "crypto")]
    fn put_signed_json(
        &self,
        anchor: &str,
        segments: &[String],
        value: JsonValue,
        certificate: Option<String>,
    ) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            if segments.is_empty() {
                let ParsedInput::Object(object) =
                    parse_input(value, &display_path(anchor, segments))?
                else {
                    return Err(PrimadbError::ExpectedObject {
                        path: display_path(anchor, segments),
                    });
                };
                inner.write_object_to_node_secure(
                    anchor,
                    object,
                    anchor,
                    certificate.as_deref(),
                )?;
            } else {
                let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)?
                else {
                    return Err(PrimadbError::ExpectedFieldPath {
                        path: display_path(anchor, segments),
                    });
                };
                inner.write_value_to_field_secure(
                    &node,
                    &field,
                    value,
                    &format!("{node}/{field}"),
                    certificate.as_deref(),
                )?;
            }
        }
        self.finalize_change(true)
    }

    fn unset(&self, anchor: &str, segments: &[String]) -> Result<()> {
        if segments.is_empty() {
            return Err(PrimadbError::ExpectedFieldPath {
                path: display_path(anchor, segments),
            });
        }

        {
            let mut inner = self.inner.lock().unwrap();
            let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)? else {
                return Err(PrimadbError::ExpectedFieldPath {
                    path: display_path(anchor, segments),
                });
            };
            inner.delete_field(&node, &field);
        }
        self.finalize_change(true)
    }

    fn set_json(&self, anchor: &str, segments: &[String], value: JsonValue) -> Result<String> {
        if segments.is_empty() {
            return Err(PrimadbError::ExpectedFieldPath {
                path: display_path(anchor, segments),
            });
        }

        let member_id = {
            let mut inner = self.inner.lock().unwrap();
            let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)? else {
                return Err(PrimadbError::ExpectedFieldPath {
                    path: display_path(anchor, segments),
                });
            };

            let parsed = parse_input(value, &display_path(anchor, segments))?;
            inner.add_member_to_set(&node, &field, parsed, &display_path(anchor, segments))?
        };
        self.finalize_change(true)?;
        Ok(member_id)
    }

    #[cfg(feature = "crypto")]
    fn set_signed_json(
        &self,
        anchor: &str,
        segments: &[String],
        value: JsonValue,
        certificate: Option<String>,
    ) -> Result<String> {
        if segments.is_empty() {
            return Err(PrimadbError::ExpectedFieldPath {
                path: display_path(anchor, segments),
            });
        }

        let member_id = {
            let mut inner = self.inner.lock().unwrap();
            let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)? else {
                return Err(PrimadbError::ExpectedFieldPath {
                    path: display_path(anchor, segments),
                });
            };

            let parsed = parse_input(value, &display_path(anchor, segments))?;
            inner.add_member_to_set_secure(
                &node,
                &field,
                parsed,
                &format!("{node}/{field}"),
                certificate.as_deref(),
            )?
        };
        self.finalize_change(true)?;
        Ok(member_id)
    }

    fn remove_json(&self, anchor: &str, segments: &[String], value: JsonValue) -> Result<String> {
        if segments.is_empty() {
            return Err(PrimadbError::ExpectedFieldPath {
                path: display_path(anchor, segments),
            });
        }

        let member_id = {
            let mut inner = self.inner.lock().unwrap();
            let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)? else {
                return Err(PrimadbError::ExpectedFieldPath {
                    path: display_path(anchor, segments),
                });
            };

            let member_id = parse_member_reference(value, &display_path(anchor, segments))?;
            inner.remove_member_from_set(&node, &field, &member_id);
            member_id
        };
        self.finalize_change(true)?;
        Ok(member_id)
    }
}

impl Default for Primadb {
    fn default() -> Self {
        Self::with_replica_id(HybridClock::default_actor())
    }
}

impl Chain {
    pub fn field(&self, key: impl Into<String>) -> Self {
        let mut segments = self.segments.clone();
        segments.push(key.into());
        Self {
            db: self.db.clone(),
            anchor: self.anchor.clone(),
            segments,
        }
    }

    pub fn path(&self) -> String {
        display_path(&self.anchor, &self.segments)
    }

    pub fn put<T: Serialize>(&self, value: T) -> Result<()> {
        self.db
            .put_json(&self.anchor, &self.segments, serde_json::to_value(value)?)
    }

    pub fn put_bytes(&self, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.db.put_json(
            &self.anchor,
            &self.segments,
            bytes_marker_value(&BinaryBytes::from(bytes.as_ref())),
        )
    }

    #[cfg(feature = "crypto")]
    pub fn put_signed<T: Serialize>(&self, value: T, certificate: Option<String>) -> Result<()> {
        self.db.put_signed_json(
            &self.anchor,
            &self.segments,
            serde_json::to_value(value)?,
            certificate,
        )
    }

    pub fn once_json(&self) -> Result<Option<JsonValue>> {
        self.db.materialize(&self.anchor, &self.segments)
    }

    pub fn once_bytes(&self) -> Result<Option<Vec<u8>>> {
        let Some(value) = self.once_json()? else {
            return Ok(None);
        };
        let ParsedInput::Bytes(bytes) = parse_input(value, &self.path())? else {
            return Ok(None);
        };
        Ok(Some(bytes.into_inner()))
    }

    pub fn unset(&self) -> Result<()> {
        self.db.unset(&self.anchor, &self.segments)
    }

    pub fn set<T: Serialize>(&self, value: T) -> Result<String> {
        self.db
            .set_json(&self.anchor, &self.segments, serde_json::to_value(value)?)
    }

    pub fn set_bytes(&self, bytes: impl AsRef<[u8]>) -> Result<String> {
        self.db.set_json(
            &self.anchor,
            &self.segments,
            bytes_marker_value(&BinaryBytes::from(bytes.as_ref())),
        )
    }

    #[cfg(feature = "crypto")]
    pub fn set_signed<T: Serialize>(
        &self,
        value: T,
        certificate: Option<String>,
    ) -> Result<String> {
        self.db.set_signed_json(
            &self.anchor,
            &self.segments,
            serde_json::to_value(value)?,
            certificate,
        )
    }

    pub fn remove<T: Serialize>(&self, value: T) -> Result<String> {
        self.db
            .remove_json(&self.anchor, &self.segments, serde_json::to_value(value)?)
    }

    pub fn put_blob(
        &self,
        data: impl AsRef<[u8]>,
        media_type: Option<&str>,
    ) -> Result<BlobRef> {
        let reference = self.db.store_blob(data.as_ref(), media_type)?;
        self.db.put_json(
            &self.anchor,
            &self.segments,
            blob_marker_value(&reference),
        )?;
        Ok(reference)
    }

    pub fn once_blob_ref(&self) -> Result<Option<BlobRef>> {
        let Some(value) = self.once_json()? else {
            return Ok(None);
        };
        let ParsedInput::Blob(reference) = parse_input(value, &self.path())? else {
            return Ok(None);
        };
        Ok(Some(reference))
    }

    pub fn get_blob(&self) -> Result<Option<StoredBlob>> {
        let Some(reference) = self.once_blob_ref()? else {
            return Ok(None);
        };
        self.db.get_blob(&reference.id)
    }

    pub fn map(&self) -> Result<Vec<MapEntry>> {
        self.db.map_at(&self.anchor, &self.segments)
    }

    pub fn lex(&self) -> LexBuilder {
        LexBuilder {
            chain: self.clone(),
            spec: LexSpec::default(),
        }
    }

    pub fn scan(&self, spec: LexSpec) -> Result<Vec<LexEntry>> {
        self.db.scan_at(&self.anchor, &self.segments, &spec)
    }

    pub fn find(&self) -> QueryBuilder {
        QueryBuilder {
            chain: self.clone(),
            spec: QuerySpec::default(),
        }
    }

    pub fn query(&self, spec: QuerySpec) -> Result<Vec<MapEntry>> {
        self.db.query_at(&self.anchor, &self.segments, &spec)
    }

    pub fn first(&self, spec: QuerySpec) -> Result<Option<MapEntry>> {
        let mut entries = self.query(spec)?;
        Ok(entries.drain(..).next())
    }

    pub fn subscribe(&self) -> Result<Subscription> {
        self.db.subscribe_to(&self.anchor, &self.segments)
    }
}

impl QueryBuilder {
    pub fn where_eq(mut self, path: impl Into<String>, value: impl Serialize) -> Result<Self> {
        self.spec.filters.push(QueryFilter::Eq {
            path: path.into(),
            value: serde_json::to_value(value)?,
        });
        Ok(self)
    }

    pub fn where_ne(mut self, path: impl Into<String>, value: impl Serialize) -> Result<Self> {
        self.spec.filters.push(QueryFilter::Ne {
            path: path.into(),
            value: serde_json::to_value(value)?,
        });
        Ok(self)
    }

    pub fn where_gt(mut self, path: impl Into<String>, value: impl Serialize) -> Result<Self> {
        self.spec.filters.push(QueryFilter::Gt {
            path: path.into(),
            value: serde_json::to_value(value)?,
        });
        Ok(self)
    }

    pub fn where_gte(mut self, path: impl Into<String>, value: impl Serialize) -> Result<Self> {
        self.spec.filters.push(QueryFilter::Gte {
            path: path.into(),
            value: serde_json::to_value(value)?,
        });
        Ok(self)
    }

    pub fn where_lt(mut self, path: impl Into<String>, value: impl Serialize) -> Result<Self> {
        self.spec.filters.push(QueryFilter::Lt {
            path: path.into(),
            value: serde_json::to_value(value)?,
        });
        Ok(self)
    }

    pub fn where_lte(mut self, path: impl Into<String>, value: impl Serialize) -> Result<Self> {
        self.spec.filters.push(QueryFilter::Lte {
            path: path.into(),
            value: serde_json::to_value(value)?,
        });
        Ok(self)
    }

    pub fn where_prefix(mut self, path: impl Into<String>, value: impl Into<String>) -> Self {
        self.spec.filters.push(QueryFilter::Prefix {
            path: path.into(),
            value: value.into(),
        });
        self
    }

    pub fn where_contains(mut self, path: impl Into<String>, value: impl Into<String>) -> Self {
        self.spec.filters.push(QueryFilter::Contains {
            path: path.into(),
            value: value.into(),
        });
        self
    }

    pub fn where_exists(mut self, path: impl Into<String>) -> Self {
        self.spec
            .filters
            .push(QueryFilter::Exists { path: path.into() });
        self
    }

    pub fn order_by(mut self, path: impl Into<String>, direction: QueryDirection) -> Self {
        self.spec.order = Some(crate::query::QueryOrder {
            path: path.into(),
            direction,
        });
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.spec.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.spec.offset = offset;
        self
    }

    pub fn spec(&self) -> &QuerySpec {
        &self.spec
    }

    pub fn run(&self) -> Result<Vec<MapEntry>> {
        self.chain.query(self.spec.clone())
    }

    pub fn first(&self) -> Result<Option<MapEntry>> {
        self.chain.first(self.spec.clone())
    }
}

impl LexBuilder {
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.spec.prefix = Some(prefix.into());
        self
    }

    pub fn start_at(mut self, key: impl Into<String>) -> Self {
        self.spec.start_at = Some(key.into());
        self
    }

    pub fn start_after(mut self, key: impl Into<String>) -> Self {
        self.spec.start_after = Some(key.into());
        self
    }

    pub fn end_at(mut self, key: impl Into<String>) -> Self {
        self.spec.end_at = Some(key.into());
        self
    }

    pub fn end_before(mut self, key: impl Into<String>) -> Self {
        self.spec.end_before = Some(key.into());
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.spec.limit = Some(limit);
        self
    }

    pub fn reverse(mut self, reverse: bool) -> Self {
        self.spec.reverse = reverse;
        self
    }

    pub fn depth(mut self, depth: usize) -> Self {
        self.spec.depth = depth.max(1);
        self
    }

    pub fn follow_links(mut self, follow_links: bool) -> Self {
        self.spec.follow_links = follow_links;
        self
    }

    pub fn spec(&self) -> &LexSpec {
        &self.spec
    }

    pub fn run(&self) -> Result<Vec<LexEntry>> {
        self.chain.scan(self.spec.clone())
    }
}

impl Subscription {
    pub fn receiver(&self) -> Receiver<Option<JsonValue>> {
        self.inner.receiver.clone()
    }

    pub async fn recv(&self) -> Option<Option<JsonValue>> {
        self.inner.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<Option<JsonValue>> {
        self.inner.receiver.try_recv().ok()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<Option<JsonValue>> {
        self.inner.receiver.recv_blocking().ok()
    }
}

impl ChangeSubscription {
    pub fn receiver(&self) -> Receiver<ChangeEvent> {
        self.inner.receiver.clone()
    }

    pub async fn recv(&self) -> Option<ChangeEvent> {
        self.inner.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<ChangeEvent> {
        self.inner.receiver.try_recv().ok()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<ChangeEvent> {
        self.inner.receiver.recv_blocking().ok()
    }
}

impl Clone for Subscription {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Clone for ChangeSubscription {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for SubscriptionInner {
    fn drop(&mut self) {
        if let Some(db) = self.db.upgrade() {
            if let Ok(mut inner) = db.lock() {
                inner.subscriptions.remove(&self.id);
            }
        }
    }
}

impl Drop for ChangeSubscriptionInner {
    fn drop(&mut self) {
        if let Some(db) = self.db.upgrade() {
            if let Ok(mut inner) = db.lock() {
                inner.change_subscriptions.remove(&self.id);
            }
        }
    }
}

impl Inner {
    fn maybe_load_node(&mut self, node: &str) -> Result<bool> {
        if self.nodes.contains_key(node) {
            return Ok(true);
        }
        if self.missing_nodes.contains(node) {
            return Ok(false);
        }
        let Some(engine) = self.storage_engine.clone() else {
            return Ok(false);
        };
        match engine.get_node(node)? {
            Some(node_state) => {
                self.nodes.insert(node.to_owned(), node_state);
                self.missing_nodes.remove(node);
                Ok(true)
            }
            None => {
                self.missing_nodes.insert(node.to_owned());
                Ok(false)
            }
        }
    }

    fn ensure_node(&mut self, node: &str) {
        self.missing_nodes.remove(node);
        self.nodes
            .entry(node.to_owned())
            .or_insert_with(|| NodeState::new(node.to_owned()));
    }

    fn ensure_field_cursor(&mut self, anchor: &str, segments: &[String]) -> Result<Cursor> {
        if segments.is_empty() {
            return Ok(Cursor::Node(anchor.to_owned()));
        }

        if !self.maybe_load_node(anchor)? {
            self.ensure_node(anchor);
        }
        let mut current = anchor.to_owned();
        for segment in &segments[..segments.len().saturating_sub(1)] {
            let _ = self.maybe_load_node(&current)?;
            let next = match self
                .nodes
                .get(&current)
                .and_then(|node| node.fields.get(segment))
            {
                Some(FieldState {
                    value: FieldValue::Link(target),
                    ..
                }) => target.clone(),
                Some(FieldState {
                    value: FieldValue::Scalar(_),
                    ..
                })
                | Some(FieldState {
                    value: FieldValue::Bytes(_),
                    ..
                })
                | Some(FieldState {
                    value: FieldValue::Blob(_),
                    ..
                }) => {
                    return Err(PrimadbError::TraversalIntoScalar {
                        node: current,
                        field: segment.clone(),
                    });
                }
                Some(FieldState {
                    value: FieldValue::Set(_),
                    ..
                }) => {
                    return Err(PrimadbError::TraversalIntoSet {
                        node: current,
                        field: segment.clone(),
                    });
                }
                None => {
                    let child = derived_child_id(&current, segment);
                    self.set_field(
                        current.clone(),
                        segment.clone(),
                        OperationValue::Link(child.clone()),
                    );
                    child
                }
            };
            self.ensure_node(&next);
            current = next;
        }

        Ok(Cursor::Field {
            node: current,
            field: segments.last().cloned().unwrap_or_default(),
        })
    }

    fn resolve_cursor(&mut self, anchor: &str, segments: &[String]) -> Result<Option<Cursor>> {
        if segments.is_empty() {
            return Ok(self
                .maybe_load_node(anchor)?
                .then(|| Cursor::Node(anchor.to_owned())));
        }

        let mut current = anchor.to_owned();
        if !self.maybe_load_node(&current)? {
            return Ok(None);
        }
        for segment in &segments[..segments.len().saturating_sub(1)] {
            if !self.maybe_load_node(&current)? {
                return Ok(None);
            }
            let Some(node) = self.nodes.get(&current) else {
                return Ok(None);
            };
            let Some(field) = node.fields.get(segment) else {
                return Ok(None);
            };
            match &field.value {
                FieldValue::Link(target) => current = target.clone(),
                FieldValue::Scalar(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {
                    return Err(PrimadbError::TraversalIntoScalar {
                        node: current,
                        field: segment.clone(),
                    });
                }
                FieldValue::Set(_) => {
                    return Err(PrimadbError::TraversalIntoSet {
                        node: current,
                        field: segment.clone(),
                    });
                }
            }
        }

        if !self.maybe_load_node(&current)? {
            return Ok(None);
        }

        Ok(Some(Cursor::Field {
            node: current,
            field: segments.last().cloned().unwrap_or_default(),
        }))
    }

    fn write_object_to_node(
        &mut self,
        node: &str,
        object: Map<String, JsonValue>,
        path: &str,
    ) -> Result<()> {
        self.ensure_node(node);
        for (field, value) in object {
            let field_path = if path.is_empty() {
                field.clone()
            } else {
                format!("{path}.{field}")
            };
            self.write_value_to_field(node, &field, value, &field_path)?;
        }
        Ok(())
    }

    #[cfg(feature = "crypto")]
    fn write_object_to_node_secure(
        &mut self,
        node: &str,
        object: Map<String, JsonValue>,
        path: &str,
        certificate: Option<&str>,
    ) -> Result<()> {
        self.ensure_node(node);
        for (field, value) in object {
            let field_path = if path.is_empty() {
                format!("{node}/{field}")
            } else {
                format!("{path}/{field}")
            };
            self.write_value_to_field_secure(node, &field, value, &field_path, certificate)?;
        }
        Ok(())
    }

    fn write_value_to_field(
        &mut self,
        node: &str,
        field: &str,
        value: JsonValue,
        path: &str,
    ) -> Result<()> {
        match parse_input(value, path)? {
            ParsedInput::Scalar(scalar) => {
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Scalar(scalar),
                );
            }
            ParsedInput::Bytes(bytes) => {
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Bytes(bytes),
                );
            }
            ParsedInput::Blob(reference) => {
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Blob(reference),
                );
            }
            ParsedInput::Link(target) => {
                self.ensure_node(&target);
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Link(target),
                );
            }
            ParsedInput::Object(object) => {
                let existing_link = self
                    .nodes
                    .get(node)
                    .and_then(|state| state.fields.get(field))
                    .and_then(|state| match &state.value {
                        FieldValue::Link(target) => Some(target.clone()),
                        _ => None,
                    });
                let child = existing_link.unwrap_or_else(|| derived_child_id(node, field));
                self.ensure_node(&child);
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Link(child.clone()),
                );
                self.write_object_to_node(&child, object, path)?;
            }
            ParsedInput::Set(members) => {
                let mut ids = Vec::new();
                for (index, member) in members.into_iter().enumerate() {
                    match member {
                        SetMember::Link(target) => {
                            self.ensure_node(&target);
                            ids.push(target);
                        }
                        SetMember::Object(object) => {
                            let member_id = self.clock.next_node_id(&format!("{field}-member"));
                            self.ensure_node(&member_id);
                            self.write_object_to_node(
                                &member_id,
                                object,
                                &format!("{path}.$set[{index}]"),
                            )?;
                            ids.push(member_id);
                        }
                    }
                }
                self.set_field(node.to_owned(), field.to_owned(), OperationValue::Set(ids));
            }
        }
        Ok(())
    }

    #[cfg(feature = "crypto")]
    fn write_value_to_field_secure(
        &mut self,
        node: &str,
        field: &str,
        value: JsonValue,
        path: &str,
        certificate: Option<&str>,
    ) -> Result<()> {
        match parse_input(value, path)? {
            ParsedInput::Scalar(scalar) => {
                let scalar = self.sign_scalar_for_path(path, scalar, certificate)?;
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Scalar(scalar),
                );
            }
            ParsedInput::Bytes(bytes) => {
                let signed = self.sign_scalar_for_path(
                    path,
                    bytes_marker_value(&bytes),
                    certificate,
                )?;
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Scalar(signed),
                );
            }
            ParsedInput::Blob(reference) => {
                let signed = self.sign_scalar_for_path(
                    path,
                    blob_marker_value(&reference),
                    certificate,
                )?;
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Scalar(signed),
                );
            }
            ParsedInput::Link(target) => {
                self.ensure_node(&target);
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Link(target),
                );
            }
            ParsedInput::Object(object) => {
                let existing_link = self
                    .nodes
                    .get(node)
                    .and_then(|state| state.fields.get(field))
                    .and_then(|state| match &state.value {
                        FieldValue::Link(target) => Some(target.clone()),
                        _ => None,
                    });
                let child = existing_link.unwrap_or_else(|| derived_child_id(node, field));
                self.ensure_node(&child);
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Link(child.clone()),
                );
                self.write_object_to_node_secure(&child, object, &child, certificate)?;
            }
            ParsedInput::Set(members) => {
                let mut ids = Vec::new();
                for member in members {
                    match member {
                        SetMember::Link(target) => {
                            self.ensure_node(&target);
                            ids.push(target);
                        }
                        SetMember::Object(object) => {
                            let member_id = self.allocate_member_id_for_path(path, field);
                            self.ensure_node(&member_id);
                            self.write_object_to_node_secure(
                                &member_id,
                                object,
                                &member_id,
                                certificate,
                            )?;
                            ids.push(member_id);
                        }
                    }
                }
                self.set_field(node.to_owned(), field.to_owned(), OperationValue::Set(ids));
            }
        }
        Ok(())
    }

    fn add_member_to_set(
        &mut self,
        node: &str,
        field: &str,
        parsed: ParsedInput,
        path: &str,
    ) -> Result<String> {
        let member_id = match parsed {
            ParsedInput::Link(target) => {
                self.ensure_node(&target);
                target
            }
            ParsedInput::Object(object) => {
                let member_id = self.clock.next_node_id(&format!("{field}-member"));
                self.ensure_node(&member_id);
                self.write_object_to_node(&member_id, object, path)?;
                member_id
            }
            ParsedInput::Scalar(_) | ParsedInput::Bytes(_) | ParsedInput::Blob(_) | ParsedInput::Set(_) => {
                return Err(PrimadbError::InvalidSetMember {
                    path: path.to_owned(),
                });
            }
        };

        self.add_set_member(node.to_owned(), field.to_owned(), member_id.clone());
        Ok(member_id)
    }

    #[cfg(feature = "crypto")]
    fn add_member_to_set_secure(
        &mut self,
        node: &str,
        field: &str,
        parsed: ParsedInput,
        path: &str,
        certificate: Option<&str>,
    ) -> Result<String> {
        let member_id = match parsed {
            ParsedInput::Link(target) => {
                self.ensure_node(&target);
                target
            }
            ParsedInput::Object(object) => {
                let member_id = self.allocate_member_id_for_path(path, field);
                self.ensure_node(&member_id);
                self.write_object_to_node_secure(&member_id, object, &member_id, certificate)?;
                member_id
            }
            ParsedInput::Scalar(_) | ParsedInput::Bytes(_) | ParsedInput::Blob(_) | ParsedInput::Set(_) => {
                return Err(PrimadbError::InvalidSetMember {
                    path: path.to_owned(),
                });
            }
        };

        self.add_set_member(node.to_owned(), field.to_owned(), member_id.clone());
        Ok(member_id)
    }

    #[cfg(feature = "crypto")]
    fn allocate_member_id_for_path(&mut self, path: &str, field: &str) -> String {
        match owner_public_key_for_path(path) {
            Some(owner_pub) => format!(
                "~{owner_pub}/{}",
                self.clock.next_node_id(&format!("{field}-member"))
            ),
            None => self.clock.next_node_id(&format!("{field}-member")),
        }
    }

    #[cfg(feature = "crypto")]
    fn sign_scalar_for_path(
        &self,
        path: &str,
        scalar: JsonValue,
        certificate: Option<&str>,
    ) -> Result<JsonValue> {
        let Some(owner_pub) = owner_public_key_for_path(path) else {
            return Ok(scalar);
        };
        let local_pub = self.security.local_public_key().ok_or_else(|| {
            PrimadbError::Crypto(format!(
                "writing owned path `{path}` requires an authenticated local user"
            ))
        })?;
        if local_pub != owner_pub && certificate.is_none() {
            return Err(PrimadbError::Crypto(format!(
                "writing owned path `{path}` requires the owner or a valid certificate"
            )));
        }
        self.security
            .sign_data_value(path, scalar, certificate.map(str::to_owned))
    }

    fn delete_field(&mut self, node: &str, field: &str) {
        let revision = self.clock.next_revision();
        let op_id = self.clock.next_op_id("delete");
        let op = Operation {
            author: self.clock.actor().to_owned(),
            revision,
            op_id,
            action: OperationAction::DeleteField {
                node: node.to_owned(),
                field: field.to_owned(),
            },
        };
        self.apply_operation_internal(op, OperationOrigin::Local);
    }

    fn set_field(&mut self, node: NodeId, field: String, value: OperationValue) {
        let revision = self.clock.next_revision();
        let op_id = self.clock.next_op_id("set");
        let op = Operation {
            author: self.clock.actor().to_owned(),
            revision,
            op_id,
            action: OperationAction::SetField { node, field, value },
        };
        self.apply_operation_internal(op, OperationOrigin::Local);
    }

    fn add_set_member(&mut self, node: NodeId, field: String, member: NodeId) {
        let revision = self.clock.next_revision();
        let op_id = self.clock.next_op_id("set-add");
        let op = Operation {
            author: self.clock.actor().to_owned(),
            revision,
            op_id,
            action: OperationAction::AddSetMember {
                node,
                field,
                member,
            },
        };
        self.apply_operation_internal(op, OperationOrigin::Local);
    }

    fn remove_member_from_set(&mut self, node: &str, field: &str, member: &str) {
        let revision = self.clock.next_revision();
        let op_id = self.clock.next_op_id("set-remove");
        let op = Operation {
            author: self.clock.actor().to_owned(),
            revision,
            op_id,
            action: OperationAction::RemoveSetMember {
                node: node.to_owned(),
                field: field.to_owned(),
                member: member.to_owned(),
            },
        };
        self.apply_operation_internal(op, OperationOrigin::Local);
    }

    fn apply_operation_internal(&mut self, op: Operation, origin: OperationOrigin) -> bool {
        if origin == OperationOrigin::Local && self.pending_ops.len() >= self.limits.max_pending_ops
        {
            return false;
        }
        self.clock.observe(&op.revision);
        let marker = VersionMarker {
            revision: op.revision.clone(),
            op_id: op.op_id.clone(),
        };

        let accepted = match &op.action {
            OperationAction::SetField { node, field, value } => {
                let links_to_ensure: Vec<NodeId> = match value {
                    OperationValue::Scalar(_)
                    | OperationValue::Bytes(_)
                    | OperationValue::Blob(_) => Vec::new(),
                    OperationValue::Link(target) => vec![target.clone()],
                    OperationValue::Set(members) => members.clone(),
                };
                for target in &links_to_ensure {
                    self.ensure_node(target);
                }

                let state = self
                    .nodes
                    .entry(node.clone())
                    .or_insert_with(|| NodeState::new(node.clone()));

                let tombstone_blocks = state
                    .tombstones
                    .get(field)
                    .map(|current| marker <= *current)
                    .unwrap_or(false);
                let field_blocks = state
                    .fields
                    .get(field)
                    .map(|current| marker <= current.version)
                    .unwrap_or(false);

                if tombstone_blocks || field_blocks {
                    false
                } else {
                    state.tombstones.remove(field);
                    let value = match value {
                        OperationValue::Scalar(value) => FieldValue::Scalar(value.clone()),
                        OperationValue::Bytes(bytes) => FieldValue::Bytes(bytes.clone()),
                        OperationValue::Blob(reference) => FieldValue::Blob(reference.clone()),
                        OperationValue::Link(target) => FieldValue::Link(target.clone()),
                        OperationValue::Set(members) => FieldValue::Set(SetState {
                            baseline: marker.clone(),
                            members: members
                                .iter()
                                .cloned()
                                .map(|member| (member, marker.clone()))
                                .collect(),
                            removed: BTreeMap::new(),
                        }),
                    };
                    state.fields.insert(
                        field.clone(),
                        FieldState {
                            value,
                            version: marker,
                        },
                    );
                    true
                }
            }
            OperationAction::AddSetMember {
                node,
                field,
                member,
            } => {
                self.ensure_node(member);
                let state = self
                    .nodes
                    .entry(node.clone())
                    .or_insert_with(|| NodeState::new(node.clone()));

                let tombstone_blocks = state
                    .tombstones
                    .get(field)
                    .map(|current| marker <= *current)
                    .unwrap_or(false);
                if tombstone_blocks {
                    false
                } else {
                    match state.fields.get_mut(field) {
                        Some(current) => match &mut current.value {
                            FieldValue::Set(set) => {
                                if marker <= set.baseline {
                                    false
                                } else {
                                    let member_blocks = set
                                        .members
                                        .get(member)
                                        .map(|current| marker <= *current)
                                        .unwrap_or(false);
                                    let removal_blocks = set
                                        .removed
                                        .get(member)
                                        .map(|current| marker <= *current)
                                        .unwrap_or(false);
                                    if member_blocks || removal_blocks {
                                        false
                                    } else {
                                        set.members.insert(member.clone(), marker.clone());
                                        set.removed.remove(member);
                                        if marker > current.version {
                                            current.version = marker;
                                        }
                                        true
                                    }
                                }
                            }
                            _ => {
                                if marker <= current.version {
                                    false
                                } else {
                                    let mut members = BTreeMap::new();
                                    members.insert(member.clone(), marker.clone());
                                    current.value = FieldValue::Set(SetState {
                                        baseline: marker.clone(),
                                        members,
                                        removed: BTreeMap::new(),
                                    });
                                    current.version = marker;
                                    true
                                }
                            }
                        },
                        None => {
                            let mut members = BTreeMap::new();
                            members.insert(member.clone(), marker.clone());
                            state.fields.insert(
                                field.clone(),
                                FieldState {
                                    value: FieldValue::Set(SetState {
                                        baseline: zero_marker(),
                                        members,
                                        removed: BTreeMap::new(),
                                    }),
                                    version: marker,
                                },
                            );
                            true
                        }
                    }
                }
            }
            OperationAction::RemoveSetMember {
                node,
                field,
                member,
            } => {
                let state = self
                    .nodes
                    .entry(node.clone())
                    .or_insert_with(|| NodeState::new(node.clone()));

                let tombstone_blocks = state
                    .tombstones
                    .get(field)
                    .map(|current| marker <= *current)
                    .unwrap_or(false);
                if tombstone_blocks {
                    false
                } else {
                    match state.fields.get_mut(field) {
                        Some(current) => match &mut current.value {
                            FieldValue::Set(set) => {
                                if marker <= set.baseline {
                                    false
                                } else {
                                    let member_blocks = set
                                        .members
                                        .get(member)
                                        .map(|current| marker <= *current)
                                        .unwrap_or(false);
                                    let removal_blocks = set
                                        .removed
                                        .get(member)
                                        .map(|current| marker <= *current)
                                        .unwrap_or(false);
                                    if member_blocks || removal_blocks {
                                        false
                                    } else {
                                        set.members.remove(member);
                                        set.removed.insert(member.clone(), marker.clone());
                                        if marker > current.version {
                                            current.version = marker;
                                        }
                                        true
                                    }
                                }
                            }
                            _ => {
                                if marker <= current.version {
                                    false
                                } else {
                                    let mut removed = BTreeMap::new();
                                    removed.insert(member.clone(), marker.clone());
                                    current.value = FieldValue::Set(SetState {
                                        baseline: zero_marker(),
                                        members: BTreeMap::new(),
                                        removed,
                                    });
                                    current.version = marker;
                                    true
                                }
                            }
                        },
                        None => {
                            let mut removed = BTreeMap::new();
                            removed.insert(member.clone(), marker.clone());
                            state.fields.insert(
                                field.clone(),
                                FieldState {
                                    value: FieldValue::Set(SetState {
                                        baseline: zero_marker(),
                                        members: BTreeMap::new(),
                                        removed,
                                    }),
                                    version: marker,
                                },
                            );
                            true
                        }
                    }
                }
            }
            OperationAction::DeleteField { node, field } => {
                let state = self
                    .nodes
                    .entry(node.clone())
                    .or_insert_with(|| NodeState::new(node.clone()));
                let field_blocks = state
                    .fields
                    .get(field)
                    .map(|current| marker <= current.version)
                    .unwrap_or(false);
                let tombstone_blocks = state
                    .tombstones
                    .get(field)
                    .map(|current| marker <= *current)
                    .unwrap_or(false);
                if field_blocks || tombstone_blocks {
                    false
                } else {
                    state.fields.remove(field);
                    state.tombstones.insert(field.clone(), marker);
                    true
                }
            }
        };

        if accepted {
            self.unflushed_ops.push(op.clone());
        }

        if accepted && origin == OperationOrigin::Local {
            self.pending_ops.push(op);
        }

        accepted
    }

    fn materialize_node(
        &mut self,
        node: &str,
        _path: &str,
        visited: &mut BTreeSet<NodeId>,
    ) -> JsonValue {
        if !visited.insert(node.to_owned()) {
            return JsonValue::Object(Map::from_iter([(
                "$ref".to_owned(),
                JsonValue::String(node.to_owned()),
            )]));
        }

        let _ = self.maybe_load_node(node);

        let output = if let Some(state) = self.nodes.get(node).cloned() {
            let mut object = Map::new();
            object.insert("$id".to_owned(), JsonValue::String(state.id));
            for (field, state) in state.fields {
                object.insert(
                    field.clone(),
                    self.materialize_field(node, &field, &state.value, visited),
                );
            }
            JsonValue::Object(object)
        } else {
            JsonValue::Object(Map::from_iter([(
                "$ref".to_owned(),
                JsonValue::String(node.to_owned()),
            )]))
        };

        visited.remove(node);
        output
    }

    fn materialize_field(
        &mut self,
        node: &str,
        field: &str,
        value: &FieldValue,
        visited: &mut BTreeSet<NodeId>,
    ) -> JsonValue {
        let field_path = format!("{node}/{field}");
        match value {
            FieldValue::Scalar(value) => self.materialize_scalar(&field_path, value),
            FieldValue::Bytes(bytes) => bytes_marker_value(bytes),
            FieldValue::Blob(reference) => blob_marker_value(reference),
            FieldValue::Link(target) => self.materialize_node(target, target, visited),
            FieldValue::Set(set) => JsonValue::Object(Map::from_iter([(
                "$set".to_owned(),
                JsonValue::Array(
                    set.members
                        .keys()
                        .map(|member| self.materialize_node(member, member, visited))
                        .collect(),
                ),
            )])),
        }
    }

    fn materialize_scalar(&self, path: &str, value: &JsonValue) -> JsonValue {
        #[cfg(feature = "crypto")]
        {
            return match self.security.verify_data_value(path, value) {
                Ok(Some(value)) => value,
                Ok(None) | Err(_) => JsonValue::Null,
            };
        }

        #[cfg(not(feature = "crypto"))]
        {
            let _ = path;
            value.clone()
        }
    }

    fn map_node(&mut self, node: &str) -> Vec<MapEntry> {
        let _ = self.maybe_load_node(node);
        self.nodes
            .get(node)
            .cloned()
            .map(|state| {
                state
                    .fields
                    .into_iter()
                    .map(|(field, state)| MapEntry {
                        key: field.clone(),
                        value: self.materialize_field(
                            node,
                            &field,
                            &state.value,
                            &mut BTreeSet::new(),
                        ),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn collect_lex_from_node(
        &mut self,
        node: &str,
        base_path: &str,
        spec: &LexSpec,
        remaining_depth: usize,
        output: &mut Vec<LexEntry>,
    ) {
        let _ = self.maybe_load_node(node);
        let Some(state) = self.nodes.get(node).cloned() else {
            return;
        };
        for (field, field_state) in state.fields {
            if !lex_key_matches(&field, spec) {
                continue;
            }

            let path = if base_path.is_empty() {
                field.clone()
            } else {
                format!("{base_path}.{field}")
            };
            output.push(LexEntry {
                path: path.clone(),
                key: field.clone(),
                value: self.materialize_field(
                    node,
                    &field,
                    &field_state.value,
                    &mut BTreeSet::new(),
                ),
            });

            if !spec.follow_links || remaining_depth <= 1 {
                continue;
            }

            match &field_state.value {
                FieldValue::Link(target) => {
                    self.collect_lex_from_node(target, &path, spec, remaining_depth - 1, output);
                }
                FieldValue::Set(set) => {
                    for member in set.members.keys() {
                        self.collect_lex_from_node(
                            member,
                            &format!("{path}.{member}"),
                            spec,
                            remaining_depth - 1,
                            output,
                        );
                    }
                }
                FieldValue::Scalar(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {}
            }
        }
    }

    fn collect_lex_from_field(
        &mut self,
        node: &str,
        field: &str,
        base_path: &str,
        spec: &LexSpec,
        remaining_depth: usize,
        output: &mut Vec<LexEntry>,
    ) {
        let _ = self.maybe_load_node(node);
        let Some(state) = self.nodes.get(node).cloned() else {
            return;
        };
        let Some(field_state) = state.fields.get(field) else {
            return;
        };

        match &field_state.value {
            FieldValue::Link(target) => {
                self.collect_lex_from_node(target, base_path, spec, remaining_depth, output);
            }
            FieldValue::Set(set) => {
                for member in set.members.keys() {
                    if !lex_key_matches(member, spec) {
                        continue;
                    }
                    let path = if base_path.is_empty() {
                        member.clone()
                    } else {
                        format!("{base_path}.{member}")
                    };
                    output.push(LexEntry {
                        path: path.clone(),
                        key: member.clone(),
                        value: self.materialize_node(member, member, &mut BTreeSet::new()),
                    });
                    if spec.follow_links && remaining_depth > 1 {
                        self.collect_lex_from_node(
                            member,
                            &path,
                            spec,
                            remaining_depth - 1,
                            output,
                        );
                    }
                }
            }
            FieldValue::Scalar(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {}
        }
    }
}

fn display_path(anchor: &str, segments: &[String]) -> String {
    if segments.is_empty() {
        anchor.to_owned()
    } else {
        format!("{anchor}.{}", segments.join("."))
    }
}

fn parse_input(value: JsonValue, path: &str) -> Result<ParsedInput> {
    match value {
        JsonValue::Object(object) => {
            if let Some(bytes) = parse_bytes_marker(&object, path)? {
                return Ok(ParsedInput::Bytes(bytes));
            }
            if let Some(reference) = parse_blob_marker(&object, path)? {
                return Ok(ParsedInput::Blob(reference));
            }
            if let Some(target) = parse_link_marker(&object) {
                return Ok(ParsedInput::Link(target));
            }
            if let Some(items) = parse_set_marker(&object) {
                let mut members = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        JsonValue::Object(object) => {
                            if let Some(target) = parse_link_marker(&object) {
                                members.push(SetMember::Link(target));
                            } else {
                                members.push(SetMember::Object(object));
                            }
                        }
                        _ => {
                            return Err(PrimadbError::InvalidSetMember {
                                path: path.to_owned(),
                            });
                        }
                    }
                }
                return Ok(ParsedInput::Set(members));
            }
            Ok(ParsedInput::Object(object))
        }
        JsonValue::Array(items) => {
            if items.iter().any(JsonValue::is_object) {
                Err(PrimadbError::ArrayOfObjectsUnsupported {
                    path: path.to_owned(),
                })
            } else {
                Ok(ParsedInput::Scalar(JsonValue::Array(items)))
            }
        }
        scalar => Ok(ParsedInput::Scalar(scalar)),
    }
}

fn parse_link_marker(object: &Map<String, JsonValue>) -> Option<String> {
    if object.len() != 1 {
        return None;
    }
    object
        .get("$link")
        .or_else(|| object.get("#"))
        .and_then(|value| match value {
            JsonValue::String(target) => Some(target.clone()),
            _ => None,
        })
}

fn parse_bytes_marker(object: &Map<String, JsonValue>, path: &str) -> Result<Option<BinaryBytes>> {
    if object.len() != 1 {
        return Ok(None);
    }
    let Some(value) = object.get("$bytes") else {
        return Ok(None);
    };
    match value {
        JsonValue::String(encoded) => BinaryBytes::from_base64(encoded)
            .map(Some)
            .map_err(|_| PrimadbError::InvalidBinaryMarker {
                path: path.to_owned(),
            }),
        _ => Err(PrimadbError::InvalidBinaryMarker {
            path: path.to_owned(),
        }),
    }
}

fn parse_blob_marker(object: &Map<String, JsonValue>, path: &str) -> Result<Option<BlobRef>> {
    if object.len() != 1 {
        return Ok(None);
    }
    let Some(value) = object.get("$blob") else {
        return Ok(None);
    };
    serde_json::from_value::<BlobRef>(value.clone())
        .map(Some)
        .map_err(|_| PrimadbError::InvalidBlobMarker {
            path: path.to_owned(),
        })
}

fn parse_set_marker(object: &Map<String, JsonValue>) -> Option<Vec<JsonValue>> {
    if object.len() != 1 {
        return None;
    }
    object.get("$set").and_then(|value| match value {
        JsonValue::Array(items) => Some(items.clone()),
        _ => None,
    })
}

fn bytes_marker_value(bytes: &BinaryBytes) -> JsonValue {
    JsonValue::Object(Map::from_iter([(
        "$bytes".to_owned(),
        JsonValue::String(bytes.to_base64()),
    )]))
}

fn blob_marker_value(reference: &BlobRef) -> JsonValue {
    JsonValue::Object(Map::from_iter([(
        "$blob".to_owned(),
        serde_json::to_value(reference).unwrap_or(JsonValue::Null),
    )]))
}

fn parse_member_reference(value: JsonValue, path: &str) -> Result<String> {
    match value {
        JsonValue::String(member) => Ok(member),
        JsonValue::Object(object) => {
            if let Some(link) = parse_link_marker(&object) {
                return Ok(link);
            }
            if let Some(JsonValue::String(id)) = object.get("$id") {
                return Ok(id.clone());
            }
            Err(PrimadbError::InvalidMemberReference {
                path: path.to_owned(),
            })
        }
        _ => Err(PrimadbError::InvalidMemberReference {
            path: path.to_owned(),
        }),
    }
}

fn merge_snapshot_into_inner(inner: &mut Inner, snapshot: DatabaseSnapshot) {
    for node in snapshot.nodes.into_values() {
        observe_node_state(&mut inner.clock, &node);
        merge_node_state(&mut inner.nodes, node);
    }
    inner.missing_nodes.clear();
}

fn observe_node_state(clock: &mut HybridClock, node: &NodeState) {
    for marker in node.tombstones.values() {
        clock.observe(&marker.revision);
    }
    for field in node.fields.values() {
        clock.observe(&field.version.revision);
        if let FieldValue::Set(set) = &field.value {
            clock.observe(&set.baseline.revision);
            for marker in set.members.values() {
                clock.observe(&marker.revision);
            }
            for marker in set.removed.values() {
                clock.observe(&marker.revision);
            }
        }
    }
}

fn merge_node_state(nodes: &mut BTreeMap<NodeId, NodeState>, incoming: NodeState) {
    let node_id = incoming.id.clone();
    let current = nodes
        .entry(node_id.clone())
        .or_insert_with(|| NodeState::new(node_id));

    for (field, tombstone) in incoming.tombstones {
        let tombstone_blocks = current
            .tombstones
            .get(&field)
            .map(|existing| tombstone <= *existing)
            .unwrap_or(false);
        let field_blocks = current
            .fields
            .get(&field)
            .map(|existing| tombstone <= existing.version)
            .unwrap_or(false);
        if tombstone_blocks || field_blocks {
            continue;
        }
        current.fields.remove(&field);
        current.tombstones.insert(field, tombstone);
    }

    for (field, incoming_field) in incoming.fields {
        let tombstone_blocks = current
            .tombstones
            .get(&field)
            .map(|existing| incoming_field.version <= *existing)
            .unwrap_or(false);
        if tombstone_blocks {
            continue;
        }
        current.tombstones.remove(&field);
        match current.fields.get_mut(&field) {
            Some(existing) => merge_field_state(existing, incoming_field),
            None => {
                current.fields.insert(field, incoming_field);
            }
        }
    }
}

fn merge_field_state(existing: &mut FieldState, incoming: FieldState) {
    match (&mut existing.value, incoming.value) {
        (FieldValue::Set(current_set), FieldValue::Set(incoming_set)) => {
            existing.value = FieldValue::Set(merge_set_state(current_set, &incoming_set));
            if incoming.version > existing.version {
                existing.version = incoming.version;
            }
        }
        (_, incoming_value) => {
            if incoming.version > existing.version {
                existing.value = incoming_value;
                existing.version = incoming.version;
            }
        }
    }
}

fn merge_set_state(current: &SetState, incoming: &SetState) -> SetState {
    let baseline = std::cmp::max(current.baseline.clone(), incoming.baseline.clone());
    let mut candidate_ids = BTreeSet::new();
    candidate_ids.extend(current.members.keys().cloned());
    candidate_ids.extend(current.removed.keys().cloned());
    candidate_ids.extend(incoming.members.keys().cloned());
    candidate_ids.extend(incoming.removed.keys().cloned());

    let mut members = BTreeMap::new();
    let mut removed = BTreeMap::new();

    for member in candidate_ids {
        let add = max_marker(
            current.members.get(&member).cloned(),
            incoming.members.get(&member).cloned(),
        );
        let drop = max_marker(
            current.removed.get(&member).cloned(),
            incoming.removed.get(&member).cloned(),
        );
        let add_valid = add.as_ref().is_some_and(|marker| marker > &baseline);
        let drop_valid = drop.as_ref().is_some_and(|marker| marker > &baseline);

        match (add_valid, drop_valid) {
            (true, true) => {
                if add > drop {
                    members.insert(member, add.expect("validated add marker"));
                } else {
                    removed.insert(member, drop.expect("validated remove marker"));
                }
            }
            (true, false) => {
                members.insert(member, add.expect("validated add marker"));
            }
            (false, true) => {
                removed.insert(member, drop.expect("validated remove marker"));
            }
            (false, false) => {}
        }
    }

    SetState {
        baseline,
        members,
        removed,
    }
}

fn max_marker(left: Option<VersionMarker>, right: Option<VersionMarker>) -> Option<VersionMarker> {
    match (left, right) {
        (Some(left), Some(right)) => Some(std::cmp::max(left, right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn zero_marker() -> VersionMarker {
    VersionMarker {
        revision: Revision {
            millis: 0,
            counter: 0,
            actor: String::new(),
        },
        op_id: String::new(),
    }
}

fn derived_child_id(parent: &str, field: &str) -> String {
    format!("{parent}/{field}")
}

fn node_matches_root(node_id: &str, root: &str) -> bool {
    node_id == root || node_id.starts_with(&format!("{root}/"))
}

fn collect_snapshot_root_closure(
    nodes: &BTreeMap<NodeId, NodeState>,
    root: &str,
) -> BTreeSet<NodeId> {
    let mut reachable = BTreeSet::new();
    let mut pending = nodes
        .keys()
        .filter(|node_id| node_matches_root(node_id, root))
        .cloned()
        .collect::<Vec<_>>();

    while let Some(node_id) = pending.pop() {
        if !reachable.insert(node_id.clone()) {
            continue;
        }
        let Some(node) = nodes.get(&node_id) else {
            continue;
        };
        for field in node.fields.values() {
            match &field.value {
                FieldValue::Scalar(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {}
                FieldValue::Link(target) => pending.push(target.clone()),
                FieldValue::Set(set) => pending.extend(set.members.keys().cloned()),
            }
        }
    }

    reachable
}

fn operation_matches_snapshot_nodes(op: &Operation, nodes: &BTreeSet<NodeId>) -> bool {
    match &op.action {
        OperationAction::SetField { node, .. }
        | OperationAction::AddSetMember { node, .. }
        | OperationAction::RemoveSetMember { node, .. }
        | OperationAction::DeleteField { node, .. } => nodes.contains(node),
    }
}

fn build_pull_responses(
    request_id: &str,
    result: RemoteResult,
    limits: &PrimadbLimits,
) -> Vec<PullResponse> {
    match result {
        RemoteResult::Get { value } => vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::Get { value },
        }],
        RemoteResult::Map { entries } => build_map_chunk_responses(
            request_id,
            entries,
            limits.max_query_entries_per_chunk.max(1),
        ),
        RemoteResult::Query { entries } => build_query_chunk_responses(
            request_id,
            entries,
            limits.max_query_entries_per_chunk.max(1),
        ),
        RemoteResult::Lex { entries } => build_lex_chunk_responses(
            request_id,
            entries,
            limits.max_query_entries_per_chunk.max(1),
        ),
        RemoteResult::Snapshot { snapshot } => {
            let node_chunks =
                chunk_btree_map(snapshot.nodes, limits.max_snapshot_nodes_per_chunk.max(1));
            let op_chunks = chunk_vec(
                snapshot.pending_ops,
                limits.max_snapshot_ops_per_chunk.max(1),
            );
            let total = node_chunks.len().max(op_chunks.len()).max(1);
            let total_u32 = total as u32;
            (0..total)
                .map(|index| PullResponse {
                    request_id: request_id.to_owned(),
                    chunk: PullChunk {
                        index: index as u32,
                        total: total_u32,
                    },
                    done: index + 1 == total,
                    result: PullResponseBody::Snapshot {
                        clock: (index == 0).then_some(snapshot.clock.clone()),
                        nodes: node_chunks.get(index).cloned().unwrap_or_default(),
                        pending_ops: op_chunks.get(index).cloned().unwrap_or_default(),
                    },
                })
                .collect()
        }
    }
}

fn build_map_chunk_responses(
    request_id: &str,
    entries: Vec<MapEntry>,
    chunk_size: usize,
) -> Vec<PullResponse> {
    let chunks = chunk_vec(entries, chunk_size);
    if chunks.is_empty() {
        return vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::Map {
                entries: Vec::new(),
            },
        }];
    }
    let total = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, entries)| PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk {
                index: index as u32,
                total: total as u32,
            },
            done: index + 1 == total,
            result: PullResponseBody::Map { entries },
        })
        .collect()
}

fn build_query_chunk_responses(
    request_id: &str,
    entries: Vec<MapEntry>,
    chunk_size: usize,
) -> Vec<PullResponse> {
    let chunks = chunk_vec(entries, chunk_size);
    if chunks.is_empty() {
        return vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::Query {
                entries: Vec::new(),
            },
        }];
    }
    let total = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, entries)| PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk {
                index: index as u32,
                total: total as u32,
            },
            done: index + 1 == total,
            result: PullResponseBody::Query { entries },
        })
        .collect()
}

fn build_lex_chunk_responses(
    request_id: &str,
    entries: Vec<LexEntry>,
    chunk_size: usize,
) -> Vec<PullResponse> {
    let chunks = chunk_vec(entries, chunk_size);
    if chunks.is_empty() {
        return vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::Lex {
                entries: Vec::new(),
            },
        }];
    }
    let total = chunks.len();
    chunks
        .into_iter()
        .enumerate()
        .map(|(index, entries)| PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk {
                index: index as u32,
                total: total as u32,
            },
            done: index + 1 == total,
            result: PullResponseBody::Lex { entries },
        })
        .collect()
}

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
fn chunk_vec<T>(items: Vec<T>, chunk_size: usize) -> Vec<Vec<T>>
where
    T: Clone + Send + Sync,
{
    if items.is_empty() {
        return Vec::new();
    }
    chunk_vec_impl(items, chunk_size.max(1))
}

#[cfg(not(any(not(target_arch = "wasm32"), feature = "wasm-threads")))]
fn chunk_vec<T: Clone>(items: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {
    if items.is_empty() {
        return Vec::new();
    }
    chunk_vec_impl(items, chunk_size.max(1))
}

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
fn chunk_btree_map<K, V>(items: BTreeMap<K, V>, chunk_size: usize) -> Vec<BTreeMap<K, V>>
where
    K: Ord + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    if items.is_empty() {
        return Vec::new();
    }

    chunk_btree_map_impl(items, chunk_size.max(1))
}

#[cfg(not(any(not(target_arch = "wasm32"), feature = "wasm-threads")))]
fn chunk_btree_map<K, V>(items: BTreeMap<K, V>, chunk_size: usize) -> Vec<BTreeMap<K, V>>
where
    K: Ord + Clone,
    V: Clone,
{
    if items.is_empty() {
        return Vec::new();
    }

    chunk_btree_map_impl(items, chunk_size.max(1))
}

fn filter_query_entries(mut entries: Vec<MapEntry>, spec: &QuerySpec) -> Vec<MapEntry> {
    if spec.filters.is_empty() {
        return entries;
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
    {
        if entries.len() >= PARALLEL_QUERY_MIN_LEN {
            return entries
                .into_par_iter()
                .filter(|entry| {
                    spec.filters
                        .iter()
                        .all(|filter| matches_filter(entry, filter))
                })
                .collect();
        }
    }

    entries.retain(|entry| {
        spec.filters
            .iter()
            .all(|filter| matches_filter(entry, filter))
    });
    entries
}

fn sort_query_entries(entries: &mut Vec<MapEntry>, order: &crate::query::QueryOrder) {
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
    {
        if entries.len() >= PARALLEL_QUERY_MIN_LEN {
            entries.par_sort_unstable_by(|left, right| compare_entries(left, right, order));
            return;
        }
    }

    entries.sort_by(|left, right| compare_entries(left, right, order));
}

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
fn chunk_vec_impl<T>(items: Vec<T>, chunk_size: usize) -> Vec<Vec<T>>
where
    T: Clone + Send + Sync,
{
    if items.len() >= PARALLEL_CHUNK_MIN_LEN {
        items
            .par_chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    } else {
        items
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    }
}

#[cfg(not(any(not(target_arch = "wasm32"), feature = "wasm-threads")))]
fn chunk_vec_impl<T>(items: Vec<T>, chunk_size: usize) -> Vec<Vec<T>>
where
    T: Clone,
{
    items
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect()
}

#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
fn chunk_btree_map_impl<K, V>(items: BTreeMap<K, V>, chunk_size: usize) -> Vec<BTreeMap<K, V>>
where
    K: Ord + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    if items.len() >= PARALLEL_CHUNK_MIN_LEN {
        let entries: Vec<_> = items.into_iter().collect();
        entries
            .par_chunks(chunk_size)
            .map(|chunk| chunk.iter().cloned().collect())
            .collect()
    } else {
        chunk_btree_map_serial(items, chunk_size)
    }
}

#[cfg(not(any(not(target_arch = "wasm32"), feature = "wasm-threads")))]
fn chunk_btree_map_impl<K, V>(items: BTreeMap<K, V>, chunk_size: usize) -> Vec<BTreeMap<K, V>>
where
    K: Ord + Clone,
    V: Clone,
{
    chunk_btree_map_serial(items, chunk_size)
}

fn chunk_btree_map_serial<K, V>(items: BTreeMap<K, V>, chunk_size: usize) -> Vec<BTreeMap<K, V>>
where
    K: Ord + Clone,
    V: Clone,
{
    let mut chunks = Vec::new();
    let mut current = BTreeMap::new();
    for (index, (key, value)) in items.into_iter().enumerate() {
        current.insert(key, value);
        if (index + 1) % chunk_size == 0 {
            chunks.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn matches_filter(entry: &MapEntry, filter: &QueryFilter) -> bool {
    match filter {
        QueryFilter::Eq { path, value } => {
            query_value(entry, path).is_some_and(|candidate| candidate == *value)
        }
        QueryFilter::Ne { path, value } => {
            query_value(entry, path).is_some_and(|candidate| candidate != *value)
        }
        QueryFilter::Gt { path, value } => query_value(entry, path)
            .map(|candidate| compare_json_values(&candidate, value) == Some(Ordering::Greater))
            .unwrap_or(false),
        QueryFilter::Gte { path, value } => query_value(entry, path)
            .map(|candidate| {
                matches!(
                    compare_json_values(&candidate, value),
                    Some(Ordering::Greater | Ordering::Equal)
                )
            })
            .unwrap_or(false),
        QueryFilter::Lt { path, value } => query_value(entry, path)
            .map(|candidate| compare_json_values(&candidate, value) == Some(Ordering::Less))
            .unwrap_or(false),
        QueryFilter::Lte { path, value } => query_value(entry, path)
            .map(|candidate| {
                matches!(
                    compare_json_values(&candidate, value),
                    Some(Ordering::Less | Ordering::Equal)
                )
            })
            .unwrap_or(false),
        QueryFilter::Prefix { path, value } => query_value(entry, path)
            .and_then(|candidate| candidate.as_str().map(str::to_owned))
            .map(|candidate| candidate.starts_with(value))
            .unwrap_or(false),
        QueryFilter::Contains { path, value } => query_value(entry, path)
            .and_then(|candidate| candidate.as_str().map(str::to_owned))
            .map(|candidate| candidate.contains(value))
            .unwrap_or(false),
        QueryFilter::Exists { path } => query_value(entry, path).is_some(),
    }
}

fn indexed_filter_path(filter: &QueryFilter) -> Option<String> {
    let path = match filter {
        QueryFilter::Eq { path, .. }
        | QueryFilter::Ne { path, .. }
        | QueryFilter::Gt { path, .. }
        | QueryFilter::Gte { path, .. }
        | QueryFilter::Lt { path, .. }
        | QueryFilter::Lte { path, .. }
        | QueryFilter::Prefix { path, .. }
        | QueryFilter::Contains { path, .. }
        | QueryFilter::Exists { path } => path,
    };
    is_direct_index_path(path).then_some(path.clone())
}

fn indexed_order_path(order: &crate::query::QueryOrder) -> Option<String> {
    is_direct_index_path(&order.path).then_some(order.path.clone())
}

fn is_direct_index_path(path: &str) -> bool {
    !path.is_empty() && path != "$key" && path != "$value" && !path.contains('.')
}

fn filter_matches_index_entry(filter: &QueryFilter, value: &JsonValue) -> bool {
    match filter {
        QueryFilter::Eq { value: expected, .. } => value == expected,
        QueryFilter::Ne { value: expected, .. } => value != expected,
        QueryFilter::Gt { value: expected, .. } => {
            compare_json_values(value, expected) == Some(Ordering::Greater)
        }
        QueryFilter::Gte { value: expected, .. } => matches!(
            compare_json_values(value, expected),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        QueryFilter::Lt { value: expected, .. } => {
            compare_json_values(value, expected) == Some(Ordering::Less)
        }
        QueryFilter::Lte { value: expected, .. } => matches!(
            compare_json_values(value, expected),
            Some(Ordering::Less | Ordering::Equal)
        ),
        QueryFilter::Prefix { value: expected, .. } => value
            .as_str()
            .map(|candidate| candidate.starts_with(expected))
            .unwrap_or(false),
        QueryFilter::Contains { value: expected, .. } => value
            .as_str()
            .map(|candidate| candidate.contains(expected))
            .unwrap_or(false),
        QueryFilter::Exists { .. } => true,
    }
}

fn compare_entries(
    left: &MapEntry,
    right: &MapEntry,
    order: &crate::query::QueryOrder,
) -> Ordering {
    let base = match (
        query_value(left, &order.path),
        query_value(right, &order.path),
    ) {
        (Some(left), Some(right)) => compare_json_values(&left, &right).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => left.key.cmp(&right.key),
    };
    match order.direction {
        QueryDirection::Asc => base.then_with(|| left.key.cmp(&right.key)),
        QueryDirection::Desc => base.reverse().then_with(|| left.key.cmp(&right.key)),
    }
}

fn query_value(entry: &MapEntry, path: &str) -> Option<JsonValue> {
    match path {
        "" | "$value" => Some(entry.value.clone()),
        "$key" => Some(JsonValue::String(entry.key.clone())),
        _ => {
            let mut current = &entry.value;
            for segment in path.split('.') {
                current = match current {
                    JsonValue::Object(object) => object.get(segment)?,
                    _ => return None,
                };
            }
            Some(current.clone())
        }
    }
}

fn compare_json_values(left: &JsonValue, right: &JsonValue) -> Option<Ordering> {
    match (left, right) {
        (JsonValue::String(left), JsonValue::String(right)) => Some(left.cmp(right)),
        (JsonValue::Number(left), JsonValue::Number(right)) => {
            let left = left.as_f64()?;
            let right = right.as_f64()?;
            left.partial_cmp(&right)
        }
        (JsonValue::Bool(left), JsonValue::Bool(right)) => Some(left.cmp(right)),
        (JsonValue::Null, JsonValue::Null) => Some(Ordering::Equal),
        _ => None,
    }
}

fn lex_key_matches(key: &str, spec: &LexSpec) -> bool {
    if let Some(prefix) = &spec.prefix {
        if !key.starts_with(prefix) {
            return false;
        }
    }
    if let Some(start_at) = &spec.start_at {
        if key < start_at.as_str() {
            return false;
        }
    }
    if let Some(start_after) = &spec.start_after {
        if key <= start_after.as_str() {
            return false;
        }
    }
    if let Some(end_at) = &spec.end_at {
        if key > end_at.as_str() {
            return false;
        }
    }
    if let Some(end_before) = &spec.end_before {
        if key >= end_before.as_str() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::Primadb;
    use crate::{
        PullRequest, PullRequestKind, PullResponseBody, QueryDirection, QueryFilter, QuerySpec,
        RemotePath, Result,
    };
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn nested_put_materializes_as_linked_graph() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        db.root("users").field("alice").put(json!({
            "name": "Alice",
            "profile": {
                "timezone": "America/New_York"
            }
        }))?;

        let users = db.root("users").once_json()?.unwrap();
        assert_eq!(users["$id"], "users");
        assert_eq!(users["alice"]["name"], "Alice");
        assert_eq!(users["alice"]["profile"]["timezone"], "America/New_York");
        Ok(())
    }

    #[test]
    fn set_members_are_unique_by_node_id() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        let member_id = db
            .root("rooms")
            .field("general")
            .field("members")
            .set(json!({"name": "Alice"}))?;
        db.root("rooms")
            .field("general")
            .field("members")
            .set(json!({"$link": member_id.clone()}))?;

        let members = db
            .root("rooms")
            .field("general")
            .field("members")
            .once_json()?
            .unwrap();
        assert_eq!(members["$set"].as_array().unwrap().len(), 1);
        assert_eq!(members["$set"][0]["$id"], member_id);
        Ok(())
    }

    #[test]
    fn set_members_can_be_removed() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        let member_id = db
            .root("rooms")
            .field("general")
            .field("members")
            .set(json!({"name": "Alice"}))?;

        db.root("rooms")
            .field("general")
            .field("members")
            .remove(json!({"$link": member_id}))?;

        let members = db
            .root("rooms")
            .field("general")
            .field("members")
            .once_json()?
            .unwrap();
        assert_eq!(members["$set"].as_array().unwrap().len(), 0);
        Ok(())
    }

    #[test]
    fn later_revisions_win_across_replicas() -> Result<()> {
        let left = Primadb::with_replica_id("left");
        let right = Primadb::with_replica_id("right");

        left.root("docs")
            .field("post")
            .put(json!({"status": "draft"}))?;
        right.apply_operations(left.drain_pending_operations()?)?;

        right
            .root("docs")
            .field("post")
            .put(json!({"status": "published"}))?;
        left.apply_operations(right.drain_pending_operations()?)?;

        let status = left.root("docs").field("post").once_json()?.unwrap();
        assert_eq!(status["status"], "published");
        Ok(())
    }

    #[test]
    fn concurrent_set_additions_union_across_replicas() -> Result<()> {
        let left = Primadb::with_replica_id("left");
        let right = Primadb::with_replica_id("right");

        let left_id = left
            .root("rooms")
            .field("general")
            .field("members")
            .set(json!({"name": "Alice"}))?;
        let right_id = right
            .root("rooms")
            .field("general")
            .field("members")
            .set(json!({"name": "Bob"}))?;

        let left_ops = left.drain_pending_operations()?;
        let right_ops = right.drain_pending_operations()?;

        left.apply_operations(right_ops)?;
        right.apply_operations(left_ops)?;

        let members = left
            .root("rooms")
            .field("general")
            .field("members")
            .once_json()?
            .unwrap();
        let set = members["$set"].as_array().unwrap();
        assert_eq!(set.len(), 2);

        let ids: std::collections::BTreeSet<_> = set
            .iter()
            .filter_map(|member| member["$id"].as_str())
            .collect();
        assert!(ids.contains(left_id.as_str()));
        assert!(ids.contains(right_id.as_str()));
        Ok(())
    }

    #[test]
    fn later_set_member_removal_wins_across_replicas() -> Result<()> {
        let left = Primadb::with_replica_id("left");
        let right = Primadb::with_replica_id("right");

        let member_id = left
            .root("rooms")
            .field("general")
            .field("members")
            .set(json!({"name": "Alice"}))?;
        right.apply_operations(left.drain_pending_operations()?)?;

        right
            .root("rooms")
            .field("general")
            .field("members")
            .remove(json!({"$link": member_id}))?;
        left.apply_operations(right.drain_pending_operations()?)?;

        let members = left
            .root("rooms")
            .field("general")
            .field("members")
            .once_json()?
            .unwrap();
        assert_eq!(members["$set"].as_array().unwrap().len(), 0);
        Ok(())
    }

    #[test]
    fn sync_envelope_json_round_trips() -> Result<()> {
        let left = Primadb::with_replica_id("left");
        let right = Primadb::with_replica_id("right");

        left.root("docs")
            .field("hello")
            .put(json!({"value": "world"}))?;

        let payload = left.export_pending_operations_json()?;
        right.apply_operations_json(&payload)?;

        let snapshot = right.root("docs").field("hello").once_json()?.unwrap();
        assert_eq!(snapshot["value"], "world");
        Ok(())
    }

    #[test]
    fn query_layer_filters_and_orders_entries() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        db.root("users").field("alice").put(json!({
            "name": "Alice",
            "age": 31,
            "profile": { "city": "Boston" }
        }))?;
        db.root("users").field("bob").put(json!({
            "name": "Bob",
            "age": 27,
            "profile": { "city": "Berlin" }
        }))?;
        db.root("users").field("carol").put(json!({
            "name": "Carol",
            "age": 35,
            "profile": { "city": "Boston" }
        }))?;

        let results = db
            .root("users")
            .find()
            .where_eq("profile.city", "Boston")?
            .where_gte("age", 30)?
            .order_by("name", QueryDirection::Desc)
            .run()?;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].value["name"], "Carol");
        assert_eq!(results[1].value["name"], "Alice");
        Ok(())
    }

    #[test]
    fn change_subscriptions_track_pending_state_transitions() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        let changes = db.subscribe_changes();

        let initial = changes.recv_blocking().unwrap();
        assert_eq!(initial.pending_ops, 0);
        assert!(!initial.data_changed);

        db.root("docs")
            .field("hello")
            .put(json!({"value": "world"}))?;
        let after_put = changes.recv_blocking().unwrap();
        assert_eq!(after_put.pending_ops, 2);
        assert!(after_put.data_changed);

        let _ = db.drain_pending_operations()?;
        let after_drain = changes.recv_blocking().unwrap();
        assert_eq!(after_drain.pending_ops, 0);
        assert!(!after_drain.data_changed);
        Ok(())
    }

    #[test]
    fn subscriptions_emit_initial_and_updated_snapshots() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        let chain = db.root("users").field("alice");
        let subscription = chain.subscribe()?;

        assert_eq!(subscription.recv_blocking(), Some(None));

        chain.put(json!({"name": "Alice"}))?;
        let update = subscription.recv_blocking().unwrap().unwrap();
        assert_eq!(update["name"], "Alice");
        Ok(())
    }

    #[test]
    fn file_persistence_round_trips_state() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-{unique}.json"));

        let first = Primadb::with_replica_id("node-a");
        assert!(!first.use_file_persistence(path.clone())?);
        first
            .root("docs")
            .field("hello")
            .put(json!({"value": "world"}))?;

        let second = Primadb::with_replica_id("node-b");
        assert!(second.use_file_persistence(path.clone())?);
        let snapshot = second.root("docs").field("hello").once_json()?.unwrap();
        assert_eq!(snapshot["value"], "world");

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn bytes_fields_round_trip_and_replicate() -> Result<()> {
        let left = Primadb::with_replica_id("bytes-left");
        let right = Primadb::with_replica_id("bytes-right");
        let payload = vec![0, 7, 42, 255, 128, 1];

        left.root("assets").field("avatar").put_bytes(payload.clone())?;
        assert_eq!(
            left.root("assets").field("avatar").once_bytes()?,
            Some(payload.clone())
        );

        right.apply_operations(left.drain_pending_operations()?)?;
        assert_eq!(right.root("assets").field("avatar").once_bytes()?, Some(payload));

        let materialized = right.root("assets").field("avatar").once_json()?.unwrap();
        assert_eq!(
            materialized,
            json!({"$bytes": crate::BinaryBytes::from(vec![0, 7, 42, 255, 128, 1]).to_base64()}),
        );
        Ok(())
    }

    #[test]
    fn memory_blob_storage_round_trips_blob_reference_and_bytes() -> Result<()> {
        let db = Primadb::with_replica_id("blob-a");
        let binding = db.open_blob_storage(crate::BlobStorageConfig::Memory)?;
        assert_eq!(binding.backend, "memory");

        let payload = b"primadb-blob-smoke".to_vec();
        let reference = db
            .root("assets")
            .field("archive")
            .put_blob(payload.clone(), Some("application/octet-stream"))?;

        assert_eq!(
            db.root("assets").field("archive").once_blob_ref()?,
            Some(reference.clone())
        );

        let blob = db.root("assets").field("archive").get_blob()?.unwrap();
        assert_eq!(blob.reference, reference);
        assert_eq!(blob.data.into_inner(), payload);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn file_blob_storage_restores_stored_blob_data() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("primadb-blob-{unique}"));

        let first = Primadb::with_replica_id("blob-file-a");
        let binding = first.open_blob_storage(crate::BlobStorageConfig::Files {
            directory: root.display().to_string(),
        })?;
        assert_eq!(binding.backend, "files");

        let reference = first
            .root("assets")
            .field("backup")
            .put_blob(b"native-file-blob".to_vec(), Some("application/octet-stream"))?;

        let second = Primadb::with_replica_id("blob-file-b");
        second.open_blob_storage(crate::BlobStorageConfig::Files {
            directory: root.display().to_string(),
        })?;
        second
            .root("assets")
            .field("backup")
            .put(json!({"$blob": reference.clone()}))?;

        let blob = second.root("assets").field("backup").get_blob()?.unwrap();
        assert_eq!(blob.reference, reference);
        assert_eq!(blob.data.into_inner(), b"native-file-blob".to_vec());

        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn lexical_scan_supports_ranges_and_recursive_follow_links() -> Result<()> {
        let db = Primadb::with_replica_id("scan-a");
        db.root("users").field("alice").put(json!({
            "name": "Alice",
            "profile": { "city": "Boston" },
            "settings": { "theme": "forest" }
        }))?;
        db.root("users").field("bob").put(json!({
            "name": "Bob",
            "profile": { "city": "Berlin" }
        }))?;

        let shallow = db
            .root("users")
            .lex()
            .start_at("alice")
            .end_before("carol")
            .run()?;
        assert_eq!(shallow.len(), 2);
        assert_eq!(shallow[0].key, "alice");
        assert_eq!(shallow[1].key, "bob");

        let deep = db
            .root("users")
            .field("alice")
            .lex()
            .follow_links(true)
            .depth(3)
            .run()?;
        assert!(
            deep.iter()
                .any(|entry| entry.path.ends_with("profile.city"))
        );
        Ok(())
    }

    #[test]
    fn radisk_storage_round_trips_state() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-radisk-{unique}"));

        let first = Primadb::with_replica_id("node-a");
        assert!(!first.use_radisk_storage(path.clone(), 2)?);
        first
            .root("docs")
            .field("hello")
            .put(json!({"value": "world"}))?;

        let second = Primadb::with_replica_id("node-b");
        assert!(second.use_radisk_storage(path.clone(), 2)?);
        let snapshot = second.root("docs").field("hello").once_json()?.unwrap();
        assert_eq!(snapshot["value"], "world");

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[test]
    fn incremental_storage_lazily_restores_and_uses_direct_indexes_for_set_queries() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-{unique}"));

        let writer = Primadb::with_replica_id("writer");
        assert!(!writer.use_radisk_storage(path.clone(), 8)?);
        for index in 0..24 {
            writer
                .root("lists")
                .field("main")
                .field("items")
                .set(json!({
                    "title": format!("Task {index}"),
                    "done": index % 2 == 0,
                    "archived": false,
                    "created_at": index,
                }))?;
        }

        let reader = Primadb::with_replica_id("reader");
        assert!(reader.use_radisk_storage(path.clone(), 8)?);
        assert_eq!(reader.stats().nodes, 0);

        let open_tasks = reader.root("lists").field("main").field("items").query(QuerySpec {
            filters: vec![
                QueryFilter::Eq {
                    path: "done".to_owned(),
                    value: json!(false),
                },
                QueryFilter::Eq {
                    path: "archived".to_owned(),
                    value: json!(false),
                },
            ],
            order: Some(crate::query::QueryOrder {
                path: "created_at".to_owned(),
                direction: QueryDirection::Asc,
            }),
            limit: Some(50),
            offset: 0,
        })?;

        assert_eq!(open_tasks.len(), 12);
        assert_eq!(open_tasks[0].value["title"], "Task 1");
        assert!(reader.stats().nodes < 24);

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[test]
    fn persisted_snapshot_preserves_local_actor_and_clears_foreign_pending_ops() -> Result<()> {
        let first = Primadb::with_replica_id("actor-a");
        first
            .root("docs")
            .field("hello")
            .put(json!({"value": "world"}))?;
        let persisted = first.export_persisted_snapshot_json()?;

        let second = Primadb::with_replica_id("actor-b");
        second.import_persisted_snapshot_json(&persisted)?;

        assert_eq!(second.replica_id(), "actor-b");
        assert!(second.pending_operations().is_empty());
        let value = second.root("docs").field("hello").once_json()?.unwrap();
        assert_eq!(value["value"], "world");
        Ok(())
    }

    #[test]
    fn merge_snapshot_preserves_local_state_and_merges_remote_sets() -> Result<()> {
        let source = Primadb::with_replica_id("source");
        let remote_notes = source.root("rooms").field("lobby").field("notes");
        remote_notes.set(json!({
            "title": "Remote note",
            "done": false,
            "created_at": 1,
        }))?;
        let snapshot = source.snapshot_for_root(Some("rooms"));

        let local = Primadb::with_replica_id("local");
        local.root("status").field("message").put(json!("keep-local"))?;
        local.merge_snapshot(snapshot)?;

        assert_eq!(local.replica_id(), "local");
        assert_eq!(
            local.root("status").field("message").once_json()?.unwrap(),
            json!("keep-local")
        );

        let merged_notes = local.root("rooms").field("lobby").field("notes").query(QuerySpec {
            filters: Vec::new(),
            order: None,
            limit: Some(10),
            offset: 0,
        })?;
        assert_eq!(merged_notes.len(), 1);
        assert_eq!(merged_notes[0].value["title"], "Remote note");
        assert_eq!(local.pending_operations().len(), 1);
        Ok(())
    }

    #[test]
    fn remote_pull_requests_and_chunked_responses_cover_large_query_and_snapshot_results()
    -> Result<()> {
        let db = Primadb::with_replica_id("pull-source");
        let notes = db.root("boards").field("shared").field("notes");
        for index in 0..90 {
            notes.set(json!({
                "title": format!("Chunk note {index}"),
                "body": "remote pull proof",
                "archived": false,
                "created_at": index,
            }))?;
        }

        let path = RemotePath::new("boards", vec!["shared".to_owned(), "notes".to_owned()]);
        let query_chunks = db.chunk_remote_result(
            "query-1",
            db.execute_pull_request(&PullRequest {
                request_id: "query-1".to_owned(),
                request: PullRequestKind::Query {
                    path: path.clone(),
                    spec: QuerySpec {
                        filters: vec![QueryFilter::Eq {
                            path: "archived".to_owned(),
                            value: json!(false),
                        }],
                        order: None,
                        limit: Some(200),
                        offset: 0,
                    },
                },
            })?,
        );
        assert!(query_chunks.len() > 1);
        let query_count = query_chunks
            .iter()
            .map(|response| match &response.result {
                PullResponseBody::Query { entries } => entries.len(),
                other => panic!("unexpected query chunk: {other:?}"),
            })
            .sum::<usize>();
        assert_eq!(query_count, 90);

        let snapshot_chunks = db.chunk_remote_result(
            "snapshot-1",
            db.execute_pull_request(&PullRequest {
                request_id: "snapshot-1".to_owned(),
                request: PullRequestKind::Snapshot { root: None },
            })?,
        );
        assert!(snapshot_chunks.len() > 1);
        let snapshot_nodes = snapshot_chunks
            .iter()
            .map(|response| match &response.result {
                PullResponseBody::Snapshot { nodes, .. } => nodes.len(),
                other => panic!("unexpected snapshot chunk: {other:?}"),
            })
            .sum::<usize>();
        assert!(snapshot_nodes >= 90);
        Ok(())
    }
}
