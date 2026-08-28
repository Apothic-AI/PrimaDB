use crate::binary::BinaryBytes;
#[cfg(not(target_arch = "wasm32"))]
use crate::blob::FileBlobStore;
use crate::blob::{
    BlobRef, BlobStorageBinding, BlobStorageConfig, BlobStore, MemoryBlobStore, StoredBlob,
};
use crate::clock::{HybridClock, Revision, VersionMarker, now_millis};
use crate::consistency::{
    ProvisionalTransaction, ScopeAuthority, ScopeConsistency, ScopeOfflineWrites, ScopePolicy,
    TransactionOptions, TransactionReport, TransactionStatus, TransactionStep,
};
#[cfg(not(target_arch = "wasm32"))]
use crate::durable::{DurableStorageBinding, DurableStorageConfig};
use crate::engine::{
    DirectIndexScan, IncrementalStore, StorageTransaction, StorageVacuumReport,
    build_storage_metadata, build_storage_transaction, build_storage_transaction_from_ops,
};
use crate::error::{PrimadbError, Result};
use crate::hardening::{PrimadbLimits, PrimadbStats};
use crate::hooks::NetworkHooks;
#[cfg(any(test, target_arch = "wasm32", feature = "native-webrtc"))]
use crate::hooks::RoomHookContext;
#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
use crate::hooks::{
    ConnectHookContext, HookDecision, HookTransport, ServeRequestContext, ServeResultContext,
};
use crate::operation::{Operation, OperationAction, OperationValue};
use crate::persistence::{PersistenceTarget, load_snapshot_payload, store_snapshot_payload};
use crate::query::{LexEntry, LexSpec, QueryDirection, QueryFilter, QuerySpec};
use crate::record::{
    RecordBatch, RecordBatchReport, RecordEntry, RecordMutation, RecordPrecondition, RecordScan,
    RecordScanResult, RecordValue,
};
#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
use crate::session_auth::{PresenceIdentity, SessionAuthConfig, VerifiedIdentity};
use crate::snapshot::DatabaseSnapshot;
use crate::storage::StorageAdapter;
use crate::sync::{
    PullChunk, PullRequest, PullRequestKind, PullResponse, PullResponseBody, RemotePath,
    RemoteResult, SyncEnvelope, SyncFrame,
};
use crate::text_search::{
    SearchStalePolicy, TextCacheFiles, TextCollectionCache, TextCollectionConfig, TextDocument,
    TextIndexStats, TextScoreScope, TextSearchResult, TextSearchSource, TextSearchSourceSummary,
    TextSearchSpec, collection_cache_from_text_cache_files,
    collection_config_from_record as text_collection_config_from_record, search_text_candidates,
    search_text_collection, text_cache_files, text_candidates_from_map_entries,
    text_candidates_from_record_entries, text_collection_config_key, text_collection_docs_prefix,
    text_collection_from_record_key, text_document_from_record, text_document_id_from_record_key,
    text_document_key, validate_text_collection_config,
};
use crate::traversal::{
    TraversalDirection, TraversalEdge, TraversalEdgeKind, TraversalEntry, TraversalResult,
    TraversalSpec, TraversalStrategy,
};
use crate::value::{FieldState, FieldValue, NodeId, NodeState, SetState};
use crate::vector::{
    VectorBackendKind, VectorCacheEntry, VectorCacheFiles, VectorCollectionCache,
    VectorCollectionConfig, VectorEntry, VectorIndexStats, VectorItemMeta, VectorManagerState,
    VectorMetric, VectorSearchResult, VectorSearchSpec, VectorStalePolicy, build_vector_ann,
    build_vector_cache_files, checksum_bytes, chunk_from_record, collection_cache_from_cache_files,
    collection_config_from_record, encode_f32_le, encode_vector_chunk, item_meta_from_record,
    records_source_hash, search_vector_collection, validate_collection_config, validate_vector,
    vector_collection_from_record_key, vector_collection_items_prefix, vector_collection_meta_key,
    vector_item_chunk_key, vector_item_chunks_prefix, vector_item_id_from_record_key,
    vector_item_meta_key,
};
#[cfg(feature = "crypto")]
use crate::{
    Identity, PublicIdentity, SecretBoxKey, SecureSyncFrame, SecurityState, StoredSnapshot,
    UserGrant, owner_public_key_for_path,
};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-webrtc"))]
use crate::{MeshConfig, NativeWebRtcMesh};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
use crate::{MoqRelayClientConfig, NativeMoqSync};
#[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
use crate::{NativeWebSocketSync, RelayClientConfig};
use async_channel::{Receiver, Sender};
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-threads"))]
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Condvar, Mutex, Weak};

const LOCAL_WATCH_QUEUE_CAPACITY: usize = 64;

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
    #[serde(default)]
    pub full_refresh: bool,
    #[serde(default)]
    pub touched_paths: Vec<String>,
    #[serde(default)]
    pub records_changed: bool,
    #[serde(default)]
    pub touched_record_keys: Vec<String>,
}

#[derive(Clone)]
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

pub struct TraversalSubscription {
    inner: Arc<TraversalSubscriptionInner>,
}

pub struct RecordWatchSubscription {
    inner: Arc<RecordWatchSubscriptionInner>,
}

pub struct VectorWatchSubscription {
    inner: Arc<VectorWatchSubscriptionInner>,
}

pub struct TextWatchSubscription {
    inner: Arc<TextWatchSubscriptionInner>,
}

#[derive(Debug, Clone)]
pub struct Scope {
    db: Primadb,
    root: NodeId,
}

pub struct Transaction<'a> {
    inner: &'a mut Inner,
    scope_root: Option<NodeId>,
    member_ids: Vec<String>,
}

pub struct TransactionChain<'tx, 'inner> {
    tx: &'tx mut Transaction<'inner>,
    anchor: NodeId,
    segments: Vec<String>,
}

#[cfg(not(target_arch = "wasm32"))]
pub trait NodeFetchScheduler: Send + Sync {
    fn fetch_nodes(&self, nodes: Vec<NodeId>);
}

#[cfg(target_arch = "wasm32")]
pub trait NodeFetchScheduler {
    fn fetch_nodes(&self, nodes: Vec<NodeId>);
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct VacuumReport {
    pub storage: StorageVacuumReport,
    pub removed_blob_entries: usize,
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

struct TraversalSubscriptionInner {
    id: u64,
    db: Weak<Mutex<Inner>>,
    receiver: Receiver<TraversalResult>,
}

struct RecordWatchSubscriptionInner {
    id: u64,
    db: Weak<Mutex<Inner>>,
    receiver: Receiver<RecordScanResult>,
}

struct VectorWatchSubscriptionInner {
    id: u64,
    db: Weak<Mutex<Inner>>,
    receiver: Receiver<VectorSearchResult>,
}

struct TextWatchSubscriptionInner {
    id: u64,
    db: Weak<Mutex<Inner>>,
    receiver: Receiver<TextSearchResult>,
}

struct Inner {
    clock: HybridClock,
    nodes: std::collections::BTreeMap<NodeId, NodeState>,
    // Record scans use storage as the base and consult only locally materialized
    // record changes. This avoids walking unrelated lazy-loaded graph nodes.
    record_overlay: BTreeMap<String, Option<RecordEntry>>,
    record_overlay_node_keys: BTreeMap<NodeId, String>,
    pending_ops: CompactedOperations,
    unflushed_ops: CompactedOperations,
    subscriptions: std::collections::BTreeMap<u64, Watcher>,
    traversal_subscriptions: std::collections::BTreeMap<u64, TraversalWatcher>,
    record_subscriptions: std::collections::BTreeMap<u64, RecordWatcher>,
    vector_subscriptions: std::collections::BTreeMap<u64, VectorWatcher>,
    text_subscriptions: std::collections::BTreeMap<u64, TextWatcher>,
    change_subscriptions: std::collections::BTreeMap<u64, ChangeWatcher>,
    next_subscription_id: u64,
    next_traversal_subscription_id: u64,
    next_record_subscription_id: u64,
    next_vector_subscription_id: u64,
    next_text_subscription_id: u64,
    next_change_subscription_id: u64,
    change_revision: u64,
    persistence: Option<PersistenceTarget>,
    storage_adapter: Option<Arc<dyn StorageAdapter>>,
    storage_engine: Option<Arc<dyn IncrementalStore>>,
    external_storage_hooks: usize,
    blob_store: Option<Arc<dyn BlobStore>>,
    missing_nodes: BTreeSet<NodeId>,
    relationship_index: RelationshipIndex,
    node_fetch_schedulers: BTreeMap<u64, Arc<dyn NodeFetchScheduler>>,
    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    next_node_fetch_scheduler_id: u64,
    scheduled_node_fetches: BTreeSet<NodeId>,
    scope_policies: BTreeMap<String, ScopePolicy>,
    provisional_transactions: BTreeMap<String, ProvisionalTransaction>,
    next_provisional_transaction_id: u64,
    next_storage_tx_id: u64,
    limits: PrimadbLimits,
    network_hooks: Option<Arc<dyn NetworkHooks>>,
    vector_collections: BTreeMap<String, VectorCollectionCache>,
    text_collections: BTreeMap<String, TextCollectionCache>,
    #[cfg(not(target_arch = "wasm32"))]
    vector_cache_root: Option<std::path::PathBuf>,
    #[cfg(not(target_arch = "wasm32"))]
    text_cache_root: Option<std::path::PathBuf>,
    cache_rebuilds: BTreeSet<CacheRebuildKey>,
    cache_rebuild_wait: Arc<Condvar>,
    #[cfg(feature = "crypto")]
    security: SecurityState,
    transaction_journal: Option<TransactionJournal>,
    #[cfg(test)]
    query_candidate_projections: usize,
    #[cfg(test)]
    watch_recomputations: usize,
    #[cfg(test)]
    record_overlay_candidates_examined: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CacheRebuildKind {
    Text,
    Vector,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CacheRebuildKey {
    kind: CacheRebuildKind,
    collection: String,
}

#[derive(Debug, Clone)]
struct VectorRebuildSnapshot {
    collection: String,
    config: VectorCollectionConfig,
    records: Vec<RecordEntry>,
    source_hash: String,
    revision: u64,
    #[cfg(not(target_arch = "wasm32"))]
    cache_root: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone)]
struct TextRebuildSnapshot {
    collection: String,
    config: TextCollectionConfig,
    records: Vec<RecordEntry>,
    source_hash: String,
    revision: u64,
    #[cfg(not(target_arch = "wasm32"))]
    cache_root: Option<std::path::PathBuf>,
}

/// The identity of the state slot an operation updates.
///
/// This is deliberately typed instead of being a formatted string. The map in
/// [`CompactedOperations`] caches one key per queued operation, so appending an
/// operation does not scan the queue or reconstruct every existing key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum OperationCompactionKey {
    Field {
        node: String,
        field: String,
    },
    SetMember {
        node: String,
        field: String,
        member: String,
    },
}

impl OperationCompactionKey {
    fn from_operation(op: &Operation) -> Self {
        match &op.action {
            OperationAction::SetField { node, field, .. }
            | OperationAction::DeleteField { node, field } => Self::Field {
                node: node.clone(),
                field: field.clone(),
            },
            OperationAction::AddSetMember {
                node,
                field,
                member,
            }
            | OperationAction::RemoveSetMember {
                node,
                field,
                member,
            } => Self::SetMember {
                node: node.clone(),
                field: field.clone(),
                member: member.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, Default)]
struct CompactedOperations {
    operations: Vec<Operation>,
    indices: HashMap<OperationCompactionKey, usize>,
}

impl CompactedOperations {
    fn from_operations<I>(operations: I) -> Self
    where
        I: IntoIterator<Item = Operation>,
    {
        let mut queue = Self::default();
        for operation in operations {
            queue.push(operation);
        }
        queue
    }

    fn push(&mut self, op: Operation) {
        let key = OperationCompactionKey::from_operation(&op);
        if let Some(&index) = self.indices.get(&key) {
            let existing = &mut self.operations[index];
            if op.revision >= existing.revision {
                // Keep the original index: compaction has always preserved
                // the first occurrence's position in the operation stream.
                *existing = op;
            }
            return;
        }

        let index = self.operations.len();
        self.operations.push(op);
        self.indices.insert(key, index);
    }

    fn as_slice(&self) -> &[Operation] {
        &self.operations
    }

    fn to_vec(&self) -> Vec<Operation> {
        self.operations.clone()
    }

    fn into_operations(self) -> Vec<Operation> {
        self.operations
    }

    fn len(&self) -> usize {
        self.operations.len()
    }

    fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    fn clear(&mut self) {
        self.operations.clear();
        self.indices.clear();
    }

    fn rebuild_indices(&mut self) {
        self.indices.clear();
        for (index, operation) in self.operations.iter().enumerate() {
            self.indices
                .insert(OperationCompactionKey::from_operation(operation), index);
        }
    }

    fn drain_prefix(&mut self, count: usize) {
        debug_assert!(count <= self.operations.len());
        self.operations.drain(..count);
        self.indices.retain(|_, index| {
            if *index < count {
                false
            } else {
                *index -= count;
                true
            }
        });
    }
}

#[derive(Debug, Clone, Default)]
struct RelationshipIndex {
    outbound: BTreeMap<NodeId, BTreeSet<TraversalEdge>>,
    inbound: BTreeMap<NodeId, BTreeSet<TraversalEdge>>,
}

#[derive(Debug, Clone)]
struct TraversalFrame {
    node: NodeId,
    depth: usize,
    path: Vec<NodeId>,
    via: Option<TraversalEdge>,
}

impl RelationshipIndex {
    fn insert(&mut self, edge: TraversalEdge) {
        self.outbound
            .entry(edge.source.clone())
            .or_default()
            .insert(edge.clone());
        self.inbound
            .entry(edge.target.clone())
            .or_default()
            .insert(edge);
    }

    fn remove_source(&mut self, source: &str) {
        let Some(edges) = self.outbound.remove(source) else {
            return;
        };
        for edge in edges {
            let remove_target = if let Some(inbound) = self.inbound.get_mut(&edge.target) {
                inbound.remove(&edge);
                inbound.is_empty()
            } else {
                false
            };
            if remove_target {
                self.inbound.remove(&edge.target);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Watcher {
    anchor: NodeId,
    segments: Vec<String>,
    path_key: String,
    last_hash: Option<String>,
    sender: Sender<Option<JsonValue>>,
}

#[derive(Debug, Clone)]
struct ChangeWatcher {
    sender: Sender<ChangeEvent>,
}

#[derive(Debug, Clone)]
struct TraversalWatcher {
    anchor: NodeId,
    segments: Vec<String>,
    spec: TraversalSpec,
    dependency_paths: BTreeSet<String>,
    last_hash: Option<String>,
    sender: Sender<TraversalResult>,
}

#[derive(Debug, Clone)]
struct RecordWatcher {
    scan: RecordScan,
    last_hash: Option<String>,
    sender: Sender<RecordScanResult>,
}

#[derive(Debug, Clone)]
struct VectorWatcher {
    collection: String,
    query: Vec<f32>,
    spec: VectorSearchSpec,
    last_hash: Option<String>,
    sender: Sender<VectorSearchResult>,
}

#[derive(Debug, Clone)]
struct TextWatcher {
    source: TextSearchSource,
    query: String,
    spec: TextSearchSpec,
    last_hash: Option<String>,
    sender: Sender<TextSearchResult>,
}

#[derive(Debug, Clone)]
enum Cursor {
    Node(NodeId),
    Field { node: NodeId, field: String },
}

#[derive(Debug, Clone)]
enum QueryCandidateSource {
    Node(NodeId),
    Field { node: NodeId, field: String },
}

#[derive(Debug, Clone)]
struct QueryCandidate {
    key: String,
    source: QueryCandidateSource,
}

#[derive(Debug)]
struct EvaluatedQueryCandidate {
    candidate: QueryCandidate,
    full_value: Option<JsonValue>,
    order_value: Option<JsonValue>,
}

enum QueryValuePath<'a> {
    Full,
    Key,
    Segments(Vec<&'a str>),
}

impl<'a> QueryValuePath<'a> {
    fn new(path: &'a str) -> Self {
        match path {
            "" | "$value" => Self::Full,
            "$key" => Self::Key,
            _ => Self::Segments(path.split('.').collect()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationOrigin {
    Local,
    Remote,
}

#[derive(Clone, Copy)]
enum OperationQueue {
    Pending,
    Unflushed,
}

#[derive(Debug, Clone, Default)]
struct ChangeImpact {
    data_changed: bool,
    full_refresh: bool,
    touched_paths: Vec<String>,
    records_changed: bool,
    touched_record_keys: Vec<String>,
}

impl ChangeImpact {
    fn pending_only() -> Self {
        Self::default()
    }

    fn full_refresh() -> Self {
        Self {
            data_changed: true,
            full_refresh: true,
            touched_paths: Vec::new(),
            records_changed: false,
            touched_record_keys: Vec::new(),
        }
    }

    fn from_ops(ops: &[Operation], nodes: &BTreeMap<NodeId, NodeState>) -> Self {
        let mut touched_paths = BTreeSet::new();
        let mut touched_record_keys = BTreeSet::new();
        let mut records_changed = false;
        let mut record_keys_complete = true;
        for op in ops {
            touched_paths.insert(operation_touched_path(op));
            if operation_is_record_op(op) {
                records_changed = true;
                if let Some(key) = operation_touched_record_key(op, nodes) {
                    touched_record_keys.insert(key);
                } else {
                    record_keys_complete = false;
                }
            }
        }
        Self {
            data_changed: !ops.is_empty(),
            full_refresh: false,
            touched_paths: touched_paths.into_iter().collect(),
            records_changed,
            touched_record_keys: if record_keys_complete {
                touched_record_keys.into_iter().collect()
            } else {
                Vec::new()
            },
        }
    }

    fn is_empty(&self) -> bool {
        !self.data_changed
            && !self.full_refresh
            && self.touched_paths.is_empty()
            && !self.records_changed
            && self.touched_record_keys.is_empty()
    }
}

struct TransactionJournal {
    clock: HybridClock,
    nodes: BTreeMap<NodeId, Option<NodeState>>,
    record_overlay: BTreeMap<String, Option<RecordEntry>>,
    record_overlay_node_keys: BTreeMap<NodeId, String>,
    pending_ops: OperationQueueUndo,
    unflushed_ops: OperationQueueUndo,
    missing_nodes: BTreeMap<NodeId, bool>,
    scheduled_node_fetches: BTreeMap<NodeId, bool>,
}

impl TransactionJournal {
    fn begin(inner: &Inner) -> Self {
        Self {
            clock: inner.clock.clone(),
            nodes: BTreeMap::new(),
            record_overlay: inner.record_overlay.clone(),
            record_overlay_node_keys: inner.record_overlay_node_keys.clone(),
            pending_ops: OperationQueueUndo::new(inner.pending_ops.len()),
            unflushed_ops: OperationQueueUndo::new(inner.unflushed_ops.len()),
            missing_nodes: BTreeMap::new(),
            scheduled_node_fetches: BTreeMap::new(),
        }
    }

    fn restore(self, inner: &mut Inner) {
        inner.clock = self.clock;
        self.pending_ops.restore(&mut inner.pending_ops);
        self.unflushed_ops.restore(&mut inner.unflushed_ops);

        let touched_nodes = self.nodes.keys().cloned().collect::<Vec<_>>();
        for (node, previous) in self.nodes {
            match previous {
                Some(state) => {
                    inner.nodes.insert(node, state);
                }
                None => {
                    inner.nodes.remove(&node);
                }
            }
        }
        for (node, was_missing) in self.missing_nodes {
            restore_set_membership(&mut inner.missing_nodes, node, was_missing);
        }
        for (node, was_scheduled) in self.scheduled_node_fetches {
            restore_set_membership(&mut inner.scheduled_node_fetches, node, was_scheduled);
        }
        for node in touched_nodes {
            inner.reindex_node_relationships(&node);
        }
        inner.record_overlay = self.record_overlay;
        inner.record_overlay_node_keys = self.record_overlay_node_keys;
    }
}

struct OperationQueueUndo {
    initial_len: usize,
    replaced: BTreeMap<usize, Operation>,
}

impl OperationQueueUndo {
    fn new(initial_len: usize) -> Self {
        Self {
            initial_len,
            replaced: BTreeMap::new(),
        }
    }

    fn restore(self, queue: &mut CompactedOperations) {
        queue.operations.truncate(self.initial_len);
        for (index, operation) in self.replaced {
            if index < queue.operations.len() {
                queue.operations[index] = operation;
            }
        }
        queue.rebuild_indices();
    }
}

fn restore_set_membership(set: &mut BTreeSet<NodeId>, node: NodeId, contained: bool) {
    if contained {
        set.insert(node);
    } else {
        set.remove(&node);
    }
}

fn storage_metadata_from_inner(inner: &Inner, next_tx_id: u64) -> crate::StorageMetadata {
    let mut metadata =
        build_storage_metadata(inner.clock.clone(), inner.pending_ops.to_vec(), next_tx_id);
    metadata.scope_policies = inner.scope_policies.clone();
    metadata.provisional_transactions = inner.provisional_transactions.clone();
    metadata.next_provisional_transaction_id = inner.next_provisional_transaction_id;
    metadata
}

impl ChangeEvent {
    pub fn merge(&mut self, other: Self) {
        self.revision = self.revision.max(other.revision);
        self.pending_ops = other.pending_ops;
        self.data_changed |= other.data_changed;
        self.full_refresh |= other.full_refresh;
        if !other.touched_paths.is_empty() {
            let mut merged: BTreeSet<_> = self.touched_paths.iter().cloned().collect();
            merged.extend(other.touched_paths);
            self.touched_paths = merged.into_iter().collect();
        }
        self.records_changed |= other.records_changed;
        if !other.touched_record_keys.is_empty() {
            let mut merged: BTreeSet<_> = self.touched_record_keys.iter().cloned().collect();
            merged.extend(other.touched_record_keys);
            self.touched_record_keys = merged.into_iter().collect();
        }
    }
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

impl std::fmt::Debug for Primadb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Primadb")
            .field("replica_id", &self.replica_id())
            .finish_non_exhaustive()
    }
}

impl Primadb {
    pub fn with_replica_id(replica_id: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                clock: HybridClock::with_actor(replica_id),
                nodes: Default::default(),
                record_overlay: Default::default(),
                record_overlay_node_keys: Default::default(),
                pending_ops: CompactedOperations::default(),
                unflushed_ops: CompactedOperations::default(),
                subscriptions: Default::default(),
                traversal_subscriptions: Default::default(),
                record_subscriptions: Default::default(),
                vector_subscriptions: Default::default(),
                text_subscriptions: Default::default(),
                change_subscriptions: Default::default(),
                next_subscription_id: 0,
                next_traversal_subscription_id: 0,
                next_record_subscription_id: 0,
                next_vector_subscription_id: 0,
                next_text_subscription_id: 0,
                next_change_subscription_id: 0,
                change_revision: 0,
                persistence: None,
                storage_adapter: None,
                storage_engine: None,
                external_storage_hooks: 0,
                blob_store: None,
                missing_nodes: BTreeSet::new(),
                relationship_index: RelationshipIndex::default(),
                node_fetch_schedulers: BTreeMap::new(),
                #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
                next_node_fetch_scheduler_id: 0,
                scheduled_node_fetches: BTreeSet::new(),
                scope_policies: BTreeMap::new(),
                provisional_transactions: BTreeMap::new(),
                next_provisional_transaction_id: 0,
                next_storage_tx_id: 1,
                limits: PrimadbLimits::default(),
                network_hooks: None,
                vector_collections: BTreeMap::new(),
                text_collections: BTreeMap::new(),
                #[cfg(not(target_arch = "wasm32"))]
                vector_cache_root: None,
                #[cfg(not(target_arch = "wasm32"))]
                text_cache_root: None,
                cache_rebuilds: BTreeSet::new(),
                cache_rebuild_wait: Arc::new(Condvar::new()),
                #[cfg(feature = "crypto")]
                security: SecurityState::default(),
                transaction_journal: None,
                #[cfg(test)]
                query_candidate_projections: 0,
                #[cfg(test)]
                watch_recomputations: 0,
                #[cfg(test)]
                record_overlay_candidates_examined: 0,
            })),
        }
    }

    pub fn replica_id(&self) -> String {
        self.inner.lock().unwrap().clock.actor().to_owned()
    }

    fn next_vector_write_id(&self) -> String {
        let mut inner = self.inner.lock().unwrap();
        let actor = inner.clock.actor().to_owned();
        let op_id = inner.clock.next_op_id("vector-write");
        format!("{actor}:{op_id}")
    }

    pub fn root(&self, node: impl Into<String>) -> Chain {
        Chain {
            db: self.clone(),
            anchor: node.into(),
            segments: Vec::new(),
        }
    }

    pub fn scope(&self, root: impl Into<String>) -> Scope {
        Scope {
            db: self.clone(),
            root: root.into(),
        }
    }

    pub fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<T>,
    {
        self.run_local_transaction(f).map(|(value, _, _)| value)
    }

    pub fn apply_transaction_steps(
        &self,
        steps: Vec<TransactionStep>,
    ) -> Result<TransactionReport> {
        self.validate_transaction_scopes(None, &steps)?;
        let (_, member_ids, operation_count) =
            self.run_local_transaction(|tx| apply_transaction_steps(tx, &steps))?;
        Ok(TransactionReport {
            status: TransactionStatus::Committed,
            operation_count,
            member_ids,
            proposal_id: None,
        })
    }

    pub fn get_record(&self, key: &str) -> Result<Option<RecordEntry>> {
        let mut inner = self.inner.lock().unwrap();
        record_entry_from_inner(&mut inner, key)
    }

    pub fn get_many_records<I>(&self, keys: I) -> Result<Vec<Option<RecordEntry>>>
    where
        I: IntoIterator,
        I::Item: AsRef<str>,
    {
        keys.into_iter()
            .map(|key| self.get_record(key.as_ref()))
            .collect()
    }

    pub fn scan_records(&self, scan: RecordScan) -> Result<RecordScanResult> {
        let storage_engine = {
            let engine = { self.inner.lock().unwrap().storage_engine.clone() };
            engine
        };
        let mut inner = self.inner.lock().unwrap();
        let entries = collect_record_entries_for_scan(&mut inner, &scan, storage_engine)?;
        Ok(record_scan_result(entries, &scan))
    }

    pub fn watch_records(&self, scan: RecordScan) -> Result<RecordWatchSubscription> {
        self.subscribe_to_records(scan)
    }

    pub fn apply_record_batch(&self, batch: RecordBatch) -> Result<RecordBatchReport> {
        let RecordBatch {
            preconditions,
            mutations,
        } = batch;

        let (mut report, _, operation_count) = self.run_local_transaction(|tx| {
            let mut report = RecordBatchReport::default();
            for precondition in &preconditions {
                assert_record_precondition(tx.inner, precondition)?;
            }
            report.preconditions = preconditions.len();
            for mutation in mutations {
                match mutation {
                    RecordMutation::Put { key, value } => {
                        put_record_inner(tx.inner, &key, value)?;
                        report.puts += 1;
                    }
                    RecordMutation::Delete { key } => {
                        delete_record_inner(tx.inner, &key);
                        report.deletes += 1;
                    }
                    RecordMutation::DeleteRange { scan } => {
                        let entries = collect_record_entries_for_scan_locked(tx.inner, &scan)?;
                        report.range_deletes += entries.len();
                        for entry in entries {
                            delete_record_inner(tx.inner, &entry.key);
                            report.deletes += 1;
                        }
                    }
                }
            }
            Ok(report)
        })?;
        report.operation_count = operation_count;
        Ok(report)
    }

    pub fn put_record(&self, key: impl Into<String>, value: RecordValue) -> Result<()> {
        self.apply_record_batch(RecordBatch {
            preconditions: Vec::new(),
            mutations: vec![RecordMutation::Put {
                key: key.into(),
                value,
            }],
        })?;
        Ok(())
    }

    pub fn put_record_json(&self, key: impl Into<String>, value: JsonValue) -> Result<()> {
        self.put_record(key, RecordValue::Json(value))
    }

    pub fn put_record_value<T: Serialize>(&self, key: impl Into<String>, value: T) -> Result<()> {
        self.put_record_json(key, serde_json::to_value(value)?)
    }

    pub fn put_record_bytes(&self, key: impl Into<String>, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.put_record(key, RecordValue::Bytes(BinaryBytes::from(bytes.as_ref())))
    }

    pub fn put_record_blob(
        &self,
        key: impl Into<String>,
        data: impl AsRef<[u8]>,
        media_type: Option<&str>,
    ) -> Result<BlobRef> {
        let reference = self.store_blob(data.as_ref(), media_type)?;
        self.put_record(key, RecordValue::Blob(reference.clone()))?;
        Ok(reference)
    }

    pub fn create_vector_collection(
        &self,
        name: impl AsRef<str>,
        mut config: VectorCollectionConfig,
    ) -> Result<()> {
        if config.chunking.chunk_bytes == 0 {
            config.chunking = Default::default();
        }
        validate_collection_config(&config)?;
        self.put_record_value(vector_collection_meta_key(name.as_ref()), config)
    }

    pub fn vector_collection_config(&self, collection: &str) -> Result<VectorCollectionConfig> {
        let Some(entry) = self.get_record(&vector_collection_meta_key(collection))? else {
            return Err(PrimadbError::Message(format!(
                "vector collection `{collection}` does not exist"
            )));
        };
        collection_config_from_record(&entry)
    }

    pub fn put_vector(
        &self,
        collection: impl AsRef<str>,
        id: impl AsRef<str>,
        vector: impl AsRef<[f32]>,
        metadata: Option<JsonValue>,
    ) -> Result<()> {
        let collection = collection.as_ref();
        let id = id.as_ref();
        let config = self.vector_collection_config(collection)?;
        let vector = vector.as_ref();
        validate_vector(vector, config.dim)?;

        let write_id = self.next_vector_write_id();
        let bytes = encode_f32_le(vector);
        let checksum = checksum_bytes(&bytes);
        let chunk_size = config.chunking.chunk_bytes.max(1);
        let chunk_count = bytes.len().div_ceil(chunk_size).max(1);
        let meta = VectorItemMeta {
            id: id.to_owned(),
            write_id: write_id.clone(),
            dim: config.dim,
            encoding: crate::vector::VECTOR_ENCODING_F32_LE.to_owned(),
            byte_length: bytes.len(),
            checksum: checksum.clone(),
            chunk_count,
            metadata,
            deleted: false,
            updated_at: Some(now_millis().to_string()),
        };

        let mut mutations = Vec::with_capacity(chunk_count + 2);
        mutations.push(RecordMutation::DeleteRange {
            scan: RecordScan {
                prefix: Some(vector_item_chunks_prefix(collection, id)),
                ..RecordScan::default()
            },
        });
        mutations.push(RecordMutation::Put {
            key: vector_item_meta_key(collection, id),
            value: RecordValue::Json(serde_json::to_value(meta)?),
        });
        for (chunk_index, chunk) in bytes.chunks(chunk_size).enumerate() {
            let header = crate::VectorChunkHeader {
                write_id: write_id.clone(),
                chunk_index,
                chunk_count,
                byte_offset: chunk_index * chunk_size,
                checksum: checksum_bytes(chunk),
            };
            mutations.push(RecordMutation::Put {
                key: vector_item_chunk_key(collection, id, chunk_index),
                value: RecordValue::Bytes(encode_vector_chunk(&header, chunk)?),
            });
        }

        self.apply_record_batch(RecordBatch {
            preconditions: Vec::new(),
            mutations,
        })?;
        Ok(())
    }

    pub fn delete_vector(&self, collection: impl AsRef<str>, id: impl AsRef<str>) -> Result<()> {
        let collection = collection.as_ref();
        let id = id.as_ref();
        let config = self.vector_collection_config(collection)?;
        let write_id = self.next_vector_write_id();
        let meta = VectorItemMeta {
            id: id.to_owned(),
            write_id,
            dim: config.dim,
            encoding: crate::vector::VECTOR_ENCODING_F32_LE.to_owned(),
            byte_length: 0,
            checksum: checksum_bytes(&[]),
            chunk_count: 0,
            metadata: None,
            deleted: true,
            updated_at: Some(now_millis().to_string()),
        };
        self.apply_record_batch(RecordBatch {
            preconditions: Vec::new(),
            mutations: vec![
                RecordMutation::Put {
                    key: vector_item_meta_key(collection, id),
                    value: RecordValue::Json(serde_json::to_value(meta)?),
                },
                RecordMutation::DeleteRange {
                    scan: RecordScan {
                        prefix: Some(vector_item_chunks_prefix(collection, id)),
                        ..RecordScan::default()
                    },
                },
            ],
        })?;
        Ok(())
    }

    pub fn get_vector(
        &self,
        collection: impl AsRef<str>,
        id: impl AsRef<str>,
    ) -> Result<Option<VectorEntry>> {
        let collection = collection.as_ref();
        let id = id.as_ref();
        self.ensure_vector_collection_ready(collection)?;
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .vector_collections
            .get(collection)
            .and_then(|cache| cache.entries.get(id))
            .map(|entry| VectorEntry {
                id: id.to_owned(),
                vector: entry.vector.clone(),
                metadata: entry.metadata.clone(),
                write_id: entry.write_id.clone(),
                checksum: entry.checksum.clone(),
            }))
    }

    pub fn search_vectors(
        &self,
        collection: impl AsRef<str>,
        query: impl AsRef<[f32]>,
        spec: VectorSearchSpec,
    ) -> Result<VectorSearchResult> {
        let collection = collection.as_ref();
        let query = query.as_ref();
        self.ensure_vector_collection_ready(collection)?;
        let inner = self.inner.lock().unwrap();
        let cache = inner.vector_collections.get(collection).ok_or_else(|| {
            PrimadbError::Message(format!("vector collection `{collection}` is not loaded"))
        })?;
        if spec.stale_policy == VectorStalePolicy::Error && cache.state != VectorManagerState::Ready
        {
            return Err(PrimadbError::Message(format!(
                "vector collection `{collection}` is {:?}",
                cache.state
            )));
        }
        search_vector_collection(cache, query, &spec)
    }

    pub fn watch_vector_search(
        &self,
        collection: impl Into<String>,
        query: impl AsRef<[f32]>,
        spec: VectorSearchSpec,
    ) -> Result<VectorWatchSubscription> {
        self.subscribe_to_vector_search(collection.into(), query.as_ref().to_vec(), spec)
    }

    pub fn vector_index_stats(&self, collection: impl AsRef<str>) -> Result<VectorIndexStats> {
        let collection = collection.as_ref();
        self.ensure_vector_collection_ready(collection)?;
        let inner = self.inner.lock().unwrap();
        inner
            .vector_collections
            .get(collection)
            .map(VectorCollectionCache::stats)
            .ok_or_else(|| {
                PrimadbError::Message(format!("vector collection `{collection}` is not loaded"))
            })
    }

    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn export_vector_cache_files(
        &self,
        collection: impl AsRef<str>,
    ) -> Result<crate::VectorCacheFiles> {
        let collection = collection.as_ref();
        self.ensure_vector_collection_ready(collection)?;
        let inner = self.inner.lock().unwrap();
        let cache = inner.vector_collections.get(collection).ok_or_else(|| {
            PrimadbError::Message(format!("vector collection `{collection}` is not loaded"))
        })?;
        let cache = cache.clone();
        let revision = inner.change_revision;
        drop(inner);
        let mut files = build_vector_cache_files(collection, &cache, now_millis().to_string())?;
        files.manifest.source_revision = Some(revision);
        Ok(files)
    }

    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    pub(crate) fn import_vector_cache_files(
        &self,
        collection: impl AsRef<str>,
        files: crate::VectorCacheFiles,
    ) -> Result<()> {
        let collection = collection.as_ref();
        let (config, source_hash, revision) = {
            let mut inner = self.inner.lock().unwrap();
            let (config, _records, source_hash) =
                vector_collection_records_and_source_hash_locked(&mut inner, collection)?;
            (config, source_hash, inner.change_revision)
        };
        if files.manifest.collection != collection
            || files.manifest.record_prefix != vector_collection_items_prefix(collection)
        {
            return Err(PrimadbError::Message(
                "vector cache manifest belongs to another collection".to_owned(),
            ));
        }
        let cache = collection_cache_from_cache_files(config, files, &source_hash)?;
        let mut inner = self.inner.lock().unwrap();
        if inner.change_revision != revision {
            return Err(PrimadbError::Message(
                "vector cache source changed while it was being imported".to_owned(),
            ));
        }
        inner
            .vector_collections
            .insert(collection.to_owned(), cache);
        Ok(())
    }

    pub fn vector_presence_capabilities(&self) -> Vec<String> {
        let mut capabilities = vec![
            "vector_exact".to_owned(),
            "vector_metric:cosine".to_owned(),
            "vector_metric:l2".to_owned(),
            "vector_metric:dot".to_owned(),
        ];
        #[cfg(feature = "vector-edgevec")]
        capabilities.push("vector_ann:edgevec".to_owned());

        let mut inner = self.inner.lock().unwrap();
        let scan = RecordScan {
            prefix: Some(format!("{}/", crate::vector::VECTOR_RECORD_PREFIX)),
            ..RecordScan::default()
        };
        let Ok(entries) = collect_record_entries_for_scan_locked(&mut inner, &scan) else {
            return capabilities;
        };
        for entry in entries {
            if !entry.key.ends_with("/meta") || entry.key.contains("/items/") {
                continue;
            }
            let Some(collection) = vector_collection_from_record_key(&entry.key) else {
                continue;
            };
            let Ok(config) = collection_config_from_record(&entry) else {
                continue;
            };
            let state = inner
                .vector_collections
                .get(&collection)
                .map(|cache| cache.state)
                .unwrap_or(VectorManagerState::Ready);
            let backend = config.backend.unwrap_or_default();
            capabilities.push(format!(
                "vector_collection:{}:{}:{}:{}:{}",
                crate::encode_component(&collection),
                config.dim,
                vector_metric_capability_name(config.metric),
                vector_manager_state_capability_name(state),
                vector_backend_capability_name(backend)
            ));
        }
        capabilities.sort();
        capabilities.dedup();
        capabilities
    }

    pub fn create_text_collection(
        &self,
        name: impl AsRef<str>,
        config: TextCollectionConfig,
    ) -> Result<()> {
        validate_text_collection_config(&config)?;
        self.put_record_value(text_collection_config_key(name.as_ref()), config)
    }

    pub fn text_collection_config(&self, collection: &str) -> Result<TextCollectionConfig> {
        let Some(entry) = self.get_record(&text_collection_config_key(collection))? else {
            return Err(PrimadbError::Message(format!(
                "text collection `{collection}` does not exist"
            )));
        };
        text_collection_config_from_record(&entry)
    }

    pub fn put_text_document(
        &self,
        collection: impl AsRef<str>,
        document: TextDocument,
    ) -> Result<()> {
        let collection = collection.as_ref();
        self.text_collection_config(collection)?;
        if document.id.trim().is_empty() {
            return Err(PrimadbError::Message(
                "text document id must not be empty".to_owned(),
            ));
        }
        self.put_record_value(text_document_key(collection, &document.id), document)
    }

    pub fn delete_text_document(
        &self,
        collection: impl AsRef<str>,
        id: impl AsRef<str>,
    ) -> Result<()> {
        self.delete_record(text_document_key(collection.as_ref(), id.as_ref()))
    }

    pub fn get_text_document(
        &self,
        collection: impl AsRef<str>,
        id: impl AsRef<str>,
    ) -> Result<Option<TextDocument>> {
        let collection = collection.as_ref();
        let id = id.as_ref();
        self.ensure_text_collection_ready(collection)?;
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .text_collections
            .get(collection)
            .and_then(|cache| cache.documents.get(id))
            .cloned())
    }

    pub fn text_search(
        &self,
        source: impl Into<TextSearchSource>,
        query: impl AsRef<str>,
        spec: TextSearchSpec,
    ) -> Result<TextSearchResult> {
        self.execute_text_search(source.into(), query.as_ref(), spec)
    }

    pub fn watch_text_search(
        &self,
        source: impl Into<TextSearchSource>,
        query: impl AsRef<str>,
        spec: TextSearchSpec,
    ) -> Result<TextWatchSubscription> {
        self.subscribe_to_text_search(source.into(), query.as_ref().to_owned(), spec)
    }

    pub fn text_index_stats(&self, collection: impl AsRef<str>) -> Result<TextIndexStats> {
        let collection = collection.as_ref();
        self.ensure_text_collection_ready(collection)?;
        let inner = self.inner.lock().unwrap();
        inner
            .text_collections
            .get(collection)
            .map(TextCollectionCache::stats)
            .ok_or_else(|| {
                PrimadbError::Message(format!("text collection `{collection}` is not loaded"))
            })
    }

    // Persistent text cache import/export is exposed to native storage paths first.
    #[allow(dead_code)]
    pub(crate) fn export_text_cache_files(
        &self,
        collection: impl AsRef<str>,
    ) -> Result<TextCacheFiles> {
        let collection = collection.as_ref();
        self.ensure_text_collection_ready(collection)?;
        let inner = self.inner.lock().unwrap();
        let cache = inner.text_collections.get(collection).ok_or_else(|| {
            PrimadbError::Message(format!("text collection `{collection}` is not loaded"))
        })?;
        let cache = cache.clone();
        let revision = inner.change_revision;
        drop(inner);
        let mut files = text_cache_files(collection, &cache, now_millis().to_string())?;
        files.manifest.source_revision = Some(revision);
        Ok(files)
    }

    // Persistent text cache import/export is exposed to native storage paths first.
    #[allow(dead_code)]
    pub(crate) fn import_text_cache_files(
        &self,
        collection: impl AsRef<str>,
        files: TextCacheFiles,
    ) -> Result<()> {
        let collection = collection.as_ref();
        let (config, source_hash, revision) = {
            let mut inner = self.inner.lock().unwrap();
            let (config, _records, source_hash) =
                text_collection_records_and_source_hash_locked(&mut inner, collection)?;
            (config, source_hash, inner.change_revision)
        };
        if files.manifest.collection != collection
            || files.manifest.record_prefix != text_collection_docs_prefix(collection)
        {
            return Err(PrimadbError::Message(
                "text cache manifest belongs to another collection".to_owned(),
            ));
        }
        let cache = collection_cache_from_text_cache_files(config, files, &source_hash)?;
        let mut inner = self.inner.lock().unwrap();
        if inner.change_revision != revision {
            return Err(PrimadbError::Message(
                "text cache source changed while it was being imported".to_owned(),
            ));
        }
        inner.text_collections.insert(collection.to_owned(), cache);
        Ok(())
    }

    pub fn text_presence_capabilities(&self) -> Vec<String> {
        let mut capabilities = vec![
            "pull_text_search".to_owned(),
            "watch_text_search".to_owned(),
            "text_bm25_exact".to_owned(),
        ];

        let mut inner = self.inner.lock().unwrap();
        let scan = RecordScan {
            prefix: Some(format!("{}/", crate::text_search::TEXT_RECORD_PREFIX)),
            ..RecordScan::default()
        };
        let Ok(entries) = collect_record_entries_for_scan_locked(&mut inner, &scan) else {
            return capabilities;
        };
        for entry in entries {
            if !entry.key.ends_with("/config") {
                continue;
            }
            let Some(collection) = text_collection_from_record_key(&entry.key) else {
                continue;
            };
            let Ok(config) = text_collection_config_from_record(&entry) else {
                continue;
            };
            let state = inner
                .text_collections
                .get(&collection)
                .map(|cache| cache.state)
                .unwrap_or_default();
            capabilities.push(format!(
                "text_collection:{}:{}:{}:{}",
                crate::encode_component(&collection),
                text_manager_state_capability_name(state),
                "exact",
                config.analyzer.version
            ));
        }
        capabilities.sort();
        capabilities.dedup();
        capabilities
    }

    pub fn delete_record(&self, key: impl Into<String>) -> Result<()> {
        self.apply_record_batch(RecordBatch {
            preconditions: Vec::new(),
            mutations: vec![RecordMutation::Delete { key: key.into() }],
        })?;
        Ok(())
    }

    pub fn sync_storage(&self) -> Result<crate::StorageSyncReport> {
        let engine = self
            .inner
            .lock()
            .unwrap()
            .storage_engine
            .clone()
            .ok_or_else(|| {
                PrimadbError::Message("incremental storage is not configured".to_owned())
            })?;
        engine.sync()
    }

    pub fn storage_recovery_report(&self) -> Option<crate::StorageRecoveryReport> {
        self.inner
            .lock()
            .unwrap()
            .storage_engine
            .clone()
            .and_then(|engine| engine.recovery_report())
    }

    pub fn scope_policy(&self, scope: &str) -> Option<ScopePolicy> {
        self.inner
            .lock()
            .unwrap()
            .scope_policies
            .get(scope)
            .cloned()
    }

    fn configure_scope_policy(&self, scope: &str, policy: ScopePolicy) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.scope_policies.insert(scope.to_owned(), policy);
        }
        self.finalize_change(ChangeImpact::pending_only())
    }

    fn provisional_transactions_for_scope(&self, scope: &str) -> Vec<ProvisionalTransaction> {
        self.inner
            .lock()
            .unwrap()
            .provisional_transactions
            .values()
            .filter(|proposal| proposal.scope == scope)
            .cloned()
            .collect()
    }

    fn queue_provisional_transaction(
        &self,
        scope: &str,
        steps: Vec<TransactionStep>,
        options: TransactionOptions,
    ) -> Result<TransactionReport> {
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_provisional_transaction_id =
                inner.next_provisional_transaction_id.saturating_add(1);
            let id = format!(
                "{}/proposal/{:x}",
                inner.clock.actor(),
                inner.next_provisional_transaction_id
            );
            let proposal = ProvisionalTransaction {
                id: id.clone(),
                scope: scope.to_owned(),
                created_at_millis: now_millis(),
                steps,
                options,
            };
            inner.provisional_transactions.insert(id.clone(), proposal);
            id
        };
        self.finalize_change(ChangeImpact::pending_only())?;
        Ok(TransactionReport {
            status: TransactionStatus::Provisional,
            operation_count: 0,
            member_ids: Vec::new(),
            proposal_id: Some(id),
        })
    }

    fn run_local_transaction<F, T>(&self, f: F) -> Result<(T, Vec<String>, usize)>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<T>,
    {
        self.run_local_transaction_in_scope(None, f)
    }

    fn run_local_transaction_in_scope<F, T>(
        &self,
        scope_root: Option<NodeId>,
        f: F,
    ) -> Result<(T, Vec<String>, usize)>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<T>,
    {
        let (result, member_ids, operation_count, impact) = {
            let mut inner = self.inner.lock().unwrap();
            inner.transaction_journal = Some(TransactionJournal::begin(&inner));
            let start_unflushed_len = inner.unflushed_ops.len();
            let (result, member_ids) = {
                let mut tx = Transaction {
                    inner: &mut inner,
                    scope_root,
                    member_ids: Vec::new(),
                };
                let result = f(&mut tx);
                let member_ids = tx.member_ids.clone();
                (result, member_ids)
            };

            match result {
                Ok(value) => {
                    let ops = inner.unflushed_ops.as_slice()[start_unflushed_len..].to_vec();
                    let operation_count = ops.len();
                    inner.transaction_journal.take();
                    (
                        Ok(value),
                        member_ids,
                        operation_count,
                        ChangeImpact::from_ops(&ops, &inner.nodes),
                    )
                }
                Err(error) => {
                    let rollback = inner
                        .transaction_journal
                        .take()
                        .expect("local transaction journal missing");
                    rollback.restore(&mut inner);
                    (Err(error), Vec::new(), 0, ChangeImpact::pending_only())
                }
            }
        };

        let value = result?;
        if !impact.is_empty() {
            self.finalize_change(impact)?;
        }
        Ok((value, member_ids, operation_count))
    }

    fn validate_transaction_scopes(
        &self,
        required_scope: Option<&str>,
        steps: &[TransactionStep],
    ) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        let mut matched_strict_scope: Option<String> = required_scope.map(str::to_owned);
        let mut saw_unscoped_path = false;
        for path in steps.iter().map(transaction_step_path) {
            if let Some(required) = required_scope {
                let display = path.path();
                if !node_matches_root(&display, required) {
                    return Err(PrimadbError::StrictScopeConflict {
                        message: format!("path `{display}` is outside scope `{required}`"),
                    });
                }
            }

            let Some((scope, policy)) = inner.policy_for_path(&path.path()) else {
                if required_scope.is_none() {
                    saw_unscoped_path = true;
                }
                if matched_strict_scope.is_some() && required_scope.is_none() {
                    return Err(PrimadbError::StrictScopeConflict {
                        message: "transaction mixes scoped and unscoped paths".to_owned(),
                    });
                }
                continue;
            };

            if policy.consistency != ScopeConsistency::Eventual {
                if saw_unscoped_path && required_scope.is_none() {
                    return Err(PrimadbError::StrictScopeConflict {
                        message: "transaction mixes scoped and unscoped paths".to_owned(),
                    });
                }
                match &matched_strict_scope {
                    Some(current) if current != scope => {
                        return Err(PrimadbError::StrictScopeConflict {
                            message: format!("transaction touches both `{current}` and `{scope}`"),
                        });
                    }
                    None => matched_strict_scope = Some(scope.to_owned()),
                    Some(_) => {}
                }
            }
        }
        Ok(())
    }

    pub fn snapshot(&self) -> DatabaseSnapshot {
        let (
            engine,
            clock,
            pending_ops,
            nodes,
            scope_policies,
            provisional_transactions,
            next_provisional_transaction_id,
        ) = {
            let inner = self.inner.lock().unwrap();
            (
                inner.storage_engine.clone(),
                inner.clock.clone(),
                inner.pending_ops.to_vec(),
                inner.nodes.clone(),
                inner.scope_policies.clone(),
                inner.provisional_transactions.clone(),
                inner.next_provisional_transaction_id,
            )
        };
        if let Some(engine) = engine {
            if let Ok(mut snapshot) = engine.export_snapshot(None) {
                snapshot.clock = clock;
                snapshot.pending_ops = pending_ops;
                snapshot.scope_policies = scope_policies;
                snapshot.provisional_transactions = provisional_transactions;
                snapshot.next_provisional_transaction_id = next_provisional_transaction_id;
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
            scope_policies,
            provisional_transactions,
            next_provisional_transaction_id,
        }
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn full_storage_transaction(&self) -> StorageTransaction {
        let inner = self.inner.lock().unwrap();
        let tx_id = inner.next_storage_tx_id;
        let metadata = storage_metadata_from_inner(&inner, tx_id.saturating_add(1));
        let mut transaction = build_storage_transaction(tx_id, metadata, inner.nodes.clone());
        transaction.journal_ops = inner.unflushed_ops.to_vec();
        transaction
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn full_storage_transaction_without_pending_ops(&self) -> StorageTransaction {
        let mut transaction = self.full_storage_transaction();
        transaction.metadata.pending_ops.clear();
        transaction
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn incremental_storage_transaction(&self) -> StorageTransaction {
        let inner = self.inner.lock().unwrap();
        let tx_id = inner.next_storage_tx_id;
        let metadata = storage_metadata_from_inner(&inner, tx_id.saturating_add(1));
        if inner.unflushed_ops.is_empty() {
            build_storage_transaction(tx_id, metadata, BTreeMap::new())
        } else {
            build_storage_transaction_from_ops(
                tx_id,
                metadata,
                &inner.nodes,
                inner.unflushed_ops.as_slice(),
            )
        }
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn incremental_storage_transaction_without_pending_ops(&self) -> StorageTransaction {
        let mut transaction = self.incremental_storage_transaction();
        transaction.metadata.pending_ops.clear();
        transaction
    }

    pub(crate) fn mark_storage_transaction_flushed(
        &self,
        transaction: &StorageTransaction,
    ) -> Result<()> {
        self.mark_durable_operations_flushed(&transaction.journal_ops)?;
        let mut inner = self.inner.lock().unwrap();
        inner.next_storage_tx_id = inner
            .next_storage_tx_id
            .max(transaction.id.saturating_add(1));
        inner.clear_flushed_record_overlay(transaction);
        Ok(())
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn register_external_storage_hook(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.external_storage_hooks = inner.external_storage_hooks.saturating_add(1);
    }

    #[cfg(any(test, target_arch = "wasm32"))]
    pub(crate) fn unregister_external_storage_hook(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.external_storage_hooks = inner.external_storage_hooks.saturating_sub(1);
    }

    fn mark_durable_operations_flushed(&self, ops: &[Operation]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if !ops.is_empty() {
            let saved = ops.len();
            if inner.unflushed_ops.len() < saved || inner.unflushed_ops.as_slice()[..saved] != *ops
            {
                return Ok(());
            }
            inner.unflushed_ops.drain_prefix(saved);
        }
        Ok(())
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
            inner.rebuild_record_overlay();
            inner.pending_ops = CompactedOperations::from_operations(snapshot.pending_ops);
            inner.scope_policies = snapshot.scope_policies;
            inner.provisional_transactions = snapshot.provisional_transactions;
            inner.next_provisional_transaction_id = snapshot.next_provisional_transaction_id;
            inner.unflushed_ops.clear();
            inner.missing_nodes.clear();
            inner.scheduled_node_fetches.clear();
            inner.rebuild_relationship_index();
        }
        self.finalize_change(ChangeImpact::full_refresh())
    }

    pub fn merge_snapshot(&self, snapshot: DatabaseSnapshot) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            merge_snapshot_into_inner(&mut inner, snapshot);
        }
        self.finalize_change(ChangeImpact::full_refresh())
    }

    fn load_persisted_snapshot(&self, snapshot: DatabaseSnapshot) -> Result<()> {
        let local_actor = self.replica_id();
        let keep_pending = snapshot.clock.actor() == local_actor;
        {
            let mut inner = self.inner.lock().unwrap();
            inner.clock = snapshot.clock.rebased_with_actor(local_actor);
            inner.nodes = snapshot.nodes;
            inner.rebuild_record_overlay();
            inner.pending_ops = if keep_pending {
                CompactedOperations::from_operations(snapshot.pending_ops)
            } else {
                CompactedOperations::default()
            };
            inner.scope_policies = snapshot.scope_policies;
            inner.provisional_transactions = snapshot.provisional_transactions;
            inner.next_provisional_transaction_id = snapshot.next_provisional_transaction_id;
            inner.unflushed_ops.clear();
            inner.missing_nodes.clear();
            inner.scheduled_node_fetches.clear();
            inner.rebuild_relationship_index();
        }
        self.finalize_change(ChangeImpact::full_refresh())
    }

    pub fn pending_operations(&self) -> Vec<Operation> {
        self.inner.lock().unwrap().pending_ops.to_vec()
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
            std::mem::take(&mut inner.pending_ops).into_operations()
        };
        if !ops.is_empty() {
            self.finalize_change(ChangeImpact::pending_only())?;
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
            self.finalize_change(ChangeImpact::pending_only())?;
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

    pub fn drain_pending_envelope_json(&self) -> Result<String> {
        self.drain_pending_operations_json()
    }

    pub fn apply_operation(&self, op: Operation) -> Result<bool> {
        self.apply_operations(std::iter::once(op))
            .map(|count| count == 1)
    }

    pub fn apply_operations<I>(&self, ops: I) -> Result<usize>
    where
        I: IntoIterator<Item = Operation>,
    {
        let (applied, impact) = {
            let mut applied = 0;
            let mut applied_ops = Vec::new();
            let mut inner = self.inner.lock().unwrap();
            for op in ops {
                if inner.apply_operation_internal(op.clone(), OperationOrigin::Remote) {
                    applied += 1;
                    applied_ops.push(op);
                }
            }
            let impact = ChangeImpact::from_ops(&applied_ops, &inner.nodes);
            (applied, impact)
        };
        if applied > 0 {
            self.finalize_change(impact)?;
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

    #[cfg(all(
        feature = "crypto",
        any(test, target_arch = "wasm32", feature = "native-websocket")
    ))]
    pub(crate) fn session_presence_identity(&self, session_id: &str) -> Option<PresenceIdentity> {
        let inner = self.inner.lock().unwrap();
        let local_user = inner.security.local_user.as_ref()?;
        let mut claims = BTreeMap::new();
        claims.insert("replicaId".to_owned(), inner.clock.actor().to_owned());
        Some(PresenceIdentity {
            public_key: local_user.public_key(),
            alias: Some(local_user.alias.clone()),
            key_scheme: "ed25519".to_owned(),
            session_id: session_id.to_owned(),
            claims,
            issued_at_millis: now_millis(),
            expires_at_millis: None,
        })
    }

    #[cfg(all(
        not(feature = "crypto"),
        any(test, target_arch = "wasm32", feature = "native-websocket")
    ))]
    pub(crate) fn session_presence_identity(&self, _session_id: &str) -> Option<PresenceIdentity> {
        None
    }

    #[cfg(all(
        feature = "crypto",
        any(test, target_arch = "wasm32", feature = "native-websocket")
    ))]
    pub(crate) fn sign_session_auth_response(
        &self,
        challenge: &crate::AuthChallenge,
        responder_peer_id: &str,
        responder_session_id: &str,
        config: &SessionAuthConfig,
    ) -> Result<Option<crate::AuthResponse>> {
        let inner = self.inner.lock().unwrap();
        let Some(local_user) = inner.security.local_user.as_ref() else {
            return Ok(None);
        };
        let mut claims = BTreeMap::new();
        claims.insert("replicaId".to_owned(), inner.clock.actor().to_owned());
        crate::session_auth::sign_auth_response(
            &local_user.identity,
            Some(local_user.alias.clone()),
            claims,
            challenge,
            responder_peer_id,
            inner.clock.actor(),
            responder_session_id,
            config,
        )
        .map(Some)
    }

    #[cfg(all(
        not(feature = "crypto"),
        any(test, target_arch = "wasm32", feature = "native-websocket")
    ))]
    pub(crate) fn sign_session_auth_response(
        &self,
        _challenge: &crate::AuthChallenge,
        _responder_peer_id: &str,
        _responder_session_id: &str,
        _config: &SessionAuthConfig,
    ) -> Result<Option<crate::AuthResponse>> {
        Ok(None)
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

    pub fn traverse_path(
        &self,
        path: &RemotePath,
        spec: &TraversalSpec,
    ) -> Result<TraversalResult> {
        self.traverse_at(&path.anchor, &path.segments, spec)
    }

    pub fn node_state(&self, id: &str) -> Result<Option<NodeState>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.maybe_load_node(id)? {
            Ok(inner.nodes.get(id).cloned())
        } else {
            Ok(None)
        }
    }

    pub fn apply_node_state(&self, node: NodeState) -> Result<bool> {
        let node_id = node.id.clone();
        let changed = {
            let mut inner = self.inner.lock().unwrap();
            let before = inner.nodes.get(&node_id).cloned();
            observe_node_state(&mut inner.clock, &node);
            merge_node_state(&mut inner.nodes, node);
            inner.refresh_record_overlay(&node_id);
            inner.missing_nodes.remove(&node_id);
            inner.scheduled_node_fetches.remove(&node_id);
            inner.reindex_node_relationships(&node_id);
            inner.nodes.get(&node_id).cloned() != before
        };
        if changed {
            let touched_record_keys = if crate::is_record_node_id(&node_id) {
                self.inner
                    .lock()
                    .unwrap()
                    .nodes
                    .get(&node_id)
                    .and_then(crate::record_key_from_node_state)
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            };
            self.finalize_change(ChangeImpact {
                data_changed: true,
                full_refresh: false,
                touched_paths: vec![node_id.clone()],
                records_changed: crate::is_record_node_id(&node_id),
                touched_record_keys,
            })?;
        }
        Ok(changed)
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn register_node_fetch_scheduler(
        &self,
        scheduler: Arc<dyn NodeFetchScheduler>,
    ) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        inner.next_node_fetch_scheduler_id = inner.next_node_fetch_scheduler_id.saturating_add(1);
        let id = inner.next_node_fetch_scheduler_id;
        inner.node_fetch_schedulers.insert(id, scheduler);
        id
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn unregister_node_fetch_scheduler(&self, id: u64) {
        self.inner.lock().unwrap().node_fetch_schedulers.remove(&id);
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn clear_scheduled_node_fetch(&self, id: &str) {
        self.inner.lock().unwrap().scheduled_node_fetches.remove(id);
    }

    pub fn snapshot_for_root(&self, root: Option<&str>) -> DatabaseSnapshot {
        let (engine, clock, pending_ops, loaded_nodes, scope_policies) = {
            let inner = self.inner.lock().unwrap();
            (
                inner.storage_engine.clone(),
                inner.clock.clone(),
                inner.pending_ops.to_vec(),
                inner.nodes.clone(),
                inner.scope_policies.clone(),
            )
        };

        let mut snapshot = if let Some(engine) = engine {
            engine.export_snapshot(None).unwrap_or(DatabaseSnapshot {
                clock: clock.clone(),
                nodes: BTreeMap::new(),
                pending_ops: Vec::new(),
                scope_policies: BTreeMap::new(),
                provisional_transactions: BTreeMap::new(),
                next_provisional_transaction_id: 0,
            })
        } else {
            DatabaseSnapshot {
                clock: clock.clone(),
                nodes: BTreeMap::new(),
                pending_ops: Vec::new(),
                scope_policies: BTreeMap::new(),
                provisional_transactions: BTreeMap::new(),
                next_provisional_transaction_id: 0,
            }
        };

        snapshot.clock = clock;
        snapshot.scope_policies = scope_policies;
        snapshot.provisional_transactions.clear();
        snapshot.next_provisional_transaction_id = 0;
        for (node_id, node_state) in loaded_nodes {
            snapshot.nodes.insert(node_id, node_state);
        }

        if let Some(root) = root {
            let reachable = collect_snapshot_root_closure(&snapshot.nodes, root);
            snapshot
                .nodes
                .retain(|node_id, _| reachable.contains(node_id));
            snapshot.pending_ops = pending_ops
                .into_iter()
                .filter(|op| operation_matches_snapshot_nodes(op, &reachable))
                .collect();
            snapshot
                .scope_policies
                .retain(|scope, _| node_matches_root(scope, root));
        } else {
            snapshot.pending_ops = pending_ops;
        }

        snapshot
    }

    pub fn execute_pull_request_kind(&self, request: &PullRequestKind) -> Result<RemoteResult> {
        match request {
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
            PullRequestKind::Records { scan } => Ok(RemoteResult::Records {
                result: self.scan_records(scan.clone())?,
            }),
            PullRequestKind::VectorSearch {
                collection,
                query,
                spec,
            } => Ok(RemoteResult::VectorSearch {
                result: self.search_vectors(collection, query, spec.clone())?,
            }),
            PullRequestKind::TextSearch {
                source,
                query,
                spec,
            } => Ok(RemoteResult::TextSearch {
                result: self.text_search(source.clone(), query, spec.clone())?,
            }),
            PullRequestKind::Node { id } => Ok(RemoteResult::Node {
                node: self.node_state(id)?,
            }),
            PullRequestKind::Snapshot { root } => Ok(RemoteResult::Snapshot {
                snapshot: self.snapshot_for_root(root.as_deref()),
            }),
            PullRequestKind::Transaction {
                scope,
                steps,
                options,
            } => Ok(RemoteResult::Transaction {
                report: self
                    .scope(scope)
                    .transaction_steps(steps.clone(), options.clone())?,
            }),
        }
    }

    pub fn execute_pull_request(&self, request: &PullRequest) -> Result<RemoteResult> {
        self.execute_pull_request_kind(&request.request)
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn allow_peer_connection(&self, context: &ConnectHookContext) -> HookDecision<()> {
        self.inner
            .lock()
            .unwrap()
            .network_hooks
            .clone()
            .map(|hooks| hooks.on_connect(context))
            .unwrap_or_else(|| HookDecision::allow(()))
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-webrtc"))]
    pub(crate) fn allow_room_join(&self, context: &RoomHookContext) -> HookDecision<()> {
        self.inner
            .lock()
            .unwrap()
            .network_hooks
            .clone()
            .map(|hooks| hooks.on_join_room(context))
            .unwrap_or_else(|| HookDecision::allow(()))
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn authorize_pull_request_for_peer(
        &self,
        peer_id: &str,
        transport: HookTransport,
        request_id: Option<&str>,
        request: &PullRequestKind,
        verified_identity: Option<&VerifiedIdentity>,
    ) -> HookDecision<PullRequestKind> {
        let hooks = self.inner.lock().unwrap().network_hooks.clone();
        let Some(hooks) = hooks else {
            return HookDecision::allow(request.clone());
        };
        hooks.on_pull(&ServeRequestContext {
            peer_id: peer_id.to_owned(),
            transport,
            request_id: request_id.map(str::to_owned),
            watch_id: None,
            request: request.clone(),
            verified_identity: verified_identity.cloned(),
        })
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn authorize_watch_request_for_peer(
        &self,
        peer_id: &str,
        transport: HookTransport,
        watch_id: &str,
        request: &PullRequestKind,
        verified_identity: Option<&VerifiedIdentity>,
    ) -> HookDecision<PullRequestKind> {
        let hooks = self.inner.lock().unwrap().network_hooks.clone();
        let Some(hooks) = hooks else {
            return HookDecision::allow(request.clone());
        };
        hooks.on_watch(&ServeRequestContext {
            peer_id: peer_id.to_owned(),
            transport,
            request_id: None,
            watch_id: Some(watch_id.to_owned()),
            request: request.clone(),
            verified_identity: verified_identity.cloned(),
        })
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn filter_served_result_for_peer(
        &self,
        peer_id: &str,
        transport: HookTransport,
        request_id: Option<&str>,
        watch_id: Option<&str>,
        request: &PullRequestKind,
        initial: bool,
        result: RemoteResult,
        verified_identity: Option<&VerifiedIdentity>,
    ) -> HookDecision<RemoteResult> {
        let hooks = self.inner.lock().unwrap().network_hooks.clone();
        let Some(hooks) = hooks else {
            return HookDecision::allow(result);
        };
        hooks.on_serve_result(
            &ServeResultContext {
                peer_id: peer_id.to_owned(),
                transport,
                request_id: request_id.map(str::to_owned),
                watch_id: watch_id.map(str::to_owned),
                request: request.clone(),
                initial,
                verified_identity: verified_identity.cloned(),
            },
            result,
        )
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn serve_pull_request_for_peer(
        &self,
        peer_id: &str,
        transport: HookTransport,
        request_id: &str,
        request: &PullRequestKind,
        verified_identity: Option<&VerifiedIdentity>,
    ) -> Result<HookDecision<RemoteResult>> {
        let request = match self
            .authorize_pull_request_for_peer(
                peer_id,
                transport,
                Some(request_id),
                request,
                verified_identity,
            )
            .into_result()
        {
            Ok(request) => request,
            Err(message) => return Ok(HookDecision::deny(message)),
        };
        let result = self.execute_pull_request_kind(&request)?;
        Ok(self.filter_served_result_for_peer(
            peer_id,
            transport,
            Some(request_id),
            None,
            &request,
            true,
            result,
            verified_identity,
        ))
    }

    #[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
    pub(crate) fn serve_watch_result_for_peer(
        &self,
        peer_id: &str,
        transport: HookTransport,
        watch_id: &str,
        request: &PullRequestKind,
        initial: bool,
        verified_identity: Option<&VerifiedIdentity>,
    ) -> Result<HookDecision<RemoteResult>> {
        let result = self.execute_pull_request_kind(request)?;
        Ok(self.filter_served_result_for_peer(
            peer_id,
            transport,
            None,
            Some(watch_id),
            request,
            initial,
            result,
            verified_identity,
        ))
    }

    pub fn chunk_remote_result(&self, request_id: &str, result: RemoteResult) -> Vec<PullResponse> {
        build_pull_responses(request_id, result, &self.limits())
    }

    pub fn chunk_watch_result(
        &self,
        watch_id: &str,
        sequence: u64,
        initial: bool,
        result: RemoteResult,
    ) -> Vec<crate::WatchEvent> {
        self.chunk_remote_result(watch_id, result)
            .into_iter()
            .map(|response| crate::WatchEvent {
                watch_id: watch_id.to_owned(),
                sequence,
                initial,
                chunk: response.chunk,
                done: response.done,
                result: response.result,
            })
            .collect()
    }

    pub fn subscribe_changes(&self) -> ChangeSubscription {
        let (sender, receiver) = async_channel::bounded(LOCAL_WATCH_QUEUE_CAPACITY);
        let (id, event) = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_change_subscription_id = inner.next_change_subscription_id.saturating_add(1);
            let id = inner.next_change_subscription_id;
            let event = ChangeEvent {
                revision: inner.change_revision,
                pending_ops: inner.pending_ops.len(),
                data_changed: false,
                full_refresh: false,
                touched_paths: Vec::new(),
                records_changed: false,
                touched_record_keys: Vec::new(),
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
            record_subscriptions: inner.record_subscriptions.len(),
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

    pub fn set_network_hooks(&self, hooks: Arc<dyn NetworkHooks>) {
        self.inner.lock().unwrap().network_hooks = Some(hooks);
    }

    pub fn clear_network_hooks(&self) {
        self.inner.lock().unwrap().network_hooks = None;
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn use_file_persistence(&self, path: impl Into<std::path::PathBuf>) -> Result<bool> {
        let target = PersistenceTarget::File(path.into());
        self.configure_persistence(target)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn use_segment_storage(
        &self,
        directory: impl Into<std::path::PathBuf>,
        journal_retention: usize,
    ) -> Result<bool> {
        let directory = directory.into();
        let store = crate::SegmentFileStore::new(directory.clone(), journal_retention)?;
        {
            let mut inner = self.inner.lock().unwrap();
            inner.vector_cache_root = Some(directory.join("vector-cache"));
            inner.text_cache_root = Some(directory.join("text-cache"));
        }
        self.attach_incremental_store(Arc::new(store))
    }

    #[cfg(target_arch = "wasm32")]
    pub fn use_browser_storage(&self, key: impl Into<String>) -> Result<bool> {
        let target = PersistenceTarget::BrowserStorage(key.into());
        self.configure_persistence(target)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_durable_storage(
        &self,
        config: DurableStorageConfig,
    ) -> Result<DurableStorageBinding> {
        match config {
            DurableStorageConfig::SnapshotFile { path } => {
                let loaded = self.use_file_persistence(path)?;
                Ok(DurableStorageBinding {
                    backend: "snapshot_file".to_owned(),
                    incremental: false,
                    loaded_existing: loaded,
                    auto_persist: true,
                    durability: None,
                    lock_mode: None,
                })
            }
            DurableStorageConfig::SegmentFiles {
                directory,
                journal_retention,
                durability,
                lock_mode,
            } => {
                let directory_path = std::path::PathBuf::from(&directory);
                let vector_cache_root = directory_path.join("vector-cache");
                let text_cache_root = directory_path.join("text-cache");
                let store = crate::SegmentFileStore::with_options(
                    directory,
                    journal_retention,
                    crate::SegmentFileStoreOptions {
                        durability,
                        lock_mode: lock_mode.clone(),
                    },
                )?;
                let loaded = self.attach_incremental_store(Arc::new(store))?;
                {
                    let mut inner = self.inner.lock().unwrap();
                    inner.vector_cache_root = Some(vector_cache_root);
                    inner.text_cache_root = Some(text_cache_root);
                }
                Ok(DurableStorageBinding {
                    backend: "segment_file".to_owned(),
                    incremental: true,
                    loaded_existing: loaded,
                    auto_persist: true,
                    durability: Some(durability),
                    lock_mode: Some(lock_mode),
                })
            }
            DurableStorageConfig::BrowserStorage { .. }
            | DurableStorageConfig::IndexedDbSnapshots { .. }
            | DurableStorageConfig::IndexedDbSegments { .. }
            | DurableStorageConfig::OpfsSegments { .. } => Err(PrimadbError::Message(
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
                    durability: None,
                })
            }
            #[cfg(not(target_arch = "wasm32"))]
            BlobStorageConfig::Files {
                directory,
                durability,
            } => {
                self.attach_blob_store(Arc::new(FileBlobStore::with_options(
                    directory,
                    crate::FileBlobStoreOptions { durability },
                )));
                Ok(BlobStorageBinding {
                    backend: "files".to_owned(),
                    content_addressed: true,
                    durability: Some(durability),
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

    #[cfg(all(not(target_arch = "wasm32"), feature = "native-moq"))]
    pub async fn connect_moq(&self, config: MoqRelayClientConfig) -> Result<NativeMoqSync> {
        NativeMoqSync::connect_with_config(self.clone(), config).await
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

    pub fn vacuum_storage(&self) -> Result<VacuumReport> {
        let (storage, blob_store, metadata, nodes, next_storage_tx_id) = {
            let inner = self.inner.lock().unwrap();
            let mut metadata = build_storage_metadata(
                inner.clock.clone(),
                inner.pending_ops.to_vec(),
                inner.next_storage_tx_id + 1,
            );
            metadata.scope_policies = inner.scope_policies.clone();
            metadata.provisional_transactions = inner.provisional_transactions.clone();
            metadata.next_provisional_transaction_id = inner.next_provisional_transaction_id;
            (
                inner.storage_engine.clone(),
                inner.blob_store.clone(),
                metadata,
                inner.nodes.clone(),
                inner.next_storage_tx_id,
            )
        };

        let storage_report = if let Some(storage) = storage {
            let transaction =
                build_storage_transaction(next_storage_tx_id, metadata, nodes.clone());
            storage.vacuum(&transaction)?
        } else {
            StorageVacuumReport::default()
        };

        let removed_blob_entries = if let Some(store) = blob_store {
            let live_blob_ids = referenced_blob_ids(&nodes);
            store.delete_unreferenced(&live_blob_ids)?
        } else {
            0
        };

        Ok(VacuumReport {
            storage: storage_report,
            removed_blob_entries,
        })
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
                inner.pending_ops = CompactedOperations::from_operations(metadata.pending_ops);
                inner.scope_policies = metadata.scope_policies;
                inner.provisional_transactions = metadata.provisional_transactions;
                inner.next_provisional_transaction_id = metadata.next_provisional_transaction_id;
                inner.unflushed_ops.clear();
                inner.nodes.clear();
                inner.record_overlay.clear();
                inner.record_overlay_node_keys.clear();
                inner.missing_nodes.clear();
                inner.scheduled_node_fetches.clear();
                inner.relationship_index = RelationshipIndex::default();
                inner.next_storage_tx_id = metadata.next_tx_id.max(1);
            } else {
                inner.next_storage_tx_id = 1;
                inner.missing_nodes.clear();
                inner.scheduled_node_fetches.clear();
                inner.relationship_index = RelationshipIndex::default();
            }
            inner.storage_engine = Some(store);
        }
        self.persist_if_needed()?;
        Ok(metadata.is_some())
    }

    pub fn close_durable_storage(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.rebuild_record_overlay();
        inner.persistence = None;
        inner.storage_adapter = None;
        inner.storage_engine = None;
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
            storage_transaction,
            external_storage_hooks,
        ) = {
            let inner = self.inner.lock().unwrap();
            let storage_transaction = inner.storage_engine.as_ref().map(|_| {
                let tx_id = inner.next_storage_tx_id;
                let metadata = storage_metadata_from_inner(&inner, tx_id.saturating_add(1));
                if inner.unflushed_ops.is_empty() {
                    build_storage_transaction(tx_id, metadata, inner.nodes.clone())
                } else {
                    build_storage_transaction_from_ops(
                        tx_id,
                        metadata,
                        &inner.nodes,
                        inner.unflushed_ops.as_slice(),
                    )
                }
            });
            (
                inner.persistence.clone(),
                inner.storage_adapter.clone(),
                inner.storage_engine.clone(),
                DatabaseSnapshot {
                    clock: inner.clock.clone(),
                    nodes: inner.nodes.clone(),
                    pending_ops: inner.pending_ops.to_vec(),
                    scope_policies: inner.scope_policies.clone(),
                    provisional_transactions: inner.provisional_transactions.clone(),
                    next_provisional_transaction_id: inner.next_provisional_transaction_id,
                },
                inner.unflushed_ops.to_vec(),
                storage_transaction,
                inner.external_storage_hooks,
            )
        };

        let has_sync_durable_backend = target.is_some() || adapter.is_some() || engine.is_some();

        if let Some(target) = target {
            store_snapshot_payload(&target, &self.export_persisted_snapshot_json()?)?;
        }

        if let Some(adapter) = adapter {
            adapter.flush(&unflushed_ops, &snapshot)?;
        }

        if let (Some(engine), Some(transaction)) = (engine, storage_transaction) {
            engine.apply_transaction(&transaction)?;
            self.mark_storage_transaction_flushed(&transaction)?;
        } else if has_sync_durable_backend || external_storage_hooks == 0 {
            self.mark_durable_operations_flushed(&unflushed_ops)?;
        }

        Ok(())
    }

    fn finalize_change(&self, impact: ChangeImpact) -> Result<()> {
        let event = {
            let mut inner = self.inner.lock().unwrap();
            inner.change_revision = inner.change_revision.saturating_add(1);
            ChangeEvent {
                revision: inner.change_revision,
                pending_ops: inner.pending_ops.len(),
                data_changed: impact.data_changed,
                full_refresh: impact.full_refresh,
                touched_paths: impact.touched_paths,
                records_changed: impact.records_changed,
                touched_record_keys: impact.touched_record_keys,
            }
        };
        self.persist_if_needed()?;
        if event.data_changed {
            self.mark_vector_collections_dirty(&event);
            self.mark_text_collections_dirty(&event);
            self.notify_subscribers(&event)?;
            self.notify_traversal_subscribers(&event)?;
            self.notify_record_subscribers(&event)?;
            self.notify_vector_subscribers(&event)?;
            self.notify_text_subscribers(&event)?;
        }
        self.notify_change_subscribers(event)?;
        Ok(())
    }

    fn finalize_local_change(&self) -> Result<()> {
        let impact = {
            let inner = self.inner.lock().unwrap();
            ChangeImpact::from_ops(inner.unflushed_ops.as_slice(), &inner.nodes)
        };
        self.finalize_change(impact)
    }

    fn mark_vector_collections_dirty(&self, event: &ChangeEvent) {
        if !event.full_refresh && !event.records_changed {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        if event.full_refresh || event.touched_record_keys.is_empty() {
            for cache in inner.vector_collections.values_mut() {
                cache.dirty = true;
                cache.state = VectorManagerState::Stale;
            }
            return;
        }
        for key in &event.touched_record_keys {
            let Some(collection) = vector_collection_from_record_key(key) else {
                continue;
            };
            if let Some(cache) = inner.vector_collections.get_mut(&collection) {
                cache.dirty = true;
                cache.state = VectorManagerState::Stale;
            }
        }
    }

    fn mark_text_collections_dirty(&self, event: &ChangeEvent) {
        if !event.full_refresh && !event.records_changed {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        if event.full_refresh || event.touched_record_keys.is_empty() {
            for cache in inner.text_collections.values_mut() {
                cache.dirty = true;
                cache.state = crate::TextIndexState::Stale;
            }
            return;
        }
        for key in &event.touched_record_keys {
            let Some(collection) = text_collection_from_record_key(key) else {
                continue;
            };
            if let Some(cache) = inner.text_collections.get_mut(&collection) {
                cache.dirty = true;
                cache.state = crate::TextIndexState::Stale;
            }
        }
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
                Ok(Some(inner.materialize_field(
                    &node,
                    &field,
                    &value,
                    &mut BTreeSet::new(),
                )))
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

        let mut inner = self.inner.lock().unwrap();
        let candidates = match inner.resolve_cursor(anchor, segments)? {
            Some(cursor) => inner.query_candidates(cursor),
            None => Vec::new(),
        };
        Ok(inner.evaluate_query_candidates(candidates, spec))
    }

    fn traverse_at(
        &self,
        anchor: &str,
        segments: &[String],
        spec: &TraversalSpec,
    ) -> Result<TraversalResult> {
        let (mut result, fetch_candidates, schedulers) = {
            let mut inner = self.inner.lock().unwrap();
            let result = inner.traverse_at(anchor, segments, spec)?;
            let fetch_candidates = if spec.fetch_missing && spec.max_fetches > 0 {
                inner.reserve_node_fetches(&result.missing, spec.max_fetches)
            } else {
                Vec::new()
            };
            let schedulers = if fetch_candidates.is_empty() {
                Vec::new()
            } else {
                inner
                    .node_fetch_schedulers
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
            };
            (result, fetch_candidates, schedulers)
        };

        if !fetch_candidates.is_empty() && !schedulers.is_empty() {
            result.fetched = fetch_candidates.len();
            for scheduler in schedulers {
                scheduler.fetch_nodes(fetch_candidates.clone());
            }
        } else if !fetch_candidates.is_empty() {
            self.inner
                .lock()
                .unwrap()
                .release_reserved_node_fetches(&fetch_candidates);
        }

        Ok(result)
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

        let ordered_index_path = spec.order.as_ref().and_then(indexed_order_path);
        let indexed_filter_groups = build_index_filter_groups(spec);
        if indexed_filter_groups.is_empty() && ordered_index_path.is_none() {
            return Ok(None);
        }

        let candidate_ids: BTreeSet<_> = set.members.keys().cloned().collect();
        let mut eligible_ids = candidate_ids.clone();
        for (path, filters) in &indexed_filter_groups {
            let scan = build_direct_index_scan(filters, None);
            let mut matched = BTreeSet::new();
            for entry in engine.scan_direct_index_entries(path, QueryDirection::Asc, &scan)? {
                if !candidate_ids.contains(&entry.node_id) {
                    continue;
                }
                if filters
                    .iter()
                    .all(|filter| filter_matches_index_entry(filter, &entry.value))
                {
                    matched.insert(entry.node_id);
                }
            }
            eligible_ids = eligible_ids
                .intersection(&matched)
                .cloned()
                .collect::<BTreeSet<_>>();
            if eligible_ids.is_empty() {
                return Ok(Some(Vec::new()));
            }
        }

        let mut candidates = Vec::new();
        if let Some(order_path) = ordered_index_path.clone() {
            let direction = spec
                .order
                .as_ref()
                .map(|order| order.direction)
                .unwrap_or(QueryDirection::Asc);
            let filters = indexed_filter_groups
                .get(&order_path)
                .map(|filters| filters.as_slice())
                .unwrap_or(&[]);
            let scan = build_direct_index_scan(filters, None);
            let target_count = spec
                .limit
                .map(|limit| spec.offset.saturating_add(limit))
                .filter(|_| can_early_stop_index_query(spec));
            let mut seen = BTreeSet::new();
            for entry in engine.scan_direct_index_entries(&order_path, direction, &scan)? {
                if !eligible_ids.contains(&entry.node_id) || !seen.insert(entry.node_id.clone()) {
                    continue;
                }
                candidates.push(QueryCandidate {
                    key: entry.node_id.clone(),
                    source: QueryCandidateSource::Node(entry.node_id),
                });
                if target_count.is_some_and(|target| candidates.len() >= target) {
                    break;
                }
            }
        } else {
            for member_id in &eligible_ids {
                candidates.push(QueryCandidate {
                    key: member_id.clone(),
                    source: QueryCandidateSource::Node(member_id.clone()),
                });
            }
        }

        Ok(Some(inner.evaluate_query_candidates(candidates, spec)))
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

        let snapshot = self.materialize(anchor, segments)?;
        let path_key = watch_path_key(anchor, segments);
        let last_hash = crate::stable_content_hash(&snapshot);

        let (sender, receiver) = async_channel::bounded(LOCAL_WATCH_QUEUE_CAPACITY);
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_subscription_id = inner.next_subscription_id.saturating_add(1);
            let id = inner.next_subscription_id;
            inner.subscriptions.insert(
                id,
                Watcher {
                    anchor: anchor.to_owned(),
                    segments: segments.to_vec(),
                    path_key,
                    last_hash,
                    sender: sender.clone(),
                },
            );
            id
        };
        let _ = sender.try_send(snapshot);

        Ok(Subscription {
            inner: Arc::new(SubscriptionInner {
                id,
                db: Arc::downgrade(&self.inner),
                receiver,
            }),
        })
    }

    fn subscribe_to_traversal(
        &self,
        anchor: &str,
        segments: &[String],
        spec: TraversalSpec,
    ) -> Result<TraversalSubscription> {
        let result = self.traverse_at(anchor, segments, &spec)?;
        let dependency_paths = traversal_dependency_paths(anchor, segments, &result);
        let last_hash = crate::stable_content_hash(&result);

        let (sender, receiver) = async_channel::bounded(LOCAL_WATCH_QUEUE_CAPACITY);
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_traversal_subscription_id =
                inner.next_traversal_subscription_id.saturating_add(1);
            let id = inner.next_traversal_subscription_id;
            inner.traversal_subscriptions.insert(
                id,
                TraversalWatcher {
                    anchor: anchor.to_owned(),
                    segments: segments.to_vec(),
                    spec,
                    dependency_paths,
                    last_hash,
                    sender: sender.clone(),
                },
            );
            id
        };
        let _ = sender.try_send(result);

        Ok(TraversalSubscription {
            inner: Arc::new(TraversalSubscriptionInner {
                id,
                db: Arc::downgrade(&self.inner),
                receiver,
            }),
        })
    }

    fn subscribe_to_records(&self, scan: RecordScan) -> Result<RecordWatchSubscription> {
        let result = self.scan_records(scan.clone())?;
        let last_hash = crate::stable_content_hash(&result);

        let (sender, receiver) = async_channel::bounded(LOCAL_WATCH_QUEUE_CAPACITY);
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_record_subscription_id = inner.next_record_subscription_id.saturating_add(1);
            let id = inner.next_record_subscription_id;
            inner.record_subscriptions.insert(
                id,
                RecordWatcher {
                    scan,
                    last_hash,
                    sender: sender.clone(),
                },
            );
            id
        };
        let _ = sender.try_send(result);

        Ok(RecordWatchSubscription {
            inner: Arc::new(RecordWatchSubscriptionInner {
                id,
                db: Arc::downgrade(&self.inner),
                receiver,
            }),
        })
    }

    fn subscribe_to_vector_search(
        &self,
        collection: String,
        query: Vec<f32>,
        spec: VectorSearchSpec,
    ) -> Result<VectorWatchSubscription> {
        let result = self.search_vectors(&collection, &query, spec.clone())?;
        let last_hash = crate::stable_content_hash(&result);

        let (sender, receiver) = async_channel::bounded(LOCAL_WATCH_QUEUE_CAPACITY);
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_vector_subscription_id = inner.next_vector_subscription_id.saturating_add(1);
            let id = inner.next_vector_subscription_id;
            inner.vector_subscriptions.insert(
                id,
                VectorWatcher {
                    collection,
                    query,
                    spec,
                    last_hash,
                    sender: sender.clone(),
                },
            );
            id
        };
        let _ = sender.try_send(result);

        Ok(VectorWatchSubscription {
            inner: Arc::new(VectorWatchSubscriptionInner {
                id,
                db: Arc::downgrade(&self.inner),
                receiver,
            }),
        })
    }

    fn subscribe_to_text_search(
        &self,
        source: TextSearchSource,
        query: String,
        spec: TextSearchSpec,
    ) -> Result<TextWatchSubscription> {
        let result = self.execute_text_search(source.clone(), &query, spec.clone())?;
        let last_hash = crate::stable_content_hash(&result);

        let (sender, receiver) = async_channel::bounded(LOCAL_WATCH_QUEUE_CAPACITY);
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_text_subscription_id = inner.next_text_subscription_id.saturating_add(1);
            let id = inner.next_text_subscription_id;
            inner.text_subscriptions.insert(
                id,
                TextWatcher {
                    source,
                    query,
                    spec,
                    last_hash,
                    sender: sender.clone(),
                },
            );
            id
        };
        let _ = sender.try_send(result);

        Ok(TextWatchSubscription {
            inner: Arc::new(TextWatchSubscriptionInner {
                id,
                db: Arc::downgrade(&self.inner),
                receiver,
            }),
        })
    }

    fn execute_text_search(
        &self,
        source: TextSearchSource,
        query: &str,
        spec: TextSearchSpec,
    ) -> Result<TextSearchResult> {
        match source {
            TextSearchSource::Collection { collection } => {
                self.ensure_text_collection_ready(&collection)?;
                let inner = self.inner.lock().unwrap();
                let cache = inner.text_collections.get(&collection).ok_or_else(|| {
                    PrimadbError::Message(format!("text collection `{collection}` is not loaded"))
                })?;
                if spec.stale_policy == SearchStalePolicy::Reject
                    && (cache.state != crate::TextIndexState::Ready || cache.dirty)
                {
                    return Err(PrimadbError::Message(format!(
                        "text collection `{collection}` is {:?}",
                        cache.state
                    )));
                }
                search_text_collection(&collection, cache, query, &spec)
            }
            TextSearchSource::GraphQuery {
                path,
                spec: query_spec,
            } => {
                if spec.candidate_policy == crate::TextCandidatePolicy::RejectPaginatedQuery
                    && (query_spec.order.is_some()
                        || query_spec.limit.is_some()
                        || query_spec.offset > 0)
                {
                    return Err(PrimadbError::Message(
                        "query-scoped text search rejects ordered, limited, or offset queries by default; set candidatePolicy to allow_preselected_candidates to rank the preselected candidate set".to_owned(),
                    ));
                }
                let truncated = query_spec.order.is_some()
                    || query_spec.limit.is_some()
                    || query_spec.offset > 0;
                let entries = self.query_path(&path, &query_spec)?;
                let candidates = text_candidates_from_map_entries(&entries, spec.fields.as_deref());
                search_text_candidates(
                    TextSearchSourceSummary::GraphQuery { path },
                    query,
                    &spec,
                    candidates,
                    truncated,
                    TextScoreScope::CandidateSet,
                )
            }
            TextSearchSource::Records { scan } => {
                let result = self.scan_records(scan.clone())?;
                let truncated =
                    scan.limit.is_some() || scan.cursor.is_some() || result.next_cursor.is_some();
                let candidates =
                    text_candidates_from_record_entries(&result.entries, spec.fields.as_deref());
                search_text_candidates(
                    TextSearchSourceSummary::Records {
                        prefix: scan.prefix.clone(),
                    },
                    query,
                    &spec,
                    candidates,
                    truncated,
                    TextScoreScope::CandidateSet,
                )
            }
        }
    }

    fn ensure_vector_collection_ready(&self, collection: &str) -> Result<()> {
        loop {
            let (key, snapshot) = {
                let mut inner = self.inner.lock().unwrap();
                let key = CacheRebuildKey {
                    kind: CacheRebuildKind::Vector,
                    collection: collection.to_owned(),
                };
                let needs_rebuild = inner
                    .vector_collections
                    .get(collection)
                    .map(|cache| cache.dirty || cache.state != VectorManagerState::Ready)
                    .unwrap_or(true);
                if !needs_rebuild {
                    return Ok(());
                }
                if inner.cache_rebuilds.contains(&key) {
                    let wait = inner.cache_rebuild_wait.clone();
                    inner = wait.wait(inner).unwrap();
                    continue;
                }
                inner.cache_rebuilds.insert(key.clone());
                if let Some(cache) = inner.vector_collections.get_mut(collection) {
                    cache.state = VectorManagerState::Rebuilding;
                }
                let snapshot = match vector_rebuild_snapshot(&mut inner, collection) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        inner.cache_rebuilds.remove(&key);
                        inner.cache_rebuild_wait.notify_all();
                        return Err(error);
                    }
                };
                (key, snapshot)
            };

            let result = build_vector_collection_off_lock(&snapshot);
            #[cfg(not(target_arch = "wasm32"))]
            let mut cache_write = None;
            let stale = {
                let mut inner = self.inner.lock().unwrap();
                match result {
                    Ok((cache, files)) if inner.change_revision == snapshot.revision => {
                        inner
                            .vector_collections
                            .insert(collection.to_owned(), cache);
                        #[cfg(target_arch = "wasm32")]
                        let _ = files;
                        #[cfg(not(target_arch = "wasm32"))]
                        {
                            cache_write = snapshot
                                .cache_root
                                .clone()
                                .zip(files)
                                .map(|(root, files)| (root, files));
                        }
                        false
                    }
                    Ok(_) => true,
                    Err(error) => {
                        inner.cache_rebuilds.remove(&key);
                        inner.cache_rebuild_wait.notify_all();
                        return Err(error);
                    }
                }
            };
            #[cfg(not(target_arch = "wasm32"))]
            if !stale && let Some((root, files)) = cache_write {
                let _ = write_native_vector_cache(&root, collection, files);
            }
            let mut inner = self.inner.lock().unwrap();
            inner.cache_rebuilds.remove(&key);
            inner.cache_rebuild_wait.notify_all();
            if !stale {
                return Ok(());
            }
        }
    }

    fn ensure_text_collection_ready(&self, collection: &str) -> Result<()> {
        loop {
            let (key, snapshot) = {
                let mut inner = self.inner.lock().unwrap();
                let key = CacheRebuildKey {
                    kind: CacheRebuildKind::Text,
                    collection: collection.to_owned(),
                };
                let needs_rebuild = inner
                    .text_collections
                    .get(collection)
                    .map(|cache| cache.dirty || cache.state != crate::TextIndexState::Ready)
                    .unwrap_or(true);
                if !needs_rebuild {
                    return Ok(());
                }
                if inner.cache_rebuilds.contains(&key) {
                    let wait = inner.cache_rebuild_wait.clone();
                    inner = wait.wait(inner).unwrap();
                    continue;
                }
                inner.cache_rebuilds.insert(key.clone());
                if let Some(cache) = inner.text_collections.get_mut(collection) {
                    cache.state = crate::TextIndexState::Rebuilding;
                }
                let snapshot = match text_rebuild_snapshot(&mut inner, collection) {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        inner.cache_rebuilds.remove(&key);
                        inner.cache_rebuild_wait.notify_all();
                        return Err(error);
                    }
                };
                (key, snapshot)
            };

            let result = build_text_collection_off_lock(&snapshot);
            #[cfg(not(target_arch = "wasm32"))]
            let mut cache_write = None;
            let stale = {
                let mut inner = self.inner.lock().unwrap();
                match result {
                    Ok((cache, files)) if inner.change_revision == snapshot.revision => {
                        inner.text_collections.insert(collection.to_owned(), cache);
                        #[cfg(target_arch = "wasm32")]
                        let _ = files;
                        #[cfg(not(target_arch = "wasm32"))]
                        {
                            cache_write = snapshot
                                .cache_root
                                .clone()
                                .zip(files)
                                .map(|(root, files)| (root, files));
                        }
                        false
                    }
                    Ok(_) => true,
                    Err(error) => {
                        inner.cache_rebuilds.remove(&key);
                        inner.cache_rebuild_wait.notify_all();
                        return Err(error);
                    }
                }
            };
            #[cfg(not(target_arch = "wasm32"))]
            if !stale && let Some((root, files)) = cache_write {
                let _ = write_native_text_cache(&root, collection, files);
            }
            let mut inner = self.inner.lock().unwrap();
            inner.cache_rebuilds.remove(&key);
            inner.cache_rebuild_wait.notify_all();
            if !stale {
                return Ok(());
            }
        }
    }

    fn notify_subscribers(&self, event: &ChangeEvent) -> Result<()> {
        let watchers: Vec<(u64, Watcher)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .subscriptions
                .iter()
                .map(|(id, watcher)| (*id, watcher.clone()))
                .collect()
        };

        let mut stale = Vec::new();
        let mut hash_updates = Vec::new();
        let mut snapshots: BTreeMap<Vec<u8>, Option<JsonValue>> = BTreeMap::new();
        for (id, watcher) in watchers {
            if !event.full_refresh && !watch_change_overlaps(&watcher.path_key, event) {
                continue;
            }
            let key = watch_key(&(watcher.anchor.clone(), watcher.segments.clone()));
            let snapshot = if let Some(snapshot) = snapshots.get(&key) {
                snapshot.clone()
            } else {
                let snapshot = self
                    .materialize(&watcher.anchor, &watcher.segments)
                    .unwrap_or(None);
                #[cfg(test)]
                self.note_watch_recomputation();
                snapshots.insert(key, snapshot.clone());
                snapshot
            };
            let snapshot_hash = crate::stable_content_hash(&snapshot);
            if snapshot_hash == watcher.last_hash {
                continue;
            }
            if !send_watch_update(&watcher.sender, snapshot) {
                stale.push(id);
            } else {
                hash_updates.push((id, snapshot_hash));
            }
        }

        if !stale.is_empty() || !hash_updates.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for (id, hash) in hash_updates {
                if let Some(watcher) = inner.subscriptions.get_mut(&id) {
                    watcher.last_hash = hash;
                }
            }
            for id in stale {
                inner.subscriptions.remove(&id);
            }
        }

        Ok(())
    }

    fn notify_record_subscribers(&self, event: &ChangeEvent) -> Result<()> {
        let watchers: Vec<(u64, RecordWatcher)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .record_subscriptions
                .iter()
                .map(|(id, watcher)| (*id, watcher.clone()))
                .collect()
        };

        let mut stale = Vec::new();
        let mut hash_updates = Vec::new();
        let mut results: BTreeMap<Vec<u8>, RecordScanResult> = BTreeMap::new();
        for (id, watcher) in watchers {
            if !record_watch_change_overlaps(&watcher.scan, event) {
                continue;
            }
            let key = watch_key(&watcher.scan);
            let result = if let Some(result) = results.get(&key) {
                result.clone()
            } else {
                let result = self.scan_records(watcher.scan.clone())?;
                #[cfg(test)]
                self.note_watch_recomputation();
                results.insert(key, result.clone());
                result
            };
            let result_hash = crate::stable_content_hash(&result);
            if result_hash == watcher.last_hash {
                continue;
            }
            if !send_watch_update(&watcher.sender, result) {
                stale.push(id);
            } else {
                hash_updates.push((id, result_hash));
            }
        }

        if !stale.is_empty() || !hash_updates.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for (id, hash) in hash_updates {
                if let Some(watcher) = inner.record_subscriptions.get_mut(&id) {
                    watcher.last_hash = hash;
                }
            }
            for id in stale {
                inner.record_subscriptions.remove(&id);
            }
        }

        Ok(())
    }

    fn notify_vector_subscribers(&self, event: &ChangeEvent) -> Result<()> {
        let watchers: Vec<(u64, VectorWatcher)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .vector_subscriptions
                .iter()
                .map(|(id, watcher)| (*id, watcher.clone()))
                .collect()
        };

        let mut stale = Vec::new();
        let mut hash_updates = Vec::new();
        let mut results: BTreeMap<Vec<u8>, VectorSearchResult> = BTreeMap::new();
        for (id, watcher) in watchers {
            if !vector_watch_change_overlaps(&watcher.collection, event) {
                continue;
            }
            let key = watch_key(&(
                watcher.collection.clone(),
                watcher.query.clone(),
                watcher.spec.clone(),
            ));
            let result = if let Some(result) = results.get(&key) {
                result.clone()
            } else {
                let result =
                    self.search_vectors(&watcher.collection, &watcher.query, watcher.spec.clone())?;
                #[cfg(test)]
                self.note_watch_recomputation();
                results.insert(key, result.clone());
                result
            };
            let result_hash = crate::stable_content_hash(&result);
            if result_hash == watcher.last_hash {
                continue;
            }
            if !send_watch_update(&watcher.sender, result) {
                stale.push(id);
            } else {
                hash_updates.push((id, result_hash));
            }
        }

        if !stale.is_empty() || !hash_updates.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for (id, hash) in hash_updates {
                if let Some(watcher) = inner.vector_subscriptions.get_mut(&id) {
                    watcher.last_hash = hash;
                }
            }
            for id in stale {
                inner.vector_subscriptions.remove(&id);
            }
        }

        Ok(())
    }

    fn notify_text_subscribers(&self, event: &ChangeEvent) -> Result<()> {
        let watchers: Vec<(u64, TextWatcher)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .text_subscriptions
                .iter()
                .map(|(id, watcher)| (*id, watcher.clone()))
                .collect()
        };

        let mut stale = Vec::new();
        let mut hash_updates = Vec::new();
        let mut results: BTreeMap<Vec<u8>, TextSearchResult> = BTreeMap::new();
        for (id, watcher) in watchers {
            if !text_watch_change_overlaps(&watcher.source, event) {
                continue;
            }
            let key = watch_key(&(
                watcher.source.clone(),
                watcher.query.clone(),
                watcher.spec.clone(),
            ));
            let result = if let Some(result) = results.get(&key) {
                result.clone()
            } else {
                let result = self.execute_text_search(
                    watcher.source.clone(),
                    &watcher.query,
                    watcher.spec.clone(),
                )?;
                #[cfg(test)]
                self.note_watch_recomputation();
                results.insert(key, result.clone());
                result
            };
            let result_hash = crate::stable_content_hash(&result);
            if result_hash == watcher.last_hash {
                continue;
            }
            if !send_watch_update(&watcher.sender, result) {
                stale.push(id);
            } else {
                hash_updates.push((id, result_hash));
            }
        }

        if !stale.is_empty() || !hash_updates.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for (id, hash) in hash_updates {
                if let Some(watcher) = inner.text_subscriptions.get_mut(&id) {
                    watcher.last_hash = hash;
                }
            }
            for id in stale {
                inner.text_subscriptions.remove(&id);
            }
        }

        Ok(())
    }

    fn notify_traversal_subscribers(&self, event: &ChangeEvent) -> Result<()> {
        let watchers: Vec<(u64, TraversalWatcher)> = {
            let inner = self.inner.lock().unwrap();
            inner
                .traversal_subscriptions
                .iter()
                .map(|(id, watcher)| (*id, watcher.clone()))
                .collect()
        };

        let mut stale = Vec::new();
        let mut updates = Vec::new();
        let mut results: BTreeMap<Vec<u8>, (TraversalResult, BTreeSet<String>)> = BTreeMap::new();
        for (id, watcher) in watchers {
            if !event.full_refresh && !traversal_watch_change_overlaps(&watcher, event) {
                continue;
            }
            let key = watch_key(&(
                watcher.anchor.clone(),
                watcher.segments.clone(),
                watcher.spec.clone(),
            ));
            let (result, dependency_paths) = if let Some((result, dependency_paths)) =
                results.get(&key)
            {
                (result.clone(), dependency_paths.clone())
            } else {
                let result = self.traverse_at(&watcher.anchor, &watcher.segments, &watcher.spec)?;
                #[cfg(test)]
                self.note_watch_recomputation();
                let dependency_paths =
                    traversal_dependency_paths(&watcher.anchor, &watcher.segments, &result);
                results.insert(key, (result.clone(), dependency_paths.clone()));
                (result, dependency_paths)
            };
            let result_hash = crate::stable_content_hash(&result);
            if result_hash == watcher.last_hash {
                continue;
            }
            if !send_watch_update(&watcher.sender, result) {
                stale.push(id);
            } else {
                updates.push((id, result_hash, dependency_paths));
            }
        }

        if !stale.is_empty() || !updates.is_empty() {
            let mut inner = self.inner.lock().unwrap();
            for (id, hash, dependency_paths) in updates {
                if let Some(watcher) = inner.traversal_subscriptions.get_mut(&id) {
                    watcher.last_hash = hash;
                    watcher.dependency_paths = dependency_paths;
                }
            }
            for id in stale {
                inner.traversal_subscriptions.remove(&id);
            }
        }

        Ok(())
    }

    #[cfg(test)]
    fn note_watch_recomputation(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.watch_recomputations = inner.watch_recomputations.saturating_add(1);
    }

    #[cfg(test)]
    fn watch_recomputation_count(&self) -> usize {
        self.inner.lock().unwrap().watch_recomputations
    }

    fn notify_change_subscribers(&self, event: ChangeEvent) -> Result<()> {
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
        let mut event = event;
        event.pending_ops = pending_ops;
        for (id, watcher) in watchers {
            if !send_watch_update(&watcher.sender, event.clone()) {
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
            put_json_inner(&mut inner, anchor, segments, value)?;
        }
        self.finalize_local_change()
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
            inner.ensure_canonical_write_allowed(anchor, segments)?;
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
        self.finalize_local_change()
    }

    fn unset(&self, anchor: &str, segments: &[String]) -> Result<()> {
        if segments.is_empty() {
            return Err(PrimadbError::ExpectedFieldPath {
                path: display_path(anchor, segments),
            });
        }

        {
            let mut inner = self.inner.lock().unwrap();
            unset_inner(&mut inner, anchor, segments)?;
        }
        self.finalize_local_change()
    }

    fn set_json(&self, anchor: &str, segments: &[String], value: JsonValue) -> Result<String> {
        if segments.is_empty() {
            return Err(PrimadbError::ExpectedFieldPath {
                path: display_path(anchor, segments),
            });
        }

        let member_id = {
            let mut inner = self.inner.lock().unwrap();
            set_json_inner(&mut inner, anchor, segments, value)?
        };
        self.finalize_local_change()?;
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
            inner.ensure_canonical_write_allowed(anchor, segments)?;
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
        self.finalize_local_change()?;
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
            remove_json_inner(&mut inner, anchor, segments, value)?
        };
        self.finalize_local_change()?;
        Ok(member_id)
    }
}

impl Default for Primadb {
    fn default() -> Self {
        Self::with_replica_id(HybridClock::default_actor())
    }
}

fn put_json_inner(
    inner: &mut Inner,
    anchor: &str,
    segments: &[String],
    value: JsonValue,
) -> Result<()> {
    inner.ensure_canonical_write_allowed(anchor, segments)?;
    if segments.is_empty() {
        let ParsedInput::Object(object) = parse_input(value, &display_path(anchor, segments))?
        else {
            return Err(PrimadbError::ExpectedObject {
                path: display_path(anchor, segments),
            });
        };
        inner.write_object_to_node(anchor, object, &display_path(anchor, segments))?;
    } else {
        let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)? else {
            return Err(PrimadbError::ExpectedFieldPath {
                path: display_path(anchor, segments),
            });
        };
        inner.write_value_to_field(&node, &field, value, &display_path(anchor, segments))?;
    }
    Ok(())
}

fn put_record_inner(inner: &mut Inner, key: &str, value: RecordValue) -> Result<()> {
    let node = crate::record_node_id(key);
    inner.set_field(
        node.clone(),
        "key".to_owned(),
        OperationValue::Scalar(JsonValue::String(key.to_owned())),
    );
    let value = match value {
        RecordValue::Json(value) => OperationValue::Scalar(value),
        RecordValue::Bytes(bytes) => OperationValue::Bytes(bytes),
        RecordValue::Blob(blob) => OperationValue::Blob(blob),
    };
    inner.set_field(node, "value".to_owned(), value);
    Ok(())
}

fn delete_record_inner(inner: &mut Inner, key: &str) {
    let node = crate::record_node_id(key);
    inner.set_field(
        node.clone(),
        "key".to_owned(),
        OperationValue::Scalar(JsonValue::String(key.to_owned())),
    );
    inner.delete_field(&node, "value");
}

fn record_entry_from_inner(inner: &mut Inner, key: &str) -> Result<Option<RecordEntry>> {
    let node_id = crate::record_node_id(key);
    let _ = inner.maybe_load_node(&node_id)?;
    Ok(inner
        .nodes
        .get(&node_id)
        .and_then(crate::record_entry_from_node_state)
        .filter(|entry| entry.key == key))
}

fn assert_record_precondition(inner: &mut Inner, precondition: &RecordPrecondition) -> Result<()> {
    match precondition {
        RecordPrecondition::Exists { key } => {
            if record_entry_from_inner(inner, key)?.is_some() {
                return Ok(());
            }
            Err(record_precondition_error(format!(
                "record `{key}` must exist"
            )))
        }
        RecordPrecondition::Absent { key } => {
            if record_entry_from_inner(inner, key)?.is_none() {
                return Ok(());
            }
            Err(record_precondition_error(format!(
                "record `{key}` must be absent"
            )))
        }
        RecordPrecondition::Value { key, value } => {
            let current = record_entry_from_inner(inner, key)?;
            if current.as_ref().map(|entry| &entry.value) == Some(value) {
                return Ok(());
            }
            Err(record_precondition_error(format!(
                "record `{key}` did not match the expected value"
            )))
        }
    }
}

fn record_precondition_error(message: String) -> PrimadbError {
    PrimadbError::TransactionConflict { message }
}

fn collect_record_entries_for_scan_locked(
    inner: &mut Inner,
    scan: &RecordScan,
) -> Result<Vec<RecordEntry>> {
    collect_record_entries_for_scan(inner, scan, inner.storage_engine.clone())
}

fn collect_record_entries_for_scan(
    inner: &mut Inner,
    scan: &RecordScan,
    storage_engine: Option<Arc<dyn IncrementalStore>>,
) -> Result<Vec<RecordEntry>> {
    let Some(storage_engine) = storage_engine else {
        return collect_record_entries_for_scan_page(inner, scan, None);
    };

    let Some(limit) = scan.limit else {
        let storage_entries = storage_engine.scan_record_entries(scan)?;
        return collect_record_entries_for_scan_page(inner, scan, storage_entries);
    };

    let page_limit = limit.saturating_add(1).max(1);
    let mut page_scan = scan.clone();
    page_scan.limit = Some(page_limit);
    let mut storage_entries = Vec::new();
    loop {
        let page = storage_engine.scan_record_entries(&page_scan)?;
        let page_is_short = page.as_ref().is_none_or(|page| page.len() < page_limit);
        let last_key = page
            .as_ref()
            .and_then(|page| page.last())
            .map(|entry| entry.key.clone());
        if let Some(page) = page {
            storage_entries.extend(page);
        }

        let merged =
            collect_record_entries_for_scan_page(inner, scan, Some(storage_entries.clone()))?;
        if merged.len() > limit || page_is_short || last_key.is_none() {
            return Ok(merged);
        }

        page_scan.cursor = last_key;
    }
}

fn collect_record_entries_for_scan_page(
    inner: &mut Inner,
    scan: &RecordScan,
    storage_entries: Option<Vec<RecordEntry>>,
) -> Result<Vec<RecordEntry>> {
    let mut entries = BTreeMap::new();
    if let Some(storage_entries) = storage_entries {
        for entry in storage_entries {
            if scan.matches_key(&entry.key) {
                entries.insert(entry.key.clone(), entry);
            }
        }
    }

    #[cfg(test)]
    let overlay_candidates_examined = inner.record_overlay.len();
    for (key, overlay_entry) in &inner.record_overlay {
        if !scan.matches_key(key) {
            continue;
        }
        if let Some(entry) = overlay_entry {
            entries.insert(entry.key.clone(), entry.clone());
        } else {
            entries.remove(key);
        }
    }
    #[cfg(test)]
    {
        inner.record_overlay_candidates_examined = inner
            .record_overlay_candidates_examined
            .saturating_add(overlay_candidates_examined);
    }

    Ok(entries.into_values().collect())
}

fn record_scan_result(mut entries: Vec<RecordEntry>, scan: &RecordScan) -> RecordScanResult {
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    if scan.reverse {
        entries.reverse();
    }
    let mut next_cursor = None;
    if let Some(limit) = scan.limit
        && entries.len() > limit
    {
        let overflow = entries.split_off(limit);
        next_cursor = entries
            .last()
            .map(|entry| entry.key.clone())
            .or_else(|| overflow.first().map(|entry| entry.key.clone()));
    }
    RecordScanResult {
        entries,
        next_cursor,
    }
}

#[derive(Debug, Default)]
struct PendingVectorItem {
    meta: Option<VectorItemMeta>,
    chunks: BTreeMap<usize, (crate::VectorChunkHeader, Vec<u8>)>,
    malformed: bool,
}

fn vector_rebuild_snapshot(inner: &mut Inner, collection: &str) -> Result<VectorRebuildSnapshot> {
    let (config, records, source_hash) =
        vector_collection_records_and_source_hash_locked(inner, collection)?;
    Ok(VectorRebuildSnapshot {
        collection: collection.to_owned(),
        config,
        records,
        source_hash,
        revision: inner.change_revision,
        #[cfg(not(target_arch = "wasm32"))]
        cache_root: inner.vector_cache_root.clone(),
    })
}

fn build_vector_collection_off_lock(
    snapshot: &VectorRebuildSnapshot,
) -> Result<(VectorCollectionCache, Option<VectorCacheFiles>)> {
    #[cfg(not(target_arch = "wasm32"))]
    let (cache, rebuilt) = if let Some(root) = &snapshot.cache_root
        && let Ok(Some(cache)) = read_native_vector_cache(
            root,
            &snapshot.collection,
            snapshot.config.clone(),
            &snapshot.source_hash,
        ) {
        (cache, false)
    } else {
        (
            assemble_vector_collection_cache(
                &snapshot.collection,
                snapshot.config.clone(),
                &snapshot.records,
                snapshot.source_hash.clone(),
            ),
            true,
        )
    };
    #[cfg(target_arch = "wasm32")]
    let (cache, rebuilt) = (
        assemble_vector_collection_cache(
            &snapshot.collection,
            snapshot.config.clone(),
            &snapshot.records,
            snapshot.source_hash.clone(),
        ),
        true,
    );
    let files = if rebuilt {
        let mut files =
            build_vector_cache_files(&snapshot.collection, &cache, now_millis().to_string())?;
        files.manifest.source_revision = Some(snapshot.revision);
        Some(files)
    } else {
        None
    };
    Ok((cache, files))
}

fn vector_collection_records_and_source_hash_locked(
    inner: &mut Inner,
    collection: &str,
) -> Result<(VectorCollectionConfig, Vec<RecordEntry>, String)> {
    let meta_key = vector_collection_meta_key(collection);
    let Some(meta_entry) = record_entry_from_inner(inner, &meta_key)? else {
        inner.vector_collections.remove(collection);
        return Err(PrimadbError::Message(format!(
            "vector collection `{collection}` does not exist"
        )));
    };
    let config = collection_config_from_record(&meta_entry)?;
    let scan = RecordScan {
        prefix: Some(vector_collection_items_prefix(collection)),
        ..RecordScan::default()
    };
    let mut record_entries = collect_record_entries_for_scan_locked(inner, &scan)?;
    record_entries.push(meta_entry);
    let source_hash = records_source_hash(&record_entries);
    Ok((config, record_entries, source_hash))
}

fn assemble_vector_collection_cache(
    collection: &str,
    config: VectorCollectionConfig,
    records: &[RecordEntry],
    source_hash: String,
) -> VectorCollectionCache {
    let mut items: BTreeMap<String, PendingVectorItem> = BTreeMap::new();
    for entry in records {
        let Some(item_id) = vector_item_id_from_record_key(collection, &entry.key) else {
            continue;
        };
        let item = items.entry(item_id).or_default();
        if entry.key.ends_with("/meta") {
            match item_meta_from_record(entry) {
                Ok(meta) => item.meta = Some(meta),
                Err(_) => item.malformed = true,
            }
        } else if entry.key.contains("/chunks/") {
            match chunk_from_record(entry) {
                Ok((header, payload)) => {
                    item.chunks.insert(header.chunk_index, (header, payload));
                }
                Err(_) => item.malformed = true,
            }
        }
    }

    let mut cache = VectorCollectionCache::empty(config.clone());
    cache.source_hash = source_hash;
    for (path_id, item) in items {
        let Some(meta) = item.meta else {
            cache.incomplete_count = cache.incomplete_count.saturating_add(1);
            continue;
        };
        if item.malformed || meta.id != path_id {
            cache.incomplete_count = cache.incomplete_count.saturating_add(1);
            continue;
        }
        if meta.deleted {
            cache.deleted_count = cache.deleted_count.saturating_add(1);
            continue;
        }
        match assemble_complete_vector_item(&config, &meta, &item.chunks) {
            Ok(entry) => {
                cache.entries.insert(meta.id.clone(), entry);
            }
            Err(_) => {
                cache.incomplete_count = cache.incomplete_count.saturating_add(1);
            }
        }
    }
    cache.state = VectorManagerState::Ready;
    cache.dirty = false;
    let _ = build_vector_ann(&mut cache);
    cache
}

fn assemble_complete_vector_item(
    config: &VectorCollectionConfig,
    meta: &VectorItemMeta,
    chunks: &BTreeMap<usize, (crate::VectorChunkHeader, Vec<u8>)>,
) -> Result<VectorCacheEntry> {
    if meta.dim != config.dim
        || meta.encoding != crate::vector::VECTOR_ENCODING_F32_LE
        || meta.chunk_count == 0
        || meta.byte_length != config.dim.saturating_mul(4)
    {
        return Err(PrimadbError::Message(
            "vector item metadata is incomplete".to_owned(),
        ));
    }
    let mut bytes = Vec::with_capacity(meta.byte_length);
    for index in 0..meta.chunk_count {
        let Some((header, payload)) = chunks.get(&index) else {
            return Err(PrimadbError::Message("missing vector chunk".to_owned()));
        };
        if header.write_id != meta.write_id
            || header.chunk_index != index
            || header.chunk_count != meta.chunk_count
            || header.byte_offset != bytes.len()
            || header.checksum != checksum_bytes(payload)
        {
            return Err(PrimadbError::Message(
                "vector chunk header mismatch".to_owned(),
            ));
        }
        bytes.extend_from_slice(payload);
    }
    if bytes.len() != meta.byte_length || checksum_bytes(&bytes) != meta.checksum {
        return Err(PrimadbError::Message("vector checksum mismatch".to_owned()));
    }
    let vector = crate::vector::decode_f32_le(&bytes)?;
    validate_vector(&vector, config.dim)?;
    Ok(VectorCacheEntry {
        vector,
        metadata: meta.metadata.clone(),
        write_id: meta.write_id.clone(),
        checksum: meta.checksum.clone(),
    })
}

fn text_rebuild_snapshot(inner: &mut Inner, collection: &str) -> Result<TextRebuildSnapshot> {
    let (config, records, source_hash) =
        text_collection_records_and_source_hash_locked(inner, collection)?;
    Ok(TextRebuildSnapshot {
        collection: collection.to_owned(),
        config,
        records,
        source_hash,
        revision: inner.change_revision,
        #[cfg(not(target_arch = "wasm32"))]
        cache_root: inner.text_cache_root.clone(),
    })
}

fn build_text_collection_off_lock(
    snapshot: &TextRebuildSnapshot,
) -> Result<(TextCollectionCache, Option<TextCacheFiles>)> {
    #[cfg(not(target_arch = "wasm32"))]
    let (cache, rebuilt) = if let Some(root) = &snapshot.cache_root
        && let Ok(Some(cache)) = read_native_text_cache(
            root,
            &snapshot.collection,
            snapshot.config.clone(),
            &snapshot.source_hash,
        ) {
        (cache, false)
    } else {
        (
            assemble_text_collection_cache(
                &snapshot.collection,
                snapshot.config.clone(),
                &snapshot.records,
                snapshot.source_hash.clone(),
            )?,
            true,
        )
    };
    #[cfg(target_arch = "wasm32")]
    let (cache, rebuilt) = (
        assemble_text_collection_cache(
            &snapshot.collection,
            snapshot.config.clone(),
            &snapshot.records,
            snapshot.source_hash.clone(),
        )?,
        true,
    );
    let files = if rebuilt {
        let mut files = text_cache_files(&snapshot.collection, &cache, now_millis().to_string())?;
        files.manifest.source_revision = Some(snapshot.revision);
        Some(files)
    } else {
        None
    };
    Ok((cache, files))
}

fn text_collection_records_and_source_hash_locked(
    inner: &mut Inner,
    collection: &str,
) -> Result<(TextCollectionConfig, Vec<RecordEntry>, String)> {
    let config_key = text_collection_config_key(collection);
    let Some(config_entry) = record_entry_from_inner(inner, &config_key)? else {
        inner.text_collections.remove(collection);
        return Err(PrimadbError::Message(format!(
            "text collection `{collection}` does not exist"
        )));
    };
    let config = text_collection_config_from_record(&config_entry)?;
    let scan = RecordScan {
        prefix: Some(text_collection_docs_prefix(collection)),
        ..RecordScan::default()
    };
    let mut record_entries = collect_record_entries_for_scan_locked(inner, &scan)?;
    record_entries.push(config_entry);
    let source_hash = records_source_hash(&record_entries);
    Ok((config, record_entries, source_hash))
}

fn assemble_text_collection_cache(
    collection: &str,
    config: TextCollectionConfig,
    records: &[RecordEntry],
    source_hash: String,
) -> Result<TextCollectionCache> {
    let mut documents = BTreeMap::new();
    let mut deleted_count = 0_usize;
    for entry in records {
        let Some(path_id) = text_document_id_from_record_key(collection, &entry.key) else {
            continue;
        };
        match text_document_from_record(entry) {
            Ok(document) if document.id == path_id => {
                documents.insert(document.id.clone(), document);
            }
            Ok(_) | Err(_) => {
                deleted_count = deleted_count.saturating_add(1);
            }
        }
    }
    let mut cache = TextCollectionCache::from_documents(config, documents, source_hash)?;
    cache.deleted_count = deleted_count;
    Ok(cache)
}

fn vector_watch_change_overlaps(collection: &str, event: &ChangeEvent) -> bool {
    if event.full_refresh {
        return true;
    }
    if !event.records_changed {
        return false;
    }
    event.touched_record_keys.is_empty()
        || event
            .touched_record_keys
            .iter()
            .any(|key| vector_collection_from_record_key(key).as_deref() == Some(collection))
}

fn text_watch_change_overlaps(source: &TextSearchSource, event: &ChangeEvent) -> bool {
    match source {
        TextSearchSource::Collection { collection } => {
            text_collection_watch_overlaps(collection, event)
        }
        TextSearchSource::GraphQuery { path, .. } => watch_change_overlaps(&path.path(), event),
        TextSearchSource::Records { scan } => record_watch_change_overlaps(scan, event),
    }
}

fn text_collection_watch_overlaps(collection: &str, event: &ChangeEvent) -> bool {
    if event.full_refresh {
        return true;
    }
    if !event.records_changed {
        return false;
    }
    event.touched_record_keys.is_empty()
        || event
            .touched_record_keys
            .iter()
            .any(|key| text_collection_from_record_key(key).as_deref() == Some(collection))
}

fn text_manager_state_capability_name(state: crate::TextIndexState) -> &'static str {
    match state {
        crate::TextIndexState::Ready => "ready",
        crate::TextIndexState::Rebuilding => "rebuilding",
        crate::TextIndexState::Stale => "stale",
        crate::TextIndexState::Failed => "failed",
    }
}

fn vector_metric_capability_name(metric: VectorMetric) -> &'static str {
    match metric {
        VectorMetric::Cosine => "cosine",
        VectorMetric::L2 => "l2",
        VectorMetric::Dot => "dot",
    }
}

fn vector_backend_capability_name(backend: VectorBackendKind) -> &'static str {
    match backend {
        VectorBackendKind::Exact => "exact",
        VectorBackendKind::Edgevec => "edgevec",
    }
}

fn vector_manager_state_capability_name(state: VectorManagerState) -> &'static str {
    match state {
        VectorManagerState::Ready => "ready",
        VectorManagerState::CatchingUp => "catching_up",
        VectorManagerState::Rebuilding => "rebuilding",
        VectorManagerState::Stale => "stale",
        VectorManagerState::Failed => "failed",
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn read_native_vector_cache(
    root: &std::path::Path,
    collection: &str,
    config: VectorCollectionConfig,
    source_hash: &str,
) -> Result<Option<VectorCollectionCache>> {
    let collection_dir = root.join(crate::encode_component(collection));
    let manifest_path = collection_dir.join("manifest.json");
    if !manifest_path.exists() {
        return Ok(None);
    }
    let manifest = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    let vectors_f32 = read_native_vector_cache_file(&collection_dir.join("vectors.f32"))?;
    let keys_bin = read_native_vector_cache_file(&collection_dir.join("keys.bin"))?;
    let metadata_bin = read_native_vector_cache_file(&collection_dir.join("metadata.bin"))?;
    let backend_edgevec =
        match read_native_vector_cache_file(&collection_dir.join("backend.edgevec")) {
            Ok(bytes) => Some(bytes),
            Err(PrimadbError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
    let files = crate::VectorCacheFiles {
        manifest,
        vectors_f32,
        keys_bin,
        metadata_bin,
        backend_edgevec,
    };
    if files.manifest.collection != collection
        || files.manifest.record_prefix != vector_collection_items_prefix(collection)
    {
        return Err(PrimadbError::Message(
            "vector cache manifest belongs to another collection".to_owned(),
        ));
    }
    collection_cache_from_cache_files(config, files, source_hash).map(Some)
}

#[cfg(not(target_arch = "wasm32"))]
fn read_native_vector_cache_file(path: &std::path::Path) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path)?;
    let len = file.metadata()?.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    // SAFETY: the file is opened read-only and copied into an owned Vec before
    // returning, so no mapped reference escapes this function.
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    Ok(mmap.to_vec())
}

#[cfg(not(target_arch = "wasm32"))]
fn write_native_vector_cache(
    root: &std::path::Path,
    collection: &str,
    files: VectorCacheFiles,
) -> Result<()> {
    let collection_dir = root.join(crate::encode_component(collection));
    std::fs::create_dir_all(&collection_dir)?;
    std::fs::write(collection_dir.join("vectors.f32.tmp"), &files.vectors_f32)?;
    std::fs::write(collection_dir.join("keys.bin.tmp"), &files.keys_bin)?;
    std::fs::write(collection_dir.join("metadata.bin.tmp"), &files.metadata_bin)?;
    if let Some(edgevec) = &files.backend_edgevec {
        std::fs::write(collection_dir.join("backend.edgevec.tmp"), edgevec)?;
    }
    std::fs::write(
        collection_dir.join("manifest.json.tmp"),
        serde_json::to_vec_pretty(&files.manifest)?,
    )?;
    std::fs::rename(
        collection_dir.join("vectors.f32.tmp"),
        collection_dir.join("vectors.f32"),
    )?;
    std::fs::rename(
        collection_dir.join("keys.bin.tmp"),
        collection_dir.join("keys.bin"),
    )?;
    std::fs::rename(
        collection_dir.join("metadata.bin.tmp"),
        collection_dir.join("metadata.bin"),
    )?;
    if files.backend_edgevec.is_some() {
        std::fs::rename(
            collection_dir.join("backend.edgevec.tmp"),
            collection_dir.join("backend.edgevec"),
        )?;
    } else {
        match std::fs::remove_file(collection_dir.join("backend.edgevec")) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    std::fs::rename(
        collection_dir.join("manifest.json.tmp"),
        collection_dir.join("manifest.json"),
    )?;
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
fn read_native_text_cache(
    root: &std::path::Path,
    collection: &str,
    config: TextCollectionConfig,
    source_hash: &str,
) -> Result<Option<TextCollectionCache>> {
    let collection_dir = root.join(crate::encode_component(collection));
    let manifest_path = collection_dir.join("manifest.json");
    if !manifest_path.exists() {
        return Ok(None);
    }
    let manifest = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    let terms_bin = read_native_vector_cache_file(&collection_dir.join("terms.bin"))?;
    let postings_bin = read_native_vector_cache_file(&collection_dir.join("postings.bin"))?;
    let docs_bin = read_native_vector_cache_file(&collection_dir.join("docs.bin"))?;
    let metadata_bin = read_native_vector_cache_file(&collection_dir.join("metadata.bin"))?;
    let files = TextCacheFiles {
        manifest,
        terms_bin,
        postings_bin,
        docs_bin,
        metadata_bin,
    };
    if files.manifest.collection != collection
        || files.manifest.record_prefix != text_collection_docs_prefix(collection)
    {
        return Err(PrimadbError::Message(
            "text cache manifest belongs to another collection".to_owned(),
        ));
    }
    collection_cache_from_text_cache_files(config, files, source_hash).map(Some)
}

#[cfg(not(target_arch = "wasm32"))]
fn write_native_text_cache(
    root: &std::path::Path,
    collection: &str,
    files: TextCacheFiles,
) -> Result<()> {
    let collection_dir = root.join(crate::encode_component(collection));
    std::fs::create_dir_all(&collection_dir)?;
    std::fs::write(collection_dir.join("terms.bin.tmp"), &files.terms_bin)?;
    std::fs::write(collection_dir.join("postings.bin.tmp"), &files.postings_bin)?;
    std::fs::write(collection_dir.join("docs.bin.tmp"), &files.docs_bin)?;
    std::fs::write(collection_dir.join("metadata.bin.tmp"), &files.metadata_bin)?;
    std::fs::write(
        collection_dir.join("manifest.json.tmp"),
        serde_json::to_vec_pretty(&files.manifest)?,
    )?;
    std::fs::rename(
        collection_dir.join("terms.bin.tmp"),
        collection_dir.join("terms.bin"),
    )?;
    std::fs::rename(
        collection_dir.join("postings.bin.tmp"),
        collection_dir.join("postings.bin"),
    )?;
    std::fs::rename(
        collection_dir.join("docs.bin.tmp"),
        collection_dir.join("docs.bin"),
    )?;
    std::fs::rename(
        collection_dir.join("metadata.bin.tmp"),
        collection_dir.join("metadata.bin"),
    )?;
    std::fs::rename(
        collection_dir.join("manifest.json.tmp"),
        collection_dir.join("manifest.json"),
    )?;
    Ok(())
}

fn unset_inner(inner: &mut Inner, anchor: &str, segments: &[String]) -> Result<()> {
    if segments.is_empty() {
        return Err(PrimadbError::ExpectedFieldPath {
            path: display_path(anchor, segments),
        });
    }
    inner.ensure_canonical_write_allowed(anchor, segments)?;
    let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)? else {
        return Err(PrimadbError::ExpectedFieldPath {
            path: display_path(anchor, segments),
        });
    };
    inner.delete_field(&node, &field);
    Ok(())
}

fn set_json_inner(
    inner: &mut Inner,
    anchor: &str,
    segments: &[String],
    value: JsonValue,
) -> Result<String> {
    if segments.is_empty() {
        return Err(PrimadbError::ExpectedFieldPath {
            path: display_path(anchor, segments),
        });
    }
    inner.ensure_canonical_write_allowed(anchor, segments)?;
    let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)? else {
        return Err(PrimadbError::ExpectedFieldPath {
            path: display_path(anchor, segments),
        });
    };
    let parsed = parse_input(value, &display_path(anchor, segments))?;
    inner.add_member_to_set(&node, &field, parsed, &display_path(anchor, segments))
}

fn remove_json_inner(
    inner: &mut Inner,
    anchor: &str,
    segments: &[String],
    value: JsonValue,
) -> Result<String> {
    if segments.is_empty() {
        return Err(PrimadbError::ExpectedFieldPath {
            path: display_path(anchor, segments),
        });
    }
    inner.ensure_canonical_write_allowed(anchor, segments)?;
    let Cursor::Field { node, field } = inner.ensure_field_cursor(anchor, segments)? else {
        return Err(PrimadbError::ExpectedFieldPath {
            path: display_path(anchor, segments),
        });
    };
    let member_id = parse_member_reference(value, &display_path(anchor, segments))?;
    inner.remove_member_from_set(&node, &field, &member_id);
    Ok(member_id)
}

fn materialize_inner(
    inner: &mut Inner,
    anchor: &str,
    segments: &[String],
) -> Result<Option<JsonValue>> {
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
            Ok(Some(inner.materialize_field(
                &node,
                &field,
                &value,
                &mut BTreeSet::new(),
            )))
        }
        None => Ok(None),
    }
}

fn path_exists_inner(inner: &mut Inner, anchor: &str, segments: &[String]) -> Result<bool> {
    match inner.resolve_cursor(anchor, segments)? {
        Some(Cursor::Node(node)) => Ok(inner.nodes.contains_key(&node)),
        Some(Cursor::Field { node, field }) => Ok(inner
            .nodes
            .get(&node)
            .and_then(|node_state| node_state.fields.get(&field))
            .is_some()),
        None => Ok(false),
    }
}

fn revision_at_inner(
    inner: &mut Inner,
    anchor: &str,
    segments: &[String],
) -> Result<Option<VersionMarker>> {
    match inner.resolve_cursor(anchor, segments)? {
        Some(Cursor::Node(node)) => Ok(inner.nodes.get(&node).and_then(node_revision_marker)),
        Some(Cursor::Field { node, field }) => {
            let Some(node_state) = inner.nodes.get(&node) else {
                return Ok(None);
            };
            Ok(node_state
                .fields
                .get(&field)
                .map(|state| state.version.clone())
                .or_else(|| node_state.tombstones.get(&field).cloned()))
        }
        None => Ok(None),
    }
}

fn node_revision_marker(node: &NodeState) -> Option<VersionMarker> {
    let mut marker = None;
    for field in node.fields.values() {
        marker = max_marker(marker, Some(field.version.clone()));
    }
    for tombstone in node.tombstones.values() {
        marker = max_marker(marker, Some(tombstone.clone()));
    }
    marker
}

fn apply_transaction_steps(tx: &mut Transaction<'_>, steps: &[TransactionStep]) -> Result<()> {
    for step in steps {
        match step.clone() {
            TransactionStep::Put { path, value } => tx.chain(path).put_json(value)?,
            TransactionStep::Unset { path } => tx.chain(path).unset()?,
            TransactionStep::Set { path, value } => {
                tx.chain(path).set_json(value)?;
            }
            TransactionStep::Remove { path, value } => {
                tx.chain(path).remove_json(value)?;
            }
            TransactionStep::AssertExists { path } => tx.chain(path).assert_exists()?,
            TransactionStep::AssertAbsent { path } => tx.chain(path).assert_absent()?,
            TransactionStep::AssertValue { path, value } => tx.chain(path).assert_value(value)?,
            TransactionStep::AssertRevision { path, revision } => {
                tx.chain(path).assert_revision(revision)?
            }
            TransactionStep::Increment { path, by } => {
                tx.chain(path).increment(by)?;
            }
        }
    }
    Ok(())
}

fn transaction_step_path(step: &TransactionStep) -> &RemotePath {
    match step {
        TransactionStep::Put { path, .. }
        | TransactionStep::Unset { path }
        | TransactionStep::Set { path, .. }
        | TransactionStep::Remove { path, .. }
        | TransactionStep::AssertExists { path }
        | TransactionStep::AssertAbsent { path }
        | TransactionStep::AssertValue { path, .. }
        | TransactionStep::AssertRevision { path, .. }
        | TransactionStep::Increment { path, .. } => path,
    }
}

fn scope_transaction_step(scope: &str, step: TransactionStep) -> TransactionStep {
    match step {
        TransactionStep::Put { path, value } => TransactionStep::Put {
            path: scope_remote_path(scope, path),
            value,
        },
        TransactionStep::Unset { path } => TransactionStep::Unset {
            path: scope_remote_path(scope, path),
        },
        TransactionStep::Set { path, value } => TransactionStep::Set {
            path: scope_remote_path(scope, path),
            value,
        },
        TransactionStep::Remove { path, value } => TransactionStep::Remove {
            path: scope_remote_path(scope, path),
            value,
        },
        TransactionStep::AssertExists { path } => TransactionStep::AssertExists {
            path: scope_remote_path(scope, path),
        },
        TransactionStep::AssertAbsent { path } => TransactionStep::AssertAbsent {
            path: scope_remote_path(scope, path),
        },
        TransactionStep::AssertValue { path, value } => TransactionStep::AssertValue {
            path: scope_remote_path(scope, path),
            value,
        },
        TransactionStep::AssertRevision { path, revision } => TransactionStep::AssertRevision {
            path: scope_remote_path(scope, path),
            revision,
        },
        TransactionStep::Increment { path, by } => TransactionStep::Increment {
            path: scope_remote_path(scope, path),
            by,
        },
    }
}

fn scope_remote_path(scope: &str, path: RemotePath) -> RemotePath {
    let (anchor, segments) = scoped_anchor_segments(Some(scope), path.anchor, path.segments);
    RemotePath { anchor, segments }
}

fn scoped_anchor_segments(
    scope: Option<&str>,
    anchor: String,
    segments: Vec<String>,
) -> (String, Vec<String>) {
    let Some(scope) = scope else {
        return (anchor, segments);
    };
    if anchor.is_empty() {
        return (scope.to_owned(), segments);
    }
    if node_matches_root(&anchor, scope) {
        return (anchor, segments);
    }
    let mut scoped_segments = Vec::with_capacity(segments.len() + 1);
    scoped_segments.push(anchor);
    scoped_segments.extend(segments);
    (scope.to_owned(), scoped_segments)
}

fn scope_policy_matches_authority_actor(policy: &ScopePolicy, actor: &str) -> bool {
    let Some(authority) = &policy.authority else {
        return false;
    };
    match authority {
        ScopeAuthority::Peer { peer_id } | ScopeAuthority::FullNode { peer_id } => {
            peer_id == actor
                || peer_id == &format!("native:{actor}")
                || peer_id == &format!("full-node:{actor}")
        }
        ScopeAuthority::Quorum { peers, threshold } => {
            *threshold <= 1
                && peers.iter().any(|peer_id| {
                    peer_id == actor
                        || peer_id == &format!("native:{actor}")
                        || peer_id == &format!("full-node:{actor}")
                })
        }
    }
}

impl Scope {
    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn configure(&self, policy: ScopePolicy) -> Result<()> {
        self.db.configure_scope_policy(&self.root, policy)
    }

    pub fn policy(&self) -> Option<ScopePolicy> {
        self.db.scope_policy(&self.root)
    }

    pub fn proposals(&self) -> Vec<ProvisionalTransaction> {
        self.db.provisional_transactions_for_scope(&self.root)
    }

    pub fn transaction<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Transaction<'_>) -> Result<T>,
    {
        let policy = self.policy().unwrap_or_default();
        if policy.consistency == ScopeConsistency::Coordinated
            && !scope_policy_matches_authority_actor(&policy, &self.db.replica_id())
        {
            return Err(PrimadbError::StrictScopeUnavailable {
                scope: self.root.clone(),
            });
        }

        self.db
            .run_local_transaction_in_scope(Some(self.root.clone()), f)
            .map(|(value, _, _)| value)
    }

    pub fn transaction_steps(
        &self,
        steps: Vec<TransactionStep>,
        options: TransactionOptions,
    ) -> Result<TransactionReport> {
        let steps = steps
            .into_iter()
            .map(|step| scope_transaction_step(&self.root, step))
            .collect::<Vec<_>>();
        self.db
            .validate_transaction_scopes(Some(&self.root), &steps)?;

        let policy = self.policy().unwrap_or_default();
        if policy.consistency == ScopeConsistency::Coordinated
            && !scope_policy_matches_authority_actor(&policy, &self.db.replica_id())
        {
            let offline = options
                .offline
                .clone()
                .unwrap_or_else(|| policy.offline_writes.clone());
            return match offline {
                ScopeOfflineWrites::Reject => Err(PrimadbError::StrictScopeUnavailable {
                    scope: self.root.clone(),
                }),
                ScopeOfflineWrites::QueueProvisional => self
                    .db
                    .queue_provisional_transaction(&self.root, steps, options),
            };
        }

        let (_, member_ids, operation_count) = self
            .db
            .run_local_transaction_in_scope(Some(self.root.clone()), |tx| {
                apply_transaction_steps(tx, &steps)
            })?;
        Ok(TransactionReport {
            status: TransactionStatus::Committed,
            operation_count,
            member_ids,
            proposal_id: None,
        })
    }
}

impl<'a> Transaction<'a> {
    pub fn root<'tx>(&'tx mut self, node: impl Into<String>) -> TransactionChain<'tx, 'a> {
        let (anchor, segments) =
            scoped_anchor_segments(self.scope_root.as_deref(), node.into(), Vec::new());
        TransactionChain {
            tx: self,
            anchor,
            segments,
        }
    }

    pub fn chain<'tx>(&'tx mut self, path: RemotePath) -> TransactionChain<'tx, 'a> {
        let (anchor, segments) =
            scoped_anchor_segments(self.scope_root.as_deref(), path.anchor, path.segments);
        TransactionChain {
            tx: self,
            anchor,
            segments,
        }
    }

    pub fn member_ids(&self) -> &[String] {
        &self.member_ids
    }

    pub fn get_record(&mut self, key: &str) -> Result<Option<RecordEntry>> {
        record_entry_from_inner(self.inner, key)
    }

    pub fn assert_record_exists(&mut self, key: &str) -> Result<()> {
        assert_record_precondition(
            self.inner,
            &RecordPrecondition::Exists {
                key: key.to_owned(),
            },
        )
    }

    pub fn assert_record_absent(&mut self, key: &str) -> Result<()> {
        assert_record_precondition(
            self.inner,
            &RecordPrecondition::Absent {
                key: key.to_owned(),
            },
        )
    }

    pub fn assert_record_value(&mut self, key: &str, value: &RecordValue) -> Result<()> {
        assert_record_precondition(
            self.inner,
            &RecordPrecondition::Value {
                key: key.to_owned(),
                value: value.clone(),
            },
        )
    }

    pub fn put_record(&mut self, key: impl Into<String>, value: RecordValue) -> Result<()> {
        let key = key.into();
        put_record_inner(self.inner, &key, value)
    }

    pub fn delete_record(&mut self, key: impl AsRef<str>) -> Result<()> {
        delete_record_inner(self.inner, key.as_ref());
        Ok(())
    }
}

impl<'tx, 'inner> TransactionChain<'tx, 'inner> {
    pub fn field(mut self, key: impl Into<String>) -> Self {
        self.segments.push(key.into());
        self
    }

    pub fn path(&self) -> String {
        display_path(&self.anchor, &self.segments)
    }

    pub fn put<T: Serialize>(&mut self, value: T) -> Result<()> {
        self.put_json(serde_json::to_value(value)?)
    }

    pub fn put_json(&mut self, value: JsonValue) -> Result<()> {
        put_json_inner(self.tx.inner, &self.anchor, &self.segments, value)
    }

    pub fn unset(&mut self) -> Result<()> {
        unset_inner(self.tx.inner, &self.anchor, &self.segments)
    }

    pub fn set<T: Serialize>(&mut self, value: T) -> Result<String> {
        self.set_json(serde_json::to_value(value)?)
    }

    pub fn set_json(&mut self, value: JsonValue) -> Result<String> {
        let member_id = set_json_inner(self.tx.inner, &self.anchor, &self.segments, value)?;
        self.tx.member_ids.push(member_id.clone());
        Ok(member_id)
    }

    pub fn remove<T: Serialize>(&mut self, value: T) -> Result<String> {
        self.remove_json(serde_json::to_value(value)?)
    }

    pub fn remove_json(&mut self, value: JsonValue) -> Result<String> {
        remove_json_inner(self.tx.inner, &self.anchor, &self.segments, value)
    }

    pub fn once_json(&mut self) -> Result<Option<JsonValue>> {
        materialize_inner(self.tx.inner, &self.anchor, &self.segments)
    }

    pub fn revision(&mut self) -> Result<Option<VersionMarker>> {
        revision_at_inner(self.tx.inner, &self.anchor, &self.segments)
    }

    pub fn assert_exists(&mut self) -> Result<()> {
        if path_exists_inner(self.tx.inner, &self.anchor, &self.segments)? {
            Ok(())
        } else {
            Err(PrimadbError::TransactionConflict {
                message: format!("expected `{}` to exist", self.path()),
            })
        }
    }

    pub fn assert_absent(&mut self) -> Result<()> {
        if path_exists_inner(self.tx.inner, &self.anchor, &self.segments)? {
            Err(PrimadbError::TransactionConflict {
                message: format!("expected `{}` to be absent", self.path()),
            })
        } else {
            Ok(())
        }
    }

    pub fn assert_value<T: Serialize>(&mut self, expected: T) -> Result<()> {
        let expected = serde_json::to_value(expected)?;
        let current = self.once_json()?;
        if current.as_ref() == Some(&expected) {
            Ok(())
        } else {
            Err(PrimadbError::TransactionConflict {
                message: format!("expected `{}` to equal `{expected}`", self.path()),
            })
        }
    }

    pub fn assert_revision(&mut self, expected: Option<VersionMarker>) -> Result<()> {
        let current = self.revision()?;
        if current == expected {
            Ok(())
        } else {
            Err(PrimadbError::TransactionConflict {
                message: format!("revision conflict at `{}`", self.path()),
            })
        }
    }

    pub fn increment(&mut self, by: f64) -> Result<f64> {
        let current = self.once_json()?;
        let base = match current {
            Some(JsonValue::Number(number)) => {
                number
                    .as_f64()
                    .ok_or_else(|| PrimadbError::TransactionConflict {
                        message: format!("`{}` is not a finite number", self.path()),
                    })?
            }
            Some(_) => {
                return Err(PrimadbError::TransactionConflict {
                    message: format!("`{}` is not numeric", self.path()),
                });
            }
            None => 0.0,
        };
        let next = base + by;
        let number = serde_json::Number::from_f64(next).ok_or_else(|| {
            PrimadbError::TransactionConflict {
                message: format!("increment produced a non-finite value at `{}`", self.path()),
            }
        })?;
        self.put_json(JsonValue::Number(number))?;
        Ok(next)
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

    pub fn put_blob(&self, data: impl AsRef<[u8]>, media_type: Option<&str>) -> Result<BlobRef> {
        let reference = self.db.store_blob(data.as_ref(), media_type)?;
        self.db
            .put_json(&self.anchor, &self.segments, blob_marker_value(&reference))?;
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

    pub fn traverse(&self, spec: TraversalSpec) -> Result<TraversalResult> {
        self.db.traverse_at(&self.anchor, &self.segments, &spec)
    }

    pub fn first(&self, spec: QuerySpec) -> Result<Option<MapEntry>> {
        let mut entries = self.query(spec)?;
        Ok(entries.drain(..).next())
    }

    pub fn subscribe(&self) -> Result<Subscription> {
        self.db.subscribe_to(&self.anchor, &self.segments)
    }

    pub fn watch_traverse(&self, spec: TraversalSpec) -> Result<TraversalSubscription> {
        self.db
            .subscribe_to_traversal(&self.anchor, &self.segments, spec)
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

impl TraversalSubscription {
    pub fn receiver(&self) -> Receiver<TraversalResult> {
        self.inner.receiver.clone()
    }

    pub async fn recv(&self) -> Option<TraversalResult> {
        self.inner.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<TraversalResult> {
        self.inner.receiver.try_recv().ok()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<TraversalResult> {
        self.inner.receiver.recv_blocking().ok()
    }
}

impl RecordWatchSubscription {
    pub fn receiver(&self) -> Receiver<RecordScanResult> {
        self.inner.receiver.clone()
    }

    pub async fn recv(&self) -> Option<RecordScanResult> {
        self.inner.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<RecordScanResult> {
        self.inner.receiver.try_recv().ok()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<RecordScanResult> {
        self.inner.receiver.recv_blocking().ok()
    }
}

impl VectorWatchSubscription {
    pub fn receiver(&self) -> Receiver<VectorSearchResult> {
        self.inner.receiver.clone()
    }

    pub async fn recv(&self) -> Option<VectorSearchResult> {
        self.inner.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<VectorSearchResult> {
        self.inner.receiver.try_recv().ok()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<VectorSearchResult> {
        self.inner.receiver.recv_blocking().ok()
    }
}

impl TextWatchSubscription {
    pub fn receiver(&self) -> Receiver<TextSearchResult> {
        self.inner.receiver.clone()
    }

    pub async fn recv(&self) -> Option<TextSearchResult> {
        self.inner.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<TextSearchResult> {
        self.inner.receiver.try_recv().ok()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<TextSearchResult> {
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

impl Clone for TraversalSubscription {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Clone for RecordWatchSubscription {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Clone for VectorWatchSubscription {
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

impl Drop for TraversalSubscriptionInner {
    fn drop(&mut self) {
        if let Some(db) = self.db.upgrade() {
            if let Ok(mut inner) = db.lock() {
                inner.traversal_subscriptions.remove(&self.id);
            }
        }
    }
}

impl Drop for RecordWatchSubscriptionInner {
    fn drop(&mut self) {
        if let Some(db) = self.db.upgrade() {
            if let Ok(mut inner) = db.lock() {
                inner.record_subscriptions.remove(&self.id);
            }
        }
    }
}

impl Drop for VectorWatchSubscriptionInner {
    fn drop(&mut self) {
        if let Some(db) = self.db.upgrade() {
            if let Ok(mut inner) = db.lock() {
                inner.vector_subscriptions.remove(&self.id);
            }
        }
    }
}

impl Drop for TextWatchSubscriptionInner {
    fn drop(&mut self) {
        if let Some(db) = self.db.upgrade() {
            if let Ok(mut inner) = db.lock() {
                inner.text_subscriptions.remove(&self.id);
            }
        }
    }
}

impl Inner {
    fn refresh_record_overlay(&mut self, node_id: &str) {
        if !crate::is_record_node_id(node_id) {
            return;
        }

        if let Some(previous_key) = self.record_overlay_node_keys.remove(node_id) {
            self.record_overlay.remove(&previous_key);
        }

        let Some(node_state) = self.nodes.get(node_id) else {
            return;
        };
        let Some(key) = crate::record_key_from_node_state(node_state) else {
            return;
        };
        self.record_overlay_node_keys
            .insert(node_id.to_owned(), key.clone());
        self.record_overlay
            .insert(key, crate::record_entry_from_node_state(node_state));
    }

    fn rebuild_record_overlay(&mut self) {
        self.record_overlay.clear();
        self.record_overlay_node_keys.clear();
        let record_nodes = self
            .nodes
            .keys()
            .filter(|node_id| crate::is_record_node_id(node_id))
            .cloned()
            .collect::<Vec<_>>();
        for node_id in record_nodes {
            self.refresh_record_overlay(&node_id);
        }
    }

    fn clear_flushed_record_overlay(&mut self, transaction: &StorageTransaction) {
        for (node_id, transaction_state) in &transaction.nodes {
            if !crate::is_record_node_id(node_id)
                || self.nodes.get(node_id) != Some(transaction_state)
            {
                continue;
            }
            let Some(key) = self.record_overlay_node_keys.remove(node_id) else {
                continue;
            };
            self.record_overlay.remove(&key);
        }
    }

    fn journal_node(&mut self, node: &str) {
        let Some(journal) = self.transaction_journal.as_mut() else {
            return;
        };
        if journal.nodes.contains_key(node) {
            return;
        }
        journal
            .nodes
            .insert(node.to_owned(), self.nodes.get(node).cloned());
    }

    fn journal_missing_node(&mut self, node: &str) {
        let Some(journal) = self.transaction_journal.as_mut() else {
            return;
        };
        if journal.missing_nodes.contains_key(node) {
            return;
        }
        journal
            .missing_nodes
            .insert(node.to_owned(), self.missing_nodes.contains(node));
    }

    fn journal_scheduled_node_fetch(&mut self, node: &str) {
        let Some(journal) = self.transaction_journal.as_mut() else {
            return;
        };
        if journal.scheduled_node_fetches.contains_key(node) {
            return;
        }
        journal
            .scheduled_node_fetches
            .insert(node.to_owned(), self.scheduled_node_fetches.contains(node));
    }

    fn journal_operation_queue(&mut self, queue: OperationQueue, op: &Operation) {
        let Some(journal) = self.transaction_journal.as_mut() else {
            return;
        };
        let (operations, undo) = match queue {
            OperationQueue::Pending => (&self.pending_ops, &mut journal.pending_ops),
            OperationQueue::Unflushed => (&self.unflushed_ops, &mut journal.unflushed_ops),
        };
        let key = OperationCompactionKey::from_operation(op);
        if let Some(&index) = operations.indices.get(&key) {
            undo.replaced
                .entry(index)
                .or_insert_with(|| operations.operations[index].clone());
        }
    }

    fn traverse_at(
        &mut self,
        anchor: &str,
        segments: &[String],
        spec: &TraversalSpec,
    ) -> Result<TraversalResult> {
        let (starts, mut missing) = self.resolve_traversal_starts(anchor, segments)?;
        let edge_fields = spec
            .edge_fields
            .as_ref()
            .map(|fields| fields.iter().cloned().collect::<BTreeSet<_>>());
        let limit = spec.limit.unwrap_or(usize::MAX);
        let mut frontier = std::collections::VecDeque::new();
        for start in starts {
            frontier.push_back(TraversalFrame {
                node: start.clone(),
                depth: 0,
                path: vec![start],
                via: None,
            });
        }

        let mut entries = Vec::new();
        let mut visited = BTreeSet::new();
        let mut missing_set = missing.drain(..).collect::<BTreeSet<_>>();
        let mut depth_limit_reached = false;
        let mut result_limit_reached = false;

        while let Some(frame) = match spec.strategy {
            TraversalStrategy::Bfs => frontier.pop_front(),
            TraversalStrategy::Dfs => frontier.pop_back(),
        } {
            if !visited.insert(frame.node.clone()) {
                continue;
            }

            if !self.maybe_load_node(&frame.node)? {
                missing_set.insert(frame.node);
                continue;
            }

            let should_emit = frame.depth > 0 || spec.include_start;
            if should_emit {
                let (matches, value) = self.traversal_node_match_and_value(&frame.node, spec)?;
                if matches {
                    if entries.len() >= limit {
                        result_limit_reached = true;
                        break;
                    }
                    entries.push(TraversalEntry {
                        node_id: frame.node.clone(),
                        depth: frame.depth,
                        path: frame.path.clone(),
                        via: frame.via.clone(),
                        value,
                    });
                }
            }

            let edges = self.traversal_edges(&frame.node, spec, edge_fields.as_ref())?;
            if frame.depth >= spec.max_depth {
                if !edges.is_empty() {
                    depth_limit_reached = true;
                }
                continue;
            }

            for edge in edges {
                let next = if edge.source == frame.node {
                    edge.target.clone()
                } else {
                    edge.source.clone()
                };
                if visited.contains(&next) {
                    continue;
                }
                let mut path = frame.path.clone();
                path.push(next.clone());
                frontier.push_back(TraversalFrame {
                    node: next,
                    depth: frame.depth + 1,
                    path,
                    via: Some(edge),
                });
            }
        }

        let missing = missing_set.into_iter().collect::<Vec<_>>();
        let complete = missing.is_empty() && !depth_limit_reached && !result_limit_reached;
        Ok(TraversalResult {
            entries,
            complete,
            timed_out: false,
            depth_limit_reached,
            result_limit_reached,
            fetched: 0,
            missing,
            denied: Vec::new(),
        })
    }

    fn resolve_traversal_starts(
        &mut self,
        anchor: &str,
        segments: &[String],
    ) -> Result<(Vec<NodeId>, Vec<NodeId>)> {
        match self.resolve_cursor(anchor, segments)? {
            Some(Cursor::Node(node)) => Ok((vec![node], Vec::new())),
            Some(Cursor::Field { node, field }) => {
                let Some(value) = self
                    .nodes
                    .get(&node)
                    .and_then(|node_state| node_state.fields.get(&field))
                    .map(|field_state| field_state.value.clone())
                else {
                    return Ok((Vec::new(), Vec::new()));
                };
                match value {
                    FieldValue::Link(target) => Ok((vec![target], Vec::new())),
                    FieldValue::Set(set) => Ok((set.members.keys().cloned().collect(), Vec::new())),
                    FieldValue::Scalar(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {
                        Ok((Vec::new(), Vec::new()))
                    }
                }
            }
            None => Ok((Vec::new(), vec![anchor.to_owned()])),
        }
    }

    fn traversal_edges(
        &mut self,
        node: &str,
        spec: &TraversalSpec,
        edge_fields: Option<&BTreeSet<String>>,
    ) -> Result<Vec<TraversalEdge>> {
        let _ = self.maybe_load_node(node)?;
        let mut edges = Vec::new();
        if matches!(
            spec.direction,
            TraversalDirection::Outbound | TraversalDirection::Both
        ) {
            if let Some(outbound) = self.relationship_index.outbound.get(node) {
                edges.extend(outbound.iter().cloned());
            }
        }
        if matches!(
            spec.direction,
            TraversalDirection::Inbound | TraversalDirection::Both
        ) {
            if let Some(inbound) = self.relationship_index.inbound.get(node) {
                edges.extend(inbound.iter().cloned());
            }
        }
        edges.retain(|edge| {
            edge_fields.is_none_or(|fields| fields.contains(&edge.field))
                && match edge.kind {
                    TraversalEdgeKind::Link => spec.follow_links,
                    TraversalEdgeKind::SetMember => spec.follow_sets,
                }
        });
        edges.sort_unstable();
        edges.dedup();
        Ok(edges)
    }

    fn traversal_node_match_and_value(
        &mut self,
        node: &str,
        spec: &TraversalSpec,
    ) -> Result<(bool, Option<JsonValue>)> {
        let needs_value = spec.include_values || !spec.filters.is_empty();
        if !needs_value {
            return Ok((true, None));
        }
        let Some(value) = self.materialize_node_shallow(node)? else {
            return Ok((false, None));
        };
        let entry = MapEntry {
            key: node.to_owned(),
            value: value.clone(),
        };
        let matches = spec
            .filters
            .iter()
            .all(|filter| matches_filter(&entry, filter));
        Ok((matches, spec.include_values.then_some(value)))
    }

    fn materialize_node_shallow(&mut self, node: &str) -> Result<Option<JsonValue>> {
        if !self.maybe_load_node(node)? {
            return Ok(None);
        }
        let Some(state) = self.nodes.get(node).cloned() else {
            return Ok(None);
        };
        let mut object = Map::new();
        object.insert("$id".to_owned(), JsonValue::String(state.id.clone()));
        for (field, state) in state.fields {
            object.insert(
                field.clone(),
                self.materialize_field_shallow(node, &field, &state.value),
            );
        }
        Ok(Some(JsonValue::Object(object)))
    }

    fn materialize_field_shallow(&self, node: &str, field: &str, value: &FieldValue) -> JsonValue {
        let field_path = format!("{node}/{field}");
        match value {
            FieldValue::Scalar(value) => self.materialize_scalar(&field_path, value),
            FieldValue::Bytes(bytes) => bytes_marker_value(bytes),
            FieldValue::Blob(reference) => blob_marker_value(reference),
            FieldValue::Link(target) => JsonValue::Object(Map::from_iter([(
                "$link".to_owned(),
                JsonValue::String(target.clone()),
            )])),
            FieldValue::Set(set) => JsonValue::Object(Map::from_iter([(
                "$set".to_owned(),
                JsonValue::Array(
                    set.members
                        .keys()
                        .map(|member| {
                            JsonValue::Object(Map::from_iter([(
                                "$link".to_owned(),
                                JsonValue::String(member.clone()),
                            )]))
                        })
                        .collect(),
                ),
            )])),
        }
    }

    fn reserve_node_fetches(&mut self, nodes: &[NodeId], max_fetches: usize) -> Vec<NodeId> {
        let mut reserved = Vec::new();
        for node in nodes {
            if reserved.len() >= max_fetches {
                break;
            }
            self.journal_scheduled_node_fetch(node);
            if self.scheduled_node_fetches.insert(node.clone()) {
                reserved.push(node.clone());
            }
        }
        reserved
    }

    fn release_reserved_node_fetches(&mut self, nodes: &[NodeId]) {
        for node in nodes {
            self.journal_scheduled_node_fetch(node);
            self.scheduled_node_fetches.remove(node);
        }
    }

    fn policy_for_path(&self, path: &str) -> Option<(&str, &ScopePolicy)> {
        if self.scope_policies.is_empty() {
            return None;
        }
        self.scope_policies
            .iter()
            .filter(|(scope, _)| node_matches_root(path, scope))
            .max_by_key(|(scope, _)| scope.len())
            .map(|(scope, policy)| (scope.as_str(), policy))
    }

    fn ensure_canonical_write_allowed(&self, anchor: &str, segments: &[String]) -> Result<()> {
        let path = watch_path_key(anchor, segments);
        let Some((scope, policy)) = self.policy_for_path(&path) else {
            return Ok(());
        };
        if policy.consistency == ScopeConsistency::Coordinated
            && !scope_policy_matches_authority_actor(policy, self.clock.actor())
        {
            return Err(PrimadbError::StrictScopeUnavailable {
                scope: scope.to_owned(),
            });
        }
        Ok(())
    }

    fn rebuild_relationship_index(&mut self) {
        self.relationship_index = RelationshipIndex::default();
        let node_ids = self.nodes.keys().cloned().collect::<Vec<_>>();
        for node in node_ids {
            self.reindex_node_relationships(&node);
        }
    }

    fn reindex_node_relationships(&mut self, node: &str) {
        self.journal_node(node);
        self.relationship_index.remove_source(node);
        let Some(state) = self.nodes.get(node) else {
            return;
        };
        for (field, field_state) in &state.fields {
            match &field_state.value {
                FieldValue::Link(target) => {
                    self.relationship_index.insert(TraversalEdge {
                        source: node.to_owned(),
                        field: field.clone(),
                        target: target.clone(),
                        kind: TraversalEdgeKind::Link,
                    });
                }
                FieldValue::Set(set) => {
                    for target in set.members.keys() {
                        self.relationship_index.insert(TraversalEdge {
                            source: node.to_owned(),
                            field: field.clone(),
                            target: target.clone(),
                            kind: TraversalEdgeKind::SetMember,
                        });
                    }
                }
                FieldValue::Scalar(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {}
            }
        }
    }

    fn maybe_load_node(&mut self, node: &str) -> Result<bool> {
        if let Some(existing) = self.nodes.get(node) {
            if !node_state_is_empty(existing) || self.storage_engine.is_none() {
                return Ok(true);
            }
        }
        if self.missing_nodes.contains(node) {
            return Ok(false);
        }
        let Some(engine) = self.storage_engine.clone() else {
            return Ok(false);
        };
        match engine.get_node(node)? {
            Some(node_state) => {
                self.journal_node(node);
                merge_node_state(&mut self.nodes, node_state);
                self.journal_missing_node(node);
                self.missing_nodes.remove(node);
                self.reindex_node_relationships(node);
                Ok(true)
            }
            None => {
                if self.nodes.contains_key(node) {
                    Ok(true)
                } else {
                    self.journal_missing_node(node);
                    self.missing_nodes.insert(node.to_owned());
                    Ok(false)
                }
            }
        }
    }

    fn ensure_node(&mut self, node: &str) {
        self.journal_node(node);
        self.journal_missing_node(node);
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
                        SetMember::Link(target) => ids.push(target),
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
                let signed =
                    self.sign_scalar_for_path(path, bytes_marker_value(&bytes), certificate)?;
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Scalar(signed),
                );
            }
            ParsedInput::Blob(reference) => {
                let signed =
                    self.sign_scalar_for_path(path, blob_marker_value(&reference), certificate)?;
                self.set_field(
                    node.to_owned(),
                    field.to_owned(),
                    OperationValue::Scalar(signed),
                );
            }
            ParsedInput::Link(target) => {
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
                        SetMember::Link(target) => ids.push(target),
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
            ParsedInput::Link(target) => target,
            ParsedInput::Object(object) => {
                let member_id = self.clock.next_node_id(&format!("{field}-member"));
                self.ensure_node(&member_id);
                self.write_object_to_node(&member_id, object, path)?;
                member_id
            }
            ParsedInput::Scalar(_)
            | ParsedInput::Bytes(_)
            | ParsedInput::Blob(_)
            | ParsedInput::Set(_) => {
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
            ParsedInput::Link(target) => target,
            ParsedInput::Object(object) => {
                let member_id = self.allocate_member_id_for_path(path, field);
                self.ensure_node(&member_id);
                self.write_object_to_node_secure(&member_id, object, &member_id, certificate)?;
                member_id
            }
            ParsedInput::Scalar(_)
            | ParsedInput::Bytes(_)
            | ParsedInput::Blob(_)
            | ParsedInput::Set(_) => {
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
        if origin == OperationOrigin::Remote && !self.remote_operation_allowed(&op) {
            return false;
        }
        self.clock.observe(&op.revision);
        let marker = VersionMarker {
            revision: op.revision.clone(),
            op_id: op.op_id.clone(),
        };

        let accepted = match &op.action {
            OperationAction::SetField { node, field, value } => {
                self.journal_node(node);
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
                self.journal_node(node);
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
                self.journal_node(node);
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
                self.journal_node(node);
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
            let source = operation_source_node(&op).to_owned();
            self.reindex_node_relationships(&source);
            self.refresh_record_overlay(&source);
            self.journal_operation_queue(OperationQueue::Unflushed, &op);
            self.unflushed_ops.push(op.clone());
        }

        if accepted && origin == OperationOrigin::Local {
            self.journal_operation_queue(OperationQueue::Pending, &op);
            self.pending_ops.push(op);
        }

        accepted
    }

    fn remote_operation_allowed(&self, op: &Operation) -> bool {
        let path = operation_touched_path(op);
        let Some((_, policy)) = self.policy_for_path(&path) else {
            return true;
        };
        if policy.consistency != ScopeConsistency::Coordinated {
            return true;
        }
        scope_policy_matches_authority_actor(policy, &op.author)
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

    fn query_candidates(&mut self, cursor: Cursor) -> Vec<QueryCandidate> {
        match cursor {
            Cursor::Node(node) => {
                let _ = self.maybe_load_node(&node);
                self.nodes
                    .get(&node)
                    .map(|state| {
                        state
                            .fields
                            .keys()
                            .map(|field| QueryCandidate {
                                key: field.clone(),
                                source: QueryCandidateSource::Field {
                                    node: node.clone(),
                                    field: field.clone(),
                                },
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            }
            Cursor::Field { node, field } => {
                let _ = self.maybe_load_node(&node);
                let Some(value) = self
                    .nodes
                    .get(&node)
                    .and_then(|state| state.fields.get(&field))
                    .map(|state| state.value.clone())
                else {
                    return Vec::new();
                };
                match value {
                    FieldValue::Link(target) => self.query_candidates(Cursor::Node(target)),
                    FieldValue::Set(set) => set
                        .members
                        .keys()
                        .cloned()
                        .map(|node| QueryCandidate {
                            key: node.clone(),
                            source: QueryCandidateSource::Node(node),
                        })
                        .collect(),
                    FieldValue::Scalar(_) | FieldValue::Bytes(_) | FieldValue::Blob(_) => {
                        Vec::new()
                    }
                }
            }
        }
    }

    fn evaluate_query_candidates(
        &mut self,
        candidates: Vec<QueryCandidate>,
        spec: &QuerySpec,
    ) -> Vec<MapEntry> {
        let filter_paths = spec
            .filters
            .iter()
            .map(|filter| QueryValuePath::new(filter_path(filter)))
            .collect::<Vec<_>>();
        let order_path = spec
            .order
            .as_ref()
            .map(|order| QueryValuePath::new(&order.path));
        let mut evaluated = candidates
            .into_iter()
            .filter_map(|candidate| {
                self.evaluate_query_candidate(candidate, spec, &filter_paths, order_path.as_ref())
            })
            .collect::<Vec<_>>();
        if let Some(order) = &spec.order {
            evaluated.sort_by(|left, right| compare_evaluated_candidates(left, right, order));
        }
        let offset = spec.offset.min(evaluated.len());
        let end = spec
            .limit
            .map(|limit| offset.saturating_add(limit).min(evaluated.len()))
            .unwrap_or(evaluated.len());
        evaluated
            .into_iter()
            .skip(offset)
            .take(end.saturating_sub(offset))
            .map(|mut evaluated| {
                let key = evaluated.candidate.key.clone();
                let value = evaluated
                    .full_value
                    .take()
                    .unwrap_or_else(|| self.materialize_query_candidate(&evaluated.candidate));
                MapEntry { key, value }
            })
            .collect()
    }

    fn evaluate_query_candidate(
        &mut self,
        candidate: QueryCandidate,
        spec: &QuerySpec,
        filter_paths: &[QueryValuePath<'_>],
        order_path: Option<&QueryValuePath<'_>>,
    ) -> Option<EvaluatedQueryCandidate> {
        let mut evaluated = EvaluatedQueryCandidate {
            candidate,
            full_value: None,
            order_value: None,
        };
        for (filter, path) in spec.filters.iter().zip(filter_paths) {
            match self.query_candidate_value(&evaluated.candidate, path) {
                Ok(value) if matches_filter_value(value.as_ref(), filter) => {}
                Ok(_) => return None,
                Err(()) => {
                    if evaluated.full_value.is_none() {
                        evaluated.full_value =
                            Some(self.materialize_query_candidate(&evaluated.candidate));
                    }
                    if !matches_filter_parts(
                        &evaluated.candidate.key,
                        evaluated.full_value.as_ref().unwrap(),
                        filter,
                    ) {
                        return None;
                    }
                }
            }
        }
        if let Some(order) = &spec.order {
            match self.query_candidate_value(&evaluated.candidate, order_path.unwrap()) {
                Ok(value) => evaluated.order_value = value,
                Err(()) => {
                    if evaluated.full_value.is_none() {
                        evaluated.full_value =
                            Some(self.materialize_query_candidate(&evaluated.candidate));
                    }
                    evaluated.order_value = query_value_parts(
                        &evaluated.candidate.key,
                        evaluated.full_value.as_ref().unwrap(),
                        &order.path,
                    );
                }
            }
        }
        Some(evaluated)
    }

    fn materialize_query_candidate(&mut self, candidate: &QueryCandidate) -> JsonValue {
        #[cfg(test)]
        {
            self.query_candidate_projections = self.query_candidate_projections.saturating_add(1);
        }
        match &candidate.source {
            QueryCandidateSource::Node(node) => {
                self.materialize_node(node, node, &mut BTreeSet::new())
            }
            QueryCandidateSource::Field { node, field } => {
                let Some(value) = self
                    .nodes
                    .get(node)
                    .and_then(|state| state.fields.get(field))
                    .map(|state| state.value.clone())
                else {
                    return JsonValue::Null;
                };
                self.materialize_field(node, field, &value, &mut BTreeSet::new())
            }
        }
    }

    fn query_candidate_value(
        &mut self,
        candidate: &QueryCandidate,
        path: &QueryValuePath<'_>,
    ) -> std::result::Result<Option<JsonValue>, ()> {
        let segments = match path {
            QueryValuePath::Full => return Err(()),
            QueryValuePath::Key => return Ok(Some(JsonValue::String(candidate.key.clone()))),
            QueryValuePath::Segments(segments) => segments,
        };
        let mut visited = BTreeSet::new();
        match &candidate.source {
            QueryCandidateSource::Node(node) => self.query_node_value(node, segments, &mut visited),
            QueryCandidateSource::Field { node, field } => {
                self.query_field_value(node, field, segments, &mut visited)
            }
        }
    }

    fn query_node_value(
        &mut self,
        node: &str,
        segments: &[&str],
        visited: &mut BTreeSet<NodeId>,
    ) -> std::result::Result<Option<JsonValue>, ()> {
        if !visited.insert(node.to_owned()) {
            return Err(());
        }
        if !self.maybe_load_node(node).unwrap_or(false) {
            visited.remove(node);
            return Err(());
        }
        let result = if let Some((first, rest)) = segments.split_first() {
            if *first == "$id" {
                if rest.is_empty() {
                    Ok(Some(JsonValue::String(node.to_owned())))
                } else {
                    Ok(None)
                }
            } else {
                self.query_field_value(node, first, rest, visited)
            }
        } else {
            Err(())
        };
        visited.remove(node);
        result
    }

    fn query_field_value(
        &mut self,
        node: &str,
        field: &str,
        segments: &[&str],
        visited: &mut BTreeSet<NodeId>,
    ) -> std::result::Result<Option<JsonValue>, ()> {
        let _ = self.maybe_load_node(node);
        let Some(value) = self
            .nodes
            .get(node)
            .and_then(|state| state.fields.get(field))
            .map(|state| &state.value)
        else {
            return Ok(None);
        };
        match value {
            FieldValue::Scalar(value) => self.query_scalar_value(node, field, value, segments),
            FieldValue::Link(_) if segments.is_empty() => Err(()),
            FieldValue::Link(target) => {
                let target = target.clone();
                self.query_node_value(&target, segments, visited)
            }
            FieldValue::Bytes(_) | FieldValue::Blob(_) | FieldValue::Set(_) => Err(()),
        }
    }

    fn query_scalar_value(
        &self,
        node: &str,
        field: &str,
        value: &JsonValue,
        segments: &[&str],
    ) -> std::result::Result<Option<JsonValue>, ()> {
        #[cfg(feature = "crypto")]
        {
            let value = self.materialize_scalar(&format!("{node}/{field}"), value);
            query_scalar_path(&value, segments)
        }

        #[cfg(not(feature = "crypto"))]
        {
            let _ = (node, field);
            query_scalar_path(value, segments)
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

fn watch_path_key(anchor: &str, segments: &[String]) -> String {
    if segments.is_empty() {
        anchor.to_owned()
    } else {
        format!("{anchor}/{}", segments.join("/"))
    }
}

fn path_overlaps(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|suffix| suffix.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn watch_change_overlaps(path: &str, event: &ChangeEvent) -> bool {
    event.full_refresh
        || event
            .touched_paths
            .iter()
            .any(|changed| path_overlaps(path, changed))
}

fn traversal_dependency_paths(
    anchor: &str,
    segments: &[String],
    result: &TraversalResult,
) -> BTreeSet<String> {
    let mut paths = BTreeSet::from([watch_path_key(anchor, segments)]);
    for entry in &result.entries {
        paths.insert(entry.node_id.clone());
        if let Some(edge) = &entry.via {
            paths.insert(edge.source.clone());
            paths.insert(format!("{}/{}", edge.source, edge.field));
            paths.insert(edge.target.clone());
        }
    }
    paths.extend(result.missing.iter().cloned());
    paths.extend(result.denied.iter().cloned());
    paths
}

fn traversal_watch_change_overlaps(watcher: &TraversalWatcher, event: &ChangeEvent) -> bool {
    event.full_refresh
        || watcher
            .dependency_paths
            .iter()
            .any(|path| watch_change_overlaps(path, event))
}

fn record_watch_change_overlaps(scan: &RecordScan, event: &ChangeEvent) -> bool {
    if event.full_refresh {
        return true;
    }
    if !event.records_changed {
        return false;
    }
    if event.touched_record_keys.is_empty() {
        return true;
    }
    event
        .touched_record_keys
        .iter()
        .any(|key| scan.matches_key(key))
}

fn operation_touched_path(op: &Operation) -> String {
    match &op.action {
        OperationAction::SetField { node, field, .. }
        | OperationAction::AddSetMember { node, field, .. }
        | OperationAction::RemoveSetMember { node, field, .. }
        | OperationAction::DeleteField { node, field } => format!("{node}/{field}"),
    }
}

fn operation_is_record_op(op: &Operation) -> bool {
    match &op.action {
        OperationAction::SetField { node, .. }
        | OperationAction::AddSetMember { node, .. }
        | OperationAction::RemoveSetMember { node, .. }
        | OperationAction::DeleteField { node, .. } => crate::is_record_node_id(node),
    }
}

fn operation_touched_record_key(
    op: &Operation,
    nodes: &BTreeMap<NodeId, NodeState>,
) -> Option<String> {
    match &op.action {
        OperationAction::SetField {
            node,
            field,
            value: OperationValue::Scalar(JsonValue::String(key)),
        } if crate::is_record_node_id(node) && field == "key" => Some(key.clone()),
        _ => {
            let node = operation_source_node(op);
            crate::is_record_node_id(node)
                .then(|| nodes.get(node).and_then(crate::record_key_from_node_state))
                .flatten()
        }
    }
}

fn watch_key<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or_default()
}

fn send_watch_update<T>(sender: &Sender<T>, value: T) -> bool {
    // Slow local consumers keep the newest state; force_send bounds the queue
    // while preserving closed-channel detection for stale watcher cleanup.
    sender.force_send(value).is_ok()
}

fn operation_source_node(op: &Operation) -> &str {
    match &op.action {
        OperationAction::SetField { node, .. }
        | OperationAction::AddSetMember { node, .. }
        | OperationAction::RemoveSetMember { node, .. }
        | OperationAction::DeleteField { node, .. } => node,
    }
}

fn referenced_blob_ids(nodes: &BTreeMap<NodeId, NodeState>) -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for node in nodes.values() {
        for field in node.fields.values() {
            if let FieldValue::Blob(reference) = &field.value {
                ids.insert(reference.id.clone());
            }
        }
    }
    ids
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
        JsonValue::String(encoded) => BinaryBytes::from_base64(encoded).map(Some).map_err(|_| {
            PrimadbError::InvalidBinaryMarker {
                path: path.to_owned(),
            }
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
    inner.scope_policies.extend(snapshot.scope_policies);
    inner.missing_nodes.clear();
    inner.scheduled_node_fetches.clear();
    inner.rebuild_relationship_index();
    inner.rebuild_record_overlay();
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

fn node_state_is_empty(node: &NodeState) -> bool {
    node.fields.is_empty() && node.tombstones.is_empty()
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
        RemoteResult::Records { result } => build_record_chunk_responses(
            request_id,
            result,
            limits.max_query_entries_per_chunk.max(1),
        ),
        RemoteResult::VectorSearch { result } => vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::VectorSearch { result },
        }],
        RemoteResult::TextSearch { result } => vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::TextSearch { result },
        }],
        RemoteResult::Node { node } => vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::Node { node },
        }],
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
                        scope_policies: (index == 0)
                            .then_some(snapshot.scope_policies.clone())
                            .unwrap_or_default(),
                    },
                })
                .collect()
        }
        RemoteResult::Transaction { report } => vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::Transaction { report },
        }],
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

fn build_record_chunk_responses(
    request_id: &str,
    result: RecordScanResult,
    chunk_size: usize,
) -> Vec<PullResponse> {
    let next_cursor = result.next_cursor;
    let chunks = chunk_vec(result.entries, chunk_size);
    if chunks.is_empty() {
        return vec![PullResponse {
            request_id: request_id.to_owned(),
            chunk: PullChunk { index: 0, total: 1 },
            done: true,
            result: PullResponseBody::Records {
                entries: Vec::new(),
                next_cursor,
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
            result: PullResponseBody::Records {
                entries,
                next_cursor: (index + 1 == total)
                    .then_some(next_cursor.clone())
                    .flatten(),
            },
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
    matches_filter_parts(&entry.key, &entry.value, filter)
}

fn matches_filter_parts(key: &str, value: &JsonValue, filter: &QueryFilter) -> bool {
    let value = query_value_parts(key, value, filter_path(filter));
    matches_filter_value(value.as_ref(), filter)
}

fn filter_path(filter: &QueryFilter) -> &str {
    match filter {
        QueryFilter::Eq { path, .. }
        | QueryFilter::Ne { path, .. }
        | QueryFilter::Gt { path, .. }
        | QueryFilter::Gte { path, .. }
        | QueryFilter::Lt { path, .. }
        | QueryFilter::Lte { path, .. }
        | QueryFilter::Prefix { path, .. }
        | QueryFilter::Contains { path, .. }
        | QueryFilter::Exists { path } => path,
    }
}

fn matches_filter_value(value: Option<&JsonValue>, filter: &QueryFilter) -> bool {
    match filter {
        QueryFilter::Eq {
            value: expected, ..
        } => value.is_some_and(|candidate| candidate == expected),
        QueryFilter::Ne {
            value: expected, ..
        } => value.is_some_and(|candidate| candidate != expected),
        QueryFilter::Gt {
            value: expected, ..
        } => value
            .and_then(|candidate| compare_json_values(candidate, expected))
            .is_some_and(|ordering| ordering == Ordering::Greater),
        QueryFilter::Gte {
            value: expected, ..
        } => value
            .and_then(|candidate| compare_json_values(candidate, expected))
            .is_some_and(|ordering| matches!(ordering, Ordering::Greater | Ordering::Equal)),
        QueryFilter::Lt {
            value: expected, ..
        } => value
            .and_then(|candidate| compare_json_values(candidate, expected))
            .is_some_and(|ordering| ordering == Ordering::Less),
        QueryFilter::Lte {
            value: expected, ..
        } => value
            .and_then(|candidate| compare_json_values(candidate, expected))
            .is_some_and(|ordering| matches!(ordering, Ordering::Less | Ordering::Equal)),
        QueryFilter::Prefix {
            value: expected, ..
        } => value
            .and_then(JsonValue::as_str)
            .is_some_and(|candidate| candidate.starts_with(expected)),
        QueryFilter::Contains {
            value: expected, ..
        } => value
            .and_then(JsonValue::as_str)
            .is_some_and(|candidate| candidate.contains(expected)),
        QueryFilter::Exists { .. } => value.is_some(),
    }
}

fn compare_evaluated_candidates(
    left: &EvaluatedQueryCandidate,
    right: &EvaluatedQueryCandidate,
    order: &crate::query::QueryOrder,
) -> Ordering {
    let base = match (&left.order_value, &right.order_value) {
        (Some(left), Some(right)) => compare_json_values(left, right).unwrap_or(Ordering::Equal),
        (Some(_), None) => Ordering::Greater,
        (None, Some(_)) => Ordering::Less,
        (None, None) => left.candidate.key.cmp(&right.candidate.key),
    };
    match order.direction {
        QueryDirection::Asc => base.then_with(|| left.candidate.key.cmp(&right.candidate.key)),
        QueryDirection::Desc => base
            .reverse()
            .then_with(|| left.candidate.key.cmp(&right.candidate.key)),
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
    !path.is_empty() && path != "$key" && path != "$value"
}

fn build_index_filter_groups<'a>(spec: &'a QuerySpec) -> BTreeMap<String, Vec<&'a QueryFilter>> {
    let mut groups = BTreeMap::new();
    for filter in &spec.filters {
        if let Some(path) = indexed_filter_path(filter) {
            groups.entry(path).or_insert_with(Vec::new).push(filter);
        }
    }
    groups
}

fn build_direct_index_scan(filters: &[&QueryFilter], limit: Option<usize>) -> DirectIndexScan {
    let mut scan = DirectIndexScan {
        limit,
        ..DirectIndexScan::default()
    };
    for filter in filters {
        match filter {
            QueryFilter::Eq { value, .. } => {
                if let Some(key) = crate::engine::sortable_scalar_key(value) {
                    scan.exact_sortable_key = Some(key);
                }
            }
            QueryFilter::Gt { value, .. } => {
                if let Some(key) = crate::engine::sortable_scalar_key(value) {
                    scan.start_after = Some(match scan.start_after.take() {
                        Some(current) => current.max(key),
                        None => key,
                    });
                }
            }
            QueryFilter::Gte { value, .. } => {
                if let Some(key) = crate::engine::sortable_scalar_key(value) {
                    scan.start_at = Some(match scan.start_at.take() {
                        Some(current) => current.max(key),
                        None => key,
                    });
                }
            }
            QueryFilter::Lt { value, .. } => {
                if let Some(key) = crate::engine::sortable_scalar_key(value) {
                    scan.end_before = Some(match scan.end_before.take() {
                        Some(current) => current.min(key),
                        None => key,
                    });
                }
            }
            QueryFilter::Lte { value, .. } => {
                if let Some(key) = crate::engine::sortable_scalar_key(value) {
                    scan.end_at = Some(match scan.end_at.take() {
                        Some(current) => current.min(key),
                        None => key,
                    });
                }
            }
            QueryFilter::Prefix { value, .. } => {
                scan.prefix_sortable_key = Some(format!(
                    "s_{}",
                    crate::engine::direct_index_encode_prefix(value)
                ));
            }
            QueryFilter::Ne { .. } | QueryFilter::Contains { .. } | QueryFilter::Exists { .. } => {}
        }
    }
    scan
}

fn can_early_stop_index_query(spec: &QuerySpec) -> bool {
    spec.order.as_ref().and_then(indexed_order_path).is_some()
        && spec
            .filters
            .iter()
            .all(|filter| indexed_filter_path(filter).is_some())
}

fn filter_matches_index_entry(filter: &QueryFilter, value: &JsonValue) -> bool {
    match filter {
        QueryFilter::Eq {
            value: expected, ..
        } => value == expected,
        QueryFilter::Ne {
            value: expected, ..
        } => value != expected,
        QueryFilter::Gt {
            value: expected, ..
        } => compare_json_values(value, expected) == Some(Ordering::Greater),
        QueryFilter::Gte {
            value: expected, ..
        } => matches!(
            compare_json_values(value, expected),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        QueryFilter::Lt {
            value: expected, ..
        } => compare_json_values(value, expected) == Some(Ordering::Less),
        QueryFilter::Lte {
            value: expected, ..
        } => matches!(
            compare_json_values(value, expected),
            Some(Ordering::Less | Ordering::Equal)
        ),
        QueryFilter::Prefix {
            value: expected, ..
        } => value
            .as_str()
            .map(|candidate| candidate.starts_with(expected))
            .unwrap_or(false),
        QueryFilter::Contains {
            value: expected, ..
        } => value
            .as_str()
            .map(|candidate| candidate.contains(expected))
            .unwrap_or(false),
        QueryFilter::Exists { .. } => true,
    }
}

fn query_value_parts(key: &str, value: &JsonValue, path: &str) -> Option<JsonValue> {
    match path {
        "" | "$value" => Some(value.clone()),
        "$key" => Some(JsonValue::String(key.to_owned())),
        _ => {
            let mut current = value;
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

fn query_scalar_path(
    value: &JsonValue,
    segments: &[&str],
) -> std::result::Result<Option<JsonValue>, ()> {
    let mut current = value;
    for segment in segments {
        current = match current {
            JsonValue::Object(object) => match object.get(*segment) {
                Some(value) => value,
                None => return Ok(None),
            },
            _ => return Ok(None),
        };
    }
    Ok(Some(current.clone()))
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
    use super::{
        CompactedOperations, LOCAL_WATCH_QUEUE_CAPACITY, NodeFetchScheduler, Primadb,
        build_pull_responses,
    };
    use crate::{
        ConnectHookContext, HookDecision, HookTransport, NetworkHooks, NodeState, PeerPresence,
        PrimadbError, PrimadbLimits, PullRequest, PullRequestKind, PullResponseBody,
        QueryDirection, QueryFilter, QuerySpec, RecordBatch, RecordEntry, RecordMutation,
        RecordPrecondition, RecordScan, RecordScanResult, RecordValue, RemotePath, Result,
        Revision, RoomHookContext, ScopeAuthority, ScopeConsistency, ScopeOfflineWrites,
        ScopePolicy, ServeRequestContext, ServeResultContext, TextCandidatePolicy,
        TextCollectionConfig, TextDocument, TextScoreScope, TextSearchSource, TextSearchSpec,
        TransactionOptions, TransactionStatus, TransactionStep, TraversalDirection, TraversalSpec,
        VectorCollectionConfig, VectorFilter, VectorMetric, VectorSearchSpec,
    };
    use crate::{Operation, OperationAction, OperationValue};
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn vector_search_spec() -> VectorSearchSpec {
        VectorSearchSpec {
            limit: 10,
            ef: None,
            filter: None::<VectorFilter>,
            include_vector: false,
            include_metadata: false,
            exact: true,
            stale_policy: Default::default(),
        }
    }

    fn test_operation(id: impl Into<String>, millis: u64, action: OperationAction) -> Operation {
        Operation {
            op_id: id.into(),
            author: "test".to_owned(),
            revision: Revision {
                millis,
                counter: 0,
                actor: "test".to_owned(),
            },
            action,
        }
    }

    #[test]
    fn compacted_operations_preserve_first_key_order_and_revision_semantics() {
        let field_a = || OperationAction::SetField {
            node: "node".to_owned(),
            field: "a".to_owned(),
            value: OperationValue::Scalar(json!(true)),
        };
        let set_member = || OperationAction::AddSetMember {
            node: "node".to_owned(),
            field: "members".to_owned(),
            member: "alice".to_owned(),
        };
        let field_b = || OperationAction::SetField {
            node: "node".to_owned(),
            field: "b".to_owned(),
            value: OperationValue::Scalar(json!(true)),
        };

        let mut queue = CompactedOperations::default();
        queue.push(test_operation("field-a-first", 2, field_a()));
        queue.push(test_operation("member-first", 2, set_member()));
        queue.push(test_operation("field-b", 2, field_b()));
        queue.push(test_operation("field-a-older", 1, field_a()));
        queue.push(test_operation(
            "field-a-latest",
            3,
            OperationAction::DeleteField {
                node: "node".to_owned(),
                field: "a".to_owned(),
            },
        ));
        queue.push(test_operation(
            "member-latest",
            3,
            OperationAction::RemoveSetMember {
                node: "node".to_owned(),
                field: "members".to_owned(),
                member: "alice".to_owned(),
            },
        ));

        let ids: Vec<_> = queue
            .as_slice()
            .iter()
            .map(|operation| operation.op_id.as_str())
            .collect();
        assert_eq!(ids, ["field-a-latest", "member-latest", "field-b"]);
        assert_eq!(queue.indices.len(), queue.len());
    }

    #[test]
    fn compacted_operation_keys_are_typed_and_cache_survives_restoration_and_drain() {
        let operations = [
            test_operation(
                "delimited-node",
                1,
                OperationAction::SetField {
                    node: "a\0b".to_owned(),
                    field: "c".to_owned(),
                    value: OperationValue::Scalar(json!(1)),
                },
            ),
            test_operation(
                "delimited-field",
                1,
                OperationAction::SetField {
                    node: "a".to_owned(),
                    field: "b\0c".to_owned(),
                    value: OperationValue::Scalar(json!(2)),
                },
            ),
        ];
        let mut queue = CompactedOperations::from_operations(operations);
        assert_eq!(queue.len(), 2);

        queue.drain_prefix(1);
        queue.push(test_operation(
            "delimited-field-newer",
            2,
            OperationAction::DeleteField {
                node: "a".to_owned(),
                field: "b\0c".to_owned(),
            },
        ));
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.as_slice()[0].op_id, "delimited-field-newer");
        assert_eq!(queue.indices.len(), 1);
    }

    #[test]
    fn compacted_operations_handle_large_keyed_batches() {
        const OPERATION_COUNT: usize = 20_000;
        let mut queue = CompactedOperations::default();
        for index in 0..OPERATION_COUNT {
            queue.push(test_operation(
                format!("first-{index}"),
                1,
                OperationAction::SetField {
                    node: "bulk".to_owned(),
                    field: format!("field-{index}"),
                    value: OperationValue::Scalar(json!(index)),
                },
            ));
        }
        for index in 0..OPERATION_COUNT {
            queue.push(test_operation(
                format!("latest-{index}"),
                2,
                OperationAction::DeleteField {
                    node: "bulk".to_owned(),
                    field: format!("field-{index}"),
                },
            ));
        }

        assert_eq!(queue.len(), OPERATION_COUNT);
        assert_eq!(queue.indices.len(), OPERATION_COUNT);
        assert_eq!(queue.as_slice()[0].op_id, "latest-0");
        assert_eq!(
            queue.as_slice()[OPERATION_COUNT - 1].op_id,
            format!("latest-{}", OPERATION_COUNT - 1)
        );
    }

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
    fn query_preserves_nested_linked_projection_order_and_pagination() -> Result<()> {
        let db = Primadb::with_replica_id("query-projection-semantics");
        for (name, rank, active) in [("alpha", 3, true), ("beta", 1, false), ("gamma", 2, true)] {
            db.root("lists").field("items").set(json!({
                "name": name,
                "profile": { "rank": rank, "active": active },
                "payload": { "nested": { "value": name } },
            }))?;
        }

        let entries = db.root("lists").field("items").query(QuerySpec {
            filters: vec![QueryFilter::Eq {
                path: "profile.active".to_owned(),
                value: json!(true),
            }],
            order: Some(crate::query::QueryOrder {
                path: "profile.rank".to_owned(),
                direction: QueryDirection::Desc,
            }),
            offset: 1,
            limit: Some(1),
        })?;

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value["name"], "gamma");
        assert_eq!(entries[0].value["profile"]["rank"], 2);
        assert_eq!(entries[0].value["payload"]["nested"]["value"], "gamma");
        assert!(entries[0].value["$id"].is_string());
        Ok(())
    }

    #[test]
    fn query_preserves_key_value_and_cycle_semantics() -> Result<()> {
        let db = Primadb::with_replica_id("query-projection-cycles");
        db.root("docs").field("first").put(json!({"rank": 2}))?;
        db.root("docs").field("second").put(json!({"rank": 1}))?;
        let first_id = db.root("docs").field("first").once_json()?.unwrap()["$id"]
            .as_str()
            .unwrap()
            .to_owned();
        let second_id = db.root("docs").field("second").once_json()?.unwrap()["$id"]
            .as_str()
            .unwrap()
            .to_owned();
        db.root(&first_id)
            .field("peer")
            .put(json!({"$link": second_id}))?;
        db.root(&second_id)
            .field("peer")
            .put(json!({"$link": first_id}))?;

        let by_key = db.root("docs").query(QuerySpec {
            filters: vec![QueryFilter::Eq {
                path: "$key".to_owned(),
                value: json!("first"),
            }],
            ..QuerySpec::default()
        })?;
        assert_eq!(by_key.len(), 1);
        assert_eq!(by_key[0].key, "first");
        assert_eq!(by_key[0].value["peer"]["peer"]["$ref"], first_id);

        let full_value = by_key[0].value.clone();
        let by_value = db.root("docs").query(QuerySpec {
            filters: vec![QueryFilter::Eq {
                path: "$value".to_owned(),
                value: full_value,
            }],
            ..QuerySpec::default()
        })?;
        assert_eq!(by_value.len(), 1);
        assert_eq!(by_value[0], by_key[0]);
        Ok(())
    }

    #[test]
    fn rejected_query_candidates_do_not_materialize_unrelated_linked_graphs() -> Result<()> {
        let db = Primadb::with_replica_id("query-projection-performance");
        for index in 0..128 {
            let child = format!("payload/{index}");
            db.root(&child).field("blob").put("x".repeat(16 * 1024))?;
            db.root("items").field("members").set(json!({
                "accepted": index == 127,
                "rank": index,
                "payload": { "$link": child },
            }))?;
        }
        db.inner.lock().unwrap().query_candidate_projections = 0;

        let entries = db.root("items").field("members").query(QuerySpec {
            filters: vec![QueryFilter::Eq {
                path: "accepted".to_owned(),
                value: json!(true),
            }],
            order: Some(crate::query::QueryOrder {
                path: "rank".to_owned(),
                direction: QueryDirection::Asc,
            }),
            limit: Some(1),
            offset: 0,
        })?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value["rank"], 127);
        assert_eq!(
            entries[0].value["payload"]["blob"].as_str().unwrap().len(),
            16 * 1024
        );
        assert_eq!(db.inner.lock().unwrap().query_candidate_projections, 1);
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
    fn vectors_are_stored_as_split_records_and_search_exactly() -> Result<()> {
        let db = Primadb::with_replica_id("vector-a");
        db.create_vector_collection(
            "docs",
            VectorCollectionConfig {
                dim: 3,
                metric: VectorMetric::L2,
                backend: None,
                hnsw: None,
                chunking: crate::VectorChunkingConfig { chunk_bytes: 8 },
            },
        )?;
        db.put_vector(
            "docs",
            "alpha",
            vec![0.0, 0.0, 0.0],
            Some(json!({"kind": "note", "title": "alpha"})),
        )?;
        db.put_vector(
            "docs",
            "beta",
            vec![10.0, 0.0, 0.0],
            Some(json!({"kind": "note", "title": "beta"})),
        )?;

        let records = db.scan_records(RecordScan {
            prefix: Some("__primadb_vectors/".to_owned()),
            ..RecordScan::default()
        })?;
        assert!(
            records
                .entries
                .iter()
                .any(|entry| entry.key.ends_with("/items/616c706861/meta"))
        );
        assert!(
            records
                .entries
                .iter()
                .any(|entry| entry.key.ends_with("/items/616c706861/chunks/0"))
        );
        assert!(
            records
                .entries
                .iter()
                .any(|entry| entry.key.ends_with("/items/616c706861/chunks/1"))
        );

        let result = db.search_vectors(
            "docs",
            vec![1.0, 0.0, 0.0],
            VectorSearchSpec {
                limit: 2,
                include_metadata: true,
                ..vector_search_spec()
            },
        )?;
        assert_eq!(result.matches.len(), 2);
        assert_eq!(result.matches[0].id, "alpha");
        assert_eq!(result.matches[1].id, "beta");
        assert_eq!(
            result.matches[0].metadata.as_ref().unwrap()["title"],
            "alpha"
        );
        Ok(())
    }

    #[test]
    fn vector_search_ignores_incomplete_split_items() -> Result<()> {
        let db = Primadb::with_replica_id("vector-b");
        db.create_vector_collection(
            "docs",
            VectorCollectionConfig {
                dim: 3,
                metric: VectorMetric::L2,
                backend: None,
                hnsw: None,
                chunking: crate::VectorChunkingConfig { chunk_bytes: 8 },
            },
        )?;
        db.put_vector("docs", "complete", vec![0.0, 0.0, 0.0], None)?;
        db.put_record_value(
            "__primadb_vectors/646f6373/items/696e636f6d706c657465/meta",
            crate::VectorItemMeta {
                id: "incomplete".to_owned(),
                write_id: "partial".to_owned(),
                dim: 3,
                encoding: "f32_le".to_owned(),
                byte_length: 12,
                checksum: "blake3:missing".to_owned(),
                chunk_count: 2,
                metadata: Some(json!({"bad": true})),
                deleted: false,
                updated_at: None,
            },
        )?;

        let result = db.search_vectors("docs", vec![0.0, 0.0, 0.0], vector_search_spec())?;
        assert_eq!(result.matches.len(), 1);
        assert_eq!(result.matches[0].id, "complete");
        assert_eq!(db.vector_index_stats("docs")?.incomplete_count, 1);
        Ok(())
    }

    #[test]
    fn vector_watches_emit_on_result_change() -> Result<()> {
        let db = Primadb::with_replica_id("vector-watch");
        db.create_vector_collection(
            "docs",
            VectorCollectionConfig {
                dim: 2,
                metric: VectorMetric::Cosine,
                backend: None,
                hnsw: None,
                chunking: Default::default(),
            },
        )?;
        db.put_vector("docs", "left", vec![1.0, 0.0], None)?;
        let watch = db.watch_vector_search("docs", vec![1.0, 0.0], vector_search_spec())?;
        let initial = watch.recv_blocking().unwrap();
        assert_eq!(initial.matches[0].id, "left");

        db.put_vector("docs", "right", vec![0.99, 0.0], None)?;
        let updated = watch.recv_blocking().unwrap();
        assert_eq!(updated.matches.len(), 2);
        assert_eq!(updated.matches[0].id, "left");
        Ok(())
    }

    #[test]
    fn text_documents_are_stored_as_records_and_search_ranked() -> Result<()> {
        let db = Primadb::with_replica_id("text-a");
        db.create_text_collection("notes", TextCollectionConfig::default())?;
        db.put_text_document(
            "notes",
            TextDocument {
                id: "alpha".to_owned(),
                fields: BTreeMap::from([
                    ("title".to_owned(), "secure mesh routing".to_owned()),
                    ("body".to_owned(), "trust routing proposal".to_owned()),
                ]),
                metadata: BTreeMap::from([("kind".to_owned(), json!("note"))]),
            },
        )?;
        db.put_text_document(
            "notes",
            TextDocument {
                id: "beta".to_owned(),
                fields: BTreeMap::from([("body".to_owned(), "unrelated local note".to_owned())]),
                metadata: BTreeMap::new(),
            },
        )?;

        let records = db.scan_records(RecordScan {
            prefix: Some("__primadb_text/".to_owned()),
            ..RecordScan::default()
        })?;
        assert!(
            records
                .entries
                .iter()
                .any(|entry| entry.key.ends_with("/docs/616c706861"))
        );

        let result = db.text_search(
            "notes",
            "secure routing",
            TextSearchSpec {
                include_metadata: true,
                ..Default::default()
            },
        )?;
        assert_eq!(
            result.matches.first().map(|item| item.id.as_str()),
            Some("alpha")
        );
        assert_eq!(result.score_scope, TextScoreScope::Collection);
        assert_eq!(
            result.matches[0].metadata.as_ref().unwrap()["kind"],
            json!("note")
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn text_collection_writes_native_cache_under_segment_storage() -> Result<()> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("primadb-text-cache-{}-{nonce}", std::process::id()));

        {
            let db = Primadb::with_replica_id("text-cache-a");
            db.use_segment_storage(&directory, 8)?;
            db.create_text_collection("notes", TextCollectionConfig::default())?;
            db.put_text_document(
                "notes",
                TextDocument {
                    id: "alpha".to_owned(),
                    fields: BTreeMap::from([(
                        "body".to_owned(),
                        "persistent text cache".to_owned(),
                    )]),
                    metadata: BTreeMap::new(),
                },
            )?;
            let result = db.text_search("notes", "persistent", TextSearchSpec::default())?;
            assert_eq!(
                result.matches.first().map(|item| item.id.as_str()),
                Some("alpha")
            );
            assert!(
                directory
                    .join("text-cache")
                    .join(crate::encode_component("notes"))
                    .join("manifest.json")
                    .exists()
            );
        }

        let cache_dir = directory
            .join("text-cache")
            .join(crate::encode_component("notes"));
        let docs_path = cache_dir.join("docs.bin");
        let mut documents: BTreeMap<String, TextDocument> =
            serde_json::from_slice(&std::fs::read(&docs_path)?)?;
        documents
            .get_mut("alpha")
            .expect("persisted document exists")
            .fields
            .insert("body".to_owned(), "rewritten after indexing".to_owned());
        std::fs::write(docs_path, serde_json::to_vec(&documents)?)?;

        let db2 = Primadb::with_replica_id("text-cache-b");
        db2.use_segment_storage(&directory, 8)?;
        let result = db2.text_search("notes", "persistent", TextSearchSpec::default())?;
        assert_eq!(
            result.matches.first().map(|item| item.id.as_str()),
            Some("alpha")
        );
        let result = db2.text_search("notes", "rewritten", TextSearchSpec::default())?;
        assert!(result.matches.is_empty());

        let _ = std::fs::remove_dir_all(&directory);
        Ok(())
    }

    #[test]
    fn text_watches_emit_initial_and_updates() -> Result<()> {
        let db = Primadb::with_replica_id("text-watch");
        db.create_text_collection("notes", TextCollectionConfig::default())?;
        db.put_text_document(
            "notes",
            TextDocument {
                id: "alpha".to_owned(),
                fields: BTreeMap::from([("body".to_owned(), "secure routing".to_owned())]),
                metadata: BTreeMap::new(),
            },
        )?;
        let watch = db.watch_text_search("notes", "vault proposal", TextSearchSpec::default())?;
        let initial = watch.recv_blocking().unwrap();
        assert!(initial.matches.is_empty());

        db.put_text_document(
            "notes",
            TextDocument {
                id: "beta".to_owned(),
                fields: BTreeMap::from([("body".to_owned(), "vault proposal".to_owned())]),
                metadata: BTreeMap::new(),
            },
        )?;
        let updated = watch.recv_blocking().unwrap();
        assert_eq!(updated.matches[0].id, "beta");
        Ok(())
    }

    #[test]
    fn indexed_text_watch_invalidates_existing_record_value_changes() -> Result<()> {
        let db = Primadb::with_replica_id("text-watch-indexed");
        db.create_text_collection("notes", TextCollectionConfig::default())?;
        db.create_text_collection("archive", TextCollectionConfig::default())?;
        db.put_text_document(
            "notes",
            TextDocument {
                id: "alpha".to_owned(),
                fields: BTreeMap::from([("body".to_owned(), "old content".to_owned())]),
                metadata: BTreeMap::new(),
            },
        )?;
        db.put_text_document(
            "archive",
            TextDocument {
                id: "old".to_owned(),
                fields: BTreeMap::from([("body".to_owned(), "archived content".to_owned())]),
                metadata: BTreeMap::new(),
            },
        )?;

        let watch = db.watch_text_search("notes", "fresh signal", TextSearchSpec::default())?;
        let unrelated_watch =
            db.watch_text_search("archive", "fresh signal", TextSearchSpec::default())?;
        assert!(watch.recv_blocking().unwrap().matches.is_empty());
        assert!(unrelated_watch.recv_blocking().unwrap().matches.is_empty());
        assert!(watch.try_recv().is_none());
        assert!(unrelated_watch.try_recv().is_none());
        let recomputations_before_update = db.watch_recomputation_count();

        db.put_text_document(
            "notes",
            TextDocument {
                id: "alpha".to_owned(),
                fields: BTreeMap::from([("body".to_owned(), "fresh signal".to_owned())]),
                metadata: BTreeMap::new(),
            },
        )?;
        let update = watch.recv_blocking().expect("indexed text update");
        assert_eq!(
            update.matches.first().map(|item| item.id.as_str()),
            Some("alpha")
        );
        assert_eq!(
            db.watch_recomputation_count() - recomputations_before_update,
            1
        );
        assert!(unrelated_watch.try_recv().is_none());
        Ok(())
    }

    #[test]
    fn equivalent_record_watches_share_one_recomputation() -> Result<()> {
        let db = Primadb::with_replica_id("watch-coalesce");
        let scan = RecordScan {
            prefix: Some("notes/".to_owned()),
            ..RecordScan::default()
        };
        let first = db.watch_records(scan.clone())?;
        let second = db.watch_records(scan)?;
        assert!(first.recv_blocking().unwrap().entries.is_empty());
        assert!(second.recv_blocking().unwrap().entries.is_empty());
        let before = db.watch_recomputation_count();

        db.put_record_json("notes/1", json!({"body": "first"}))?;
        assert_eq!(db.watch_recomputation_count() - before, 1);
        assert_eq!(first.recv_blocking().unwrap().entries.len(), 1);
        assert_eq!(second.recv_blocking().unwrap().entries.len(), 1);
        assert!(first.try_recv().is_none());
        assert!(second.try_recv().is_none());
        Ok(())
    }

    #[test]
    fn indexed_vector_watch_invalidates_existing_record_value_changes() -> Result<()> {
        let db = Primadb::with_replica_id("vector-watch-indexed");
        for collection in ["docs", "archive"] {
            db.create_vector_collection(
                collection,
                VectorCollectionConfig {
                    dim: 2,
                    metric: VectorMetric::L2,
                    backend: None,
                    hnsw: None,
                    chunking: Default::default(),
                },
            )?;
        }
        db.put_vector("docs", "alpha", vec![10.0, 0.0], None)?;
        db.put_vector("archive", "old", vec![20.0, 0.0], None)?;

        let watch = db.watch_vector_search("docs", [0.0, 0.0], vector_search_spec())?;
        let unrelated = db.watch_vector_search("archive", [0.0, 0.0], vector_search_spec())?;
        let initial = watch.recv_blocking().expect("initial vector result");
        assert_eq!(initial.matches[0].distance, 10.0);
        assert_eq!(unrelated.recv_blocking().unwrap().matches[0].distance, 20.0);
        let recomputations_before_update = db.watch_recomputation_count();

        db.put_vector("docs", "alpha", vec![1.0, 0.0], None)?;
        let update = watch.recv_blocking().expect("indexed vector update");
        assert_eq!(update.matches[0].distance, 1.0);
        assert_eq!(
            db.watch_recomputation_count() - recomputations_before_update,
            1
        );
        assert!(unrelated.try_recv().is_none());
        Ok(())
    }

    #[test]
    fn local_watch_queue_is_bounded_and_keeps_latest_state() -> Result<()> {
        let db = Primadb::with_replica_id("watch-backpressure");
        let watch = db.root("queue").field("value").subscribe()?;
        assert_eq!(watch.recv_blocking(), Some(None));

        for value in 0..(LOCAL_WATCH_QUEUE_CAPACITY + 16) {
            db.root("queue").field("value").put(json!(value))?;
        }
        assert!(watch.receiver().len() <= LOCAL_WATCH_QUEUE_CAPACITY);

        let mut values = Vec::new();
        while let Some(Some(value)) = watch.try_recv() {
            values.push(value.as_i64().expect("numeric watch value"));
        }
        assert_eq!(
            values.last().copied(),
            Some((LOCAL_WATCH_QUEUE_CAPACITY + 15) as i64)
        );
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        Ok(())
    }

    #[test]
    fn closed_local_watch_is_removed_as_stale() -> Result<()> {
        let db = Primadb::with_replica_id("watch-stale");
        let watch = db.root("stale").subscribe()?;
        assert_eq!(db.stats().subscriptions, 1);
        assert_eq!(watch.recv_blocking(), Some(None));
        watch.receiver().close();

        db.root("stale").put(json!({"value": true}))?;
        assert_eq!(db.stats().subscriptions, 0);
        Ok(())
    }

    #[test]
    fn query_scoped_text_search_rejects_paginated_queries_by_default() -> Result<()> {
        let db = Primadb::with_replica_id("text-query");
        db.root("posts").field("a").put(json!({
            "room": "mesh",
            "body": "secure routing notes"
        }))?;
        let source = TextSearchSource::GraphQuery {
            path: RemotePath::new("posts", vec![]),
            spec: QuerySpec {
                filters: vec![QueryFilter::Eq {
                    path: "room".to_owned(),
                    value: json!("mesh"),
                }],
                limit: Some(1),
                ..QuerySpec::default()
            },
        };
        assert!(
            db.text_search(source.clone(), "routing", TextSearchSpec::default())
                .is_err()
        );
        let result = db.text_search(
            source,
            "routing",
            TextSearchSpec {
                candidate_policy: TextCandidatePolicy::AllowPreselectedCandidates,
                ..Default::default()
            },
        )?;
        assert!(result.truncated_candidates);
        assert_eq!(result.score_scope, TextScoreScope::CandidateSet);
        Ok(())
    }

    #[test]
    fn record_scan_text_search_ranks_json_string_leaves() -> Result<()> {
        let db = Primadb::with_replica_id("text-records");
        db.put_record_json(
            "memory/1",
            json!({"topic": "trust", "body": "trust proposal in mesh"}),
        )?;
        db.put_record_json("memory/2", json!({"body": "unrelated"}))?;
        let result = db.text_search(
            TextSearchSource::Records {
                scan: RecordScan {
                    prefix: Some("memory/".to_owned()),
                    ..RecordScan::default()
                },
            },
            "trust proposal",
            TextSearchSpec::default(),
        )?;
        assert_eq!(
            result.matches.first().map(|item| item.id.as_str()),
            Some("memory/1")
        );
        assert_eq!(result.score_scope, TextScoreScope::CandidateSet);
        Ok(())
    }

    #[test]
    fn vector_remote_result_chunks_round_trip() -> Result<()> {
        let result = crate::VectorSearchResult {
            matches: vec![crate::VectorMatch {
                id: "a".to_owned(),
                distance: 0.0,
                metadata: None,
                vector: None,
            }],
            exact: true,
            backend: crate::VectorBackendKind::Exact,
            state: crate::VectorManagerState::Ready,
            stale: false,
            approximate_reason: None,
        };
        let responses = build_pull_responses(
            "request-1",
            crate::RemoteResult::VectorSearch {
                result: result.clone(),
            },
            &PrimadbLimits::default(),
        );
        assert_eq!(responses.len(), 1);
        match &responses[0].result {
            PullResponseBody::VectorSearch { result: actual } => assert_eq!(actual, &result),
            other => panic!("unexpected response body: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn text_remote_result_chunks_round_trip() -> Result<()> {
        let result = crate::TextSearchResult {
            source: crate::TextSearchSourceSummary::Collection {
                collection: "notes".to_owned(),
            },
            query: "mesh".to_owned(),
            matches: Vec::new(),
            backend: crate::TextSearchBackend::Exact,
            exact: true,
            stale: false,
            candidate_count: 0,
            searched_count: 0,
            truncated_candidates: false,
            score_scope: crate::TextScoreScope::Collection,
        };
        let responses = build_pull_responses(
            "request-1",
            crate::RemoteResult::TextSearch {
                result: result.clone(),
            },
            &PrimadbLimits::default(),
        );
        assert_eq!(responses.len(), 1);
        match &responses[0].result {
            PullResponseBody::TextSearch { result: actual } => assert_eq!(actual, &result),
            other => panic!("unexpected response body: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn vector_cache_files_round_trip_against_authoritative_records() -> Result<()> {
        let db = Primadb::with_replica_id("vector-cache-a");
        db.create_vector_collection(
            "docs",
            VectorCollectionConfig {
                dim: 2,
                metric: VectorMetric::L2,
                backend: None,
                hnsw: None,
                chunking: Default::default(),
            },
        )?;
        db.put_vector("docs", "a", vec![0.0, 0.0], Some(json!({"rank": 1})))?;
        db.put_vector("docs", "b", vec![3.0, 0.0], Some(json!({"rank": 2})))?;

        let files = db.export_vector_cache_files("docs")?;
        let replica = Primadb::with_replica_id("vector-cache-b");
        replica.apply_sync_envelope(db.sync_envelope())?;
        replica.import_vector_cache_files("docs", files)?;

        let result = replica.search_vectors(
            "docs",
            vec![0.1, 0.0],
            VectorSearchSpec {
                include_metadata: true,
                ..vector_search_spec()
            },
        )?;
        assert_eq!(result.matches[0].id, "a");
        assert_eq!(result.matches[0].metadata.as_ref().unwrap()["rank"], 1);
        Ok(())
    }

    #[test]
    fn vector_presence_capabilities_include_collections() -> Result<()> {
        let db = Primadb::with_replica_id("vector-caps");
        db.create_vector_collection(
            "docs",
            VectorCollectionConfig {
                dim: 1536,
                metric: VectorMetric::Cosine,
                backend: Some(crate::VectorBackendKind::Edgevec),
                hnsw: None,
                chunking: Default::default(),
            },
        )?;
        let capabilities = db.vector_presence_capabilities();
        assert!(capabilities.iter().any(|item| item == "vector_exact"));
        assert!(
            capabilities.iter().any(
                |item| item.starts_with("vector_collection:646f6373:1536:cosine:ready:edgevec")
            )
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_segment_storage_writes_vector_cache_files() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-vector-cache-{unique}"));

        let first = Primadb::with_replica_id("vector-cache-native-a");
        assert!(!first.use_segment_storage(path.clone(), 2)?);
        first.create_vector_collection(
            "docs",
            VectorCollectionConfig {
                dim: 2,
                metric: VectorMetric::L2,
                backend: None,
                hnsw: None,
                chunking: Default::default(),
            },
        )?;
        first.put_vector("docs", "a", vec![0.0, 0.0], None)?;
        let _ = first.search_vectors("docs", vec![0.0, 0.0], vector_search_spec())?;
        let cache_dir = path
            .join("vector-cache")
            .join(crate::encode_component("docs"));
        assert!(cache_dir.join("manifest.json").exists());
        assert!(cache_dir.join("vectors.f32").exists());
        assert!(cache_dir.join("keys.bin").exists());
        assert!(cache_dir.join("metadata.bin").exists());
        drop(first);

        let second = Primadb::with_replica_id("vector-cache-native-b");
        assert!(second.use_segment_storage(path.clone(), 2)?);
        let result = second.search_vectors("docs", vec![0.0, 0.0], vector_search_spec())?;
        assert_eq!(result.matches[0].id, "a");

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[test]
    fn concurrent_text_and_vector_cache_rebuilds_are_consistent() -> Result<()> {
        let db = Primadb::with_replica_id("cache-concurrency");
        db.create_text_collection("notes", TextCollectionConfig::default())?;
        db.put_text_document(
            "notes",
            TextDocument {
                id: "first".to_owned(),
                fields: BTreeMap::from([("body".to_owned(), "before rebuild".to_owned())]),
                metadata: BTreeMap::new(),
            },
        )?;
        db.create_vector_collection(
            "vectors",
            VectorCollectionConfig {
                dim: 2,
                metric: VectorMetric::L2,
                backend: None,
                hnsw: None,
                chunking: Default::default(),
            },
        )?;
        db.put_vector("vectors", "first", vec![0.0, 0.0], None)?;
        db.text_search("notes", "before", TextSearchSpec::default())?;
        db.search_vectors("vectors", [0.0, 0.0], vector_search_spec())?;

        db.put_text_document(
            "notes",
            TextDocument {
                id: "second".to_owned(),
                fields: BTreeMap::from([("body".to_owned(), "after rebuild".to_owned())]),
                metadata: BTreeMap::new(),
            },
        )?;
        db.put_vector("vectors", "second", vec![1.0, 0.0], None)?;

        let barrier = Arc::new(Barrier::new(9));
        let mut workers = Vec::new();
        for _ in 0..8 {
            let db = db.clone();
            let barrier = barrier.clone();
            workers.push(thread::spawn(move || {
                barrier.wait();
                let text = db.text_search("notes", "after", TextSearchSpec::default())?;
                let vector = db.search_vectors("vectors", [1.0, 0.0], vector_search_spec())?;
                Ok::<_, PrimadbError>((text, vector))
            }));
        }
        barrier.wait();
        for worker in workers {
            let (text, vector) = worker.join().expect("cache worker panicked")?;
            assert_eq!(
                text.matches.first().map(|item| item.id.as_str()),
                Some("second")
            );
            assert_eq!(
                vector.matches.first().map(|item| item.id.as_str()),
                Some("second")
            );
        }
        Ok(())
    }

    #[cfg(feature = "vector-edgevec")]
    #[test]
    fn edgevec_backend_serves_ann_search() -> Result<()> {
        let db = Primadb::with_replica_id("vector-edgevec");
        db.create_vector_collection(
            "docs",
            VectorCollectionConfig {
                dim: 3,
                metric: VectorMetric::Cosine,
                backend: Some(crate::VectorBackendKind::Edgevec),
                hnsw: None,
                chunking: Default::default(),
            },
        )?;
        db.put_vector("docs", "x", vec![1.0, 0.0, 0.0], None)?;
        db.put_vector("docs", "y", vec![0.0, 1.0, 0.0], None)?;

        let result = db.search_vectors(
            "docs",
            vec![1.0, 0.0, 0.0],
            VectorSearchSpec {
                limit: 1,
                exact: false,
                ..vector_search_spec()
            },
        )?;
        assert_eq!(result.backend, crate::VectorBackendKind::Edgevec);
        assert!(!result.exact);
        assert_eq!(result.matches[0].id, "x");
        Ok(())
    }

    #[test]
    fn local_transaction_commits_writes_in_one_watch_batch() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        let changes = db.subscribe_changes();
        let initial = changes.recv_blocking().unwrap();
        assert_eq!(initial.revision, 0);

        db.transaction(|tx| {
            tx.root("docs").field("a").put(json!({"title": "A"}))?;
            tx.root("docs").field("b").put(json!({"title": "B"}))?;
            Ok(())
        })?;

        let event = changes.recv_blocking().unwrap();
        assert!(event.data_changed);
        assert_eq!(event.revision, 1);
        assert!(changes.try_recv().is_none());
        assert_eq!(
            db.root("docs").field("a").once_json()?.unwrap()["title"],
            "A"
        );
        assert_eq!(
            db.root("docs").field("b").once_json()?.unwrap()["title"],
            "B"
        );
        Ok(())
    }

    #[test]
    fn local_transaction_rolls_back_on_failed_precondition() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        db.root("docs").field("a").put(json!({"version": 1}))?;
        let pending_before = db.pending_operations().len();

        let result = db.transaction(|tx| {
            tx.root("docs").field("a").put(json!({"version": 2}))?;
            tx.root("docs").field("a").assert_absent()?;
            Ok(())
        });

        assert!(matches!(
            result,
            Err(PrimadbError::TransactionConflict { .. })
        ));
        assert_eq!(db.pending_operations().len(), pending_before);
        assert_eq!(
            db.root("docs").field("a").once_json()?.unwrap()["version"],
            1
        );
        Ok(())
    }

    #[test]
    fn local_transaction_rollback_restores_relationships_and_compacted_queues() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        db.root("people").field("alice").put(json!({
            "name": "Alice"
        }))?;
        db.root("people")
            .field("bob")
            .field("friend")
            .put(json!({"$link": "people/alice"}))?;
        let pending_before = db.pending_operations();
        let snapshot_before = db.snapshot();

        let result = db.transaction(|tx| {
            tx.root("people")
                .field("bob")
                .field("friend")
                .put(json!({"$link": "people/charlie"}))?;
            tx.root("people").field("bob").assert_absent()
        });

        assert!(matches!(
            result,
            Err(PrimadbError::TransactionConflict { .. })
        ));
        assert_eq!(db.snapshot(), snapshot_before);
        assert_eq!(db.pending_operations(), pending_before);
        let traversal = db.root("people").field("bob").traverse(TraversalSpec {
            direction: TraversalDirection::Outbound,
            max_depth: 1,
            ..TraversalSpec::default()
        })?;
        assert_eq!(
            traversal
                .entries
                .iter()
                .map(|entry| entry.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["people/alice"]
        );
        Ok(())
    }

    #[test]
    fn transaction_steps_support_revision_cas_and_increment() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        db.root("accounts")
            .field("alice")
            .field("balance")
            .put(10)?;
        let revision = db.transaction(|tx| {
            tx.root("accounts")
                .field("alice")
                .field("balance")
                .revision()
        })?;

        let path = RemotePath::new("accounts", vec!["alice".to_owned(), "balance".to_owned()]);
        let report = db.apply_transaction_steps(vec![
            TransactionStep::AssertRevision {
                path: path.clone(),
                revision,
            },
            TransactionStep::Increment { path, by: 5.0 },
        ])?;

        assert_eq!(report.status, TransactionStatus::Committed);
        assert_eq!(report.operation_count, 1);
        assert_eq!(
            db.root("accounts")
                .field("alice")
                .field("balance")
                .once_json()?,
            Some(json!(15.0))
        );

        let stale = db.apply_transaction_steps(vec![TransactionStep::AssertRevision {
            path: RemotePath::new("accounts", vec!["alice".to_owned(), "balance".to_owned()]),
            revision: None,
        }]);
        assert!(matches!(
            stale,
            Err(PrimadbError::TransactionConflict { .. })
        ));
        Ok(())
    }

    #[test]
    fn transaction_steps_reject_unscoped_then_strict_scope_mix() -> Result<()> {
        let db = Primadb::with_replica_id("ledger");
        db.scope("accounts").configure(ScopePolicy {
            consistency: ScopeConsistency::Coordinated,
            authority: Some(ScopeAuthority::FullNode {
                peer_id: "native:ledger".to_owned(),
            }),
            ..ScopePolicy::default()
        })?;

        let result = db.apply_transaction_steps(vec![
            TransactionStep::Put {
                path: RemotePath::new("notes", vec!["latest".to_owned()]),
                value: json!("eventual"),
            },
            TransactionStep::Increment {
                path: RemotePath::new("accounts", vec!["alice".to_owned(), "balance".to_owned()]),
                by: 1.0,
            },
        ]);

        assert!(matches!(
            result,
            Err(PrimadbError::StrictScopeConflict { .. })
        ));
        assert_eq!(
            db.root("notes").field("latest").once_json()?,
            None,
            "validation must fail before any unscoped write is committed"
        );
        Ok(())
    }

    #[test]
    fn transaction_steps_treat_local_transactional_scopes_as_strict_boundaries() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        db.scope("catalog").configure(ScopePolicy {
            consistency: ScopeConsistency::LocalTransactional,
            ..ScopePolicy::default()
        })?;

        let result = db.apply_transaction_steps(vec![
            TransactionStep::Put {
                path: RemotePath::new("catalog", vec!["sku-1".to_owned()]),
                value: json!({"stock": 1}),
            },
            TransactionStep::Put {
                path: RemotePath::new("notes", vec!["latest".to_owned()]),
                value: json!("eventual"),
            },
        ]);

        assert!(matches!(
            result,
            Err(PrimadbError::StrictScopeConflict { .. })
        ));
        assert_eq!(
            db.root("catalog").field("sku-1").once_json()?,
            None,
            "mixed strict/eventual transaction must not partially commit"
        );
        Ok(())
    }

    #[test]
    fn coordinated_scope_rejects_canonical_write_when_authority_unavailable() -> Result<()> {
        let db = Primadb::with_replica_id("peer-a");
        db.scope("accounts").configure(ScopePolicy {
            consistency: ScopeConsistency::Coordinated,
            authority: Some(ScopeAuthority::FullNode {
                peer_id: "native:ledger".to_owned(),
            }),
            offline_writes: ScopeOfflineWrites::Reject,
            ..ScopePolicy::default()
        })?;

        let write = db.root("accounts").field("alice").field("balance").put(10);
        assert!(matches!(
            write,
            Err(PrimadbError::StrictScopeUnavailable { .. })
        ));
        assert_eq!(
            db.root("accounts")
                .field("alice")
                .field("balance")
                .once_json()?,
            None
        );
        Ok(())
    }

    #[test]
    fn coordinated_scope_can_queue_provisional_write_without_committing() -> Result<()> {
        let db = Primadb::with_replica_id("peer-a");
        let scope = db.scope("accounts");
        scope.configure(ScopePolicy {
            consistency: ScopeConsistency::Coordinated,
            authority: Some(ScopeAuthority::FullNode {
                peer_id: "native:ledger".to_owned(),
            }),
            offline_writes: ScopeOfflineWrites::QueueProvisional,
            ..ScopePolicy::default()
        })?;

        let report = scope.transaction_steps(
            vec![TransactionStep::Increment {
                path: RemotePath::new("alice", vec!["balance".to_owned()]),
                by: 10.0,
            }],
            TransactionOptions::default(),
        )?;

        assert_eq!(report.status, TransactionStatus::Provisional);
        assert!(report.proposal_id.is_some());
        assert_eq!(scope.proposals().len(), 1);
        assert_eq!(
            db.root("accounts")
                .field("alice")
                .field("balance")
                .once_json()?,
            None
        );
        Ok(())
    }

    #[test]
    fn coordinated_scope_local_authority_can_commit() -> Result<()> {
        let db = Primadb::with_replica_id("ledger");
        let scope = db.scope("accounts");
        scope.configure(ScopePolicy {
            consistency: ScopeConsistency::Coordinated,
            authority: Some(ScopeAuthority::FullNode {
                peer_id: "native:ledger".to_owned(),
            }),
            ..ScopePolicy::default()
        })?;

        let report = scope.transaction_steps(
            vec![TransactionStep::Increment {
                path: RemotePath::new("alice", vec!["balance".to_owned()]),
                by: 10.0,
            }],
            TransactionOptions::default(),
        )?;

        assert_eq!(report.status, TransactionStatus::Committed);
        assert_eq!(
            db.root("accounts")
                .field("alice")
                .field("balance")
                .once_json()?,
            Some(json!(10.0))
        );
        Ok(())
    }

    #[test]
    fn pull_transaction_request_commits_on_authority() -> Result<()> {
        let db = Primadb::with_replica_id("ledger");
        db.scope("accounts").configure(ScopePolicy {
            consistency: ScopeConsistency::Coordinated,
            authority: Some(ScopeAuthority::FullNode {
                peer_id: "native:ledger".to_owned(),
            }),
            ..ScopePolicy::default()
        })?;

        let result = db.execute_pull_request(&PullRequest {
            request_id: "tx-1".to_owned(),
            request: PullRequestKind::Transaction {
                scope: "accounts".to_owned(),
                steps: vec![TransactionStep::Increment {
                    path: RemotePath::new("alice", vec!["balance".to_owned()]),
                    by: 12.0,
                }],
                options: TransactionOptions::default(),
            },
        })?;

        match result {
            crate::RemoteResult::Transaction { report } => {
                assert_eq!(report.status, TransactionStatus::Committed);
                assert!(report.operation_count >= 1);
            }
            other => panic!("unexpected result: {other:?}"),
        }
        assert_eq!(
            db.root("accounts")
                .field("alice")
                .field("balance")
                .once_json()?,
            Some(json!(12.0))
        );
        Ok(())
    }

    #[test]
    fn pull_records_request_scans_keyed_records() -> Result<()> {
        let db = Primadb::with_replica_id("records-pull");
        db.put_record_json("agentfs/chunk/2/000000", json!({"bytes": 5}))?;
        db.put_record_json("agentfs/chunk/2/000001", json!({"bytes": 6}))?;
        db.put_record_json("agentfs/inode/2", json!({"kind": "file"}))?;

        let result = db.execute_pull_request(&PullRequest {
            request_id: "records-1".to_owned(),
            request: PullRequestKind::Records {
                scan: RecordScan {
                    prefix: Some("agentfs/chunk/2/".to_owned()),
                    ..RecordScan::default()
                },
            },
        })?;

        match result {
            crate::RemoteResult::Records { result } => {
                let keys = result
                    .entries
                    .iter()
                    .map(|entry| entry.key.as_str())
                    .collect::<Vec<_>>();
                assert_eq!(keys, ["agentfs/chunk/2/000000", "agentfs/chunk/2/000001"]);
                assert_eq!(result.next_cursor, None);
            }
            other => panic!("unexpected result: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn coordinated_scope_rejects_remote_ops_from_non_authority() -> Result<()> {
        let receiver = Primadb::with_replica_id("receiver");
        receiver.scope("accounts").configure(ScopePolicy {
            consistency: ScopeConsistency::Coordinated,
            authority: Some(ScopeAuthority::FullNode {
                peer_id: "native:ledger".to_owned(),
            }),
            ..ScopePolicy::default()
        })?;

        let rogue = Primadb::with_replica_id("rogue");
        rogue
            .root("accounts")
            .field("alice")
            .field("balance")
            .put(99)?;
        assert_eq!(
            receiver.apply_operations(rogue.drain_pending_operations()?)?,
            0
        );
        assert_eq!(
            receiver
                .root("accounts")
                .field("alice")
                .field("balance")
                .once_json()?,
            None
        );

        let authority = Primadb::with_replica_id("ledger");
        authority
            .root("accounts")
            .field("alice")
            .field("balance")
            .put(7)?;
        assert_eq!(
            receiver.apply_operations(authority.drain_pending_operations()?)?,
            2
        );
        assert_eq!(
            receiver
                .root("accounts")
                .field("alice")
                .field("balance")
                .once_json()?,
            Some(json!(7))
        );
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
    fn local_record_watch_emits_initial_and_matching_scan_updates() -> Result<()> {
        let db = Primadb::with_replica_id("record-watch");
        db.put_record_json("agentfs/chunk/2/000000", json!({"bytes": 5}))?;
        db.put_record_json("agentfs/inode/2", json!({"kind": "file"}))?;

        let watch = db.watch_records(RecordScan {
            prefix: Some("agentfs/chunk/2/".to_owned()),
            ..RecordScan::default()
        })?;

        let initial = watch.recv_blocking().expect("initial record scan");
        assert_eq!(initial.entries.len(), 1);
        assert_eq!(initial.entries[0].key, "agentfs/chunk/2/000000");
        assert!(watch.try_recv().is_none());

        db.put_record_json("agentfs/inode/3", json!({"kind": "dir"}))?;
        assert!(
            watch.try_recv().is_none(),
            "non-overlapping record keys must not refresh the watch"
        );

        db.put_record_json("agentfs/chunk/2/000001", json!({"bytes": 6}))?;
        let updated = watch.recv_blocking().expect("matching record update");
        let keys = updated
            .entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["agentfs/chunk/2/000000", "agentfs/chunk/2/000001"]);
        assert!(watch.try_recv().is_none());

        db.put_record_json("agentfs/chunk/2/000001", json!({"bytes": 6}))?;
        assert!(
            watch.try_recv().is_none(),
            "matching writes with unchanged scan content must not re-emit"
        );

        db.delete_record("agentfs/chunk/2/000000")?;
        let deleted = watch.recv_blocking().expect("matching record delete");
        let keys = deleted
            .entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["agentfs/chunk/2/000001"]);
        Ok(())
    }

    #[test]
    fn record_pull_responses_chunk_entries_and_keep_cursor_on_final_chunk() {
        let result = RecordScanResult {
            entries: (0..5)
                .map(|index| RecordEntry {
                    key: format!("agentfs/chunk/2/{index:06}"),
                    value: RecordValue::Json(json!({ "index": index })),
                })
                .collect(),
            next_cursor: Some("agentfs/chunk/2/000004".to_owned()),
        };
        let responses = build_pull_responses(
            "records-1",
            crate::RemoteResult::Records { result },
            &PrimadbLimits {
                max_query_entries_per_chunk: 2,
                ..PrimadbLimits::default()
            },
        );

        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0].chunk.index, 0);
        assert_eq!(responses[0].chunk.total, 3);
        assert!(!responses[0].done);
        assert_eq!(responses[1].chunk.index, 1);
        assert!(!responses[1].done);
        assert_eq!(responses[2].chunk.index, 2);
        assert!(responses[2].done);

        match &responses[0].result {
            PullResponseBody::Records {
                entries,
                next_cursor,
            } => {
                assert_eq!(entries.len(), 2);
                assert_eq!(next_cursor, &None);
            }
            other => panic!("unexpected chunk: {other:?}"),
        }
        match &responses[2].result {
            PullResponseBody::Records {
                entries,
                next_cursor,
            } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(next_cursor.as_deref(), Some("agentfs/chunk/2/000004"));
            }
            other => panic!("unexpected chunk: {other:?}"),
        }
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
    fn traverse_walks_outbound_links_without_materializing_whole_graph() -> Result<()> {
        let db = Primadb::with_replica_id("traverse-a");
        db.root("people").field("alice").put(json!({
            "name": "Alice",
            "friend": {"$link": "people/bob"}
        }))?;
        db.root("people").field("bob").put(json!({
            "name": "Bob",
            "friend": {"$link": "people/alice"}
        }))?;

        let result = db.root("people").field("alice").traverse(TraversalSpec {
            max_depth: 2,
            include_values: true,
            ..TraversalSpec::default()
        })?;

        assert!(result.complete);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].node_id, "people/bob");
        assert_eq!(result.entries[0].depth, 1);
        assert_eq!(result.entries[0].value.as_ref().unwrap()["name"], "Bob");
        assert_eq!(
            result.entries[0].value.as_ref().unwrap()["friend"],
            json!({"$link": "people/alice"})
        );
        Ok(())
    }

    #[test]
    fn traverse_uses_reverse_relationship_index_for_inbound_edges() -> Result<()> {
        let db = Primadb::with_replica_id("traverse-b");
        db.root("people").field("alice").put(json!({
            "name": "Alice",
            "friend": {"$link": "people/bob"}
        }))?;
        db.root("people").field("bob").put(json!({"name": "Bob"}))?;

        let result = db.root("people").field("bob").traverse(TraversalSpec {
            direction: TraversalDirection::Inbound,
            max_depth: 2,
            include_values: true,
            ..TraversalSpec::default()
        })?;

        assert!(result.complete);
        assert!(result.entries.iter().any(|entry| {
            entry.node_id == "people/alice" && entry.via.as_ref().unwrap().field == "friend"
        }));
        Ok(())
    }

    #[test]
    fn traverse_respects_set_edges_field_filters_and_limits() -> Result<()> {
        let db = Primadb::with_replica_id("traverse-c");
        db.root("people")
            .field("alice")
            .put(json!({"name": "Alice"}))?;
        db.root("people").field("bob").put(json!({"name": "Bob"}))?;
        db.root("rooms").field("lobby").put(json!({
            "members": {
                "$set": [
                    {"$link": "people/alice"},
                    {"$link": "people/bob"}
                ]
            },
            "owner": {"$link": "people/alice"}
        }))?;

        let result = db.root("rooms").field("lobby").traverse(TraversalSpec {
            max_depth: 1,
            limit: Some(1),
            edge_fields: Some(vec!["members".to_owned()]),
            ..TraversalSpec::default()
        })?;

        assert!(!result.complete);
        assert!(result.result_limit_reached);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].via.as_ref().unwrap().field, "members");
        Ok(())
    }

    #[derive(Default)]
    struct TestFetchScheduler {
        nodes: std::sync::Mutex<Vec<String>>,
    }

    impl NodeFetchScheduler for TestFetchScheduler {
        fn fetch_nodes(&self, nodes: Vec<String>) {
            self.nodes.lock().unwrap().extend(nodes);
        }
    }

    #[test]
    fn traverse_schedules_lazy_fetch_for_absent_link_targets() -> Result<()> {
        let db = Primadb::with_replica_id("traverse-d");
        db.root("people").field("alice").put(json!({
            "name": "Alice",
            "friend": {"$link": "people/bob"}
        }))?;
        let scheduler = Arc::new(TestFetchScheduler::default());
        db.register_node_fetch_scheduler(scheduler.clone());

        let result = db.root("people").field("alice").traverse(TraversalSpec {
            max_depth: 1,
            ..TraversalSpec::default()
        })?;

        assert!(!result.complete);
        assert_eq!(result.missing, vec!["people/bob"]);
        assert_eq!(result.fetched, 1);
        assert_eq!(scheduler.nodes.lock().unwrap().as_slice(), ["people/bob"]);
        Ok(())
    }

    #[test]
    fn traverse_treats_explicit_empty_nodes_as_known() -> Result<()> {
        let db = Primadb::with_replica_id("traverse-empty-node");
        db.root("people").field("bob").put(json!({}))?;
        db.root("people").field("alice").put(json!({
            "friend": {"$link": "people/bob"}
        }))?;

        let result = db.root("people").field("alice").traverse(TraversalSpec {
            max_depth: 1,
            include_values: true,
            ..TraversalSpec::default()
        })?;

        assert!(result.complete);
        assert!(result.missing.is_empty());
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].node_id, "people/bob");
        assert_eq!(
            result.entries[0].value.as_ref().unwrap()["$id"],
            "people/bob"
        );
        Ok(())
    }

    #[test]
    fn watch_traverse_updates_when_dependency_changes() -> Result<()> {
        let db = Primadb::with_replica_id("traverse-e");
        db.root("people")
            .field("alice")
            .put(json!({"friend": {"$link": "people/bob"}}))?;
        db.root("people").field("bob").put(json!({"name": "Bob"}))?;

        let watch = db
            .root("people")
            .field("alice")
            .watch_traverse(TraversalSpec {
                max_depth: 1,
                include_values: true,
                ..TraversalSpec::default()
            })?;
        let initial = watch.recv_blocking().unwrap();
        assert_eq!(initial.entries[0].value.as_ref().unwrap()["name"], "Bob");

        db.root("unrelated")
            .field("item")
            .put(json!({"name": "Noop"}))?;
        assert!(watch.try_recv().is_none());

        db.root("people")
            .field("bob")
            .put(json!({"name": "Robert"}))?;
        let updated = watch.recv_blocking().unwrap();
        assert_eq!(updated.entries[0].value.as_ref().unwrap()["name"], "Robert");
        Ok(())
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
    #[tokio::test]
    async fn watch_traverse_fetches_missing_node_from_relay_peer() -> Result<()> {
        let relay = crate::NativeRelayServer::bind("127.0.0.1:0").await?;
        let source_db = Primadb::with_replica_id("traverse-relay-source");
        source_db
            .root("people")
            .field("bob")
            .put(json!({"name": "Bob"}))?;
        source_db.drain_pending_operations()?;

        let client_db = Primadb::with_replica_id("traverse-relay-client");
        client_db.root("people").field("alice").put(json!({
            "friend": {"$link": "people/bob"}
        }))?;
        client_db.drain_pending_operations()?;

        let mut source_sync = source_db
            .connect_relay(crate::RelayClientConfig {
                url: relay.url(),
                retry_interval_ms: 50,
                session_auth: crate::SessionAuthConfig::default(),
            })
            .await?;
        let mut client_sync = client_db
            .connect_relay(crate::RelayClientConfig {
                url: relay.url(),
                retry_interval_ms: 50,
                session_auth: crate::SessionAuthConfig::default(),
            })
            .await?;

        wait_for_native_relay(
            || {
                source_sync.is_connected()
                    && client_sync.is_connected()
                    && source_sync.known_peer_count() >= 1
                    && client_sync.known_peer_count() >= 1
            },
            "relay clients to discover each other",
        )
        .await?;

        let watch = client_db
            .root("people")
            .field("alice")
            .watch_traverse(TraversalSpec {
                max_depth: 1,
                include_values: true,
                ..TraversalSpec::default()
            })?;
        let initial = watch.recv().await.expect("initial traversal result");
        assert!(!initial.complete);
        assert_eq!(initial.missing, vec!["people/bob"]);
        assert_eq!(initial.fetched, 1);

        let updated = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let result = watch.recv().await.ok_or_else(|| {
                    crate::PrimadbError::Message("traversal watch closed".to_owned())
                })?;
                if result.complete
                    && result.entries.iter().any(|entry| {
                        entry
                            .value
                            .as_ref()
                            .is_some_and(|value| value["name"] == "Bob")
                    })
                {
                    return Ok::<_, crate::PrimadbError>(result);
                }
            }
        })
        .await
        .map_err(|_| {
            crate::PrimadbError::Message("timed out waiting for lazy node fetch".to_owned())
        })??;

        assert!(updated.missing.is_empty());
        assert_eq!(
            client_db.node_state("people/bob")?.unwrap().id,
            "people/bob"
        );

        source_sync.close();
        client_sync.close();
        relay.close().await;
        Ok(())
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "native-websocket"))]
    async fn wait_for_native_relay<F>(mut condition: F, description: &str) -> Result<()>
    where
        F: FnMut() -> bool,
    {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if condition() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Err(crate::PrimadbError::Message(format!(
            "timed out waiting for {description}"
        )))
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
    fn subscriptions_skip_unrelated_changes() -> Result<()> {
        let db = Primadb::with_replica_id("node-a");
        let subscription = db.root("users").field("alice").subscribe()?;

        assert_eq!(subscription.recv_blocking(), Some(None));

        db.root("docs")
            .field("hello")
            .put(json!({"value": "world"}))?;
        assert_eq!(subscription.try_recv(), None);

        db.root("users")
            .field("alice")
            .put(json!({"name": "Alice"}))?;
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
    fn file_persistence_round_trips_provisional_transactions() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-proposals-{unique}.json"));

        let first = Primadb::with_replica_id("proposal-a");
        assert!(!first.use_file_persistence(path.clone())?);
        let scope = first.scope("accounts");
        scope.configure(ScopePolicy {
            consistency: ScopeConsistency::Coordinated,
            authority: Some(ScopeAuthority::FullNode {
                peer_id: "native:ledger".to_owned(),
            }),
            offline_writes: ScopeOfflineWrites::QueueProvisional,
            ..ScopePolicy::default()
        })?;
        scope.transaction_steps(
            vec![TransactionStep::Increment {
                path: RemotePath::new("alice", vec!["balance".to_owned()]),
                by: 1.0,
            }],
            TransactionOptions::default(),
        )?;

        let second = Primadb::with_replica_id("proposal-b");
        assert!(second.use_file_persistence(path.clone())?);
        let proposals = second.scope("accounts").proposals();
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].scope, "accounts");

        let _ = std::fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn bytes_fields_round_trip_and_replicate() -> Result<()> {
        let left = Primadb::with_replica_id("bytes-left");
        let right = Primadb::with_replica_id("bytes-right");
        let payload = vec![0, 7, 42, 255, 128, 1];

        left.root("assets")
            .field("avatar")
            .put_bytes(payload.clone())?;
        assert_eq!(
            left.root("assets").field("avatar").once_bytes()?,
            Some(payload.clone())
        );

        right.apply_operations(left.drain_pending_operations()?)?;
        assert_eq!(
            right.root("assets").field("avatar").once_bytes()?,
            Some(payload)
        );

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
            durability: Default::default(),
        })?;
        assert_eq!(binding.backend, "files");

        let reference = first.root("assets").field("backup").put_blob(
            b"native-file-blob".to_vec(),
            Some("application/octet-stream"),
        )?;

        let second = Primadb::with_replica_id("blob-file-b");
        second.open_blob_storage(crate::BlobStorageConfig::Files {
            directory: root.display().to_string(),
            durability: Default::default(),
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
    fn segment_storage_round_trips_state() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-{unique}"));

        let first = Primadb::with_replica_id("node-a");
        assert!(!first.use_segment_storage(path.clone(), 2)?);
        first
            .root("docs")
            .field("hello")
            .put(json!({"value": "world"}))?;
        drop(first);

        let second = Primadb::with_replica_id("node-b");
        assert!(second.use_segment_storage(path.clone(), 2)?);
        let snapshot = second.root("docs").field("hello").once_json()?.unwrap();
        assert_eq!(snapshot["value"], "world");

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[test]
    fn segment_storage_round_trips_provisional_transactions() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-proposals-{unique}"));

        let first = Primadb::with_replica_id("proposal-segment-a");
        assert!(!first.use_segment_storage(path.clone(), 2)?);
        let scope = first.scope("accounts");
        scope.configure(ScopePolicy {
            consistency: ScopeConsistency::Coordinated,
            authority: Some(ScopeAuthority::FullNode {
                peer_id: "native:ledger".to_owned(),
            }),
            offline_writes: ScopeOfflineWrites::QueueProvisional,
            ..ScopePolicy::default()
        })?;
        scope.transaction_steps(
            vec![TransactionStep::Increment {
                path: RemotePath::new("alice", vec!["balance".to_owned()]),
                by: 1.0,
            }],
            TransactionOptions::default(),
        )?;
        drop(scope);
        drop(first);

        let second = Primadb::with_replica_id("proposal-segment-b");
        assert!(second.use_segment_storage(path.clone(), 2)?);
        let proposals = second.scope("accounts").proposals();
        assert_eq!(proposals.len(), 1);
        assert_eq!(proposals[0].scope, "accounts");

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
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
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
        drop(writer);

        let reader = Primadb::with_replica_id("reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        assert_eq!(reader.stats().nodes, 0);

        let open_tasks = reader
            .root("lists")
            .field("main")
            .field("items")
            .query(QuerySpec {
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
    fn incremental_storage_pushes_down_nested_scalar_indexes() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-nested-{unique}"));

        let writer = Primadb::with_replica_id("writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        for index in 0..24 {
            writer
                .root("lists")
                .field("main")
                .field("items")
                .set(json!({
                    "title": format!("Task {index}"),
                    "flags": { "archived": index % 5 == 0 },
                    "profile": { "rank": index },
                }))?;
        }
        drop(writer);

        let reader = Primadb::with_replica_id("reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        assert_eq!(reader.stats().nodes, 0);

        let ranked = reader
            .root("lists")
            .field("main")
            .field("items")
            .query(QuerySpec {
                filters: vec![
                    QueryFilter::Eq {
                        path: "flags.archived".to_owned(),
                        value: json!(false),
                    },
                    QueryFilter::Gte {
                        path: "profile.rank".to_owned(),
                        value: json!(10),
                    },
                ],
                order: Some(crate::query::QueryOrder {
                    path: "profile.rank".to_owned(),
                    direction: QueryDirection::Desc,
                }),
                limit: Some(3),
                offset: 0,
            })?;

        assert_eq!(ranked.len(), 3);
        assert_eq!(ranked[0].value["title"], "Task 23");
        assert_eq!(ranked[1].value["title"], "Task 22");
        assert_eq!(ranked[2].value["title"], "Task 21");
        assert!(reader.stats().nodes < 24);

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_files_persist_large_string_scalar_without_filename_limit() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-large-scalar-{unique}"));
        let first_ciphertext = format!("v1.{}", "a".repeat(64 * 1024));
        let second_ciphertext = format!("v2.{}", "b".repeat(64 * 1024));

        let writer = Primadb::with_replica_id("large-scalar-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        let checkpoint = writer.root("starla").field("runtime").field("default");
        checkpoint.put(json!({
            "kind": "starla.encryptedRuntimeCheckpoint",
            "version": 1,
            "namespace": "default",
            "encryption": {
                "algorithm": "xchacha20poly1305",
                "ciphertext": first_ciphertext,
            },
        }))?;
        checkpoint.put(json!({
            "kind": "starla.encryptedRuntimeCheckpoint",
            "version": 1,
            "namespace": "default",
            "encryption": {
                "algorithm": "xchacha20poly1305",
                "ciphertext": second_ciphertext,
            },
        }))?;
        drop(checkpoint);
        drop(writer);

        let reader = Primadb::with_replica_id("large-scalar-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        let restored = reader
            .root("starla")
            .field("runtime")
            .field("default")
            .once_json()?
            .unwrap();
        assert_eq!(
            restored["encryption"]["ciphertext"].as_str(),
            Some(second_ciphertext.as_str())
        );

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_files_query_large_string_scalar_direct_indexes() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-large-index-{unique}"));
        let alpha_ciphertext = format!("checkpoint-alpha.{}", "x".repeat(64 * 1024));
        let beta_ciphertext = format!("checkpoint-beta.{}", "y".repeat(64 * 1024));

        let writer = Primadb::with_replica_id("large-index-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        writer.root("checkpoints").field("items").set(json!({
            "name": "alpha",
            "encryption": {
                "ciphertext": alpha_ciphertext,
            },
        }))?;
        writer.root("checkpoints").field("items").set(json!({
            "name": "beta",
            "encryption": {
                "ciphertext": beta_ciphertext,
            },
        }))?;
        drop(writer);

        let reader = Primadb::with_replica_id("large-index-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        let exact = reader.root("checkpoints").field("items").query(QuerySpec {
            filters: vec![QueryFilter::Eq {
                path: "encryption.ciphertext".to_owned(),
                value: json!(alpha_ciphertext),
            }],
            order: Some(crate::query::QueryOrder {
                path: "name".to_owned(),
                direction: QueryDirection::Asc,
            }),
            limit: Some(10),
            offset: 0,
        })?;
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].value["name"], "alpha");

        let prefixed = reader.root("checkpoints").field("items").query(QuerySpec {
            filters: vec![QueryFilter::Prefix {
                path: "encryption.ciphertext".to_owned(),
                value: "checkpoint-beta.".to_owned(),
            }],
            order: Some(crate::query::QueryOrder {
                path: "name".to_owned(),
                direction: QueryDirection::Asc,
            }),
            limit: Some(10),
            offset: 0,
        })?;
        assert_eq!(prefixed.len(), 1);
        assert_eq!(prefixed[0].value["name"], "beta");

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_files_coalesce_unchanged_direct_index_bucket_writes() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-write-metrics-{unique}"));
        let value = json!({
            "name": "checkpoint",
            "version": 7,
            "enabled": true,
        });

        let writer = Primadb::with_replica_id("write-metrics-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        crate::engine::reset_segment_write_metrics_for_test(path.clone());
        writer
            .root("checkpoints")
            .field("current")
            .put(value.clone())?;
        let first = crate::engine::segment_write_metrics_for_test(path.clone());
        assert!(first.direct_index_writes > 0);
        assert!(first.bytes_written > 0);
        assert_eq!(first.file_syncs, first.file_writes);

        crate::engine::reset_segment_write_metrics_for_test(path.clone());
        writer.root("checkpoints").field("current").put(value)?;
        let unchanged = crate::engine::segment_write_metrics_for_test(path.clone());
        assert_eq!(unchanged.direct_index_writes, 0);
        assert_eq!(unchanged.direct_index_directory_syncs, 0);

        crate::engine::reset_segment_write_metrics_for_test(path.clone());
        writer.root("checkpoints").field("current").put(json!({
            "name": "checkpoint",
            "version": 8,
            "enabled": true,
        }))?;
        let changed = crate::engine::segment_write_metrics_for_test(path.clone());
        assert_eq!(changed.direct_index_writes, 2);
        assert!(changed.direct_index_directory_syncs > 0);
        drop(writer);

        let reader = Primadb::with_replica_id("write-metrics-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        assert_eq!(
            reader.root("checkpoints").field("current").once_json()?,
            Some(json!({
                "$id": "checkpoints/current",
                "name": "checkpoint",
                "version": 8,
                "enabled": true,
            }))
        );

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_files_keep_durability_mode_sync_policies() -> Result<()> {
        let modes = [
            ("full", crate::SegmentDurability::Full, true, true),
            ("data", crate::SegmentDurability::Data, true, false),
            ("relaxed", crate::SegmentDurability::Relaxed, false, false),
        ];

        for (name, durability, expect_file_sync, expect_directory_sync) in modes {
            let unique = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let path =
                std::env::temp_dir().join(format!("primadb-segment-durability-{name}-{unique}"));
            let db = Primadb::with_replica_id(format!("durability-{name}"));
            db.open_durable_storage(crate::DurableStorageConfig::SegmentFiles {
                directory: path.display().to_string(),
                journal_retention: 8,
                durability,
                lock_mode: crate::SegmentLockMode::Exclusive,
            })?;
            crate::engine::reset_segment_write_metrics_for_test(path.clone());
            db.root("durability").field("value").put(json!(1))?;
            let metrics = crate::engine::segment_write_metrics_for_test(path.clone());

            assert_eq!(
                metrics.file_syncs > 0,
                expect_file_sync,
                "{name} file sync policy"
            );
            assert_eq!(
                metrics.direct_index_directory_syncs > 0,
                expect_directory_sync,
                "{name} directory sync policy"
            );
            drop(db);
            let _ = std::fs::remove_dir_all(path);
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_files_enforce_single_writer_lock() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-lock-{unique}"));

        let first = Primadb::with_replica_id("lock-a");
        assert!(!first.use_segment_storage(path.clone(), 8)?);

        let second = Primadb::with_replica_id("lock-b");
        let blocked = second.use_segment_storage(path.clone(), 8);
        assert!(blocked.is_err());

        drop(first);
        let third = Primadb::with_replica_id("lock-c");
        assert!(third.use_segment_storage(path.clone(), 8).is_ok());

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_files_recover_pending_commit_on_startup() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-recovery-{unique}"));

        let writer = Primadb::with_replica_id("recovery-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        crate::engine::set_segment_fault_point_for_test(
            path.clone(),
            crate::engine::SegmentFaultPoint::AfterNodeWrites,
        );
        let failed = writer
            .root("docs")
            .field("crash")
            .put(json!({"value": "survived"}));
        assert!(failed.is_err());
        drop(writer);

        let reader = Primadb::with_replica_id("recovery-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        let report = reader.storage_recovery_report().unwrap_or_default();
        assert_eq!(report.applied_transactions, 1);
        let restored = reader.root("docs").field("crash").once_json()?.unwrap();
        assert_eq!(restored["value"], "survived");

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_files_recover_journal_in_transaction_order() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-segment-order-recovery-{unique}"));

        let writer = Primadb::with_replica_id("recovery-order-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        writer.root("first").field("value").put("one")?;
        crate::engine::set_segment_fault_point_for_test(
            path.clone(),
            crate::engine::SegmentFaultPoint::AfterJournalWrite,
        );
        let failed = writer.root("second").field("value").put("two");
        assert!(failed.is_err());
        drop(writer);

        let _ = std::fs::remove_file(path.join("manifest.json"));
        let _ = std::fs::remove_dir_all(path.join("nodes"));

        let reader = Primadb::with_replica_id("recovery-order-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        let report = reader.storage_recovery_report().unwrap_or_default();
        assert_eq!(report.applied_transactions, 3);
        assert_eq!(
            reader.root("first").field("value").once_json()?,
            Some(json!("one"))
        );
        assert_eq!(
            reader.root("second").field("value").once_json()?,
            Some(json!("two"))
        );

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn record_batch_scans_and_range_deletes_with_segment_pushdown() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-records-{unique}"));

        let writer = Primadb::with_replica_id("record-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        let report = writer.apply_record_batch(RecordBatch {
            preconditions: vec![RecordPrecondition::Absent {
                key: "agentfs/inode/1".to_owned(),
            }],
            mutations: vec![
                RecordMutation::Put {
                    key: "agentfs/inode/1".to_owned(),
                    value: RecordValue::Json(json!({"kind": "dir", "mode": 0o755})),
                },
                RecordMutation::Put {
                    key: "agentfs/dentry/1/README.md".to_owned(),
                    value: RecordValue::Json(json!({"ino": 2})),
                },
                RecordMutation::Put {
                    key: "agentfs/chunk/2/000000".to_owned(),
                    value: RecordValue::Bytes(crate::BinaryBytes::from(b"hello".as_slice())),
                },
                RecordMutation::Put {
                    key: "agentfs/chunk/2/000001".to_owned(),
                    value: RecordValue::Bytes(crate::BinaryBytes::from(b" world".as_slice())),
                },
            ],
        })?;
        assert_eq!(report.preconditions, 1);
        assert_eq!(report.puts, 4);
        drop(writer);

        let corrupt_root = path.join("records").join("by_key");
        let mut corrupt_path = corrupt_root;
        corrupt_path.push(crate::encode_component("zzzzzz"));
        std::fs::create_dir_all(&corrupt_path)?;
        std::fs::write(corrupt_path.join("entry.json"), b"not json")?;

        let reader = Primadb::with_replica_id("record-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        assert_eq!(reader.stats().nodes, 0);

        let chunks = reader.scan_records(RecordScan {
            prefix: Some("agentfs/chunk/2/".to_owned()),
            ..RecordScan::default()
        })?;
        assert_eq!(chunks.entries.len(), 2);
        assert_eq!(reader.stats().nodes, 0);
        let reverse_page = reader.scan_records(RecordScan {
            prefix: Some("agentfs/chunk/2/".to_owned()),
            reverse: true,
            cursor: Some("agentfs/chunk/2/000001".to_owned()),
            ..RecordScan::default()
        })?;
        assert_eq!(reverse_page.entries.len(), 1);
        assert_eq!(reverse_page.entries[0].key, "agentfs/chunk/2/000000");

        reader.transaction(|tx| {
            let dentry = tx
                .get_record("agentfs/dentry/1/README.md")?
                .expect("dentry should exist in transaction");
            assert_eq!(dentry.value, RecordValue::Json(json!({"ino": 2})));
            tx.assert_record_exists("agentfs/inode/1")?;
            tx.assert_record_absent("agentfs/missing")?;
            tx.assert_record_value(
                "agentfs/inode/1",
                &RecordValue::Json(json!({"kind": "dir", "mode": 0o755})),
            )?;
            tx.put_record(
                "agentfs/chunk/2/000002",
                RecordValue::Bytes(crate::BinaryBytes::from(b"!".as_slice())),
            )?;
            tx.delete_record("agentfs/chunk/2/000002")?;
            Ok(())
        })?;

        let failed = reader.apply_record_batch(RecordBatch {
            preconditions: vec![RecordPrecondition::Absent {
                key: "agentfs/inode/1".to_owned(),
            }],
            mutations: vec![RecordMutation::Put {
                key: "agentfs/inode/1".to_owned(),
                value: RecordValue::Json(json!({"kind": "file"})),
            }],
        });
        assert!(matches!(
            failed,
            Err(PrimadbError::TransactionConflict { .. })
        ));
        assert_eq!(
            reader.get_record("agentfs/inode/1")?.unwrap().value,
            RecordValue::Json(json!({"kind": "dir", "mode": 0o755}))
        );

        let delete_report = reader.apply_record_batch(RecordBatch {
            preconditions: vec![RecordPrecondition::Exists {
                key: "agentfs/chunk/2/000000".to_owned(),
            }],
            mutations: vec![RecordMutation::DeleteRange {
                scan: RecordScan {
                    prefix: Some("agentfs/chunk/2/".to_owned()),
                    ..RecordScan::default()
                },
            }],
        })?;
        assert_eq!(delete_report.preconditions, 1);
        assert_eq!(delete_report.range_deletes, 2);
        assert!(reader.sync_storage()?.synced);
        drop(reader);

        let reopened = Primadb::with_replica_id("record-reopened");
        assert!(reopened.use_segment_storage(path.clone(), 8)?);
        let chunks = reopened.scan_records(RecordScan {
            prefix: Some("agentfs/chunk/2/".to_owned()),
            ..RecordScan::default()
        })?;
        assert!(chunks.entries.is_empty());
        let dentry = reopened
            .get_record("agentfs/dentry/1/README.md")?
            .expect("dentry should remain");
        assert_eq!(dentry.value, RecordValue::Json(json!({"ino": 2})));

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_record_scans_page_large_directories_in_both_directions() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-record-scan-pages-{unique}"));

        let writer = Primadb::with_replica_id("record-page-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        for index in 0..128 {
            writer.put_record_json(&format!("records/{index:03}"), json!({"index": index}))?;
        }
        drop(writer);

        let reader = Primadb::with_replica_id("record-page-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        assert_eq!(reader.stats().nodes, 0);

        let overlay = Primadb::with_replica_id("record-page-overlay");
        overlay.put_record_json("records/000", json!({"overlay": true}))?;
        overlay.delete_record("records/001")?;
        overlay.delete_record("records/002")?;
        overlay.put_record_json("records/000.5", json!({"overlay": true}))?;
        let overlay_nodes = overlay.inner.lock().unwrap().nodes.clone();
        {
            let mut inner = reader.inner.lock().unwrap();
            inner.nodes.extend(overlay_nodes);
            inner.rebuild_record_overlay();
        }

        let overlay_page = reader.scan_records(RecordScan {
            prefix: Some("records/".to_owned()),
            limit: Some(2),
            ..RecordScan::default()
        })?;
        assert_eq!(overlay_page.entries[0].key, "records/000");
        assert_eq!(overlay_page.entries[1].key, "records/000.5");
        assert_eq!(overlay_page.next_cursor.as_deref(), Some("records/000.5"));
        {
            let mut inner = reader.inner.lock().unwrap();
            inner.nodes.clear();
            inner.rebuild_record_overlay();
        }

        let forward = reader.scan_records(RecordScan {
            prefix: Some("records/".to_owned()),
            limit: Some(7),
            ..RecordScan::default()
        })?;
        assert_eq!(forward.entries.len(), 7);
        assert_eq!(forward.entries[0].key, "records/000");
        assert_eq!(forward.entries[6].key, "records/006");
        assert_eq!(forward.next_cursor.as_deref(), Some("records/006"));

        let forward_next = reader.scan_records(RecordScan {
            prefix: Some("records/".to_owned()),
            cursor: forward.next_cursor,
            limit: Some(7),
            ..RecordScan::default()
        })?;
        assert_eq!(forward_next.entries[0].key, "records/007");
        assert_eq!(forward_next.entries[6].key, "records/013");

        let reverse = reader.scan_records(RecordScan {
            prefix: Some("records/".to_owned()),
            reverse: true,
            limit: Some(7),
            ..RecordScan::default()
        })?;
        assert_eq!(reverse.entries.len(), 7);
        assert_eq!(reverse.entries[0].key, "records/127");
        assert_eq!(reverse.entries[6].key, "records/121");
        assert_eq!(reverse.next_cursor.as_deref(), Some("records/121"));

        let reverse_next = reader.scan_records(RecordScan {
            prefix: Some("records/".to_owned()),
            reverse: true,
            cursor: reverse.next_cursor,
            limit: Some(7),
            ..RecordScan::default()
        })?;
        assert_eq!(reverse_next.entries[0].key, "records/120");
        assert_eq!(reverse_next.entries[6].key, "records/114");

        let range = reader.scan_records(RecordScan {
            prefix: Some("records/".to_owned()),
            start_after: Some("records/050".to_owned()),
            end_before: Some("records/055".to_owned()),
            limit: Some(2),
            ..RecordScan::default()
        })?;
        assert_eq!(
            range
                .entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            ["records/051", "records/052"]
        );
        assert_eq!(range.next_cursor.as_deref(), Some("records/052"));

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn storage_record_page_does_not_scan_unrelated_loaded_nodes() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-record-overlay-index-{unique}"));

        let writer = Primadb::with_replica_id("record-overlay-index-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        for index in 0..256 {
            writer.put_record_json(&format!("page/{index:04}"), json!({"index": index}))?;
        }
        drop(writer);

        let reader = Primadb::with_replica_id("record-overlay-index-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        {
            let mut inner = reader.inner.lock().unwrap();
            for index in 0..10_000 {
                let node_id = format!("loaded-graph-node/{index:05}");
                inner.nodes.insert(node_id.clone(), NodeState::new(node_id));
            }
            inner.record_overlay_candidates_examined = 0;
        }
        for index in 0..256 {
            assert!(reader.get_record(&format!("page/{index:04}"))?.is_some());
        }
        reader
            .inner
            .lock()
            .unwrap()
            .record_overlay_candidates_examined = 0;

        let page = reader.scan_records(RecordScan {
            prefix: Some("page/".to_owned()),
            limit: Some(4),
            ..RecordScan::default()
        })?;
        assert_eq!(page.entries.len(), 4);
        assert_eq!(page.entries[0].key, "page/0000");
        assert_eq!(page.entries[3].key, "page/0003");
        assert_eq!(page.next_cursor.as_deref(), Some("page/0003"));
        assert_eq!(
            reader
                .inner
                .lock()
                .unwrap()
                .record_overlay_candidates_examined,
            0
        );

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn segment_files_persist_large_record_keys_without_filename_limit() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("primadb-record-long-key-{unique}"));
        let long_key = format!("agentfs/long/{}", "x".repeat(8192));
        let long_prefix = format!("agentfs/long/{}", "x".repeat(300));

        let writer = Primadb::with_replica_id("long-record-key-writer");
        assert!(!writer.use_segment_storage(path.clone(), 8)?);
        writer.put_record_json(&long_key, json!({"ok": true}))?;
        drop(writer);

        let reader = Primadb::with_replica_id("long-record-key-reader");
        assert!(reader.use_segment_storage(path.clone(), 8)?);
        let restored = reader.get_record(&long_key)?.expect("long key should load");
        assert_eq!(restored.value, RecordValue::Json(json!({"ok": true})));
        let page = reader.scan_records(RecordScan {
            prefix: Some(long_prefix),
            ..RecordScan::default()
        })?;
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].key, long_key);

        let _ = std::fs::remove_dir_all(path);
        Ok(())
    }

    #[test]
    fn external_incremental_storage_keeps_ops_until_confirmed() -> Result<()> {
        let db = Primadb::with_replica_id("browser-durable");
        db.register_external_storage_hook();

        db.root("checkpoint")
            .field("active")
            .put("x".repeat(4096))?;
        let first = db.incremental_storage_transaction();
        assert_eq!(first.journal_ops.len(), 1);

        db.root("checkpoint").field("next").put("y".repeat(4096))?;
        db.mark_storage_transaction_flushed(&first)?;

        let second = db.incremental_storage_transaction();
        assert_eq!(second.journal_ops.len(), 1);
        assert!(second.nodes.len() <= 2);
        db.mark_storage_transaction_flushed(&second)?;

        let empty = db.incremental_storage_transaction();
        assert!(empty.journal_ops.is_empty());
        assert!(empty.nodes.is_empty());
        db.unregister_external_storage_hook();
        Ok(())
    }

    #[test]
    fn incremental_storage_transactions_are_bounded_for_repeated_large_updates() -> Result<()> {
        let db = Primadb::with_replica_id("bounded-browser-durable");
        db.register_external_storage_hook();

        for index in 0..32 {
            db.root("docs")
                .field(format!("doc-{index}"))
                .put(json!({"title": format!("Document {index}")}))?;
        }
        let initial = db.full_storage_transaction();
        let full_node_count = initial.nodes.len();
        assert!(full_node_count >= 32);
        db.mark_storage_transaction_flushed(&initial)?;

        for version in 0..8 {
            db.root("checkpoint")
                .field("active")
                .put(format!("{version}:{}", "z".repeat(64 * 1024)))?;
            let transaction = db.incremental_storage_transaction();
            assert_eq!(transaction.journal_ops.len(), 1);
            assert!(
                transaction.nodes.len() < full_node_count / 4,
                "incremental transaction rewrote too many nodes: {} of {full_node_count}",
                transaction.nodes.len()
            );
            db.mark_storage_transaction_flushed(&transaction)?;
        }

        db.unregister_external_storage_hook();
        Ok(())
    }

    #[test]
    fn pending_and_unflushed_operations_compact_repeated_large_field_updates() -> Result<()> {
        let db = Primadb::with_replica_id("compact-large-updates");
        db.register_external_storage_hook();

        for version in 0..16 {
            db.root("checkpoint")
                .field("active")
                .put(format!("{version}:{}", "q".repeat(64 * 1024)))?;
        }

        assert_eq!(db.pending_operations().len(), 1);
        let transaction = db.incremental_storage_transaction();
        assert_eq!(transaction.journal_ops.len(), 1);
        assert_eq!(transaction.metadata.pending_ops.len(), 1);
        let browser_transaction = db.incremental_storage_transaction_without_pending_ops();
        assert_eq!(browser_transaction.journal_ops.len(), 1);
        assert!(browser_transaction.metadata.pending_ops.is_empty());
        db.mark_storage_transaction_flushed(&transaction)?;
        assert_eq!(db.pending_operations().len(), 1);
        db.drain_pending_operations()?;
        assert!(db.pending_operations().is_empty());

        db.unregister_external_storage_hook();
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn vacuum_storage_removes_orphaned_segment_and_blob_entries() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let storage_root = std::env::temp_dir().join(format!("primadb-vacuum-store-{unique}"));
        let blob_root = std::env::temp_dir().join(format!("primadb-vacuum-blobs-{unique}"));

        let db = Primadb::with_replica_id("vacuum-a");
        assert!(!db.use_segment_storage(storage_root.clone(), 8)?);
        db.open_blob_storage(crate::BlobStorageConfig::Files {
            directory: blob_root.display().to_string(),
            durability: Default::default(),
        })?;
        let live_blob = db
            .root("assets")
            .field("keep")
            .put_blob(b"keep-me".to_vec(), Some("application/octet-stream"))?;
        db.root("docs")
            .field("hello")
            .put(json!({"value": "world"}))?;

        let orphan_node = storage_root
            .join("nodes")
            .join(format!("{}.json", crate::encode_component("orphan/node")));
        let orphan_auth = storage_root
            .join("auth")
            .join(format!("{}.json", crate::encode_component("orphan/node")));
        let orphan_manifest = storage_root
            .join("node_indexes")
            .join(format!("{}.json", crate::encode_component("orphan/node")));
        std::fs::write(&orphan_node, b"{}")?;
        std::fs::write(&orphan_auth, b"{}")?;
        std::fs::write(&orphan_manifest, b"{}")?;

        let stale_key = crate::direct_index_key("ghost.path", "s_dead", "orphan/node");
        let mut stale_index = storage_root.join("indexes");
        for segment in stale_key.split('/') {
            stale_index.push(segment);
        }
        stale_index.set_extension("json");
        if let Some(parent) = stale_index.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&stale_index, b"{}")?;

        let orphan_blob_dir = blob_root.join("blobs").join("sha256_deadbeef");
        std::fs::create_dir_all(&orphan_blob_dir)?;
        std::fs::write(orphan_blob_dir.join("meta.json"), b"{}")?;
        std::fs::write(orphan_blob_dir.join("data.bin"), b"orphan")?;

        let report = db.vacuum_storage()?;
        assert!(report.storage.removed_node_files >= 1);
        assert!(report.storage.removed_auth_files >= 1);
        assert!(report.storage.removed_index_manifests >= 1);
        assert!(report.storage.removed_direct_index_files >= 1);
        assert!(report.removed_blob_entries >= 1);

        assert!(!orphan_node.exists());
        assert!(!orphan_auth.exists());
        assert!(!orphan_manifest.exists());
        assert!(!stale_index.exists());
        assert!(!orphan_blob_dir.exists());
        assert!(db.get_blob(&live_blob.id)?.is_some());

        let _ = std::fs::remove_dir_all(storage_root);
        let _ = std::fs::remove_dir_all(blob_root);
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
        local
            .root("status")
            .field("message")
            .put(json!("keep-local"))?;
        local.merge_snapshot(snapshot)?;

        assert_eq!(local.replica_id(), "local");
        assert_eq!(
            local.root("status").field("message").once_json()?.unwrap(),
            json!("keep-local")
        );

        let merged_notes = local
            .root("rooms")
            .field("lobby")
            .field("notes")
            .query(QuerySpec {
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

    #[derive(Default)]
    struct TestHooks;

    impl NetworkHooks for TestHooks {
        fn on_connect(&self, context: &ConnectHookContext) -> HookDecision<()> {
            if context
                .peer
                .metadata
                .get("deny_connect")
                .is_some_and(|value| value == "true")
            {
                HookDecision::deny("peer denied by connect hook")
            } else {
                HookDecision::allow(())
            }
        }

        fn on_join_room(&self, context: &RoomHookContext) -> HookDecision<()> {
            if context.room == "private-room" {
                HookDecision::deny("room denied by room hook")
            } else {
                HookDecision::allow(())
            }
        }

        fn on_pull(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
            match &context.request {
                PullRequestKind::Get { path } if path.anchor == "private" => {
                    HookDecision::deny("private root denied")
                }
                PullRequestKind::Query { path, spec } if path.anchor == "rooms" => {
                    let mut spec = spec.clone();
                    spec.limit = Some(1);
                    HookDecision::allow(PullRequestKind::Query {
                        path: path.clone(),
                        spec,
                    })
                }
                _ => HookDecision::allow(context.request.clone()),
            }
        }

        fn on_watch(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
            match &context.request {
                PullRequestKind::Get { path } if path.anchor == "private" => {
                    HookDecision::deny("private watch denied")
                }
                _ => HookDecision::allow(context.request.clone()),
            }
        }

        fn on_serve_result(
            &self,
            _context: &ServeResultContext,
            result: crate::RemoteResult,
        ) -> HookDecision<crate::RemoteResult> {
            match result {
                crate::RemoteResult::Get { .. } => HookDecision::allow(crate::RemoteResult::Get {
                    value: Some(json!({"masked": true})),
                }),
                other => HookDecision::allow(other),
            }
        }
    }

    #[derive(Default)]
    struct VerifiedOnlyHooks;

    impl NetworkHooks for VerifiedOnlyHooks {
        fn on_pull(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
            if context
                .verified_identity
                .as_ref()
                .and_then(|identity| identity.alias.as_deref())
                == Some("team-a")
            {
                HookDecision::allow(context.request.clone())
            } else {
                HookDecision::deny("verified team identity required")
            }
        }
    }

    #[test]
    fn network_hooks_can_gate_peer_discovery_and_room_joins() {
        let db = Primadb::with_replica_id("hooks-a");
        db.set_network_hooks(Arc::new(TestHooks));

        let denied_peer = PeerPresence {
            peer_id: "peer-denied".to_owned(),
            replica_id: "peer-denied".to_owned(),
            transport: "websocket".to_owned(),
            identity: None,
            capabilities: Vec::new(),
            topics: Vec::new(),
            metadata: [("deny_connect".to_owned(), "true".to_owned())]
                .into_iter()
                .collect(),
        };
        assert!(matches!(
            db.allow_peer_connection(&ConnectHookContext {
                peer: denied_peer,
                transport: HookTransport::Relay,
                relay_url: Some("ws://127.0.0.1:9010".to_owned()),
                verified_identity: None,
            }),
            HookDecision::Deny { .. }
        ));

        assert!(matches!(
            db.allow_room_join(&RoomHookContext {
                peer_id: "peer-1".to_owned(),
                room: "private-room".to_owned(),
                transport: HookTransport::Mesh,
                peer: None,
                verified_identity: None,
            }),
            HookDecision::Deny { .. }
        ));

        db.clear_network_hooks();
        let allowed_peer = PeerPresence {
            peer_id: "peer-ok".to_owned(),
            replica_id: "peer-ok".to_owned(),
            transport: "websocket".to_owned(),
            identity: None,
            capabilities: Vec::new(),
            topics: Vec::new(),
            metadata: Default::default(),
        };
        assert!(matches!(
            db.allow_peer_connection(&ConnectHookContext {
                peer: allowed_peer,
                transport: HookTransport::Relay,
                relay_url: None,
                verified_identity: None,
            }),
            HookDecision::Allow { .. }
        ));
    }

    #[test]
    fn network_hooks_can_rewrite_pull_requests_and_redact_results() -> Result<()> {
        let db = Primadb::with_replica_id("hooks-b");
        db.set_network_hooks(Arc::new(TestHooks));

        let notes = db.root("rooms").field("lobby").field("notes");
        for index in 0..3 {
            notes.set(json!({
                "title": format!("Note {index}"),
                "created_at": index,
            }))?;
        }
        db.root("docs").field("secret").put(json!("top-secret"))?;

        let query = db.serve_pull_request_for_peer(
            "peer-1",
            HookTransport::Relay,
            "query-1",
            &PullRequestKind::Query {
                path: RemotePath::new("rooms", vec!["lobby".to_owned(), "notes".to_owned()]),
                spec: QuerySpec {
                    filters: Vec::new(),
                    order: Some(crate::QueryOrder {
                        path: "created_at".to_owned(),
                        direction: QueryDirection::Asc,
                    }),
                    limit: None,
                    offset: 0,
                },
            },
            None,
        )?;
        match query {
            HookDecision::Allow {
                value: crate::RemoteResult::Query { entries },
            } => assert_eq!(entries.len(), 1),
            other => panic!("unexpected query result: {other:?}"),
        }

        let redacted = db.serve_pull_request_for_peer(
            "peer-1",
            HookTransport::Relay,
            "get-1",
            &PullRequestKind::Get {
                path: RemotePath::new("docs", vec!["secret".to_owned()]),
            },
            None,
        )?;
        match redacted {
            HookDecision::Allow {
                value: crate::RemoteResult::Get { value },
            } => assert_eq!(value, Some(json!({"masked": true}))),
            other => panic!("unexpected get result: {other:?}"),
        }

        let denied = db.serve_pull_request_for_peer(
            "peer-1",
            HookTransport::Relay,
            "get-2",
            &PullRequestKind::Get {
                path: RemotePath::new("private", vec!["secret".to_owned()]),
            },
            None,
        )?;
        assert!(matches!(denied, HookDecision::Deny { .. }));
        Ok(())
    }

    #[test]
    fn network_hooks_receive_verified_identity_context() -> Result<()> {
        let db = Primadb::with_replica_id("hooks-verified");
        db.root("docs").field("public").put(json!("visible"))?;
        db.set_network_hooks(Arc::new(VerifiedOnlyHooks));

        let request = PullRequestKind::Get {
            path: RemotePath::new("docs", vec!["public".to_owned()]),
        };
        let denied = db.serve_pull_request_for_peer(
            "peer-1",
            HookTransport::Relay,
            "get-1",
            &request,
            None,
        )?;
        assert!(matches!(denied, HookDecision::Deny { .. }));

        let verified_identity = crate::VerifiedIdentity {
            public_key: "public-key".to_owned(),
            alias: Some("team-a".to_owned()),
            peer_id: "peer-1".to_owned(),
            replica_id: "replica-1".to_owned(),
            transport: "relay".to_owned(),
            session_id: "session-1".to_owned(),
            claims: Default::default(),
            issued_at_millis: 1,
            expires_at_millis: None,
            trust: crate::IdentityTrust::Verified,
        };
        let allowed = db.serve_pull_request_for_peer(
            "peer-1",
            HookTransport::Relay,
            "get-2",
            &request,
            Some(&verified_identity),
        )?;
        assert!(matches!(allowed, HookDecision::Allow { .. }));
        Ok(())
    }

    #[test]
    fn network_hooks_can_gate_watch_requests_and_redact_watch_results() -> Result<()> {
        let db = Primadb::with_replica_id("hooks-c");
        db.set_network_hooks(Arc::new(TestHooks));
        db.root("docs").field("public").put(json!("visible"))?;

        let denied = db.authorize_watch_request_for_peer(
            "peer-1",
            HookTransport::Relay,
            "watch-1",
            &PullRequestKind::Get {
                path: RemotePath::new("private", vec!["secret".to_owned()]),
            },
            None,
        );
        assert!(matches!(denied, HookDecision::Deny { .. }));

        let allowed = db.authorize_watch_request_for_peer(
            "peer-1",
            HookTransport::Relay,
            "watch-2",
            &PullRequestKind::Get {
                path: RemotePath::new("docs", vec!["public".to_owned()]),
            },
            None,
        );
        assert!(matches!(allowed, HookDecision::Allow { .. }));

        let result = db.serve_watch_result_for_peer(
            "peer-1",
            HookTransport::Relay,
            "watch-2",
            &PullRequestKind::Get {
                path: RemotePath::new("docs", vec!["public".to_owned()]),
            },
            true,
            None,
        )?;
        match result {
            HookDecision::Allow {
                value: crate::RemoteResult::Get { value },
            } => assert_eq!(value, Some(json!({"masked": true}))),
            other => panic!("unexpected watch result: {other:?}"),
        }
        Ok(())
    }
}
