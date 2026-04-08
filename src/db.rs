use crate::clock::{HybridClock, Revision, VersionMarker};
use crate::error::{PrimadbError, Result};
use crate::hardening::{PrimadbLimits, PrimadbStats};
use crate::operation::{Operation, OperationAction, OperationValue};
use crate::persistence::{PersistenceTarget, load_snapshot_payload, store_snapshot_payload};
use crate::query::{LexEntry, LexSpec, QueryDirection, QueryFilter, QuerySpec};
use crate::snapshot::DatabaseSnapshot;
use crate::storage::StorageAdapter;
use crate::sync::{SyncEnvelope, SyncFrame};
use crate::value::{FieldState, FieldValue, NodeId, NodeState, SetState};
#[cfg(feature = "crypto")]
use crate::{
    Identity, PublicIdentity, SecretBoxKey, SecureSyncFrame, SecurityState, StoredSnapshot,
    UserGrant,
};
use async_channel::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as JsonValue};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

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
        let inner = self.inner.lock().unwrap();
        DatabaseSnapshot {
            clock: inner.clock.clone(),
            nodes: inner.nodes.clone(),
            pending_ops: inner.pending_ops.clone(),
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
        inner.security.encode_sync_frame(inner.clock.actor(), roots, frame)
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
        let adapter = crate::storage::RadiskFileAdapter::new(
            directory,
            self.replica_id(),
            compaction_threshold,
        );
        self.attach_storage_adapter(Arc::new(adapter))
    }

    #[cfg(target_arch = "wasm32")]
    pub fn use_browser_storage(&self, key: impl Into<String>) -> Result<bool> {
        let target = PersistenceTarget::BrowserStorage(key.into());
        self.configure_persistence(target)
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
        self.root(format!("~{public_key}")).put(serde_json::json!({
            "alias": alias,
            "pub": public_key,
        }))
    }

    #[cfg(feature = "crypto")]
    pub fn set_snapshot_encryption_key(&self, key: SecretBoxKey) {
        self.inner.lock().unwrap().security.set_snapshot_encryption_key(key);
    }

    #[cfg(feature = "crypto")]
    pub fn set_transport_encryption_key(&self, key: SecretBoxKey) {
        self.inner.lock().unwrap().security.set_transport_encryption_key(key);
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
        let (target, adapter, snapshot, unflushed_ops) = {
            let inner = self.inner.lock().unwrap();
            (
                inner.persistence.clone(),
                inner.storage_adapter.clone(),
                DatabaseSnapshot {
                    clock: inner.clock.clone(),
                    nodes: inner.nodes.clone(),
                    pending_ops: inner.pending_ops.clone(),
                },
                inner.unflushed_ops.clone(),
            )
        };

        if let Some(target) = target {
            store_snapshot_payload(&target, &self.export_persisted_snapshot_json()?)?;
        }

        if let Some(adapter) = adapter {
            adapter.flush(&unflushed_ops, &snapshot)?;
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
        let inner = self.inner.lock().unwrap();
        match inner.resolve_cursor(anchor, segments)? {
            Some(Cursor::Node(node)) => {
                Ok(Some(inner.materialize_node(&node, &mut BTreeSet::new())))
            }
            Some(Cursor::Field { node, field }) => {
                let node_state = match inner.nodes.get(&node) {
                    Some(node_state) => node_state,
                    None => return Ok(None),
                };
                let field_state = match node_state.fields.get(&field) {
                    Some(field_state) => field_state,
                    None => return Ok(None),
                };
                Ok(Some(inner.materialize_field(
                    &field_state.value,
                    &mut BTreeSet::new(),
                )))
            }
            None => Ok(None),
        }
    }

    fn map_at(&self, anchor: &str, segments: &[String]) -> Result<Vec<MapEntry>> {
        let inner = self.inner.lock().unwrap();
        match inner.resolve_cursor(anchor, segments)? {
            Some(Cursor::Node(node)) => Ok(inner.map_node(&node)),
            Some(Cursor::Field { node, field }) => {
                let Some(node_state) = inner.nodes.get(&node) else {
                    return Ok(Vec::new());
                };
                let Some(field_state) = node_state.fields.get(&field) else {
                    return Ok(Vec::new());
                };
                match &field_state.value {
                    FieldValue::Link(target) => Ok(inner.map_node(target)),
                    FieldValue::Set(set) => Ok(set
                        .members
                        .keys()
                        .map(|member| MapEntry {
                            key: member.clone(),
                            value: inner.materialize_node(member, &mut BTreeSet::new()),
                        })
                        .collect()),
                    FieldValue::Scalar(_) => Ok(Vec::new()),
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
        let mut entries = self.map_at(anchor, segments)?;
        entries.retain(|entry| {
            spec.filters
                .iter()
                .all(|filter| matches_filter(entry, filter))
        });

        if let Some(order) = &spec.order {
            entries.sort_by(|left, right| compare_entries(left, right, order));
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

    fn scan_at(&self, anchor: &str, segments: &[String], spec: &LexSpec) -> Result<Vec<LexEntry>> {
        let inner = self.inner.lock().unwrap();
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
            let inner = self.inner.lock().unwrap();
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

    pub fn once_json(&self) -> Result<Option<JsonValue>> {
        self.db.materialize(&self.anchor, &self.segments)
    }

    pub fn unset(&self) -> Result<()> {
        self.db.unset(&self.anchor, &self.segments)
    }

    pub fn set<T: Serialize>(&self, value: T) -> Result<String> {
        self.db
            .set_json(&self.anchor, &self.segments, serde_json::to_value(value)?)
    }

    pub fn remove<T: Serialize>(&self, value: T) -> Result<String> {
        self.db
            .remove_json(&self.anchor, &self.segments, serde_json::to_value(value)?)
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
    fn ensure_node(&mut self, node: &str) {
        self.nodes
            .entry(node.to_owned())
            .or_insert_with(|| NodeState::new(node.to_owned()));
    }

    fn ensure_field_cursor(&mut self, anchor: &str, segments: &[String]) -> Result<Cursor> {
        if segments.is_empty() {
            return Ok(Cursor::Node(anchor.to_owned()));
        }

        self.ensure_node(anchor);
        let mut current = anchor.to_owned();
        for segment in &segments[..segments.len().saturating_sub(1)] {
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

    fn resolve_cursor(&self, anchor: &str, segments: &[String]) -> Result<Option<Cursor>> {
        if segments.is_empty() {
            return Ok(self
                .nodes
                .contains_key(anchor)
                .then(|| Cursor::Node(anchor.to_owned())));
        }

        let mut current = anchor.to_owned();
        let Some(_) = self.nodes.get(&current) else {
            return Ok(None);
        };
        for segment in &segments[..segments.len().saturating_sub(1)] {
            let Some(node) = self.nodes.get(&current) else {
                return Ok(None);
            };
            let Some(field) = node.fields.get(segment) else {
                return Ok(None);
            };
            match &field.value {
                FieldValue::Link(target) => current = target.clone(),
                FieldValue::Scalar(_) => {
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
            ParsedInput::Scalar(_) | ParsedInput::Set(_) => {
                return Err(PrimadbError::InvalidSetMember {
                    path: path.to_owned(),
                });
            }
        };

        self.add_set_member(node.to_owned(), field.to_owned(), member_id.clone());
        Ok(member_id)
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
                    OperationValue::Scalar(_) => Vec::new(),
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

    fn materialize_node(&self, node: &str, visited: &mut BTreeSet<NodeId>) -> JsonValue {
        if !visited.insert(node.to_owned()) {
            return JsonValue::Object(Map::from_iter([(
                "$ref".to_owned(),
                JsonValue::String(node.to_owned()),
            )]));
        }

        let output = if let Some(state) = self.nodes.get(node) {
            let mut object = Map::new();
            object.insert("$id".to_owned(), JsonValue::String(state.id.clone()));
            for (field, state) in &state.fields {
                object.insert(field.clone(), self.materialize_field(&state.value, visited));
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

    fn materialize_field(&self, value: &FieldValue, visited: &mut BTreeSet<NodeId>) -> JsonValue {
        match value {
            FieldValue::Scalar(value) => value.clone(),
            FieldValue::Link(target) => self.materialize_node(target, visited),
            FieldValue::Set(set) => JsonValue::Object(Map::from_iter([(
                "$set".to_owned(),
                JsonValue::Array(
                    set.members
                        .keys()
                        .map(|member| self.materialize_node(member, visited))
                        .collect(),
                ),
            )])),
        }
    }

    fn map_node(&self, node: &str) -> Vec<MapEntry> {
        self.nodes
            .get(node)
            .map(|state| {
                state
                    .fields
                    .iter()
                    .map(|(field, state)| MapEntry {
                        key: field.clone(),
                        value: self.materialize_field(&state.value, &mut BTreeSet::new()),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn collect_lex_from_node(
        &self,
        node: &str,
        base_path: &str,
        spec: &LexSpec,
        remaining_depth: usize,
        output: &mut Vec<LexEntry>,
    ) {
        let Some(state) = self.nodes.get(node) else {
            return;
        };
        for (field, field_state) in &state.fields {
            if !lex_key_matches(field, spec) {
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
                value: self.materialize_field(&field_state.value, &mut BTreeSet::new()),
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
                FieldValue::Scalar(_) => {}
            }
        }
    }

    fn collect_lex_from_field(
        &self,
        node: &str,
        field: &str,
        base_path: &str,
        spec: &LexSpec,
        remaining_depth: usize,
        output: &mut Vec<LexEntry>,
    ) {
        let Some(state) = self.nodes.get(node) else {
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
                        value: self.materialize_node(member, &mut BTreeSet::new()),
                    });
                    if spec.follow_links && remaining_depth > 1 {
                        self.collect_lex_from_node(member, &path, spec, remaining_depth - 1, output);
                    }
                }
            }
            FieldValue::Scalar(_) => {}
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

fn parse_set_marker(object: &Map<String, JsonValue>) -> Option<Vec<JsonValue>> {
    if object.len() != 1 {
        return None;
    }
    object.get("$set").and_then(|value| match value {
        JsonValue::Array(items) => Some(items.clone()),
        _ => None,
    })
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
    use crate::{QueryDirection, Result};
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
        assert!(deep.iter().any(|entry| entry.path.ends_with("profile.city")));
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
}
