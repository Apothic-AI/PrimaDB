use crate::RouteTransportKind;
use crate::value::{NodeId, NodeState};
use crate::{
    DatabaseSnapshot, HybridClock, LexEntry, LexSpec, MapEntry, Operation, QuerySpec, RecordEntry,
    RecordScan, RecordScanResult, ScopePolicy, TransactionOptions, TransactionReport,
    TransactionStep, VectorSearchResult, VectorSearchSpec,
};
use async_channel::Receiver;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SyncEnvelope {
    pub from: String,
    pub ops: Vec<Operation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SyncFrame {
    Sync {
        from: String,
        message_id: String,
        ops: Vec<Operation>,
    },
    Ack {
        from: String,
        message_id: String,
        applied: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemotePath {
    pub anchor: String,
    #[serde(default)]
    pub segments: Vec<String>,
}

impl RemotePath {
    pub fn new(anchor: impl Into<String>, segments: Vec<String>) -> Self {
        Self {
            anchor: anchor.into(),
            segments,
        }
    }

    pub fn path(&self) -> String {
        if self.segments.is_empty() {
            self.anchor.clone()
        } else {
            format!("{}/{}", self.anchor, self.segments.join("/"))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PullRequestKind {
    Get {
        path: RemotePath,
    },
    Map {
        path: RemotePath,
    },
    Query {
        path: RemotePath,
        spec: QuerySpec,
    },
    Lex {
        path: RemotePath,
        spec: LexSpec,
    },
    Records {
        scan: RecordScan,
    },
    VectorSearch {
        collection: String,
        query: Vec<f32>,
        spec: VectorSearchSpec,
    },
    Node {
        id: NodeId,
    },
    Snapshot {
        root: Option<String>,
    },
    Transaction {
        scope: String,
        #[serde(default)]
        steps: Vec<TransactionStep>,
        #[serde(default)]
        options: TransactionOptions,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PullRequest {
    pub request_id: String,
    pub request: PullRequestKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteInterestPolicy {
    #[serde(default)]
    pub target: RemoteInterestTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peers: Vec<String>,
    #[serde(default)]
    pub require_capability: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RemoteInterestTarget {
    #[default]
    Any,
    Peer,
    Peers,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PullChunk {
    pub index: u32,
    pub total: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PullResponseBody {
    Get {
        value: Option<JsonValue>,
    },
    Map {
        entries: Vec<MapEntry>,
    },
    Query {
        entries: Vec<MapEntry>,
    },
    Lex {
        entries: Vec<LexEntry>,
    },
    Records {
        entries: Vec<RecordEntry>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_cursor: Option<String>,
    },
    VectorSearch {
        result: VectorSearchResult,
    },
    Node {
        node: Option<NodeState>,
    },
    Snapshot {
        #[serde(default)]
        clock: Option<HybridClock>,
        #[serde(default)]
        nodes: BTreeMap<NodeId, NodeState>,
        #[serde(default)]
        pending_ops: Vec<Operation>,
        #[serde(default)]
        scope_policies: BTreeMap<String, ScopePolicy>,
    },
    Transaction {
        report: TransactionReport,
    },
    Error {
        message: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PullResponse {
    pub request_id: String,
    pub chunk: PullChunk,
    pub done: bool,
    pub result: PullResponseBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WatchRequestKind {
    Subscribe { request: PullRequestKind },
    Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WatchRequest {
    pub watch_id: String,
    pub request: WatchRequestKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WatchEvent {
    pub watch_id: String,
    pub sequence: u64,
    pub initial: bool,
    pub chunk: PullChunk,
    pub done: bool,
    pub result: PullResponseBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteResult {
    Get { value: Option<JsonValue> },
    Map { entries: Vec<MapEntry> },
    Query { entries: Vec<MapEntry> },
    Lex { entries: Vec<LexEntry> },
    Records { result: RecordScanResult },
    VectorSearch { result: VectorSearchResult },
    Node { node: Option<NodeState> },
    Snapshot { snapshot: DatabaseSnapshot },
    Transaction { report: TransactionReport },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoteWatchMessage {
    pub initial: bool,
    pub result: RemoteResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemotePeerFailure {
    pub peer_id: String,
    pub transport: RouteTransportKind,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemotePeerRecords {
    pub peer_id: String,
    pub transport: RouteTransportKind,
    pub result: RecordScanResult,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteRecordConflictSource {
    pub peer_id: String,
    pub transport: RouteTransportKind,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteRecordConflict {
    pub key: String,
    pub winner_peer_id: String,
    pub winner_hash: String,
    pub sources: Vec<RemoteRecordConflictSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteRecordsFanIn {
    pub request_id: String,
    pub records: Vec<RemotePeerRecords>,
    pub failures: Vec<RemotePeerFailure>,
    pub merged: RecordScanResult,
    pub conflicts: Vec<RemoteRecordConflict>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RemoteFanInWatchEvent {
    Update {
        peer_id: String,
        transport: RouteTransportKind,
        initial: bool,
        sequence: u64,
        result: RemoteResult,
    },
    Failure {
        peer_id: String,
        transport: RouteTransportKind,
        message: String,
        terminal: bool,
    },
    Closed,
}

pub struct RemoteWatchSubscription {
    inner: Arc<RemoteWatchSubscriptionInner>,
}

pub struct RemoteFanInWatch {
    inner: Arc<RemoteFanInWatchInner>,
}

struct RemoteWatchSubscriptionInner {
    receiver: Receiver<std::result::Result<RemoteWatchMessage, String>>,
    cancel: Box<dyn Fn() + Send + Sync>,
}

struct RemoteFanInWatchInner {
    receiver: Receiver<RemoteFanInWatchEvent>,
    cancel: Box<dyn Fn() + Send + Sync>,
}

impl PullRequestKind {
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Get { .. } => "get",
            Self::Map { .. } => "map",
            Self::Query { .. } => "query",
            Self::Lex { .. } => "lex",
            Self::Records { .. } => "records",
            Self::VectorSearch { .. } => "vector_search",
            Self::Node { .. } => "node",
            Self::Snapshot { .. } => "snapshot",
            Self::Transaction { .. } => "transaction",
        }
    }

    pub fn interest_path(&self) -> Option<String> {
        match self {
            Self::Get { path }
            | Self::Map { path }
            | Self::Query { path, .. }
            | Self::Lex { path, .. } => Some(path.path()),
            Self::Records { .. } => None,
            Self::VectorSearch { collection, .. } => {
                Some(format!("__primadb_vectors/{collection}"))
            }
            Self::Node { id } => Some(id.clone()),
            Self::Snapshot { root } => root.clone(),
            Self::Transaction { scope, .. } => Some(scope.clone()),
        }
    }
}

impl Default for RemoteInterestPolicy {
    fn default() -> Self {
        Self {
            target: RemoteInterestTarget::Any,
            peer_id: None,
            peers: Vec::new(),
            require_capability: false,
        }
    }
}

impl RemoteInterestPolicy {
    pub fn any() -> Self {
        Self::default()
    }

    pub fn peer(peer_id: impl Into<String>) -> Self {
        Self {
            target: RemoteInterestTarget::Peer,
            peer_id: Some(peer_id.into()),
            peers: Vec::new(),
            require_capability: false,
        }
    }

    pub fn peers(peer_ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            target: RemoteInterestTarget::Peers,
            peer_id: None,
            peers: peer_ids.into_iter().map(Into::into).collect(),
            require_capability: false,
        }
    }
}

impl PullResponse {
    pub fn is_final(&self) -> bool {
        self.done || self.chunk.index.saturating_add(1) >= self.chunk.total
    }
}

impl RemoteWatchSubscription {
    #[cfg(any(test, feature = "native-websocket"))]
    pub(crate) fn new(
        receiver: Receiver<std::result::Result<RemoteWatchMessage, String>>,
        cancel: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(RemoteWatchSubscriptionInner {
                receiver,
                cancel: Box::new(cancel),
            }),
        }
    }

    pub fn receiver(&self) -> Receiver<std::result::Result<RemoteWatchMessage, String>> {
        self.inner.receiver.clone()
    }

    pub async fn recv(&self) -> Option<std::result::Result<RemoteWatchMessage, String>> {
        self.inner.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<std::result::Result<RemoteWatchMessage, String>> {
        self.inner.receiver.try_recv().ok()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<std::result::Result<RemoteWatchMessage, String>> {
        self.inner.receiver.recv_blocking().ok()
    }

    pub fn close(&self) {
        (self.inner.cancel)();
    }
}

impl RemoteFanInWatch {
    #[cfg(any(test, feature = "native-websocket"))]
    pub(crate) fn new(
        receiver: Receiver<RemoteFanInWatchEvent>,
        cancel: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: Arc::new(RemoteFanInWatchInner {
                receiver,
                cancel: Box::new(cancel),
            }),
        }
    }

    pub fn receiver(&self) -> Receiver<RemoteFanInWatchEvent> {
        self.inner.receiver.clone()
    }

    pub async fn recv(&self) -> Option<RemoteFanInWatchEvent> {
        self.inner.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<RemoteFanInWatchEvent> {
        self.inner.receiver.try_recv().ok()
    }

    pub fn drain(&self) -> Vec<RemoteFanInWatchEvent> {
        let mut events = Vec::new();
        while let Ok(event) = self.inner.receiver.try_recv() {
            events.push(event);
        }
        events
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<RemoteFanInWatchEvent> {
        self.inner.receiver.recv_blocking().ok()
    }

    pub fn close(&self) {
        (self.inner.cancel)();
        self.inner.receiver.close();
    }
}

pub fn merge_remote_records_fan_in(
    request_id: impl Into<String>,
    records: Vec<RemotePeerRecords>,
    failures: Vec<RemotePeerFailure>,
) -> RemoteRecordsFanIn {
    #[derive(Debug, Clone)]
    struct SourceRecord {
        peer_id: String,
        transport: RouteTransportKind,
        entry: RecordEntry,
        content_hash: String,
    }

    let mut by_key: BTreeMap<String, Vec<SourceRecord>> = BTreeMap::new();
    let mut cursors = BTreeMap::new();
    for peer_records in &records {
        if let Some(cursor) = &peer_records.result.next_cursor {
            cursors.insert(peer_records.peer_id.clone(), cursor.clone());
        }
        for entry in &peer_records.result.entries {
            let content_hash = stable_content_hash(entry).unwrap_or_else(|| "unknown".to_owned());
            by_key
                .entry(entry.key.clone())
                .or_default()
                .push(SourceRecord {
                    peer_id: peer_records.peer_id.clone(),
                    transport: peer_records.transport.clone(),
                    entry: entry.clone(),
                    content_hash,
                });
        }
    }

    let mut entries = Vec::new();
    let mut conflicts = Vec::new();
    for (key, mut sources) in by_key {
        sources.sort_by(|left, right| {
            left.peer_id
                .cmp(&right.peer_id)
                .then_with(|| left.content_hash.cmp(&right.content_hash))
                .then_with(|| left.transport.as_str().cmp(right.transport.as_str()))
        });
        let winner = sources[0].clone();
        let all_same = sources
            .iter()
            .all(|source| source.content_hash == winner.content_hash);
        if !all_same {
            conflicts.push(RemoteRecordConflict {
                key,
                winner_peer_id: winner.peer_id.clone(),
                winner_hash: winner.content_hash.clone(),
                sources: sources
                    .iter()
                    .map(|source| RemoteRecordConflictSource {
                        peer_id: source.peer_id.clone(),
                        transport: source.transport.clone(),
                        content_hash: source.content_hash.clone(),
                    })
                    .collect(),
            });
        }
        entries.push(winner.entry);
    }

    let next_cursor = if cursors.is_empty() {
        None
    } else {
        serde_json::to_string(&cursors).ok()
    };

    RemoteRecordsFanIn {
        request_id: request_id.into(),
        records,
        failures,
        merged: RecordScanResult {
            entries,
            next_cursor,
        },
        conflicts,
    }
}

pub fn stable_content_hash<T>(value: &T) -> Option<String>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value).ok()?;
    Some(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}

#[cfg(test)]
mod tests {
    use super::{
        RemotePeerFailure, RemotePeerRecords, merge_remote_records_fan_in, stable_content_hash,
    };
    use crate::{RecordEntry, RecordScanResult, RecordValue, RouteTransportKind};
    use serde_json::json;

    #[test]
    fn stable_content_hash_uses_deterministic_blake3() {
        let left = stable_content_hash(&json!({ "key": "value" })).unwrap();
        let right = stable_content_hash(&json!({ "key": "value" })).unwrap();
        let changed = stable_content_hash(&json!({ "key": "changed" })).unwrap();

        assert_eq!(left, right);
        assert_ne!(left, changed);
        assert!(left.starts_with("blake3:"));
        assert_eq!(left.len(), "blake3:".len() + 64);
    }

    #[test]
    fn records_fan_in_merge_tags_sources_failures_and_conflicts() {
        let peer_a = RemotePeerRecords {
            peer_id: "peer-a".to_owned(),
            transport: RouteTransportKind::WebSocket,
            result: RecordScanResult {
                entries: vec![
                    RecordEntry {
                        key: "shared".to_owned(),
                        value: RecordValue::Json(json!({"winner": true})),
                    },
                    RecordEntry {
                        key: "unique-a".to_owned(),
                        value: RecordValue::Json(json!(1)),
                    },
                ],
                next_cursor: Some("cursor-a".to_owned()),
            },
        };
        let peer_b = RemotePeerRecords {
            peer_id: "peer-b".to_owned(),
            transport: RouteTransportKind::Moq,
            result: RecordScanResult {
                entries: vec![RecordEntry {
                    key: "shared".to_owned(),
                    value: RecordValue::Json(json!({"winner": false})),
                }],
                next_cursor: None,
            },
        };
        let failure = RemotePeerFailure {
            peer_id: "peer-c".to_owned(),
            transport: RouteTransportKind::WebRtc,
            message: "denied".to_owned(),
        };

        let fan_in = merge_remote_records_fan_in("request-1", vec![peer_b, peer_a], vec![failure]);
        assert_eq!(fan_in.request_id, "request-1");
        assert_eq!(fan_in.failures.len(), 1);
        assert_eq!(fan_in.merged.entries.len(), 2);
        assert_eq!(fan_in.merged.entries[0].key, "shared");
        assert_eq!(fan_in.merged.entries[1].key, "unique-a");
        assert_eq!(fan_in.conflicts.len(), 1);
        assert_eq!(fan_in.conflicts[0].key, "shared");
        assert_eq!(fan_in.conflicts[0].winner_peer_id, "peer-a");
        assert!(
            fan_in
                .merged
                .next_cursor
                .as_deref()
                .is_some_and(|cursor| cursor.contains("peer-a"))
        );
    }
}

pub fn error_pull_response(request_id: &str, message: impl Into<String>) -> PullResponse {
    PullResponse {
        request_id: request_id.to_owned(),
        chunk: PullChunk { index: 0, total: 1 },
        done: true,
        result: PullResponseBody::Error {
            message: message.into(),
        },
    }
}

pub fn error_watch_event(
    watch_id: &str,
    sequence: u64,
    initial: bool,
    message: impl Into<String>,
) -> WatchEvent {
    WatchEvent {
        watch_id: watch_id.to_owned(),
        sequence,
        initial,
        chunk: PullChunk { index: 0, total: 1 },
        done: true,
        result: PullResponseBody::Error {
            message: message.into(),
        },
    }
}
