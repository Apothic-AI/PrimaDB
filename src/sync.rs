use crate::value::{NodeId, NodeState};
use crate::{
    DatabaseSnapshot, HybridClock, LexEntry, LexSpec, MapEntry, Operation, QuerySpec, RecordEntry,
    RecordScan, RecordScanResult, ScopePolicy, TransactionOptions, TransactionReport,
    TransactionStep,
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
    Node { node: Option<NodeState> },
    Snapshot { snapshot: DatabaseSnapshot },
    Transaction { report: TransactionReport },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoteWatchMessage {
    pub initial: bool,
    pub result: RemoteResult,
}

pub struct RemoteWatchSubscription {
    inner: Arc<RemoteWatchSubscriptionInner>,
}

struct RemoteWatchSubscriptionInner {
    receiver: Receiver<std::result::Result<RemoteWatchMessage, String>>,
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
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
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

pub fn stable_content_hash<T>(value: &T) -> Option<String>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value).ok()?;
    Some(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}

#[cfg(test)]
mod tests {
    use super::stable_content_hash;
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
