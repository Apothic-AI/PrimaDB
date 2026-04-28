use crate::clock::VersionMarker;
use crate::sync::RemotePath;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScopeConsistency {
    #[default]
    Eventual,
    LocalTransactional,
    Coordinated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeAuthority {
    Peer {
        #[serde(rename = "peerId", alias = "peer_id")]
        peer_id: String,
    },
    FullNode {
        #[serde(rename = "peerId", alias = "peer_id")]
        peer_id: String,
    },
    Quorum {
        peers: Vec<String>,
        threshold: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScopeIsolation {
    #[default]
    Serializable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScopeReadMode {
    #[default]
    Cached,
    Authority,
    Quorum,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScopeOfflineWrites {
    #[default]
    Reject,
    QueueProvisional,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScopePolicy {
    #[serde(default)]
    pub consistency: ScopeConsistency,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authority: Option<ScopeAuthority>,
    #[serde(default)]
    pub isolation: ScopeIsolation,
    #[serde(default)]
    pub read_mode: ScopeReadMode,
    #[serde(default)]
    pub offline_writes: ScopeOfflineWrites,
}

impl Default for ScopePolicy {
    fn default() -> Self {
        Self {
            consistency: ScopeConsistency::Eventual,
            authority: None,
            isolation: ScopeIsolation::Serializable,
            read_mode: ScopeReadMode::Cached,
            offline_writes: ScopeOfflineWrites::Reject,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TransactionOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offline: Option<ScopeOfflineWrites>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransactionStep {
    Put {
        path: RemotePath,
        value: JsonValue,
    },
    Unset {
        path: RemotePath,
    },
    Set {
        path: RemotePath,
        value: JsonValue,
    },
    Remove {
        path: RemotePath,
        value: JsonValue,
    },
    AssertExists {
        path: RemotePath,
    },
    AssertAbsent {
        path: RemotePath,
    },
    AssertValue {
        path: RemotePath,
        value: JsonValue,
    },
    AssertRevision {
        path: RemotePath,
        revision: Option<VersionMarker>,
    },
    Increment {
        path: RemotePath,
        by: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatus {
    Committed,
    Provisional,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TransactionReport {
    pub status: TransactionStatus,
    pub operation_count: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub member_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProvisionalTransaction {
    pub id: String,
    pub scope: String,
    pub created_at_millis: u64,
    pub steps: Vec<TransactionStep>,
    #[serde(default)]
    pub options: TransactionOptions,
}
