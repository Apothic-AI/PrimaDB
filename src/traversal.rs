use crate::query::QueryFilter;
use crate::value::NodeId;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TraversalDirection {
    #[default]
    Outbound,
    Inbound,
    Both,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TraversalStrategy {
    #[default]
    Bfs,
    Dfs,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum TraversalEdgeKind {
    Link,
    SetMember,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub struct TraversalEdge {
    pub source: NodeId,
    pub field: String,
    pub target: NodeId,
    pub kind: TraversalEdgeKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TraversalSpec {
    #[serde(default)]
    pub direction: TraversalDirection,
    #[serde(default)]
    pub strategy: TraversalStrategy,
    #[serde(default = "default_traversal_depth")]
    pub max_depth: usize,
    #[serde(default = "default_traversal_limit")]
    pub limit: Option<usize>,
    #[serde(default)]
    pub edge_fields: Option<Vec<String>>,
    #[serde(default = "default_true")]
    pub follow_links: bool,
    #[serde(default = "default_true")]
    pub follow_sets: bool,
    #[serde(default)]
    pub include_start: bool,
    #[serde(default)]
    pub include_values: bool,
    #[serde(default)]
    pub filters: Vec<QueryFilter>,
    #[serde(default = "default_true")]
    pub fetch_missing: bool,
    #[serde(default = "default_fetch_limit")]
    pub max_fetches: usize,
}

impl Default for TraversalSpec {
    fn default() -> Self {
        Self {
            direction: TraversalDirection::default(),
            strategy: TraversalStrategy::default(),
            max_depth: default_traversal_depth(),
            limit: default_traversal_limit(),
            edge_fields: None,
            follow_links: true,
            follow_sets: true,
            include_start: false,
            include_values: false,
            filters: Vec::new(),
            fetch_missing: true,
            max_fetches: default_fetch_limit(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TraversalEntry {
    pub node_id: NodeId,
    pub depth: usize,
    pub path: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub via: Option<TraversalEdge>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TraversalResult {
    pub entries: Vec<TraversalEntry>,
    pub complete: bool,
    pub timed_out: bool,
    pub depth_limit_reached: bool,
    pub result_limit_reached: bool,
    pub fetched: usize,
    pub missing: Vec<NodeId>,
    pub denied: Vec<NodeId>,
}

fn default_traversal_depth() -> usize {
    1
}

fn default_traversal_limit() -> Option<usize> {
    Some(1024)
}

fn default_fetch_limit() -> usize {
    64
}

fn default_true() -> bool {
    true
}
