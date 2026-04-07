use crate::clock::VersionMarker;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

pub type NodeId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SetState {
    pub baseline: VersionMarker,
    pub members: BTreeMap<NodeId, VersionMarker>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FieldValue {
    Scalar(JsonValue),
    Link(NodeId),
    Set(SetState),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldState {
    pub value: FieldValue,
    pub version: VersionMarker,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeState {
    pub id: NodeId,
    pub fields: BTreeMap<String, FieldState>,
    pub tombstones: BTreeMap<String, VersionMarker>,
}

impl NodeState {
    pub fn new(id: impl Into<NodeId>) -> Self {
        Self {
            id: id.into(),
            fields: BTreeMap::new(),
            tombstones: BTreeMap::new(),
        }
    }
}
