use crate::clock::Revision;
use crate::value::NodeId;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum OperationValue {
    Scalar(JsonValue),
    Link(NodeId),
    Set(Vec<NodeId>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Operation {
    pub op_id: String,
    pub author: String,
    pub revision: Revision,
    pub action: OperationAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OperationAction {
    SetField {
        node: NodeId,
        field: String,
        value: OperationValue,
    },
    AddSetMember {
        node: NodeId,
        field: String,
        member: NodeId,
    },
    DeleteField {
        node: NodeId,
        field: String,
    },
}
