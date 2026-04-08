use crate::clock::HybridClock;
use crate::operation::Operation;
use crate::value::{NodeId, NodeState};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatabaseSnapshot {
    pub clock: HybridClock,
    pub nodes: BTreeMap<NodeId, NodeState>,
    pub pending_ops: Vec<Operation>,
}
