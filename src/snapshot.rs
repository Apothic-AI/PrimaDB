use crate::clock::HybridClock;
use crate::consistency::{ProvisionalTransaction, ScopePolicy};
use crate::operation::Operation;
use crate::value::{NodeId, NodeState};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatabaseSnapshot {
    pub clock: HybridClock,
    pub nodes: BTreeMap<NodeId, NodeState>,
    pub pending_ops: Vec<Operation>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub scope_policies: BTreeMap<String, ScopePolicy>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub provisional_transactions: BTreeMap<String, ProvisionalTransaction>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub next_provisional_transaction_id: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}
