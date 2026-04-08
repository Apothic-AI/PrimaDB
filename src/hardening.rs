use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimadbLimits {
    pub max_pending_ops: usize,
    pub max_ops_per_message: usize,
    pub max_route_payload_bytes: usize,
    pub max_seen_routes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PrimadbStats {
    pub replica_id: String,
    pub nodes: usize,
    pub pending_ops: usize,
    pub subscriptions: usize,
    pub change_subscriptions: usize,
    pub unflushed_ops: usize,
}

impl Default for PrimadbLimits {
    fn default() -> Self {
        Self {
            max_pending_ops: 32_768,
            max_ops_per_message: 1_024,
            max_route_payload_bytes: 512 * 1024,
            max_seen_routes: 4_096,
        }
    }
}
