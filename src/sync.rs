use crate::Operation;
use serde::{Deserialize, Serialize};

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
