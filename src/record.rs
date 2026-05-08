use crate::binary::BinaryBytes;
use crate::blob::BlobRef;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum RecordValue {
    Json(JsonValue),
    Bytes(BinaryBytes),
    Blob(BlobRef),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RecordEntry {
    pub key: String,
    pub value: RecordValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RecordScan {
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub start_at: Option<String>,
    #[serde(default)]
    pub start_after: Option<String>,
    #[serde(default)]
    pub end_at: Option<String>,
    #[serde(default)]
    pub end_before: Option<String>,
    #[serde(default)]
    pub reverse: bool,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl RecordScan {
    pub fn matches_key(&self, key: &str) -> bool {
        if let Some(prefix) = &self.prefix
            && !key.starts_with(prefix)
        {
            return false;
        }
        if let Some(cursor) = &self.cursor {
            if self.reverse {
                if key >= cursor.as_str() {
                    return false;
                }
            } else if key <= cursor.as_str() {
                return false;
            }
        }
        if let Some(start_at) = &self.start_at
            && key < start_at.as_str()
        {
            return false;
        }
        if let Some(start_after) = &self.start_after
            && key <= start_after.as_str()
        {
            return false;
        }
        if let Some(end_at) = &self.end_at
            && key > end_at.as_str()
        {
            return false;
        }
        if let Some(end_before) = &self.end_before
            && key >= end_before.as_str()
        {
            return false;
        }
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RecordScanResult {
    pub entries: Vec<RecordEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RecordMutation {
    Put { key: String, value: RecordValue },
    Delete { key: String },
    DeleteRange { scan: RecordScan },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RecordBatch {
    #[serde(default)]
    pub mutations: Vec<RecordMutation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RecordBatchReport {
    pub puts: usize,
    pub deletes: usize,
    pub range_deletes: usize,
    pub operation_count: usize,
}
