use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueryDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryOrder {
    pub path: String,
    #[serde(default)]
    pub direction: QueryDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueryFilter {
    Eq { path: String, value: JsonValue },
    Ne { path: String, value: JsonValue },
    Gt { path: String, value: JsonValue },
    Gte { path: String, value: JsonValue },
    Lt { path: String, value: JsonValue },
    Lte { path: String, value: JsonValue },
    Prefix { path: String, value: String },
    Contains { path: String, value: String },
    Exists { path: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct QuerySpec {
    #[serde(default)]
    pub filters: Vec<QueryFilter>,
    #[serde(default)]
    pub order: Option<QueryOrder>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LexEntry {
    pub path: String,
    pub key: String,
    pub value: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LexSpec {
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
    #[serde(default = "default_depth")]
    pub depth: usize,
    #[serde(default)]
    pub follow_links: bool,
}

impl Default for LexSpec {
    fn default() -> Self {
        Self {
            prefix: None,
            start_at: None,
            start_after: None,
            end_at: None,
            end_before: None,
            reverse: false,
            limit: None,
            depth: default_depth(),
            follow_links: false,
        }
    }
}

fn default_depth() -> usize {
    1
}
