use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SegmentDurability {
    #[default]
    Full,
    Data,
    Relaxed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum SegmentLockMode {
    Exclusive,
    Wait { timeout_millis: u64 },
    Disabled,
}

impl Default for SegmentLockMode {
    fn default() -> Self {
        Self::Exclusive
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct SegmentFileStoreOptions {
    #[serde(default)]
    pub durability: SegmentDurability,
    #[serde(default)]
    pub lock_mode: SegmentLockMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum DurableStorageConfig {
    BrowserStorage {
        key: String,
    },
    IndexedDbSnapshots {
        database_name: String,
        store_name: String,
        key: String,
        #[serde(default = "default_true")]
        load_existing: bool,
        #[serde(default = "default_true")]
        auto_persist: bool,
    },
    IndexedDbSegments {
        database_name: String,
        store_name: String,
        namespace: String,
        #[serde(default = "default_true")]
        load_existing: bool,
        #[serde(default = "default_true")]
        auto_persist: bool,
    },
    OpfsSegments {
        directory: String,
        namespace: String,
        #[serde(default = "default_true")]
        load_existing: bool,
        #[serde(default = "default_true")]
        auto_persist: bool,
    },
    SnapshotFile {
        path: String,
    },
    SegmentFiles {
        directory: String,
        #[serde(default = "default_journal_retention")]
        journal_retention: usize,
        #[serde(default)]
        durability: SegmentDurability,
        #[serde(default)]
        lock_mode: SegmentLockMode,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DurableStorageBinding {
    pub backend: String,
    pub incremental: bool,
    pub loaded_existing: bool,
    pub auto_persist: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability: Option<SegmentDurability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_mode: Option<SegmentLockMode>,
}

const fn default_true() -> bool {
    true
}

const fn default_journal_retention() -> usize {
    8
}
