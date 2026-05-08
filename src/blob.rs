use crate::binary::BinaryBytes;
use crate::durable::SegmentDurability;
#[cfg(not(target_arch = "wasm32"))]
use crate::error::PrimadbError;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt::Debug;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(not(target_arch = "wasm32"))]
static BLOB_TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlobRef {
    pub id: String,
    pub bytes: usize,
    #[serde(default)]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StoredBlob {
    pub reference: BlobRef,
    pub data: BinaryBytes,
}

pub trait BlobStore: Debug + Send + Sync {
    fn name(&self) -> &str;
    fn put_blob(&self, data: &[u8], media_type: Option<&str>) -> Result<BlobRef>;
    fn get_blob(&self, blob_id: &str) -> Result<Option<StoredBlob>>;
    fn has_blob(&self, blob_id: &str) -> Result<bool> {
        Ok(self.get_blob(blob_id)?.is_some())
    }
    fn delete_unreferenced(&self, live_blob_ids: &BTreeSet<String>) -> Result<usize> {
        let _ = live_blob_ids;
        Ok(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum BlobStorageConfig {
    Memory,
    #[cfg(not(target_arch = "wasm32"))]
    Files {
        directory: String,
        #[serde(default)]
        durability: SegmentDurability,
    },
    #[cfg(target_arch = "wasm32")]
    IndexedDb {
        database_name: String,
        store_name: String,
        namespace: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BlobStorageBinding {
    pub backend: String,
    pub content_addressed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability: Option<SegmentDurability>,
}

#[derive(Debug, Clone, Default)]
pub struct MemoryBlobStore {
    blobs: Arc<Mutex<std::collections::BTreeMap<String, StoredBlob>>>,
}

impl MemoryBlobStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BlobStore for MemoryBlobStore {
    fn name(&self) -> &str {
        "memory"
    }

    fn put_blob(&self, data: &[u8], media_type: Option<&str>) -> Result<BlobRef> {
        let reference = blob_ref_for_data(data, media_type);
        let blob = StoredBlob {
            reference: reference.clone(),
            data: BinaryBytes::from(data),
        };
        self.blobs
            .lock()
            .unwrap()
            .insert(reference.id.clone(), blob);
        Ok(reference)
    }

    fn get_blob(&self, blob_id: &str) -> Result<Option<StoredBlob>> {
        Ok(self.blobs.lock().unwrap().get(blob_id).cloned())
    }

    fn delete_unreferenced(&self, live_blob_ids: &BTreeSet<String>) -> Result<usize> {
        let mut blobs = self.blobs.lock().unwrap();
        let before = blobs.len();
        blobs.retain(|blob_id, _| live_blob_ids.contains(blob_id));
        Ok(before.saturating_sub(blobs.len()))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct FileBlobStore {
    root: std::path::PathBuf,
    durability: SegmentDurability,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct FileBlobStoreOptions {
    #[serde(default)]
    pub durability: SegmentDurability,
}

#[cfg(not(target_arch = "wasm32"))]
impl FileBlobStore {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self::with_options(root, FileBlobStoreOptions::default())
    }

    pub fn with_options(
        root: impl Into<std::path::PathBuf>,
        options: FileBlobStoreOptions,
    ) -> Self {
        Self {
            root: root.into(),
            durability: options.durability,
        }
    }

    fn ensure_layout(&self) -> Result<()> {
        std::fs::create_dir_all(self.root.join("blobs"))?;
        if matches!(self.durability, SegmentDurability::Full) {
            self.sync_dir(&self.root)?;
        }
        Ok(())
    }

    fn blob_dir(&self, blob_id: &str) -> std::path::PathBuf {
        self.root.join("blobs").join(blob_id.replace(':', "_"))
    }

    fn blob_meta_path(&self, blob_id: &str) -> std::path::PathBuf {
        self.blob_dir(blob_id).join("meta.json")
    }

    fn blob_data_path(&self, blob_id: &str) -> std::path::PathBuf {
        self.blob_dir(blob_id).join("data.bin")
    }

    fn write_file(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        if matches!(self.durability, SegmentDurability::Relaxed) {
            std::fs::write(path, bytes)?;
            return Ok(());
        }

        let parent = path.parent().ok_or_else(|| {
            PrimadbError::Message(format!("path `{}` has no parent directory", path.display()))
        })?;
        let temp_path = self.temp_path_for(path);
        {
            use std::io::Write;

            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)?;
            file.write_all(bytes)?;
            file.flush()?;
            match self.durability {
                SegmentDurability::Full => file.sync_all()?,
                SegmentDurability::Data => file.sync_data()?,
                SegmentDurability::Relaxed => {}
            }
        }
        replace_file(&temp_path, path)?;
        if matches!(self.durability, SegmentDurability::Full) {
            self.sync_dir(parent)?;
        }
        Ok(())
    }

    fn temp_path_for(&self, path: &std::path::Path) -> std::path::PathBuf {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("primadb-blob");
        let counter = BLOB_TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        path.with_file_name(format!(
            "{name}.tmp-{}-{}-{}",
            std::process::id(),
            crate::clock::now_millis(),
            counter
        ))
    }

    fn sync_dir(&self, path: &std::path::Path) -> Result<()> {
        let file = std::fs::File::open(path)?;
        file.sync_all()?;
        Ok(())
    }
}

#[cfg(all(not(target_arch = "wasm32"), not(windows)))]
fn replace_file(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    std::fs::rename(from, to)?;
    Ok(())
}

#[cfg(windows)]
fn replace_file(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    unsafe extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let from_wide = from
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let to_wide = to
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let ok = unsafe {
        MoveFileExW(
            from_wide.as_ptr(),
            to_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok == 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl BlobStore for FileBlobStore {
    fn name(&self) -> &str {
        "files"
    }

    fn put_blob(&self, data: &[u8], media_type: Option<&str>) -> Result<BlobRef> {
        self.ensure_layout()?;
        let reference = blob_ref_for_data(data, media_type);
        let dir = self.blob_dir(&reference.id);
        std::fs::create_dir_all(&dir)?;
        self.write_file(&self.blob_data_path(&reference.id), data)?;
        self.write_file(
            &self.blob_meta_path(&reference.id),
            &serde_json::to_vec(&reference)?,
        )?;
        if matches!(self.durability, SegmentDurability::Full) {
            self.sync_dir(&dir)?;
        }
        Ok(reference)
    }

    fn get_blob(&self, blob_id: &str) -> Result<Option<StoredBlob>> {
        self.ensure_layout()?;
        let meta_path = self.blob_meta_path(blob_id);
        let data_path = self.blob_data_path(blob_id);
        if !meta_path.exists() || !data_path.exists() {
            return Ok(None);
        }
        let reference: BlobRef = serde_json::from_str(&std::fs::read_to_string(meta_path)?)?;
        let data = std::fs::read(data_path)?;
        if reference.bytes != data.len() {
            return Err(PrimadbError::Message(format!(
                "blob `{blob_id}` metadata length {} does not match stored bytes {}",
                reference.bytes,
                data.len()
            )));
        }
        Ok(Some(StoredBlob {
            reference,
            data: BinaryBytes::from(data),
        }))
    }

    fn delete_unreferenced(&self, live_blob_ids: &BTreeSet<String>) -> Result<usize> {
        self.ensure_layout()?;
        let root = self.root.join("blobs");
        if !root.exists() {
            return Ok(0);
        }
        let mut removed = 0;
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let blob_id = name.replace('_', ":");
            if !live_blob_ids.contains(&blob_id) {
                std::fs::remove_dir_all(path)?;
                if matches!(self.durability, SegmentDurability::Full) {
                    self.sync_dir(&self.root.join("blobs"))?;
                }
                removed += 1;
            }
        }
        Ok(removed)
    }
}

pub fn blob_ref_for_data(data: &[u8], media_type: Option<&str>) -> BlobRef {
    let digest = blake3::hash(data);
    BlobRef {
        id: format!("blake3:{}", digest.to_hex()),
        bytes: data.len(),
        media_type: media_type.map(str::to_owned),
    }
}

#[cfg(test)]
mod tests {
    use super::blob_ref_for_data;

    #[test]
    fn blob_refs_use_deterministic_blake3_ids() {
        let left = blob_ref_for_data(b"primadb", Some("application/octet-stream"));
        let right = blob_ref_for_data(b"primadb", Some("application/octet-stream"));
        let changed = blob_ref_for_data(b"primadb!", Some("application/octet-stream"));

        assert_eq!(left.id, right.id);
        assert_ne!(left.id, changed.id);
        assert!(left.id.starts_with("blake3:"));
        assert_eq!(left.id.len(), "blake3:".len() + 64);
        assert_eq!(left.bytes, 7);
    }
}
