use crate::engine::decode_component;
use crate::wasm::{WasmSegmentStorageEstimate, WasmSegmentWriteSummary};
use crate::{DatabaseSnapshot, NodeState, StorageMetadata, StorageTransaction, encode_component};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

const META_FILE: &str = "meta.json";
const NODES_DIR: &str = "nodes";
const AUTH_DIR: &str = "auth";
const SEGMENTS_DIR: &str = "segments";

pub(crate) async fn replace_segment_transaction_opfs(
    directory: &str,
    namespace: &str,
    transaction: &StorageTransaction,
) -> std::result::Result<WasmSegmentWriteSummary, JsValue> {
    validate_opfs_path(directory, namespace)?;
    let segment_dir = opfs_segment_dir(directory, namespace, true).await?;
    let mut summary = write_segment_transaction_files(&segment_dir, transaction).await?;
    summary.entries_deleted = prune_stale_segment_files(&segment_dir, transaction)
        .await?
        .saturating_add(summary.entries_deleted);

    Ok(summary)
}

pub(crate) async fn apply_segment_transaction_opfs(
    directory: &str,
    namespace: &str,
    transaction: &StorageTransaction,
) -> std::result::Result<WasmSegmentWriteSummary, JsValue> {
    validate_opfs_path(directory, namespace)?;
    let segment_dir = opfs_segment_dir(directory, namespace, true).await?;
    let nodes_dir = get_child_directory(&segment_dir, NODES_DIR, true).await?;
    let auth_dir = get_child_directory(&segment_dir, AUTH_DIR, true).await?;
    let mut entries_deleted = 0_u64;

    let mut touched_nodes = crate::touched_nodes(&transaction.journal_ops);
    touched_nodes.extend(transaction.nodes.keys().cloned());
    for node_id in &touched_nodes {
        if transaction.nodes.contains_key(node_id) {
            continue;
        }
        let file_name = node_file_name(node_id);
        entries_deleted = entries_deleted
            .saturating_add(remove_entry_if_exists(&nodes_dir, &file_name).await? as u64);
        entries_deleted = entries_deleted
            .saturating_add(remove_entry_if_exists(&auth_dir, &file_name).await? as u64);
    }

    let written = write_segment_transaction_files(&segment_dir, transaction).await?;
    Ok(WasmSegmentWriteSummary {
        entries_deleted,
        ..written
    })
}

pub(crate) async fn load_segment_snapshot_opfs(
    directory: &str,
    namespace: &str,
) -> std::result::Result<Option<DatabaseSnapshot>, JsValue> {
    validate_opfs_path(directory, namespace)?;
    let segment_dir = match opfs_segment_dir(directory, namespace, false).await {
        Ok(dir) => dir,
        Err(error) if is_not_found_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let Some(metadata) = read_json_file::<StorageMetadata>(&segment_dir, META_FILE).await? else {
        return Ok(None);
    };
    let nodes_dir = match get_child_directory(&segment_dir, NODES_DIR, false).await {
        Ok(dir) => dir,
        Err(error) if is_not_found_error(&error) => {
            return Ok(Some(snapshot_from_parts(metadata, BTreeMap::new())));
        }
        Err(error) => return Err(error),
    };

    let mut nodes = BTreeMap::new();
    for file_name in list_directory_entry_names(&nodes_dir).await? {
        let Some(encoded_node) = file_name.strip_suffix(".json") else {
            continue;
        };
        let node_id = decode_component(encoded_node).map_err(error_to_js)?;
        if let Some(node_state) = read_json_file::<NodeState>(&nodes_dir, &file_name).await? {
            nodes.insert(node_id, node_state);
        }
    }

    Ok(Some(snapshot_from_parts(metadata, nodes)))
}

pub(crate) async fn estimate_segment_namespace_opfs(
    directory: &str,
    namespace: &str,
) -> std::result::Result<WasmSegmentStorageEstimate, JsValue> {
    validate_opfs_path(directory, namespace)?;
    let mut estimate = match opfs_segment_dir(directory, namespace, false).await {
        Ok(dir) => estimate_directory(&dir).await?,
        Err(error) if is_not_found_error(&error) => WasmSegmentStorageEstimate::default(),
        Err(error) => return Err(error),
    };
    if let Ok((usage, quota)) = estimate_origin_storage().await {
        estimate.origin_usage = usage;
        estimate.origin_quota = quota;
    }
    Ok(estimate)
}

async fn write_segment_transaction_files(
    segment_dir: &web_sys::FileSystemDirectoryHandle,
    transaction: &StorageTransaction,
) -> std::result::Result<WasmSegmentWriteSummary, JsValue> {
    let nodes_dir = get_child_directory(segment_dir, NODES_DIR, true).await?;
    let auth_dir = get_child_directory(segment_dir, AUTH_DIR, true).await?;
    let mut entries_written = 0_u64;
    let mut estimated_bytes_written = 0_u64;

    for (node_id, node_state) in &transaction.nodes {
        let file_name = node_file_name(node_id);
        estimated_bytes_written = estimated_bytes_written.saturating_add(
            write_json_file(&nodes_dir, &file_name, node_state)
                .await?
                .saturating_add(file_name.len() as u64),
        );
        entries_written = entries_written.saturating_add(1);
    }

    for (node_id, auth_meta) in &transaction.auth_meta {
        let file_name = node_file_name(node_id);
        estimated_bytes_written = estimated_bytes_written.saturating_add(
            write_json_file(&auth_dir, &file_name, auth_meta)
                .await?
                .saturating_add(file_name.len() as u64),
        );
        entries_written = entries_written.saturating_add(1);
    }

    estimated_bytes_written = estimated_bytes_written.saturating_add(
        write_json_file(segment_dir, META_FILE, &transaction.metadata)
            .await?
            .saturating_add(META_FILE.len() as u64),
    );
    entries_written = entries_written.saturating_add(1);

    Ok(WasmSegmentWriteSummary {
        entries_written,
        entries_deleted: 0,
        estimated_bytes_written,
    })
}

async fn opfs_root_dir() -> std::result::Result<web_sys::FileSystemDirectoryHandle, JsValue> {
    let global = js_sys::global();
    let navigator = js_sys::Reflect::get(&global, &JsValue::from_str("navigator"))?;
    if navigator.is_undefined() || navigator.is_null() {
        return Err(JsValue::from_str(
            "OPFS is unavailable because navigator is missing",
        ));
    }
    let storage = js_sys::Reflect::get(&navigator, &JsValue::from_str("storage"))?;
    if storage.is_undefined() || storage.is_null() {
        return Err(JsValue::from_str(
            "OPFS is unavailable because navigator.storage is missing",
        ));
    }
    let get_directory = js_sys::Reflect::get(&storage, &JsValue::from_str("getDirectory"))?
        .dyn_into::<js_sys::Function>()
        .map_err(|_| JsValue::from_str("OPFS navigator.storage.getDirectory is missing"))?;
    let promise = get_directory
        .call0(&storage)?
        .dyn_into::<js_sys::Promise>()
        .map_err(|_| JsValue::from_str("OPFS getDirectory did not return a Promise"))?;
    JsFuture::from(promise)
        .await?
        .dyn_into::<web_sys::FileSystemDirectoryHandle>()
        .map_err(|_| JsValue::from_str("OPFS root handle has an unexpected type"))
}

async fn opfs_segments_parent_dir(
    directory: &str,
    create: bool,
) -> std::result::Result<web_sys::FileSystemDirectoryHandle, JsValue> {
    let mut current = opfs_root_dir().await?;
    for component in directory_components(directory)? {
        current = get_child_directory(&current, &component, create).await?;
    }
    get_child_directory(&current, SEGMENTS_DIR, create).await
}

async fn opfs_segment_dir(
    directory: &str,
    namespace: &str,
    create: bool,
) -> std::result::Result<web_sys::FileSystemDirectoryHandle, JsValue> {
    let parent = opfs_segments_parent_dir(directory, create).await?;
    get_child_directory(&parent, &encode_component(namespace), create).await
}

async fn get_child_directory(
    parent: &web_sys::FileSystemDirectoryHandle,
    name: &str,
    create: bool,
) -> std::result::Result<web_sys::FileSystemDirectoryHandle, JsValue> {
    let options = web_sys::FileSystemGetDirectoryOptions::new();
    options.set_create(create);
    JsFuture::from(parent.get_directory_handle_with_options(name, &options))
        .await?
        .dyn_into::<web_sys::FileSystemDirectoryHandle>()
        .map_err(|_| JsValue::from_str("OPFS directory handle has an unexpected type"))
}

async fn get_child_file(
    parent: &web_sys::FileSystemDirectoryHandle,
    name: &str,
    create: bool,
) -> std::result::Result<web_sys::FileSystemFileHandle, JsValue> {
    let options = web_sys::FileSystemGetFileOptions::new();
    options.set_create(create);
    JsFuture::from(parent.get_file_handle_with_options(name, &options))
        .await?
        .dyn_into::<web_sys::FileSystemFileHandle>()
        .map_err(|_| JsValue::from_str("OPFS file handle has an unexpected type"))
}

async fn write_json_file<T: Serialize>(
    dir: &web_sys::FileSystemDirectoryHandle,
    name: &str,
    value: &T,
) -> std::result::Result<u64, JsValue> {
    let bytes = serde_json::to_vec(value).map_err(error_to_js)?;
    write_file(dir, name, &bytes).await?;
    Ok(bytes.len() as u64)
}

async fn write_file(
    dir: &web_sys::FileSystemDirectoryHandle,
    name: &str,
    bytes: &[u8],
) -> std::result::Result<(), JsValue> {
    let file = get_child_file(dir, name, true).await?;
    let writable = JsFuture::from(file.create_writable())
        .await?
        .dyn_into::<web_sys::FileSystemWritableFileStream>()
        .map_err(|_| JsValue::from_str("OPFS writable stream has an unexpected type"))?;
    JsFuture::from(writable.write_with_u8_array(bytes)?).await?;
    let stream: web_sys::WritableStream = writable.unchecked_into();
    JsFuture::from(stream.close()).await?;
    Ok(())
}

async fn read_json_file<T: serde::de::DeserializeOwned>(
    dir: &web_sys::FileSystemDirectoryHandle,
    name: &str,
) -> std::result::Result<Option<T>, JsValue> {
    match read_file(dir, name).await {
        Ok(Some(bytes)) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(error_to_js),
        Ok(None) => Ok(None),
        Err(error) if is_not_found_error(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn read_file(
    dir: &web_sys::FileSystemDirectoryHandle,
    name: &str,
) -> std::result::Result<Option<Vec<u8>>, JsValue> {
    let file_handle = match get_child_file(dir, name, false).await {
        Ok(handle) => handle,
        Err(error) if is_not_found_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let file = JsFuture::from(file_handle.get_file())
        .await?
        .dyn_into::<web_sys::File>()
        .map_err(|_| JsValue::from_str("OPFS File handle returned an unexpected type"))?;
    let buffer = JsFuture::from(file.array_buffer()).await?;
    let bytes = js_sys::Uint8Array::new(&buffer).to_vec();
    Ok(Some(bytes))
}

async fn list_directory_entry_names(
    dir: &web_sys::FileSystemDirectoryHandle,
) -> std::result::Result<Vec<String>, JsValue> {
    let iterator = dir.entries();
    let mut names = Vec::new();
    loop {
        let next = JsFuture::from(iterator.next()?).await?;
        if iterator_result_done(&next)? {
            break;
        }
        let entry = js_sys::Array::from(&iterator_result_value(&next)?);
        if entry.length() < 1 {
            continue;
        }
        if let Some(name) = entry.get(0).as_string() {
            names.push(name);
        }
    }
    Ok(names)
}

async fn estimate_directory(
    dir: &web_sys::FileSystemDirectoryHandle,
) -> std::result::Result<WasmSegmentStorageEstimate, JsValue> {
    let mut key_count = 0_u64;
    let mut estimated_bytes = 0_u64;
    let mut pending = vec![dir.clone()];

    while let Some(current) = pending.pop() {
        let iterator = current.entries();
        loop {
            let next = JsFuture::from(iterator.next()?).await?;
            if iterator_result_done(&next)? {
                break;
            }
            let entry = js_sys::Array::from(&iterator_result_value(&next)?);
            if entry.length() < 2 {
                continue;
            }
            let name = entry.get(0).as_string().unwrap_or_default();
            let handle = entry.get(1);
            if let Ok(dir_handle) = handle
                .clone()
                .dyn_into::<web_sys::FileSystemDirectoryHandle>()
            {
                pending.push(dir_handle);
                continue;
            }
            if let Ok(file_handle) = handle.dyn_into::<web_sys::FileSystemFileHandle>() {
                let file = JsFuture::from(file_handle.get_file())
                    .await?
                    .dyn_into::<web_sys::File>()
                    .map_err(|_| {
                        JsValue::from_str("OPFS File handle returned an unexpected type")
                    })?;
                key_count = key_count.saturating_add(1);
                estimated_bytes =
                    estimated_bytes.saturating_add(name.len() as u64 + file.size() as u64);
            }
        }
    }

    Ok(WasmSegmentStorageEstimate {
        key_count,
        estimated_bytes,
        origin_usage: None,
        origin_quota: None,
    })
}

async fn prune_stale_segment_files(
    segment_dir: &web_sys::FileSystemDirectoryHandle,
    transaction: &StorageTransaction,
) -> std::result::Result<u64, JsValue> {
    let nodes_dir = get_child_directory(segment_dir, NODES_DIR, true).await?;
    let auth_dir = get_child_directory(segment_dir, AUTH_DIR, true).await?;
    let live_nodes = transaction
        .nodes
        .keys()
        .map(|node_id| node_file_name(node_id))
        .collect::<BTreeSet<_>>();
    let live_auth = transaction
        .auth_meta
        .keys()
        .map(|node_id| node_file_name(node_id))
        .collect::<BTreeSet<_>>();
    let removed_nodes = prune_stale_files(&nodes_dir, &live_nodes).await?;
    let removed_auth = prune_stale_files(&auth_dir, &live_auth).await?;
    Ok(removed_nodes.saturating_add(removed_auth))
}

async fn prune_stale_files(
    dir: &web_sys::FileSystemDirectoryHandle,
    live_files: &BTreeSet<String>,
) -> std::result::Result<u64, JsValue> {
    let mut removed = 0_u64;
    for file_name in list_directory_entry_names(dir).await? {
        if live_files.contains(&file_name) {
            continue;
        }
        removed = removed.saturating_add(remove_entry_if_exists(dir, &file_name).await? as u64);
    }
    Ok(removed)
}

fn iterator_result_done(value: &JsValue) -> std::result::Result<bool, JsValue> {
    Ok(js_sys::Reflect::get(value, &JsValue::from_str("done"))?
        .as_bool()
        .unwrap_or(false))
}

fn iterator_result_value(value: &JsValue) -> std::result::Result<JsValue, JsValue> {
    js_sys::Reflect::get(value, &JsValue::from_str("value"))
}

async fn estimate_origin_storage() -> std::result::Result<(Option<u64>, Option<u64>), JsValue> {
    let global = js_sys::global();
    let navigator = js_sys::Reflect::get(&global, &JsValue::from_str("navigator"))?;
    let storage = js_sys::Reflect::get(&navigator, &JsValue::from_str("storage"))?;
    let estimate = js_sys::Reflect::get(&storage, &JsValue::from_str("estimate"))?
        .dyn_into::<js_sys::Function>()
        .map_err(|_| JsValue::from_str("navigator.storage.estimate is missing"))?;
    let promise = estimate
        .call0(&storage)?
        .dyn_into::<js_sys::Promise>()
        .map_err(|_| JsValue::from_str("navigator.storage.estimate did not return a Promise"))?;
    let value = JsFuture::from(promise).await?;
    let usage = js_sys::Reflect::get(&value, &JsValue::from_str("usage"))?
        .as_f64()
        .map(|value| value as u64);
    let quota = js_sys::Reflect::get(&value, &JsValue::from_str("quota"))?
        .as_f64()
        .map(|value| value as u64);
    Ok((usage, quota))
}

async fn remove_entry_if_exists(
    dir: &web_sys::FileSystemDirectoryHandle,
    name: &str,
) -> std::result::Result<bool, JsValue> {
    match JsFuture::from(dir.remove_entry(name)).await {
        Ok(_) => Ok(true),
        Err(error) if is_not_found_error(&error) => Ok(false),
        Err(error) => Err(error),
    }
}

fn snapshot_from_parts(
    metadata: StorageMetadata,
    nodes: BTreeMap<String, NodeState>,
) -> DatabaseSnapshot {
    DatabaseSnapshot {
        clock: metadata.clock,
        nodes,
        pending_ops: metadata.pending_ops,
        scope_policies: metadata.scope_policies,
        provisional_transactions: metadata.provisional_transactions,
        next_provisional_transaction_id: metadata.next_provisional_transaction_id,
    }
}

fn validate_opfs_path(directory: &str, namespace: &str) -> std::result::Result<(), JsValue> {
    if directory.trim().is_empty() {
        return Err(JsValue::from_str("OPFS directory must not be empty"));
    }
    if namespace.trim().is_empty() {
        return Err(JsValue::from_str("OPFS namespace must not be empty"));
    }
    Ok(())
}

fn directory_components(directory: &str) -> std::result::Result<Vec<String>, JsValue> {
    let components = directory
        .split('/')
        .filter(|component| !component.is_empty())
        .map(encode_component)
        .collect::<Vec<_>>();
    if components.is_empty() {
        return Err(JsValue::from_str("OPFS directory must not be empty"));
    }
    Ok(components)
}

fn node_file_name(node_id: &str) -> String {
    format!("{}.json", encode_component(node_id))
}

fn is_not_found_error(error: &JsValue) -> bool {
    if let Some(exception) = error.dyn_ref::<web_sys::DomException>() {
        return exception.name() == "NotFoundError";
    }
    js_sys::Reflect::get(error, &JsValue::from_str("name"))
        .ok()
        .and_then(|value| value.as_string())
        .is_some_and(|name| name == "NotFoundError")
}

fn error_to_js(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}
