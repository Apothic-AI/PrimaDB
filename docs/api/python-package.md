---
title: Python Package API
sidebar_position: 7
---

This page covers the `primadb-python` package surface. It is generated directly from the public stub file shipped with the package.

> Generated from `packages/primadb-python/python/primadb/__init__.pyi`.

## `PresenceIdentity`

Kind: class

```py
class PresenceIdentity(TypedDict, total=False):
    publicKey: str
    alias: Optional[str]
    keyScheme: str
    sessionId: str
    claims: dict[str, str]
    issuedAtMillis: int
    expiresAtMillis: Optional[int]
```

## `VerifiedIdentity`

Kind: class

```py
class VerifiedIdentity(TypedDict, total=False):
    publicKey: str
    alias: Optional[str]
    peerId: str
    replicaId: str
    transport: str
    sessionId: str
    claims: dict[str, str]
    issuedAtMillis: int
    expiresAtMillis: Optional[int]
    trust: str
```

## `IdentityKeyPair`

Kind: class

```py
class IdentityKeyPair(TypedDict):
    publicKey: str
    secretKey: str
```

## `PasswordKeyDerivationParams`

Kind: class

```py
class PasswordKeyDerivationParams(TypedDict, total=False):
    memoryCostKiB: int
    timeCost: int
    parallelism: int
```

## `PasswordKeyDerivationOptions`

Kind: class

```py
class PasswordKeyDerivationOptions(PasswordKeyDerivationParams, total=False):
    saltBase64: Optional[str]
```

## `PasswordDerivedKey`

Kind: class

```py
class PasswordDerivedKey(TypedDict):
    algorithm: str
    keyBase64: str
    saltBase64: str
    params: PasswordKeyDerivationParams
```

## `UserGrant`

Kind: class

```py
class UserGrant(TypedDict, total=False):
    root: str
    read: bool
    write: bool
```

## `PeerHookContext`

Kind: class

```py
class PeerHookContext(TypedDict, total=False):
    peerId: str
    replicaId: str
    transport: str
    identity: Optional[PresenceIdentity]
    capabilities: list[str]
    topics: list[str]
    metadata: dict[str, str]
```

## `ConnectHookContext`

Kind: class

```py
class ConnectHookContext(TypedDict, total=False):
    peer: PeerHookContext
    transport: str
    relayUrl: Optional[str]
    verifiedIdentity: Optional[VerifiedIdentity]
```

## `RoomHookContext`

Kind: class

```py
class RoomHookContext(TypedDict, total=False):
    peerId: str
    room: str
    transport: str
    peer: Optional[PeerHookContext]
    verifiedIdentity: Optional[VerifiedIdentity]
```

## `ServeRequestContext`

Kind: class

```py
class ServeRequestContext(TypedDict, total=False):
    peerId: str
    transport: str
    requestId: Optional[str]
    watchId: Optional[str]
    request: Any
    verifiedIdentity: Optional[VerifiedIdentity]
```

## `ServeResultContext`

Kind: class

```py
class ServeResultContext(TypedDict, total=False):
    peerId: str
    transport: str
    requestId: Optional[str]
    watchId: Optional[str]
    request: Any
    initial: bool
    verifiedIdentity: Optional[VerifiedIdentity]
```

## `SessionAuthConfig`

Kind: class

```py
class SessionAuthConfig(TypedDict, total=False):
    requireAuthenticatedPeers: bool
    trustedPublicKeys: list[str]
    trustedAliases: list[str]
    challengeTimeoutMs: int
    sessionTtlMs: int
    allowUnauthenticatedPresence: bool
```

## `RelayClientConfig`

Kind: class

```py
class RelayClientConfig(TypedDict, total=False):
    url: str
    retryIntervalMs: int
    sessionAuth: SessionAuthConfig
```

## `IceServerConfig`

Kind: class

```py
class IceServerConfig(TypedDict, total=False):
    urls: str | list[str]
    username: Optional[str]
    credential: Optional[str]
```

## `MeshConfig`

Kind: class

```py
class MeshConfig(TypedDict, total=False):
    room: str
    signaling: str
    relayUrl: Optional[str]
    retryIntervalMs: int
    iceServers: list[IceServerConfig]
    sessionAuth: SessionAuthConfig
```

## `SegmentDurability`

Kind: type alias

```py
SegmentDurability = Literal["full", "data", "relaxed"]
```

## `SegmentLockExclusive`

Kind: class

```py
class SegmentLockExclusive(TypedDict):
    kind: Literal["exclusive"]
```

## `SegmentLockWait`

Kind: class

```py
class SegmentLockWait(TypedDict):
    kind: Literal["wait"]
    timeoutMillis: int
```

## `SegmentLockDisabled`

Kind: class

```py
class SegmentLockDisabled(TypedDict):
    kind: Literal["disabled"]
```

## `SegmentLockMode`

Kind: type alias

```py
SegmentLockMode = SegmentLockExclusive | SegmentLockWait | SegmentLockDisabled
```

## `SnapshotFileStorageConfig`

Kind: class

```py
class SnapshotFileStorageConfig(TypedDict):
    kind: Literal["snapshot_file"]
    path: str
```

## `SegmentFilesStorageConfig`

Kind: class

```py
class SegmentFilesStorageConfig(TypedDict, total=False):
    kind: Literal["segment_files"]
    directory: str
    journalRetention: int
    durability: SegmentDurability
    lockMode: SegmentLockMode
```

## `DurableStorageConfig`

Kind: type alias

```py
DurableStorageConfig = SnapshotFileStorageConfig | SegmentFilesStorageConfig
```

## `DurableStorageBinding`

Kind: class

```py
class DurableStorageBinding(TypedDict, total=False):
    backend: str
    incremental: bool
    loadedExisting: bool
    autoPersist: bool
    durability: SegmentDurability
    lockMode: SegmentLockMode
```

## `MemoryBlobStorageConfig`

Kind: class

```py
class MemoryBlobStorageConfig(TypedDict):
    kind: Literal["memory"]
```

## `FilesBlobStorageConfig`

Kind: class

```py
class FilesBlobStorageConfig(TypedDict, total=False):
    kind: Literal["files"]
    directory: str
    durability: SegmentDurability
```

## `BlobStorageConfig`

Kind: type alias

```py
BlobStorageConfig = MemoryBlobStorageConfig | FilesBlobStorageConfig
```

## `BlobStorageBinding`

Kind: class

```py
class BlobStorageBinding(TypedDict, total=False):
    backend: str
    contentAddressed: bool
    durability: SegmentDurability
```

## `BlobRef`

Kind: class

```py
class BlobRef(TypedDict, total=False):
    id: str
    bytes: int
    mediaType: Optional[str]
```

## `RecordJsonValue`

Kind: class

```py
class RecordJsonValue(TypedDict):
    kind: Literal["json"]
    value: Any
```

## `RecordBytesValue`

Kind: class

```py
class RecordBytesValue(TypedDict):
    kind: Literal["bytes"]
    value: str
```

## `RecordBlobValue`

Kind: class

```py
class RecordBlobValue(TypedDict):
    kind: Literal["blob"]
    value: BlobRef
```

## `RecordValue`

Kind: type alias

```py
RecordValue = RecordJsonValue | RecordBytesValue | RecordBlobValue
```

## `RecordEntry`

Kind: class

```py
class RecordEntry(TypedDict):
    key: str
    value: RecordValue
```

## `RecordScan`

Kind: class

```py
class RecordScan(TypedDict, total=False):
    prefix: Optional[str]
    startAt: Optional[str]
    startAfter: Optional[str]
    endAt: Optional[str]
    endBefore: Optional[str]
    reverse: bool
    limit: Optional[int]
    cursor: Optional[str]
```

## `RecordScanResult`

Kind: class

```py
class RecordScanResult(TypedDict, total=False):
    entries: list[RecordEntry]
    nextCursor: Optional[str]
```

## `RemoteInterestTarget`

Kind: type alias

```py
RemoteInterestTarget = Literal["any", "peer", "peers"]
```

## `RemoteInterestPolicy`

Kind: class

```py
class RemoteInterestPolicy(TypedDict, total=False):
    target: RemoteInterestTarget
    peerId: Optional[str]
    peers: list[str]
    requireCapability: bool
```

## `RecordPutMutation`

Kind: class

```py
class RecordPutMutation(TypedDict):
    kind: Literal["put"]
    key: str
    value: RecordValue
```

## `RecordDeleteMutation`

Kind: class

```py
class RecordDeleteMutation(TypedDict):
    kind: Literal["delete"]
    key: str
```

## `RecordDeleteRangeMutation`

Kind: class

```py
class RecordDeleteRangeMutation(TypedDict):
    kind: Literal["delete_range"]
    scan: RecordScan
```

## `RecordMutation`

Kind: type alias

```py
RecordMutation = RecordPutMutation | RecordDeleteMutation | RecordDeleteRangeMutation
```

## `RecordExistsPrecondition`

Kind: class

```py
class RecordExistsPrecondition(TypedDict):
    kind: Literal["exists"]
    key: str
```

## `RecordAbsentPrecondition`

Kind: class

```py
class RecordAbsentPrecondition(TypedDict):
    kind: Literal["absent"]
    key: str
```

## `RecordValuePrecondition`

Kind: class

```py
class RecordValuePrecondition(TypedDict):
    kind: Literal["value"]
    key: str
    value: RecordValue
```

## `RecordPrecondition`

Kind: type alias

```py
RecordPrecondition = RecordExistsPrecondition | RecordAbsentPrecondition | RecordValuePrecondition
```

## `RecordBatch`

Kind: class

```py
class RecordBatch(TypedDict, total=False):
    preconditions: list[RecordPrecondition]
    mutations: list[RecordMutation]
```

## `RecordBatchReport`

Kind: class

```py
class RecordBatchReport(TypedDict):
    preconditions: int
    puts: int
    deletes: int
    rangeDeletes: int
    operationCount: int
```

## `StorageSyncReport`

Kind: class

```py
class StorageSyncReport(TypedDict):
    backend: str
    durability: str
    synced: bool
```

## `StorageRecoveryReport`

Kind: class

```py
class StorageRecoveryReport(TypedDict):
    appliedTransactions: int
    skippedTransactions: int
    removedPendingFiles: int
    removedTempFiles: int
    quarantinedFiles: int
```

## `ScriptPathGrant`

Kind: class

```py
class ScriptPathGrant(TypedDict, total=False):
    root: str
    segments: list[str]
    recursive: bool
```

## `ScriptCapabilities`

Kind: class

```py
class ScriptCapabilities(TypedDict, total=False):
    read: list[ScriptPathGrant]
    query: list[ScriptPathGrant]
    traverse: list[ScriptPathGrant]
    write: list[ScriptPathGrant]
    transaction: list[ScriptPathGrant]
```

## `ScriptLimits`

Kind: class

```py
class ScriptLimits(TypedDict, total=False):
    maxOperations: int
    maxCallLevels: int
    maxVariables: int
    maxFunctions: int
    maxModules: int
    maxExpressionDepth: int
    maxStringBytes: int
    maxArraySize: int
    maxMapSize: int
```

## `NodeScript`

Kind: class

```py
class NodeScript(TypedDict, total=False):
    id: str
    runtime: str
    entry: str
    source: str
    sourceHash: Optional[str]
    author: Optional[str]
    signature: Optional[str]
    capabilities: ScriptCapabilities
    metadata: Any
```

## `ScriptExecutionOptions`

Kind: class

```py
class ScriptExecutionOptions(TypedDict, total=False):
    args: Any
    capabilities: ScriptCapabilities
    applyWrites: bool
    limits: ScriptLimits
```

## `NetworkHooks`

Kind: class

```py
class NetworkHooks(Protocol):
    def on_connect(self, context: ConnectHookContext, /) -> Any: ...
    def on_join_room(self, context: RoomHookContext, /) -> Any: ...
    def on_pull(self, context: ServeRequestContext, /) -> Any: ...
    def on_watch(self, context: ServeRequestContext, /) -> Any: ...
    def on_serve_result(self, context: ServeResultContext, result: Any, /) -> Any: ...
```

## `Primadb`

Kind: class

```py
class Primadb:
    def __init__(self, replica_id: Optional[str] = ...) -> None: ...
    def replica_id(self) -> str: ...
    def chain(self, root: str) -> Chain: ...
    def scope(self, root: str) -> Scope: ...
    def transaction(self, steps: Any) -> Any: ...
    def snapshot(self) -> Any: ...
    def snapshot_for_root(self, root: Optional[str] = ...) -> Any: ...
    def node_state(self, id: str) -> Any: ...
    def apply_node_state(self, node: Any) -> bool: ...
    def export_snapshot_json(self) -> str: ...
    def import_snapshot_json(self, payload: str) -> None: ...
    def merge_snapshot_json(self, payload: str) -> None: ...
    def pending_operations(self) -> Any: ...
    def pending_envelope(self) -> Any: ...
    def export_pending_operations_json(self) -> str: ...
    def drain_pending_operations(self) -> Any: ...
    def drain_pending_envelope(self) -> Any: ...
    def drain_pending_envelope_json(self) -> str: ...
    def apply_operations(self, operations: Any) -> int: ...
    def apply_envelope(self, envelope: Any) -> int: ...
    def apply_operations_json(self, payload: str) -> int: ...
    def open_durable_storage(self, config: DurableStorageConfig | dict[str, Any]) -> DurableStorageBinding: ...
    def open_blob_storage(self, config: BlobStorageConfig | dict[str, Any]) -> BlobStorageBinding: ...
    def close_durable_storage(self) -> None: ...
    def sync_storage(self) -> StorageSyncReport: ...
    def storage_recovery_report(self) -> Optional[StorageRecoveryReport]: ...
    def put_record(self, key: str, value: Any) -> None: ...
    def put_record_bytes(self, key: str, value: bytes) -> None: ...
    def put_record_blob(self, key: str, value: bytes, media_type: Optional[str] = ...) -> BlobRef: ...
    def get_record(self, key: str) -> Optional[RecordEntry]: ...
    def scan_records(self, scan: RecordScan | dict[str, Any]) -> RecordScanResult: ...
    def watch_records(self, scan: RecordScan | dict[str, Any]) -> RecordWatchSubscription: ...
    def apply_record_batch(self, batch: RecordBatch | dict[str, Any]) -> RecordBatchReport: ...
    def delete_record(self, key: str) -> None: ...
    def attach_node_script(self, path: Any, script: NodeScript | dict[str, Any]) -> None: ...
    def remove_node_script(self, path: Any, script_id: str) -> None: ...
    def node_scripts(self, path: Any) -> list[NodeScript]: ...
    def execute_node_scripts(
        self,
        path: Any,
        options: Optional[ScriptExecutionOptions | dict[str, Any]] = ...,
    ) -> Any: ...
    def register_user(self, alias: str, public_key: str, grants: list[UserGrant]) -> None: ...
    def authenticate_local_user(self, alias: str, secret_key: str, grants: list[UserGrant]) -> None: ...
    def set_require_signed_sync(self, required: bool) -> None: ...
    def set_snapshot_encryption_key(self, key_base64: str) -> None: ...
    def set_transport_encryption_key(self, key_base64: str) -> None: ...
    def connect_relay(self, config: RelayClientConfig | dict[str, Any]) -> WebSocketSync: ...
    def connect_mesh(self, config: MeshConfig | dict[str, Any]) -> WebRtcMesh: ...
    def set_network_hooks(self, hooks: NetworkHooks | dict[str, Any] | object | None) -> None: ...
    def clear_network_hooks(self) -> None: ...
```

## `derive_password_key`

Kind: function

```py
def derive_password_key(password: str, options: Optional[PasswordKeyDerivationOptions | dict[str, Any]] = ...) -> PasswordDerivedKey: ...
```

## `generate_identity`

Kind: function

```py
def generate_identity() -> IdentityKeyPair: ...
```

## `Scope`

Kind: class

```py
class Scope:
    def root(self) -> str: ...
    def configure(self, policy: Any) -> None: ...
    def policy(self) -> Any: ...
    def proposals(self) -> Any: ...
    def transaction(self, steps: Any, options: Optional[Any] = ...) -> Any: ...
```

## `PrimadbMoqFrame`

Kind: class

```py
@dataclass
class PrimadbMoqFrame:
    path: str
    track: str
    sequence: int
    payload: bytes
    def json(self) -> Any: ...
```

## `PrimadbMoqSession`

Kind: class

```py
class PrimadbMoqSession:
    db: Any
    path: str
    track: str
    def __init__(self, db: Any, *, path: str, track: str = ...) -> None: ...
    def subscribe_from(self, publisher: PrimadbMoqSession) -> None: ...
    def flush_pending(self) -> int: ...
    def receive_frame(self, frame: PrimadbMoqFrame) -> int: ...
    def close(self) -> None: ...
```

## `PrimadbMoqLoopback`

Kind: class

```py
class PrimadbMoqLoopback:
    publisher: PrimadbMoqSession
    subscriber: PrimadbMoqSession
    def __init__(self, publisher: PrimadbMoqSession, subscriber: PrimadbMoqSession) -> None: ...
    def flush(self) -> int: ...
    def close(self) -> None: ...
```

## `create_primadb_moq_loopback`

Kind: function

```py
def create_primadb_moq_loopback(
    *,
    publisher_db: Any,
    subscriber_db: Any,
    path: str,
    track: str = ...,
) -> PrimadbMoqLoopback: ...
```

## `Chain`

Kind: class

```py
class Chain:
    def field(self, key: str) -> Chain: ...
    def path(self) -> str: ...
    def put(self, value: Any) -> None: ...
    def put_bytes(self, value: bytes) -> None: ...
    def put_signed(self, value: Any, certificate: Optional[str] = ...) -> None: ...
    def once(self) -> Any: ...
    def once_bytes(self) -> Optional[bytes]: ...
    def unset(self) -> None: ...
    def set(self, value: Any) -> str: ...
    def set_signed(self, value: Any, certificate: Optional[str] = ...) -> str: ...
    def remove(self, value: Any) -> str: ...
    def put_blob(self, value: bytes, media_type: Optional[str] = ...) -> Any: ...
    def blob_ref(self) -> Any: ...
    def get_blob(self) -> Optional[bytes]: ...
    def map(self) -> Any: ...
    def query(self, spec: Any) -> Any: ...
    def first_query(self, spec: Any) -> Any: ...
    def scan(self, spec: Any) -> Any: ...
    def traverse(self, spec: Any) -> Any: ...
    def subscribe(self) -> Subscription: ...
    def watch_traverse(self, spec: Any) -> TraversalSubscription: ...
```

## `Subscription`

Kind: class

```py
class Subscription:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...
```

## `TraversalSubscription`

Kind: class

```py
class TraversalSubscription:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...
```

## `RecordWatchSubscription`

Kind: class

```py
class RecordWatchSubscription:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...
```

## `RelayServer`

Kind: class

```py
class RelayServer:
    @staticmethod
    def listen(config: Any) -> RelayServer: ...
    def bind_addr(self) -> str: ...
    def url(self) -> str: ...
    def client_count(self) -> int: ...
    def peer_count(self) -> int: ...
    def close(self) -> None: ...
```

## `RemoteWatch`

Kind: class

```py
class RemoteWatch:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...
```

## `WebSocketSync`

Kind: class

```py
class WebSocketSync:
    def is_connected(self) -> bool: ...
    def pending_count(self) -> int: ...
    def inflight_count(self) -> int: ...
    def known_peer_count(self) -> int: ...
    def recommended_peers(self) -> Any: ...
    def get(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def query(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def lex(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def records(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RecordScanResult: ...
    def node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def remote_get(self, peer_id: str, path: Any) -> Any: ...
    def remote_query(self, peer_id: str, path: Any, spec: Any) -> Any: ...
    def remote_lex(self, peer_id: str, path: Any, spec: Any) -> Any: ...
    def remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RecordScanResult: ...
    def remote_node(self, peer_id: str, id: str) -> Any: ...
    def remote_snapshot(self, peer_id: str, root: Optional[str] = ...) -> Any: ...
    def remote_transaction(
        self,
        peer_id: str,
        scope: str,
        steps: Any,
        options: Optional[Any] = ...,
    ) -> Any: ...
    def watch_get(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_map(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_query(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_lex(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_records(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_remote_get(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_map(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_query(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_lex(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_node(self, peer_id: str, id: str) -> RemoteWatch: ...
    def watch_remote_snapshot(self, peer_id: str, root: Optional[str] = ...) -> RemoteWatch: ...
    def flush_pending(self) -> int: ...
    def retry_inflight(self) -> int: ...
    def close(self) -> None: ...
```

## `WebRtcMesh`

Kind: class

```py
class WebRtcMesh:
    def peer_id(self) -> str: ...
    def signaling_mode(self) -> str: ...
    def relay_url(self) -> Optional[str]: ...
    def relay_connected(self) -> bool: ...
    def peer_count(self) -> int: ...
    def open_peer_count(self) -> int: ...
    def inflight_count(self) -> int: ...
    def recommended_peers(self) -> Any: ...
    def watch_get(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_map(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_query(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_lex(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_records(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_remote_get(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_map(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_query(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_lex(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_node(self, peer_id: str, id: str) -> RemoteWatch: ...
    def watch_remote_snapshot(self, peer_id: str, root: Optional[str] = ...) -> RemoteWatch: ...
    def flush_pending(self) -> int: ...
    def retry_inflight(self) -> int: ...
    def close(self) -> None: ...
```

## Remote interest selection

`WebSocketSync.get(...)`, `query(...)`, `lex(...)`, `records(...)`, `node(...)`, and `snapshot(...)` select a connected/recommended peer automatically. Relay and mesh watches are available through `watch_get(...)`, `watch_query(...)`, `watch_records(...)`, and the other `watch_*` helpers.

Pass `RemoteInterestPolicy` only when needed, for example `{"target": "peer", "peerId": "native:ledger", "requireCapability": True}`. The explicit `remote_*` and `watch_remote_*` methods still target a concrete peer id.

## Strict consistency and transactions

PrimaDB is eventual/local-first by default. Strict consistency APIs are opt-in and scoped to a graph root.

- `db.transaction(...)` applies a step array atomically on the local replica.
- `db.scope(root).configure(...)` stores a scope policy for that root.
- `scope.transaction(...)` runs a step array inside the scope and prefixes relative step paths with the scope root.
- `consistency: "local_transactional"` marks the scope as a transaction boundary without adding network coordination.
- `consistency: "coordinated"` requires the configured authority for canonical writes.
- Non-authority peers use `offlineWrites: "reject"` to fail immediately or `offlineWrites: "queue_provisional"` to store a durable local proposal that normal reads and watches do not treat as committed graph state.
- Relay sync clients expose `remote_transaction(...)` to submit a coordinated transaction to an authority peer.

The current coordinated implementation is a single-authority path. Quorum policies and strict authority read modes are represented in the policy model but are not full consensus or distributed multi-scope transactions yet.

## Traversal semantics

`Chain.traverse(...)` returns the current local traversal result immediately. With an active relay or mesh connection, missing linked nodes are scheduled for bounded background fetch.

`Chain.watch_traverse(...)` receives updated traversal results as fetched nodes merge into the local graph.

`TraversalResult.fetched` is the number of background node fetches scheduled by that evaluation.
