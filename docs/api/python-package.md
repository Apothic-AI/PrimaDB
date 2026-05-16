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

## `RouteTarget`

Kind: type alias

```py
RouteTarget = dict[str, Any]
```

## `RouteTransportKind`

Kind: type alias

```py
RouteTransportKind = Literal["web_socket", "moq", "web_rtc", "broadcast_channel", "in_memory"]
```

## `ApplicationRouteMessage`

Kind: class

```py
class ApplicationRouteMessage(TypedDict, total=False):
    namespace: str
    protocol: str
    topic: Optional[str]
    body: Any
    metadata: dict[str, Any]
```

## `ApplicationRouteAuthStatus`

Kind: type alias

```py
ApplicationRouteAuthStatus = Literal[
```

## `ApplicationRouteContext`

Kind: class

```py
class ApplicationRouteContext(TypedDict, total=False):
    sourcePeerId: str
    transport: RouteTransportKind
    underlayId: Optional[str]
    direct: bool
    relayRouted: bool
    gatewayRouted: bool
    gatewayPeerId: Optional[str]
    authStatus: ApplicationRouteAuthStatus
    provenance: list[str]
```

## `ApplicationRouteEvent`

Kind: type alias

```py
ApplicationRouteEvent = TypedDict(
```

## `ApplicationRouteFilter`

Kind: class

```py
class ApplicationRouteFilter(TypedDict, total=False):
    namespace: Optional[str]
    protocol: Optional[str]
    topic: Optional[str]
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

## `MoqDraft`

Kind: type alias

```py
MoqDraft = Literal["draft07", "draft14", "draft_latest"]
```

## `MoqRelayClientConfig`

Kind: class

```py
class MoqRelayClientConfig(TypedDict, total=False):
    url: str
    path: str
    track: str
    channel: str
    subscribe: list[str]
    draft: MoqDraft
    retryIntervalMs: int
    tlsDisableVerify: bool
    sessionAuth: SessionAuthConfig
```

## `WebSocketRelayEndpointConfig`

Kind: class

```py
class WebSocketRelayEndpointConfig(RelayClientConfig, total=False):
    kind: Literal["web_socket"]
```

## `MoqRelayEndpointConfig`

Kind: class

```py
class MoqRelayEndpointConfig(MoqRelayClientConfig, total=False):
    kind: Literal["moq"]
```

## `RelayEndpointConfig`

Kind: type alias

```py
RelayEndpointConfig = WebSocketRelayEndpointConfig | MoqRelayEndpointConfig
```

## `RelayServerConfig`

Kind: class

```py
class RelayServerConfig(TypedDict, total=False):
    bind: str
    moq: Optional[MoqRelayClientConfig]
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
    relayEndpoint: Optional[RelayEndpointConfig]
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

## `VectorMetric`

Kind: type alias

```py
VectorMetric = Literal["cosine", "l2", "dot"]
```

## `VectorBackendKind`

Kind: type alias

```py
VectorBackendKind = Literal["exact", "edgevec"]
```

## `VectorManagerState`

Kind: type alias

```py
VectorManagerState = Literal["ready", "catching_up", "rebuilding", "stale", "failed"]
```

## `VectorStalePolicy`

Kind: type alias

```py
VectorStalePolicy = Literal["fallback_exact", "allow_stale", "error"]
```

## `VectorHnswConfig`

Kind: class

```py
class VectorHnswConfig(TypedDict, total=False):
    m: Optional[int]
    efConstruction: Optional[int]
    efSearch: Optional[int]
    tombstoneRebuildRatio: Optional[float]
```

## `VectorChunkingConfig`

Kind: class

```py
class VectorChunkingConfig(TypedDict):
    chunkBytes: int
```

## `VectorCollectionConfig`

Kind: class

```py
class VectorCollectionConfig(TypedDict, total=False):
    dim: int
    metric: VectorMetric
    backend: Optional[VectorBackendKind]
    hnsw: Optional[VectorHnswConfig]
    chunking: VectorChunkingConfig
```

## `VectorEntry`

Kind: class

```py
class VectorEntry(TypedDict, total=False):
    id: str
    vector: list[float]
    metadata: Any
    writeId: str
    checksum: str
```

## `VectorMetadataFilter`

Kind: class

```py
class VectorMetadataFilter(TypedDict, total=False):
    eq: dict[str, Any]
    prefix: dict[str, str]
    exists: list[str]
```

## `VectorFilter`

Kind: class

```py
class VectorFilter(TypedDict, total=False):
    idPrefix: Optional[str]
    ids: list[str]
    metadata: Optional[VectorMetadataFilter]
```

## `VectorSearchSpec`

Kind: class

```py
class VectorSearchSpec(TypedDict, total=False):
    limit: int
    ef: Optional[int]
    filter: Optional[VectorFilter]
    includeVector: bool
    includeMetadata: bool
    exact: bool
    stalePolicy: VectorStalePolicy
```

## `VectorMatch`

Kind: class

```py
class VectorMatch(TypedDict, total=False):
    id: str
    distance: float
    metadata: Any
    vector: list[float]
```

## `VectorSearchResult`

Kind: class

```py
class VectorSearchResult(TypedDict, total=False):
    matches: list[VectorMatch]
    exact: bool
    backend: VectorBackendKind
    state: VectorManagerState
    stale: bool
    approximateReason: Optional[str]
```

## `TextFieldConfig`

Kind: class

```py
class TextFieldConfig(TypedDict, total=False):
    name: str
    weight: float
    indexed: bool
    stored: bool
```

## `TextAnalyzerConfig`

Kind: class

```py
class TextAnalyzerConfig(TypedDict, total=False):
    kind: Literal["simple"]
    lowercase: bool
    unicodeNormalization: Optional[str]
    stopwords: Optional[str]
    stemming: Optional[str]
    version: int
```

## `TextCollectionConfig`

Kind: class

```py
class TextCollectionConfig(TypedDict, total=False):
    fields: list[TextFieldConfig]
    analyzer: TextAnalyzerConfig
    k1: float
    b: float
    metadata: dict[str, Any]
```

## `TextDocument`

Kind: class

```py
class TextDocument(TypedDict, total=False):
    id: str
    fields: dict[str, str]
    metadata: dict[str, Any]
```

## `TextSearchSource`

Kind: type alias

```py
TextSearchSource = str | dict[str, Any]
```

## `TextSearchSpec`

Kind: class

```py
class TextSearchSpec(TypedDict, total=False):
    limit: Optional[int]
    offset: Optional[int]
    fields: Optional[list[str]]
    includeMetadata: bool
    includeSnippets: bool
    explain: bool
    exact: bool
    stalePolicy: Literal["allow", "refresh", "reject"]
    candidateLimit: Optional[int]
    candidatePolicy: Literal["reject_paginated_query", "allow_preselected_candidates"]
```

## `TextSearchMatch`

Kind: class

```py
class TextSearchMatch(TypedDict, total=False):
    id: str
    score: float
    fieldHits: list[dict[str, Any]]
    metadata: Optional[dict[str, Any]]
    snippets: Optional[list[dict[str, str]]]
    explanation: Optional[str]
```

## `TextSearchResult`

Kind: class

```py
class TextSearchResult(TypedDict, total=False):
    source: Any
    query: str
    matches: list[TextSearchMatch]
    backend: Literal["exact"]
    exact: bool
    stale: bool
    candidateCount: int
    searchedCount: int
    truncatedCandidates: bool
    scoreScope: Literal["collection", "candidate_set", "peer_local"]
```

## `TextIndexStats`

Kind: class

```py
class TextIndexStats(TypedDict):
    documentCount: int
    deletedCount: int
    termCount: int
    totalTerms: int
    averageFieldLength: int
    state: Literal["ready", "rebuilding", "stale", "failed"]
    sourceHash: str
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
    def create_vector_collection(self, name: str, config: VectorCollectionConfig | dict[str, Any]) -> None: ...
    def put_vector(self, collection: str, id: str, vector: list[float], metadata: Optional[Any] = ...) -> None: ...
    def delete_vector(self, collection: str, id: str) -> None: ...
    def get_vector(self, collection: str, id: str) -> Optional[VectorEntry]: ...
    def search_vectors(self, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any]) -> VectorSearchResult: ...
    def watch_vector_search(self, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any]) -> VectorWatchSubscription: ...
    def create_text_collection(self, name: str, config: TextCollectionConfig | dict[str, Any]) -> None: ...
    def put_text_document(self, collection: str, document: TextDocument | dict[str, Any]) -> None: ...
    def delete_text_document(self, collection: str, id: str) -> None: ...
    def get_text_document(self, collection: str, id: str) -> Optional[TextDocument]: ...
    def text_search(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any]) -> TextSearchResult: ...
    def watch_text_search(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any]) -> TextWatchSubscription: ...
    def text_index_stats(self, collection: str) -> TextIndexStats: ...
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

## `PrimadbMoqApplicationRouteSubscription`

Kind: class

```py
class PrimadbMoqApplicationRouteSubscription:
    filter: dict[str, Any]
    def next(self) -> Optional[dict[str, Any]]: ...
    def try_next(self) -> Optional[dict[str, Any]]: ...
    def drain(self) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...
```

## `PrimadbMoqSession`

Kind: class

```py
class PrimadbMoqSession:
    db: Any
    path: str
    track: str
    channel: str
    peer_id: str
    def __init__(
        self,
        db: Any,
        *,
        path: str,
        track: str = ...,
        channel: str = ...,
        peer_id: Optional[str] = ...,
    ) -> None: ...
    def subscribe_from(self, publisher: PrimadbMoqSession) -> None: ...
    def on_route(self, handler: Any) -> Any: ...
    def add_accepted_peer_id(self, peer_id: str) -> Any: ...
    def known_peers(self) -> list[dict[str, Any]]: ...
    def recommended_peers(self) -> list[dict[str, Any]]: ...
    def publish_application(self, message: dict[str, Any], target: Optional[dict[str, Any]] = ...) -> int: ...
    def send_application(
        self,
        namespace: str,
        protocol: str,
        topic: Optional[str],
        body: Any,
        metadata: Optional[dict[str, Any]] = ...,
        target: Optional[dict[str, Any]] = ...,
    ) -> int: ...
    def subscribe_applications(
        self,
        filter: Optional[dict[str, Any]] = ...,
    ) -> PrimadbMoqApplicationRouteSubscription: ...
    def next_application(self, filter: Optional[dict[str, Any]] = ...) -> Optional[dict[str, Any]]: ...
    def try_next_application(self, filter: Optional[dict[str, Any]] = ...) -> Optional[dict[str, Any]]: ...
    def drain_applications(self, filter: Optional[dict[str, Any]] = ...) -> list[dict[str, Any]]: ...
    def create_route(
        self,
        payload: dict[str, Any],
        target: Optional[dict[str, Any]] = ...,
        reply_to: Optional[str] = ...,
    ) -> dict[str, Any]: ...
    def send_route(self, route: dict[str, Any]) -> int: ...
    def announce_presence(self) -> int: ...
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
    channel: str = ...,
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

## `VectorWatchSubscription`

Kind: class

```py
class VectorWatchSubscription:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...
```

## `TextWatchSubscription`

Kind: class

```py
class TextWatchSubscription:
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

## `ApplicationRouteSubscription`

Kind: class

```py
class ApplicationRouteSubscription:
    def next(self) -> Optional[ApplicationRouteEvent]: ...
    def try_next(self) -> Optional[ApplicationRouteEvent]: ...
    def drain(self) -> list[ApplicationRouteEvent]: ...
    def close(self) -> None: ...
```

## `RemotePeerFailure`

Kind: class

```py
class RemotePeerFailure(TypedDict):
    peerId: str
    transport: RouteTransportKind
    message: str
```

## `RemotePeerRecords`

Kind: class

```py
class RemotePeerRecords(TypedDict):
    peerId: str
    transport: RouteTransportKind
    result: RecordScanResult
```

## `RemotePeerTextSearch`

Kind: class

```py
class RemotePeerTextSearch(TypedDict):
    peerId: str
    transport: RouteTransportKind
    result: TextSearchResult
```

## `RemoteRecordConflictSource`

Kind: class

```py
class RemoteRecordConflictSource(TypedDict):
    peerId: str
    transport: RouteTransportKind
    contentHash: str
```

## `RemoteRecordConflict`

Kind: class

```py
class RemoteRecordConflict(TypedDict):
    key: str
    winnerPeerId: str
    winnerHash: str
    sources: list[RemoteRecordConflictSource]
```

## `RemoteRecordsFanIn`

Kind: class

```py
class RemoteRecordsFanIn(TypedDict):
    requestId: str
    records: list[RemotePeerRecords]
    failures: list[RemotePeerFailure]
    merged: RecordScanResult
    conflicts: list[RemoteRecordConflict]
```

## `RemoteTextSearchFanIn`

Kind: class

```py
class RemoteTextSearchFanIn(TypedDict):
    requestId: str
    results: list[RemotePeerTextSearch]
    failures: list[RemotePeerFailure]
    merged: TextSearchResult
```

## `RemoteFanInWatchEvent`

Kind: type alias

```py
RemoteFanInWatchEvent = dict[str, Any]
```

## `RemoteFanInWatch`

Kind: class

```py
class RemoteFanInWatch:
    def next(self) -> Optional[RemoteFanInWatchEvent]: ...
    def try_next(self) -> Optional[RemoteFanInWatchEvent]: ...
    def drain(self) -> list[RemoteFanInWatchEvent]: ...
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
    def publish_application(self, message: ApplicationRouteMessage | dict[str, Any], target: Optional[RouteTarget] = ...) -> Any: ...
    def send_application(self, namespace: str, protocol: str, topic: Optional[str], body: Any, metadata: Optional[dict[str, Any]] = ..., target: Optional[RouteTarget] = ...) -> Any: ...
    def send_route_envelope(self, route: dict[str, Any]) -> Any: ...
    def subscribe_applications(self, filter: Optional[ApplicationRouteFilter] = ...) -> ApplicationRouteSubscription: ...
    def get(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def query(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def lex(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def records(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RecordScanResult: ...
    def records_fan_in(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteRecordsFanIn: ...
    def vector_search(self, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> VectorSearchResult: ...
    def text_search(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> TextSearchResult: ...
    def text_search_fan_in(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteTextSearchFanIn: ...
    def node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def remote_get(self, peer_id: str, path: Any) -> Any: ...
    def remote_query(self, peer_id: str, path: Any, spec: Any) -> Any: ...
    def remote_lex(self, peer_id: str, path: Any, spec: Any) -> Any: ...
    def remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RecordScanResult: ...
    def remote_vector_search(self, peer_id: str, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any]) -> VectorSearchResult: ...
    def remote_text_search(self, peer_id: str, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any]) -> TextSearchResult: ...
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
    def watch_records_fan_in(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteFanInWatch: ...
    def watch_vector_search(self, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_text_search(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_text_search_fan_in(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteFanInWatch: ...
    def watch_node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_remote_get(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_map(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_query(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_lex(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_vector_search(self, peer_id: str, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_text_search(self, peer_id: str, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any]) -> RemoteWatch: ...
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
    def publish_application(self, message: ApplicationRouteMessage | dict[str, Any], target: Optional[RouteTarget] = ...) -> Any: ...
    def send_application(self, namespace: str, protocol: str, topic: Optional[str], body: Any, metadata: Optional[dict[str, Any]] = ..., target: Optional[RouteTarget] = ...) -> Any: ...
    def send_route_envelope(self, route: dict[str, Any]) -> Any: ...
    def subscribe_applications(self, filter: Optional[ApplicationRouteFilter] = ...) -> ApplicationRouteSubscription: ...
    def records_fan_in(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteRecordsFanIn: ...
    def text_search_fan_in(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteTextSearchFanIn: ...
    def watch_get(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_map(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_query(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_lex(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_records(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_records_fan_in(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteFanInWatch: ...
    def watch_vector_search(self, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_text_search(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_text_search_fan_in(self, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteFanInWatch: ...
    def watch_node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_remote_get(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_map(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_query(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_lex(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_vector_search(self, peer_id: str, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_text_search(self, peer_id: str, source: TextSearchSource, query: str, spec: TextSearchSpec | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_node(self, peer_id: str, id: str) -> RemoteWatch: ...
    def watch_remote_snapshot(self, peer_id: str, root: Optional[str] = ...) -> RemoteWatch: ...
    def flush_pending(self) -> int: ...
    def retry_inflight(self) -> int: ...
    def close(self) -> None: ...
```

## Remote interest selection

`WebSocketSync.get(...)`, `query(...)`, `lex(...)`, `records(...)`, `node(...)`, and `snapshot(...)` select a connected/recommended peer automatically. Relay and mesh watches are available through `watch_get(...)`, `watch_query(...)`, `watch_records(...)`, and the other `watch_*` helpers.

Pass `RemoteInterestPolicy` only when needed, for example `{"target": "peer", "peerId": "native:ledger", "requireCapability": True}`. The explicit `remote_*` and `watch_remote_*` methods still target a concrete peer id.

## Application routes

Application route APIs carry caller-defined messages inside `RoutePayload::Application` / `{ kind: "application" }` while preserving the surrounding `RouteEnvelope` metadata.

Use `publish_application(...)` when the caller has already assembled an application message, or `send_application(...)` for the namespace/protocol/topic/body convenience shape.

`subscribe_applications(...)` returns a filtered subscription with deterministic `next`/`tryNext`/`drain`/`close` behavior. Received events include route id, source peer, channel, target, receive time, transport kind where available, verified identity when available, and an `ApplicationRouteContext` with underlay/provenance/auth-status metadata.

`RouteOverlaySession` and `PrimadbRouteOverlaySession` own multiple route underlays, apply a send policy, report per-underlay delivery attempts, and dedupe duplicate application events delivered through multiple paths. Native relay/MoQ/WebRTC handles expose route-overlay underlay adapters so callers can send once instead of manually looping over transports.

Application streams use the same route machinery with stream id, sequence number, chunk data, final flags, close/error/ack/nack frame kinds, and ordered reassembly. They are intended for larger trusted app messages that should not require callers to invent another envelope above `RouteEnvelope`.

These APIs are RouteEnvelope-level. They do not expose raw WebSocket, WebRTC, WebTransport, or MoQ socket handles.

## Record fan-in

`records_fan_in(...)` sends a record scan to every currently reachable peer that matches the supplied `RemoteInterestPolicy` instead of selecting one ambient peer.

`watch_records_fan_in(...)` keeps child watches open across all matching peers and emits source-tagged updates plus partial failures. Closing the returned watch cancels all child watches.

Fan-in results include per-peer records, a deterministic merged result, conflict metadata, and partial failure diagnostics. Per-peer source metadata is preserved so callers can apply their own trust or dedupe policy above the built-in deterministic merge.

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
