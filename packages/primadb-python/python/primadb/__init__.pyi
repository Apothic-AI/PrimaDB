from dataclasses import dataclass
from typing import Any, Literal, Optional, Protocol, TypedDict

class PresenceIdentity(TypedDict, total=False):
    publicKey: str
    alias: Optional[str]
    keyScheme: str
    sessionId: str
    claims: dict[str, str]
    issuedAtMillis: int
    expiresAtMillis: Optional[int]

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

RouteTarget = dict[str, Any]
RouteTransportKind = Literal["web_socket", "moq", "web_rtc", "broadcast_channel", "in_memory"]

class ApplicationRouteMessage(TypedDict, total=False):
    namespace: str
    protocol: str
    topic: Optional[str]
    body: Any
    metadata: dict[str, Any]

ApplicationRouteEvent = TypedDict(
    "ApplicationRouteEvent",
    {
        "routeId": str,
        "from": str,
        "channel": str,
        "target": RouteTarget,
        "issuedAtMillis": int,
        "receivedAtMillis": int,
        "transport": RouteTransportKind,
        "verifiedIdentity": Optional[VerifiedIdentity],
        "message": ApplicationRouteMessage,
    },
    total=False,
)

class ApplicationRouteFilter(TypedDict, total=False):
    namespace: Optional[str]
    protocol: Optional[str]
    topic: Optional[str]

class IdentityKeyPair(TypedDict):
    publicKey: str
    secretKey: str

class PasswordKeyDerivationParams(TypedDict, total=False):
    memoryCostKiB: int
    timeCost: int
    parallelism: int

class PasswordKeyDerivationOptions(PasswordKeyDerivationParams, total=False):
    saltBase64: Optional[str]

class PasswordDerivedKey(TypedDict):
    algorithm: str
    keyBase64: str
    saltBase64: str
    params: PasswordKeyDerivationParams

class UserGrant(TypedDict, total=False):
    root: str
    read: bool
    write: bool

class PeerHookContext(TypedDict, total=False):
    peerId: str
    replicaId: str
    transport: str
    identity: Optional[PresenceIdentity]
    capabilities: list[str]
    topics: list[str]
    metadata: dict[str, str]

class ConnectHookContext(TypedDict, total=False):
    peer: PeerHookContext
    transport: str
    relayUrl: Optional[str]
    verifiedIdentity: Optional[VerifiedIdentity]

class RoomHookContext(TypedDict, total=False):
    peerId: str
    room: str
    transport: str
    peer: Optional[PeerHookContext]
    verifiedIdentity: Optional[VerifiedIdentity]

class ServeRequestContext(TypedDict, total=False):
    peerId: str
    transport: str
    requestId: Optional[str]
    watchId: Optional[str]
    request: Any
    verifiedIdentity: Optional[VerifiedIdentity]

class ServeResultContext(TypedDict, total=False):
    peerId: str
    transport: str
    requestId: Optional[str]
    watchId: Optional[str]
    request: Any
    initial: bool
    verifiedIdentity: Optional[VerifiedIdentity]

class SessionAuthConfig(TypedDict, total=False):
    requireAuthenticatedPeers: bool
    trustedPublicKeys: list[str]
    trustedAliases: list[str]
    challengeTimeoutMs: int
    sessionTtlMs: int
    allowUnauthenticatedPresence: bool

class RelayClientConfig(TypedDict, total=False):
    url: str
    retryIntervalMs: int
    sessionAuth: SessionAuthConfig

MoqDraft = Literal["draft07", "draft14", "draft_latest"]

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

class WebSocketRelayEndpointConfig(RelayClientConfig, total=False):
    kind: Literal["web_socket"]

class MoqRelayEndpointConfig(MoqRelayClientConfig, total=False):
    kind: Literal["moq"]

RelayEndpointConfig = WebSocketRelayEndpointConfig | MoqRelayEndpointConfig

class RelayServerConfig(TypedDict, total=False):
    bind: str
    moq: Optional[MoqRelayClientConfig]

class IceServerConfig(TypedDict, total=False):
    urls: str | list[str]
    username: Optional[str]
    credential: Optional[str]

class MeshConfig(TypedDict, total=False):
    room: str
    signaling: str
    relayUrl: Optional[str]
    relayEndpoint: Optional[RelayEndpointConfig]
    retryIntervalMs: int
    iceServers: list[IceServerConfig]
    sessionAuth: SessionAuthConfig

SegmentDurability = Literal["full", "data", "relaxed"]

class SegmentLockExclusive(TypedDict):
    kind: Literal["exclusive"]

class SegmentLockWait(TypedDict):
    kind: Literal["wait"]
    timeoutMillis: int

class SegmentLockDisabled(TypedDict):
    kind: Literal["disabled"]

SegmentLockMode = SegmentLockExclusive | SegmentLockWait | SegmentLockDisabled

class SnapshotFileStorageConfig(TypedDict):
    kind: Literal["snapshot_file"]
    path: str

class SegmentFilesStorageConfig(TypedDict, total=False):
    kind: Literal["segment_files"]
    directory: str
    journalRetention: int
    durability: SegmentDurability
    lockMode: SegmentLockMode

DurableStorageConfig = SnapshotFileStorageConfig | SegmentFilesStorageConfig

class DurableStorageBinding(TypedDict, total=False):
    backend: str
    incremental: bool
    loadedExisting: bool
    autoPersist: bool
    durability: SegmentDurability
    lockMode: SegmentLockMode

class MemoryBlobStorageConfig(TypedDict):
    kind: Literal["memory"]

class FilesBlobStorageConfig(TypedDict, total=False):
    kind: Literal["files"]
    directory: str
    durability: SegmentDurability

BlobStorageConfig = MemoryBlobStorageConfig | FilesBlobStorageConfig

class BlobStorageBinding(TypedDict, total=False):
    backend: str
    contentAddressed: bool
    durability: SegmentDurability

class BlobRef(TypedDict, total=False):
    id: str
    bytes: int
    mediaType: Optional[str]

class RecordJsonValue(TypedDict):
    kind: Literal["json"]
    value: Any

class RecordBytesValue(TypedDict):
    kind: Literal["bytes"]
    value: str

class RecordBlobValue(TypedDict):
    kind: Literal["blob"]
    value: BlobRef

RecordValue = RecordJsonValue | RecordBytesValue | RecordBlobValue

class RecordEntry(TypedDict):
    key: str
    value: RecordValue

class RecordScan(TypedDict, total=False):
    prefix: Optional[str]
    startAt: Optional[str]
    startAfter: Optional[str]
    endAt: Optional[str]
    endBefore: Optional[str]
    reverse: bool
    limit: Optional[int]
    cursor: Optional[str]

class RecordScanResult(TypedDict, total=False):
    entries: list[RecordEntry]
    nextCursor: Optional[str]

RemoteInterestTarget = Literal["any", "peer", "peers"]

class RemoteInterestPolicy(TypedDict, total=False):
    target: RemoteInterestTarget
    peerId: Optional[str]
    peers: list[str]
    requireCapability: bool

class RecordPutMutation(TypedDict):
    kind: Literal["put"]
    key: str
    value: RecordValue

class RecordDeleteMutation(TypedDict):
    kind: Literal["delete"]
    key: str

class RecordDeleteRangeMutation(TypedDict):
    kind: Literal["delete_range"]
    scan: RecordScan

RecordMutation = RecordPutMutation | RecordDeleteMutation | RecordDeleteRangeMutation

class RecordExistsPrecondition(TypedDict):
    kind: Literal["exists"]
    key: str

class RecordAbsentPrecondition(TypedDict):
    kind: Literal["absent"]
    key: str

class RecordValuePrecondition(TypedDict):
    kind: Literal["value"]
    key: str
    value: RecordValue

RecordPrecondition = RecordExistsPrecondition | RecordAbsentPrecondition | RecordValuePrecondition

class RecordBatch(TypedDict, total=False):
    preconditions: list[RecordPrecondition]
    mutations: list[RecordMutation]

class RecordBatchReport(TypedDict):
    preconditions: int
    puts: int
    deletes: int
    rangeDeletes: int
    operationCount: int

VectorMetric = Literal["cosine", "l2", "dot"]
VectorBackendKind = Literal["exact", "edgevec"]
VectorManagerState = Literal["ready", "catching_up", "rebuilding", "stale", "failed"]
VectorStalePolicy = Literal["fallback_exact", "allow_stale", "error"]

class VectorHnswConfig(TypedDict, total=False):
    m: Optional[int]
    efConstruction: Optional[int]
    efSearch: Optional[int]
    tombstoneRebuildRatio: Optional[float]

class VectorChunkingConfig(TypedDict):
    chunkBytes: int

class VectorCollectionConfig(TypedDict, total=False):
    dim: int
    metric: VectorMetric
    backend: Optional[VectorBackendKind]
    hnsw: Optional[VectorHnswConfig]
    chunking: VectorChunkingConfig

class VectorEntry(TypedDict, total=False):
    id: str
    vector: list[float]
    metadata: Any
    writeId: str
    checksum: str

class VectorMetadataFilter(TypedDict, total=False):
    eq: dict[str, Any]
    prefix: dict[str, str]
    exists: list[str]

class VectorFilter(TypedDict, total=False):
    idPrefix: Optional[str]
    ids: list[str]
    metadata: Optional[VectorMetadataFilter]

class VectorSearchSpec(TypedDict, total=False):
    limit: int
    ef: Optional[int]
    filter: Optional[VectorFilter]
    includeVector: bool
    includeMetadata: bool
    exact: bool
    stalePolicy: VectorStalePolicy

class VectorMatch(TypedDict, total=False):
    id: str
    distance: float
    metadata: Any
    vector: list[float]

class VectorSearchResult(TypedDict, total=False):
    matches: list[VectorMatch]
    exact: bool
    backend: VectorBackendKind
    state: VectorManagerState
    stale: bool
    approximateReason: Optional[str]

class StorageSyncReport(TypedDict):
    backend: str
    durability: str
    synced: bool

class StorageRecoveryReport(TypedDict):
    appliedTransactions: int
    skippedTransactions: int
    removedPendingFiles: int
    removedTempFiles: int
    quarantinedFiles: int

class ScriptPathGrant(TypedDict, total=False):
    root: str
    segments: list[str]
    recursive: bool

class ScriptCapabilities(TypedDict, total=False):
    read: list[ScriptPathGrant]
    query: list[ScriptPathGrant]
    traverse: list[ScriptPathGrant]
    write: list[ScriptPathGrant]
    transaction: list[ScriptPathGrant]

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

class ScriptExecutionOptions(TypedDict, total=False):
    args: Any
    capabilities: ScriptCapabilities
    applyWrites: bool
    limits: ScriptLimits

class NetworkHooks(Protocol):
    def on_connect(self, context: ConnectHookContext, /) -> Any: ...
    def on_join_room(self, context: RoomHookContext, /) -> Any: ...
    def on_pull(self, context: ServeRequestContext, /) -> Any: ...
    def on_watch(self, context: ServeRequestContext, /) -> Any: ...
    def on_serve_result(self, context: ServeResultContext, result: Any, /) -> Any: ...

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

def derive_password_key(password: str, options: Optional[PasswordKeyDerivationOptions | dict[str, Any]] = ...) -> PasswordDerivedKey: ...
def generate_identity() -> IdentityKeyPair: ...

class Scope:
    def root(self) -> str: ...
    def configure(self, policy: Any) -> None: ...
    def policy(self) -> Any: ...
    def proposals(self) -> Any: ...
    def transaction(self, steps: Any, options: Optional[Any] = ...) -> Any: ...

@dataclass
class PrimadbMoqFrame:
    path: str
    track: str
    sequence: int
    payload: bytes
    def json(self) -> Any: ...

class PrimadbMoqApplicationRouteSubscription:
    filter: dict[str, Any]
    def next(self) -> Optional[dict[str, Any]]: ...
    def try_next(self) -> Optional[dict[str, Any]]: ...
    def drain(self) -> list[dict[str, Any]]: ...
    def close(self) -> None: ...

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

class PrimadbMoqLoopback:
    publisher: PrimadbMoqSession
    subscriber: PrimadbMoqSession
    def __init__(self, publisher: PrimadbMoqSession, subscriber: PrimadbMoqSession) -> None: ...
    def flush(self) -> int: ...
    def close(self) -> None: ...

def create_primadb_moq_loopback(
    *,
    publisher_db: Any,
    subscriber_db: Any,
    path: str,
    track: str = ...,
    channel: str = ...,
) -> PrimadbMoqLoopback: ...

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

class Subscription:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...

class TraversalSubscription:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...

class RecordWatchSubscription:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...

class VectorWatchSubscription:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...

class RelayServer:
    @staticmethod
    def listen(config: Any) -> RelayServer: ...
    def bind_addr(self) -> str: ...
    def url(self) -> str: ...
    def client_count(self) -> int: ...
    def peer_count(self) -> int: ...
    def close(self) -> None: ...

class RemoteWatch:
    def next(self) -> Any: ...
    def try_next(self) -> Any: ...
    def close(self) -> None: ...

class ApplicationRouteSubscription:
    def next(self) -> Optional[ApplicationRouteEvent]: ...
    def try_next(self) -> Optional[ApplicationRouteEvent]: ...
    def drain(self) -> list[ApplicationRouteEvent]: ...
    def close(self) -> None: ...

class RemotePeerFailure(TypedDict):
    peerId: str
    transport: RouteTransportKind
    message: str

class RemotePeerRecords(TypedDict):
    peerId: str
    transport: RouteTransportKind
    result: RecordScanResult

class RemoteRecordConflictSource(TypedDict):
    peerId: str
    transport: RouteTransportKind
    contentHash: str

class RemoteRecordConflict(TypedDict):
    key: str
    winnerPeerId: str
    winnerHash: str
    sources: list[RemoteRecordConflictSource]

class RemoteRecordsFanIn(TypedDict):
    requestId: str
    records: list[RemotePeerRecords]
    failures: list[RemotePeerFailure]
    merged: RecordScanResult
    conflicts: list[RemoteRecordConflict]

RemoteFanInWatchEvent = dict[str, Any]

class RemoteFanInWatch:
    def next(self) -> Optional[RemoteFanInWatchEvent]: ...
    def try_next(self) -> Optional[RemoteFanInWatchEvent]: ...
    def drain(self) -> list[RemoteFanInWatchEvent]: ...
    def close(self) -> None: ...

class WebSocketSync:
    def is_connected(self) -> bool: ...
    def pending_count(self) -> int: ...
    def inflight_count(self) -> int: ...
    def known_peer_count(self) -> int: ...
    def recommended_peers(self) -> Any: ...
    def publish_application(self, message: ApplicationRouteMessage | dict[str, Any], target: Optional[RouteTarget] = ...) -> Any: ...
    def send_application(self, namespace: str, protocol: str, topic: Optional[str], body: Any, metadata: Optional[dict[str, Any]] = ..., target: Optional[RouteTarget] = ...) -> Any: ...
    def subscribe_applications(self, filter: Optional[ApplicationRouteFilter] = ...) -> ApplicationRouteSubscription: ...
    def get(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def query(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def lex(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def records(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RecordScanResult: ...
    def records_fan_in(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteRecordsFanIn: ...
    def vector_search(self, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> VectorSearchResult: ...
    def node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> Any: ...
    def remote_get(self, peer_id: str, path: Any) -> Any: ...
    def remote_query(self, peer_id: str, path: Any, spec: Any) -> Any: ...
    def remote_lex(self, peer_id: str, path: Any, spec: Any) -> Any: ...
    def remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RecordScanResult: ...
    def remote_vector_search(self, peer_id: str, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any]) -> VectorSearchResult: ...
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
    def watch_node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_remote_get(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_map(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_query(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_lex(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_vector_search(self, peer_id: str, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_node(self, peer_id: str, id: str) -> RemoteWatch: ...
    def watch_remote_snapshot(self, peer_id: str, root: Optional[str] = ...) -> RemoteWatch: ...
    def flush_pending(self) -> int: ...
    def retry_inflight(self) -> int: ...
    def close(self) -> None: ...

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
    def subscribe_applications(self, filter: Optional[ApplicationRouteFilter] = ...) -> ApplicationRouteSubscription: ...
    def records_fan_in(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteRecordsFanIn: ...
    def watch_get(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_map(self, path: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_query(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_lex(self, path: Any, spec: Any, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_records(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_records_fan_in(self, scan: RecordScan | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteFanInWatch: ...
    def watch_vector_search(self, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any], policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_node(self, id: str, policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_snapshot(self, root: Optional[str] = ..., policy: Optional[RemoteInterestPolicy] = ...) -> RemoteWatch: ...
    def watch_remote_get(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_map(self, peer_id: str, path: Any) -> RemoteWatch: ...
    def watch_remote_query(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_lex(self, peer_id: str, path: Any, spec: Any) -> RemoteWatch: ...
    def watch_remote_records(self, peer_id: str, scan: RecordScan | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_vector_search(self, peer_id: str, collection: str, query: list[float], spec: VectorSearchSpec | dict[str, Any]) -> RemoteWatch: ...
    def watch_remote_node(self, peer_id: str, id: str) -> RemoteWatch: ...
    def watch_remote_snapshot(self, peer_id: str, root: Optional[str] = ...) -> RemoteWatch: ...
    def flush_pending(self) -> int: ...
    def retry_inflight(self) -> int: ...
    def close(self) -> None: ...
