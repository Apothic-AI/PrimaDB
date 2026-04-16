use crate::DatabaseSnapshot;
use crate::clock::now_millis;
use crate::sync::{PullRequest, PullResponse, WatchEvent, WatchRequest, stable_content_hash};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum RouteTarget {
    Broadcast,
    Peer(String),
    Topic(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerPresence {
    pub peer_id: String,
    pub replica_id: String,
    pub transport: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub topics: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeerRecommendation {
    pub peer: PeerPresence,
    #[serde(default)]
    pub relay_urls: Vec<String>,
    #[serde(default)]
    pub score: u16,
    #[serde(default = "now_millis")]
    pub discovered_at_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RouteBatchItem {
    Sync {
        encoding: String,
        payload: JsonValue,
    },
    Presence {
        peer: PeerPresence,
    },
    Signal {
        room: String,
        payload: JsonValue,
    },
    SnapshotRequest {
        root: Option<String>,
    },
    SnapshotResponse {
        root: Option<String>,
        snapshot: DatabaseSnapshot,
    },
    PullRequest {
        request: PullRequest,
    },
    PullResponse {
        response: PullResponse,
    },
    WatchRequest {
        request: WatchRequest,
    },
    WatchEvent {
        event: WatchEvent,
    },
    PeerExchange {
        peers: Vec<PeerRecommendation>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoutePayload {
    Sync {
        encoding: String,
        payload: JsonValue,
    },
    Presence {
        peer: PeerPresence,
    },
    Signal {
        room: String,
        payload: JsonValue,
    },
    SnapshotRequest {
        root: Option<String>,
    },
    SnapshotResponse {
        root: Option<String>,
        snapshot: DatabaseSnapshot,
    },
    PullRequest {
        request: PullRequest,
    },
    PullResponse {
        response: PullResponse,
    },
    WatchRequest {
        request: WatchRequest,
    },
    WatchEvent {
        event: WatchEvent,
    },
    PeerExchange {
        peers: Vec<PeerRecommendation>,
    },
    Batch {
        items: Vec<RouteBatchItem>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouteEnvelope {
    pub route_id: String,
    pub from: String,
    pub channel: String,
    pub target: RouteTarget,
    pub ttl: u8,
    pub hops: u8,
    pub issued_at_millis: u64,
    #[serde(default)]
    pub reply_to: Option<String>,
    #[serde(default)]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub seen_by: Vec<String>,
    pub payload: RoutePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouterConfig {
    pub peer_id: String,
    pub default_channel: String,
    pub default_ttl: u8,
    pub max_seen_routes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RouterStats {
    pub seen_routes: usize,
    pub known_peers: usize,
    pub delivered_routes: u64,
    pub forwarded_routes: u64,
    pub duplicate_routes: u64,
}

#[derive(Debug, Clone)]
pub struct Router {
    config: RouterConfig,
    inner: Arc<Mutex<RouterInner>>,
}

#[derive(Debug)]
struct RouterInner {
    next_route_seq: u64,
    seen_routes: BTreeMap<String, u64>,
    seen_content: BTreeMap<String, u64>,
    peers: BTreeMap<String, PeerPresence>,
    delivered_routes: u64,
    forwarded_routes: u64,
    duplicate_routes: u64,
}

#[derive(Debug, Clone)]
pub struct RouteDecision {
    pub deliver: bool,
    pub forward: Option<RouteEnvelope>,
    pub duplicate: bool,
}

impl RouterConfig {
    pub fn new(peer_id: impl Into<String>) -> Self {
        Self {
            peer_id: peer_id.into(),
            default_channel: "primadb".to_owned(),
            default_ttl: 6,
            max_seen_routes: 4_096,
        }
    }
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self::new(format!("peer-{}", now_millis()))
    }
}

impl Router {
    pub fn new(config: RouterConfig) -> Self {
        Self {
            config,
            inner: Arc::new(Mutex::new(RouterInner {
                next_route_seq: 0,
                seen_routes: BTreeMap::new(),
                seen_content: BTreeMap::new(),
                peers: BTreeMap::new(),
                delivered_routes: 0,
                forwarded_routes: 0,
                duplicate_routes: 0,
            })),
        }
    }

    pub fn peer_id(&self) -> &str {
        &self.config.peer_id
    }

    pub fn known_peers(&self) -> Vec<PeerPresence> {
        self.inner.lock().unwrap().peers.values().cloned().collect()
    }

    pub fn forget_peer(&self, peer_id: &str) {
        self.inner.lock().unwrap().peers.remove(peer_id);
    }

    pub fn stats(&self) -> RouterStats {
        let inner = self.inner.lock().unwrap();
        RouterStats {
            seen_routes: inner.seen_routes.len(),
            known_peers: inner.peers.len(),
            delivered_routes: inner.delivered_routes,
            forwarded_routes: inner.forwarded_routes,
            duplicate_routes: inner.duplicate_routes,
        }
    }

    pub fn wrap_sync(
        &self,
        encoding: impl Into<String>,
        payload: JsonValue,
        target: RouteTarget,
    ) -> RouteEnvelope {
        self.wrap_payload(
            RoutePayload::Sync {
                encoding: encoding.into(),
                payload,
            },
            target,
            None,
        )
    }

    pub fn wrap_signal(
        &self,
        room: impl Into<String>,
        payload: JsonValue,
        target: RouteTarget,
    ) -> RouteEnvelope {
        self.wrap_payload(
            RoutePayload::Signal {
                room: room.into(),
                payload,
            },
            target,
            None,
        )
    }

    pub fn wrap_pull_request(&self, request: PullRequest, target: RouteTarget) -> RouteEnvelope {
        self.wrap_payload(RoutePayload::PullRequest { request }, target, None)
    }

    pub fn wrap_pull_response(
        &self,
        response: PullResponse,
        target: RouteTarget,
        reply_to: impl Into<Option<String>>,
    ) -> RouteEnvelope {
        self.wrap_payload(
            RoutePayload::PullResponse { response },
            target,
            reply_to.into(),
        )
    }

    pub fn wrap_watch_request(
        &self,
        request: WatchRequest,
        target: RouteTarget,
        reply_to: impl Into<Option<String>>,
    ) -> RouteEnvelope {
        let mut route = self.wrap_payload(
            RoutePayload::WatchRequest { request },
            target,
            reply_to.into(),
        );
        route.content_hash = None;
        route
    }

    pub fn wrap_watch_event(
        &self,
        event: WatchEvent,
        target: RouteTarget,
        reply_to: impl Into<Option<String>>,
    ) -> RouteEnvelope {
        let mut route =
            self.wrap_payload(RoutePayload::WatchEvent { event }, target, reply_to.into());
        route.content_hash = None;
        route
    }

    pub fn wrap_batch(
        &self,
        items: Vec<RouteBatchItem>,
        target: RouteTarget,
        reply_to: impl Into<Option<String>>,
    ) -> RouteEnvelope {
        self.wrap_payload(RoutePayload::Batch { items }, target, reply_to.into())
    }

    pub fn wrap_batch_item(
        &self,
        item: RouteBatchItem,
        target: RouteTarget,
        reply_to: impl Into<Option<String>>,
    ) -> RouteEnvelope {
        self.wrap_payload(RoutePayload::from_batch_item(item), target, reply_to.into())
    }

    pub fn presence(
        &self,
        replica_id: impl Into<String>,
        transport: impl Into<String>,
        capabilities: Vec<String>,
        topics: Vec<String>,
    ) -> RouteEnvelope {
        let peer = PeerPresence {
            peer_id: self.config.peer_id.clone(),
            replica_id: replica_id.into(),
            transport: transport.into(),
            capabilities,
            topics,
            metadata: BTreeMap::new(),
        };
        self.wrap_payload(
            RoutePayload::Presence { peer },
            RouteTarget::Broadcast,
            None,
        )
    }

    pub fn peer_exchange(
        &self,
        peers: Vec<PeerRecommendation>,
        target: RouteTarget,
        reply_to: impl Into<Option<String>>,
    ) -> RouteEnvelope {
        self.wrap_payload(
            RoutePayload::PeerExchange { peers },
            target,
            reply_to.into(),
        )
    }

    pub fn snapshot_request(&self, root: Option<String>, target: RouteTarget) -> RouteEnvelope {
        self.wrap_payload(RoutePayload::SnapshotRequest { root }, target, None)
    }

    pub fn snapshot_response(
        &self,
        root: Option<String>,
        snapshot: DatabaseSnapshot,
        target: RouteTarget,
    ) -> RouteEnvelope {
        self.wrap_payload(
            RoutePayload::SnapshotResponse { root, snapshot },
            target,
            None,
        )
    }

    pub fn accept(&self, envelope: RouteEnvelope) -> RouteDecision {
        let duplicate = {
            let mut inner = self.inner.lock().unwrap();
            let seen_self = envelope
                .seen_by
                .iter()
                .any(|peer_id| peer_id == &self.config.peer_id);
            let seen_route = inner.seen_routes.contains_key(&envelope.route_id);
            let duplicate_content = dedupe_key(&envelope)
                .as_ref()
                .is_some_and(|key| inner.seen_content.contains_key(key));
            if seen_self || seen_route || duplicate_content {
                inner.duplicate_routes = inner.duplicate_routes.saturating_add(1);
                true
            } else {
                let seen_at = now_millis();
                inner.seen_routes.insert(envelope.route_id.clone(), seen_at);
                trim_seen_cache(&mut inner.seen_routes, self.config.max_seen_routes);
                if let Some(key) = dedupe_key(&envelope) {
                    inner.seen_content.insert(key, seen_at);
                    trim_seen_cache(&mut inner.seen_content, self.config.max_seen_routes);
                }
                if let RoutePayload::Presence { peer } = &envelope.payload {
                    update_peer_presence(&mut inner.peers, peer);
                }
                false
            }
        };

        if duplicate {
            return RouteDecision {
                deliver: false,
                forward: None,
                duplicate: true,
            };
        }

        let deliver = matches_target(
            &self.config.peer_id,
            &self.config.default_channel,
            &envelope,
        );
        let forward = if envelope.ttl > 1 {
            let mut forwarded = envelope.clone();
            forwarded.ttl = forwarded.ttl.saturating_sub(1);
            forwarded.hops = forwarded.hops.saturating_add(1);
            if !forwarded
                .seen_by
                .iter()
                .any(|peer_id| peer_id == &self.config.peer_id)
            {
                forwarded.seen_by.push(self.config.peer_id.clone());
            }
            Some(forwarded)
        } else {
            None
        };

        let mut inner = self.inner.lock().unwrap();
        if deliver {
            inner.delivered_routes = inner.delivered_routes.saturating_add(1);
        }
        if forward.is_some() {
            inner.forwarded_routes = inner.forwarded_routes.saturating_add(1);
        }
        RouteDecision {
            deliver,
            forward,
            duplicate: false,
        }
    }

    fn wrap_payload(
        &self,
        payload: RoutePayload,
        target: RouteTarget,
        reply_to: Option<String>,
    ) -> RouteEnvelope {
        let route_id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_route_seq = inner.next_route_seq.saturating_add(1);
            format!("{}/route/{:x}", self.config.peer_id, inner.next_route_seq)
        };

        let content_hash = stable_content_hash(&payload);
        RouteEnvelope {
            route_id,
            from: self.config.peer_id.clone(),
            channel: self.config.default_channel.clone(),
            target,
            ttl: self.config.default_ttl,
            hops: 0,
            issued_at_millis: now_millis(),
            reply_to,
            content_hash,
            seen_by: vec![self.config.peer_id.clone()],
            payload,
        }
    }
}

impl RoutePayload {
    pub fn from_batch_item(item: RouteBatchItem) -> Self {
        match item {
            RouteBatchItem::Sync { encoding, payload } => Self::Sync { encoding, payload },
            RouteBatchItem::Presence { peer } => Self::Presence { peer },
            RouteBatchItem::Signal { room, payload } => Self::Signal { room, payload },
            RouteBatchItem::SnapshotRequest { root } => Self::SnapshotRequest { root },
            RouteBatchItem::SnapshotResponse { root, snapshot } => {
                Self::SnapshotResponse { root, snapshot }
            }
            RouteBatchItem::PullRequest { request } => Self::PullRequest { request },
            RouteBatchItem::PullResponse { response } => Self::PullResponse { response },
            RouteBatchItem::WatchRequest { request } => Self::WatchRequest { request },
            RouteBatchItem::WatchEvent { event } => Self::WatchEvent { event },
            RouteBatchItem::PeerExchange { peers } => Self::PeerExchange { peers },
        }
    }
}

fn update_peer_presence(peers: &mut BTreeMap<String, PeerPresence>, peer: &PeerPresence) {
    if peer.metadata.get("state").map(String::as_str) == Some("offline") {
        peers.remove(&peer.peer_id);
    } else {
        peers.insert(peer.peer_id.clone(), peer.clone());
    }
}

fn dedupe_key(envelope: &RouteEnvelope) -> Option<String> {
    envelope.content_hash.as_ref().map(|content_hash| {
        format!(
            "{}:{}:{content_hash}",
            envelope.from,
            envelope.reply_to.as_deref().unwrap_or_default()
        )
    })
}

fn matches_target(peer_id: &str, channel: &str, envelope: &RouteEnvelope) -> bool {
    match &envelope.target {
        RouteTarget::Broadcast => envelope.channel == channel,
        RouteTarget::Peer(target) => target == peer_id,
        RouteTarget::Topic(topic) => envelope.channel == *topic || topic == channel,
    }
}

fn trim_seen_cache(seen: &mut BTreeMap<String, u64>, max_seen_routes: usize) {
    while seen.len() > max_seen_routes {
        let Some(oldest) = seen.keys().next().cloned() else {
            break;
        };
        seen.remove(&oldest);
    }
}

#[cfg(test)]
mod tests {
    use super::{PeerPresence, RouteEnvelope, RoutePayload, RouteTarget, Router, RouterConfig};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn duplicate_routes_are_rejected() {
        let router = Router::new(RouterConfig::new("peer-a"));
        let mut route = router.wrap_sync("json", json!({"ok": true}), RouteTarget::Broadcast);
        route.from = "peer-b".to_owned();
        route.seen_by = vec!["peer-b".to_owned()];
        let first = router.accept(route.clone());
        let second = router.accept(route);
        assert!(first.deliver);
        assert!(second.duplicate);
    }

    #[test]
    fn duplicate_content_hashes_are_rejected_even_with_new_route_ids() {
        let router = Router::new(RouterConfig::new("peer-a"));
        let mut route = router.wrap_sync("json", json!({"ok": true}), RouteTarget::Broadcast);
        route.from = "peer-b".to_owned();
        route.seen_by = vec!["peer-b".to_owned()];

        let mut retry = route.clone();
        retry.route_id = "peer-b/route/retry".to_owned();

        let first = router.accept(route);
        let second = router.accept(retry);
        assert!(first.deliver);
        assert!(second.duplicate);
    }

    #[test]
    fn seen_by_hints_prevent_route_loops() {
        let router = Router::new(RouterConfig::new("peer-a"));
        let mut route = router.wrap_sync("json", json!({"ok": true}), RouteTarget::Broadcast);
        route.from = "peer-b".to_owned();
        route.seen_by = vec!["peer-b".to_owned(), "peer-a".to_owned()];

        let decision = router.accept(route);
        assert!(decision.duplicate);
        assert!(!decision.deliver);
    }

    #[test]
    fn offline_presence_removes_known_peer() {
        let router = Router::new(RouterConfig::new("peer-a"));
        let mut online = router.presence("replica-b", "websocket", vec![], vec![]);
        if let RoutePayload::Presence { peer } = &mut online.payload {
            peer.peer_id = "peer-b".to_owned();
        }
        online.from = "peer-b".to_owned();
        online.seen_by = vec!["peer-b".to_owned()];
        router.accept(online);
        assert_eq!(router.known_peers().len(), 1);

        let mut offline_peer = PeerPresence {
            peer_id: "peer-b".to_owned(),
            replica_id: "replica-b".to_owned(),
            transport: "websocket".to_owned(),
            capabilities: Vec::new(),
            topics: Vec::new(),
            metadata: BTreeMap::new(),
        };
        offline_peer
            .metadata
            .insert("state".to_owned(), "offline".to_owned());
        let offline = RouteEnvelope {
            route_id: "peer-b/offline/1".to_owned(),
            from: "relay".to_owned(),
            channel: "primadb".to_owned(),
            target: RouteTarget::Broadcast,
            ttl: 1,
            hops: 0,
            issued_at_millis: 0,
            reply_to: None,
            content_hash: None,
            seen_by: vec!["relay".to_owned()],
            payload: RoutePayload::Presence { peer: offline_peer },
        };

        router.accept(offline);
        assert!(router.known_peers().is_empty());
    }
}
