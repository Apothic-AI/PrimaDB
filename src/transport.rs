use crate::clock::now_millis;
use crate::{
    PeerPresence, PeerRecommendation, Result, RouteBatchItem, RouteEnvelope, RoutePayload,
    RouteTarget, stable_content_hash,
};
use async_channel::{Receiver, Sender, unbounded};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_ROUTE_RELAY_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouteTransportKind {
    WebSocket,
    Moq,
    WebRtc,
    BroadcastChannel,
    InMemory,
}

impl RouteTransportKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WebSocket => "websocket",
            Self::Moq => "moq",
            Self::WebRtc => "webrtc",
            Self::BroadcastChannel => "broadcast_channel",
            Self::InMemory => "in_memory",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RouteSessionInfo {
    pub session_id: String,
    pub peer_id: Option<String>,
    pub transport: RouteTransportKind,
    #[serde(default)]
    pub relay_routed: bool,
    #[serde(default)]
    pub direct: bool,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct PresenceRecord {
    peer: PeerPresence,
    route: RouteEnvelope,
}

#[derive(Debug, Clone)]
struct RouteRelaySession<T> {
    handle: T,
    presence: Option<PresenceRecord>,
}

#[derive(Debug, Clone)]
pub struct RouteRelayForward<T> {
    pub bootstrap: Option<RouteEnvelope>,
    pub recipients: Vec<T>,
    pub route: Option<RouteEnvelope>,
}

impl<T> Default for RouteRelayForward<T> {
    fn default() -> Self {
        Self {
            bootstrap: None,
            recipients: Vec::new(),
            route: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RouteRelayCore<T> {
    relay_peer_id: String,
    max_seen_routes: usize,
    sessions: BTreeMap<u64, RouteRelaySession<T>>,
    peer_index: BTreeMap<String, u64>,
    seen_routes: BTreeMap<String, u64>,
    seen_content: BTreeMap<String, u64>,
}

impl<T: Clone> RouteRelayCore<T> {
    pub fn new(relay_peer_id: impl Into<String>, max_seen_routes: usize) -> Self {
        Self {
            relay_peer_id: relay_peer_id.into(),
            max_seen_routes: max_seen_routes.max(1),
            sessions: BTreeMap::new(),
            peer_index: BTreeMap::new(),
            seen_routes: BTreeMap::new(),
            seen_content: BTreeMap::new(),
        }
    }

    pub fn insert_session(&mut self, session_id: u64, handle: T) {
        self.sessions.insert(
            session_id,
            RouteRelaySession {
                handle,
                presence: None,
            },
        );
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn peer_count(&self) -> usize {
        self.peer_index.len()
    }

    pub fn session_handle(&self, session_id: u64) -> Option<T> {
        self.sessions
            .get(&session_id)
            .map(|session| session.handle.clone())
    }

    pub fn session_handles_except(&self, session_id: u64) -> Vec<T> {
        self.sessions
            .iter()
            .filter_map(|(candidate_id, session)| {
                if *candidate_id == session_id {
                    None
                } else {
                    Some(session.handle.clone())
                }
            })
            .collect()
    }

    pub fn forward_route(
        &mut self,
        session_id: u64,
        mut route: RouteEnvelope,
    ) -> Result<RouteRelayForward<T>> {
        if route
            .seen_by
            .iter()
            .any(|peer_id| peer_id == &self.relay_peer_id)
            || self.seen_routes.contains_key(&route.route_id)
            || dedupe_key(&route)
                .as_ref()
                .is_some_and(|key| self.seen_content.contains_key(key))
        {
            return Ok(RouteRelayForward::default());
        }

        let seen_at = now_millis();
        self.seen_routes.insert(route.route_id.clone(), seen_at);
        trim_seen_cache(&mut self.seen_routes, self.max_seen_routes);
        if let Some(key) = dedupe_key(&route) {
            self.seen_content.insert(key, seen_at);
            trim_seen_cache(&mut self.seen_content, self.max_seen_routes);
        }

        if !route
            .seen_by
            .iter()
            .any(|peer_id| peer_id == &self.relay_peer_id)
        {
            route.seen_by.push(self.relay_peer_id.clone());
        }
        if route.content_hash.is_none() {
            route.content_hash = stable_content_hash(&route.payload);
        }

        let mut bootstrap = None;
        if let RoutePayload::Presence { peer } = &route.payload {
            let existing = self
                .sessions
                .iter()
                .filter_map(|(candidate_id, session)| {
                    if *candidate_id == session_id {
                        None
                    } else {
                        session
                            .presence
                            .as_ref()
                            .map(|presence| presence.route.clone())
                    }
                })
                .collect::<Vec<_>>();
            let recommendations = self.collect_peer_recommendations(Some(session_id));
            bootstrap = build_bootstrap_route(
                &self.relay_peer_id,
                route.channel.clone(),
                peer.peer_id.clone(),
                existing,
                recommendations,
            );

            let previous_peer_id = self
                .sessions
                .get(&session_id)
                .and_then(|session| session.presence.as_ref())
                .map(|presence| presence.peer.peer_id.clone());
            if let Some(previous_peer_id) = previous_peer_id {
                self.peer_index.remove(&previous_peer_id);
            }
            self.peer_index.insert(peer.peer_id.clone(), session_id);
            if let Some(session) = self.sessions.get_mut(&session_id) {
                session.presence = Some(PresenceRecord {
                    peer: peer.clone(),
                    route: route.clone(),
                });
            }
        }

        let recipients = self.collect_route_recipients(session_id, &route);
        Ok(RouteRelayForward {
            bootstrap,
            recipients,
            route: Some(route),
        })
    }

    pub fn disconnect_session(&mut self, session_id: u64) -> Option<RouteRelayForward<T>> {
        let session = self.sessions.remove(&session_id)?;
        let presence = session.presence?;
        self.peer_index.remove(&presence.peer.peer_id);

        let mut offline_peer = presence.peer.clone();
        offline_peer
            .metadata
            .insert("state".to_owned(), "offline".to_owned());
        let payload = RoutePayload::Presence { peer: offline_peer };
        let route = RouteEnvelope {
            route_id: format!(
                "{}/disconnect/{:x}",
                self.relay_peer_id,
                NEXT_ROUTE_RELAY_ID.fetch_add(1, Ordering::Relaxed)
            ),
            from: self.relay_peer_id.clone(),
            channel: presence.route.channel.clone(),
            target: RouteTarget::Broadcast,
            ttl: 1,
            hops: 0,
            issued_at_millis: now_millis(),
            reply_to: None,
            content_hash: stable_content_hash(&payload),
            seen_by: vec![self.relay_peer_id.clone()],
            payload,
        };
        let recipients = self.collect_route_recipients(session_id, &route);
        Some(RouteRelayForward {
            bootstrap: None,
            recipients,
            route: Some(route),
        })
    }

    fn collect_route_recipients(&self, sender_id: u64, route: &RouteEnvelope) -> Vec<T> {
        match &route.target {
            RouteTarget::Peer(peer_id) => self
                .peer_index
                .get(peer_id)
                .and_then(|session_id| {
                    if *session_id == sender_id {
                        None
                    } else {
                        self.sessions
                            .get(session_id)
                            .map(|session| session.handle.clone())
                    }
                })
                .into_iter()
                .collect(),
            RouteTarget::Broadcast => self
                .sessions
                .iter()
                .filter_map(|(candidate_id, session)| {
                    if *candidate_id == sender_id {
                        return None;
                    }
                    match &session.presence {
                        Some(presence) if presence.route.channel == route.channel => {
                            Some(session.handle.clone())
                        }
                        Some(_) => None,
                        None => Some(session.handle.clone()),
                    }
                })
                .collect(),
            RouteTarget::Topic(topic) => self
                .sessions
                .iter()
                .filter_map(|(candidate_id, session)| {
                    if *candidate_id == sender_id {
                        return None;
                    }
                    let Some(presence) = &session.presence else {
                        return None;
                    };
                    if presence
                        .peer
                        .topics
                        .iter()
                        .any(|candidate| candidate == topic)
                        || presence.route.channel == *topic
                    {
                        Some(session.handle.clone())
                    } else {
                        None
                    }
                })
                .collect(),
        }
    }

    fn collect_peer_recommendations(
        &self,
        exclude_session_id: Option<u64>,
    ) -> Vec<PeerRecommendation> {
        self.sessions
            .iter()
            .filter_map(|(session_id, session)| {
                if exclude_session_id == Some(*session_id) {
                    return None;
                }
                let presence = session.presence.as_ref()?;
                let relay_urls = presence
                    .peer
                    .metadata
                    .get("relay_url")
                    .into_iter()
                    .flat_map(|value| value.split(','))
                    .map(str::trim)
                    .filter(|candidate| !candidate.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                Some(PeerRecommendation {
                    peer: presence.peer.clone(),
                    relay_urls,
                    score: recommendation_score(&presence.peer),
                    discovered_at_millis: now_millis(),
                })
            })
            .collect()
    }
}

pub struct InMemoryRouteSession {
    id: u64,
    hub: InMemoryRouteHub,
    receiver: Receiver<RouteEnvelope>,
}

impl InMemoryRouteSession {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn send(&self, route: RouteEnvelope) -> Result<()> {
        self.hub.forward(self.id, route)
    }

    pub async fn recv(&self) -> Option<RouteEnvelope> {
        self.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<RouteEnvelope> {
        self.receiver.try_recv().ok()
    }

    pub fn close(&self) {
        self.hub.disconnect(self.id);
    }
}

#[derive(Clone)]
pub struct InMemoryRouteHub {
    inner: Arc<Mutex<InMemoryRouteHubInner>>,
}

struct InMemoryRouteHubInner {
    next_session_id: u64,
    relay: RouteRelayCore<Sender<RouteEnvelope>>,
}

impl Default for InMemoryRouteHub {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryRouteHub {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(InMemoryRouteHubInner {
                next_session_id: 0,
                relay: RouteRelayCore::new("in-memory-relay", 8_192),
            })),
        }
    }

    pub fn connect(&self) -> InMemoryRouteSession {
        let (sender, receiver) = unbounded();
        let id = {
            let mut inner = self.inner.lock().unwrap();
            inner.next_session_id = inner.next_session_id.saturating_add(1);
            let id = inner.next_session_id;
            inner.relay.insert_session(id, sender);
            id
        };
        InMemoryRouteSession {
            id,
            hub: self.clone(),
            receiver,
        }
    }

    pub fn session_count(&self) -> usize {
        self.inner.lock().unwrap().relay.session_count()
    }

    pub fn peer_count(&self) -> usize {
        self.inner.lock().unwrap().relay.peer_count()
    }

    fn forward(&self, session_id: u64, route: RouteEnvelope) -> Result<()> {
        let forward = {
            self.inner
                .lock()
                .unwrap()
                .relay
                .forward_route(session_id, route)?
        };
        self.dispatch(session_id, forward)
    }

    fn disconnect(&self, session_id: u64) {
        let forward = {
            self.inner
                .lock()
                .unwrap()
                .relay
                .disconnect_session(session_id)
        };
        if let Some(forward) = forward {
            let _ = self.dispatch(session_id, forward);
        }
    }

    fn dispatch(
        &self,
        session_id: u64,
        forward: RouteRelayForward<Sender<RouteEnvelope>>,
    ) -> Result<()> {
        if let Some(bootstrap) = forward.bootstrap {
            let sender = {
                let inner = self.inner.lock().unwrap();
                inner.relay.session_handle(session_id)
            };
            if let Some(sender) = sender {
                let _ = sender.try_send(bootstrap);
            }
        }
        if let Some(route) = forward.route {
            for recipient in forward.recipients {
                let _ = recipient.try_send(route.clone());
            }
        }
        Ok(())
    }
}

fn build_bootstrap_route(
    relay_peer_id: &str,
    channel: String,
    target_peer_id: String,
    existing: Vec<RouteEnvelope>,
    recommendations: Vec<PeerRecommendation>,
) -> Option<RouteEnvelope> {
    let mut items = existing
        .into_iter()
        .filter_map(|route| match route.payload {
            RoutePayload::Presence { peer } => Some(RouteBatchItem::Presence { peer }),
            _ => None,
        })
        .collect::<Vec<_>>();

    if !recommendations.is_empty() {
        items.push(RouteBatchItem::PeerExchange {
            peers: recommendations,
        });
    }

    if items.is_empty() {
        return None;
    }

    let payload = RoutePayload::Batch { items };
    Some(RouteEnvelope {
        route_id: format!(
            "{relay_peer_id}/bootstrap/{:x}",
            NEXT_ROUTE_RELAY_ID.fetch_add(1, Ordering::Relaxed)
        ),
        from: relay_peer_id.to_owned(),
        channel,
        target: RouteTarget::Peer(target_peer_id),
        ttl: 1,
        hops: 0,
        issued_at_millis: now_millis(),
        reply_to: None,
        content_hash: stable_content_hash(&payload),
        seen_by: vec![relay_peer_id.to_owned()],
        payload,
    })
}

fn recommendation_score(peer: &PeerPresence) -> u16 {
    let capability_bonus = peer.capabilities.len().min(8) as u16 * 10;
    let topic_bonus = peer.topics.len().min(8) as u16 * 5;
    50 + capability_bonus + topic_bonus
}

fn dedupe_key(route: &RouteEnvelope) -> Option<String> {
    route.content_hash.as_ref().map(|content_hash| {
        format!(
            "{}:{}:{content_hash}",
            route.from,
            route.reply_to.as_deref().unwrap_or_default()
        )
    })
}

fn trim_seen_cache(cache: &mut BTreeMap<String, u64>, max: usize) {
    while cache.len() > max {
        let Some(oldest) = cache.keys().next().cloned() else {
            break;
        };
        cache.remove(&oldest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Router, RouterConfig};

    fn router(peer_id: &str, channel: &str) -> Router {
        Router::new(RouterConfig {
            peer_id: peer_id.to_owned(),
            default_channel: channel.to_owned(),
            default_ttl: 6,
            max_seen_routes: 128,
        })
    }

    fn presence(router: &Router, topic: &str) -> RouteEnvelope {
        router.presence(
            router.peer_id().to_owned(),
            "in_memory",
            vec!["test".to_owned()],
            vec![topic.to_owned()],
        )
    }

    fn drain(session: &InMemoryRouteSession) {
        while session.try_recv().is_some() {}
    }

    #[test]
    fn in_memory_broadcast_and_peer_routes() {
        let hub = InMemoryRouteHub::new();
        let a = hub.connect();
        let b = hub.connect();
        let c = hub.connect();
        let router_a = router("a", "room");
        let router_b = router("b", "room");
        let router_c = router("c", "other");

        a.send(presence(&router_a, "room")).unwrap();
        b.send(presence(&router_b, "room")).unwrap();
        c.send(presence(&router_c, "other")).unwrap();
        assert_eq!(hub.peer_count(), 3);
        drain(&a);
        drain(&b);
        drain(&c);

        let broadcast = router_a.peer_exchange(Vec::new(), RouteTarget::Broadcast, None);
        a.send(broadcast.clone()).unwrap();
        assert_eq!(
            b.try_recv().map(|route| route.route_id),
            Some(broadcast.route_id.clone())
        );
        assert!(c.try_recv().is_none());

        let peer = router_a.wrap_signal(
            "peer-test",
            serde_json::json!({ "id": "peer-route" }),
            RouteTarget::Peer("b".to_owned()),
        );
        a.send(peer.clone()).unwrap();
        assert_eq!(
            b.try_recv().map(|route| route.route_id),
            Some(peer.route_id)
        );
        assert!(c.try_recv().is_none());
    }

    #[test]
    fn in_memory_topic_routes_and_duplicate_suppression() {
        let hub = InMemoryRouteHub::new();
        let a = hub.connect();
        let b = hub.connect();
        let c = hub.connect();
        let router_a = router("a", "test");
        let router_b = router("b", "test");
        let router_c = router("c", "test");

        a.send(presence(&router_a, "alpha")).unwrap();
        b.send(presence(&router_b, "alpha")).unwrap();
        c.send(presence(&router_c, "beta")).unwrap();
        drain(&a);
        drain(&b);
        drain(&c);

        let topic =
            router_a.peer_exchange(Vec::new(), RouteTarget::Topic("alpha".to_owned()), None);
        a.send(topic.clone()).unwrap();
        assert_eq!(
            b.try_recv().map(|route| route.route_id),
            Some(topic.route_id.clone())
        );
        assert!(c.try_recv().is_none());

        a.send(topic).unwrap();
        assert!(b.try_recv().is_none());
        assert!(c.try_recv().is_none());
    }

    #[test]
    fn in_memory_presence_bootstrap_and_disconnect() {
        let hub = InMemoryRouteHub::new();
        let a = hub.connect();
        let b = hub.connect();
        let router_a = router("a", "room");
        let router_b = router("b", "room");

        a.send(presence(&router_a, "room")).unwrap();
        drain(&b);
        b.send(presence(&router_b, "room")).unwrap();

        let bootstrap = b.try_recv().expect("new peer should receive bootstrap");
        assert!(matches!(bootstrap.payload, RoutePayload::Batch { .. }));

        a.close();
        let offline = b
            .try_recv()
            .expect("remaining peer should receive offline presence");
        match offline.payload {
            RoutePayload::Presence { peer } => {
                assert_eq!(peer.peer_id, "a");
                assert_eq!(
                    peer.metadata.get("state").map(String::as_str),
                    Some("offline")
                );
            }
            other => panic!("unexpected offline payload: {other:?}"),
        }
    }
}
