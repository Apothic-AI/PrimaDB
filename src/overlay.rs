use crate::app_route::{ApplicationRouteBus, ApplicationRouteContext};
use crate::clock::now_millis;
use crate::{
    ApplicationRouteEvent, ApplicationRouteFilter, ApplicationRouteMessage,
    ApplicationRouteSubscription, InMemoryRouteSession, Result, RouteEnvelope, RoutePayload,
    RouteTarget, RouteTransportKind, Router, RouterConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

pub const APPLICATION_STREAM_NAMESPACE: &str = "primadb.applicationStream";
pub const APPLICATION_STREAM_PROTOCOL_V1: &str = "primadb.applicationStream.v1";

type BoxRouteSendFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type AsyncRouteSender = Arc<dyn Fn(RouteEnvelope) -> BoxRouteSendFuture + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouteOverlaySendMode {
    FirstSuccess,
    FanOut,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RouteOverlayPolicy {
    #[serde(default)]
    pub preferred_transports: Vec<RouteTransportKind>,
    #[serde(default)]
    pub send_mode: RouteOverlaySendMode,
    #[serde(default = "default_true")]
    pub direct_first: bool,
    #[serde(default = "default_true")]
    pub allow_direct: bool,
    #[serde(default = "default_true")]
    pub allow_relay: bool,
    #[serde(default)]
    pub require_direct: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RouteOverlayUnderlayInfo {
    pub id: String,
    pub transport: RouteTransportKind,
    #[serde(default)]
    pub direct: bool,
    #[serde(default)]
    pub relay_routed: bool,
    #[serde(default)]
    pub connected: bool,
    #[serde(default)]
    pub priority: u16,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RouteOverlayDeliveryAttempt {
    pub underlay: RouteOverlayUnderlayInfo,
    pub attempted_at_millis: u64,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RouteOverlaySendReport {
    pub route: RouteEnvelope,
    #[serde(default)]
    pub attempts: Vec<RouteOverlayDeliveryAttempt>,
    #[serde(default)]
    pub delivered_underlay_ids: Vec<String>,
    #[serde(default)]
    pub failed_underlay_ids: Vec<String>,
    #[serde(default)]
    pub delivered_peer_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fallback_reason: Option<String>,
    #[serde(default)]
    pub duplicate_suppressed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct RouteOverlayPumpReport {
    pub received_routes: usize,
    pub delivered_application_routes: usize,
    pub delivered_stream_events: usize,
    pub duplicate_suppressed: usize,
    #[serde(default)]
    pub underlay_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationStreamFrameKind {
    Open,
    Data,
    Ack,
    Nack,
    Close,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationStreamFrame {
    pub stream_id: String,
    pub sequence: u64,
    pub kind: ApplicationStreamFrameKind,
    pub namespace: String,
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk: Option<String>,
    #[serde(default)]
    pub final_chunk: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack_sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationStreamEvent {
    pub stream_id: String,
    pub from: String,
    pub transport: RouteTransportKind,
    pub namespace: String,
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub body: JsonValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationStreamSendOptions {
    pub namespace: String,
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub body: JsonValue,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, JsonValue>,
    pub target: RouteTarget,
    #[serde(default = "default_stream_chunk_chars")]
    pub max_chunk_chars: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationStreamSendReport {
    pub stream_id: String,
    #[serde(default)]
    pub frame_reports: Vec<RouteOverlaySendReport>,
}

#[derive(Clone)]
pub struct RouteOverlayUnderlayHandle {
    info: RouteOverlayUnderlayInfo,
    send_route: Arc<dyn Fn(RouteEnvelope) -> Result<()> + Send + Sync>,
    send_route_async: Option<AsyncRouteSender>,
    drain_routes: Arc<dyn Fn() -> Vec<RouteEnvelope> + Send + Sync>,
    drain_application_events: Arc<dyn Fn() -> Vec<ApplicationRouteEvent> + Send + Sync>,
    connected: Arc<dyn Fn() -> bool + Send + Sync>,
}

pub struct RouteOverlaySession {
    router: Router,
    applications: ApplicationRouteBus,
    policy: Arc<Mutex<RouteOverlayPolicy>>,
    underlays: Arc<Mutex<BTreeMap<String, RouteOverlayUnderlayHandle>>>,
    seen_event_ids: Arc<Mutex<BTreeSet<String>>>,
    streams: Arc<Mutex<ApplicationStreamAssembler>>,
    stream_events: Arc<Mutex<VecDeque<ApplicationStreamEvent>>>,
}

#[derive(Debug, Default)]
pub struct ApplicationStreamAssembler {
    buffers: BTreeMap<String, ApplicationStreamBuffer>,
}

#[derive(Debug, Clone)]
struct ApplicationStreamBuffer {
    from: String,
    transport: RouteTransportKind,
    namespace: String,
    protocol: String,
    topic: Option<String>,
    metadata: BTreeMap<String, JsonValue>,
    chunks: BTreeMap<u64, String>,
    final_sequence: Option<u64>,
}

fn default_true() -> bool {
    true
}

fn default_stream_chunk_chars() -> usize {
    16 * 1024
}

impl Default for RouteOverlaySendMode {
    fn default() -> Self {
        Self::FirstSuccess
    }
}

impl Default for RouteOverlayPolicy {
    fn default() -> Self {
        Self {
            preferred_transports: vec![
                RouteTransportKind::WebRtc,
                RouteTransportKind::Moq,
                RouteTransportKind::WebSocket,
                RouteTransportKind::BroadcastChannel,
                RouteTransportKind::InMemory,
            ],
            send_mode: RouteOverlaySendMode::FirstSuccess,
            direct_first: true,
            allow_direct: true,
            allow_relay: true,
            require_direct: false,
        }
    }
}

impl RouteOverlayUnderlayHandle {
    pub fn new(
        info: RouteOverlayUnderlayInfo,
        send_route: impl Fn(RouteEnvelope) -> Result<()> + Send + Sync + 'static,
        drain_routes: impl Fn() -> Vec<RouteEnvelope> + Send + Sync + 'static,
        connected: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            info,
            send_route: Arc::new(send_route),
            send_route_async: None,
            drain_routes: Arc::new(drain_routes),
            drain_application_events: Arc::new(Vec::new),
            connected: Arc::new(connected),
        }
    }

    pub fn async_sender<Fut>(
        id: impl Into<String>,
        transport: RouteTransportKind,
        direct: bool,
        relay_routed: bool,
        send_route: impl Fn(RouteEnvelope) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let send_route = Arc::new(send_route);
        Self {
            info: RouteOverlayUnderlayInfo {
                id: id.into(),
                transport,
                direct,
                relay_routed,
                connected: true,
                priority: 0,
                metadata: BTreeMap::new(),
            },
            send_route: Arc::new(|_| {
                Err(crate::PrimadbError::Message(
                    "route underlay requires async overlay send".to_owned(),
                ))
            }),
            send_route_async: Some(Arc::new(move |route| {
                let send_route = send_route.clone();
                Box::pin(async move { send_route(route).await })
            })),
            drain_routes: Arc::new(Vec::new),
            drain_application_events: Arc::new(Vec::new),
            connected: Arc::new(|| true),
        }
    }

    pub fn with_application_events(
        mut self,
        drain_application_events: impl Fn() -> Vec<ApplicationRouteEvent> + Send + Sync + 'static,
    ) -> Self {
        self.drain_application_events = Arc::new(drain_application_events);
        self
    }

    pub fn with_connected(mut self, connected: impl Fn() -> bool + Send + Sync + 'static) -> Self {
        self.connected = Arc::new(connected);
        self
    }

    pub fn sender(
        id: impl Into<String>,
        transport: RouteTransportKind,
        direct: bool,
        relay_routed: bool,
        send_route: impl Fn(RouteEnvelope) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self::new(
            RouteOverlayUnderlayInfo {
                id: id.into(),
                transport,
                direct,
                relay_routed,
                connected: true,
                priority: 0,
                metadata: BTreeMap::new(),
            },
            send_route,
            Vec::new,
            || true,
        )
    }

    pub fn in_memory(id: impl Into<String>, session: InMemoryRouteSession) -> Self {
        let session = Arc::new(session);
        let send_session = session.clone();
        let drain_session = session.clone();
        Self::new(
            RouteOverlayUnderlayInfo {
                id: id.into(),
                transport: RouteTransportKind::InMemory,
                direct: false,
                relay_routed: true,
                connected: true,
                priority: 0,
                metadata: BTreeMap::new(),
            },
            move |route| send_session.send(route),
            move || {
                let mut routes = Vec::new();
                while let Some(route) = drain_session.try_recv() {
                    routes.push(route);
                }
                routes
            },
            || true,
        )
    }

    pub fn info(&self) -> RouteOverlayUnderlayInfo {
        let mut info = self.info.clone();
        info.connected = (self.connected)();
        info
    }
}

impl RouteOverlaySession {
    pub fn new(config: RouterConfig) -> Self {
        Self {
            router: Router::new(config),
            applications: ApplicationRouteBus::default(),
            policy: Arc::new(Mutex::new(RouteOverlayPolicy::default())),
            underlays: Arc::new(Mutex::new(BTreeMap::new())),
            seen_event_ids: Arc::new(Mutex::new(BTreeSet::new())),
            streams: Arc::new(Mutex::new(ApplicationStreamAssembler::default())),
            stream_events: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    pub fn policy(&self) -> RouteOverlayPolicy {
        self.policy.lock().unwrap().clone()
    }

    pub fn set_policy(&self, policy: RouteOverlayPolicy) {
        *self.policy.lock().unwrap() = policy;
    }

    pub fn add_underlay(&self, underlay: RouteOverlayUnderlayHandle) {
        self.underlays
            .lock()
            .unwrap()
            .insert(underlay.info.id.clone(), underlay);
    }

    pub fn remove_underlay(&self, underlay_id: &str) -> Option<RouteOverlayUnderlayInfo> {
        self.underlays
            .lock()
            .unwrap()
            .remove(underlay_id)
            .map(|underlay| underlay.info())
    }

    pub fn underlays(&self) -> Vec<RouteOverlayUnderlayInfo> {
        self.underlays
            .lock()
            .unwrap()
            .values()
            .map(RouteOverlayUnderlayHandle::info)
            .collect()
    }

    pub fn subscribe_applications(
        &self,
        filter: ApplicationRouteFilter,
    ) -> ApplicationRouteSubscription {
        self.applications.subscribe(filter)
    }

    pub fn publish_application(
        &self,
        message: ApplicationRouteMessage,
        target: RouteTarget,
    ) -> Result<RouteOverlaySendReport> {
        let route = self.router.wrap_application(message, target, None);
        self.send_route(route)
    }

    pub fn send_application(
        &self,
        namespace: impl Into<String>,
        protocol: impl Into<String>,
        topic: Option<String>,
        body: JsonValue,
        metadata: BTreeMap<String, JsonValue>,
        target: RouteTarget,
    ) -> Result<RouteOverlaySendReport> {
        self.publish_application(
            ApplicationRouteMessage::new(namespace, protocol, topic, body, metadata),
            target,
        )
    }

    pub fn send_route(&self, route: RouteEnvelope) -> Result<RouteOverlaySendReport> {
        let policy = self.policy();
        let mut underlays = self.ordered_underlays(&policy);
        let mut attempts = Vec::new();
        let mut delivered_underlay_ids = Vec::new();
        let mut failed_underlay_ids = Vec::new();

        if underlays.is_empty() {
            return Ok(RouteOverlaySendReport {
                route,
                attempts,
                delivered_underlay_ids,
                failed_underlay_ids,
                delivered_peer_ids: Vec::new(),
                fallback_reason: Some(
                    "no connected underlay matched the route overlay policy".to_owned(),
                ),
                duplicate_suppressed: 0,
            });
        }

        for underlay in underlays.drain(..) {
            let info = underlay.info();
            let attempted_at_millis = now_millis();
            match (underlay.send_route)(route.clone()) {
                Ok(()) => {
                    delivered_underlay_ids.push(info.id.clone());
                    attempts.push(RouteOverlayDeliveryAttempt {
                        underlay: info,
                        attempted_at_millis,
                        success: true,
                        message: None,
                    });
                    if policy.send_mode == RouteOverlaySendMode::FirstSuccess {
                        break;
                    }
                }
                Err(error) => {
                    failed_underlay_ids.push(info.id.clone());
                    attempts.push(RouteOverlayDeliveryAttempt {
                        underlay: info,
                        attempted_at_millis,
                        success: false,
                        message: Some(error.to_string()),
                    });
                }
            }
        }

        let fallback_reason = if delivered_underlay_ids.is_empty() {
            Some("all attempted route underlays failed".to_owned())
        } else if !failed_underlay_ids.is_empty() {
            Some(
                "one or more preferred underlays failed before a later underlay delivered"
                    .to_owned(),
            )
        } else {
            None
        };
        let delivered_peer_ids = match &route.target {
            RouteTarget::Peer(peer_id) if !delivered_underlay_ids.is_empty() => {
                vec![peer_id.clone()]
            }
            _ => Vec::new(),
        };

        Ok(RouteOverlaySendReport {
            route,
            attempts,
            delivered_underlay_ids,
            failed_underlay_ids,
            delivered_peer_ids,
            fallback_reason,
            duplicate_suppressed: 0,
        })
    }

    pub async fn send_route_async(&self, route: RouteEnvelope) -> Result<RouteOverlaySendReport> {
        let policy = self.policy();
        let mut underlays = self.ordered_underlays(&policy);
        let mut attempts = Vec::new();
        let mut delivered_underlay_ids = Vec::new();
        let mut failed_underlay_ids = Vec::new();

        if underlays.is_empty() {
            return Ok(RouteOverlaySendReport {
                route,
                attempts,
                delivered_underlay_ids,
                failed_underlay_ids,
                delivered_peer_ids: Vec::new(),
                fallback_reason: Some(
                    "no connected underlay matched the route overlay policy".to_owned(),
                ),
                duplicate_suppressed: 0,
            });
        }

        for underlay in underlays.drain(..) {
            let info = underlay.info();
            let attempted_at_millis = now_millis();
            let result = if let Some(send_route_async) = &underlay.send_route_async {
                send_route_async(route.clone()).await
            } else {
                (underlay.send_route)(route.clone())
            };
            match result {
                Ok(()) => {
                    delivered_underlay_ids.push(info.id.clone());
                    attempts.push(RouteOverlayDeliveryAttempt {
                        underlay: info,
                        attempted_at_millis,
                        success: true,
                        message: None,
                    });
                    if policy.send_mode == RouteOverlaySendMode::FirstSuccess {
                        break;
                    }
                }
                Err(error) => {
                    failed_underlay_ids.push(info.id.clone());
                    attempts.push(RouteOverlayDeliveryAttempt {
                        underlay: info,
                        attempted_at_millis,
                        success: false,
                        message: Some(error.to_string()),
                    });
                }
            }
        }

        let fallback_reason = if delivered_underlay_ids.is_empty() {
            Some("all attempted route underlays failed".to_owned())
        } else if !failed_underlay_ids.is_empty() {
            Some(
                "one or more preferred underlays failed before a later underlay delivered"
                    .to_owned(),
            )
        } else {
            None
        };
        let delivered_peer_ids = match &route.target {
            RouteTarget::Peer(peer_id) if !delivered_underlay_ids.is_empty() => {
                vec![peer_id.clone()]
            }
            _ => Vec::new(),
        };

        Ok(RouteOverlaySendReport {
            route,
            attempts,
            delivered_underlay_ids,
            failed_underlay_ids,
            delivered_peer_ids,
            fallback_reason,
            duplicate_suppressed: 0,
        })
    }

    pub async fn publish_application_async(
        &self,
        message: ApplicationRouteMessage,
        target: RouteTarget,
    ) -> Result<RouteOverlaySendReport> {
        let route = self.router.wrap_application(message, target, None);
        self.send_route_async(route).await
    }

    pub async fn send_application_async(
        &self,
        namespace: impl Into<String>,
        protocol: impl Into<String>,
        topic: Option<String>,
        body: JsonValue,
        metadata: BTreeMap<String, JsonValue>,
        target: RouteTarget,
    ) -> Result<RouteOverlaySendReport> {
        self.publish_application_async(
            ApplicationRouteMessage::new(namespace, protocol, topic, body, metadata),
            target,
        )
        .await
    }

    pub fn pump(&self) -> RouteOverlayPumpReport {
        let underlays = self
            .underlays
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut report = RouteOverlayPumpReport::default();
        for underlay in underlays {
            let info = underlay.info();
            let routes = (underlay.drain_routes)();
            if !routes.is_empty() {
                report.underlay_ids.push(info.id.clone());
            }
            for route in routes {
                report.received_routes = report.received_routes.saturating_add(1);
                if self.accept_route_from_underlay(&info, route) {
                    report.delivered_application_routes =
                        report.delivered_application_routes.saturating_add(1);
                    report.delivered_stream_events = self.stream_events.lock().unwrap().len();
                } else {
                    report.duplicate_suppressed = report.duplicate_suppressed.saturating_add(1);
                }
            }
            for event in (underlay.drain_application_events)() {
                report.received_routes = report.received_routes.saturating_add(1);
                if self.accept_application_event_from_underlay(&info, event) {
                    report.delivered_application_routes =
                        report.delivered_application_routes.saturating_add(1);
                    report.delivered_stream_events = self.stream_events.lock().unwrap().len();
                } else {
                    report.duplicate_suppressed = report.duplicate_suppressed.saturating_add(1);
                }
            }
        }
        report
    }

    pub fn drain_stream_events(&self) -> Vec<ApplicationStreamEvent> {
        let mut events = self.stream_events.lock().unwrap();
        events.drain(..).collect()
    }

    pub fn send_application_stream(
        &self,
        options: ApplicationStreamSendOptions,
    ) -> Result<ApplicationStreamSendReport> {
        let stream_id = format!("{}/stream/{:x}", self.router.peer_id(), now_millis());
        let mut frame_reports = Vec::new();
        let max_chunk_chars = options.max_chunk_chars.max(1);
        let body = serde_json::to_string(&options.body)?;
        let chunks = chunk_string(&body, max_chunk_chars);

        let open = ApplicationStreamFrame {
            stream_id: stream_id.clone(),
            sequence: 0,
            kind: ApplicationStreamFrameKind::Open,
            namespace: options.namespace.clone(),
            protocol: options.protocol.clone(),
            topic: options.topic.clone(),
            chunk: None,
            final_chunk: chunks.is_empty(),
            ack_sequence: None,
            error: None,
            metadata: options.metadata.clone(),
        };
        frame_reports
            .push(self.publish_application(stream_frame_message(open)?, options.target.clone())?);

        for (index, chunk) in chunks.iter().enumerate() {
            let sequence = index as u64 + 1;
            let frame = ApplicationStreamFrame {
                stream_id: stream_id.clone(),
                sequence,
                kind: ApplicationStreamFrameKind::Data,
                namespace: options.namespace.clone(),
                protocol: options.protocol.clone(),
                topic: options.topic.clone(),
                chunk: Some(chunk.clone()),
                final_chunk: index + 1 == chunks.len(),
                ack_sequence: None,
                error: None,
                metadata: options.metadata.clone(),
            };
            frame_reports.push(
                self.publish_application(stream_frame_message(frame)?, options.target.clone())?,
            );
        }

        let close = ApplicationStreamFrame {
            stream_id: stream_id.clone(),
            sequence: chunks.len() as u64 + 1,
            kind: ApplicationStreamFrameKind::Close,
            namespace: options.namespace,
            protocol: options.protocol,
            topic: options.topic,
            chunk: None,
            final_chunk: true,
            ack_sequence: None,
            error: None,
            metadata: options.metadata,
        };
        frame_reports.push(self.publish_application(stream_frame_message(close)?, options.target)?);

        Ok(ApplicationStreamSendReport {
            stream_id,
            frame_reports,
        })
    }

    fn ordered_underlays(&self, policy: &RouteOverlayPolicy) -> Vec<RouteOverlayUnderlayHandle> {
        let mut underlays = self
            .underlays
            .lock()
            .unwrap()
            .values()
            .filter(|underlay| {
                let info = underlay.info();
                info.connected
                    && (!policy.require_direct || info.direct)
                    && (policy.allow_direct || !info.direct)
                    && (policy.allow_relay || !info.relay_routed)
            })
            .cloned()
            .collect::<Vec<_>>();
        underlays.sort_by_key(|underlay| {
            let info = underlay.info();
            let preferred = policy
                .preferred_transports
                .iter()
                .position(|transport| transport == &info.transport)
                .unwrap_or(policy.preferred_transports.len() + 16);
            let direct_bias = if policy.direct_first && info.direct {
                0
            } else {
                1
            };
            (direct_bias, preferred, info.priority, info.id)
        });
        underlays
    }

    fn accept_route_from_underlay(
        &self,
        underlay: &RouteOverlayUnderlayInfo,
        route: RouteEnvelope,
    ) -> bool {
        let decision = self.router.accept(route.clone());
        if decision.duplicate || !decision.deliver {
            return false;
        }
        self.dispatch_payload(underlay, &route, route.payload.clone())
    }

    fn dispatch_payload(
        &self,
        underlay: &RouteOverlayUnderlayInfo,
        route: &RouteEnvelope,
        payload: RoutePayload,
    ) -> bool {
        match payload {
            RoutePayload::Application { message } => {
                let event_id = format!("{}:{}", route.route_id, application_event_key(&message));
                let mut seen = self.seen_event_ids.lock().unwrap();
                if !seen.insert(event_id) {
                    return false;
                }
                drop(seen);

                let mut context =
                    ApplicationRouteContext::new(route.from.clone(), underlay.transport.clone());
                context.underlay_id = Some(underlay.id.clone());
                context.direct = underlay.direct;
                context.relay_routed = underlay.relay_routed;
                let event = ApplicationRouteEvent {
                    route_id: route.route_id.clone(),
                    from: route.from.clone(),
                    channel: route.channel.clone(),
                    target: route.target.clone(),
                    issued_at_millis: route.issued_at_millis,
                    received_at_millis: now_millis(),
                    transport: underlay.transport.clone(),
                    verified_identity: None,
                    context,
                    message: message.clone(),
                };
                if let Some(stream_event) = self.streams.lock().unwrap().accept_event(&event) {
                    self.stream_events.lock().unwrap().push_back(stream_event);
                }
                self.applications.publish(event);
                true
            }
            RoutePayload::Batch { items } => {
                let mut delivered = false;
                for item in items {
                    delivered |=
                        self.dispatch_payload(underlay, route, RoutePayload::from_batch_item(item));
                }
                delivered
            }
            _ => false,
        }
    }

    fn accept_application_event_from_underlay(
        &self,
        underlay: &RouteOverlayUnderlayInfo,
        mut event: ApplicationRouteEvent,
    ) -> bool {
        let event_id = format!(
            "{}:{}",
            event.route_id,
            application_event_key(&event.message)
        );
        let mut seen = self.seen_event_ids.lock().unwrap();
        if !seen.insert(event_id) {
            return false;
        }
        drop(seen);

        if event.context.underlay_id.is_none() {
            event.context.underlay_id = Some(underlay.id.clone());
        }
        if event.context.provenance.is_empty() {
            event
                .context
                .provenance
                .push(underlay.transport.as_str().to_owned());
        }
        if let Some(stream_event) = self.streams.lock().unwrap().accept_event(&event) {
            self.stream_events.lock().unwrap().push_back(stream_event);
        }
        self.applications.publish(event);
        true
    }
}

impl ApplicationStreamAssembler {
    pub fn accept_event(
        &mut self,
        event: &ApplicationRouteEvent,
    ) -> Option<ApplicationStreamEvent> {
        if event.message.namespace != APPLICATION_STREAM_NAMESPACE
            || event.message.protocol != APPLICATION_STREAM_PROTOCOL_V1
        {
            return None;
        }
        let frame: ApplicationStreamFrame =
            serde_json::from_value(event.message.body.clone()).ok()?;
        self.accept_frame(event, frame)
    }

    pub fn accept_frame(
        &mut self,
        event: &ApplicationRouteEvent,
        frame: ApplicationStreamFrame,
    ) -> Option<ApplicationStreamEvent> {
        match frame.kind {
            ApplicationStreamFrameKind::Open => {
                self.buffers.insert(
                    frame.stream_id.clone(),
                    ApplicationStreamBuffer {
                        from: event.from.clone(),
                        transport: event.transport.clone(),
                        namespace: frame.namespace,
                        protocol: frame.protocol,
                        topic: frame.topic,
                        metadata: frame.metadata,
                        chunks: BTreeMap::new(),
                        final_sequence: None,
                    },
                );
                None
            }
            ApplicationStreamFrameKind::Data => {
                let buffer = self
                    .buffers
                    .entry(frame.stream_id.clone())
                    .or_insert_with(|| ApplicationStreamBuffer {
                        from: event.from.clone(),
                        transport: event.transport.clone(),
                        namespace: frame.namespace.clone(),
                        protocol: frame.protocol.clone(),
                        topic: frame.topic.clone(),
                        metadata: frame.metadata.clone(),
                        chunks: BTreeMap::new(),
                        final_sequence: None,
                    });
                if let Some(chunk) = frame.chunk {
                    buffer.chunks.entry(frame.sequence).or_insert(chunk);
                }
                if frame.final_chunk {
                    buffer.final_sequence = Some(frame.sequence);
                }
                self.try_complete(&frame.stream_id)
            }
            ApplicationStreamFrameKind::Close => {
                if let Some(buffer) = self.buffers.get_mut(&frame.stream_id)
                    && buffer.final_sequence.is_none()
                    && frame.sequence > 0
                {
                    buffer.final_sequence = Some(frame.sequence.saturating_sub(1));
                }
                self.try_complete(&frame.stream_id)
            }
            ApplicationStreamFrameKind::Ack
            | ApplicationStreamFrameKind::Nack
            | ApplicationStreamFrameKind::Error => None,
        }
    }

    fn try_complete(&mut self, stream_id: &str) -> Option<ApplicationStreamEvent> {
        let final_sequence = self.buffers.get(stream_id)?.final_sequence?;
        let buffer = self.buffers.get(stream_id)?;
        let mut joined = String::new();
        for sequence in 1..=final_sequence {
            let chunk = buffer.chunks.get(&sequence)?;
            joined.push_str(chunk);
        }
        let body = serde_json::from_str(&joined).ok()?;
        let buffer = self.buffers.remove(stream_id)?;
        Some(ApplicationStreamEvent {
            stream_id: stream_id.to_owned(),
            from: buffer.from,
            transport: buffer.transport,
            namespace: buffer.namespace,
            protocol: buffer.protocol,
            topic: buffer.topic,
            body,
            metadata: buffer.metadata,
        })
    }
}

fn stream_frame_message(frame: ApplicationStreamFrame) -> Result<ApplicationRouteMessage> {
    Ok(ApplicationRouteMessage::new(
        APPLICATION_STREAM_NAMESPACE,
        APPLICATION_STREAM_PROTOCOL_V1,
        frame.topic.clone(),
        serde_json::to_value(frame)?,
        BTreeMap::new(),
    ))
}

fn chunk_string(value: &str, max_chars: usize) -> Vec<String> {
    if value.is_empty() {
        return vec![String::new()];
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for character in value.chars() {
        current.push(character);
        if current.chars().count() >= max_chars {
            chunks.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn application_event_key(message: &ApplicationRouteMessage) -> String {
    format!(
        "{}:{}:{}",
        message.namespace,
        message.protocol,
        message.topic.as_deref().unwrap_or_default()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryRouteHub, PrimadbError};

    fn overlay(peer_id: &str) -> RouteOverlaySession {
        RouteOverlaySession::new(RouterConfig {
            peer_id: peer_id.to_owned(),
            default_channel: "room".to_owned(),
            default_ttl: 6,
            max_seen_routes: 256,
        })
    }

    fn presence(peer_id: &str) -> RouteEnvelope {
        Router::new(RouterConfig {
            peer_id: peer_id.to_owned(),
            default_channel: "room".to_owned(),
            default_ttl: 6,
            max_seen_routes: 256,
        })
        .presence(
            format!("{peer_id}-replica"),
            "in_memory",
            vec!["application_routes".to_owned()],
            vec!["room".to_owned()],
        )
    }

    fn app_message(value: &str) -> ApplicationRouteMessage {
        ApplicationRouteMessage::new(
            "test.mesh",
            "message.v1",
            Some("chat".to_owned()),
            serde_json::json!({ "value": value }),
            BTreeMap::new(),
        )
    }

    #[test]
    fn overlay_falls_back_after_failed_underlay() {
        let hub = InMemoryRouteHub::new();
        let sender_session = hub.connect();
        let receiver_session = hub.connect();
        receiver_session.send(presence("receiver")).unwrap();
        while sender_session.try_recv().is_some() {}

        let sender = overlay("sender");
        sender.add_underlay(RouteOverlayUnderlayHandle::sender(
            "failed",
            RouteTransportKind::WebRtc,
            true,
            false,
            |_| Err(PrimadbError::Message("not open".to_owned())),
        ));
        sender.add_underlay(RouteOverlayUnderlayHandle::in_memory(
            "memory",
            sender_session,
        ));

        let report = sender
            .publish_application(
                app_message("hello"),
                RouteTarget::Peer("receiver".to_owned()),
            )
            .unwrap();
        assert_eq!(report.attempts.len(), 2);
        assert_eq!(report.delivered_underlay_ids, vec!["memory"]);
        assert_eq!(report.failed_underlay_ids, vec!["failed"]);
        assert!(report.fallback_reason.is_some());
        assert!(receiver_session.try_recv().is_some());
    }

    #[test]
    fn overlay_fanout_suppresses_duplicate_receive() {
        let hub_a = InMemoryRouteHub::new();
        let hub_b = InMemoryRouteHub::new();
        let sender_a = hub_a.connect();
        let receiver_a = hub_a.connect();
        let sender_b = hub_b.connect();
        let receiver_b = hub_b.connect();
        receiver_a.send(presence("receiver")).unwrap();
        receiver_b.send(presence("receiver")).unwrap();
        while sender_a.try_recv().is_some() {}
        while sender_b.try_recv().is_some() {}

        let sender = overlay("sender");
        sender.set_policy(RouteOverlayPolicy {
            send_mode: RouteOverlaySendMode::FanOut,
            ..RouteOverlayPolicy::default()
        });
        sender.add_underlay(RouteOverlayUnderlayHandle::in_memory("a", sender_a));
        sender.add_underlay(RouteOverlayUnderlayHandle::in_memory("b", sender_b));

        let receiver = overlay("receiver");
        receiver.add_underlay(RouteOverlayUnderlayHandle::in_memory("a", receiver_a));
        receiver.add_underlay(RouteOverlayUnderlayHandle::in_memory("b", receiver_b));
        let subscription = receiver.subscribe_applications(ApplicationRouteFilter {
            namespace: Some("test.mesh".to_owned()),
            protocol: None,
            topic: None,
        });

        let report = sender
            .publish_application(
                app_message("hello"),
                RouteTarget::Peer("receiver".to_owned()),
            )
            .unwrap();
        assert_eq!(report.delivered_underlay_ids.len(), 2);
        let pump = receiver.pump();
        assert_eq!(pump.received_routes, 2);
        assert_eq!(pump.delivered_application_routes, 1);
        assert_eq!(pump.duplicate_suppressed, 1);
        assert_eq!(subscription.drain().len(), 1);
    }

    #[test]
    fn overlay_application_stream_reassembles_chunks() {
        let hub = InMemoryRouteHub::new();
        let sender_session = hub.connect();
        let receiver_session = hub.connect();
        receiver_session.send(presence("receiver")).unwrap();
        while sender_session.try_recv().is_some() {}

        let sender = overlay("sender");
        sender.add_underlay(RouteOverlayUnderlayHandle::in_memory(
            "memory",
            sender_session,
        ));
        let receiver = overlay("receiver");
        receiver.add_underlay(RouteOverlayUnderlayHandle::in_memory(
            "memory",
            receiver_session,
        ));

        let stream = sender
            .send_application_stream(ApplicationStreamSendOptions {
                namespace: "starla.mesh".to_owned(),
                protocol: "channel.v1".to_owned(),
                topic: Some("chat".to_owned()),
                body: serde_json::json!({ "text": "hello world" }),
                metadata: BTreeMap::new(),
                target: RouteTarget::Peer("receiver".to_owned()),
                max_chunk_chars: 5,
            })
            .unwrap();
        assert!(stream.frame_reports.len() > 2);
        receiver.pump();
        let events = receiver.drain_stream_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].body, serde_json::json!({ "text": "hello world" }));
        assert_eq!(events[0].namespace, "starla.mesh");
        assert_eq!(events[0].protocol, "channel.v1");
    }

    #[test]
    fn stream_assembler_handles_out_of_order_chunks() {
        let mut assembler = ApplicationStreamAssembler::default();
        let event = ApplicationRouteEvent {
            route_id: "route".to_owned(),
            from: "peer-a".to_owned(),
            channel: "room".to_owned(),
            target: RouteTarget::Broadcast,
            issued_at_millis: 1,
            received_at_millis: 2,
            transport: RouteTransportKind::InMemory,
            verified_identity: None,
            context: ApplicationRouteContext::new("peer-a", RouteTransportKind::InMemory),
            message: app_message("unused"),
        };
        let base = ApplicationStreamFrame {
            stream_id: "stream-1".to_owned(),
            sequence: 0,
            kind: ApplicationStreamFrameKind::Open,
            namespace: "ns".to_owned(),
            protocol: "proto".to_owned(),
            topic: None,
            chunk: None,
            final_chunk: false,
            ack_sequence: None,
            error: None,
            metadata: BTreeMap::new(),
        };
        assert!(assembler.accept_frame(&event, base.clone()).is_none());
        let mut second = base.clone();
        second.kind = ApplicationStreamFrameKind::Data;
        second.sequence = 2;
        second.chunk = Some("}".to_owned());
        second.final_chunk = true;
        assert!(assembler.accept_frame(&event, second).is_none());
        let mut first = base;
        first.kind = ApplicationStreamFrameKind::Data;
        first.sequence = 1;
        first.chunk = Some("{\"ok\":true".to_owned());
        let complete = assembler.accept_frame(&event, first).unwrap();
        assert_eq!(complete.body, serde_json::json!({"ok": true}));
    }

    #[test]
    fn route_overlay_underlay_info_serializes() {
        let info = RouteOverlayUnderlayInfo {
            id: "memory".to_owned(),
            transport: RouteTransportKind::InMemory,
            direct: false,
            relay_routed: true,
            connected: true,
            priority: 0,
            metadata: BTreeMap::new(),
        };
        let encoded = serde_json::to_value(info).unwrap();
        assert_eq!(encoded["transport"], "in_memory");
    }

    #[tokio::test]
    async fn overlay_async_underlay_sends() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_underlay = sent.clone();
        let sender = overlay("sender");
        sender.add_underlay(RouteOverlayUnderlayHandle::async_sender(
            "async-webrtc",
            RouteTransportKind::WebRtc,
            true,
            false,
            move |route| {
                let sent_underlay = sent_underlay.clone();
                async move {
                    sent_underlay.lock().unwrap().push(route);
                    Ok(())
                }
            },
        ));

        let report = sender
            .publish_application_async(app_message("hello"), RouteTarget::Broadcast)
            .await
            .unwrap();
        assert_eq!(report.delivered_underlay_ids, vec!["async-webrtc"]);
        assert_eq!(sent.lock().unwrap().len(), 1);
    }
}
