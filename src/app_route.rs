use crate::{RouteTarget, RouteTransportKind, VerifiedIdentity};
use async_channel::Receiver;
use async_channel::{Sender, bounded};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

const DEFAULT_APPLICATION_QUEUE_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationRouteMessage {
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
pub struct ApplicationRouteEvent {
    pub route_id: String,
    pub from: String,
    pub channel: String,
    pub target: RouteTarget,
    pub issued_at_millis: u64,
    pub received_at_millis: u64,
    pub transport: RouteTransportKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_identity: Option<VerifiedIdentity>,
    #[serde(default)]
    pub context: ApplicationRouteContext,
    pub message: ApplicationRouteMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationRouteAuthStatus {
    Unknown,
    NotRequired,
    Unauthenticated,
    Authenticated,
    RequiredButMissing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationRouteContext {
    pub source_peer_id: String,
    pub transport: RouteTransportKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub underlay_id: Option<String>,
    #[serde(default)]
    pub direct: bool,
    #[serde(default)]
    pub relay_routed: bool,
    #[serde(default)]
    pub gateway_routed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_peer_id: Option<String>,
    #[serde(default)]
    pub auth_status: ApplicationRouteAuthStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub provenance: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationRouteFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
}

pub struct ApplicationRouteSubscription {
    receiver: Receiver<ApplicationRouteEvent>,
}

#[derive(Debug, Clone)]
pub(crate) struct ApplicationRouteBus {
    capacity: usize,
    subscribers: Arc<Mutex<Vec<ApplicationRouteSubscriber>>>,
}

#[derive(Debug)]
struct ApplicationRouteSubscriber {
    filter: ApplicationRouteFilter,
    sender: Sender<ApplicationRouteEvent>,
}

impl ApplicationRouteMessage {
    pub fn new(
        namespace: impl Into<String>,
        protocol: impl Into<String>,
        topic: Option<String>,
        body: JsonValue,
        metadata: BTreeMap<String, JsonValue>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            protocol: protocol.into(),
            topic,
            body,
            metadata,
        }
    }
}

impl Default for ApplicationRouteAuthStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

impl ApplicationRouteContext {
    pub fn new(source_peer_id: impl Into<String>, transport: RouteTransportKind) -> Self {
        Self {
            source_peer_id: source_peer_id.into(),
            transport,
            underlay_id: None,
            direct: false,
            relay_routed: false,
            gateway_routed: false,
            gateway_peer_id: None,
            auth_status: ApplicationRouteAuthStatus::Unknown,
            provenance: Vec::new(),
        }
    }

    pub fn with_verified_identity(
        source_peer_id: impl Into<String>,
        transport: RouteTransportKind,
        verified_identity: Option<&VerifiedIdentity>,
        require_authenticated: bool,
    ) -> Self {
        let auth_status = if verified_identity.is_some() {
            ApplicationRouteAuthStatus::Authenticated
        } else if require_authenticated {
            ApplicationRouteAuthStatus::RequiredButMissing
        } else {
            ApplicationRouteAuthStatus::NotRequired
        };
        Self {
            auth_status,
            ..Self::new(source_peer_id, transport)
        }
    }
}

impl Default for ApplicationRouteContext {
    fn default() -> Self {
        Self::new("", RouteTransportKind::InMemory)
    }
}

impl ApplicationRouteFilter {
    pub fn matches(&self, message: &ApplicationRouteMessage) -> bool {
        self.namespace
            .as_ref()
            .is_none_or(|namespace| namespace == &message.namespace)
            && self
                .protocol
                .as_ref()
                .is_none_or(|protocol| protocol == &message.protocol)
            && self.topic.as_ref().is_none_or(|topic| {
                message
                    .topic
                    .as_ref()
                    .is_some_and(|candidate| candidate == topic)
            })
    }
}

impl Default for ApplicationRouteBus {
    fn default() -> Self {
        Self::new(DEFAULT_APPLICATION_QUEUE_CAPACITY)
    }
}

impl ApplicationRouteBus {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            subscribers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn subscribe(&self, filter: ApplicationRouteFilter) -> ApplicationRouteSubscription {
        let (sender, receiver) = bounded(self.capacity);
        self.subscribers
            .lock()
            .unwrap()
            .push(ApplicationRouteSubscriber { filter, sender });
        ApplicationRouteSubscription { receiver }
    }

    pub(crate) fn publish(&self, event: ApplicationRouteEvent) -> usize {
        let mut delivered = 0;
        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.retain(|subscriber| !subscriber.sender.is_closed());
        for subscriber in subscribers.iter() {
            if subscriber.filter.matches(&event.message)
                && subscriber.sender.try_send(event.clone()).is_ok()
            {
                delivered += 1;
            }
        }
        delivered
    }
}

impl ApplicationRouteSubscription {
    pub async fn recv(&self) -> Option<ApplicationRouteEvent> {
        self.receiver.recv().await.ok()
    }

    pub fn try_recv(&self) -> Option<ApplicationRouteEvent> {
        self.receiver.try_recv().ok()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn recv_blocking(&self) -> Option<ApplicationRouteEvent> {
        self.receiver.recv_blocking().ok()
    }

    pub fn drain(&self) -> Vec<ApplicationRouteEvent> {
        let mut events = Vec::new();
        while let Ok(event) = self.receiver.try_recv() {
            events.push(event);
        }
        events
    }

    pub fn close(&self) {
        self.receiver.close();
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ApplicationRouteBus, ApplicationRouteContext, ApplicationRouteEvent,
        ApplicationRouteFilter, ApplicationRouteMessage,
    };
    use crate::{RouteTarget, RouteTransportKind};
    use serde_json::json;
    use std::collections::BTreeMap;

    fn event(protocol: &str, topic: Option<&str>) -> ApplicationRouteEvent {
        ApplicationRouteEvent {
            route_id: "route-1".to_owned(),
            from: "peer-a".to_owned(),
            channel: "starla".to_owned(),
            target: RouteTarget::Broadcast,
            issued_at_millis: 1,
            received_at_millis: 2,
            transport: RouteTransportKind::InMemory,
            verified_identity: None,
            context: ApplicationRouteContext::new("peer-a", RouteTransportKind::InMemory),
            message: ApplicationRouteMessage::new(
                "starla.mesh",
                protocol,
                topic.map(str::to_owned),
                json!({"ok": true}),
                BTreeMap::new(),
            ),
        }
    }

    #[test]
    fn application_bus_filters_and_drains_events() {
        let bus = ApplicationRouteBus::default();
        let subscription = bus.subscribe(ApplicationRouteFilter {
            namespace: Some("starla.mesh".to_owned()),
            protocol: Some("trust.v1".to_owned()),
            topic: Some("proposal".to_owned()),
        });

        assert_eq!(bus.publish(event("memory.v1", Some("proposal"))), 0);
        assert_eq!(bus.publish(event("trust.v1", Some("proposal"))), 1);
        assert_eq!(subscription.drain().len(), 1);
        assert!(subscription.try_recv().is_none());
    }
}
