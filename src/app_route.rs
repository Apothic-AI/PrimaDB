use crate::{RouteTarget, RouteTransportKind, VerifiedIdentity};
use async_channel::Receiver;
#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
use async_channel::{Sender, bounded};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
use std::sync::{Arc, Mutex};

#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
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
    pub message: ApplicationRouteMessage,
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
#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
pub(crate) struct ApplicationRouteBus {
    capacity: usize,
    subscribers: Arc<Mutex<Vec<ApplicationRouteSubscriber>>>,
}

#[derive(Debug)]
#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
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

#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
impl Default for ApplicationRouteBus {
    fn default() -> Self {
        Self::new(DEFAULT_APPLICATION_QUEUE_CAPACITY)
    }
}

#[cfg(any(test, target_arch = "wasm32", feature = "native-websocket"))]
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
        ApplicationRouteBus, ApplicationRouteEvent, ApplicationRouteFilter, ApplicationRouteMessage,
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
