use crate::clock::now_millis;
use crate::{MoqRelayClientConfig, PrimadbError, Result, RouteEnvelope};
use async_channel::{Receiver, Sender, unbounded};
use bytes::Bytes;
use moq_lite::{Broadcast, Origin, OriginConsumer, Track};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use url::Url;

const ROUTE_FRAME_TYPE: &str = "primadb.route.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MoqRouteFrame {
    #[serde(rename = "type")]
    frame_type: String,
    from: String,
    sent_at: u64,
    route: RouteEnvelope,
}

pub struct NativeMoqRouteClient {
    config: MoqRelayClientConfig,
    outbound: Sender<RouteEnvelope>,
    inbound: Receiver<RouteEnvelope>,
    connected: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl NativeMoqRouteClient {
    pub async fn connect(config: MoqRelayClientConfig) -> Result<Self> {
        let (outbound, outbound_rx) = unbounded();
        let (inbound_tx, inbound) = unbounded();
        let connected = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));

        let task_config = config.clone();
        let task_connected = connected.clone();
        let task_closed = closed.clone();
        let task = tokio::spawn(async move {
            run_route_client(
                task_config,
                outbound_rx,
                inbound_tx,
                task_connected,
                task_closed,
            )
            .await;
        });

        Ok(Self {
            config,
            outbound,
            inbound,
            connected,
            closed,
            task: Some(task),
        })
    }

    pub fn config(&self) -> &MoqRelayClientConfig {
        &self.config
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    pub fn send_route(&self, route: RouteEnvelope) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(PrimadbError::Message(
                "native MoQ route client is closed".to_owned(),
            ));
        }
        self.outbound
            .try_send(route)
            .map_err(|error| PrimadbError::Message(error.to_string()))
    }

    pub async fn recv_route(&self) -> Result<RouteEnvelope> {
        self.inbound
            .recv()
            .await
            .map_err(|error| PrimadbError::Message(error.to_string()))
    }

    pub fn try_recv_route(&self) -> Option<RouteEnvelope> {
        self.inbound.try_recv().ok()
    }

    pub fn shutdown(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
        self.outbound.close();
    }

    pub async fn close(&mut self) {
        self.shutdown();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for NativeMoqRouteClient {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
        self.outbound.close();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run_route_client(
    config: MoqRelayClientConfig,
    outbound: Receiver<RouteEnvelope>,
    inbound: Sender<RouteEnvelope>,
    connected: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
) {
    let retry_interval = Duration::from_millis(config.retry_interval_ms.max(1));
    while !closed.load(Ordering::SeqCst) {
        if run_route_session(
            &config,
            outbound.clone(),
            inbound.clone(),
            connected.clone(),
            closed.clone(),
        )
        .await
        .is_err()
        {
            connected.store(false, Ordering::SeqCst);
        }
        if closed.load(Ordering::SeqCst) {
            break;
        }
        tokio::time::sleep(retry_interval).await;
    }
}

async fn run_route_session(
    config: &MoqRelayClientConfig,
    outbound: Receiver<RouteEnvelope>,
    inbound: Sender<RouteEnvelope>,
    connected: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
) -> Result<()> {
    let url = Url::parse(&config.url)
        .map_err(|error| PrimadbError::Message(format!("invalid MoQ relay URL: {error}")))?;
    let client = moq_native::ClientConfig::default()
        .init()
        .map_err(|error| PrimadbError::Message(error.to_string()))?;

    let local_origin = Origin::random().produce();
    let remote_origin = Origin::random().produce();
    let remote_consumer = remote_origin.consume();

    let mut broadcast = Broadcast::new().produce();
    let track = broadcast
        .create_track(Track::new(config.track.clone()))
        .map_err(|error| PrimadbError::Message(error.to_string()))?;
    if !local_origin.publish_broadcast(config.path.as_str(), broadcast.consume()) {
        return Err(PrimadbError::Message(format!(
            "MoQ route path `{}` is outside the publish scope",
            config.path
        )));
    }

    let session = client
        .with_publish(local_origin.consume())
        .with_consume(remote_origin)
        .connect(url)
        .await
        .map_err(|error| PrimadbError::Message(error.to_string()))?;
    connected.store(true, Ordering::SeqCst);

    let writer_closed = closed.clone();
    let writer = tokio::spawn(write_route_track(outbound, track, writer_closed));

    let mut readers = Vec::new();
    let subscribe_paths = if config.subscribe.is_empty() {
        vec![config.path.clone()]
    } else {
        config.subscribe.clone()
    };
    for path in subscribe_paths {
        readers.push(tokio::spawn(read_route_path(
            remote_consumer.consume(),
            path,
            config.track.clone(),
            inbound.clone(),
            closed.clone(),
        )));
    }

    loop {
        tokio::select! {
            result = session.closed() => {
                connected.store(false, Ordering::SeqCst);
                if let Err(error) = result {
                    return Err(PrimadbError::Message(error.to_string()));
                }
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if closed.load(Ordering::SeqCst) {
                    connected.store(false, Ordering::SeqCst);
                    break;
                }
            }
        }
    }

    writer.abort();
    let _ = writer.await;
    for reader in readers {
        reader.abort();
        let _ = reader.await;
    }
    Ok(())
}

async fn write_route_track(
    outbound: Receiver<RouteEnvelope>,
    mut track: moq_lite::TrackProducer,
    closed: Arc<AtomicBool>,
) {
    while !closed.load(Ordering::SeqCst) {
        let Ok(route) = outbound.recv().await else {
            break;
        };
        let frame = MoqRouteFrame {
            frame_type: ROUTE_FRAME_TYPE.to_owned(),
            from: route.from.clone(),
            sent_at: now_millis(),
            route,
        };
        let Ok(payload) = serde_json::to_vec(&frame) else {
            continue;
        };
        if track.write_frame(Bytes::from(payload)).is_err() {
            break;
        }
    }
}

async fn read_route_path(
    consumer: OriginConsumer,
    path: String,
    track_name: String,
    inbound: Sender<RouteEnvelope>,
    closed: Arc<AtomicBool>,
) {
    while !closed.load(Ordering::SeqCst) {
        let Some(broadcast) = consumer.announced_broadcast(path.as_str()).await else {
            break;
        };
        let Ok(mut track) = broadcast.subscribe_track(&Track::new(track_name.clone())) else {
            continue;
        };
        while !closed.load(Ordering::SeqCst) {
            let frame = match track.read_frame().await {
                Ok(Some(frame)) => frame,
                Ok(None) | Err(_) => break,
            };
            let Some(route) = decode_route_frame(&frame) else {
                continue;
            };
            let _ = inbound.try_send(route);
        }
    }
}

fn decode_route_frame(frame: &[u8]) -> Option<RouteEnvelope> {
    if let Ok(frame) = serde_json::from_slice::<MoqRouteFrame>(frame) {
        if frame.frame_type == ROUTE_FRAME_TYPE {
            return Some(frame.route);
        }
    }
    serde_json::from_slice::<RouteEnvelope>(frame).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RoutePayload, RouteTarget, Router, RouterConfig};

    #[test]
    fn moq_route_frame_decodes_wrapped_and_bare_routes() {
        let router = Router::new(RouterConfig::new("moq:test"));
        let route = router.peer_exchange(Vec::new(), RouteTarget::Broadcast, None);
        let frame = MoqRouteFrame {
            frame_type: ROUTE_FRAME_TYPE.to_owned(),
            from: route.from.clone(),
            sent_at: 1,
            route: route.clone(),
        };

        let encoded = serde_json::to_vec(&frame).unwrap();
        assert_eq!(decode_route_frame(&encoded), Some(route.clone()));

        let bare = serde_json::to_vec(&route).unwrap();
        assert_eq!(decode_route_frame(&bare), Some(route));
    }

    #[test]
    fn moq_route_frame_rejects_wrong_frame_type() {
        let router = Router::new(RouterConfig::new("moq:test"));
        let route = router.peer_exchange(Vec::new(), RouteTarget::Broadcast, None);
        let frame = MoqRouteFrame {
            frame_type: "other".to_owned(),
            from: route.from.clone(),
            sent_at: 1,
            route,
        };

        let encoded = serde_json::to_vec(&frame).unwrap();
        assert!(decode_route_frame(&encoded).is_none());
    }

    #[test]
    fn moq_route_frame_preserves_route_payload() {
        let router = Router::new(RouterConfig::new("moq:test"));
        let route = router.wrap_signal(
            "room",
            serde_json::json!({ "signal": true }),
            RouteTarget::Peer("peer-b".to_owned()),
        );
        let encoded = serde_json::to_vec(&MoqRouteFrame {
            frame_type: ROUTE_FRAME_TYPE.to_owned(),
            from: route.from.clone(),
            sent_at: 1,
            route: route.clone(),
        })
        .unwrap();
        let decoded = decode_route_frame(&encoded).unwrap();
        assert!(matches!(decoded.payload, RoutePayload::Signal { .. }));
        assert_eq!(decoded.target, RouteTarget::Peer("peer-b".to_owned()));
    }
}
