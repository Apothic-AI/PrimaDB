use crate::clock::now_millis;
use crate::{MoqRelayClientConfig, PrimadbError, Result, RouteEnvelope};
use async_channel::{Receiver, Sender, unbounded};
use bytes::Bytes;
use moq_native_ietf::quic;
use moq_transport::coding::TrackNamespace;
use moq_transport::serve::{TrackReader, TrackReaderMode, Tracks};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use url::Url;

const ROUTE_FRAME_TYPE: &str = "primadb.route.v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IetfMoqRouteFrame {
    #[serde(rename = "type")]
    frame_type: String,
    from: String,
    sent_at: u64,
    route: RouteEnvelope,
}

pub struct NativeIetfMoqRouteClient {
    config: MoqRelayClientConfig,
    outbound: Sender<RouteEnvelope>,
    inbound: Receiver<RouteEnvelope>,
    connected: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    task: Option<JoinHandle<()>>,
}

impl NativeIetfMoqRouteClient {
    pub async fn connect(config: MoqRelayClientConfig) -> Result<Self> {
        let (outbound, outbound_rx) = unbounded();
        let (inbound_tx, inbound) = unbounded();
        let connected = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));

        let task_config = config.clone();
        let task_connected = connected.clone();
        let task_closed = closed.clone();
        let task = tokio::spawn(async move {
            run_ietf_route_client(
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
                "native IETF MoQ route client is closed".to_owned(),
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

impl Drop for NativeIetfMoqRouteClient {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
        self.outbound.close();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn run_ietf_route_client(
    config: MoqRelayClientConfig,
    outbound: Receiver<RouteEnvelope>,
    inbound: Sender<RouteEnvelope>,
    connected: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
) {
    let retry_interval = Duration::from_millis(config.retry_interval_ms.max(1));
    while !closed.load(Ordering::SeqCst) {
        if run_ietf_route_session(
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

async fn run_ietf_route_session(
    config: &MoqRelayClientConfig,
    outbound: Receiver<RouteEnvelope>,
    inbound: Sender<RouteEnvelope>,
    connected: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
) -> Result<()> {
    let url = Url::parse(&config.url)
        .map_err(|error| PrimadbError::Message(format!("invalid MoQ relay URL: {error}")))?;
    let namespace = route_namespace(&config.path)?;
    let subscribe_namespaces = if config.subscribe.is_empty() {
        vec![config.path.clone()]
    } else {
        config.subscribe.clone()
    };

    let mut tls_args = moq_native_ietf::tls::Args::default();
    tls_args.disable_verify = config.tls_disable_verify
        || truthy_env("PRIMADB_MOQ_TLS_DISABLE_VERIFY")
        || truthy_env("TLS_DISABLE_VERIFY");
    let tls = tls_args
        .load()
        .map_err(|error| PrimadbError::Message(error.to_string()))?;

    let bind: SocketAddr = "[::]:0"
        .parse()
        .map_err(|error| PrimadbError::Message(format!("invalid MoQ bind address: {error}")))?;
    let quic_config = quic::Config::new(bind, None, tls)
        .map_err(|error| PrimadbError::Message(error.to_string()))?;
    let endpoint = quic::Endpoint::new(quic_config)
        .map_err(|error| PrimadbError::Message(error.to_string()))?;

    let (webtransport, _connection_id, transport) = endpoint
        .client
        .connect(&url, None)
        .await
        .map_err(|error| PrimadbError::Message(error.to_string()))?;
    let (session, mut publisher, subscriber) =
        moq_transport::session::Session::connect(webtransport, None, transport)
            .await
            .map_err(|error| PrimadbError::Message(error.to_string()))?;
    connected.store(true, Ordering::SeqCst);

    let (mut writer, _request, reader) = Tracks::new(namespace).produce();
    let track = writer
        .create(&config.track)
        .ok_or_else(|| PrimadbError::Message("IETF MoQ route tracks closed".to_owned()))?;
    let subgroups = track
        .subgroups()
        .map_err(|error| PrimadbError::Message(error.to_string()))?;

    let writer_closed = closed.clone();
    let writer_task = tokio::spawn(write_ietf_route_track(outbound, subgroups, writer_closed));
    let announce_task = tokio::spawn(async move {
        publisher
            .announce(reader)
            .await
            .map_err(|error| error.to_string())
    });

    let mut reader_tasks = Vec::new();
    for path in subscribe_namespaces {
        reader_tasks.push(tokio::spawn(read_ietf_route_path(
            subscriber.clone(),
            path,
            config.track.clone(),
            inbound.clone(),
            closed.clone(),
            config.retry_interval_ms.max(1),
        )));
    }

    let result = tokio::select! {
        result = session.run() => {
            connected.store(false, Ordering::SeqCst);
            result.map_err(|error| PrimadbError::Message(error.to_string()))
        }
        result = announce_task => {
            connected.store(false, Ordering::SeqCst);
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(PrimadbError::Message(error)),
                Err(error) => Err(PrimadbError::Message(error.to_string())),
            }
        }
        result = writer_task => {
            connected.store(false, Ordering::SeqCst);
            match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(PrimadbError::Message(error.to_string())),
            }
        }
    };

    for reader in reader_tasks {
        reader.abort();
        let _ = reader.await;
    }

    result
}

async fn write_ietf_route_track(
    outbound: Receiver<RouteEnvelope>,
    mut track: moq_transport::serve::SubgroupsWriter,
    closed: Arc<AtomicBool>,
) -> Result<()> {
    while !closed.load(Ordering::SeqCst) {
        let Ok(route) = outbound.recv().await else {
            break;
        };
        let frame = IetfMoqRouteFrame {
            frame_type: ROUTE_FRAME_TYPE.to_owned(),
            from: route.from.clone(),
            sent_at: now_millis(),
            route,
        };
        let payload =
            serde_json::to_vec(&frame).map_err(|error| PrimadbError::Message(error.to_string()))?;
        let mut group = track
            .append(0)
            .map_err(|error| PrimadbError::Message(error.to_string()))?;
        group
            .write(Bytes::from(payload))
            .map_err(|error| PrimadbError::Message(error.to_string()))?;
    }
    Ok(())
}

async fn read_ietf_route_path(
    subscriber: moq_transport::session::Subscriber,
    path: String,
    track_name: String,
    inbound: Sender<RouteEnvelope>,
    closed: Arc<AtomicBool>,
    retry_interval_ms: u64,
) {
    let namespace = match route_namespace(&path) {
        Ok(namespace) => namespace,
        Err(_) => return,
    };
    let retry_interval = Duration::from_millis(retry_interval_ms);

    while !closed.load(Ordering::SeqCst) {
        let (mut writer, _request, mut reader) = Tracks::new(namespace.clone()).produce();
        let Some(track) = writer.create(&track_name) else {
            return;
        };
        let mut subscribe_task = tokio::spawn({
            let mut subscriber = subscriber.clone();
            async move { subscriber.subscribe(track).await }
        });

        let Some(track_reader) = reader.subscribe(namespace.clone(), &track_name) else {
            subscribe_task.abort();
            let _ = subscribe_task.await;
            return;
        };
        let mut read_task = tokio::spawn(read_ietf_track(
            track_reader,
            inbound.clone(),
            closed.clone(),
        ));

        let subscribe_finished = tokio::select! {
            _ = &mut subscribe_task => true,
            _ = &mut read_task => false,
        };
        if !subscribe_finished {
            subscribe_task.abort();
            let _ = subscribe_task.await;
        }
        if subscribe_finished {
            read_task.abort();
            let _ = read_task.await;
        }

        if !closed.load(Ordering::SeqCst) {
            tokio::time::sleep(retry_interval).await;
        }
    }
}

async fn read_ietf_track(
    track: TrackReader,
    inbound: Sender<RouteEnvelope>,
    closed: Arc<AtomicBool>,
) {
    let Ok(mode) = track.mode().await else {
        return;
    };
    let TrackReaderMode::Subgroups(mut groups) = mode else {
        return;
    };

    while !closed.load(Ordering::SeqCst) {
        let group = match groups.next().await {
            Ok(Some(group)) => group,
            Ok(None) | Err(_) => break,
        };
        read_ietf_group(group, &inbound, &closed).await;
    }
}

async fn read_ietf_group(
    mut group: moq_transport::serve::SubgroupReader,
    inbound: &Sender<RouteEnvelope>,
    closed: &Arc<AtomicBool>,
) {
    while !closed.load(Ordering::SeqCst) {
        let mut object = match group.next().await {
            Ok(Some(object)) => object,
            Ok(None) | Err(_) => break,
        };
        let payload = match object.read_all().await {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        let Some(route) = decode_ietf_route_frame(&payload) else {
            continue;
        };
        let _ = inbound.try_send(route);
    }
}

fn route_namespace(path: &str) -> Result<TrackNamespace> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Err(PrimadbError::Message(
            "IETF MoQ route namespace cannot be empty".to_owned(),
        ));
    }
    Ok(TrackNamespace::from_utf8_path(trimmed))
}

fn truthy_env(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

fn decode_ietf_route_frame(frame: &[u8]) -> Option<RouteEnvelope> {
    if let Ok(frame) = serde_json::from_slice::<IetfMoqRouteFrame>(frame) {
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
    fn ietf_moq_route_frame_decodes_wrapped_and_bare_routes() {
        let router = Router::new(RouterConfig::new("moq:test"));
        let route = router.peer_exchange(Vec::new(), RouteTarget::Broadcast, None);
        let frame = IetfMoqRouteFrame {
            frame_type: ROUTE_FRAME_TYPE.to_owned(),
            from: route.from.clone(),
            sent_at: 1,
            route: route.clone(),
        };

        let encoded = serde_json::to_vec(&frame).unwrap();
        assert_eq!(decode_ietf_route_frame(&encoded), Some(route.clone()));

        let bare = serde_json::to_vec(&route).unwrap();
        assert_eq!(decode_ietf_route_frame(&bare), Some(route));
    }

    #[test]
    fn ietf_moq_route_frame_rejects_wrong_frame_type() {
        let router = Router::new(RouterConfig::new("moq:test"));
        let route = router.peer_exchange(Vec::new(), RouteTarget::Broadcast, None);
        let frame = IetfMoqRouteFrame {
            frame_type: "other".to_owned(),
            from: route.from.clone(),
            sent_at: 1,
            route,
        };

        let encoded = serde_json::to_vec(&frame).unwrap();
        assert!(decode_ietf_route_frame(&encoded).is_none());
    }

    #[test]
    fn ietf_moq_route_frame_preserves_route_payload() {
        let router = Router::new(RouterConfig::new("moq:test"));
        let route = router.wrap_signal(
            "room",
            serde_json::json!({ "signal": true }),
            RouteTarget::Peer("peer-b".to_owned()),
        );
        let encoded = serde_json::to_vec(&IetfMoqRouteFrame {
            frame_type: ROUTE_FRAME_TYPE.to_owned(),
            from: route.from.clone(),
            sent_at: 1,
            route: route.clone(),
        })
        .unwrap();
        let decoded = decode_ietf_route_frame(&encoded).unwrap();
        assert!(matches!(decoded.payload, RoutePayload::Signal { .. }));
        assert_eq!(decoded.target, RouteTarget::Peer("peer-b".to_owned()));
    }
}
