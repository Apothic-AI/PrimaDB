use crate::native_moq_draft07::NativeDraft07MoqRouteClient;
use crate::native_moq_ietf::NativeIetfMoqRouteClient;
use crate::{MoqDraft, MoqRelayClientConfig, Result, RouteEnvelope};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeMoqRouteClientBackend {
    IetfDraft07,
    IetfDraft14,
}

pub struct NativeMoqRouteClient {
    inner: NativeMoqRouteClientInner,
}

enum NativeMoqRouteClientInner {
    Draft07(NativeDraft07MoqRouteClient),
    Draft14(NativeIetfMoqRouteClient),
}

impl NativeMoqRouteClient {
    pub async fn connect(config: MoqRelayClientConfig) -> Result<Self> {
        let inner = match config.draft {
            MoqDraft::Draft07 => NativeMoqRouteClientInner::Draft07(
                NativeDraft07MoqRouteClient::connect(config).await?,
            ),
            MoqDraft::Draft14 | MoqDraft::DraftLatest => {
                NativeMoqRouteClientInner::Draft14(NativeIetfMoqRouteClient::connect(config).await?)
            }
        };
        Ok(Self { inner })
    }

    pub fn backend(&self) -> NativeMoqRouteClientBackend {
        match &self.inner {
            NativeMoqRouteClientInner::Draft07(_) => NativeMoqRouteClientBackend::IetfDraft07,
            NativeMoqRouteClientInner::Draft14(_) => NativeMoqRouteClientBackend::IetfDraft14,
        }
    }

    pub fn config(&self) -> &MoqRelayClientConfig {
        match &self.inner {
            NativeMoqRouteClientInner::Draft07(client) => client.config(),
            NativeMoqRouteClientInner::Draft14(client) => client.config(),
        }
    }

    pub fn is_connected(&self) -> bool {
        match &self.inner {
            NativeMoqRouteClientInner::Draft07(client) => client.is_connected(),
            NativeMoqRouteClientInner::Draft14(client) => client.is_connected(),
        }
    }

    pub fn send_route(&self, route: RouteEnvelope) -> Result<()> {
        match &self.inner {
            NativeMoqRouteClientInner::Draft07(client) => client.send_route(route),
            NativeMoqRouteClientInner::Draft14(client) => client.send_route(route),
        }
    }

    pub async fn recv_route(&self) -> Result<RouteEnvelope> {
        match &self.inner {
            NativeMoqRouteClientInner::Draft07(client) => client.recv_route().await,
            NativeMoqRouteClientInner::Draft14(client) => client.recv_route().await,
        }
    }

    pub fn try_recv_route(&self) -> Option<RouteEnvelope> {
        match &self.inner {
            NativeMoqRouteClientInner::Draft07(client) => client.try_recv_route(),
            NativeMoqRouteClientInner::Draft14(client) => client.try_recv_route(),
        }
    }

    pub fn shutdown(&self) {
        match &self.inner {
            NativeMoqRouteClientInner::Draft07(client) => client.shutdown(),
            NativeMoqRouteClientInner::Draft14(client) => client.shutdown(),
        }
    }

    pub async fn close(&mut self) {
        match &mut self.inner {
            NativeMoqRouteClientInner::Draft07(client) => client.close().await,
            NativeMoqRouteClientInner::Draft14(client) => client.close().await,
        }
    }
}
