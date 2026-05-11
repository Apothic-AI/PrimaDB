use crate::SessionAuthConfig;
use serde::{Deserialize, Serialize};

fn default_retry_interval_ms() -> u64 {
    2_000
}

fn default_moq_route_track() -> String {
    "routes".to_owned()
}

fn default_moq_channel() -> String {
    "primadb-sync".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MeshSignalingMode {
    Relay,
    BroadcastChannel,
}

impl Default for MeshSignalingMode {
    fn default() -> Self {
        Self::Relay
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum IceServerUrls {
    One(String),
    Many(Vec<String>),
}

impl IceServerUrls {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            Self::One(url) => vec![url],
            Self::Many(urls) => urls,
        }
    }

    pub fn as_slice(&self) -> Vec<&str> {
        match self {
            Self::One(url) => vec![url.as_str()],
            Self::Many(urls) => urls.iter().map(String::as_str).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IceServerConfig {
    pub urls: IceServerUrls,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub credential: Option<String>,
}

impl IceServerConfig {
    pub fn default_stun_servers() -> Vec<Self> {
        Vec::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RelayClientConfig {
    pub url: String,
    #[serde(default = "default_retry_interval_ms")]
    pub retry_interval_ms: u64,
    #[serde(default, alias = "sessionAuth")]
    pub session_auth: SessionAuthConfig,
}

impl RelayClientConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            retry_interval_ms: default_retry_interval_ms(),
            session_auth: SessionAuthConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MoqDraft {
    Draft07,
    Draft14,
    DraftLatest,
}

impl Default for MoqDraft {
    fn default() -> Self {
        Self::DraftLatest
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MoqRelayClientConfig {
    pub url: String,
    pub path: String,
    #[serde(default = "default_moq_route_track")]
    pub track: String,
    #[serde(default = "default_moq_channel")]
    pub channel: String,
    #[serde(default)]
    pub subscribe: Vec<String>,
    #[serde(default)]
    pub draft: MoqDraft,
    #[serde(default = "default_retry_interval_ms")]
    pub retry_interval_ms: u64,
    #[serde(default, alias = "sessionAuth")]
    pub session_auth: SessionAuthConfig,
}

impl MoqRelayClientConfig {
    pub fn new(url: impl Into<String>, path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            url: url.into(),
            path: path.clone(),
            track: default_moq_route_track(),
            channel: default_moq_channel(),
            subscribe: vec![path],
            draft: MoqDraft::default(),
            retry_interval_ms: default_retry_interval_ms(),
            session_auth: SessionAuthConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RelayEndpointConfig {
    WebSocket(RelayClientConfig),
    Moq(MoqRelayClientConfig),
}

impl RelayEndpointConfig {
    pub fn websocket(url: impl Into<String>) -> Self {
        Self::WebSocket(RelayClientConfig::new(url))
    }

    pub fn moq(url: impl Into<String>, path: impl Into<String>) -> Self {
        Self::Moq(MoqRelayClientConfig::new(url, path))
    }

    pub fn session_auth(&self) -> &SessionAuthConfig {
        match self {
            Self::WebSocket(config) => &config.session_auth,
            Self::Moq(config) => &config.session_auth,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RelayServerConfig {
    pub bind: String,
}

impl RelayServerConfig {
    pub fn new(bind: impl Into<String>) -> Self {
        Self { bind: bind.into() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MeshConfig {
    pub room: String,
    #[serde(default)]
    pub signaling: MeshSignalingMode,
    #[serde(default)]
    pub relay_url: Option<String>,
    #[serde(default, alias = "relayEndpoint")]
    pub relay_endpoint: Option<RelayEndpointConfig>,
    #[serde(default = "default_retry_interval_ms")]
    pub retry_interval_ms: u64,
    #[serde(default, alias = "iceServers")]
    pub ice_servers: Vec<IceServerConfig>,
    #[serde(default, alias = "sessionAuth")]
    pub session_auth: SessionAuthConfig,
}

impl MeshConfig {
    pub fn relay(room: impl Into<String>, relay_url: impl Into<String>) -> Self {
        Self {
            room: room.into(),
            signaling: MeshSignalingMode::Relay,
            relay_url: Some(relay_url.into()),
            relay_endpoint: None,
            retry_interval_ms: default_retry_interval_ms(),
            ice_servers: IceServerConfig::default_stun_servers(),
            session_auth: SessionAuthConfig::default(),
        }
    }

    pub fn broadcast(room: impl Into<String>) -> Self {
        Self {
            room: room.into(),
            signaling: MeshSignalingMode::BroadcastChannel,
            relay_url: None,
            relay_endpoint: None,
            retry_interval_ms: default_retry_interval_ms(),
            ice_servers: IceServerConfig::default_stun_servers(),
            session_auth: SessionAuthConfig::default(),
        }
    }

    pub fn effective_ice_servers(&self) -> Vec<IceServerConfig> {
        self.ice_servers.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MeshSignal {
    Join {
        room: String,
        from: String,
    },
    Offer {
        room: String,
        from: String,
        to: String,
        sdp: String,
    },
    Answer {
        room: String,
        from: String,
        to: String,
        sdp: String,
    },
    Ice {
        room: String,
        from: String,
        to: String,
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    },
    Leave {
        room: String,
        from: String,
    },
}
