use crate::{PeerPresence, PullRequestKind, RemoteResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HookTransport {
    Relay,
    Mesh,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ConnectHookContext {
    pub peer: PeerPresence,
    pub transport: HookTransport,
    #[serde(default)]
    pub relay_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoomHookContext {
    pub peer_id: String,
    pub room: String,
    pub transport: HookTransport,
    #[serde(default)]
    pub peer: Option<PeerPresence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ServeRequestContext {
    pub peer_id: String,
    pub transport: HookTransport,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub watch_id: Option<String>,
    pub request: PullRequestKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ServeResultContext {
    pub peer_id: String,
    pub transport: HookTransport,
    #[serde(default)]
    pub request_id: Option<String>,
    #[serde(default)]
    pub watch_id: Option<String>,
    pub request: PullRequestKind,
    #[serde(default)]
    pub initial: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HookDecision<T> {
    Allow { value: T },
    Deny { message: String },
}

impl<T> HookDecision<T> {
    pub fn allow(value: T) -> Self {
        Self::Allow { value }
    }

    pub fn deny(message: impl Into<String>) -> Self {
        Self::Deny {
            message: message.into(),
        }
    }

    pub fn into_result(self) -> std::result::Result<T, String> {
        match self {
            Self::Allow { value } => Ok(value),
            Self::Deny { message } => Err(message),
        }
    }
}

pub trait NetworkHooks: Send + Sync {
    fn on_connect(&self, context: &ConnectHookContext) -> HookDecision<()> {
        let _ = context;
        HookDecision::allow(())
    }

    fn on_join_room(&self, context: &RoomHookContext) -> HookDecision<()> {
        let _ = context;
        HookDecision::allow(())
    }

    fn on_pull(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
        HookDecision::allow(context.request.clone())
    }

    fn on_watch(&self, context: &ServeRequestContext) -> HookDecision<PullRequestKind> {
        HookDecision::allow(context.request.clone())
    }

    fn on_serve_result(
        &self,
        _context: &ServeResultContext,
        result: RemoteResult,
    ) -> HookDecision<RemoteResult> {
        HookDecision::allow(result)
    }
}
