use crate::{PeerPresence, PullRequestKind, RemoteResult, VerifiedIdentity};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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
    #[serde(default)]
    pub verified_identity: Option<VerifiedIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoomHookContext {
    pub peer_id: String,
    pub room: String,
    pub transport: HookTransport,
    #[serde(default)]
    pub peer: Option<PeerPresence>,
    #[serde(default)]
    pub verified_identity: Option<VerifiedIdentity>,
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
    #[serde(default)]
    pub verified_identity: Option<VerifiedIdentity>,
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
    #[serde(default)]
    pub verified_identity: Option<VerifiedIdentity>,
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

pub fn parse_void_hook_json(
    response: Option<JsonValue>,
    default_message: &str,
) -> HookDecision<()> {
    match response {
        None | Some(JsonValue::Null) => HookDecision::allow(()),
        Some(JsonValue::Bool(allow)) => {
            if allow {
                HookDecision::allow(())
            } else {
                HookDecision::deny(default_message)
            }
        }
        Some(JsonValue::String(message)) => HookDecision::deny(message),
        Some(value @ JsonValue::Object(_)) => {
            if let Some(wrapper) = hook_wrapper_from_json(&value) {
                if !wrapper.allow {
                    return HookDecision::deny(
                        wrapper
                            .message
                            .unwrap_or_else(|| default_message.to_owned()),
                    );
                }
                HookDecision::allow(())
            } else {
                HookDecision::deny(
                    "invalid network hook return value: expected boolean, string, or decision object",
                )
            }
        }
        Some(_) => HookDecision::deny(
            "invalid network hook return value: expected boolean, string, or decision object",
        ),
    }
}

pub fn parse_request_hook_json(
    response: Option<JsonValue>,
    default_request: &PullRequestKind,
    default_message: &str,
) -> HookDecision<PullRequestKind> {
    match response {
        None | Some(JsonValue::Null) => HookDecision::allow(default_request.clone()),
        Some(JsonValue::Bool(allow)) => {
            if allow {
                HookDecision::allow(default_request.clone())
            } else {
                HookDecision::deny(default_message)
            }
        }
        Some(JsonValue::String(message)) => HookDecision::deny(message),
        Some(value @ JsonValue::Object(_)) => {
            if let Some(wrapper) = hook_wrapper_from_json(&value) {
                if !wrapper.allow {
                    return HookDecision::deny(
                        wrapper
                            .message
                            .unwrap_or_else(|| default_message.to_owned()),
                    );
                }
                if let Some(request) = wrapper.request {
                    return match serde_json::from_value::<PullRequestKind>(request) {
                        Ok(request) => HookDecision::allow(request),
                        Err(error) => HookDecision::deny(error.to_string()),
                    };
                }
                return HookDecision::allow(default_request.clone());
            }
            match serde_json::from_value::<PullRequestKind>(value) {
                Ok(request) => HookDecision::allow(request),
                Err(error) => HookDecision::deny(error.to_string()),
            }
        }
        Some(value) => match serde_json::from_value::<PullRequestKind>(value) {
            Ok(request) => HookDecision::allow(request),
            Err(error) => HookDecision::deny(error.to_string()),
        },
    }
}

pub fn parse_result_hook_json(
    response: Option<JsonValue>,
    default_result: RemoteResult,
    default_message: &str,
) -> HookDecision<RemoteResult> {
    match response {
        None | Some(JsonValue::Null) => HookDecision::allow(default_result),
        Some(JsonValue::Bool(allow)) => {
            if allow {
                HookDecision::allow(default_result)
            } else {
                HookDecision::deny(default_message)
            }
        }
        Some(JsonValue::String(message)) => HookDecision::deny(message),
        Some(value @ JsonValue::Object(_)) => {
            if let Some(wrapper) = hook_wrapper_from_json(&value) {
                if !wrapper.allow {
                    return HookDecision::deny(
                        wrapper
                            .message
                            .unwrap_or_else(|| default_message.to_owned()),
                    );
                }
                if let Some(result) = wrapper.result {
                    return match serde_json::from_value::<RemoteResult>(result) {
                        Ok(result) => HookDecision::allow(result),
                        Err(error) => HookDecision::deny(error.to_string()),
                    };
                }
                return HookDecision::allow(default_result);
            }
            match serde_json::from_value::<RemoteResult>(value) {
                Ok(result) => HookDecision::allow(result),
                Err(error) => HookDecision::deny(error.to_string()),
            }
        }
        Some(value) => match serde_json::from_value::<RemoteResult>(value) {
            Ok(result) => HookDecision::allow(result),
            Err(error) => HookDecision::deny(error.to_string()),
        },
    }
}

#[derive(Debug, Default)]
struct HookWrapper {
    allow: bool,
    message: Option<String>,
    request: Option<JsonValue>,
    result: Option<JsonValue>,
}

fn hook_wrapper_from_json(value: &JsonValue) -> Option<HookWrapper> {
    let object = value.as_object()?;
    let has_wrapper_fields = ["allow", "message", "request", "result"]
        .into_iter()
        .any(|key| object.contains_key(key));
    if !has_wrapper_fields {
        return None;
    }
    Some(HookWrapper {
        allow: object
            .get("allow")
            .and_then(|value| value.as_bool())
            .unwrap_or(true),
        message: object
            .get("message")
            .and_then(|value| value.as_str())
            .map(|value| value.to_owned()),
        request: object.get("request").cloned(),
        result: object.get("result").cloned(),
    })
}
