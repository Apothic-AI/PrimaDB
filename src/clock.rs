use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Revision {
    pub millis: u64,
    pub counter: u32,
    pub actor: String,
}

impl Ord for Revision {
    fn cmp(&self, other: &Self) -> Ordering {
        self.millis
            .cmp(&other.millis)
            .then_with(|| self.counter.cmp(&other.counter))
            .then_with(|| self.actor.cmp(&other.actor))
    }
}

impl PartialOrd for Revision {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionMarker {
    pub revision: Revision,
    pub op_id: String,
}

impl Ord for VersionMarker {
    fn cmp(&self, other: &Self) -> Ordering {
        self.revision
            .cmp(&other.revision)
            .then_with(|| self.op_id.cmp(&other.op_id))
    }
}

impl PartialOrd for VersionMarker {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HybridClock {
    actor: String,
    last_millis: u64,
    counter: u32,
    sequence: u64,
}

impl HybridClock {
    pub fn with_actor(actor: impl Into<String>) -> Self {
        Self {
            actor: actor.into(),
            last_millis: now_millis(),
            counter: 0,
            sequence: 0,
        }
    }

    pub fn actor(&self) -> &str {
        &self.actor
    }

    pub fn next_revision(&mut self) -> Revision {
        let now = now_millis();
        if now > self.last_millis {
            self.last_millis = now;
            self.counter = 0;
        } else {
            self.counter = self.counter.saturating_add(1);
        }

        Revision {
            millis: self.last_millis,
            counter: self.counter,
            actor: self.actor.clone(),
        }
    }

    pub fn observe(&mut self, revision: &Revision) {
        if revision.millis > self.last_millis {
            self.last_millis = revision.millis;
            self.counter = revision.counter;
        } else if revision.millis == self.last_millis && revision.counter > self.counter {
            self.counter = revision.counter;
        }
    }

    pub fn next_op_id(&mut self, namespace: &str) -> String {
        self.sequence = self.sequence.saturating_add(1);
        format!(
            "{}/{}/{:x}-{:x}-{:x}",
            sanitize_component(&self.actor),
            sanitize_component(namespace),
            self.last_millis,
            self.counter,
            self.sequence
        )
    }

    pub fn next_node_id(&mut self, hint: &str) -> String {
        self.sequence = self.sequence.saturating_add(1);
        format!(
            "{}/{}/{:x}",
            sanitize_component(&self.actor),
            sanitize_component(hint),
            self.sequence
        )
    }

    pub fn default_actor() -> String {
        format!("replica-{:x}", now_millis())
    }
}

impl Default for HybridClock {
    fn default() -> Self {
        Self::with_actor(Self::default_actor())
    }
}

pub(crate) fn now_millis() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        js_sys::Date::now() as u64
    }

    #[cfg(not(target_arch = "wasm32"))]
    {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
    }
}

fn sanitize_component(input: &str) -> String {
    let sanitized: String = input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
                ch
            } else {
                '-'
            }
        })
        .collect();

    if sanitized.is_empty() {
        "node".to_owned()
    } else {
        sanitized
    }
}
