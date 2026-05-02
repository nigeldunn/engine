//! Core ID types. All persisted IDs are strings to keep storage portable
//! and debugging painless.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A workflow instance ID. One per ticket.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowId(pub String);

impl WorkflowId {
    pub fn new(s: impl Into<String>) -> Self { Self(s.into()) }
    pub fn as_str(&self) -> &str { &self.0 }
    pub fn as_bytes(&self) -> &[u8] { self.0.as_bytes() }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

/// A single event's globally-unique ID. UUIDv7 so it's roughly time-ordered.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventId(pub String);

impl EventId {
    pub fn new() -> Self { Self(format!("evt_{}", uuid::Uuid::now_v7())) }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl Default for EventId {
    fn default() -> Self { Self::new() }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

/// Deterministic action ID derived from (workflow_id, sequence, action_index, kind).
/// Same inputs always produce the same ID. NOT a UUID.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ActionId(pub String);

impl ActionId {
    pub fn derive(
        workflow_id: &WorkflowId,
        sequence: u64,
        action_index: u32,
        action_kind: &str,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(workflow_id.as_bytes());
        hasher.update(&sequence.to_be_bytes());
        hasher.update(&action_index.to_be_bytes());
        hasher.update(action_kind.as_bytes());
        let hash = hasher.finalize();
        let encoded = base32::encode(
            base32::Alphabet::Rfc4648Lower { padding: false },
            &hash.as_bytes()[..16],
        );
        Self(format!("act_{}", encoded))
    }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl fmt::Display for ActionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}

/// Identifies a dispatcher process for lease ownership.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DispatcherId(pub String);

impl DispatcherId {
    pub fn new() -> Self {
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into());
        let pid = std::process::id();
        let boot = uuid::Uuid::now_v7().simple().to_string();
        let boot_short: String = boot.chars().take(8).collect();
        Self(format!("disp-{}-{}-{}", host, pid, boot_short))
    }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl Default for DispatcherId {
    fn default() -> Self { Self::new() }
}

impl fmt::Display for DispatcherId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.0) }
}