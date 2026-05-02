//! The event envelope that lives in the log, and the command shape callers
//! use to advance a workflow.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::ids::{ActionId, EventId, WorkflowId};

/// What caused this event - critical for debugging, replay, and tracing.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Causation {
    /// External trigger - webhook, API call, manual operator action.
    External { source: String, request_id: String },
    /// Result of a dispatched action (the common case).
    Action { action_id: ActionId },
    /// Triggered by a timer/timeout.
    Timer { timer_id: String },
    /// Triggered by a human operator.
    Human { user_id: String, action_id: Option<ActionId> },
    /// System-generated (cleanup, reaper, etc).
    System { reason: String },
}

impl Causation {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::External { .. } => "external",
            Self::Action { .. } => "action",
            Self::Timer { .. } => "timer",
            Self::Human { .. } => "human",
            Self::System { .. } => "system",
        }
    }
    pub fn ref_id(&self) -> Option<String> {
        match self {
            Self::External { request_id, .. } => Some(request_id.clone()),
            Self::Action { action_id } => Some(action_id.0.clone()),
            Self::Timer { timer_id } => Some(timer_id.clone()),
            Self::Human { action_id, .. } => action_id.as_ref().map(|a| a.0.clone()),
            Self::System { .. } => None,
        }
    }
}

/// The persisted event envelope. Payload is opaque JSON at the storage layer;
/// typed decoding happens at the reducer boundary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: EventId,
    pub workflow_id: WorkflowId,
    pub sequence: u64,
    pub recorded_at: DateTime<Utc>,
    pub payload_type: String,
    pub payload_schema_version: u32,
    pub causation: Causation,
    pub trace_id: Option<String>,
    pub payload: Json,
}

/// A command to advance a workflow. The executor assigns sequence, event_id,
/// and recorded_at inside the transaction so callers can't race.
#[derive(Clone, Debug)]
pub struct EventCommand {
    pub workflow_id: WorkflowId,
    pub payload_type: String,
    pub payload_schema_version: u32,
    pub payload: Json,
    pub causation: Causation,
    pub trace_id: Option<String>,
    /// Optional caller-supplied dedup key (e.g. webhook delivery ID).
    /// If a previous successful advance used the same key, this is a no-op
    /// and returns the prior outcome.
    pub ingress_dedup_key: Option<String>,
}

/// What the executor returns from a successful advance.
#[derive(Clone, Debug)]
pub struct AdvanceOutcome {
    pub event_id: EventId,
    pub sequence: u64,
    pub actions_enqueued: Vec<ActionId>,
    /// True if this was an idempotent replay of a prior command.
    pub deduplicated: bool,
}
