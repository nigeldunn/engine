//! Actions are the side-effect intentions produced by reducers. They live
//! in the outbox table until a dispatcher claims and executes them.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::ids::{ActionId, DispatcherId, EventId, WorkflowId};

/// What a reducer produces. The executor turns this into an outbox row.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Action {
    pub kind: String,
    pub payload: Json,
    /// How long to wait before first attempt. Usually zero.
    #[serde(default)]
    pub delay_seconds: u64,
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
}

fn default_max_attempts() -> u32 { 5 }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionState {
    /// Created with the source event, not yet picked up.
    Pending,
    /// Dispatcher has claimed it under a lease.
    InProgress,
    /// External effect confirmed and outcome event written.
    Succeeded,
    /// Permanent failure after exhausted retries.
    Failed,
    /// Probe attempts exhausted; we could not determine whether the side
    /// effect happened. Operationally distinct from `Failed`.
    FailedProbeExhausted,
    /// Cancelled by orchestrator (e.g. workflow aborted).
    Cancelled,
}

impl ActionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InProgress => "in_progress",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::FailedProbeExhausted => "failed_probe_exhausted",
            Self::Cancelled => "cancelled",
        }
    }
    #[allow(clippy::should_implement_trait)] // intentional: returns Option, per CLAUDE.md convention
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "in_progress" => Some(Self::InProgress),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "failed_probe_exhausted" => Some(Self::FailedProbeExhausted),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// A row from actions_outbox after it's been claimed by a dispatcher.
#[derive(Clone, Debug)]
pub struct ClaimedAction {
    pub action_id: ActionId,
    pub workflow_id: WorkflowId,
    pub source_sequence: u64,
    pub kind: String,
    pub payload: Json,
    pub attempt: u32,
    pub max_attempts: u32,
    pub probe_attempt: u32,
    pub max_probe_attempts: u32,
    pub claimed_by: DispatcherId,
    pub lease_expires_at: DateTime<Utc>,
}

/// Result of a dispatch attempt.
#[derive(Clone, Debug)]
pub enum AttemptOutcome {
    /// External effect confirmed. Includes optional external reference (e.g. PR URL).
    Succeeded {
        external_ref: Option<String>,
        /// The outcome event command (caller will pass to advance).
        outcome_event: crate::event::EventCommand,
    },
    /// Transient failure - retry with backoff.
    TransientFail { error: String },
    /// Permanent failure - don't retry.
    PermanentFail { error: String },
    /// The sink itself is unhealthy (auth failed, repo inaccessible, etc).
    /// The action returns to `Pending` without incrementing `attempt`, and
    /// the dispatcher persists the unhealthy state so subsequent claims
    /// for this sink are filtered out until recovery.
    SinkUnhealthy {
        reason: crate::health::SinkUnhealthyReason,
        detail: String,
    },
}

/// Failure reason returned by the executor when it can't finalize a result.
#[derive(Clone, Debug)]
pub struct FinalizeError {
    pub action_id: ActionId,
    pub message: String,
}

/// Tracking type for action outcome events linked back to outbox rows.
#[derive(Clone, Debug)]
pub struct OutboxOutcomeUpdate {
    pub action_id: ActionId,
    pub state: ActionState,
    pub external_ref: Option<String>,
    pub outcome_event_id: Option<EventId>,
    pub last_error: Option<String>,
}
