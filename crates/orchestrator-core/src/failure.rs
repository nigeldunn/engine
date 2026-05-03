//! Failure events written by the dispatcher when an action transitions to a
//! permanent terminal state. These let pure reducers (which can't observe
//! outbox state directly) react to action failures and produce compensating
//! events.
//!
//! Two terminal states emit failure events:
//! - `ActionState::Failed` → `core.action.failed.v1`. Caused by a sink
//!   returning `PermanentFail`, OR by transient retry budget exhaustion.
//! - `ActionState::FailedProbeExhausted` → `core.action.probe_exhausted.v1`.
//!   Caused by `Sink::find_existing` returning `Err` more times than
//!   `max_probe_attempts` allows.
//!
//! `Cancelled` and `SinkUnhealthy` do not emit failure events:
//! `Cancelled` is operator-driven (out of v1 scope); `SinkUnhealthy`
//! returns the action to `Pending` and is recoverable.
//!
//! ## Atomicity
//!
//! Each failure-event write is paired with a state-transition write. To
//! tolerate dispatcher crashes between the two, the dispatcher writes the
//! **event first** via `Executor::advance` with a deterministic
//! `ingress_dedup_key`; then performs the state transition. On crash and
//! reclaim, the action is re-attempted, fails again, the dedup'd event
//! write is a no-op, and the state transition completes. No event is ever
//! written twice; no permanent failure is silently swallowed.
//!
//! Distinct dedup-key prefixes for the two terminal states prevent
//! collision under the unique index on `events.ingress_dedup_key`:
//! - `action_failed:{action_id}`
//! - `probe_exhausted:{action_id}`

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::action::{ActionState, ClaimedAction};
use crate::event::{Causation, EventCommand};
use crate::ids::ActionId;

pub const EVT_ACTION_FAILED: &str = "core.action.failed.v1";
pub const EVT_ACTION_PROBE_EXHAUSTED: &str = "core.action.probe_exhausted.v1";

/// Cap on the embedded `original_payload`. Larger payloads (up to the
/// 5 MiB `commit_patch` ceiling) are dropped to `None` and a
/// `payload_truncated: true` marker is set so reducers can tell the
/// payload existed but isn't accessible directly.
pub const MAX_ORIGINAL_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionFailedPayload {
    pub action_id: ActionId,
    pub kind: String,
    /// The action's original payload, embedded for reducer convenience.
    /// `None` when the serialized payload exceeded `MAX_ORIGINAL_PAYLOAD_BYTES`;
    /// in that case `payload_truncated` is true.
    pub original_payload: Option<Json>,
    pub payload_truncated: bool,
    /// `Failed` for `EVT_ACTION_FAILED`; `FailedProbeExhausted` for
    /// `EVT_ACTION_PROBE_EXHAUSTED`. Carrying it explicitly saves the
    /// reducer from inferring it from the `payload_type`.
    pub final_state: ActionState,
    pub last_error: String,
    pub attempts: u32,
    pub probe_attempts: u32,
}

/// Build the `EventCommand` for a failed action. The dedup key prefix
/// distinguishes the two terminal states so they cannot collide on
/// `events.ingress_dedup_key`'s unique index.
pub fn build_failure_event_command(
    action: &ClaimedAction,
    final_state: ActionState,
    last_error: String,
    attempts: u32,
    probe_attempts: u32,
) -> EventCommand {
    let payload_type = match final_state {
        ActionState::Failed => EVT_ACTION_FAILED,
        ActionState::FailedProbeExhausted => EVT_ACTION_PROBE_EXHAUSTED,
        other => panic!(
            "build_failure_event_command called with non-terminal state {:?}",
            other
        ),
    };
    let dedup_prefix = match final_state {
        ActionState::Failed => "action_failed",
        ActionState::FailedProbeExhausted => "probe_exhausted",
        other => panic!("unreachable: {:?}", other),
    };

    let (original_payload, payload_truncated) = embed_or_truncate(&action.payload);

    let body = ActionFailedPayload {
        action_id: action.action_id.clone(),
        kind: action.kind.clone(),
        original_payload,
        payload_truncated,
        final_state,
        last_error,
        attempts,
        probe_attempts,
    };

    EventCommand {
        workflow_id: action.workflow_id.clone(),
        payload_type: payload_type.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body).expect("ActionFailedPayload serializes infallibly"),
        causation: Causation::Action {
            action_id: action.action_id.clone(),
        },
        trace_id: None,
        ingress_dedup_key: Some(format!("{}:{}", dedup_prefix, action.action_id.as_str())),
    }
}

fn embed_or_truncate(payload: &Json) -> (Option<Json>, bool) {
    let serialized = serde_json::to_vec(payload).unwrap_or_default();
    if serialized.len() <= MAX_ORIGINAL_PAYLOAD_BYTES {
        (Some(payload.clone()), false)
    } else {
        (None, true)
    }
}

/// Convenience accessor for reducer-side decoding.
pub fn decode_action_failed(event_payload: &Json) -> Option<ActionFailedPayload> {
    serde_json::from_value(event_payload.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{DispatcherId, WorkflowId};
    use chrono::Utc;
    use serde_json::json;

    fn sample_action(payload: Json) -> ClaimedAction {
        let workflow_id = WorkflowId::new("wf-1");
        let action_id = ActionId::derive(&workflow_id, 0, 0, "x.y.z");
        ClaimedAction {
            action_id,
            workflow_id,
            source_sequence: 0,
            kind: "x.y.z".into(),
            payload,
            attempt: 4,
            max_attempts: 5,
            probe_attempt: 0,
            max_probe_attempts: 20,
            claimed_by: DispatcherId::new(),
            lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
        }
    }

    #[test]
    fn failed_event_uses_action_failed_payload_type() {
        let action = sample_action(json!({"k": "v"}));
        let cmd = build_failure_event_command(
            &action,
            ActionState::Failed,
            "err".into(),
            5,
            0,
        );
        assert_eq!(cmd.payload_type, EVT_ACTION_FAILED);
        assert_eq!(
            cmd.ingress_dedup_key.as_deref(),
            Some(format!("action_failed:{}", action.action_id.as_str()).as_str())
        );
    }

    #[test]
    fn probe_exhausted_event_uses_distinct_dedup_prefix() {
        let action = sample_action(json!({"k": "v"}));
        let cmd = build_failure_event_command(
            &action,
            ActionState::FailedProbeExhausted,
            "probe err".into(),
            1,
            20,
        );
        assert_eq!(cmd.payload_type, EVT_ACTION_PROBE_EXHAUSTED);
        assert_eq!(
            cmd.ingress_dedup_key.as_deref(),
            Some(format!("probe_exhausted:{}", action.action_id.as_str()).as_str())
        );
    }

    #[test]
    fn small_payload_embeds_unchanged() {
        let payload = json!({"a": "b", "c": [1, 2, 3]});
        let action = sample_action(payload.clone());
        let cmd = build_failure_event_command(
            &action,
            ActionState::Failed,
            "err".into(),
            1,
            0,
        );
        let decoded: ActionFailedPayload =
            serde_json::from_value(cmd.payload).unwrap();
        assert_eq!(decoded.original_payload, Some(payload));
        assert!(!decoded.payload_truncated);
    }

    #[test]
    fn oversized_payload_is_truncated() {
        let big_string: String = "x".repeat(MAX_ORIGINAL_PAYLOAD_BYTES + 100);
        let payload = json!({"big": big_string});
        let action = sample_action(payload);
        let cmd = build_failure_event_command(
            &action,
            ActionState::Failed,
            "err".into(),
            1,
            0,
        );
        let decoded: ActionFailedPayload =
            serde_json::from_value(cmd.payload).unwrap();
        assert!(decoded.original_payload.is_none());
        assert!(decoded.payload_truncated);
    }

    #[test]
    fn causation_is_action_with_correct_id() {
        let action = sample_action(json!({}));
        let cmd = build_failure_event_command(
            &action,
            ActionState::Failed,
            "err".into(),
            1,
            0,
        );
        match cmd.causation {
            Causation::Action { action_id } => {
                assert_eq!(action_id, action.action_id);
            }
            other => panic!("expected Causation::Action, got {:?}", other),
        }
    }

    #[test]
    fn carries_attempts_and_error() {
        let action = sample_action(json!({}));
        let cmd = build_failure_event_command(
            &action,
            ActionState::Failed,
            "the underlying error message".into(),
            3,
            0,
        );
        let decoded: ActionFailedPayload =
            serde_json::from_value(cmd.payload).unwrap();
        assert_eq!(decoded.attempts, 3);
        assert_eq!(decoded.last_error, "the underlying error message");
        assert_eq!(decoded.kind, "x.y.z");
    }

    #[test]
    fn decode_action_failed_round_trips() {
        let action = sample_action(json!({"a": 1}));
        let cmd = build_failure_event_command(
            &action,
            ActionState::Failed,
            "err".into(),
            5,
            0,
        );
        let decoded = decode_action_failed(&cmd.payload).unwrap();
        assert_eq!(decoded.attempts, 5);
        assert_eq!(decoded.action_id, action.action_id);
    }

    #[test]
    #[should_panic]
    fn build_failure_event_panics_on_non_terminal_state() {
        let action = sample_action(json!({}));
        let _ = build_failure_event_command(
            &action,
            ActionState::Pending, // not terminal
            "err".into(),
            0,
            0,
        );
    }
}
