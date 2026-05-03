//! Outcome event types and constructors for the GitHub sink.
//!
//! Outcome events are written through `Executor::advance` after a successful
//! `Sink::execute`. The reducer pattern-matches on `payload_type` (e.g.
//! `"github.branch_ensured.v1"`) and decodes the typed payload from
//! `event.payload`.

use orchestrator_core::{ActionId, Causation, EventCommand, WorkflowId};
use serde::{Deserialize, Serialize};

use crate::action::{EnsureBranchPayload, RepoRef};

pub const EVT_BRANCH_ENSURED: &str = "github.branch_ensured.v1";

/// Outcome event payload for a successful `github.ensure_branch`.
///
/// `already_existed = true` means we observed an existing branch (either via
/// the dispatcher's `find_existing` probe path on a retry, or via the
/// `execute` 422-fallback probe). The branch is at `head_sha`, which equals
/// `base_sha` for a healthy outcome — if it doesn't, `find_existing` would
/// have returned `Err(...)` and we wouldn't be here.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BranchEnsured {
    pub repo: RepoRef,
    pub branch_name: String,
    pub head_sha: String,
    pub action_id: ActionId,
    pub ticket_id: String,
    pub already_existed: bool,
}

/// Build the `EventCommand` to advance the workflow with a `BranchEnsured`
/// outcome. Caller passes this to `Executor::advance` after the GitHub
/// side effect is confirmed.
pub fn branch_ensured_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    payload: &EnsureBranchPayload,
    head_sha: String,
    already_existed: bool,
) -> EventCommand {
    let body = BranchEnsured {
        repo: payload.repo.clone(),
        branch_name: payload.branch_name.clone(),
        head_sha,
        action_id: action_id.clone(),
        ticket_id: payload.ticket_id.clone(),
        already_existed,
    };
    EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: EVT_BRANCH_ENSURED.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body).expect("BranchEnsured serializes infallibly"),
        causation: Causation::Action {
            action_id: action_id.clone(),
        },
        trace_id: None,
        ingress_dedup_key: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_payload() -> EnsureBranchPayload {
        EnsureBranchPayload {
            repo: RepoRef {
                owner: "octo".into(),
                name: "world".into(),
            },
            base_branch: "main".into(),
            base_sha: "0123456789abcdef0123456789abcdef01234567".into(),
            branch_name: "auto/eng-1/abc".into(),
            ticket_id: "ENG-1".into(),
        }
    }

    #[test]
    fn event_carries_action_id_and_repo() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_test".into());
        let evt = branch_ensured_event(
            &wf,
            &aid,
            &sample_payload(),
            "0123456789abcdef0123456789abcdef01234567".into(),
            false,
        );
        assert_eq!(evt.payload_type, EVT_BRANCH_ENSURED);
        assert_eq!(evt.payload_schema_version, 1);
        let decoded: BranchEnsured = serde_json::from_value(evt.payload).unwrap();
        assert_eq!(decoded.action_id, aid);
        assert_eq!(decoded.repo.owner, "octo");
        assert!(!decoded.already_existed);
    }

    #[test]
    fn already_existed_flag_propagates() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_test".into());
        let evt = branch_ensured_event(
            &wf,
            &aid,
            &sample_payload(),
            "deadbeef0000000000000000000000000000beef".into(),
            true,
        );
        let decoded: BranchEnsured = serde_json::from_value(evt.payload).unwrap();
        assert!(decoded.already_existed);
        assert_eq!(decoded.head_sha, "deadbeef0000000000000000000000000000beef");
    }

    #[test]
    fn causation_points_to_action() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_xyz".into());
        let evt = branch_ensured_event(
            &wf,
            &aid,
            &sample_payload(),
            "0".repeat(40),
            false,
        );
        let serialized = serde_json::to_value(&evt.causation).unwrap();
        assert_eq!(serialized, json!({"kind": "action", "action_id": "act_xyz"}));
    }
}
