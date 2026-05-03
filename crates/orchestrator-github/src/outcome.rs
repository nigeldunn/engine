//! Outcome event types and constructors for the GitHub sink.
//!
//! Outcome events are written through `Executor::advance` after a successful
//! `Sink::execute`. The reducer pattern-matches on `payload_type` (e.g.
//! `"github.branch_ensured.v1"`) and decodes the typed payload from
//! `event.payload`.

use orchestrator_core::{ActionId, Causation, EventCommand, WorkflowId};
use serde::{Deserialize, Serialize};

use crate::action::{
    ClosePrPayload, CommitPatchPayload, EnsureBranchPayload, OpenPrPayload,
    SetPrStatusPayload, UpdatePrMetadataPayload, RepoRef,
};

pub const EVT_BRANCH_ENSURED: &str = "github.branch_ensured.v1";
pub const EVT_COMMIT_PUSHED: &str = "github.commit_pushed.v1";
pub const EVT_PR_OPENED: &str = "github.pr_opened.v1";
pub const EVT_PR_METADATA_UPDATED: &str = "github.pr_metadata_updated.v1";
pub const EVT_PR_STATUS_SET: &str = "github.pr_status_set.v1";
pub const EVT_PR_CLOSED: &str = "github.pr_closed.v1";

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

/// Outcome event payload for a successful `github.commit_patch`.
///
/// `is_at_head` distinguishes "we landed and the branch is still at our
/// commit" (true) from "we landed previously but other commits have since
/// landed on top" (false). Captured at the point where the outcome event
/// is constructed, so it reflects the truth at that moment.
///
/// `head_sha_at_probe` is the branch HEAD as observed during this attempt's
/// confirmation step (either `commit_sha` itself for a fresh execute, or the
/// real branch HEAD when we recovered via probe).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitPushed {
    pub repo: RepoRef,
    pub branch: String,
    pub commit_sha: String,
    pub parent_sha: String,
    pub action_id: ActionId,
    pub ticket_id: String,
    pub files_changed: Vec<String>,
    pub is_at_head: bool,
    pub head_sha_at_probe: String,
}

#[allow(clippy::too_many_arguments)]
pub fn commit_pushed_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    payload: &CommitPatchPayload,
    commit_sha: String,
    parent_sha: String,
    is_at_head: bool,
    head_sha_at_probe: String,
) -> EventCommand {
    let body = CommitPushed {
        repo: payload.repo.clone(),
        branch: payload.branch.clone(),
        commit_sha,
        parent_sha,
        action_id: action_id.clone(),
        ticket_id: payload.ticket_id.clone(),
        files_changed: payload.files.iter().map(|f| f.path.clone()).collect(),
        is_at_head,
        head_sha_at_probe,
    };
    EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: EVT_COMMIT_PUSHED.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body).expect("CommitPushed serializes infallibly"),
        causation: Causation::Action {
            action_id: action_id.clone(),
        },
        trace_id: None,
        ingress_dedup_key: None,
    }
}

/// Outcome event payload for a successful `github.open_pr`.
///
/// `state` reflects the PR's state at the moment we observed it — usually
/// `"open"` for a freshly-opened PR, but can be `"closed"` if the probe
/// path recovered an idempotent landing of a PR that was subsequently
/// closed by a human. Treat as observed-at-probe-time, not durable.
///
/// `already_existed` is true when probe (either the dispatcher's
/// `find_existing` path or our own 422-fallback inside execute) found a
/// PR carrying our marker, false when this attempt opened the PR fresh.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrOpened {
    pub repo: RepoRef,
    pub pr_number: u64,
    pub html_url: String,
    pub head_branch: String,
    pub base_branch: String,
    pub head_sha: String,
    pub base_sha: String,
    pub draft: bool,
    pub state: String,
    pub action_id: ActionId,
    pub ticket_id: String,
    pub already_existed: bool,
}

#[allow(clippy::too_many_arguments)]
pub fn pr_opened_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    payload: &OpenPrPayload,
    pr_number: u64,
    html_url: String,
    head_sha: String,
    base_sha: String,
    state: String,
    draft: bool,
    already_existed: bool,
) -> EventCommand {
    let body = PrOpened {
        repo: payload.repo.clone(),
        pr_number,
        html_url,
        head_branch: payload.head_branch.clone(),
        base_branch: payload.base_branch.clone(),
        head_sha,
        base_sha,
        draft,
        state,
        action_id: action_id.clone(),
        ticket_id: payload.ticket_id.clone(),
        already_existed,
    };
    EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: EVT_PR_OPENED.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body).expect("PrOpened serializes infallibly"),
        causation: Causation::Action {
            action_id: action_id.clone(),
        },
        trace_id: None,
        ingress_dedup_key: None,
    }
}

/// Outcome event for `github.update_pr_metadata`. Records the *applied*
/// title/body as observed in the PATCH response, not the requested values
/// — matches the existing convention (`open_pr`, `commit_patch`) of
/// recording what GitHub said happened.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrMetadataUpdated {
    pub repo: RepoRef,
    pub pr_number: u64,
    pub title: String,
    pub body: String,
    pub action_id: ActionId,
    pub ticket_id: String,
}

pub fn pr_metadata_updated_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    payload: &UpdatePrMetadataPayload,
    title: String,
    body: String,
) -> EventCommand {
    let body_payload = PrMetadataUpdated {
        repo: payload.repo.clone(),
        pr_number: payload.pr_number,
        title,
        body,
        action_id: action_id.clone(),
        ticket_id: payload.ticket_id.clone(),
    };
    EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: EVT_PR_METADATA_UPDATED.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body_payload)
            .expect("PrMetadataUpdated serializes infallibly"),
        causation: Causation::Action {
            action_id: action_id.clone(),
        },
        trace_id: None,
        ingress_dedup_key: None,
    }
}

/// Outcome event for `github.set_pr_status`. `requested_reviewers` is the
/// canonical list as observed in the response — additive semantics mean
/// it may include reviewers from prior calls, not just the ones this
/// action requested.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrStatusSet {
    pub repo: RepoRef,
    pub pr_number: u64,
    pub draft: bool,
    pub requested_reviewers: Vec<String>,
    pub action_id: ActionId,
    pub ticket_id: String,
}

pub fn pr_status_set_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    payload: &SetPrStatusPayload,
    draft: bool,
    requested_reviewers: Vec<String>,
) -> EventCommand {
    let body_payload = PrStatusSet {
        repo: payload.repo.clone(),
        pr_number: payload.pr_number,
        draft,
        requested_reviewers,
        action_id: action_id.clone(),
        ticket_id: payload.ticket_id.clone(),
    };
    EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: EVT_PR_STATUS_SET.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body_payload)
            .expect("PrStatusSet serializes infallibly"),
        causation: Causation::Action {
            action_id: action_id.clone(),
        },
        trace_id: None,
        ingress_dedup_key: None,
    }
}

/// Outcome event for `github.close_pr`. Records observed `state` from the
/// PATCH response — should be `"closed"` after a successful call.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrClosed {
    pub repo: RepoRef,
    pub pr_number: u64,
    pub state: String,
    pub action_id: ActionId,
    pub ticket_id: String,
}

pub fn pr_closed_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    payload: &ClosePrPayload,
    state: String,
) -> EventCommand {
    let body_payload = PrClosed {
        repo: payload.repo.clone(),
        pr_number: payload.pr_number,
        state,
        action_id: action_id.clone(),
        ticket_id: payload.ticket_id.clone(),
    };
    EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: EVT_PR_CLOSED.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body_payload).expect("PrClosed serializes infallibly"),
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

    // ── commit_pushed event ────────────────────────────────────────────

    use crate::action::{CommitPatchPayload, FileChange};

    fn sample_commit_payload() -> CommitPatchPayload {
        CommitPatchPayload {
            repo: RepoRef {
                owner: "octo".into(),
                name: "world".into(),
            },
            branch: "auto/eng-1/abc".into(),
            expected_parent_sha: "0123456789abcdef0123456789abcdef01234567".into(),
            commit_message: "fix the thing".into(),
            author: None,
            files: vec![
                FileChange {
                    path: "src/a.rs".into(),
                    mode: None,
                    content: Some("fn a() {}\n".into()),
                },
                FileChange {
                    path: "old.txt".into(),
                    mode: None,
                    content: None,
                },
            ],
            ticket_id: "ENG-1".into(),
        }
    }

    #[test]
    fn commit_pushed_event_captures_files_changed_and_flags() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_commit".into());
        let evt = commit_pushed_event(
            &wf,
            &aid,
            &sample_commit_payload(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into(),
            "0123456789abcdef0123456789abcdef01234567".into(),
            true,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into(),
        );
        assert_eq!(evt.payload_type, EVT_COMMIT_PUSHED);
        assert_eq!(evt.payload_schema_version, 1);
        let decoded: CommitPushed = serde_json::from_value(evt.payload).unwrap();
        assert_eq!(decoded.action_id, aid);
        assert_eq!(decoded.commit_sha, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
        assert_eq!(decoded.parent_sha, "0123456789abcdef0123456789abcdef01234567");
        assert!(decoded.is_at_head);
        assert_eq!(
            decoded.files_changed,
            vec!["src/a.rs".to_string(), "old.txt".to_string()]
        );
    }

    #[test]
    fn commit_pushed_event_with_buried_commit() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_buried".into());
        let evt = commit_pushed_event(
            &wf,
            &aid,
            &sample_commit_payload(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into(),
            "0123456789abcdef0123456789abcdef01234567".into(),
            false,
            "cafef00dcafef00dcafef00dcafef00dcafef00d".into(),
        );
        let decoded: CommitPushed = serde_json::from_value(evt.payload).unwrap();
        assert!(!decoded.is_at_head);
        assert_eq!(decoded.head_sha_at_probe, "cafef00dcafef00dcafef00dcafef00dcafef00d");
    }

    // ── pr_opened event ────────────────────────────────────────────────

    use crate::action::OpenPrPayload;

    fn sample_open_pr() -> OpenPrPayload {
        OpenPrPayload {
            repo: RepoRef {
                owner: "octo".into(),
                name: "world".into(),
            },
            head_branch: "auto/eng-1/abc".into(),
            base_branch: "main".into(),
            title: "[orch-test] eng-1".into(),
            body: "Closes ENG-1.".into(),
            draft: false,
            ticket_id: "ENG-1".into(),
        }
    }

    #[test]
    fn pr_opened_event_carries_all_fields() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_pr".into());
        let evt = pr_opened_event(
            &wf,
            &aid,
            &sample_open_pr(),
            42,
            "https://github.com/octo/world/pull/42".into(),
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".into(),
            "0123456789abcdef0123456789abcdef01234567".into(),
            "open".into(),
            false,
            false,
        );
        assert_eq!(evt.payload_type, EVT_PR_OPENED);
        assert_eq!(evt.payload_schema_version, 1);
        let decoded: PrOpened = serde_json::from_value(evt.payload).unwrap();
        assert_eq!(decoded.pr_number, 42);
        assert_eq!(decoded.html_url, "https://github.com/octo/world/pull/42");
        assert_eq!(decoded.head_branch, "auto/eng-1/abc");
        assert_eq!(decoded.base_branch, "main");
        assert_eq!(decoded.state, "open");
        assert!(!decoded.draft);
        assert!(!decoded.already_existed);
    }

    #[test]
    fn pr_opened_event_already_existed_flag_propagates() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_pr".into());
        let evt = pr_opened_event(
            &wf,
            &aid,
            &sample_open_pr(),
            42,
            "https://github.com/octo/world/pull/42".into(),
            "0".repeat(40),
            "1".repeat(40),
            "closed".into(),
            true,
            true,
        );
        let decoded: PrOpened = serde_json::from_value(evt.payload).unwrap();
        assert!(decoded.already_existed);
        assert_eq!(decoded.state, "closed");
        assert!(decoded.draft);
    }

    // ── PATCH-triple events ────────────────────────────────────────────

    use crate::action::{ClosePrPayload, SetPrStatusPayload, UpdatePrMetadataPayload};

    #[test]
    fn pr_metadata_updated_event_records_applied_state() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_meta".into());
        let payload = UpdatePrMetadataPayload {
            repo: RepoRef {
                owner: "octo".into(),
                name: "world".into(),
            },
            pr_number: 7,
            title: Some("requested title".into()),
            body: Some("requested body".into()),
            ticket_id: "ENG-1".into(),
        };
        let evt = pr_metadata_updated_event(
            &wf,
            &aid,
            &payload,
            "applied title".into(),
            "applied body".into(),
        );
        assert_eq!(evt.payload_type, EVT_PR_METADATA_UPDATED);
        let decoded: PrMetadataUpdated = serde_json::from_value(evt.payload).unwrap();
        assert_eq!(decoded.title, "applied title");
        assert_eq!(decoded.body, "applied body");
        assert_eq!(decoded.pr_number, 7);
    }

    #[test]
    fn pr_status_set_event_carries_draft_and_reviewers() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_status".into());
        let payload = SetPrStatusPayload {
            repo: RepoRef {
                owner: "octo".into(),
                name: "world".into(),
            },
            pr_number: 7,
            draft: Some(false),
            requested_reviewers: vec!["alice".into()],
            ticket_id: "ENG-1".into(),
        };
        let evt = pr_status_set_event(
            &wf,
            &aid,
            &payload,
            false,
            vec!["alice".into(), "bob".into()],
        );
        assert_eq!(evt.payload_type, EVT_PR_STATUS_SET);
        let decoded: PrStatusSet = serde_json::from_value(evt.payload).unwrap();
        assert!(!decoded.draft);
        assert_eq!(decoded.requested_reviewers, vec!["alice", "bob"]);
    }

    #[test]
    fn pr_closed_event_records_state() {
        let wf = WorkflowId::new("wf");
        let aid = ActionId("act_close".into());
        let payload = ClosePrPayload {
            repo: RepoRef {
                owner: "octo".into(),
                name: "world".into(),
            },
            pr_number: 7,
            ticket_id: "ENG-1".into(),
        };
        let evt = pr_closed_event(&wf, &aid, &payload, "closed".into());
        assert_eq!(evt.payload_type, EVT_PR_CLOSED);
        let decoded: PrClosed = serde_json::from_value(evt.payload).unwrap();
        assert_eq!(decoded.state, "closed");
        assert_eq!(decoded.pr_number, 7);
    }
}
