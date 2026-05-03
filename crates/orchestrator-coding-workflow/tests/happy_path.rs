//! Linear single-task happy path through the reducer:
//! ingest → triage → plan → ensure_branch → code → commit → review →
//! security → open PR → await → merged.
//!
//! Pure-function tests: feed each event into the reducer, assert the
//! resulting state and emitted actions. No dispatcher, no real GitHub.

use orchestrator_coding_workflow::*;
use orchestrator_core::{
    ActionId, Causation, EventEnvelope, EventId, Reducer, WorkflowId,
};
use orchestrator_github::RepoRef;
use serde_json::{json, Value};

fn workflow_id() -> WorkflowId {
    WorkflowId::new("wf-test")
}

fn make_envelope(seq: u64, payload_type: &str, payload: Value) -> EventEnvelope {
    EventEnvelope {
        event_id: EventId::new(),
        workflow_id: workflow_id(),
        sequence: seq,
        recorded_at: chrono::Utc::now(),
        payload_type: payload_type.into(),
        payload_schema_version: 1,
        causation: Causation::External {
            source: "test".into(),
            request_id: format!("r-{}", seq),
        },
        trace_id: None,
        payload,
    }
}

fn ticket_ingested_payload() -> Value {
    json!({
        "ticket": { "source": "manual", "id": "ENG-123" },
        "repo": { "owner": "octo", "name": "world" },
        "base_branch": "main",
        "base_sha": "0123456789abcdef0123456789abcdef01234567",
        "cost_budget_cents": 100_000_u64,
    })
}

#[test]
fn ticket_ingested_transitions_to_triaging_and_emits_triage_action() {
    let r = WorkflowReducer;
    let state = WorkflowState::default();
    assert_eq!(state.status, WorkflowStatus::Empty);

    let ev = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    let new_state = r.reduce(state, &ev).unwrap();
    assert_eq!(new_state.status, WorkflowStatus::Triaging);
    assert_eq!(new_state.ticket.as_ref().unwrap().id, "ENG-123");
    assert_eq!(new_state.cost_budget_cents, Some(100_000));
    assert_eq!(new_state.pending_action_ids.len(), 1);

    let actions = r.derive_actions(&new_state, &ev).unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].kind, KIND_AGENT_TRIAGE);
}

#[test]
fn budget_zero_at_ingestion_halts_immediately() {
    let r = WorkflowReducer;
    let state = WorkflowState::default();
    let mut p = ticket_ingested_payload();
    p["cost_budget_cents"] = json!(0_u64);

    let ev = make_envelope(0, EVT_TICKET_INGESTED, p);
    let new_state = r.reduce(state, &ev).unwrap();
    assert_eq!(new_state.status, WorkflowStatus::Failed);
    let actions = r.derive_actions(&new_state, &ev).unwrap();
    assert!(actions.is_empty());
}

#[test]
fn linear_happy_path_runs_to_merged() {
    let r = WorkflowReducer;
    let mut state = WorkflowState::default();

    // 1. Ticket ingested → Triaging.
    let ev0 = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    state = r.reduce(state, &ev0).unwrap();
    let actions = r.derive_actions(&state, &ev0).unwrap();
    let triage_action = &actions[0];
    let triage_id = ActionId::derive(&workflow_id(), 0, 0, KIND_AGENT_TRIAGE);

    // 2. Triage completes (accepted) → Planning.
    let ev1 = make_envelope(
        1,
        EVT_TRIAGE_COMPLETED,
        json!({
            "action_id": triage_id.0,
            "accepted": true,
        }),
    );
    state = r.reduce(state, &ev1).unwrap();
    assert_eq!(state.status, WorkflowStatus::Planning);
    let actions = r.derive_actions(&state, &ev1).unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].kind, KIND_AGENT_PLANNER);

    let planner_id = ActionId::derive(&workflow_id(), 1, 0, KIND_AGENT_PLANNER);

    // 3. Plan proposed (1 task) → EnsuringBranch.
    let ev2 = make_envelope(
        2,
        EVT_PLAN_PROPOSED,
        json!({
            "action_id": planner_id.0,
            "tasks": [{
                "description": "Add feature foo",
                "files_in_scope": ["src/foo.rs"],
            }],
        }),
    );
    state = r.reduce(state, &ev2).unwrap();
    assert_eq!(state.status, WorkflowStatus::EnsuringBranch);
    assert!(state.plan.is_some());
    let actions = r.derive_actions(&state, &ev2).unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].kind, KIND_ENSURE_BRANCH);
    let branch_payload: Value = actions[0].payload.clone();
    let branch_name = branch_payload["branch_name"].as_str().unwrap();
    assert!(branch_name.starts_with("auto/eng-123/"));

    let ensure_branch_id =
        ActionId::derive(&workflow_id(), 2, 0, KIND_ENSURE_BRANCH);

    // 4. Branch ensured → Coding.
    let ev3 = make_envelope(
        3,
        orchestrator_github::EVT_BRANCH_ENSURED,
        json!({
            "action_id": ensure_branch_id.0,
            "branch_name": branch_name,
            "head_sha": "0123456789abcdef0123456789abcdef01234567",
        }),
    );
    state = r.reduce(state, &ev3).unwrap();
    assert_eq!(state.status, WorkflowStatus::Coding);
    assert_eq!(state.branch_name.as_deref(), Some(branch_name));
    let actions = r.derive_actions(&state, &ev3).unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].kind, KIND_AGENT_CODER);

    let coder_id = ActionId::derive(&workflow_id(), 3, 0, KIND_AGENT_CODER);

    // 5. Coder output → PushingCommit.
    let ev4 = make_envelope(
        4,
        EVT_CODER_OUTPUT,
        json!({
            "action_id": coder_id.0,
            "task_idx": 0,
            "patch": {
                "files": [{
                    "path": "src/foo.rs",
                    "content": "fn foo() {}\n",
                }],
            },
            "notes": "Added foo function",
        }),
    );
    state = r.reduce(state, &ev4).unwrap();
    assert_eq!(state.status, WorkflowStatus::PushingCommit);
    let actions = r.derive_actions(&state, &ev4).unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].kind, KIND_COMMIT_PATCH);
    let cp_payload: Value = actions[0].payload.clone();
    assert_eq!(
        cp_payload["files"][0]["path"].as_str(),
        Some("src/foo.rs")
    );

    let commit_patch_id = ActionId::derive(&workflow_id(), 4, 0, KIND_COMMIT_PATCH);

    // 6. Commit pushed → Reviewing.
    let ev5 = make_envelope(
        5,
        orchestrator_github::EVT_COMMIT_PUSHED,
        json!({
            "action_id": commit_patch_id.0,
            "commit_sha": "abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "head_sha_at_probe": "abcdefabcdefabcdefabcdefabcdefabcdefabcd",
            "is_at_head": true,
        }),
    );
    state = r.reduce(state, &ev5).unwrap();
    assert_eq!(state.status, WorkflowStatus::Reviewing);
    let actions = r.derive_actions(&state, &ev5).unwrap();
    assert_eq!(actions.len(), 1);
    assert_eq!(actions[0].kind, KIND_AGENT_REVIEWER);

    let reviewer_id = ActionId::derive(&workflow_id(), 5, 0, KIND_AGENT_REVIEWER);

    // 7. Reviewer passes → SecurityReviewing.
    let ev6 = make_envelope(
        6,
        EVT_REVIEWER_OUTPUT,
        json!({
            "action_id": reviewer_id.0,
            "passed": true,
        }),
    );
    state = r.reduce(state, &ev6).unwrap();
    assert_eq!(state.status, WorkflowStatus::SecurityReviewing);
    let actions = r.derive_actions(&state, &ev6).unwrap();
    assert_eq!(actions[0].kind, KIND_AGENT_SECURITY_REVIEWER);

    let sec_id = ActionId::derive(&workflow_id(), 6, 0, KIND_AGENT_SECURITY_REVIEWER);

    // 8. Security review passes (no findings) → OpeningPr.
    let ev7 = make_envelope(
        7,
        EVT_SECURITY_REVIEWER_OUTPUT,
        json!({
            "action_id": sec_id.0,
            "passed": true,
            "findings": [],
        }),
    );
    state = r.reduce(state, &ev7).unwrap();
    assert_eq!(state.status, WorkflowStatus::OpeningPr);
    let actions = r.derive_actions(&state, &ev7).unwrap();
    assert_eq!(actions[0].kind, KIND_OPEN_PR);

    let open_pr_id = ActionId::derive(&workflow_id(), 7, 0, KIND_OPEN_PR);

    // 9. PR opened → AwaitingHumanApproval.
    let ev8 = make_envelope(
        8,
        orchestrator_github::EVT_PR_OPENED,
        json!({
            "action_id": open_pr_id.0,
            "pr_number": 42_u64,
            "html_url": "https://github.com/octo/world/pull/42",
        }),
    );
    state = r.reduce(state, &ev8).unwrap();
    assert_eq!(state.status, WorkflowStatus::AwaitingHumanApproval);
    assert_eq!(state.pr_number, Some(42));
    let actions = r.derive_actions(&state, &ev8).unwrap();
    assert!(actions.is_empty());

    // 10. PR merged (from webhook) → Merged.
    let ev9 = make_envelope(
        9,
        EVT_PR_MERGED,
        json!({
            "repo": { "owner": "octo", "name": "world" },
            "pr_number": 42_u64,
            "merge_commit_sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        }),
    );
    state = r.reduce(state, &ev9).unwrap();
    assert_eq!(state.status, WorkflowStatus::Merged);
    assert_eq!(
        state.merge_commit_sha.as_deref(),
        Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
    );
    let actions = r.derive_actions(&state, &ev9).unwrap();
    assert!(actions.is_empty());

    assert!(state.pending_action_ids.is_empty(), "all pending should clear");
    assert!(state.failure.is_none());
    let _ = triage_action; // silence unused
}

#[test]
fn triage_rejection_halts_workflow() {
    let r = WorkflowReducer;
    let mut state = WorkflowState::default();
    let ev0 = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    state = r.reduce(state, &ev0).unwrap();
    let triage_id = ActionId::derive(&workflow_id(), 0, 0, KIND_AGENT_TRIAGE);

    let ev1 = make_envelope(
        1,
        EVT_TRIAGE_COMPLETED,
        json!({
            "action_id": triage_id.0,
            "accepted": false,
            "reason": "out of scope",
        }),
    );
    state = r.reduce(state, &ev1).unwrap();
    assert_eq!(state.status, WorkflowStatus::Failed);
    assert!(state
        .failure
        .as_ref()
        .unwrap()
        .reason
        .contains("out of scope"));
    let actions = r.derive_actions(&state, &ev1).unwrap();
    assert!(actions.is_empty());
}

#[test]
fn multi_task_plan_halts_in_v1() {
    let r = WorkflowReducer;
    let mut state = WorkflowState::default();
    let ev0 = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    state = r.reduce(state, &ev0).unwrap();

    let triage_id = ActionId::derive(&workflow_id(), 0, 0, KIND_AGENT_TRIAGE);
    let ev1 = make_envelope(
        1,
        EVT_TRIAGE_COMPLETED,
        json!({"action_id": triage_id.0, "accepted": true}),
    );
    state = r.reduce(state, &ev1).unwrap();

    let planner_id = ActionId::derive(&workflow_id(), 1, 0, KIND_AGENT_PLANNER);
    let ev2 = make_envelope(
        2,
        EVT_PLAN_PROPOSED,
        json!({
            "action_id": planner_id.0,
            "tasks": [
                { "description": "Task 1", "files_in_scope": [] },
                { "description": "Task 2", "files_in_scope": [] },
            ],
        }),
    );
    state = r.reduce(state, &ev2).unwrap();
    assert_eq!(state.status, WorkflowStatus::Failed);
    assert!(state
        .failure
        .as_ref()
        .unwrap()
        .reason
        .contains("single-task"));
}

#[test]
fn reviewer_rejection_halts_workflow() {
    let r = WorkflowReducer;
    let mut state = drive_to_reviewing(&r);

    let reviewer_id = ActionId::derive(&workflow_id(), 5, 0, KIND_AGENT_REVIEWER);
    let ev = make_envelope(
        6,
        EVT_REVIEWER_OUTPUT,
        json!({
            "action_id": reviewer_id.0,
            "passed": false,
            "feedback": "needs more tests",
        }),
    );
    state = r.reduce(state, &ev).unwrap();
    assert_eq!(state.status, WorkflowStatus::Failed);
    assert!(state.failure.unwrap().reason.contains("needs more tests"));
}

#[test]
fn high_severity_security_finding_halts_workflow() {
    let r = WorkflowReducer;
    let mut state = drive_to_security_reviewing(&r);
    let sec_id = ActionId::derive(&workflow_id(), 6, 0, KIND_AGENT_SECURITY_REVIEWER);
    let ev = make_envelope(
        7,
        EVT_SECURITY_REVIEWER_OUTPUT,
        json!({
            "action_id": sec_id.0,
            "passed": true,
            "findings": [{
                "severity": "critical",
                "message": "SQL injection",
            }],
        }),
    );
    state = r.reduce(state, &ev).unwrap();
    assert_eq!(state.status, WorkflowStatus::Failed);
    assert!(state.failure.unwrap().reason.contains("security"));
}

#[test]
fn budget_exceeded_via_consumed_event_halts_workflow() {
    let r = WorkflowReducer;
    let mut state = WorkflowState::default();
    let ev0 = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    state = r.reduce(state, &ev0).unwrap();
    assert_eq!(state.status, WorkflowStatus::Triaging);

    // Consume exactly the cap.
    let triage_id = ActionId::derive(&workflow_id(), 0, 0, KIND_AGENT_TRIAGE);
    let ev1 = make_envelope(
        1,
        EVT_BUDGET_CONSUMED,
        json!({
            "action_id": triage_id.0,
            "cents": 100_000_u64,
            "category": "agent.triage",
        }),
    );
    state = r.reduce(state, &ev1).unwrap();
    assert_eq!(state.status, WorkflowStatus::Failed);
    assert_eq!(state.cost_consumed_cents, 100_000);
    let actions = r.derive_actions(&state, &ev1).unwrap();
    assert!(actions.is_empty());
}

#[test]
fn budget_under_cap_does_not_halt() {
    let r = WorkflowReducer;
    let mut state = WorkflowState::default();
    let ev0 = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    state = r.reduce(state, &ev0).unwrap();

    let triage_id = ActionId::derive(&workflow_id(), 0, 0, KIND_AGENT_TRIAGE);
    let ev1 = make_envelope(
        1,
        EVT_BUDGET_CONSUMED,
        json!({
            "action_id": triage_id.0,
            "cents": 50_000_u64,
            "category": "agent.triage",
        }),
    );
    state = r.reduce(state, &ev1).unwrap();
    assert_eq!(state.status, WorkflowStatus::Triaging);
    assert_eq!(state.cost_consumed_cents, 50_000);
}

#[test]
fn action_failed_event_for_pending_action_halts() {
    let r = WorkflowReducer;
    let mut state = WorkflowState::default();
    let ev0 = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    state = r.reduce(state, &ev0).unwrap();
    assert_eq!(state.status, WorkflowStatus::Triaging);

    let triage_id = ActionId::derive(&workflow_id(), 0, 0, KIND_AGENT_TRIAGE);

    // Synthesize an action.failed.v1 event.
    let failure_payload = json!({
        "action_id": triage_id.0,
        "kind": KIND_AGENT_TRIAGE,
        "original_payload": null,
        "payload_truncated": false,
        "final_state": "failed",
        "last_error": "agent service unreachable",
        "attempts": 5,
        "probe_attempts": 0,
    });
    let ev1 = make_envelope(
        1,
        orchestrator_core::EVT_ACTION_FAILED,
        failure_payload,
    );
    state = r.reduce(state, &ev1).unwrap();
    assert_eq!(state.status, WorkflowStatus::Failed);
    let f = state.failure.unwrap();
    assert!(f.reason.contains(KIND_AGENT_TRIAGE));
    assert_eq!(f.last_error.as_deref(), Some("agent service unreachable"));
}

#[test]
fn action_failed_for_unknown_action_id_is_ignored() {
    let r = WorkflowReducer;
    let mut state = WorkflowState::default();
    let ev0 = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    state = r.reduce(state, &ev0).unwrap();

    // Failure for an action_id NOT in pending_action_ids.
    let unknown_id = ActionId::derive(&workflow_id(), 99, 99, "some.other.kind");
    let ev1 = make_envelope(
        1,
        orchestrator_core::EVT_ACTION_FAILED,
        json!({
            "action_id": unknown_id.0,
            "kind": "some.other.kind",
            "original_payload": null,
            "payload_truncated": false,
            "final_state": "failed",
            "last_error": "nope",
            "attempts": 1,
            "probe_attempts": 0,
        }),
    );
    state = r.reduce(state, &ev1).unwrap();
    // Still in Triaging — failure was for an action we don't track.
    assert_eq!(state.status, WorkflowStatus::Triaging);
}

// ── helpers ────────────────────────────────────────────────────────────

fn drive_to_reviewing(r: &WorkflowReducer) -> WorkflowState {
    let mut state = WorkflowState::default();
    let ev0 = make_envelope(0, EVT_TICKET_INGESTED, ticket_ingested_payload());
    state = r.reduce(state, &ev0).unwrap();

    let triage_id = ActionId::derive(&workflow_id(), 0, 0, KIND_AGENT_TRIAGE);
    let ev1 = make_envelope(
        1,
        EVT_TRIAGE_COMPLETED,
        json!({"action_id": triage_id.0, "accepted": true}),
    );
    state = r.reduce(state, &ev1).unwrap();

    let planner_id = ActionId::derive(&workflow_id(), 1, 0, KIND_AGENT_PLANNER);
    let ev2 = make_envelope(
        2,
        EVT_PLAN_PROPOSED,
        json!({
            "action_id": planner_id.0,
            "tasks": [{ "description": "task", "files_in_scope": [] }],
        }),
    );
    state = r.reduce(state, &ev2).unwrap();

    let ensure_id = ActionId::derive(&workflow_id(), 2, 0, KIND_ENSURE_BRANCH);
    let ev3 = make_envelope(
        3,
        orchestrator_github::EVT_BRANCH_ENSURED,
        json!({
            "action_id": ensure_id.0,
            "branch_name": "auto/eng-123/x",
            "head_sha": "0".repeat(40),
        }),
    );
    state = r.reduce(state, &ev3).unwrap();

    let coder_id = ActionId::derive(&workflow_id(), 3, 0, KIND_AGENT_CODER);
    let ev4 = make_envelope(
        4,
        EVT_CODER_OUTPUT,
        json!({
            "action_id": coder_id.0,
            "task_idx": 0,
            "patch": { "files": [{ "path": "x", "content": "y" }] },
            "notes": "",
        }),
    );
    state = r.reduce(state, &ev4).unwrap();

    let cp_id = ActionId::derive(&workflow_id(), 4, 0, KIND_COMMIT_PATCH);
    let ev5 = make_envelope(
        5,
        orchestrator_github::EVT_COMMIT_PUSHED,
        json!({
            "action_id": cp_id.0,
            "commit_sha": "a".repeat(40),
            "head_sha_at_probe": "a".repeat(40),
            "is_at_head": true,
        }),
    );
    state = r.reduce(state, &ev5).unwrap();
    assert_eq!(state.status, WorkflowStatus::Reviewing);
    state
}

fn drive_to_security_reviewing(r: &WorkflowReducer) -> WorkflowState {
    let mut state = drive_to_reviewing(r);
    let reviewer_id = ActionId::derive(&workflow_id(), 5, 0, KIND_AGENT_REVIEWER);
    let ev = make_envelope(
        6,
        EVT_REVIEWER_OUTPUT,
        json!({"action_id": reviewer_id.0, "passed": true}),
    );
    state = r.reduce(state, &ev).unwrap();
    assert_eq!(state.status, WorkflowStatus::SecurityReviewing);
    state
}

#[allow(dead_code)]
fn _silence(_r: &RepoRef) {}
