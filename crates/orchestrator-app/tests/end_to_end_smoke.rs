//! End-to-end smoke test of the v1 GitHub-driven coding workflow.
//!
//! Boots a real Storage + Executor + Dispatcher with the actual
//! `WorkflowReducer`, but swaps in an in-memory stub for the agent
//! service (no HTTP) and a stub Sink for github actions (no real
//! GitHub). Then ingests a ticket and waits for the workflow to
//! drive itself end-to-end:
//!
//! ingest → triage → plan → ensure_branch → code → commit → review →
//! security review → open PR → AwaitingHumanApproval → (synthesize
//! PrMerged) → Merged.
//!
//! Proves the cross-component wiring (dispatcher ↔ sinks ↔ reducer)
//! that the unit tests for each layer can't see in isolation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orchestrator_agent_runner::{
    AgentClient, AgentError, AgentRunResult, AgentRunStatus, AgentRunnerSink,
};
use orchestrator_app::ingest::{ingest_ticket, IngestRequest};
use orchestrator_coding_workflow::{
    events::{TicketIngested, TicketRef, EVT_PR_MERGED},
    state::{WorkflowState, WorkflowStatus},
    WorkflowReducer,
};
use orchestrator_core::{
    ActionId, AttemptOutcome, Causation, ClaimedAction, Dispatcher,
    DispatcherConfig as CoreDispatcherConfig, DispatcherError, EventCommand, Executor, Sink,
    WorkflowId,
};
use orchestrator_github::{
    branch_ensured_event, commit_pushed_event, decode_commit_patch, decode_ensure_branch,
    decode_open_pr, pr_opened_event, RepoRef, KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH,
    KIND_OPEN_PR,
};
use serde_json::json;

// ── stubs ──────────────────────────────────────────────────────────────

/// Canned-response AgentClient: every `run` call returns immediately
/// with a payload matching the workflow reducer's expected JSON shape.
/// `status` would only be called if `run` returned StillRunning; the
/// happy path never does, so it panics if exercised (signals a test
/// expectation mismatch).
struct StubAgentClient;

#[async_trait]
impl AgentClient for StubAgentClient {
    async fn run(
        &self,
        agent_type: &str,
        action_id: &ActionId,
        _payload: &serde_json::Value,
        _request_id: &str,
    ) -> Result<AgentRunResult, AgentError> {
        let output = match agent_type {
            "triage" => json!({ "action_id": action_id.0, "accepted": true }),
            "planner" => json!({
                "action_id": action_id.0,
                "tasks": [{
                    "description": "smoke-test single task",
                    "files_in_scope": ["src/lib.rs"],
                }],
            }),
            "coder" => json!({
                "action_id": action_id.0,
                "task_idx": 0,
                "patch": {
                    "files": [{ "path": "src/lib.rs", "content": "pub fn it_works() {}\n" }],
                },
                "notes": "smoke-test patch",
            }),
            "reviewer" => json!({ "action_id": action_id.0, "passed": true }),
            "security_reviewer" => {
                json!({ "action_id": action_id.0, "passed": true, "findings": [] })
            }
            other => panic!("unexpected agent_type in smoke test: {other}"),
        };
        Ok(AgentRunResult::Finished { output, cost_cents: Some(50) })
    }

    async fn status(
        &self,
        _agent_type: &str,
        _action_id: &ActionId,
    ) -> Result<AgentRunStatus, AgentError> {
        unreachable!("status should not be called: stub run() returns Finished synchronously")
    }

    async fn health(&self) -> Result<(), AgentError> {
        Ok(())
    }
}

/// Stub Sink for the three github action kinds the v1 workflow drives:
/// ensure_branch, commit_patch, open_pr. Each `execute` call decodes
/// the action payload via the public `decode_*` helpers and constructs
/// the matching outcome event via the public `*_event` helpers — no
/// fake field-by-field construction, so a schema change in
/// orchestrator-github breaks this test loudly.
struct StubGithubSink;

const STUB_HEAD_SHA: &str = "abcdef0123456789abcdef0123456789abcdef01";
const STUB_COMMIT_SHA: &str = "1234567890abcdef1234567890abcdef12345678";
const STUB_PR_NUMBER: u64 = 42;

#[async_trait]
impl Sink for StubGithubSink {
    fn handles(&self) -> &[&'static str] {
        &[KIND_ENSURE_BRANCH, KIND_COMMIT_PATCH, KIND_OPEN_PR]
    }

    fn sink_key(&self) -> &str {
        "github"
    }

    async fn execute(&self, action: &ClaimedAction) -> Result<AttemptOutcome, DispatcherError> {
        let outcome_event = match action.kind.as_str() {
            KIND_ENSURE_BRANCH => {
                let payload = decode_ensure_branch(&action.payload)
                    .map_err(|e| DispatcherError::Internal(e.to_string()))?;
                branch_ensured_event(
                    &action.workflow_id,
                    &action.action_id,
                    &payload,
                    STUB_HEAD_SHA.into(),
                    false,
                )
            }
            KIND_COMMIT_PATCH => {
                let payload = decode_commit_patch(&action.payload)
                    .map_err(|e| DispatcherError::Internal(e.to_string()))?;
                let parent_sha = payload.expected_parent_sha.clone();
                commit_pushed_event(
                    &action.workflow_id,
                    &action.action_id,
                    &payload,
                    STUB_COMMIT_SHA.into(),
                    parent_sha,
                    true,
                    STUB_COMMIT_SHA.into(),
                )
            }
            KIND_OPEN_PR => {
                let payload = decode_open_pr(&action.payload)
                    .map_err(|e| DispatcherError::Internal(e.to_string()))?;
                pr_opened_event(
                    &action.workflow_id,
                    &action.action_id,
                    &payload,
                    STUB_PR_NUMBER,
                    format!(
                        "https://example.test/{}/{}/pull/{STUB_PR_NUMBER}",
                        payload.repo.owner, payload.repo.name,
                    ),
                    STUB_COMMIT_SHA.into(),
                    STUB_HEAD_SHA.into(),
                    "open".into(),
                    payload.draft,
                    false,
                )
            }
            other => {
                return Err(DispatcherError::Internal(format!(
                    "stub github sink does not handle: {other}",
                )));
            }
        };
        Ok(AttemptOutcome::Succeeded {
            external_ref: None,
            outcome_event,
            side_events: vec![],
        })
    }
}

// ── helpers ────────────────────────────────────────────────────────────

/// Read the cached workflow state directly from the snapshots table.
/// Snapshots have no production read path (they're an internal cache),
/// but tests use them as a deterministic observable for reducer outcomes
/// (status, merge_commit_sha) that aren't visible from the event log
/// alone. The privileged read lives in `orchestrator_core::test_support`.
async fn read_workflow_state(
    executor: &Executor<WorkflowReducer>,
    workflow_id: &WorkflowId,
) -> Option<WorkflowState> {
    let value =
        orchestrator_core::test_support::read_snapshot_state(executor.storage(), workflow_id)
            .await?;
    Some(serde_json::from_value(value).expect("state schema match"))
}

/// Poll the event log until an event with `payload_type` appears for
/// `workflow_id`, or `deadline` elapses. Used as a synchronization
/// barrier between the test driver and the dispatcher's loop — the
/// arrival of a specific event-type is a stronger signal than any
/// in-memory state because it goes through the same Storage that the
/// reducer + dispatcher both observe.
async fn poll_until_event(
    executor: &Executor<WorkflowReducer>,
    workflow_id: &WorkflowId,
    payload_type: &str,
    deadline: Duration,
) {
    let started = std::time::Instant::now();
    loop {
        if let Ok(events) = executor.storage().read_events(workflow_id).await {
            if events.iter().any(|e| e.payload_type == payload_type) {
                return;
            }
            if started.elapsed() > deadline {
                let observed: Vec<_> =
                    events.iter().map(|e| e.payload_type.clone()).collect();
                panic!(
                    "workflow {} did not produce {} within {:?}; observed: {:?}",
                    workflow_id, payload_type, deadline, observed,
                );
            }
        } else if started.elapsed() > deadline {
            panic!("workflow {} never appeared within {:?}", workflow_id, deadline);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ── the test ───────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_path_drives_through_to_merged() {
    let _ = tracing_subscriber::fmt::try_init();

    let (storage, _db) = orchestrator_core::test_support::fresh_storage().await;
    let executor = Arc::new(Executor::new(storage, WorkflowReducer));

    let mut dispatcher = Dispatcher::new(
        executor.clone(),
        CoreDispatcherConfig {
            poll_interval: Duration::from_millis(20),
            health_check_interval: Duration::from_secs(60),
            ..Default::default()
        },
    );
    dispatcher.register(AgentRunnerSink::new(StubAgentClient));
    dispatcher.register(StubGithubSink);

    let shutdown = dispatcher.shutdown_handle();
    let dispatcher_join = tokio::spawn(dispatcher.run());

    let workflow_id = WorkflowId::new("manual:SMOKE-1");

    // Ingest the ticket. From here the workflow should drive itself.
    ingest_ticket(
        &executor,
        IngestRequest {
            workflow_id: None,
            ticket_ingested: TicketIngested {
                ticket: TicketRef {
                    source: "manual".into(),
                    id: "SMOKE-1".into(),
                },
                repo: RepoRef {
                    owner: "octo".into(),
                    name: "world".into(),
                },
                base_branch: "main".into(),
                base_sha: STUB_HEAD_SHA.into(),
                cost_budget_cents: Some(1_000_000),
                require_architecture_review: false,
            },
        },
    )
    .await
    .expect("ingest");

    // Drive the loop: triage → plan → ensure_branch → code → commit →
    // review → security → open_pr. The presence of `github.pr_opened.v1`
    // in the log is the strongest "we made it to AwaitingHumanApproval"
    // signal — it's the last event the dispatcher writes before the
    // workflow halts waiting for the merge webhook.
    poll_until_event(
        &executor,
        &workflow_id,
        "github.pr_opened.v1",
        Duration::from_secs(15),
    )
    .await;

    // Synthesize the merge webhook by appending PrMerged directly.
    executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: EVT_PR_MERGED.into(),
            payload_schema_version: 1,
            payload: json!({
                "repo": { "owner": "octo", "name": "world" },
                "pr_number": STUB_PR_NUMBER,
                "merge_commit_sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
            }),
            causation: Causation::External {
                source: "smoke_test".into(),
                request_id: "merge-1".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("merge-1".into()),
        })
        .await
        .expect("synthesize PrMerged");

    poll_until_event(
        &executor,
        &workflow_id,
        EVT_PR_MERGED,
        Duration::from_secs(5),
    )
    .await;

    // Round-18: event presence in the log is necessary but not
    // sufficient — Storage::advance always appends the event, even if
    // the reducer ignores it. Read the snapshot back and assert the
    // workflow ACTUALLY transitioned to Merged.
    let state = read_workflow_state(&executor, &workflow_id)
        .await
        .expect("snapshot must exist after PrMerged");
    assert_eq!(
        state.status,
        WorkflowStatus::Merged,
        "workflow status must transition to Merged; got {:?}",
        state.status,
    );
    assert_eq!(
        state.merge_commit_sha.as_deref(),
        Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
        "merge_commit_sha should reflect the synthesized webhook payload",
    );
    assert_eq!(state.pr_number, Some(STUB_PR_NUMBER));

    // Sanity check on the event sequence.
    //
    // Stage A wake-driven dispatch (commit 428322d) creates a real
    // interleaving: `finalize_success` writes the outcome event first
    // (which enqueues the reducer-derived follow-up action into the
    // outbox), then its side events. If the wake fires before the
    // side-event writes complete, the next action handler can begin a
    // fresh `advance` against the same workflow; per-workflow sequence
    // conflicts get retried, so the pending side event lands at a
    // later sequence than the next main outcome.
    //
    // The strict-order assertion that used to live here flaked under
    // exactly that pattern (task #6). We now check two weaker but
    // still meaningful invariants:
    //   1. The "spine" of main outcome events appears in causal order.
    //      Each spine entry's reducer transitions the workflow into the
    //      next step, so this order is a hard invariant of the design.
    //   2. The total count of side `core.budget.consumed.v1` events
    //      matches the number of agent steps that emit them.
    //
    // Per-event causation is verified upstream by individual reducer
    // / dispatcher tests; this smoke test only needs to confirm the
    // end-to-end flow reached every expected step.
    let events = executor.storage().read_events(&workflow_id).await.unwrap();
    let payload_types: Vec<_> = events.iter().map(|e| e.payload_type.as_str()).collect();

    let spine: Vec<&str> = payload_types
        .iter()
        .copied()
        .filter(|p| *p != "core.budget.consumed.v1")
        .collect();
    let expected_spine: Vec<&str> = vec![
        "workflow.ticket_ingested.v1",
        "agent.triage.completed.v1",
        "agent.plan.proposed.v1",
        "github.branch_ensured.v1",
        "agent.coder.output.v1",
        "github.commit_pushed.v1",
        "agent.reviewer.output.v1",
        "agent.security_reviewer.output.v1",
        "github.pr_opened.v1",
        "github.pr_merged.v1",
    ];
    assert_eq!(
        spine, expected_spine,
        "main outcome spine diverged from the v1 happy path; full event log: {payload_types:?}",
    );

    // Five agent steps emit `core.budget.consumed.v1` as a side event:
    // triage, plan, coder, reviewer, security_reviewer. Branch /
    // commit / pr_opened / pr_merged are pure github operations and
    // emit no budget.
    let budget_count = payload_types
        .iter()
        .filter(|p| **p == "core.budget.consumed.v1")
        .count();
    assert_eq!(
        budget_count, 5,
        "expected one budget event per agent step (5); full event log: {payload_types:?}",
    );

    // Tear down the dispatcher cleanly.
    shutdown.notify_one();
    tokio::time::timeout(Duration::from_secs(5), dispatcher_join)
        .await
        .expect("dispatcher must drain within 5s")
        .expect("dispatcher task must not panic")
        .expect("dispatcher must return Ok");
}
