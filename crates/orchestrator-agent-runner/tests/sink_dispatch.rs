//! Tests for the AgentRunnerSink dispatch logic using a mock AgentClient.
//! No HTTP — the mock impl directly returns the configured response.

use async_trait::async_trait;
use chrono::Utc;
use orchestrator_agent_runner::{
    AgentClient, AgentError, AgentRunResult, AgentRunStatus, AgentRunnerSink,
};
use orchestrator_coding_workflow::{
    EVT_BUDGET_CONSUMED, EVT_CODER_OUTPUT, EVT_REVIEWER_OUTPUT, EVT_TRIAGE_COMPLETED,
    KIND_AGENT_CODER, KIND_AGENT_REVIEWER, KIND_AGENT_TRIAGE,
};
use orchestrator_core::{
    ActionId, AttemptOutcome, ClaimedAction, DispatcherId, ExistingResult, Sink, SinkHealthScope,
    SinkHealthState, WorkflowId,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
struct MockResponses {
    run: Arc<Mutex<Option<Result<AgentRunResult, AgentError>>>>,
    status: Arc<Mutex<Option<Result<AgentRunStatus, AgentError>>>>,
    health: Arc<Mutex<Option<Result<(), AgentError>>>>,
    last_run_payload: Arc<Mutex<Option<Value>>>,
    last_run_request_id: Arc<Mutex<Option<String>>>,
}

#[derive(Clone)]
struct MockAgentClient {
    resp: MockResponses,
}

impl MockAgentClient {
    fn new() -> (Self, MockResponses) {
        let resp = MockResponses::default();
        (
            Self {
                resp: resp.clone(),
            },
            resp,
        )
    }
}

#[async_trait]
impl AgentClient for MockAgentClient {
    async fn run(
        &self,
        _agent_type: &str,
        _action_id: &ActionId,
        payload: &Value,
        request_id: &str,
    ) -> Result<AgentRunResult, AgentError> {
        *self.resp.last_run_payload.lock().unwrap() = Some(payload.clone());
        *self.resp.last_run_request_id.lock().unwrap() = Some(request_id.to_string());
        self.resp
            .run
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| Err(AgentError::Transport("no mock response set".into())))
    }
    async fn status(
        &self,
        _agent_type: &str,
        _action_id: &ActionId,
    ) -> Result<AgentRunStatus, AgentError> {
        self.resp
            .status
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| Err(AgentError::Transport("no mock status set".into())))
    }
    async fn health(&self) -> Result<(), AgentError> {
        self.resp
            .health
            .lock()
            .unwrap()
            .take()
            .unwrap_or(Ok(()))
    }
}

fn workflow_id() -> WorkflowId {
    WorkflowId::new("wf-test")
}

fn make_action(kind: &str) -> ClaimedAction {
    let action_id = ActionId::derive(&workflow_id(), 0, 0, kind);
    ClaimedAction {
        action_id,
        workflow_id: workflow_id(),
        source_sequence: 0,
        kind: kind.into(),
        payload: json!({"some": "input"}),
        attempt: 0,
        max_attempts: 5,
        probe_attempt: 0,
        max_probe_attempts: 20,
        claimed_by: DispatcherId::new(),
        lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
    }
}

#[tokio::test]
async fn execute_finished_emits_outcome_event_and_budget_side_event() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    let action = make_action(KIND_AGENT_TRIAGE);

    *resp.run.lock().unwrap() = Some(Ok(AgentRunResult::Finished {
        output: json!({
            "action_id": action.action_id.0,
            "accepted": true,
            "reason": null,
        }),
        cost_cents: Some(120),
    }));

    let outcome = sink.execute(&action).await.unwrap();
    let (outcome_event, side_events) = match outcome {
        AttemptOutcome::Succeeded {
            outcome_event,
            side_events,
            ..
        } => (outcome_event, side_events),
        other => panic!("expected Succeeded, got {:?}", other),
    };
    assert_eq!(outcome_event.payload_type, EVT_TRIAGE_COMPLETED);
    assert!(outcome_event.trace_id.is_some(), "request_id stamped onto trace_id");
    assert_eq!(side_events.len(), 1);
    assert_eq!(side_events[0].payload_type, EVT_BUDGET_CONSUMED);
    let budget_payload = &side_events[0].payload;
    assert_eq!(budget_payload["cents"].as_u64(), Some(120));
    assert_eq!(budget_payload["category"].as_str(), Some("agent.triage"));
    assert!(side_events[0].ingress_dedup_key.as_ref().unwrap().starts_with("budget_consumed:"));
}

#[tokio::test]
async fn execute_finished_without_cost_emits_no_side_events() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    let action = make_action(KIND_AGENT_TRIAGE);
    *resp.run.lock().unwrap() = Some(Ok(AgentRunResult::Finished {
        output: json!({
            "action_id": action.action_id.0,
            "accepted": true,
        }),
        cost_cents: None,
    }));
    let outcome = sink.execute(&action).await.unwrap();
    let side_events = match outcome {
        AttemptOutcome::Succeeded { side_events, .. } => side_events,
        other => panic!("expected Succeeded, got {:?}", other),
    };
    assert!(side_events.is_empty());
}

#[tokio::test]
async fn execute_zero_cost_emits_no_side_events() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    let action = make_action(KIND_AGENT_TRIAGE);
    *resp.run.lock().unwrap() = Some(Ok(AgentRunResult::Finished {
        output: json!({
            "action_id": action.action_id.0,
            "accepted": true,
        }),
        cost_cents: Some(0),
    }));
    let outcome = sink.execute(&action).await.unwrap();
    let side_events = match outcome {
        AttemptOutcome::Succeeded { side_events, .. } => side_events,
        _ => panic!("expected Succeeded"),
    };
    // 0 cents → don't bother emitting (Codex round-3 G.2: degrade gracefully).
    assert!(side_events.is_empty());
}

#[tokio::test]
async fn execute_still_running_returns_transient_fail() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.run.lock().unwrap() = Some(Ok(AgentRunResult::StillRunning));
    let outcome = sink.execute(&make_action(KIND_AGENT_TRIAGE)).await.unwrap();
    assert!(matches!(outcome, AttemptOutcome::TransientFail { .. }));
}

#[tokio::test]
async fn execute_unknown_agent_type_returns_permanent_fail() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.run.lock().unwrap() = Some(Err(AgentError::UnknownAgentType("not registered".into())));
    let outcome = sink.execute(&make_action(KIND_AGENT_TRIAGE)).await.unwrap();
    assert!(matches!(outcome, AttemptOutcome::PermanentFail { .. }));
}

#[tokio::test]
async fn execute_invalid_input_returns_permanent_fail() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.run.lock().unwrap() = Some(Err(AgentError::InvalidInput("payload missing field".into())));
    let outcome = sink.execute(&make_action(KIND_AGENT_TRIAGE)).await.unwrap();
    assert!(matches!(outcome, AttemptOutcome::PermanentFail { .. }));
}

#[tokio::test]
async fn execute_auth_failure_returns_sink_unhealthy() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.run.lock().unwrap() = Some(Err(AgentError::AuthenticationFailed("bad token".into())));
    let outcome = sink.execute(&make_action(KIND_AGENT_TRIAGE)).await.unwrap();
    assert!(matches!(outcome, AttemptOutcome::SinkUnhealthy { .. }));
}

#[tokio::test]
async fn execute_transport_returns_transient_fail() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.run.lock().unwrap() = Some(Err(AgentError::Transport("connection reset".into())));
    let outcome = sink.execute(&make_action(KIND_AGENT_TRIAGE)).await.unwrap();
    assert!(matches!(outcome, AttemptOutcome::TransientFail { .. }));
}

#[tokio::test]
async fn execute_malformed_output_returns_permanent_fail() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    let action = make_action(KIND_AGENT_TRIAGE);
    // Output missing the required `accepted` field.
    *resp.run.lock().unwrap() = Some(Ok(AgentRunResult::Finished {
        output: json!({"some_garbage": true}),
        cost_cents: None,
    }));
    let outcome = sink.execute(&action).await.unwrap();
    let err = match outcome {
        AttemptOutcome::PermanentFail { error } => error,
        other => panic!("expected PermanentFail, got {:?}", other),
    };
    assert!(err.contains("malformed"));
}

#[tokio::test]
async fn probe_not_found_returns_ok_none() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.status.lock().unwrap() = Some(Ok(AgentRunStatus::NotFound));
    let result = sink.find_existing(&make_action(KIND_AGENT_TRIAGE)).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn probe_running_returns_err() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.status.lock().unwrap() = Some(Ok(AgentRunStatus::Running));
    let result = sink.find_existing(&make_action(KIND_AGENT_TRIAGE)).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn probe_finished_returns_existing_result() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    let action = make_action(KIND_AGENT_REVIEWER);
    *resp.status.lock().unwrap() = Some(Ok(AgentRunStatus::Finished {
        output: json!({
            "action_id": action.action_id.0,
            "passed": true,
        }),
        cost_cents: Some(50),
    }));
    let result: Option<ExistingResult> = sink.find_existing(&action).await.unwrap();
    let existing = result.unwrap();
    assert_eq!(existing.outcome_event.payload_type, EVT_REVIEWER_OUTPUT);
    assert_eq!(existing.side_events.len(), 1);
    assert_eq!(existing.side_events[0].payload_type, EVT_BUDGET_CONSUMED);
}

#[tokio::test]
async fn check_health_healthy() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.health.lock().unwrap() = Some(Ok(()));
    let state = sink.check_health(SinkHealthScope::default()).await;
    assert!(matches!(state, SinkHealthState::Healthy));
}

#[tokio::test]
async fn check_health_auth_failure() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.health.lock().unwrap() = Some(Err(AgentError::AuthenticationFailed("nope".into())));
    let state = sink.check_health(SinkHealthScope::default()).await;
    assert!(matches!(state, SinkHealthState::Unhealthy { .. }));
}

#[tokio::test]
async fn check_health_server_error_indeterminate() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    *resp.health.lock().unwrap() = Some(Err(AgentError::ServerError {
        status: 503,
        detail: "down".into(),
    }));
    let state = sink.check_health(SinkHealthScope::default()).await;
    assert!(matches!(state, SinkHealthState::Indeterminate { .. }));
}

#[tokio::test]
async fn coder_action_routes_to_coder_spec() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    let action = make_action(KIND_AGENT_CODER);
    *resp.run.lock().unwrap() = Some(Ok(AgentRunResult::Finished {
        output: json!({
            "action_id": action.action_id.0,
            "task_idx": 0,
            "patch": { "files": [] },
            "notes": "",
        }),
        cost_cents: Some(500),
    }));
    let outcome = sink.execute(&action).await.unwrap();
    let event = match outcome {
        AttemptOutcome::Succeeded { outcome_event, side_events, .. } => {
            assert_eq!(side_events[0].payload["category"].as_str(), Some("agent.coder"));
            outcome_event
        }
        other => panic!("expected Succeeded, got {:?}", other),
    };
    assert_eq!(event.payload_type, EVT_CODER_OUTPUT);
}

#[tokio::test]
async fn run_call_includes_request_id() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    let action = make_action(KIND_AGENT_TRIAGE);
    *resp.run.lock().unwrap() = Some(Ok(AgentRunResult::Finished {
        output: json!({
            "action_id": action.action_id.0,
            "accepted": true,
        }),
        cost_cents: None,
    }));
    let _ = sink.execute(&action).await.unwrap();
    let request_id = resp.last_run_request_id.lock().unwrap().clone().unwrap();
    assert!(request_id.starts_with("req_"));
}

#[tokio::test]
async fn run_call_passes_payload_through() {
    let (client, resp) = MockAgentClient::new();
    let sink = AgentRunnerSink::new(client);
    let action = make_action(KIND_AGENT_TRIAGE);
    *resp.run.lock().unwrap() = Some(Ok(AgentRunResult::Finished {
        output: json!({
            "action_id": action.action_id.0,
            "accepted": true,
        }),
        cost_cents: None,
    }));
    let _ = sink.execute(&action).await.unwrap();
    let observed = resp.last_run_payload.lock().unwrap().clone().unwrap();
    assert_eq!(observed["some"].as_str(), Some("input"));
}
