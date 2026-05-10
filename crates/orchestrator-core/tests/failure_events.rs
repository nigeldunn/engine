//! Tests for the M11a failure-event mechanism: when an action permanently
//! fails, the dispatcher writes an event so reducers can observe the
//! failure and produce compensating events.

use async_trait::async_trait;
use orchestrator_core::test_support::fresh_storage;
use orchestrator_core::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
struct FailureTrackerState {
    pub fail_events: u32,
    pub probe_exhausted_events: u32,
    pub last_failed_action_id: Option<String>,
    pub last_failed_kind: Option<String>,
    pub last_attempts: u32,
    pub last_payload_truncated: bool,
}

/// Reducer that emits one action with `max_attempts = 1` and tracks
/// observed failure events.
struct FailureTrackerReducer;

impl Reducer for FailureTrackerReducer {
    type State = FailureTrackerState;

    fn state_version(&self) -> u32 {
        1
    }

    fn reduce(
        &self,
        mut state: Self::State,
        event: &EventEnvelope,
    ) -> Result<Self::State, ExecutorError> {
        match event.payload_type.as_str() {
            "trigger.v1" => {}
            EVT_ACTION_FAILED => {
                state.fail_events += 1;
                let payload = decode_action_failed(&event.payload)
                    .ok_or_else(|| ExecutorError::Reducer("decode failed".into()))?;
                state.last_failed_action_id = Some(payload.action_id.0.clone());
                state.last_failed_kind = Some(payload.kind.clone());
                state.last_attempts = payload.attempts;
                state.last_payload_truncated = payload.payload_truncated;
            }
            EVT_ACTION_PROBE_EXHAUSTED => {
                state.probe_exhausted_events += 1;
            }
            other => return Err(ExecutorError::Reducer(format!("unknown event {}", other))),
        }
        Ok(state)
    }

    fn derive_actions(
        &self,
        _state: &Self::State,
        ev: &EventEnvelope,
    ) -> Result<Vec<Action>, ExecutorError> {
        if ev.payload_type == "trigger.v1" {
            Ok(vec![Action {
                kind: "test.failing_action".into(),
                payload: ev.payload.clone(),
                delay_seconds: 0,
                max_attempts: 1, // exhaust immediately on first failure
                max_probe_attempts: 1,
            }])
        } else {
            Ok(vec![])
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SinkBehavior {
    AlwaysTransientFail,
    AlwaysPermanentFail,
}

struct ConfigurableSink {
    sink_key: String,
    behavior: SinkBehavior,
    invocations: Arc<AtomicUsize>,
}

#[async_trait]
impl Sink for ConfigurableSink {
    fn handles(&self) -> &[&'static str] {
        &["test.failing_action"]
    }
    fn sink_key(&self) -> &str {
        &self.sink_key
    }
    async fn execute(&self, _action: &ClaimedAction) -> Result<AttemptOutcome, DispatcherError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        match self.behavior {
            SinkBehavior::AlwaysTransientFail => Ok(AttemptOutcome::TransientFail {
                error: "simulated transient".into(),
            }),
            SinkBehavior::AlwaysPermanentFail => Ok(AttemptOutcome::PermanentFail {
                error: "simulated permanent".into(),
            }),
        }
    }
}

async fn setup(
    behavior: SinkBehavior,
) -> (
    Arc<Executor<FailureTrackerReducer>>,
    Arc<AtomicUsize>,
    Arc<tokio::sync::Notify>,
) {
    let (storage, _db) = fresh_storage().await;
    let executor = Arc::new(Executor::new(storage, FailureTrackerReducer));
    let mut dispatcher = Dispatcher::new(
        executor.clone(),
        DispatcherConfig {
            poll_interval: Duration::from_millis(50),
            lease_duration: Duration::from_secs(30),
            health_check_interval: Duration::from_millis(200),
            sink_unhealthy_retry_delay: Duration::from_millis(100),
            ..Default::default()
        },
    );
    let invocations = Arc::new(AtomicUsize::new(0));
    let sink = ConfigurableSink {
        sink_key: "test-sink".into(),
        behavior,
        invocations: invocations.clone(),
    };
    dispatcher.register(sink);
    let shutdown = dispatcher.shutdown_handle();
    tokio::spawn(dispatcher.run());
    (executor, invocations, shutdown)
}

async fn wait_for_event(
    executor: &Executor<FailureTrackerReducer>,
    workflow_id: &WorkflowId,
    payload_type: &str,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        let events = executor.storage().read_events(workflow_id).await.unwrap();
        if events.iter().any(|e| e.payload_type == payload_type) {
            return true;
        }
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn permanent_fail_writes_action_failed_event() {
    let (executor, invocations, shutdown) = setup(SinkBehavior::AlwaysPermanentFail).await;
    let workflow_id = WorkflowId::new("wf-permanent");

    executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: "trigger.v1".into(),
            payload_schema_version: 1,
            payload: json!({"hello": "world"}),
            causation: Causation::External {
                source: "test".into(),
                request_id: "r-1".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("trigger-1".into()),
        })
        .await
        .unwrap();

    let observed = wait_for_event(
        &executor,
        &workflow_id,
        EVT_ACTION_FAILED,
        Duration::from_secs(3),
    )
    .await;
    assert!(observed, "expected EVT_ACTION_FAILED event to land");

    let events = executor.storage().read_events(&workflow_id).await.unwrap();
    let failed = events
        .iter()
        .find(|e| e.payload_type == EVT_ACTION_FAILED)
        .unwrap();
    let payload = decode_action_failed(&failed.payload).unwrap();
    assert_eq!(payload.kind, "test.failing_action");
    assert_eq!(payload.final_state, ActionState::Failed);
    assert_eq!(payload.attempts, 1);
    assert!(!payload.payload_truncated);
    assert_eq!(
        payload.original_payload.as_ref().and_then(|v| v.get("hello")),
        Some(&json!("world"))
    );
    assert_eq!(invocations.load(Ordering::SeqCst), 1);

    shutdown.notify_one();
}

#[tokio::test]
async fn transient_fail_with_max_attempts_one_writes_failure_event() {
    let (executor, invocations, shutdown) = setup(SinkBehavior::AlwaysTransientFail).await;
    let workflow_id = WorkflowId::new("wf-transient");

    executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: "trigger.v1".into(),
            payload_schema_version: 1,
            payload: json!({}),
            causation: Causation::External {
                source: "test".into(),
                request_id: "r-2".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("trigger-2".into()),
        })
        .await
        .unwrap();

    let observed = wait_for_event(
        &executor,
        &workflow_id,
        EVT_ACTION_FAILED,
        Duration::from_secs(3),
    )
    .await;
    assert!(observed, "expected EVT_ACTION_FAILED event to land");
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    shutdown.notify_one();
}

#[tokio::test]
async fn failure_event_dedup_keys_distinguish_failed_from_probe_exhausted() {
    // Sanity: build commands for both terminal states with the same
    // action and verify their dedup keys are different.
    let workflow_id = WorkflowId::new("wf");
    let action_id = ActionId::derive(&workflow_id, 0, 0, "k");
    let action = ClaimedAction {
        action_id: action_id.clone(),
        workflow_id,
        source_sequence: 0,
        kind: "k".into(),
        payload: json!({}),
        attempt: 0,
        max_attempts: 1,
        probe_attempt: 0,
        max_probe_attempts: 1,
        claimed_by: DispatcherId::new(),
        lease_expires_at: chrono::Utc::now() + chrono::Duration::seconds(60),
    };

    let failed = build_failure_event_command(
        &action,
        ActionState::Failed,
        "err".into(),
        1,
        0,
    );
    let probe = build_failure_event_command(
        &action,
        ActionState::FailedProbeExhausted,
        "err".into(),
        0,
        1,
    );
    assert_ne!(failed.ingress_dedup_key, probe.ingress_dedup_key);
    assert!(failed.ingress_dedup_key.unwrap().starts_with("action_failed:"));
    assert!(probe.ingress_dedup_key.unwrap().starts_with("probe_exhausted:"));
}

#[tokio::test]
async fn failure_event_advance_is_idempotent_via_dedup_key() {
    // Verify the dedup-key contract: writing the same failure event
    // command twice through executor.advance returns the prior outcome
    // on the second call, without producing a duplicate event.
    let (storage, _db) = fresh_storage().await;
    let executor = Executor::new(storage, FailureTrackerReducer);
    let workflow_id = WorkflowId::new("wf-dedup");

    // Seed the workflow with a trigger event to satisfy the reducer.
    executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: "trigger.v1".into(),
            payload_schema_version: 1,
            payload: json!({}),
            causation: Causation::External {
                source: "test".into(),
                request_id: "seed".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("seed".into()),
        })
        .await
        .unwrap();

    let action_id = ActionId::derive(&workflow_id, 0, 0, "test.failing_action");
    let action = ClaimedAction {
        action_id,
        workflow_id: workflow_id.clone(),
        source_sequence: 0,
        kind: "test.failing_action".into(),
        payload: json!({}),
        attempt: 0,
        max_attempts: 1,
        probe_attempt: 0,
        max_probe_attempts: 1,
        claimed_by: DispatcherId::new(),
        lease_expires_at: chrono::Utc::now() + chrono::Duration::seconds(60),
    };

    let cmd1 = build_failure_event_command(
        &action,
        ActionState::Failed,
        "first".into(),
        1,
        0,
    );
    let cmd2 = build_failure_event_command(
        &action,
        ActionState::Failed,
        "second".into(), // different error message
        1,
        0,
    );

    let first = executor.advance(cmd1).await.unwrap();
    assert!(!first.deduplicated);

    let second = executor.advance(cmd2).await.unwrap();
    assert!(second.deduplicated, "second advance should hit dedup branch");
    assert_eq!(first.event_id.0, second.event_id.0);

    let events = executor.storage().read_events(&workflow_id).await.unwrap();
    let failure_events: Vec<_> = events
        .iter()
        .filter(|e| e.payload_type == EVT_ACTION_FAILED)
        .collect();
    assert_eq!(
        failure_events.len(),
        1,
        "should be exactly one failure event despite two advance calls"
    );
}

// ── side events ──────────────────────────────────────────────────────

/// Reducer that emits a single action and tracks side-event observation.
#[derive(Default, Clone, Serialize, Deserialize, Debug)]
struct SideEventState {
    pub primary_observed: u32,
    pub side_observed: u32,
    pub side_dedup_keys_seen: Vec<String>,
}

struct SideEventReducer;

impl Reducer for SideEventReducer {
    type State = SideEventState;
    fn state_version(&self) -> u32 {
        1
    }
    fn reduce(
        &self,
        mut state: Self::State,
        ev: &EventEnvelope,
    ) -> Result<Self::State, ExecutorError> {
        match ev.payload_type.as_str() {
            "trigger.v1" => {}
            "primary.v1" => state.primary_observed += 1,
            "side.v1" => {
                state.side_observed += 1;
            }
            _ => {}
        }
        Ok(state)
    }
    fn derive_actions(
        &self,
        _state: &Self::State,
        ev: &EventEnvelope,
    ) -> Result<Vec<Action>, ExecutorError> {
        if ev.payload_type == "trigger.v1" {
            Ok(vec![Action {
                kind: "test.side_event_action".into(),
                payload: serde_json::json!({}),
                delay_seconds: 0,
                max_attempts: 1,
                max_probe_attempts: 20,
            }])
        } else {
            Ok(vec![])
        }
    }
}

struct SideEventSink;

#[async_trait]
impl Sink for SideEventSink {
    fn handles(&self) -> &[&'static str] {
        &["test.side_event_action"]
    }
    fn sink_key(&self) -> &str {
        "test-side-events"
    }
    async fn execute(
        &self,
        action: &ClaimedAction,
    ) -> Result<AttemptOutcome, DispatcherError> {
        let primary = EventCommand {
            workflow_id: action.workflow_id.clone(),
            payload_type: "primary.v1".into(),
            payload_schema_version: 1,
            payload: serde_json::json!({}),
            causation: Causation::Action {
                action_id: action.action_id.clone(),
            },
            trace_id: None,
            ingress_dedup_key: None,
        };
        let side = EventCommand {
            workflow_id: action.workflow_id.clone(),
            payload_type: "side.v1".into(),
            payload_schema_version: 1,
            payload: serde_json::json!({"note": "side event"}),
            causation: Causation::Action {
                action_id: action.action_id.clone(),
            },
            trace_id: None,
            ingress_dedup_key: Some(format!("side:{}", action.action_id.as_str())),
        };
        Ok(AttemptOutcome::Succeeded {
            external_ref: None,
            outcome_event: primary,
            side_events: vec![side],
        })
    }
}

#[tokio::test]
async fn side_events_are_written_after_primary_outcome() {
    let (storage, _db) = fresh_storage().await;
    let executor = Arc::new(Executor::new(storage, SideEventReducer));
    let mut dispatcher = Dispatcher::new(
        executor.clone(),
        DispatcherConfig {
            poll_interval: Duration::from_millis(50),
            ..Default::default()
        },
    );
    dispatcher.register(SideEventSink);
    let shutdown = dispatcher.shutdown_handle();
    tokio::spawn(dispatcher.run());

    let workflow_id = WorkflowId::new("wf-side");
    executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: "trigger.v1".into(),
            payload_schema_version: 1,
            payload: serde_json::json!({}),
            causation: Causation::External {
                source: "t".into(),
                request_id: "r".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("trigger".into()),
        })
        .await
        .unwrap();

    // Wait for both primary and side events to land.
    let mut both_observed = false;
    for _ in 0..30 {
        let events = executor.storage().read_events(&workflow_id).await.unwrap();
        let has_primary = events.iter().any(|e| e.payload_type == "primary.v1");
        let has_side = events.iter().any(|e| e.payload_type == "side.v1");
        if has_primary && has_side {
            both_observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(both_observed, "expected both primary and side events to land");

    // Verify ordering: primary comes BEFORE side in sequence.
    let events = executor.storage().read_events(&workflow_id).await.unwrap();
    let primary_seq = events
        .iter()
        .find(|e| e.payload_type == "primary.v1")
        .unwrap()
        .sequence;
    let side_seq = events
        .iter()
        .find(|e| e.payload_type == "side.v1")
        .unwrap()
        .sequence;
    assert!(
        primary_seq < side_seq,
        "primary {} should come before side {}",
        primary_seq,
        side_seq
    );

    shutdown.notify_one();
}

