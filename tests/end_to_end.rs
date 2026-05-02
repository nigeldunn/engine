//! End-to-end integration test for executor + dispatcher + sink.
//!
//! Models a trivial "counter" workflow:
//!   - On `increment` event, bump counter and emit a `notify` action.
//!   - The notify sink writes a `notified` event back.
//!   - On `notified`, transition to "done".

use async_trait::async_trait;
use chrono::Duration as ChronoDuration;
use orchestrator_core::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
struct CounterState {
    value: u64,
    notified: bool,
}

struct CounterReducer;

impl Reducer for CounterReducer {
    type State = CounterState;

    fn state_version(&self) -> u32 { 1 }

    fn reduce(
        &self,
        mut state: Self::State,
        event: &EventEnvelope,
    ) -> Result<Self::State, ExecutorError> {
        match event.payload_type.as_str() {
            "increment.v1" => state.value += 1,
            "notified.v1" => state.notified = true,
            other => return Err(ExecutorError::Reducer(format!("unknown event {}", other))),
        }
        Ok(state)
    }

    fn derive_actions(
        &self,
        new_state: &Self::State,
        triggering_event: &EventEnvelope,
    ) -> Result<Vec<Action>, ExecutorError> {
        // Emit notify action only on increment events, not on notified events.
        if triggering_event.payload_type == "increment.v1" {
            Ok(vec![Action {
                kind: "notify".into(),
                payload: json!({ "value": new_state.value }),
                delay_seconds: 0,
                max_attempts: 3,
            }])
        } else {
            Ok(vec![])
        }
    }
}

struct CountingSink {
    invocations: Arc<AtomicUsize>,
    fail_first_n: Arc<AtomicUsize>,
}

#[async_trait]
impl Sink for CountingSink {
    fn handles(&self) -> &[&'static str] { &["notify"] }

    async fn execute(&self, action: &ClaimedAction) -> Result<AttemptOutcome, DispatcherError> {
        let n = self.invocations.fetch_add(1, Ordering::SeqCst);
        let fail_until = self.fail_first_n.load(Ordering::SeqCst);

        if n < fail_until {
            return Ok(AttemptOutcome::TransientFail {
                error: format!("simulated failure {}", n),
            });
        }

        Ok(AttemptOutcome::Succeeded {
            external_ref: Some(format!("notification-{}", action.action_id)),
            outcome_event: EventCommand {
                workflow_id: action.workflow_id.clone(),
                payload_type: "notified.v1".into(),
                payload_schema_version: 1,
                payload: json!({ "via_action": action.action_id.as_str() }),
                causation: Causation::Action {
                    action_id: action.action_id.clone(),
                },
                trace_id: None,
                ingress_dedup_key: None,
            },
        })
    }
}

async fn setup() -> (Arc<Executor<CounterReducer>>, Arc<tokio::sync::Notify>) {
    let storage = Storage::open("sqlite::memory:").await.unwrap();
    let executor = Arc::new(Executor::new(storage, CounterReducer));
    let mut dispatcher = Dispatcher::new(
        executor.clone(),
        DispatcherConfig {
            poll_interval: Duration::from_millis(50),
            lease_duration: ChronoDuration::seconds(30),
            ..Default::default()
        },
    );
    dispatcher.register(CountingSink {
        invocations: Arc::new(AtomicUsize::new(0)),
        fail_first_n: Arc::new(AtomicUsize::new(0)),
    });
    let shutdown = dispatcher.shutdown_handle();
    tokio::spawn(dispatcher.run());
    (executor, shutdown)
}

#[tokio::test]
async fn happy_path_increment_to_done() {
    let _ = tracing_subscriber::fmt::try_init();
    let (executor, shutdown) = setup().await;

    let workflow_id = WorkflowId::new("wf-1");

    let outcome = executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: "increment.v1".into(),
            payload_schema_version: 1,
            payload: json!({}),
            causation: Causation::External {
                source: "test".into(),
                request_id: "req-1".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("req-1".into()),
        })
        .await
        .unwrap();

    assert_eq!(outcome.sequence, 0);
    assert_eq!(outcome.actions_enqueued.len(), 1);

    // Wait for the dispatcher to run the action and write the outcome event.
    let mut notified = false;
    for _ in 0..50 {
        let events = executor.storage().read_events(&workflow_id).await.unwrap();
        if events.iter().any(|e| e.payload_type == "notified.v1") {
            notified = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(notified, "expected notified event to appear");

    shutdown.notify_one();
}

#[tokio::test]
async fn ingress_dedup_returns_prior_outcome() {
    let _ = tracing_subscriber::fmt::try_init();
    let storage = Storage::open("sqlite::memory:").await.unwrap();
    let executor = Executor::new(storage, CounterReducer);

    let workflow_id = WorkflowId::new("wf-dedup");

    let cmd = EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: "increment.v1".into(),
        payload_schema_version: 1,
        payload: json!({}),
        causation: Causation::External {
            source: "test".into(),
            request_id: "req-X".into(),
        },
        trace_id: None,
        ingress_dedup_key: Some("delivery-XYZ".into()),
    };

    let first = executor.advance(cmd.clone()).await.unwrap();
    assert!(!first.deduplicated);

    let second = executor.advance(cmd).await.unwrap();
    assert!(second.deduplicated);
    assert_eq!(first.event_id.0, second.event_id.0);
    assert_eq!(first.sequence, second.sequence);
}

#[tokio::test]
async fn deterministic_action_id() {
    let wf = WorkflowId::new("wf");
    let a = ActionId::derive(&wf, 42, 0, "notify");
    let b = ActionId::derive(&wf, 42, 0, "notify");
    let c = ActionId::derive(&wf, 42, 1, "notify");
    assert_eq!(a.0, b.0);
    assert_ne!(a.0, c.0);
}
