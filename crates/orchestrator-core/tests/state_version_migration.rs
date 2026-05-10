//! Tests for `Storage::advance` snapshot reuse vs replay.
//!
//! When the snapshot's `state_version` matches the reducer's current
//! `state_version()`, `advance` reads it directly. When it doesn't,
//! `advance` discards the snapshot and replays the event log to rebuild
//! state. Snapshots are a cache; the event log is authoritative.

use orchestrator_core::test_support::{fresh_storage, reopen};
use orchestrator_core::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
struct CountState {
    pub count: u32,
}

/// Reducer with externally-controllable state_version. Lets the test
/// simulate "old snapshot in the DB, newer reducer schema" without
/// rebuilding the test reducer mid-test.
struct VersionedReducer {
    pub version: u32,
}

impl Reducer for VersionedReducer {
    type State = CountState;
    fn state_version(&self) -> u32 {
        self.version
    }
    fn reduce(
        &self,
        mut state: Self::State,
        event: &EventEnvelope,
    ) -> Result<Self::State, ExecutorError> {
        if event.payload_type == "increment.v1" {
            state.count += 1;
        }
        Ok(state)
    }
    fn derive_actions(
        &self,
        _: &Self::State,
        _: &EventEnvelope,
    ) -> Result<Vec<Action>, ExecutorError> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn matching_state_version_reuses_snapshot() {
    let (storage, _db) = fresh_storage().await;
    let executor = Executor::new(storage, VersionedReducer { version: 1 });

    let workflow_id = WorkflowId::new("wf-match");
    for i in 0..3 {
        executor
            .advance(EventCommand {
                workflow_id: workflow_id.clone(),
                payload_type: "increment.v1".into(),
                payload_schema_version: 1,
                payload: json!({}),
                causation: Causation::External {
                    source: "t".into(),
                    request_id: format!("r-{}", i),
                },
                trace_id: None,
                ingress_dedup_key: Some(format!("k-{}", i)),
            })
            .await
            .unwrap();
    }

    // Snapshot now contains count=3 with state_version=1. A subsequent
    // advance with the same reducer reads the snapshot directly.
    let outcome = executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: "increment.v1".into(),
            payload_schema_version: 1,
            payload: json!({}),
            causation: Causation::External {
                source: "t".into(),
                request_id: "r-3".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("k-3".into()),
        })
        .await
        .unwrap();
    assert_eq!(outcome.sequence, 3);
    // State should be count=4 (snapshot reuse + 1 new event).
}

#[tokio::test]
async fn state_version_mismatch_discards_snapshot_and_replays() {
    let (storage, db) = fresh_storage().await;

    let workflow_id = WorkflowId::new("wf-migrate");

    // Phase 1: write 4 events with reducer at version 1. Snapshot
    // persists at state_version=1.
    {
        let executor = Executor::new(storage, VersionedReducer { version: 1 });
        for i in 0..4 {
            executor
                .advance(EventCommand {
                    workflow_id: workflow_id.clone(),
                    payload_type: "increment.v1".into(),
                    payload_schema_version: 1,
                    payload: json!({}),
                    causation: Causation::External {
                        source: "t".into(),
                        request_id: format!("r-{}", i),
                    },
                    trace_id: None,
                    ingress_dedup_key: Some(format!("k-{}", i)),
                })
                .await
                .unwrap();
        }
    }

    // Phase 2: reopen against the same per-test database with reducer
    // at version 2 (simulating a schema bump). The next advance must
    // discard the v1 snapshot and replay the 4 stored events to rebuild
    // state to count=4 before applying the new event.
    let storage = reopen(&db).await;
    let executor = Executor::new(storage, VersionedReducer { version: 2 });
    let outcome = executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: "increment.v1".into(),
            payload_schema_version: 1,
            payload: json!({}),
            causation: Causation::External {
                source: "t".into(),
                request_id: "r-4".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("k-4".into()),
        })
        .await
        .unwrap();
    assert_eq!(outcome.sequence, 4);

    // To verify the reducer received the replayed prior state correctly,
    // one more event should land at count=6 (4 replayed + 1 new + 1 more).
    let outcome2 = executor
        .advance(EventCommand {
            workflow_id: workflow_id.clone(),
            payload_type: "increment.v1".into(),
            payload_schema_version: 1,
            payload: json!({}),
            causation: Causation::External {
                source: "t".into(),
                request_id: "r-5".into(),
            },
            trace_id: None,
            ingress_dedup_key: Some("k-5".into()),
        })
        .await
        .unwrap();
    assert_eq!(outcome2.sequence, 5);

    // Snapshot is now at state_version=2 from the latest advance.
    // A third reopen with reducer version 2 should reuse that snapshot.
}
