//! Integration test: ActionBuilder ids must byte-match what Storage::advance
//! derives when it iterates the returned Vec<Action>.

use orchestrator_core::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
struct UnitState;

/// A reducer that emits three actions of distinct kinds via ActionBuilder
/// and returns `builder.into_actions()` — exactly the canonical pattern.
struct MultiActionReducer;

impl Reducer for MultiActionReducer {
    type State = UnitState;

    fn state_version(&self) -> u32 {
        1
    }

    fn reduce(
        &self,
        state: Self::State,
        _event: &EventEnvelope,
    ) -> Result<Self::State, ExecutorError> {
        Ok(state)
    }

    fn derive_actions(
        &self,
        _new_state: &Self::State,
        ev: &EventEnvelope,
    ) -> Result<Vec<Action>, ExecutorError> {
        let wf = ev.workflow_id.clone();
        let mut b = ActionBuilder::new(&wf, ev.sequence);
        b.push(Action {
            kind: "kind.alpha".into(),
            payload: json!({}),
            delay_seconds: 0,
            max_attempts: 1,
            max_probe_attempts: 20,
        });
        b.push(Action {
            kind: "kind.bravo".into(),
            payload: json!({}),
            delay_seconds: 0,
            max_attempts: 1,
            max_probe_attempts: 20,
        });
        b.push(Action {
            kind: "kind.charlie".into(),
            payload: json!({}),
            delay_seconds: 0,
            max_attempts: 1,
            max_probe_attempts: 20,
        });
        Ok(b.into_actions())
    }
}

#[tokio::test]
async fn action_builder_ids_match_storage_advance_indices() {
    let storage = Storage::open("sqlite::memory:").await.unwrap();
    let executor = Executor::new(storage, MultiActionReducer);

    let wf = WorkflowId::new("wf-builder");
    let outcome = executor
        .advance(EventCommand {
            workflow_id: wf.clone(),
            payload_type: "trigger.v1".into(),
            payload_schema_version: 1,
            payload: json!({}),
            causation: Causation::External {
                source: "test".into(),
                request_id: "r-1".into(),
            },
            trace_id: None,
            ingress_dedup_key: None,
        })
        .await
        .unwrap();

    // Reproduce the same builder calls outside the reducer for sequence=0.
    let mut b = ActionBuilder::new(&wf, 0);
    let r0 = b.push(Action {
        kind: "kind.alpha".into(),
        payload: json!({}),
        delay_seconds: 0,
        max_attempts: 1,
        max_probe_attempts: 20,
    });
    let r1 = b.push(Action {
        kind: "kind.bravo".into(),
        payload: json!({}),
        delay_seconds: 0,
        max_attempts: 1,
        max_probe_attempts: 20,
    });
    let r2 = b.push(Action {
        kind: "kind.charlie".into(),
        payload: json!({}),
        delay_seconds: 0,
        max_attempts: 1,
        max_probe_attempts: 20,
    });

    assert_eq!(outcome.actions_enqueued.len(), 3);
    assert_eq!(outcome.actions_enqueued[0], r0.action_id);
    assert_eq!(outcome.actions_enqueued[1], r1.action_id);
    assert_eq!(outcome.actions_enqueued[2], r2.action_id);

    // Cross-check each ref against the raw `ActionId::derive` call — proving
    // the builder mirrors `Storage::advance`'s indexing contract directly.
    assert_eq!(r0.action_id, ActionId::derive(&wf, 0, 0, "kind.alpha"));
    assert_eq!(r1.action_id, ActionId::derive(&wf, 0, 1, "kind.bravo"));
    assert_eq!(r2.action_id, ActionId::derive(&wf, 0, 2, "kind.charlie"));

    // Kinds carried on the ref match the actions.
    assert_eq!(r0.kind, "kind.alpha");
    assert_eq!(r1.kind, "kind.bravo");
    assert_eq!(r2.kind, "kind.charlie");

    // Short forms are 16 chars and equal the base32 body prefix.
    for r in [&r0, &r1, &r2] {
        assert_eq!(r.short.len(), 16);
        assert_eq!(r.short, &r.action_id.as_str()[4..20]);
    }
}

#[tokio::test]
async fn changing_only_kind_changes_the_persisted_id() {
    // Two single-action reducers that differ only in `kind` must produce
    // different ids at the same workflow_id and sequence.
    struct SingleKind(&'static str);
    impl Reducer for SingleKind {
        type State = UnitState;
        fn state_version(&self) -> u32 {
            1
        }
        fn reduce(
            &self,
            state: Self::State,
            _: &EventEnvelope,
        ) -> Result<Self::State, ExecutorError> {
            Ok(state)
        }
        fn derive_actions(
            &self,
            _: &Self::State,
            ev: &EventEnvelope,
        ) -> Result<Vec<Action>, ExecutorError> {
            let mut b = ActionBuilder::new(&ev.workflow_id, ev.sequence);
            b.push(Action {
                kind: self.0.into(),
                payload: json!({}),
                delay_seconds: 0,
                max_attempts: 1,
                max_probe_attempts: 20,
            });
            Ok(b.into_actions())
        }
    }

    let storage_a = Storage::open("sqlite::memory:").await.unwrap();
    let exec_a = Executor::new(storage_a, SingleKind("kind.x"));
    let storage_b = Storage::open("sqlite::memory:").await.unwrap();
    let exec_b = Executor::new(storage_b, SingleKind("kind.y"));

    let wf = WorkflowId::new("wf-kind");
    let cmd = || EventCommand {
        workflow_id: wf.clone(),
        payload_type: "t.v1".into(),
        payload_schema_version: 1,
        payload: json!({}),
        causation: Causation::External {
            source: "t".into(),
            request_id: "r".into(),
        },
        trace_id: None,
        ingress_dedup_key: None,
    };

    let oa = exec_a.advance(cmd()).await.unwrap();
    let ob = exec_b.advance(cmd()).await.unwrap();
    assert_ne!(oa.actions_enqueued[0], ob.actions_enqueued[0]);
}
