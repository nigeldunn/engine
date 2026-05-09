//! Ticket-ingest pipeline: parse the request, derive the workflow id,
//! preflight the events table for a dedup-key conflict, then advance
//! the executor.
//!
//! Shared between the HTTP handler (in `server.rs`) and the CLI
//! subcommand (in `main.rs`) so both speak the same wire shape.
//!
//! ## WorkflowId derivation
//!
//! Default: `format!("{source}:{id}", ticket.source, ticket.id)`. The
//! `ingress_dedup_key` is set to the same string. Re-POSTing the same
//! ticket with the same payload is therefore idempotent — `Storage`'s
//! unique index on `events.ingress_dedup_key` collapses duplicates.
//!
//! Callers can pass an explicit `workflow_id` to spawn an intentional
//! second workflow for the same ticket (e.g., a retry after a halt).
//! In that case both `WorkflowId` and `ingress_dedup_key` use the
//! supplied value.
//!
//! ## 409 conflict detection
//!
//! `Storage::advance` already dedup'd the same key — but it returns
//! the prior outcome silently regardless of whether the new payload
//! matches the stored one. That would mask configuration drift (same
//! ticket id, different repo / base / budget). To return a clean 409
//! we preflight the events table for the dedup key and compare the
//! stored payload against the new one BEFORE calling advance.

use std::sync::Arc;

use orchestrator_coding_workflow::{
    events::{TicketIngested, EVT_TICKET_INGESTED},
    WorkflowReducer,
};
use orchestrator_core::{AdvanceOutcome, Causation, EventCommand, Executor, Storage, WorkflowId};
use serde::Deserialize;
use sqlx::Row;
use tracing::{debug, instrument, warn};

/// HTTP request body for `POST /tickets`. The TicketIngested fields
/// are flattened in alongside the optional `workflow_id` override so
/// the API surface is one flat JSON object.
#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    /// Optional explicit workflow id. Defaults to
    /// `format!("{source}:{id}", ticket.source, ticket.id)`.
    /// Operators set this to spawn a fresh workflow for a ticket that
    /// already has a (possibly halted) prior workflow.
    #[serde(default)]
    pub workflow_id: Option<String>,

    #[serde(flatten)]
    pub ticket_ingested: TicketIngested,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("dedup conflict for {dedup_key}: stored payload differs from new one")]
    Conflict {
        dedup_key: String,
        stored: serde_json::Value,
        attempted: serde_json::Value,
    },

    #[error("dedup preflight query failed for {dedup_key}: {source}")]
    Preflight {
        dedup_key: String,
        #[source]
        source: Box<sqlx::Error>,
    },

    #[error("could not encode ticket payload as JSON: {0}")]
    Encode(serde_json::Error),

    #[error("executor.advance failed: {0}")]
    Advance(orchestrator_core::ExecutorError),
}

/// Outcome of a successful ingest. `Created` means the call wrote a
/// fresh `TicketIngested` event; `AlreadyExists` means the dedup key
/// matched an existing event whose payload was bit-identical (so
/// nothing new was written).
#[derive(Debug, PartialEq, Eq)]
pub enum IngestOutcome {
    Created { workflow_id: WorkflowId },
    AlreadyExists { workflow_id: WorkflowId },
}

/// Run the ingest pipeline for one request. Used by both the HTTP
/// handler and the CLI subcommand.
#[instrument(
    skip(executor, request),
    fields(
        ticket_source = %request.ticket_ingested.ticket.source,
        ticket_id = %request.ticket_ingested.ticket.id,
    ),
)]
pub async fn ingest_ticket(
    executor: &Arc<Executor<WorkflowReducer>>,
    request: IngestRequest,
) -> Result<IngestOutcome, IngestError> {
    let prepared = prepare_ingest(executor, request).await?;
    if let Some(outcome) = prepared.preflight_decision() {
        return outcome;
    }
    finalize_ingest(executor, prepared).await
}

/// Inputs for the post-preflight half of `ingest_ticket`. Visible at
/// crate scope so tests can drive the two phases separately and sneak
/// a competing write in between, deterministically triggering the
/// post-dedup branch in `finalize_ingest`.
pub(crate) struct PreparedIngest {
    workflow_id: WorkflowId,
    dedup_key: String,
    payload: serde_json::Value,
    /// Result of the preflight `check_dedup_conflict` lookup. `None`
    /// means no prior row; `Some` means the row exists and we already
    /// decided the outcome.
    preflight: Option<serde_json::Value>,
}

impl PreparedIngest {
    /// If preflight saw a row, return the classified outcome. Callers
    /// that want to skip preflight (e.g., tests exercising the
    /// post-dedup branch) construct `PreparedIngest` with
    /// `preflight = None` and call `finalize_ingest` directly.
    fn preflight_decision(&self) -> Option<Result<IngestOutcome, IngestError>> {
        self.preflight.as_ref().map(|stored| {
            classify_against_stored(
                stored.clone(),
                &self.payload,
                self.workflow_id.clone(),
                self.dedup_key.clone(),
            )
        })
    }
}

/// Phase 1: derive the workflow id, encode the payload, run preflight.
pub(crate) async fn prepare_ingest(
    executor: &Arc<Executor<WorkflowReducer>>,
    request: IngestRequest,
) -> Result<PreparedIngest, IngestError> {
    let workflow_id = request.workflow_id.clone().unwrap_or_else(|| {
        format!(
            "{}:{}",
            request.ticket_ingested.ticket.source,
            request.ticket_ingested.ticket.id,
        )
    });
    let dedup_key = workflow_id.clone();
    let payload = serde_json::to_value(&request.ticket_ingested).map_err(IngestError::Encode)?;
    let preflight =
        check_dedup_conflict(executor.storage(), &dedup_key)
            .await
            .map_err(|source| IngestError::Preflight {
                dedup_key: dedup_key.clone(),
                source: Box::new(source),
            })?;
    Ok(PreparedIngest {
        workflow_id: WorkflowId::new(workflow_id),
        dedup_key,
        payload,
        preflight,
    })
}

/// Phase 2: call `executor.advance`, handle the deduplicated flag, and
/// classify against the stored payload if we lost a race.
pub(crate) async fn finalize_ingest(
    executor: &Arc<Executor<WorkflowReducer>>,
    prepared: PreparedIngest,
) -> Result<IngestOutcome, IngestError> {
    let PreparedIngest {
        workflow_id,
        dedup_key,
        payload,
        preflight: _,
    } = prepared;

    let cmd = EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: EVT_TICKET_INGESTED.into(),
        payload_schema_version: 1,
        payload: payload.clone(),
        causation: Causation::External {
            source: "ticket_ingest".into(),
            request_id: dedup_key.clone(),
        },
        trace_id: None,
        ingress_dedup_key: Some(dedup_key.clone()),
    };
    let advance_outcome: AdvanceOutcome =
        executor.advance(cmd).await.map_err(IngestError::Advance)?;

    if advance_outcome.deduplicated {
        // Codex stop-gate round-15: a concurrent ingest beat us to the
        // insert (preflight saw no row, but `Storage::advance`'s unique
        // index on `events.ingress_dedup_key` caught the duplicate).
        // Re-query and compare so a race-loser whose payload differs
        // from the race-winner's still sees a typed Conflict — without
        // this, the loser would silently return Created.
        debug!(workflow_id = %workflow_id, "advance was deduplicated; re-checking stored payload");
        let stored = check_dedup_conflict(executor.storage(), &dedup_key)
            .await
            .map_err(|source| IngestError::Preflight {
                dedup_key: dedup_key.clone(),
                source: Box::new(source),
            })?
            .ok_or_else(|| IngestError::Preflight {
                dedup_key: dedup_key.clone(),
                source: Box::new(sqlx::Error::Protocol(
                    "advance reported deduplicated but post-write lookup found no row".into(),
                )),
            })?;
        return classify_against_stored(stored, &payload, workflow_id, dedup_key);
    }

    Ok(IngestOutcome::Created { workflow_id })
}

/// Decide between idempotent-re-post and dedup-conflict given the
/// payload that actually landed in the events table. Used by both the
/// preflight branch (caller saw the row before calling advance) and
/// the post-advance dedup branch (caller lost a race against another
/// ingester).
fn classify_against_stored(
    stored: serde_json::Value,
    our_payload: &serde_json::Value,
    workflow_id: WorkflowId,
    dedup_key: String,
) -> Result<IngestOutcome, IngestError> {
    if stored == *our_payload {
        debug!(workflow_id = %workflow_id, "ingest matches stored payload; idempotent");
        Ok(IngestOutcome::AlreadyExists { workflow_id })
    } else {
        warn!(workflow_id = %workflow_id, "ingest payload differs from stored event; rejecting");
        Err(IngestError::Conflict {
            dedup_key,
            stored,
            attempted: our_payload.clone(),
        })
    }
}

/// Look up the prior `TicketIngested` event for `dedup_key` and return
/// its payload if present. The caller compares against the new payload
/// to distinguish idempotent re-posts (matching) from configuration
/// drift (mismatch → 409).
async fn check_dedup_conflict(
    storage: &Storage,
    dedup_key: &str,
) -> Result<Option<serde_json::Value>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT payload FROM events
        WHERE ingress_dedup_key = ?
          AND payload_type = 'workflow.ticket_ingested.v1'
        LIMIT 1
        "#,
    )
    .bind(dedup_key)
    .fetch_optional(storage.pool())
    .await?;

    let Some(r) = row else {
        return Ok(None);
    };
    let stored_text: String = r.try_get("payload")?;
    let stored = serde_json::from_str(&stored_text)
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
    Ok(Some(stored))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_coding_workflow::events::{TicketIngested, TicketRef};
    use orchestrator_core::Executor;
    use orchestrator_github::RepoRef;

    async fn fixture() -> Arc<Executor<WorkflowReducer>> {
        let storage = Storage::open("sqlite::memory:").await.unwrap();
        Arc::new(Executor::new(storage, WorkflowReducer))
    }

    fn sample_ticket() -> TicketIngested {
        TicketIngested {
            ticket: TicketRef {
                source: "manual".into(),
                id: "ENG-123".into(),
            },
            repo: RepoRef {
                owner: "octo".into(),
                name: "world".into(),
            },
            base_branch: "main".into(),
            base_sha: "0123456789abcdef0123456789abcdef01234567".into(),
            cost_budget_cents: Some(100_000),
            require_architecture_review: false,
        }
    }

    #[tokio::test]
    async fn first_ingest_writes_event_and_derives_workflow_id_from_ticket() {
        let exec = fixture().await;
        let outcome = ingest_ticket(
            &exec,
            IngestRequest {
                workflow_id: None,
                ticket_ingested: sample_ticket(),
            },
        )
        .await
        .expect("first ingest must succeed");
        assert_eq!(
            outcome,
            IngestOutcome::Created {
                workflow_id: WorkflowId::new("manual:ENG-123"),
            },
        );
        let events = exec
            .storage()
            .read_events(&WorkflowId::new("manual:ENG-123"))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload_type, EVT_TICKET_INGESTED);
    }

    #[tokio::test]
    async fn re_ingest_with_identical_payload_is_idempotent() {
        let exec = fixture().await;
        let req = || IngestRequest {
            workflow_id: None,
            ticket_ingested: sample_ticket(),
        };
        let _ = ingest_ticket(&exec, req()).await.unwrap();
        let outcome = ingest_ticket(&exec, req()).await.unwrap();
        assert_eq!(
            outcome,
            IngestOutcome::AlreadyExists {
                workflow_id: WorkflowId::new("manual:ENG-123"),
            },
        );
        let events = exec
            .storage()
            .read_events(&WorkflowId::new("manual:ENG-123"))
            .await
            .unwrap();
        assert_eq!(events.len(), 1, "no second event for matching re-post");
    }

    #[tokio::test]
    async fn re_ingest_with_conflicting_payload_returns_conflict() {
        let exec = fixture().await;
        let _ = ingest_ticket(
            &exec,
            IngestRequest {
                workflow_id: None,
                ticket_ingested: sample_ticket(),
            },
        )
        .await
        .unwrap();

        let mut second = sample_ticket();
        second.base_branch = "develop".into(); // differs from first
        let err = ingest_ticket(
            &exec,
            IngestRequest {
                workflow_id: None,
                ticket_ingested: second,
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, IngestError::Conflict { ref dedup_key, .. } if dedup_key == "manual:ENG-123"),
            "got: {err:?}",
        );
        let events = exec
            .storage()
            .read_events(&WorkflowId::new("manual:ENG-123"))
            .await
            .unwrap();
        assert_eq!(events.len(), 1, "no second event for conflicting re-post");
    }

    #[tokio::test]
    async fn explicit_workflow_id_override_decouples_from_ticket_id() {
        let exec = fixture().await;
        let outcome = ingest_ticket(
            &exec,
            IngestRequest {
                workflow_id: Some("manual:ENG-123#run-2".into()),
                ticket_ingested: sample_ticket(),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            outcome,
            IngestOutcome::Created {
                workflow_id: WorkflowId::new("manual:ENG-123#run-2"),
            },
        );
        // The default-derived workflow_id has nothing in it.
        let default_events = exec
            .storage()
            .read_events(&WorkflowId::new("manual:ENG-123"))
            .await
            .unwrap();
        assert!(default_events.is_empty());
        // The override target has the event.
        let override_events = exec
            .storage()
            .read_events(&WorkflowId::new("manual:ENG-123#run-2"))
            .await
            .unwrap();
        assert_eq!(override_events.len(), 1);
    }

    #[test]
    fn classify_returns_already_exists_when_stored_matches() {
        let payload = serde_json::json!({"a": 1});
        let result = classify_against_stored(
            payload.clone(),
            &payload,
            WorkflowId::new("wf"),
            "wf".into(),
        );
        assert!(matches!(result, Ok(IngestOutcome::AlreadyExists { .. })));
    }

    #[test]
    fn classify_returns_conflict_when_stored_differs() {
        let stored = serde_json::json!({"a": 1});
        let attempted = serde_json::json!({"a": 2});
        let result = classify_against_stored(
            stored,
            &attempted,
            WorkflowId::new("wf"),
            "wf".into(),
        );
        assert!(
            matches!(result, Err(IngestError::Conflict { ref dedup_key, .. }) if dedup_key == "wf"),
            "got: {result:?}",
        );
    }

    #[tokio::test]
    async fn finalize_ingest_returns_conflict_when_advance_dedups_against_different_payload() {
        // Codex stop-gate round-16. The round-15 fix added a
        // post-dedup classification branch in `finalize_ingest`, but
        // that branch is unreachable through `ingest_ticket` alone in
        // a single-threaded test (the third call's preflight catches
        // the prior write before advance is reached). To exercise the
        // branch through the actual production path we drive the two
        // phases separately and sneak a competing write in between —
        // the race-loser's `executor.advance` then trips
        // `deduplicated = true` and our classifier runs.
        let exec = fixture().await;

        // Phase 1 of request B (the future race-loser): preflight
        // returns None because the events table is still empty.
        let req_b = IngestRequest {
            workflow_id: None,
            ticket_ingested: TicketIngested {
                base_branch: "develop".into(),
                ..sample_ticket()
            },
        };
        let prepared_b = prepare_ingest(&exec, req_b).await.unwrap();
        assert!(
            prepared_b.preflight.is_none(),
            "preflight must miss for the post-dedup branch to be reachable",
        );

        // Race winner: ingest the same ticket id with a DIFFERENT
        // payload (base_branch=main). Completes between B's preflight
        // and B's advance.
        let req_a = IngestRequest {
            workflow_id: None,
            ticket_ingested: TicketIngested {
                base_branch: "main".into(),
                ..sample_ticket()
            },
        };
        let outcome_a = ingest_ticket(&exec, req_a).await.unwrap();
        assert!(matches!(outcome_a, IngestOutcome::Created { .. }));

        // Phase 2 of request B: advance hits the unique-index dedup
        // path (because A's commit is now visible at BEGIN time),
        // returns deduplicated=true; our post-dedup re-query reads
        // payload A and the classifier rejects with Conflict.
        let outcome_b = finalize_ingest(&exec, prepared_b).await.unwrap_err();
        assert!(
            matches!(outcome_b, IngestError::Conflict { ref dedup_key, .. } if dedup_key == "manual:ENG-123"),
            "got: {outcome_b:?}",
        );

        // Only A's event landed.
        let events = exec
            .storage()
            .read_events(&WorkflowId::new("manual:ENG-123"))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload["base_branch"], "main");
    }

    #[tokio::test]
    async fn finalize_ingest_returns_already_exists_when_advance_dedups_against_identical_payload() {
        // Same setup as above but the race winner's payload matches
        // ours — so the classifier returns AlreadyExists rather than
        // Conflict. Proves both arms of the post-dedup branch.
        let exec = fixture().await;
        let req = || IngestRequest {
            workflow_id: None,
            ticket_ingested: sample_ticket(),
        };

        let prepared_b = prepare_ingest(&exec, req()).await.unwrap();
        assert!(prepared_b.preflight.is_none());

        let outcome_a = ingest_ticket(&exec, req()).await.unwrap();
        assert!(matches!(outcome_a, IngestOutcome::Created { .. }));

        let outcome_b = finalize_ingest(&exec, prepared_b).await.unwrap();
        assert!(
            matches!(outcome_b, IngestOutcome::AlreadyExists { .. }),
            "got: {outcome_b:?}",
        );

        let events = exec
            .storage()
            .read_events(&WorkflowId::new("manual:ENG-123"))
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn preflight_failure_propagates_when_pool_is_closed() {
        let exec = fixture().await;
        exec.storage().pool().close().await;
        let err = ingest_ticket(
            &exec,
            IngestRequest {
                workflow_id: None,
                ticket_ingested: sample_ticket(),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, IngestError::Preflight { .. }),
            "got: {err:?}",
        );
    }
}
