//! Webhook routing glue for the app binary.
//!
//! The orchestrator-github-webhook crate is transport-only — it parses
//! the delivery and validates the HMAC, then hands a
//! `GithubWebhookDelivery` to a closure the consumer supplies. This
//! module is that closure: it resolves `WorkflowId` from the events
//! table, builds an `EventCommand` via the workflow translator, and
//! calls `executor.advance(...)`.
//!
//! Workflow-id resolution (M13 design): we query the `events` table
//! for the `EVT_GH_PR_OPENED` event whose payload carries the matching
//! `(repo.owner, repo.name, pr_number)`. The data is already there
//! because the github sink writes that outcome event when the PR is
//! opened; no PR-body markers are involved.

use std::sync::Arc;
use std::time::{Duration, Instant};

use orchestrator_coding_workflow::{translate_github_webhook, WorkflowReducer};
use orchestrator_core::{Executor, Storage, WorkflowId};
use orchestrator_github_webhook::GithubWebhookDelivery;
use sqlx::Row;
use tracing::{debug, instrument, warn};

/// Find the `WorkflowId` whose stored `github.pr_opened.v1` event
/// matches `(owner, name, pr_number)`. Owner and name are compared
/// case-insensitively (GitHub normalizes them server-side, so a
/// user-typed `Octo/World` and the API-canonical `octo/world` describe
/// the same repository).
///
/// Returns:
/// - `Ok(Some(id))` — found a matching workflow.
/// - `Ok(None)` — no row matched. Definitively not a tracked PR.
/// - `Err(_)` — the query itself failed (DB closed, schema missing,
///   transient connection issue). Caller MUST treat this as a
///   transient failure (HTTP 500 to GitHub) so the delivery is retried —
///   silently mapping it to `Ok(None)` would conflate "no PR" with "we
///   couldn't tell" and drop real merge events.
#[instrument(skip(storage), fields(owner = %owner, name = %name, pr_number = pr_number))]
pub async fn resolve_workflow_id_from_pr(
    storage: &Storage,
    owner: &str,
    name: &str,
    pr_number: u64,
) -> Result<Option<WorkflowId>, sqlx::Error> {
    let row = sqlx::query(
        r#"
        SELECT workflow_id FROM events
        WHERE payload_type = 'github.pr_opened.v1'
          AND json_extract(payload, '$.repo.owner') = ? COLLATE NOCASE
          AND json_extract(payload, '$.repo.name')  = ? COLLATE NOCASE
          AND json_extract(payload, '$.pr_number')  = ?
        ORDER BY recorded_at DESC
        LIMIT 1
        "#,
    )
    .bind(owner)
    .bind(name)
    .bind(pr_number as i64)
    .fetch_optional(storage.pool())
    .await?;

    let Some(r) = row else {
        debug!("no PrOpened event for (owner, name, pr_number)");
        return Ok(None);
    };

    match r.try_get::<String, _>("workflow_id") {
        Ok(id) => Ok(Some(WorkflowId::new(id))),
        Err(e) => {
            // Column decode failures indicate schema corruption — surface
            // as a lookup error (transient bucket; an operator will see
            // the 500 and dig in).
            warn!(error = %e, "workflow_id column decode failed");
            Err(e)
        }
    }
}

/// Retry the workflow-id lookup until it succeeds, hits a non-recoverable
/// outcome, or the budget is exhausted. Why: GitHub does NOT auto-retry
/// failed webhook deliveries, so the recovery for a delivery that races
/// the dispatcher's `PrOpened` write must live inside this handler call.
///
/// Returns:
/// - `Ok(Some(id))` on first successful resolution.
/// - `Ok(None)` if the deadline elapses with **no errors** seen during
///   the budget AND no row found (treat as genuinely untracked).
/// - `Err(_)` if the deadline elapses and we saw at least one query
///   error during the budget (uncertainty wins — we can't distinguish
///   "tracked PR with intermittent lookup failures" from "untracked PR
///   with intermittent lookup failures", so surface 500 to the operator
///   for manual redelivery rather than silently 200 a possibly-tracked
///   merge).
pub async fn resolve_workflow_id_with_retry(
    storage: &Storage,
    owner: &str,
    name: &str,
    pr_number: u64,
    total_budget: Duration,
    backoff: Duration,
) -> Result<Option<WorkflowId>, sqlx::Error> {
    retry_until_resolved(total_budget, backoff, || {
        resolve_workflow_id_from_pr(storage, owner, name, pr_number)
    })
    .await
}

/// Generic retry-with-error-stickiness loop. Factored out so tests can
/// inject controlled probe sequences (alternating Err / Ok(None) /
/// Ok(Some)) — the real `resolve_workflow_id_from_pr` doesn't expose
/// hooks for that.
///
/// The retry is bounded by **wall-clock** elapsed time, not iteration
/// count: each `probe()` call runs under `tokio::time::timeout` of the
/// remaining budget, and the backoff sleep is capped to not push past
/// the deadline. Without this, a single slow probe (e.g., a DB query
/// hitting `busy_timeout=5000`) could keep the handler open for tens
/// of seconds even though the operator configured a tight budget.
async fn retry_until_resolved<F, Fut>(
    total_budget: Duration,
    backoff: Duration,
    mut probe: F,
) -> Result<Option<WorkflowId>, sqlx::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Option<WorkflowId>, sqlx::Error>>,
{
    let deadline = Instant::now() + total_budget;
    // Sticky: once we've seen an error, we keep it until either a real
    // resolution wins (returned immediately) or the deadline elapses
    // (we report the error as the conservative outcome — see docs on
    // resolve_workflow_id_with_retry).
    let mut sticky_error: Option<sqlx::Error> = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return match sticky_error {
                Some(e) => Err(e),
                None => Ok(None),
            };
        }
        match tokio::time::timeout(remaining, probe()).await {
            Ok(Ok(Some(id))) => return Ok(Some(id)),
            Ok(Ok(None)) => {}
            Ok(Err(e)) => sticky_error = Some(e),
            Err(_elapsed) => {
                // Probe didn't finish within the remaining budget.
                // Treat as a "couldn't tell" outcome (Err) so the
                // handler returns 500 — silent Ok(None) here would
                // mask a possibly-tracked merge.
                let synthetic = sqlx::Error::Protocol(format!(
                    "workflow lookup exceeded retry budget of {}ms",
                    total_budget.as_millis(),
                ));
                return Err(sticky_error.unwrap_or(synthetic));
            }
        }
        // Cap the backoff so it never pushes us past the deadline —
        // otherwise a 200ms backoff right before a 100ms-remaining
        // deadline would silently extend the handler by 100ms.
        let until_deadline = deadline.saturating_duration_since(Instant::now());
        if until_deadline.is_zero() {
            return match sticky_error {
                Some(e) => Err(e),
                None => Ok(None),
            };
        }
        tokio::time::sleep(backoff.min(until_deadline)).await;
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HandleDeliveryError {
    /// The delivery isn't an event we react to (wrong type, wrong action,
    /// or PR closed without merge). Webhook handler returns 200 anyway —
    /// from GitHub's perspective the delivery was accepted.
    #[error("event ignored (not a tracked workflow event type)")]
    Ignored,

    /// Lookup query failed (DB closed, schema missing, transient
    /// connection issue). Caller MUST surface as 500 so GitHub retries —
    /// distinct from `UnresolvedWorkflow`, which is "definitely no
    /// workflow" rather than "we couldn't tell".
    #[error("workflow lookup failed for delivery {delivery_id}: {source}")]
    Lookup {
        delivery_id: String,
        #[source]
        source: Box<sqlx::Error>,
    },

    /// No workflow matched this delivery's `(repo, pr_number)`. The PR
    /// was opened outside the orchestrator (or its PrOpened event was
    /// never recorded). Webhook handler returns 200 — retrying won't
    /// change the answer.
    #[error("workflow_id unresolved for delivery {delivery_id} ({owner}/{name}#{pr_number})")]
    UnresolvedWorkflow {
        delivery_id: String,
        owner: String,
        name: String,
        pr_number: u64,
    },

    #[error("executor.advance failed: {0}")]
    Advance(orchestrator_core::ExecutorError),
}

/// Run the full webhook handling pipeline for one delivery: pre-filter
/// non-merge events, retry the workflow-id lookup until the supplied
/// budget elapses, translate the delivery to an `EventCommand`, and
/// advance the executor. The budget + backoff are owned by config
/// (`[server.webhook]`) and threaded all the way through the closure
/// the router invokes; tests use small values via `with_budget` so
/// they don't sit through 5 production seconds.
#[instrument(
    skip(executor, delivery),
    fields(event_type = %delivery.event_type, delivery_id = %delivery.delivery_id),
)]
pub async fn handle_delivery_with_budget(
    executor: &Arc<Executor<WorkflowReducer>>,
    delivery: &GithubWebhookDelivery,
    retry_budget: Duration,
    retry_backoff: Duration,
) -> Result<(), HandleDeliveryError> {
    // Quick pre-filter so we don't pay for a storage lookup on events
    // the translator would ignore. Mirrors translate_github_webhook's
    // own filter exactly — keep them in sync if the translator grows
    // new event types.
    if delivery.event_type != "pull_request"
        || delivery.action.as_deref() != Some("closed")
        || delivery.payload["pull_request"]["merged"].as_bool() != Some(true)
    {
        return Err(HandleDeliveryError::Ignored);
    }

    let owner = delivery.payload["repository"]["owner"]["login"]
        .as_str()
        .ok_or(HandleDeliveryError::Ignored)?
        .to_string();
    let name = delivery.payload["repository"]["name"]
        .as_str()
        .ok_or(HandleDeliveryError::Ignored)?
        .to_string();
    let pr_number = delivery.payload["pull_request"]["number"]
        .as_u64()
        .ok_or(HandleDeliveryError::Ignored)?;

    let workflow_id = match resolve_workflow_id_with_retry(
        executor.storage(),
        &owner,
        &name,
        pr_number,
        retry_budget,
        retry_backoff,
    )
    .await
    {
        Ok(Some(id)) => id,
        Ok(None) => {
            return Err(HandleDeliveryError::UnresolvedWorkflow {
                delivery_id: delivery.delivery_id.clone(),
                owner,
                name,
                pr_number,
            });
        }
        Err(source) => {
            return Err(HandleDeliveryError::Lookup {
                delivery_id: delivery.delivery_id.clone(),
                source: Box::new(source),
            });
        }
    };

    // The translator's resolver closure now has nothing to do — we
    // already know the workflow_id. The translator still does the
    // PrMerged payload construction and ingress_dedup_key wiring.
    let cmd = translate_github_webhook(delivery, |_| Some(workflow_id.clone()))
        .ok_or(HandleDeliveryError::Ignored)?;

    executor
        .advance(cmd)
        .await
        .map(|_| ())
        .map_err(HandleDeliveryError::Advance)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_coding_workflow::WorkflowReducer;
    use orchestrator_core::Executor;
    use serde_json::json;

    async fn fixture() -> Arc<Executor<WorkflowReducer>> {
        let storage = Storage::open("sqlite::memory:").await.unwrap();
        Arc::new(Executor::new(storage, WorkflowReducer))
    }

    /// A scripted probe that returns a pre-recorded sequence of
    /// `Result<Option<WorkflowId>, sqlx::Error>` values, one per call,
    /// then repeats the last entry for any further calls. Lets us
    /// drive `retry_until_resolved` through controlled interleavings
    /// of Err / Ok(None) / Ok(Some) without standing up a real DB.
    struct ScriptedProbe {
        responses: std::sync::Mutex<std::vec::IntoIter<Result<Option<WorkflowId>, sqlx::Error>>>,
        last: std::sync::Mutex<Option<Result<Option<WorkflowId>, sqlx::Error>>>,
    }

    impl ScriptedProbe {
        fn new(seq: Vec<Result<Option<WorkflowId>, sqlx::Error>>) -> Self {
            Self {
                responses: std::sync::Mutex::new(seq.into_iter()),
                last: std::sync::Mutex::new(None),
            }
        }

        fn next(&self) -> Result<Option<WorkflowId>, sqlx::Error> {
            let next = self.responses.lock().unwrap().next();
            match next {
                Some(r) => {
                    *self.last.lock().unwrap() = Some(clone_result(&r));
                    r
                }
                None => clone_result(self.last.lock().unwrap().as_ref().unwrap()),
            }
        }
    }

    fn clone_result(
        r: &Result<Option<WorkflowId>, sqlx::Error>,
    ) -> Result<Option<WorkflowId>, sqlx::Error> {
        match r {
            Ok(opt) => Ok(opt.clone()),
            // sqlx::Error isn't Clone; rebuild a shallow proxy with the
            // same Display rendering so test assertions keep working.
            Err(e) => Err(sqlx::Error::Protocol(e.to_string())),
        }
    }

    async fn run_retry(
        probe: ScriptedProbe,
        budget: Duration,
        backoff: Duration,
    ) -> Result<Option<WorkflowId>, sqlx::Error> {
        let probe = std::sync::Arc::new(probe);
        retry_until_resolved(budget, backoff, || {
            let probe = probe.clone();
            async move { probe.next() }
        })
        .await
    }

    #[tokio::test]
    async fn retry_returns_immediately_when_resolved_on_first_call() {
        let probe = ScriptedProbe::new(vec![Ok(Some(WorkflowId::new("wf-x")))]);
        let started = Instant::now();
        let out = run_retry(probe, Duration::from_secs(60), Duration::from_millis(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out.as_str(), "wf-x");
        assert!(started.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn retry_returns_ok_none_when_every_attempt_was_clean_no_row() {
        // Pure no-row sequence; deadline elapses; truly untracked.
        let probe = ScriptedProbe::new(vec![Ok(None)]);
        let r = run_retry(probe, Duration::from_millis(50), Duration::from_millis(10)).await;
        assert!(matches!(r, Ok(None)), "got: {r:?}");
    }

    #[tokio::test]
    async fn retry_surfaces_error_if_any_attempt_errored_even_when_last_was_ok_none() {
        // Codex stop-gate round-10 regression: a flapping DB whose
        // budget happens to end on Ok(None) used to be reported as
        // Ok(None) → 200, silently dropping a possibly-tracked merge.
        // Now we surface Err → 500 so the operator can redeliver.
        let probe = ScriptedProbe::new(vec![
            Err(sqlx::Error::Protocol("transient hiccup".into())),
            Ok(None),
            Ok(None),
        ]);
        let r = run_retry(probe, Duration::from_millis(50), Duration::from_millis(10)).await;
        assert!(r.is_err(), "got: {r:?}");
    }

    #[tokio::test]
    async fn retry_returns_within_budget_even_when_probe_blocks() {
        // Codex stop-gate round-19 regression: a probe that takes longer
        // than the budget would previously run to completion before the
        // deadline check fired, blowing the configured bound. With the
        // probe wrapped in `tokio::time::timeout(remaining, ...)`, the
        // helper aborts the slow probe and returns Err immediately at
        // the deadline.
        let probe = || async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<Option<WorkflowId>, sqlx::Error>(None)
        };
        let started = Instant::now();
        let result = retry_until_resolved(
            Duration::from_millis(50),
            Duration::from_millis(10),
            probe,
        )
        .await;
        let elapsed = started.elapsed();
        assert!(result.is_err(), "got: {result:?}");
        // Generous upper bound — slow CI shouldn't flake but the test
        // still proves the slow probe was aborted (not waited out for
        // its full 60s sleep).
        assert!(
            elapsed < Duration::from_secs(1),
            "retry took {elapsed:?}; should have aborted near the 50ms budget",
        );
    }

    #[tokio::test]
    async fn retry_succeeds_when_resolution_arrives_after_initial_errors() {
        // Race scenario with a flaky DB: errors first, then no-row,
        // then resolution. Should succeed once Ok(Some) wins.
        let probe = ScriptedProbe::new(vec![
            Err(sqlx::Error::Protocol("first".into())),
            Ok(None),
            Ok(Some(WorkflowId::new("wf-recovered"))),
        ]);
        let r = run_retry(probe, Duration::from_millis(500), Duration::from_millis(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.as_str(), "wf-recovered");
    }

    /// Insert a synthetic PrOpened event directly into the events
    /// table. Bypasses the reducer (which would reject the event
    /// without prior workflow state); fine for testing the resolver
    /// query in isolation.
    async fn insert_pr_opened(
        storage: &Storage,
        workflow_id: &str,
        owner: &str,
        name: &str,
        pr_number: u64,
    ) {
        let payload = json!({
            "action_id": "test-action",
            "repo": { "owner": owner, "name": name },
            "pr_number": pr_number,
            "html_url": format!("https://github.com/{owner}/{name}/pull/{pr_number}"),
            "state": "open",
        });
        sqlx::query(
            r#"
            INSERT INTO events (
                workflow_id, sequence, event_id, recorded_at,
                payload_type, payload_schema_version,
                causation_kind, causation_ref, payload, ingress_dedup_key
            ) VALUES (?, ?, ?, ?, 'github.pr_opened.v1', 1, 'system', NULL, ?, NULL)
            "#,
        )
        .bind(workflow_id)
        .bind(0_i64)
        .bind(format!("ev-{workflow_id}-{pr_number}"))
        .bind("2026-01-01T00:00:00Z")
        .bind(payload.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn returns_workflow_id_for_matching_pr() {
        let exec = fixture().await;
        insert_pr_opened(exec.storage(), "wf-1", "octo", "world", 42).await;
        let id = resolve_workflow_id_from_pr(exec.storage(), "octo", "world", 42)
            .await
            .expect("query must succeed")
            .expect("must find workflow");
        assert_eq!(id.as_str(), "wf-1");
    }

    #[tokio::test]
    async fn matches_repo_owner_and_name_case_insensitively() {
        let exec = fixture().await;
        insert_pr_opened(exec.storage(), "wf-1", "octo", "world", 42).await;
        let id = resolve_workflow_id_from_pr(exec.storage(), "Octo", "World", 42)
            .await
            .expect("query must succeed")
            .expect("case-insensitive owner/name must match");
        assert_eq!(id.as_str(), "wf-1");
    }

    #[tokio::test]
    async fn returns_ok_none_for_different_repo() {
        let exec = fixture().await;
        insert_pr_opened(exec.storage(), "wf-1", "octo", "world", 42).await;
        assert!(matches!(
            resolve_workflow_id_from_pr(exec.storage(), "evil", "world", 42).await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn returns_ok_none_for_different_pr_number() {
        let exec = fixture().await;
        insert_pr_opened(exec.storage(), "wf-1", "octo", "world", 42).await;
        assert!(matches!(
            resolve_workflow_id_from_pr(exec.storage(), "octo", "world", 7).await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn returns_ok_none_when_no_pr_opened_event_exists() {
        let exec = fixture().await;
        assert!(matches!(
            resolve_workflow_id_from_pr(exec.storage(), "octo", "world", 42).await,
            Ok(None)
        ));
    }

    #[tokio::test]
    async fn returns_err_when_pool_is_closed() {
        // Closing the pool simulates a transient infra failure (DB
        // restart, connection limit, etc.). The resolver MUST propagate
        // the error so the webhook handler returns 500 and GitHub
        // retries — silently mapping it to None would drop a real merge.
        let exec = fixture().await;
        exec.storage().pool().close().await;
        let result = resolve_workflow_id_from_pr(exec.storage(), "octo", "world", 42).await;
        assert!(result.is_err(), "got: {result:?}");
    }

    fn pr_merged_delivery(delivery_id: &str, owner: &str, name: &str, pr_number: u64) -> GithubWebhookDelivery {
        GithubWebhookDelivery {
            event_type: "pull_request".into(),
            delivery_id: delivery_id.into(),
            action: Some("closed".into()),
            payload: json!({
                "action": "closed",
                "pull_request": {
                    "number": pr_number,
                    "merged": true,
                    "merge_commit_sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                },
                "repository": {
                    "owner": { "login": owner },
                    "name": name,
                },
            }),
        }
    }

    /// Tight budget so tests don't wait 5 production seconds.
    const TEST_BUDGET: Duration = Duration::from_millis(50);
    const TEST_BACKOFF: Duration = Duration::from_millis(10);

    #[tokio::test]
    async fn handle_delivery_advances_workflow_for_resolved_pr() {
        let exec = fixture().await;
        insert_pr_opened(exec.storage(), "wf-1", "octo", "world", 42).await;

        let delivery = pr_merged_delivery("delivery-test-1", "octo", "world", 42);
        handle_delivery_with_budget(&exec, &delivery, TEST_BUDGET, TEST_BACKOFF)
            .await
            .expect("delivery for resolvable PR must advance");

        let events = exec
            .storage()
            .read_events(&WorkflowId::new("wf-1"))
            .await
            .unwrap();
        assert_eq!(events.len(), 2, "PrOpened seed + PrMerged");
        assert_eq!(events[1].payload_type, "github.pr_merged.v1");

        // Operator-driven redelivery (GitHub does not auto-retry, but
        // a manual redeliver via `POST /app/hook/deliveries/{id}/attempts`
        // produces the same delivery_id) is dedup'd by delivery_id via
        // the events.ingress_dedup_key unique index.
        handle_delivery_with_budget(&exec, &delivery, TEST_BUDGET, TEST_BACKOFF)
            .await
            .expect("redelivery must succeed (dedup hit returns success)");
        let events = exec
            .storage()
            .read_events(&WorkflowId::new("wf-1"))
            .await
            .unwrap();
        assert_eq!(events.len(), 2, "redelivery must dedup");
    }

    #[tokio::test]
    async fn handle_delivery_returns_unresolved_for_unknown_pr_after_retry_budget() {
        let exec = fixture().await;
        let delivery = pr_merged_delivery("d2", "nobody", "noproject", 999);
        let started = Instant::now();
        let err = handle_delivery_with_budget(&exec, &delivery, TEST_BUDGET, TEST_BACKOFF)
            .await
            .unwrap_err();
        assert!(
            matches!(err, HandleDeliveryError::UnresolvedWorkflow { .. }),
            "got: {err:?}"
        );
        // Sanity: the handler did spend (at least) the retry budget
        // before giving up — the race recovery path has a chance to
        // catch a slow PrOpened write.
        assert!(
            started.elapsed() >= TEST_BUDGET,
            "handler returned in {:?}; expected to consume the retry budget",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn handle_delivery_recovers_when_pr_opened_lands_during_retry_window() {
        // Codex stop-gate round-9 regression: GitHub does not auto-retry
        // failed deliveries, so the in-handler retry must catch the
        // open-then-merge race. We spawn the handler with a generous
        // budget and concurrently insert PrOpened after a short delay;
        // the handler should resolve mid-flight and advance.
        let exec = fixture().await;
        let delivery = pr_merged_delivery("d-race", "octo", "world", 42);

        let exec_inserter = exec.clone();
        let inserter = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            insert_pr_opened(exec_inserter.storage(), "wf-race", "octo", "world", 42).await;
        });

        let outcome = handle_delivery_with_budget(
            &exec,
            &delivery,
            Duration::from_millis(500),
            Duration::from_millis(20),
        )
        .await;
        inserter.await.unwrap();
        outcome.expect("handler must resolve once PrOpened lands");

        let events = exec
            .storage()
            .read_events(&WorkflowId::new("wf-race"))
            .await
            .unwrap();
        assert_eq!(events.len(), 2, "PrOpened seed (delayed) + PrMerged");
    }

    #[tokio::test]
    async fn handle_delivery_ignores_pull_request_closed_without_merge() {
        let exec = fixture().await;
        insert_pr_opened(exec.storage(), "wf-1", "octo", "world", 42).await;

        let delivery = GithubWebhookDelivery {
            event_type: "pull_request".into(),
            delivery_id: "d3".into(),
            action: Some("closed".into()),
            payload: json!({
                "action": "closed",
                "pull_request": { "number": 42, "merged": false },
                "repository": { "owner": { "login": "octo" }, "name": "world" },
            }),
        };
        let err = handle_delivery_with_budget(&exec, &delivery, TEST_BUDGET, TEST_BACKOFF)
            .await
            .unwrap_err();
        assert!(matches!(err, HandleDeliveryError::Ignored), "got: {err:?}");
    }

    #[tokio::test]
    async fn handle_delivery_surfaces_lookup_failure_when_pool_is_closed() {
        let exec = fixture().await;
        exec.storage().pool().close().await;
        let delivery = pr_merged_delivery("d-lookup-err", "octo", "world", 42);
        let err = handle_delivery_with_budget(&exec, &delivery, TEST_BUDGET, TEST_BACKOFF)
            .await
            .unwrap_err();
        assert!(
            matches!(err, HandleDeliveryError::Lookup { .. }),
            "got: {err:?}"
        );
    }

    #[tokio::test]
    async fn handle_delivery_ignores_unrelated_event_types() {
        let exec = fixture().await;
        let delivery = GithubWebhookDelivery {
            event_type: "issue_comment".into(),
            delivery_id: "d4".into(),
            action: Some("created".into()),
            payload: json!({}),
        };
        let err = handle_delivery_with_budget(&exec, &delivery, TEST_BUDGET, TEST_BACKOFF)
            .await
            .unwrap_err();
        assert!(matches!(err, HandleDeliveryError::Ignored), "got: {err:?}");
    }
}
