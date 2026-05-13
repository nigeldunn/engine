//! Integration: drive the production webhook router end-to-end via
//! `tower::ServiceExt::oneshot`. No real network bind. Verifies that a
//! signed `pull_request.closed{merged:true}` delivery for a known PR
//! resolves to the correct workflow and appends a `PrMerged` event.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use orchestrator_app::server::{build_health_router, build_webhook_router};
use orchestrator_coding_workflow::WorkflowReducer;
use orchestrator_core::test_support::{fresh_storage, DbGuard};
use orchestrator_core::{Executor, WorkflowId};
use serde_json::json;
use sha2::Sha256;
use tokio::sync::Notify;
use tower::ServiceExt;

type HmacSha256 = Hmac<Sha256>;

const SECRET: &str = "shared-secret-for-tests";

/// Tight retry budget so tests don't sit through the production 5s
/// budget. Backoff equally short — we just need at least one extra
/// attempt to demonstrate the loop runs.
/// Budget sized for Postgres connection-acquire + first-query overhead
/// on a fresh per-test database (~50–100ms in practice). Still fast.
const TEST_BUDGET: Duration = Duration::from_millis(500);
const TEST_BACKOFF: Duration = Duration::from_millis(20);

fn signed_post(
    secret: &str,
    event_type: &str,
    delivery_id: &str,
    body: Vec<u8>,
) -> Request<Body> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(&body);
    let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    Request::builder()
        .uri("/")
        .method("POST")
        .header("x-github-event", event_type)
        .header("x-github-delivery", delivery_id)
        .header("x-hub-signature-256", sig)
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

async fn fixture() -> (Arc<Executor<WorkflowReducer>>, DbGuard) {
    let (storage, db) = fresh_storage().await;
    (Arc::new(Executor::new(storage, WorkflowReducer)), db)
}

use orchestrator_core::test_support::insert_pr_opened_event as insert_pr_opened;

#[tokio::test]
async fn signed_pull_request_merged_webhook_appends_pr_merged_event() {
    let (exec, _db) = fixture().await;
    insert_pr_opened(exec.storage(), "wf-route-1", "octo", "world", 42).await;

    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF, Arc::new(Notify::new()));

    let body = serde_json::to_vec(&json!({
        "action": "closed",
        "pull_request": {
            "number": 42,
            "merged": true,
            "merge_commit_sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        },
        "repository": {
            "owner": { "login": "octo" },
            "name": "world",
        },
    }))
    .unwrap();

    let req = signed_post(SECRET, "pull_request", "delivery-merged-1", body);
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let events = exec
        .storage()
        .read_events(&WorkflowId::new("wf-route-1"))
        .await
        .unwrap();
    assert_eq!(events.len(), 2, "PrOpened seed + PrMerged from webhook");
    assert_eq!(events[1].payload_type, "github.pr_merged.v1");
    assert_eq!(
        events[1].payload["merge_commit_sha"].as_str(),
        Some("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
    );
}

/// Stage-A wake contract at the HTTP boundary: a successful PR-merged
/// delivery must fire `wake.notify_one()` (so the dispatcher claims the
/// reducer-derived next action without waiting out `poll_interval`); an
/// ignored delivery (closed-without-merge) must NOT fire wake — no event
/// was written. Codex Stage-A re-review WARN: the dispatcher-level
/// latency test in `orchestrator-core` only proves the select! reacts to
/// a wake — it doesn't exercise the production handler closure.
#[tokio::test]
async fn webhook_handler_fires_wake_on_merged_but_not_on_ignored() {
    let (exec, _db) = fixture().await;
    insert_pr_opened(exec.storage(), "wf-route-wake", "octo", "world", 7).await;

    let wake = Arc::new(Notify::new());
    let router = build_webhook_router(
        SECRET.into(),
        exec.clone(),
        TEST_BUDGET,
        TEST_BACKOFF,
        wake.clone(),
    );

    // Merged delivery → handler writes PrMerged + must fire wake.
    let merged_body = serde_json::to_vec(&json!({
        "action": "closed",
        "pull_request": {
            "number": 7,
            "merged": true,
            "merge_commit_sha": "feedfacefeedfacefeedfacefeedfacefeedface",
        },
        "repository": { "owner": { "login": "octo" }, "name": "world" },
    }))
    .unwrap();
    let req = signed_post(SECRET, "pull_request", "delivery-wake-merged", merged_body);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // notify_one fires synchronously inside the handler closure before the
    // 200 is returned, so a permit is already stored by the time we poll.
    tokio::time::timeout(Duration::from_millis(50), wake.notified())
        .await
        .expect("merged delivery must store a wake permit");

    // Closed-but-not-merged delivery is HandleDeliveryError::Ignored —
    // no event written, no wake. Permit consumed above, so an additional
    // notify would unblock the next `notified()`; assert the opposite.
    let ignored_body = serde_json::to_vec(&json!({
        "action": "closed",
        "pull_request": {
            "number": 7,
            "merged": false,
        },
        "repository": { "owner": { "login": "octo" }, "name": "world" },
    }))
    .unwrap();
    let req = signed_post(SECRET, "pull_request", "delivery-wake-ignored", ignored_body);
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let res = tokio::time::timeout(Duration::from_millis(50), wake.notified()).await;
    assert!(
        res.is_err(),
        "ignored delivery must NOT fire wake (no event was written)"
    );
}

#[tokio::test]
async fn webhook_for_truly_untracked_pr_returns_200_after_retry_budget() {
    // Codex stop-gate round-9: GitHub does NOT auto-retry failed
    // deliveries, so 500 here would just mark the delivery failed in
    // the GitHub UI without any recovery path. Instead, the handler
    // retries the workflow lookup for the configured budget (5s in
    // production, TEST_BUDGET=50ms here) to absorb the open-then-merge
    // race window between `open_pr.execute` returning success and
    // `executor.advance` writing the PrOpened event. After the budget
    // is exhausted with no resolution, we treat the PR as genuinely
    // untracked and 200 the delivery.
    let (exec, _db) = fixture().await;
    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF, Arc::new(Notify::new()));

    let body = serde_json::to_vec(&json!({
        "action": "closed",
        "pull_request": {
            "number": 99,
            "merged": true,
            "merge_commit_sha": "0".repeat(40),
        },
        "repository": { "owner": { "login": "nobody" }, "name": "noproject" },
    }))
    .unwrap();
    let req = signed_post(SECRET, "pull_request", "delivery-orphan-1", body);
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Sanity: nothing got written for the orphan PR.
    let events = exec
        .storage()
        .read_events(&WorkflowId::new("nobody/noproject"))
        .await
        .unwrap();
    assert!(events.is_empty());
}

#[tokio::test]
async fn webhook_with_bad_signature_returns_403() {
    let (exec, _db) = fixture().await;
    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF, Arc::new(Notify::new()));
    let body = b"{}".to_vec();
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("x-github-event", "pull_request")
        .header("x-github-delivery", "d-bad")
        .header("x-hub-signature-256", "sha256=00")
        .body(Body::from(body))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn webhook_returns_500_when_workflow_lookup_fails() {
    // Closing the pool forces the resolver query to fail. The handler
    // MUST surface this as 500 so GitHub retries — silently 200ing it
    // (the bug Codex flagged) would drop a real merge event.
    let (exec, _db) = fixture().await;
    exec.storage().close().await;
    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF, Arc::new(Notify::new()));

    let body = serde_json::to_vec(&json!({
        "action": "closed",
        "pull_request": {
            "number": 42,
            "merged": true,
            "merge_commit_sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        },
        "repository": { "owner": { "login": "octo" }, "name": "world" },
    }))
    .unwrap();
    let req = signed_post(SECRET, "pull_request", "delivery-lookup-fail", body);
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn webhook_for_pull_request_closed_without_merge_is_ignored() {
    let (exec, _db) = fixture().await;
    insert_pr_opened(exec.storage(), "wf-route-2", "octo", "world", 42).await;
    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF, Arc::new(Notify::new()));

    let body = serde_json::to_vec(&json!({
        "action": "closed",
        "pull_request": { "number": 42, "merged": false },
        "repository": { "owner": { "login": "octo" }, "name": "world" },
    }))
    .unwrap();
    let req = signed_post(SECRET, "pull_request", "delivery-not-merged", body);
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Only the seed event remains.
    let events = exec
        .storage()
        .read_events(&WorkflowId::new("wf-route-2"))
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
}

/// Stage-C contract: `GET /healthz` returns 200 OK without touching any
/// dispatcher or storage state. Built from `build_health_router()` alone
/// — no `Executor` or `Storage` is ever in scope, so this test
/// structurally proves the route cannot hit Postgres. ECS / ALB /
/// container health probes rely on this to keep Aurora paused at idle.
#[tokio::test]
async fn healthz_returns_200_without_touching_storage() {
    let router = build_health_router();
    let req = Request::builder()
        .uri("/healthz")
        .method("GET")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Verifies that an unsigned POST to `/healthz` is rejected with 405
/// rather than reaching a handler that might leak state. Cheap guard so
/// a future maintainer doesn't accidentally widen the route to `any()`.
#[tokio::test]
async fn healthz_rejects_non_get() {
    let router = build_health_router();
    let req = Request::builder()
        .uri("/healthz")
        .method("POST")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}
