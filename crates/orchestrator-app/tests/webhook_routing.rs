//! Integration: drive the production webhook router end-to-end via
//! `tower::ServiceExt::oneshot`. No real network bind. Verifies that a
//! signed `pull_request.closed{merged:true}` delivery for a known PR
//! resolves to the correct workflow and appends a `PrMerged` event.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use orchestrator_app::server::build_webhook_router;
use orchestrator_coding_workflow::WorkflowReducer;
use orchestrator_core::test_support::{fresh_storage, DbGuard};
use orchestrator_core::{Executor, WorkflowId};
use serde_json::json;
use sha2::Sha256;
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

    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF);

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
    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF);

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
    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF);
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
    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF);

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
    let router = build_webhook_router(SECRET.into(), exec.clone(), TEST_BUDGET, TEST_BACKOFF);

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
