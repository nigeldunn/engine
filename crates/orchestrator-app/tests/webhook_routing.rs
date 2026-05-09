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
use orchestrator_core::{Executor, Storage, WorkflowId};
use serde_json::json;
use sha2::Sha256;
use tower::ServiceExt;

type HmacSha256 = Hmac<Sha256>;

const SECRET: &str = "shared-secret-for-tests";

/// Tight retry budget so tests don't sit through the production 5s
/// budget. Backoff equally short — we just need at least one extra
/// attempt to demonstrate the loop runs.
const TEST_BUDGET: Duration = Duration::from_millis(50);
const TEST_BACKOFF: Duration = Duration::from_millis(10);

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

async fn fixture() -> Arc<Executor<WorkflowReducer>> {
    let storage = Storage::open("sqlite::memory:").await.unwrap();
    Arc::new(Executor::new(storage, WorkflowReducer))
}

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
async fn signed_pull_request_merged_webhook_appends_pr_merged_event() {
    let exec = fixture().await;
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
    let exec = fixture().await;
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
    let exec = fixture().await;
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
    let exec = fixture().await;
    exec.storage().pool().close().await;
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
    let exec = fixture().await;
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
