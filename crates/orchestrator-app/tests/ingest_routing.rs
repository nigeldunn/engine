//! Integration tests for the `POST /tickets` router. tower::oneshot
//! drives the production handler without binding a port.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use orchestrator_app::server::build_ingest_router;
use orchestrator_coding_workflow::WorkflowReducer;
use orchestrator_core::test_support::{fresh_storage, DbGuard};
use orchestrator_core::{Executor, WorkflowId};
use serde_json::json;
use tokio::sync::Notify;
use tower::ServiceExt;

const BEARER: &str = "secret-test-token";

async fn fixture() -> (Arc<Executor<WorkflowReducer>>, DbGuard) {
    let (storage, db) = fresh_storage().await;
    (Arc::new(Executor::new(storage, WorkflowReducer)), db)
}

fn ticket_body(base_branch: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "ticket": { "source": "manual", "id": "ENG-123" },
        "repo": { "owner": "octo", "name": "world" },
        "base_branch": base_branch,
        "base_sha": "0123456789abcdef0123456789abcdef01234567",
        "cost_budget_cents": 100_000_u64,
    }))
    .unwrap()
}

fn post_request(token: Option<&str>, body: Vec<u8>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/tickets")
        .header("content-type", "application/json");
    if let Some(t) = token {
        builder = builder.header("authorization", format!("Bearer {t}"));
    }
    builder.body(Body::from(body)).unwrap()
}

#[tokio::test]
async fn first_post_creates_workflow_and_returns_201() {
    let (exec, _db) = fixture().await;
    let router = build_ingest_router(Some(BEARER.into()), exec.clone(), Arc::new(Notify::new()));

    let resp = router
        .oneshot(post_request(Some(BEARER), ticket_body("main")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let body_bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["workflow_id"], "manual:ENG-123");
    assert_eq!(body["status"], "created");

    let events = exec
        .storage()
        .read_events(&WorkflowId::new("manual:ENG-123"))
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload_type, "workflow.ticket_ingested.v1");
}

#[tokio::test]
async fn re_post_with_identical_payload_returns_200_already_exists() {
    let (exec, _db) = fixture().await;
    let router = build_ingest_router(None, exec.clone(), Arc::new(Notify::new())); // no auth — loopback

    let r1 = router
        .clone()
        .oneshot(post_request(None, ticket_body("main")))
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::CREATED);

    let r2 = router
        .oneshot(post_request(None, ticket_body("main")))
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let body_bytes = to_bytes(r2.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["status"], "already_exists");

    // Only one event in the log.
    let events = exec
        .storage()
        .read_events(&WorkflowId::new("manual:ENG-123"))
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
}

/// Stage-A wake contract at the HTTP boundary: `Created` ingests must
/// fire `wake.notify_one()` so the dispatcher claims without waiting out
/// `poll_interval`; `AlreadyExists` (no event written) must NOT fire wake.
/// Codex Stage-A re-review WARN: the latency test in `orchestrator-core`
/// only proves the dispatcher's select! reacts to a wake — it doesn't
/// exercise the production handler closure. This test closes that gap.
#[tokio::test]
async fn ingest_handler_fires_wake_on_created_but_not_already_exists() {
    use std::time::Duration;
    let (exec, _db) = fixture().await;
    let wake = Arc::new(Notify::new());
    let router = build_ingest_router(None, exec.clone(), wake.clone());

    // First POST returns 201 Created — handler must store a wake permit.
    let r1 = router
        .clone()
        .oneshot(post_request(None, ticket_body("main")))
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::CREATED);

    // The handler calls `notify_one` synchronously before returning, so
    // by the time we observe the 201 a permit is already stored. A short
    // timeout is enough; we just want to fail fast if wake was missed.
    tokio::time::timeout(Duration::from_millis(50), wake.notified())
        .await
        .expect("Created ingest must store a wake permit");

    // Re-POST returns 200 AlreadyExists — no event written, no wake. We
    // just consumed the prior permit, so any new permit would unblock
    // the next `notified()` immediately. Assert the opposite: it times
    // out, proving the handler did NOT fire wake on the dedup path.
    let r2 = router
        .oneshot(post_request(None, ticket_body("main")))
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::OK);
    let res = tokio::time::timeout(Duration::from_millis(50), wake.notified()).await;
    assert!(
        res.is_err(),
        "AlreadyExists must NOT fire wake (no event was written)"
    );
}

#[tokio::test]
async fn re_post_with_conflicting_payload_returns_409() {
    let (exec, _db) = fixture().await;
    let router = build_ingest_router(None, exec.clone(), Arc::new(Notify::new()));

    let r1 = router
        .clone()
        .oneshot(post_request(None, ticket_body("main")))
        .await
        .unwrap();
    assert_eq!(r1.status(), StatusCode::CREATED);

    // Same ticket id, different base_branch → dedup conflict.
    let r2 = router
        .oneshot(post_request(None, ticket_body("develop")))
        .await
        .unwrap();
    assert_eq!(r2.status(), StatusCode::CONFLICT);
    let body_bytes = to_bytes(r2.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["error"], "dedup_conflict");
    assert_eq!(body["dedup_key"], "manual:ENG-123");

    // Still only one event.
    let events = exec
        .storage()
        .read_events(&WorkflowId::new("manual:ENG-123"))
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn missing_bearer_token_returns_401_when_auth_configured() {
    let (exec, _db) = fixture().await;
    let router = build_ingest_router(Some(BEARER.into()), exec.clone(), Arc::new(Notify::new()));

    let resp = router
        .oneshot(post_request(None, ticket_body("main")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let events = exec
        .storage()
        .read_events(&WorkflowId::new("manual:ENG-123"))
        .await
        .unwrap();
    assert!(events.is_empty(), "auth failure must not write events");
}

#[tokio::test]
async fn wrong_bearer_token_returns_401() {
    let (exec, _db) = fixture().await;
    let router = build_ingest_router(Some(BEARER.into()), exec.clone(), Arc::new(Notify::new()));

    let resp = router
        .oneshot(post_request(Some("wrong-token"), ticket_body("main")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn malformed_json_returns_400() {
    let (exec, _db) = fixture().await;
    let router = build_ingest_router(None, exec.clone(), Arc::new(Notify::new()));

    let req = Request::builder()
        .method("POST")
        .uri("/tickets")
        .header("content-type", "application/json")
        .body(Body::from("not json at all"))
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn workflow_id_override_decouples_from_ticket_id() {
    let (exec, _db) = fixture().await;
    let router = build_ingest_router(None, exec.clone(), Arc::new(Notify::new()));

    let body = serde_json::to_vec(&json!({
        "ticket": { "source": "manual", "id": "ENG-123" },
        "repo": { "owner": "octo", "name": "world" },
        "base_branch": "main",
        "base_sha": "0123456789abcdef0123456789abcdef01234567",
        "cost_budget_cents": 100_000_u64,
        "workflow_id": "manual:ENG-123#run-2",
    }))
    .unwrap();

    let resp = router
        .oneshot(post_request(None, body))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let body_bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body["workflow_id"], "manual:ENG-123#run-2");

    // Default-derived workflow has no events.
    let default_events = exec
        .storage()
        .read_events(&WorkflowId::new("manual:ENG-123"))
        .await
        .unwrap();
    assert!(default_events.is_empty());
    // Override target has the event.
    let override_events = exec
        .storage()
        .read_events(&WorkflowId::new("manual:ENG-123#run-2"))
        .await
        .unwrap();
    assert_eq!(override_events.len(), 1);
}
