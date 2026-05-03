//! Integration tests for the webhook router. All run under `cargo test`
//! (no `#[ignore]`) — `tower::ServiceExt::oneshot` exercises the router
//! without needing a real network bind.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use orchestrator_github_webhook::{router, GithubWebhookConfig, GithubWebhookDelivery};
use sha2::Sha256;
use std::sync::{Arc, Mutex};
use tower::ServiceExt;

type HmacSha256 = Hmac<Sha256>;

const SECRET: &str = "shared-secret-for-tests";

fn signed_request(
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

fn ok_handler() -> impl Fn(GithubWebhookDelivery) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    + Clone
{
    |_d: GithubWebhookDelivery| Box::pin(async move { Ok::<(), String>(()) })
}

#[tokio::test]
async fn happy_path_returns_200_and_invokes_handler() {
    let captured: Arc<Mutex<Option<GithubWebhookDelivery>>> = Arc::new(Mutex::new(None));
    let cap_clone = captured.clone();
    let app = router(GithubWebhookConfig::new(SECRET), move |d| {
        let cap = cap_clone.clone();
        async move {
            *cap.lock().unwrap() = Some(d);
            Ok::<(), String>(())
        }
    });

    let body = br#"{"action":"opened","number":42,"pull_request":{"id":1}}"#.to_vec();
    let req = signed_request(SECRET, "pull_request", "delivery-uuid-1", body);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let d = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler should have run");
    assert_eq!(d.event_type, "pull_request");
    assert_eq!(d.delivery_id, "delivery-uuid-1");
    assert_eq!(d.action.as_deref(), Some("opened"));
    assert_eq!(d.payload["number"].as_u64(), Some(42));
}

#[tokio::test]
async fn rejects_missing_event_header_with_400() {
    let app = router(GithubWebhookConfig::new(SECRET), ok_handler());
    let body = b"{}".to_vec();
    let mut mac = HmacSha256::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(&body);
    let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("x-github-delivery", "d")
        .header("x-hub-signature-256", sig)
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_missing_delivery_header_with_400() {
    let app = router(GithubWebhookConfig::new(SECRET), ok_handler());
    let body = b"{}".to_vec();
    let mut mac = HmacSha256::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(&body);
    let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("x-github-event", "pull_request")
        .header("x-hub-signature-256", sig)
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_missing_signature_header_with_400() {
    let app = router(GithubWebhookConfig::new(SECRET), ok_handler());
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("x-github-event", "pull_request")
        .header("x-github-delivery", "d")
        .body(Body::from("{}".as_bytes().to_vec()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_malformed_signature_with_400() {
    let app = router(GithubWebhookConfig::new(SECRET), ok_handler());
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("x-github-event", "pull_request")
        .header("x-github-delivery", "d")
        .header("x-hub-signature-256", "not-the-right-prefix")
        .body(Body::from("{}".as_bytes().to_vec()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn rejects_signature_mismatch_with_403() {
    let app = router(GithubWebhookConfig::new(SECRET), ok_handler());
    let body = br#"{"action":"opened"}"#.to_vec();
    // Sign with a DIFFERENT secret.
    let mut mac = HmacSha256::new_from_slice(b"wrong-secret").unwrap();
    mac.update(&body);
    let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("x-github-event", "pull_request")
        .header("x-github-delivery", "d")
        .header("x-hub-signature-256", sig)
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn rejects_signature_for_modified_body_with_403() {
    let app = router(GithubWebhookConfig::new(SECRET), ok_handler());
    // Sign over one body, send a different body with the same signature.
    let mut mac = HmacSha256::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(b"{}");
    let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
    let req = Request::builder()
        .uri("/")
        .method("POST")
        .header("x-github-event", "pull_request")
        .header("x-github-delivery", "d")
        .header("x-hub-signature-256", sig)
        .body(Body::from("{\"tampered\":true}".as_bytes().to_vec()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn rejects_invalid_json_with_422() {
    let app = router(GithubWebhookConfig::new(SECRET), ok_handler());
    let body = b"not valid json".to_vec();
    let req = signed_request(SECRET, "pull_request", "d", body);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn handler_error_returns_500() {
    let app = router(GithubWebhookConfig::new(SECRET), |_d| async move {
        Err::<(), &'static str>("boom")
    });
    let body = b"{}".to_vec();
    let req = signed_request(SECRET, "pull_request", "d", body);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn delivery_id_matches_header_for_dedup_use() {
    // Verifies the contract that the consumer can use d.delivery_id
    // as ingress_dedup_key for the executor.
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let cap_clone = captured.clone();
    let app = router(GithubWebhookConfig::new(SECRET), move |d| {
        let cap = cap_clone.clone();
        async move {
            *cap.lock().unwrap() = Some(d.delivery_id);
            Ok::<(), String>(())
        }
    });

    let body = b"{}".to_vec();
    let req = signed_request(SECRET, "ping", "delivery-abc-123", body);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        captured.lock().unwrap().as_deref(),
        Some("delivery-abc-123")
    );
}

#[tokio::test]
async fn body_too_large_yields_413_or_400() {
    // axum's DefaultBodyLimit returns 413 when the body exceeds the cap.
    // The exact behavior across axum versions can also surface as a
    // BAD_REQUEST in some configurations; accept either as long as
    // the request is rejected before the handler runs.
    let captured: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let cap = captured.clone();
    let app = router(
        GithubWebhookConfig::new(SECRET).with_max_body_bytes(64),
        move |_d| {
            let cap = cap.clone();
            async move {
                *cap.lock().unwrap() = true;
                Ok::<(), String>(())
            }
        },
    );

    // Build a body larger than the 64-byte cap.
    let body = vec![b'x'; 256];
    let req = signed_request(SECRET, "push", "d", body);
    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status() == StatusCode::PAYLOAD_TOO_LARGE
            || resp.status() == StatusCode::BAD_REQUEST,
        "expected oversized body to be rejected with 413 or 400, got {}",
        resp.status()
    );
    assert!(
        !*captured.lock().unwrap(),
        "handler should not run for oversized body"
    );
}

#[tokio::test]
async fn error_response_bodies_are_short_safe_strings() {
    // Sanity: error responses don't leak internal detail.
    let app = router(GithubWebhookConfig::new(SECRET), ok_handler());
    let body = b"not json".to_vec();
    let req = signed_request(SECRET, "pull_request", "d", body);
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let bytes = to_bytes(resp.into_body(), 1024).await.unwrap();
    let s = std::str::from_utf8(&bytes).unwrap();
    // We return a short canonical string, not the serde_json error detail.
    assert_eq!(s, "invalid JSON");
}
