//! Axum router builder for the webhook ingestion endpoint.
//!
//! The lib returns a `Router` rather than serving directly, so consumers
//! integrate webhook ingestion alongside whatever else their app exposes.
//! Bind address, graceful shutdown, TLS, and observability stay on the
//! consumer side.
//!
//! **Raw-body invariant.** HMAC validation is computed over the raw bytes
//! of the request body. The handler uses axum's `Bytes` extractor (NOT
//! `Json`) so the body bytes are preserved exactly between HMAC check and
//! JSON parsing. A future refactor that swaps `Bytes` for `Json` would
//! silently break signature validation — don't do it.

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use std::future::Future;
use std::sync::Arc;
use tracing::{debug, error, warn};

use crate::delivery::{parse_delivery, GithubWebhookDelivery};
use crate::error::WebhookError;
use crate::hmac::validate_hmac_sha256;

const DEFAULT_MAX_BODY_BYTES: usize = 25 * 1024 * 1024;

/// Configuration for the GitHub webhook router.
#[derive(Clone)]
pub struct GithubWebhookConfig {
    /// Shared secret configured at the GitHub App level. The secret is
    /// used as the HMAC-SHA256 key against the raw request body.
    pub secret: String,
    /// Max body size in bytes. GitHub's documented hard cap is 25 MiB;
    /// the default matches that. Bodies larger than this get a 413
    /// directly from axum's `DefaultBodyLimit` middleware before our
    /// handler runs.
    pub max_body_bytes: usize,
}

impl GithubWebhookConfig {
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            secret: secret.into(),
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }

    pub fn with_max_body_bytes(mut self, max: usize) -> Self {
        self.max_body_bytes = max;
        self
    }
}

type BoxFuture<T> = std::pin::Pin<Box<dyn Future<Output = T> + Send + 'static>>;
type BoxedHandler = Box<
    dyn Fn(GithubWebhookDelivery) -> BoxFuture<Result<(), String>> + Send + Sync + 'static,
>;

/// Internal router state. Type-erased through `dyn Fn` so the router's
/// signature stays simple regardless of the consumer's closure type.
struct WebhookState {
    secret: String,
    handler: BoxedHandler,
}

/// Build an axum `Router` exposing a single `POST /` endpoint that
/// accepts GitHub webhook deliveries.
///
/// The `handler` closure runs after HMAC validation and JSON parsing
/// succeed. Closure errors are logged at error level and surface to
/// the client as `500 Internal Server Error`. The webhook crate is
/// transport-only — translating a `GithubWebhookDelivery` into an
/// `EventCommand` (and resolving `workflow_id` from delivery contents)
/// is the consumer's responsibility.
///
/// Mount under any prefix you like, e.g. `Router::new().nest("/webhook/github", router(...))`.
pub fn router<F, Fut, E>(config: GithubWebhookConfig, handler: F) -> Router
where
    F: Fn(GithubWebhookDelivery) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: std::fmt::Display + Send + 'static,
{
    // Box the handler future so the router type stays simple.
    let boxed: BoxedHandler = Box::new(move |d: GithubWebhookDelivery| {
        let fut = handler(d);
        Box::pin(async move { fut.await.map_err(|e| e.to_string()) })
    });

    let state = Arc::new(WebhookState {
        secret: config.secret,
        handler: boxed,
    });

    Router::new()
        .route("/", post(handle))
        .layer(DefaultBodyLimit::max(config.max_body_bytes))
        .with_state(state)
}

async fn handle(
    State(state): State<Arc<WebhookState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, ErrorResponse> {
    let event_type = header_str(&headers, "x-github-event")
        .ok_or_else(|| ErrorResponse::from_err(WebhookError::MissingHeader("X-GitHub-Event")))?;
    let delivery_id = header_str(&headers, "x-github-delivery").ok_or_else(|| {
        ErrorResponse::from_err(WebhookError::MissingHeader("X-GitHub-Delivery"))
    })?;
    let sig_header = header_str(&headers, "x-hub-signature-256").ok_or_else(|| {
        ErrorResponse::from_err(WebhookError::MissingHeader("X-Hub-Signature-256"))
    })?;

    validate_hmac_sha256(&state.secret, &body, &sig_header)
        .map_err(ErrorResponse::from_err)?;

    let delivery = parse_delivery(&event_type, &delivery_id, &body)
        .map_err(ErrorResponse::from_err)?;

    debug!(
        event_type = %delivery.event_type,
        delivery_id = %delivery.delivery_id,
        action = ?delivery.action,
        "webhook accepted"
    );

    if let Err(msg) = (state.handler)(delivery).await {
        error!(error = %msg, "webhook handler returned error");
        return Err(ErrorResponse::from_err(WebhookError::Handler(msg)));
    }

    Ok(StatusCode::OK)
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

/// HTTP error response. Body strings are intentionally short — they
/// don't leak internal detail (e.g., the specific JSON parse position).
struct ErrorResponse {
    status: StatusCode,
    body: &'static str,
}

impl ErrorResponse {
    fn from_err(e: WebhookError) -> Self {
        let status = StatusCode::from_u16(e.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body: &'static str = match &e {
            WebhookError::MissingHeader(_) => "missing required header",
            WebhookError::MalformedSignature(_) => "malformed signature header",
            WebhookError::SignatureMismatch => "signature mismatch",
            WebhookError::JsonParse(_) => "invalid JSON",
            WebhookError::Handler(_) => "internal handler error",
        };
        warn!(error = %e, status = %status, "webhook rejected");
        Self { status, body }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> axum::response::Response {
        (self.status, self.body).into_response()
    }
}
