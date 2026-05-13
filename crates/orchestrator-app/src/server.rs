//! HTTP server tasks (webhook ingest in slice 3, ticket ingest in slice 4).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use orchestrator_coding_workflow::WorkflowReducer;
use orchestrator_core::Executor;
use orchestrator_github_webhook::{router as webhook_router, GithubWebhookConfig};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tracing::{error, info, instrument, warn};

use crate::ingest::{ingest_ticket, IngestError, IngestOutcome, IngestRequest};
use crate::webhook::{handle_delivery_with_budget, HandleDeliveryError};

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("bind {addr} failed: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },

    #[error("axum::serve failed: {0}")]
    Serve(std::io::Error),
}

/// Bind the webhook listener. Split from `run_webhook` so the runtime
/// can fail boot synchronously when the address is in use, rather than
/// spawning a task that immediately errors out unobserved.
#[instrument(fields(addr = %listen))]
pub async fn bind_webhook_listener(listen: SocketAddr) -> Result<TcpListener, ServerError> {
    TcpListener::bind(listen)
        .await
        .map_err(|source| ServerError::Bind { addr: listen, source })
}

/// Process-only health check. Returns `200 OK` without touching Storage,
/// the dispatcher, or any sink. This is what ECS / ALB / API Gateway
/// container health probes must hit — anything that queries Postgres
/// would defeat the Aurora auto-pause work in Stages A and B
/// (see `docs/AWS_ECS_AURORA_DEPLOYMENT.md`).
async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Build a tiny router that exposes only `GET /healthz`. The path lives
/// at the root regardless of the webhook's configured `path_prefix`, so
/// load balancers and container health checks don't need to know how the
/// operator configured webhook routing.
pub fn build_health_router() -> Router {
    Router::new().route("/healthz", get(healthz))
}

/// Run the webhook server on an already-bound listener until `shutdown`
/// fires. Errors during a single delivery do NOT take the server down —
/// they're mapped to HTTP responses or logged. `wake` is the dispatcher's
/// wake handle: every successful delivery fires `notify_one()` so a fresh
/// event is claimed without waiting for the next poll cycle (Aurora idle
/// behavior: see `docs/AWS_ECS_AURORA_DEPLOYMENT.md`).
// Two `Arc<Notify>` parameters (`wake` and `shutdown`) are distinct
// concerns and intentionally kept separate — coalescing them would
// reintroduce the multi-consumer waker race the dispatcher already
// fixed. The retry budget + backoff are also distinct knobs, so a
// `RetryPolicy` wrapper would just hide them. Localized allow is
// cheaper than the abstractions.
#[allow(clippy::too_many_arguments)]
#[instrument(skip(listener, executor, secret, wake, shutdown), fields(prefix = %path_prefix))]
pub async fn run_webhook(
    listener: TcpListener,
    path_prefix: String,
    secret: String,
    executor: Arc<Executor<WorkflowReducer>>,
    lookup_retry_budget: Duration,
    lookup_retry_backoff: Duration,
    wake: Arc<Notify>,
    shutdown: Arc<Notify>,
) -> Result<(), ServerError> {
    let inner = build_webhook_router(
        secret,
        executor,
        lookup_retry_budget,
        lookup_retry_backoff,
        wake,
    );
    // `/healthz` is merged at the root regardless of the webhook
    // `path_prefix` so health probes have a stable path. The two
    // routers can't conflict — webhook traffic lives under
    // `path_prefix` (or `/` when empty, but the github router only
    // handles its own POST endpoints, not GET /healthz).
    let router = build_health_router().merge(
        if path_prefix.is_empty() || path_prefix == "/" {
            inner
        } else {
            axum::Router::new().nest(&path_prefix, inner)
        },
    );

    info!("webhook server listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.notified().await;
            info!("webhook server received shutdown");
        })
        .await
        .map_err(ServerError::Serve)?;
    info!("webhook server stopped");
    Ok(())
}

/// Construct the github webhook router with the production handler
/// closure: resolve workflow_id from the events table (with bounded
/// retry), translate the delivery, advance the executor. Exposed so
/// tests can drive the router via `tower::ServiceExt::oneshot` without
/// binding a port. Tests pass small budgets to avoid sleeping through
/// the production 5-second budget.
///
/// `wake` is fired exactly once per successfully-advanced delivery so the
/// dispatcher's claim loop picks up reducer-derived actions without
/// waiting for the next `poll_interval` tick. The same handle is shared
/// across deliveries; concurrent calls are safe (`Notify::notify_one` is
/// idempotent — at most one permit is stored).
pub fn build_webhook_router(
    secret: String,
    executor: Arc<Executor<WorkflowReducer>>,
    lookup_retry_budget: Duration,
    lookup_retry_backoff: Duration,
    wake: Arc<Notify>,
) -> axum::Router {
    let config = GithubWebhookConfig::new(secret);
    webhook_router(config, move |delivery| {
        let executor = executor.clone();
        let wake = wake.clone();
        async move {
            match handle_delivery_with_budget(
                &executor,
                &delivery,
                lookup_retry_budget,
                lookup_retry_backoff,
            )
            .await
            {
                Ok(()) => {
                    wake.notify_one();
                    Ok::<(), String>(())
                }
                Err(HandleDeliveryError::Ignored) => {
                    // Common for non-merged closes / unrelated events;
                    // debug-level so it doesn't drown the logs.
                    tracing::debug!(
                        event_type = %delivery.event_type,
                        delivery_id = %delivery.delivery_id,
                        "webhook delivery ignored"
                    );
                    Ok(())
                }
                Err(HandleDeliveryError::UnresolvedWorkflow {
                    delivery_id,
                    owner,
                    name,
                    pr_number,
                }) => {
                    // We've already exhausted `LOOKUP_RETRY_BUDGET`
                    // (default 5s of in-handler retries) at this point.
                    // GitHub does NOT auto-retry failed deliveries
                    // (Codex stop-gate round-9), so 500 here would just
                    // mark the delivery failed in the GitHub UI without
                    // any recovery. 200 acknowledges the delivery; the
                    // race window for tracked merges is covered by the
                    // retry budget above. If a tracked merge somehow
                    // races past the budget, the operator must manually
                    // redeliver via the GitHub App API.
                    warn!(
                        %delivery_id,
                        %owner, %name, %pr_number,
                        "no workflow for PR after retry budget; treating as untracked"
                    );
                    Ok(())
                }
                Err(HandleDeliveryError::Lookup { delivery_id, source }) => {
                    // The DB was unavailable for the entire retry
                    // budget. Return 500 so the failure shows up in
                    // GitHub's deliveries view — operator must redeliver
                    // manually (POST /app/hook/deliveries/{id}/attempts)
                    // once the DB is back, since GitHub does not
                    // auto-retry.
                    error!(
                        %delivery_id, error = %source,
                        "PR lookup failed for entire retry budget; \
                         500 for visibility — operator must redeliver"
                    );
                    Err(format!("workflow lookup failed: {source}"))
                }
                Err(HandleDeliveryError::Advance(e)) => {
                    // Real failure — let GitHub retry by responding 500.
                    error!(error = %e, delivery_id = %delivery.delivery_id, "advance failed");
                    Err(e.to_string())
                }
            }
        }
    })
}

// ── ingest server ──────────────────────────────────────────────────────

/// Bind the ticket-ingest listener. Same bind-before-spawn pattern as
/// the webhook server (Codex round-11).
#[instrument(fields(addr = %listen))]
pub async fn bind_ingest_listener(listen: SocketAddr) -> Result<TcpListener, ServerError> {
    TcpListener::bind(listen)
        .await
        .map_err(|source| ServerError::Bind { addr: listen, source })
}

/// Run the ticket-ingest server on an already-bound listener until
/// `shutdown` fires. `bearer_token` is the resolved value (or None on
/// loopback-only deployments). Auth is enforced uniformly for every
/// endpoint when set. `wake` is the dispatcher's wake handle: every
/// `Created` ingest fires `notify_one()` so a new workflow's first
/// derived action is claimed without waiting for `poll_interval` (Aurora
/// idle behavior).
#[instrument(skip(listener, executor, bearer_token, wake, shutdown))]
pub async fn run_ingest(
    listener: TcpListener,
    bearer_token: Option<String>,
    executor: Arc<Executor<WorkflowReducer>>,
    wake: Arc<Notify>,
    shutdown: Arc<Notify>,
) -> Result<(), ServerError> {
    let router = build_ingest_router(bearer_token, executor, wake);
    info!("ingest server listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            shutdown.notified().await;
            info!("ingest server received shutdown");
        })
        .await
        .map_err(ServerError::Serve)?;
    info!("ingest server stopped");
    Ok(())
}

/// State shared by the ingest router. Cloned per request via axum's
/// State extractor.
#[derive(Clone)]
struct IngestState {
    executor: Arc<Executor<WorkflowReducer>>,
    bearer_token: Option<Arc<String>>,
    /// Dispatcher wake — fired after a successful `Created` ingest so the
    /// dispatcher doesn't wait out `poll_interval` before claiming the new
    /// workflow's first action. `AlreadyExists` does not wake (no new event
    /// was written).
    wake: Arc<Notify>,
}

/// Construct the `POST /tickets` router. Exposed so tests can drive it
/// via `tower::ServiceExt::oneshot` without binding a port.
pub fn build_ingest_router(
    bearer_token: Option<String>,
    executor: Arc<Executor<WorkflowReducer>>,
    wake: Arc<Notify>,
) -> Router {
    let state = IngestState {
        executor,
        bearer_token: bearer_token.map(Arc::new),
        wake,
    };
    Router::new()
        .route("/tickets", post(handle_ingest))
        .with_state(state)
}

async fn handle_ingest(
    State(state): State<IngestState>,
    headers: HeaderMap,
    body: Result<Json<IngestRequest>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    if let Some(expected) = &state.bearer_token {
        let provided = headers
            .get("authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "))
            .unwrap_or("");
        // Constant-time compare so an attacker can't time their way to
        // the right token byte-by-byte.
        if provided.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() != 1 {
            return (StatusCode::UNAUTHORIZED, "bearer token missing or invalid").into_response();
        }
    }

    let Json(request) = match body {
        Ok(j) => j,
        Err(rej) => {
            return (StatusCode::BAD_REQUEST, format!("malformed request body: {rej}"))
                .into_response();
        }
    };

    match ingest_ticket(&state.executor, request).await {
        Ok(IngestOutcome::Created { workflow_id }) => {
            state.wake.notify_one();
            (
                StatusCode::CREATED,
                Json(serde_json::json!({
                    "workflow_id": workflow_id.as_str(),
                    "status": "created",
                })),
            )
                .into_response()
        }
        Ok(IngestOutcome::AlreadyExists { workflow_id }) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "workflow_id": workflow_id.as_str(),
                "status": "already_exists",
            })),
        )
            .into_response(),
        Err(IngestError::Conflict { dedup_key, .. }) => {
            warn!(%dedup_key, "ingest payload conflicts with stored event");
            (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "dedup_conflict",
                    "dedup_key": dedup_key,
                    "detail": "a different payload was previously ingested under this id",
                })),
            )
                .into_response()
        }
        Err(IngestError::Encode(e)) => {
            error!(error = %e, "ingest payload could not be encoded");
            (StatusCode::INTERNAL_SERVER_ERROR, "payload encode failed").into_response()
        }
        Err(IngestError::Preflight { dedup_key, source }) => {
            error!(%dedup_key, error = %source, "ingest preflight query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "dedup preflight failed").into_response()
        }
        Err(IngestError::Advance(e)) => {
            error!(error = %e, "ingest advance failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "advance failed").into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bind_returns_err_when_port_is_already_in_use() {
        // Codex round-11: previously bind happened inside the spawned
        // server task, so a port collision silently became a dead task
        // while Runtime::boot reported success. With bind split out,
        // the failure now surfaces synchronously to the caller.
        let occupier = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let occupied_addr = occupier.local_addr().unwrap();

        let result = bind_webhook_listener(occupied_addr).await;
        assert!(
            matches!(result, Err(ServerError::Bind { .. })),
            "got: {result:?}"
        );
    }
}
