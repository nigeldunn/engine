//! `AgentClient` trait + default `HttpAgentClient`.
//!
//! Trait abstracts the agent service so tests inject a stub without HTTP.
//! `HttpAgentClient` posts to `/run/{agent_type}` and gets `/status/{type}/{id}`
//! per the M12 contract.

use async_trait::async_trait;
use orchestrator_core::ActionId;
use serde_json::Value;

use crate::errors::AgentError;

/// Result of `AgentClient::run`. The `Finished` variant carries the
/// agent's output JSON and an optional cost (in fixed-point USD cents).
/// `StillRunning` is a protocol-violation fallback — the v1 contract
/// expects `run` to block until finished.
#[derive(Debug, Clone)]
pub enum AgentRunResult {
    Finished {
        output: Value,
        cost_cents: Option<u64>,
    },
    StillRunning,
}

/// Result of `AgentClient::status` (used for crash-recovery probes).
#[derive(Debug, Clone)]
pub enum AgentRunStatus {
    NotFound,
    Running,
    Finished {
        output: Value,
        cost_cents: Option<u64>,
    },
}

/// Per-call HTTP correlation id. Sent as `X-Request-Id` and stamped
/// onto the outcome event's `trace_id` field for local correlation.
/// NOT the durable workflow trace.
pub fn fresh_request_id() -> String {
    format!("req_{}", uuid::Uuid::now_v7().simple())
}

#[async_trait]
pub trait AgentClient: Send + Sync + 'static {
    async fn run(
        &self,
        agent_type: &str,
        action_id: &ActionId,
        payload: &Value,
        request_id: &str,
    ) -> Result<AgentRunResult, AgentError>;

    async fn status(
        &self,
        agent_type: &str,
        action_id: &ActionId,
    ) -> Result<AgentRunStatus, AgentError>;

    async fn health(&self) -> Result<(), AgentError>;
}

/// Default HTTP implementation. POST `/run/{agent_type}`, GET
/// `/status/{agent_type}/{action_id}`, GET `/healthz`.
///
/// Request body shape (run):
/// ```json
/// { "action_id": "...", "payload": <opaque> }
/// ```
///
/// Response body shape (run, status finished):
/// ```json
/// { "status": "finished", "output": <opaque>, "cost_cents": <u64?> }
/// ```
///
/// Auth: optional bearer token injected as `Authorization: Bearer <token>`.
pub struct HttpAgentClient {
    base_url: String,
    bearer_token: Option<String>,
    http: reqwest::Client,
}

impl HttpAgentClient {
    pub fn new(base_url: impl Into<String>, bearer_token: Option<String>) -> Self {
        Self {
            base_url: base_url.into(),
            bearer_token,
            http: reqwest::Client::new(),
        }
    }

    fn endpoint(&self, path: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        format!("{}{}", base, path)
    }

    fn add_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.bearer_token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }
}

#[derive(serde::Deserialize)]
struct RunResponse {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    output: Option<Value>,
    #[serde(default)]
    cost_cents: Option<u64>,
}

#[async_trait]
impl AgentClient for HttpAgentClient {
    async fn run(
        &self,
        agent_type: &str,
        action_id: &ActionId,
        payload: &Value,
        request_id: &str,
    ) -> Result<AgentRunResult, AgentError> {
        let url = self.endpoint(&format!("/run/{}", agent_type));
        let body = serde_json::json!({
            "action_id": action_id.as_str(),
            "payload": payload,
        });
        let req = self
            .http
            .post(&url)
            .header("X-Request-Id", request_id)
            .json(&body);
        let resp = self.add_auth(req).send().await.map_err(transport_err)?;
        classify_run_response(resp).await
    }

    async fn status(
        &self,
        agent_type: &str,
        action_id: &ActionId,
    ) -> Result<AgentRunStatus, AgentError> {
        let url = self.endpoint(&format!("/status/{}/{}", agent_type, action_id.as_str()));
        let req = self.http.get(&url);
        let resp = self.add_auth(req).send().await.map_err(transport_err)?;
        classify_status_response(resp).await
    }

    async fn health(&self) -> Result<(), AgentError> {
        let url = self.endpoint("/healthz");
        let resp = self.add_auth(self.http.get(&url)).send().await.map_err(transport_err)?;
        let status = resp.status().as_u16();
        match status {
            200 => Ok(()),
            401 => Err(AgentError::AuthenticationFailed("/healthz 401".into())),
            403 => Err(AgentError::PermissionDenied("/healthz 403".into())),
            500..=599 => Err(AgentError::ServerError {
                status,
                detail: "/healthz".into(),
            }),
            _ => Err(AgentError::Transport(format!(
                "/healthz returned HTTP {}",
                status
            ))),
        }
    }
}

async fn classify_run_response(resp: reqwest::Response) -> Result<AgentRunResult, AgentError> {
    let status = resp.status().as_u16();
    let body_text = resp.text().await.unwrap_or_default();
    match status {
        200 | 201 => {
            let parsed: RunResponse = serde_json::from_str(&body_text)
                .map_err(|e| AgentError::MalformedOutput(format!("JSON parse: {}", e)))?;
            match parsed.status.as_deref() {
                Some("finished") | None => {
                    let output = parsed.output.ok_or_else(|| {
                        AgentError::MalformedOutput("response missing 'output'".into())
                    })?;
                    Ok(AgentRunResult::Finished {
                        output,
                        cost_cents: parsed.cost_cents,
                    })
                }
                Some("running") => Ok(AgentRunResult::StillRunning),
                Some(other) => Err(AgentError::MalformedOutput(format!(
                    "unknown status: {}",
                    other
                ))),
            }
        }
        401 => Err(AgentError::AuthenticationFailed(body_text)),
        403 => Err(AgentError::PermissionDenied(body_text)),
        404 => Err(AgentError::UnknownAgentType(body_text)),
        422 => Err(AgentError::InvalidInput(body_text)),
        429 => Err(AgentError::RateLimit(body_text)),
        500..=599 => Err(AgentError::ServerError {
            status,
            detail: body_text,
        }),
        _ => Err(AgentError::Transport(format!(
            "unexpected HTTP {}: {}",
            status, body_text
        ))),
    }
}

async fn classify_status_response(
    resp: reqwest::Response,
) -> Result<AgentRunStatus, AgentError> {
    let status = resp.status().as_u16();
    let body_text = resp.text().await.unwrap_or_default();
    match status {
        200 => {
            let parsed: RunResponse = serde_json::from_str(&body_text)
                .map_err(|e| AgentError::MalformedOutput(format!("JSON parse: {}", e)))?;
            match parsed.status.as_deref() {
                Some("running") => Ok(AgentRunStatus::Running),
                Some("finished") | None => {
                    let output = parsed.output.ok_or_else(|| {
                        AgentError::MalformedOutput("status finished missing 'output'".into())
                    })?;
                    Ok(AgentRunStatus::Finished {
                        output,
                        cost_cents: parsed.cost_cents,
                    })
                }
                Some(other) => Err(AgentError::MalformedOutput(format!(
                    "unknown status: {}",
                    other
                ))),
            }
        }
        404 => Ok(AgentRunStatus::NotFound),
        401 => Err(AgentError::AuthenticationFailed(body_text)),
        403 => Err(AgentError::PermissionDenied(body_text)),
        429 => Err(AgentError::RateLimit(body_text)),
        500..=599 => Err(AgentError::ServerError {
            status,
            detail: body_text,
        }),
        _ => Err(AgentError::Transport(format!(
            "unexpected HTTP {}: {}",
            status, body_text
        ))),
    }
}

fn transport_err(e: reqwest::Error) -> AgentError {
    AgentError::Transport(e.to_string())
}
