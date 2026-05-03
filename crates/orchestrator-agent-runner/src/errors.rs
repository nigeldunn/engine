//! Error classification for agent service HTTP responses.
//!
//! Mirrors `orchestrator-github`'s ErrorClass pattern but for the
//! agent-runner crate's HTTP contract. Per the M12 round-3 classification
//! table.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("auth failed: {0}")]
    AuthenticationFailed(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("rate limit: {0}")]
    RateLimit(String),
    #[error("agent_type not found: {0}")]
    UnknownAgentType(String),
    #[error("agent rejected input: {0}")]
    InvalidInput(String),
    #[error("agent returned malformed output: {0}")]
    MalformedOutput(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("server error {status}: {detail}")]
    ServerError { status: u16, detail: String },
}
