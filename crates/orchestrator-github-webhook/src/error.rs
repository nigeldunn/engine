//! Errors emitted by the webhook ingestion pipeline. Mapped to HTTP status
//! codes per the table in PLAN.md (M10):
//!
//! - 400 — missing required header / malformed signature header
//! - 403 — signature mismatch (HMAC fails)
//! - 413 — body exceeds `max_body_bytes` (axum's `DefaultBodyLimit` returns
//!   this directly without going through `WebhookError`)
//! - 422 — JSON body parse error
//! - 500 — handler closure returned `Err`

use thiserror::Error;

#[derive(Debug, Error)]
pub enum WebhookError {
    #[error("missing required header: {0}")]
    MissingHeader(&'static str),

    #[error("malformed signature header: {0}")]
    MalformedSignature(String),

    #[error("signature mismatch")]
    SignatureMismatch,

    #[error("JSON parse error: {0}")]
    JsonParse(String),

    #[error("handler error: {0}")]
    Handler(String),
}

impl WebhookError {
    pub fn http_status(&self) -> u16 {
        match self {
            Self::MissingHeader(_) => 400,
            Self::MalformedSignature(_) => 400,
            Self::SignatureMismatch => 403,
            Self::JsonParse(_) => 422,
            Self::Handler(_) => 500,
        }
    }
}
