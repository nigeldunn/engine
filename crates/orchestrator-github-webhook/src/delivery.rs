//! `GithubWebhookDelivery` — the parsed envelope handed to the consumer's
//! handler closure.
//!
//! Payloads are kept opaque (`serde_json::Value`); the consumer is M11's
//! workflow reducer, which knows which event/action combinations matter
//! and decodes them. Typing out 30+ GitHub webhook event variants here
//! would be premature.

use serde_json::Value;

use crate::error::WebhookError;

#[derive(Debug, Clone)]
pub struct GithubWebhookDelivery {
    /// Value of the `X-GitHub-Event` header (e.g., `"pull_request"`,
    /// `"issue_comment"`, `"push"`).
    pub event_type: String,
    /// Value of the `X-GitHub-Delivery` header (UUID assigned by GitHub).
    /// Use as `EventCommand::ingress_dedup_key` so retries from GitHub's
    /// at-least-once delivery semantics are absorbed at
    /// `Storage::advance` time.
    pub delivery_id: String,
    /// Value of `payload.action` if the JSON body is an object with an
    /// `action` field. Many GitHub events carry an `action` (`"opened"`,
    /// `"closed"`, `"merged"`, `"created"`, etc.); this surfaces it
    /// pre-decoded so the consumer can pattern-match on
    /// `(event_type, action)` without re-parsing.
    pub action: Option<String>,
    /// The full webhook payload as parsed JSON.
    pub payload: Value,
}

/// Parse the validated webhook body. Caller is responsible for HMAC
/// validation (see `validate_hmac_sha256`) before calling this.
pub fn parse_delivery(
    event_type: &str,
    delivery_id: &str,
    body: &[u8],
) -> Result<GithubWebhookDelivery, WebhookError> {
    if event_type.is_empty() {
        return Err(WebhookError::MissingHeader("X-GitHub-Event"));
    }
    if delivery_id.is_empty() {
        return Err(WebhookError::MissingHeader("X-GitHub-Delivery"));
    }
    let payload: Value =
        serde_json::from_slice(body).map_err(|e| WebhookError::JsonParse(e.to_string()))?;
    let action = payload
        .get("action")
        .and_then(|a| a.as_str())
        .map(|s| s.to_string());
    Ok(GithubWebhookDelivery {
        event_type: event_type.to_string(),
        delivery_id: delivery_id.to_string(),
        action,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_well_formed_delivery_with_action() {
        let body = br#"{"action":"opened","number":42,"pull_request":{"id":1}}"#;
        let d = parse_delivery("pull_request", "delivery-uuid", body).unwrap();
        assert_eq!(d.event_type, "pull_request");
        assert_eq!(d.delivery_id, "delivery-uuid");
        assert_eq!(d.action.as_deref(), Some("opened"));
        assert_eq!(d.payload["number"].as_u64(), Some(42));
    }

    #[test]
    fn parses_event_without_action_field() {
        let body = br#"{"ref":"refs/heads/main","before":"a","after":"b"}"#;
        let d = parse_delivery("push", "delivery-uuid", body).unwrap();
        assert!(d.action.is_none());
    }

    #[test]
    fn parses_event_with_non_string_action() {
        // Defensive: if a payload's `action` field is e.g. a number,
        // we should leave the parsed action as None rather than fail.
        let body = br#"{"action":42}"#;
        let d = parse_delivery("custom", "d", body).unwrap();
        assert!(d.action.is_none());
    }

    #[test]
    fn rejects_empty_event_type() {
        let err = parse_delivery("", "delivery", b"{}").unwrap_err();
        assert!(matches!(
            err,
            WebhookError::MissingHeader("X-GitHub-Event")
        ));
    }

    #[test]
    fn rejects_empty_delivery_id() {
        let err = parse_delivery("pull_request", "", b"{}").unwrap_err();
        assert!(matches!(
            err,
            WebhookError::MissingHeader("X-GitHub-Delivery")
        ));
    }

    #[test]
    fn rejects_invalid_json() {
        let err = parse_delivery("pull_request", "d", b"not json").unwrap_err();
        assert!(matches!(err, WebhookError::JsonParse(_)));
    }

    #[test]
    fn rejects_empty_body() {
        let err = parse_delivery("pull_request", "d", b"").unwrap_err();
        assert!(matches!(err, WebhookError::JsonParse(_)));
    }
}
