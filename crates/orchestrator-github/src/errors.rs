//! Coarse classification of `octocrab::Error` into actionable categories.
//!
//! Per-action code maps `ErrorClass` → `AttemptOutcome` because the right
//! mapping depends on context (e.g., 404 on a workflow precondition is
//! `PermanentFail`, but 404 on a thing we just created might be
//! `TransientFail`). This module just answers "what kind of error is this?"
//! and leaves the policy to the caller.
//!
//! Mappings track the classification table in PLAN.md.

use std::fmt;

/// Coarse buckets that GitHub HTTP failures fall into.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorClass {
    /// 401.
    AuthenticationFailed { detail: String },
    /// 403 with a permission-denied message (not rate-limit).
    PermissionDenied { detail: String },
    /// 403 / 429 with rate-limit indicators.
    RateLimit { detail: String },
    /// 404. `detail` carries the GitHub message verbatim for log fidelity.
    NotFound { detail: String },
    /// 409.
    Conflict { detail: String },
    /// 422 with body message "Reference already exists" — the canonical
    /// idempotent-create signal that ensure_branch translates via probe.
    ReferenceAlreadyExists { detail: String },
    /// Other 422 (validation errors).
    Validation { detail: String },
    /// 5xx, network failure, timeout — transient by default.
    Transient { detail: String },
    /// Other 4xx (400, 410, etc.) — usually permanent config errors.
    OtherClient { status: u16, detail: String },
    /// Non-HTTP error (serialization, builder, etc.). Treated as Transient
    /// by most callers but kept distinct for logging.
    Other { detail: String },
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthenticationFailed { detail } => write!(f, "auth failed: {}", detail),
            Self::PermissionDenied { detail } => write!(f, "permission denied: {}", detail),
            Self::RateLimit { detail } => write!(f, "rate limit: {}", detail),
            Self::NotFound { detail } => write!(f, "not found: {}", detail),
            Self::Conflict { detail } => write!(f, "conflict: {}", detail),
            Self::ReferenceAlreadyExists { detail } => {
                write!(f, "reference already exists: {}", detail)
            }
            Self::Validation { detail } => write!(f, "validation error: {}", detail),
            Self::Transient { detail } => write!(f, "transient: {}", detail),
            Self::OtherClient { status, detail } => write!(f, "HTTP {}: {}", status, detail),
            Self::Other { detail } => write!(f, "other: {}", detail),
        }
    }
}

/// Classify an `octocrab::Error`. Thin adapter around `classify_response`
/// for the common case of a structured GitHub HTTP failure; transport-level
/// errors fall through to `Transient`.
pub fn classify_github_error(err: &octocrab::Error) -> ErrorClass {
    match err {
        octocrab::Error::GitHub { source, .. } => {
            classify_response(source.status_code.as_u16(), &source.message)
        }
        // Anything that isn't a structured GitHub response — connection
        // refused, TLS error, timeout, JSON deserialization — is treated
        // as transient. Per-action code can downgrade if it has more info.
        _ => ErrorClass::Transient {
            detail: format!("transport: {}", err),
        },
    }
}

/// Pure classifier: HTTP status + message body → `ErrorClass`. Exposed so
/// tests can exercise the full classification table without constructing
/// `octocrab::Error` values (which are `#[non_exhaustive]`).
pub fn classify_response(status: u16, message: &str) -> ErrorClass {
    let detail = message.to_string();
    match status {
        401 => ErrorClass::AuthenticationFailed { detail },
        403 => {
            if is_rate_limit(message) {
                ErrorClass::RateLimit { detail }
            } else {
                ErrorClass::PermissionDenied { detail }
            }
        }
        404 => ErrorClass::NotFound { detail },
        409 => ErrorClass::Conflict { detail },
        422 => {
            if message.eq_ignore_ascii_case("Reference already exists") {
                ErrorClass::ReferenceAlreadyExists { detail }
            } else {
                ErrorClass::Validation { detail }
            }
        }
        429 => ErrorClass::RateLimit { detail },
        500..=599 => ErrorClass::Transient {
            detail: format!("HTTP {}: {}", status, message),
        },
        _ => ErrorClass::OtherClient {
            status,
            detail,
        },
    }
}

/// GitHub returns 403 for both permission denial and rate limiting (primary
/// and secondary). The message text and headers distinguish them. We use
/// message substrings since octocrab's `GitHubError` exposes the text.
fn is_rate_limit(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("rate limit")
        || lower.contains("abuse detection")
        || lower.contains("secondary rate limit")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_401_as_auth_failed() {
        assert!(matches!(
            classify_response(401, "Bad credentials"),
            ErrorClass::AuthenticationFailed { .. }
        ));
    }

    #[test]
    fn classifies_403_permission_as_permission_denied() {
        assert!(matches!(
            classify_response(403, "Resource not accessible by integration"),
            ErrorClass::PermissionDenied { .. }
        ));
    }

    #[test]
    fn classifies_403_rate_limit_as_rate_limit() {
        assert!(matches!(
            classify_response(403, "API rate limit exceeded"),
            ErrorClass::RateLimit { .. }
        ));
    }

    #[test]
    fn classifies_403_secondary_rate_limit_as_rate_limit() {
        assert!(matches!(
            classify_response(403, "You have triggered an abuse detection mechanism"),
            ErrorClass::RateLimit { .. }
        ));
    }

    #[test]
    fn classifies_404_as_not_found() {
        assert!(matches!(
            classify_response(404, "Not Found"),
            ErrorClass::NotFound { .. }
        ));
    }

    #[test]
    fn classifies_409_as_conflict() {
        assert!(matches!(
            classify_response(409, "Conflict"),
            ErrorClass::Conflict { .. }
        ));
    }

    #[test]
    fn classifies_422_reference_already_exists_specifically() {
        assert!(matches!(
            classify_response(422, "Reference already exists"),
            ErrorClass::ReferenceAlreadyExists { .. }
        ));
        // Case-insensitive — guards against future GitHub message tweaks.
        assert!(matches!(
            classify_response(422, "reference already exists"),
            ErrorClass::ReferenceAlreadyExists { .. }
        ));
    }

    #[test]
    fn classifies_422_other_as_validation() {
        assert!(matches!(
            classify_response(422, "Validation Failed"),
            ErrorClass::Validation { .. }
        ));
    }

    #[test]
    fn classifies_429_as_rate_limit() {
        assert!(matches!(
            classify_response(429, "Too Many Requests"),
            ErrorClass::RateLimit { .. }
        ));
    }

    #[test]
    fn classifies_500_as_transient() {
        assert!(matches!(
            classify_response(500, "Internal Server Error"),
            ErrorClass::Transient { .. }
        ));
    }

    #[test]
    fn classifies_503_as_transient() {
        assert!(matches!(
            classify_response(503, "Service Unavailable"),
            ErrorClass::Transient { .. }
        ));
    }

    #[test]
    fn classifies_410_as_other_client() {
        assert!(matches!(
            classify_response(410, "Gone"),
            ErrorClass::OtherClient { status: 410, .. }
        ));
    }
}
