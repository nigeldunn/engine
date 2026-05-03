//! Typed action payloads for the GitHub sink.
//!
//! Action payloads cross the dispatcher boundary as untyped `serde_json::Value`
//! (per CLAUDE.md). The sink is the typed-decode boundary, and decoding here
//! is also the right place to reject malformed input — a buggy reducer or
//! out-of-band-injected outbox row should yield a `PermanentFail` rather than
//! a confusing 422 from GitHub later.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Action kind: ensure a branch exists at a known base SHA in a repo.
pub const KIND_ENSURE_BRANCH: &str = "github.ensure_branch";

/// All action kinds the GitHub sink handles. Mirror this in `Sink::handles()`
/// so registration stays consistent.
pub const ALL_KINDS: &[&str] = &[KIND_ENSURE_BRANCH];

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("invalid payload JSON: {0}")]
    Serde(String),
    #[error("invalid {field}: {detail}")]
    Validation {
        field: &'static str,
        detail: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepoRef {
    pub owner: String,
    pub name: String,
}

impl RepoRef {
    pub fn full(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }

    pub fn validate(&self) -> Result<(), DecodeError> {
        validate_owner(&self.owner)?;
        validate_repo_name(&self.name)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnsureBranchPayload {
    pub repo: RepoRef,
    pub base_branch: String,
    pub base_sha: String,
    /// Pre-computed by the reducer using `slugify` + `ActionBuilder`.
    pub branch_name: String,
    pub ticket_id: String,
}

impl EnsureBranchPayload {
    pub fn validate(&self) -> Result<(), DecodeError> {
        self.repo.validate()?;
        require_non_empty("base_branch", &self.base_branch, 250)?;
        validate_sha("base_sha", &self.base_sha)?;
        validate_branch_name(&self.branch_name)?;
        require_non_empty("ticket_id", &self.ticket_id, 250)?;
        Ok(())
    }
}

/// Decode a typed payload from the JSON the dispatcher hands us. Combines
/// serde + structural validation so a malformed payload becomes one error
/// rather than two stages of failure.
pub fn decode_ensure_branch(
    raw: &serde_json::Value,
) -> Result<EnsureBranchPayload, DecodeError> {
    let payload: EnsureBranchPayload = serde_json::from_value(raw.clone())
        .map_err(|e| DecodeError::Serde(e.to_string()))?;
    payload.validate()?;
    Ok(payload)
}

// ── validators ──────────────────────────────────────────────────────────

fn require_non_empty(field: &'static str, s: &str, max_len: usize) -> Result<(), DecodeError> {
    if s.is_empty() {
        return Err(DecodeError::Validation {
            field,
            detail: "empty".into(),
        });
    }
    if s.len() > max_len {
        return Err(DecodeError::Validation {
            field,
            detail: format!("exceeds {} chars", max_len),
        });
    }
    Ok(())
}

/// GitHub usernames / org names: 1-39 ASCII alphanumerics + single hyphens,
/// no leading/trailing hyphen, no consecutive hyphens.
fn validate_owner(s: &str) -> Result<(), DecodeError> {
    require_non_empty("repo.owner", s, 39)?;
    if s.starts_with('-') || s.ends_with('-') {
        return Err(DecodeError::Validation {
            field: "repo.owner",
            detail: "cannot begin or end with '-'".into(),
        });
    }
    if s.contains("--") {
        return Err(DecodeError::Validation {
            field: "repo.owner",
            detail: "cannot contain consecutive hyphens".into(),
        });
    }
    if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(DecodeError::Validation {
            field: "repo.owner",
            detail: "must be ASCII alphanumeric or '-'".into(),
        });
    }
    Ok(())
}

/// GitHub repository names: 1-100 ASCII alphanumerics + `.`, `-`, `_`.
/// Cannot be `.` or `..` alone, cannot start or end with `.`.
fn validate_repo_name(s: &str) -> Result<(), DecodeError> {
    require_non_empty("repo.name", s, 100)?;
    if s == "." || s == ".." {
        return Err(DecodeError::Validation {
            field: "repo.name",
            detail: "cannot be '.' or '..'".into(),
        });
    }
    if s.starts_with('.') || s.ends_with('.') {
        return Err(DecodeError::Validation {
            field: "repo.name",
            detail: "cannot begin or end with '.'".into(),
        });
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
    {
        return Err(DecodeError::Validation {
            field: "repo.name",
            detail: "must be ASCII alphanumeric or '.', '-', '_'".into(),
        });
    }
    Ok(())
}

/// `git check-ref-format` subset sufficient to reject anything GitHub
/// will refuse. Pretty strict; the reducer's `slugify` already produces
/// names that pass this trivially.
fn validate_branch_name(s: &str) -> Result<(), DecodeError> {
    require_non_empty("branch_name", s, 250)?;
    let bad = |detail: &str| DecodeError::Validation {
        field: "branch_name",
        detail: detail.into(),
    };
    if s.starts_with('-') {
        return Err(bad("cannot start with '-'"));
    }
    if s.starts_with('/') || s.ends_with('/') {
        return Err(bad("cannot start or end with '/'"));
    }
    if s.contains("//") {
        return Err(bad("cannot contain '//'"));
    }
    if s.contains("..") {
        return Err(bad("cannot contain '..'"));
    }
    if s.contains('\\') {
        return Err(bad("cannot contain backslash"));
    }
    if s.ends_with(".lock") {
        return Err(bad("cannot end with '.lock'"));
    }
    if s == "@" {
        return Err(bad("cannot be a single '@'"));
    }
    if s.chars().any(|c| c.is_ascii_control() || c == ' ') {
        return Err(bad("cannot contain control or whitespace chars"));
    }
    if s.chars().any(|c| matches!(c, '~' | '^' | ':' | '?' | '*' | '[')) {
        return Err(bad("cannot contain '~', '^', ':', '?', '*', or '['"));
    }
    Ok(())
}

/// SHA-1 commit ids: exactly 40 lowercase hex chars. GitHub returns SHAs
/// lowercase; we require the same so payload SHAs round-trip equality with
/// `Reference.object.sha` from the API.
fn validate_sha(field: &'static str, s: &str) -> Result<(), DecodeError> {
    if s.len() != 40 {
        return Err(DecodeError::Validation {
            field,
            detail: format!("must be exactly 40 chars (got {})", s.len()),
        });
    }
    if !s
        .chars()
        .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    {
        return Err(DecodeError::Validation {
            field,
            detail: "must be lowercase hex".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn good_payload() -> serde_json::Value {
        json!({
            "repo": { "owner": "octo-org", "name": "hello-world" },
            "base_branch": "main",
            "base_sha": "0123456789abcdef0123456789abcdef01234567",
            "branch_name": "auto/eng-123/abcdef0123456789",
            "ticket_id": "ENG-123",
        })
    }

    #[test]
    fn good_payload_decodes() {
        let p = decode_ensure_branch(&good_payload()).expect("must decode");
        assert_eq!(p.repo.full(), "octo-org/hello-world");
        assert_eq!(p.branch_name, "auto/eng-123/abcdef0123456789");
    }

    #[test]
    fn missing_required_field_is_serde_error() {
        let mut bad = good_payload();
        bad.as_object_mut().unwrap().remove("ticket_id");
        let err = decode_ensure_branch(&bad).expect_err("missing field must fail");
        assert!(matches!(err, DecodeError::Serde(_)));
    }

    #[test]
    fn empty_owner_rejected() {
        let mut bad = good_payload();
        bad["repo"]["owner"] = json!("");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "repo.owner", .. }));
    }

    #[test]
    fn owner_with_leading_hyphen_rejected() {
        let mut bad = good_payload();
        bad["repo"]["owner"] = json!("-bad");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "repo.owner", .. }));
    }

    #[test]
    fn owner_with_consecutive_hyphens_rejected() {
        let mut bad = good_payload();
        bad["repo"]["owner"] = json!("foo--bar");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "repo.owner", .. }));
    }

    #[test]
    fn owner_too_long_rejected() {
        let mut bad = good_payload();
        bad["repo"]["owner"] = json!("a".repeat(40));
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "repo.owner", .. }));
    }

    #[test]
    fn repo_name_with_leading_dot_rejected() {
        let mut bad = good_payload();
        bad["repo"]["name"] = json!(".secret");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "repo.name", .. }));
    }

    #[test]
    fn repo_name_with_slash_rejected() {
        let mut bad = good_payload();
        bad["repo"]["name"] = json!("foo/bar");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "repo.name", .. }));
    }

    #[test]
    fn branch_with_double_dot_rejected() {
        let mut bad = good_payload();
        bad["branch_name"] = json!("auto/foo..bar");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "branch_name", .. }));
    }

    #[test]
    fn branch_with_double_slash_rejected() {
        let mut bad = good_payload();
        bad["branch_name"] = json!("auto//foo");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "branch_name", .. }));
    }

    #[test]
    fn branch_with_lock_suffix_rejected() {
        let mut bad = good_payload();
        bad["branch_name"] = json!("auto/foo.lock");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "branch_name", .. }));
    }

    #[test]
    fn branch_with_space_rejected() {
        let mut bad = good_payload();
        bad["branch_name"] = json!("auto/foo bar");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "branch_name", .. }));
    }

    #[test]
    fn branch_with_special_char_rejected() {
        for bad_char in ['~', '^', ':', '?', '*', '['] {
            let mut bad = good_payload();
            bad["branch_name"] = json!(format!("auto/foo{}bar", bad_char));
            let err = decode_ensure_branch(&bad).unwrap_err();
            assert!(
                matches!(err, DecodeError::Validation { field: "branch_name", .. }),
                "char {:?} should be rejected",
                bad_char
            );
        }
    }

    #[test]
    fn branch_with_leading_slash_rejected() {
        let mut bad = good_payload();
        bad["branch_name"] = json!("/leading");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "branch_name", .. }));
    }

    #[test]
    fn sha_uppercase_rejected() {
        let mut bad = good_payload();
        bad["base_sha"] = json!("ABCDEF0123456789ABCDEF0123456789ABCDEF01");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "base_sha", .. }));
    }

    #[test]
    fn sha_wrong_length_rejected() {
        let mut bad = good_payload();
        bad["base_sha"] = json!("0123abc"); // too short
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "base_sha", .. }));
    }

    #[test]
    fn sha_non_hex_rejected() {
        let mut bad = good_payload();
        bad["base_sha"] = json!("zzzzzzzz0123456789abcdef0123456789abcdef");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "base_sha", .. }));
    }

    #[test]
    fn empty_ticket_id_rejected() {
        let mut bad = good_payload();
        bad["ticket_id"] = json!("");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "ticket_id", .. }));
    }

    #[test]
    fn empty_base_branch_rejected() {
        let mut bad = good_payload();
        bad["base_branch"] = json!("");
        let err = decode_ensure_branch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "base_branch", .. }));
    }

    #[test]
    fn payload_round_trips_via_serde() {
        let original = decode_ensure_branch(&good_payload()).unwrap();
        let json = serde_json::to_value(&original).unwrap();
        let again = decode_ensure_branch(&json).unwrap();
        assert_eq!(original.repo, again.repo);
        assert_eq!(original.branch_name, again.branch_name);
    }
}
