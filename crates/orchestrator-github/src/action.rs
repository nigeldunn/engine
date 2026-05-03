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

/// Action kind: push a multi-file commit onto a branch via the Git Data API.
pub const KIND_COMMIT_PATCH: &str = "github.commit_patch";

/// All action kinds the GitHub sink handles. Mirror this in `Sink::handles()`
/// so registration stays consistent.
pub const ALL_KINDS: &[&str] = &[KIND_ENSURE_BRANCH, KIND_COMMIT_PATCH];

/// Total file-content bytes per `commit_patch` action. Bounds outbox row size
/// (the payload sits in SQLite). 5 MiB comfortably fits ~50 files of ~100KB
/// each, which is more than any realistic agent-emitted patch.
pub const MAX_TOTAL_CONTENT_BYTES: usize = 5 * 1024 * 1024;

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

// ── commit_patch ────────────────────────────────────────────────────────

/// Author / committer override for a `commit_patch`. When `None`, the
/// commit is attributed to the App's installation bot account.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitAuthor {
    pub name: String,
    pub email: String,
}

/// One file's worth of change in a `commit_patch`.
///
/// `mode` defaults to `"100644"` when absent. v1 accepts only `"100644"`
/// (regular file) and `"100755"` (executable file). Symlinks and submodules
/// are out of scope.
///
/// `content = None` deletes the file. `content = Some("")` creates an
/// empty file. UTF-8 only — binary patches are not supported in v1.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChange {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitPatchPayload {
    pub repo: RepoRef,
    pub branch: String,
    pub expected_parent_sha: String,
    pub commit_message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<CommitAuthor>,
    pub files: Vec<FileChange>,
    pub ticket_id: String,
}

impl CommitPatchPayload {
    pub fn validate(&self) -> Result<(), DecodeError> {
        self.repo.validate()?;
        validate_branch_name_with_field("branch", &self.branch)?;
        validate_sha("expected_parent_sha", &self.expected_parent_sha)?;
        require_non_empty("commit_message", &self.commit_message, 8192)?;
        require_non_empty("ticket_id", &self.ticket_id, 250)?;

        if let Some(author) = &self.author {
            require_non_empty("author.name", &author.name, 256)?;
            require_non_empty("author.email", &author.email, 256)?;
            if !author.email.contains('@') {
                return Err(DecodeError::Validation {
                    field: "author.email",
                    detail: "must contain '@'".into(),
                });
            }
        }

        if self.files.is_empty() {
            return Err(DecodeError::Validation {
                field: "files",
                detail: "must contain at least one file change".into(),
            });
        }

        let mut total_bytes: usize = 0;
        for f in &self.files {
            validate_file_path(&f.path)?;
            if let Some(mode) = &f.mode {
                validate_file_mode(mode)?;
            }
            if let Some(c) = &f.content {
                total_bytes = total_bytes.saturating_add(c.len());
            }
        }
        if total_bytes > MAX_TOTAL_CONTENT_BYTES {
            return Err(DecodeError::Validation {
                field: "files",
                detail: format!(
                    "total content size {} exceeds cap {} bytes",
                    total_bytes, MAX_TOTAL_CONTENT_BYTES
                ),
            });
        }
        Ok(())
    }
}

pub fn decode_commit_patch(
    raw: &serde_json::Value,
) -> Result<CommitPatchPayload, DecodeError> {
    let payload: CommitPatchPayload = serde_json::from_value(raw.clone())
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
fn validate_branch_name_with_field(
    field: &'static str,
    s: &str,
) -> Result<(), DecodeError> {
    require_non_empty(field, s, 250)?;
    let bad = |detail: &str| DecodeError::Validation {
        field,
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

/// Backwards-compatible wrapper used by `EnsureBranchPayload::branch_name`.
fn validate_branch_name(s: &str) -> Result<(), DecodeError> {
    validate_branch_name_with_field("branch_name", s)
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

/// Repository-relative file paths. Conservative: rejects any input that's
/// not a forward-slash-separated, ASCII-printable, normal-looking path.
/// GitHub itself rejects `.git/`-prefixed writes, but we belt-and-braces.
fn validate_file_path(path: &str) -> Result<(), DecodeError> {
    let bad = |detail: &str| DecodeError::Validation {
        field: "files[].path",
        detail: detail.into(),
    };
    if path.is_empty() {
        return Err(bad("empty"));
    }
    if path.len() > 4096 {
        return Err(bad("exceeds 4096 chars"));
    }
    if path.starts_with('/') {
        return Err(bad("must be repo-relative (no leading '/')"));
    }
    if path.ends_with('/') {
        return Err(bad("path must not end with '/'"));
    }
    if path.contains('\\') {
        return Err(bad("must not contain backslash"));
    }
    if path.contains('\0') {
        return Err(bad("must not contain NUL byte"));
    }
    if path == ".git" || path.starts_with(".git/") {
        return Err(bad("path must not be inside .git/"));
    }
    for component in path.split('/') {
        if component.is_empty() {
            return Err(bad("must not contain empty path components ('//')"));
        }
        if component == ".." {
            return Err(bad("must not contain '..' component"));
        }
    }
    Ok(())
}

/// File modes accepted at v1: regular file, executable file. Symlinks
/// (`120000`) and submodules (`160000`) are explicitly out of scope.
fn validate_file_mode(mode: &str) -> Result<(), DecodeError> {
    match mode {
        "100644" | "100755" => Ok(()),
        _ => Err(DecodeError::Validation {
            field: "files[].mode",
            detail: format!("must be \"100644\" or \"100755\" (got {:?})", mode),
        }),
    }
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

    // ── commit_patch validation ────────────────────────────────────────

    fn good_commit_patch() -> serde_json::Value {
        json!({
            "repo": { "owner": "octo-org", "name": "hello-world" },
            "branch": "auto/eng-123/abcdef0123456789",
            "expected_parent_sha": "0123456789abcdef0123456789abcdef01234567",
            "commit_message": "fix the thing",
            "files": [
                { "path": "src/main.rs", "content": "fn main() {}\n" },
                { "path": "old.txt" }
            ],
            "ticket_id": "ENG-123",
        })
    }

    #[test]
    fn good_commit_patch_decodes() {
        let p = decode_commit_patch(&good_commit_patch()).expect("must decode");
        assert_eq!(p.files.len(), 2);
        assert_eq!(p.files[0].path, "src/main.rs");
        assert!(p.files[0].content.is_some());
        // Second file: no `content` field → deletion.
        assert!(p.files[1].content.is_none());
    }

    #[test]
    fn empty_files_rejected() {
        let mut bad = good_commit_patch();
        bad["files"] = json!([]);
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "files", .. }));
    }

    #[test]
    fn bad_branch_name_rejected_under_branch_field() {
        let mut bad = good_commit_patch();
        bad["branch"] = json!("foo bar");
        let err = decode_commit_patch(&bad).unwrap_err();
        // Field name is "branch" for commit_patch (vs "branch_name" for ensure_branch).
        assert!(matches!(err, DecodeError::Validation { field: "branch", .. }));
    }

    #[test]
    fn bad_parent_sha_rejected() {
        let mut bad = good_commit_patch();
        bad["expected_parent_sha"] = json!("DEADBEEF");
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "expected_parent_sha", .. }));
    }

    #[test]
    fn empty_commit_message_rejected() {
        let mut bad = good_commit_patch();
        bad["commit_message"] = json!("");
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "commit_message", .. }));
    }

    #[test]
    fn path_with_leading_slash_rejected() {
        let mut bad = good_commit_patch();
        bad["files"] = json!([{ "path": "/absolute", "content": "" }]);
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "files[].path", .. }));
    }

    #[test]
    fn path_with_dotdot_component_rejected() {
        for path in ["../escape", "src/../escape", "a/b/.."] {
            let mut bad = good_commit_patch();
            bad["files"] = json!([{ "path": path, "content": "x" }]);
            let err = decode_commit_patch(&bad).unwrap_err();
            assert!(
                matches!(err, DecodeError::Validation { field: "files[].path", .. }),
                "{} should be rejected",
                path
            );
        }
    }

    #[test]
    fn path_with_double_slash_rejected() {
        let mut bad = good_commit_patch();
        bad["files"] = json!([{ "path": "src//main.rs", "content": "" }]);
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "files[].path", .. }));
    }

    #[test]
    fn path_into_dot_git_rejected() {
        for path in [".git", ".git/config", ".git/hooks/pre-commit"] {
            let mut bad = good_commit_patch();
            bad["files"] = json!([{ "path": path, "content": "x" }]);
            let err = decode_commit_patch(&bad).unwrap_err();
            assert!(
                matches!(err, DecodeError::Validation { field: "files[].path", .. }),
                "{} should be rejected",
                path
            );
        }
    }

    #[test]
    fn path_with_backslash_rejected() {
        let mut bad = good_commit_patch();
        bad["files"] = json!([{ "path": "src\\main.rs", "content": "" }]);
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "files[].path", .. }));
    }

    #[test]
    fn unsupported_mode_rejected() {
        for mode in ["120000", "040000", "160000", "777", ""] {
            let mut bad = good_commit_patch();
            bad["files"] = json!([
                { "path": "x", "mode": mode, "content": "" }
            ]);
            let err = decode_commit_patch(&bad).unwrap_err();
            assert!(
                matches!(err, DecodeError::Validation { field: "files[].mode", .. }),
                "mode {:?} should be rejected",
                mode
            );
        }
    }

    #[test]
    fn supported_modes_accepted() {
        for mode in ["100644", "100755"] {
            let mut p = good_commit_patch();
            p["files"] = json!([
                { "path": "x", "mode": mode, "content": "" }
            ]);
            decode_commit_patch(&p).unwrap_or_else(|e| panic!("mode {} should accept: {}", mode, e));
        }
    }

    #[test]
    fn total_content_over_cap_rejected() {
        let huge = "x".repeat(MAX_TOTAL_CONTENT_BYTES + 1);
        let mut bad = good_commit_patch();
        bad["files"] = json!([{ "path": "huge.bin", "content": huge }]);
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "files", .. }));
    }

    #[test]
    fn deletion_has_none_content() {
        // A FileChange with no `content` is a deletion. Empty `content`
        // is an empty file, not a deletion — different semantics.
        let mut p = good_commit_patch();
        p["files"] = json!([
            { "path": "delete.me" },
            { "path": "empty.txt", "content": "" }
        ]);
        let p = decode_commit_patch(&p).unwrap();
        assert!(p.files[0].content.is_none(), "missing content = delete");
        assert_eq!(p.files[1].content.as_deref(), Some(""), "empty content = empty file");
    }

    #[test]
    fn author_with_bad_email_rejected() {
        let mut bad = good_commit_patch();
        bad["author"] = json!({ "name": "Alice", "email": "alice-no-at-sign" });
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "author.email", .. }));
    }

    #[test]
    fn author_with_empty_name_rejected() {
        let mut bad = good_commit_patch();
        bad["author"] = json!({ "name": "", "email": "alice@example.com" });
        let err = decode_commit_patch(&bad).unwrap_err();
        assert!(matches!(err, DecodeError::Validation { field: "author.name", .. }));
    }

    #[test]
    fn author_optional_passes_when_absent() {
        let p = decode_commit_patch(&good_commit_patch()).unwrap();
        assert!(p.author.is_none());
    }

    #[test]
    fn commit_patch_round_trips_via_serde() {
        let original = decode_commit_patch(&good_commit_patch()).unwrap();
        let json = serde_json::to_value(&original).unwrap();
        let again = decode_commit_patch(&json).unwrap();
        assert_eq!(original.repo, again.repo);
        assert_eq!(original.branch, again.branch);
        assert_eq!(original.files.len(), again.files.len());
    }
}
