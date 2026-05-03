//! GitHub sink for orchestrator-core.
//!
//! M3 skeleton + M4 typed payloads/outcomes. Action kinds are wired into
//! the sink as their `execute`/`probe` logic lands. Health probe is the
//! global App-level `GET /app` check from M3 (per-repo probes deferred).

pub mod action;
pub mod actions;
pub mod auth;
pub mod client;
pub mod errors;
pub mod extractor;
pub mod health;
pub mod outcome;
pub mod sink;
pub mod trailer;

pub use action::{
    decode_commit_patch, decode_ensure_branch, CommitAuthor, CommitPatchPayload, DecodeError,
    EnsureBranchPayload, FileChange, RepoRef, ALL_KINDS, KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH,
    MAX_TOTAL_CONTENT_BYTES,
};
pub use auth::{GithubAuth, GithubAuthError};
pub use client::{app_client, installation_client};
pub use errors::{classify_github_error, classify_response, ErrorClass};
pub use extractor::GithubHintExtractor;
pub use outcome::{
    branch_ensured_event, commit_pushed_event, BranchEnsured, CommitPushed, EVT_BRANCH_ENSURED,
    EVT_COMMIT_PUSHED,
};
pub use sink::GithubSink;
pub use trailer::{append_action_id_trailer, extract_action_id_trailer};
