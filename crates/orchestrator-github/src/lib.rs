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
pub mod marker;
pub mod outcome;
pub mod sink;
pub mod trailer;

pub use action::{
    decode_commit_patch, decode_ensure_branch, decode_open_pr, CommitAuthor, CommitPatchPayload,
    DecodeError, EnsureBranchPayload, FileChange, OpenPrPayload, RepoRef, ALL_KINDS,
    KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH, KIND_OPEN_PR, MAX_PR_BODY_LEN, MAX_PR_TITLE_LEN,
    MAX_TOTAL_CONTENT_BYTES,
};
pub use auth::{GithubAuth, GithubAuthError};
pub use client::{app_client, installation_client};
pub use errors::{classify_github_error, classify_response, ErrorClass};
pub use extractor::GithubHintExtractor;
pub use marker::{append_action_id_marker, extract_action_id_marker};
pub use outcome::{
    branch_ensured_event, commit_pushed_event, pr_opened_event, BranchEnsured, CommitPushed,
    PrOpened, EVT_BRANCH_ENSURED, EVT_COMMIT_PUSHED, EVT_PR_OPENED,
};
pub use sink::GithubSink;
pub use trailer::{append_action_id_trailer, extract_action_id_trailer};
