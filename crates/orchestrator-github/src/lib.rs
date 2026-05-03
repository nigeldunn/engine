//! GitHub sink for orchestrator-core.
//!
//! M3 skeleton + M4 typed payloads/outcomes. Action kinds are wired into
//! the sink as their `execute`/`probe` logic lands. Health probe is the
//! global App-level `GET /app` check from M3 (per-repo probes deferred).

pub mod action;
pub mod auth;
pub mod client;
pub mod errors;
pub mod extractor;
pub mod health;
pub mod outcome;
pub mod sink;

pub use action::{
    decode_ensure_branch, DecodeError, EnsureBranchPayload, RepoRef, ALL_KINDS,
    KIND_ENSURE_BRANCH,
};
pub use auth::{GithubAuth, GithubAuthError};
pub use client::{app_client, installation_client};
pub use errors::{classify_github_error, classify_response, ErrorClass};
pub use extractor::GithubHintExtractor;
pub use outcome::{branch_ensured_event, BranchEnsured, EVT_BRANCH_ENSURED};
pub use sink::GithubSink;
