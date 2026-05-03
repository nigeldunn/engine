//! GitHub sink for orchestrator-core.
//!
//! M3 skeleton: registers no action kinds yet. `check_health` performs the
//! global App-level auth probe via `GET /app`. Real action kinds
//! (`github.ensure_branch`, `github.commit_patch`, ...) land in M4+.

pub mod auth;
pub mod extractor;
pub mod health;
pub mod sink;

pub use auth::{GithubAuth, GithubAuthError};
pub use extractor::GithubHintExtractor;
pub use sink::GithubSink;
