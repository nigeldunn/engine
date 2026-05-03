//! GithubSink: the `Sink` impl. Routes each action kind to its module under
//! `actions::*`. `check_health` keeps the M3 global App-auth probe; per-repo
//! probes can be added when a future kind needs them.

use async_trait::async_trait;
use orchestrator_core::{
    AttemptOutcome, ClaimedAction, DispatcherError, ExistingResult, Sink, SinkHealthScope,
    SinkHealthState,
};
use std::sync::Arc;

use crate::action::{
    ALL_KINDS, KIND_CLOSE_PR, KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH, KIND_OPEN_PR,
    KIND_POST_ISSUE_COMMENT, KIND_SET_PR_STATUS, KIND_UPDATE_PR_METADATA,
};
use crate::actions;
use crate::auth::GithubAuth;
use crate::health;

const SINK_KEY: &str = "github";

/// GitHub sink. Holds a shared `GithubAuth` for App-level operations.
pub struct GithubSink {
    auth: Arc<GithubAuth>,
}

impl GithubSink {
    pub fn new(auth: GithubAuth) -> Self {
        Self {
            auth: Arc::new(auth),
        }
    }

    pub fn from_arc(auth: Arc<GithubAuth>) -> Self {
        Self { auth }
    }

    pub fn auth(&self) -> &Arc<GithubAuth> {
        &self.auth
    }
}

#[async_trait]
impl Sink for GithubSink {
    fn handles(&self) -> &[&'static str] {
        ALL_KINDS
    }

    fn sink_key(&self) -> &str {
        SINK_KEY
    }

    async fn check_health(&self, scope: SinkHealthScope) -> SinkHealthState {
        health::check_health(&self.auth, scope).await
    }

    async fn find_existing(
        &self,
        action: &ClaimedAction,
    ) -> Result<Option<ExistingResult>, DispatcherError> {
        match action.kind.as_str() {
            KIND_ENSURE_BRANCH => actions::ensure_branch::probe(&self.auth, action).await,
            KIND_COMMIT_PATCH => actions::commit_patch::probe(&self.auth, action).await,
            KIND_OPEN_PR => actions::open_pr::probe(&self.auth, action).await,
            KIND_POST_ISSUE_COMMENT => {
                actions::post_issue_comment::probe(&self.auth, action).await
            }
            // PATCH triple: no probe, last-write-wins. Returning Ok(None)
            // tells the dispatcher 'execute may proceed' — execute then
            // re-applies the same intent on retry.
            KIND_UPDATE_PR_METADATA | KIND_SET_PR_STATUS | KIND_CLOSE_PR => Ok(None),
            other => Err(DispatcherError::Internal(format!(
                "github sink: no probe for unhandled kind '{}'",
                other
            ))),
        }
    }

    async fn execute(
        &self,
        action: &ClaimedAction,
    ) -> Result<AttemptOutcome, DispatcherError> {
        match action.kind.as_str() {
            KIND_ENSURE_BRANCH => actions::ensure_branch::execute(&self.auth, action).await,
            KIND_COMMIT_PATCH => actions::commit_patch::execute(&self.auth, action).await,
            KIND_OPEN_PR => actions::open_pr::execute(&self.auth, action).await,
            KIND_UPDATE_PR_METADATA => {
                actions::update_pr_metadata::execute(&self.auth, action).await
            }
            KIND_SET_PR_STATUS => actions::set_pr_status::execute(&self.auth, action).await,
            KIND_CLOSE_PR => actions::close_pr::execute(&self.auth, action).await,
            KIND_POST_ISSUE_COMMENT => {
                actions::post_issue_comment::execute(&self.auth, action).await
            }
            other => Err(DispatcherError::Internal(format!(
                "github sink: no executor for unhandled kind '{}'",
                other
            ))),
        }
    }
}
