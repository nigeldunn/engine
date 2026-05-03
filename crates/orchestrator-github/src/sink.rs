//! GithubSink: the `Sink` impl. M3 registers no action kinds; only the
//! global App-auth `check_health` does real work. `execute` returns
//! `Internal` defensively — the dispatcher should never route an unhandled
//! kind here, but if a misconfiguration does so, we record the failure
//! rather than panic the dispatcher task.

use async_trait::async_trait;
use orchestrator_core::{
    AttemptOutcome, ClaimedAction, DispatcherError, Sink, SinkHealthScope, SinkHealthState,
};
use std::sync::Arc;

use crate::auth::GithubAuth;
use crate::health;

const SINK_KEY: &str = "github";

/// GitHub sink. Holds a shared `GithubAuth` for App-level operations.
///
/// M3: declares zero action kinds via `handles()`. M4+ extends this list as
/// each `github.*` action is implemented.
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
        &[]
    }

    fn sink_key(&self) -> &str {
        SINK_KEY
    }

    async fn check_health(&self, scope: SinkHealthScope) -> SinkHealthState {
        health::check_health(&self.auth, scope).await
    }

    async fn execute(
        &self,
        _action: &ClaimedAction,
    ) -> Result<AttemptOutcome, DispatcherError> {
        Err(DispatcherError::Internal(
            "github sink registers no action kinds; received unexpected execute call".into(),
        ))
    }
}
