//! `AgentRunnerSink<C: AgentClient>` — the `Sink` impl that handles all
//! 5 `agent.run_*` action kinds. Generic over the agent client so tests
//! can substitute a mock implementation.

use async_trait::async_trait;
use orchestrator_coding_workflow::{
    KIND_AGENT_CODER, KIND_AGENT_PLANNER, KIND_AGENT_REVIEWER, KIND_AGENT_SECURITY_REVIEWER,
    KIND_AGENT_TRIAGE,
};
use orchestrator_core::{
    AttemptOutcome, ClaimedAction, DispatcherError, ExistingResult, Sink, SinkHealthScope,
    SinkHealthState, SinkUnhealthyReason,
};
use std::sync::Arc;

use crate::actions::{coder, planner, reviewer, security_reviewer, triage};
use crate::client::AgentClient;
use crate::dispatch::{self, AgentSpec};
use crate::errors::AgentError;

const SINK_KEY: &str = "agent-runner";
const ALL_KINDS: &[&str] = &[
    KIND_AGENT_TRIAGE,
    KIND_AGENT_PLANNER,
    KIND_AGENT_CODER,
    KIND_AGENT_REVIEWER,
    KIND_AGENT_SECURITY_REVIEWER,
];

pub struct AgentRunnerSink<C: AgentClient> {
    client: Arc<C>,
}

impl<C: AgentClient> AgentRunnerSink<C> {
    pub fn new(client: C) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    pub fn from_arc(client: Arc<C>) -> Self {
        Self { client }
    }

    fn spec_for_kind(kind: &str) -> Option<&'static AgentSpec> {
        match kind {
            KIND_AGENT_TRIAGE => Some(&triage::SPEC),
            KIND_AGENT_PLANNER => Some(&planner::SPEC),
            KIND_AGENT_CODER => Some(&coder::SPEC),
            KIND_AGENT_REVIEWER => Some(&reviewer::SPEC),
            KIND_AGENT_SECURITY_REVIEWER => Some(&security_reviewer::SPEC),
            _ => None,
        }
    }
}

#[async_trait]
impl<C: AgentClient> Sink for AgentRunnerSink<C> {
    fn handles(&self) -> &[&'static str] {
        ALL_KINDS
    }

    fn sink_key(&self) -> &str {
        SINK_KEY
    }

    async fn check_health(&self, _scope: SinkHealthScope) -> SinkHealthState {
        match self.client.health().await {
            Ok(()) => SinkHealthState::Healthy,
            Err(AgentError::AuthenticationFailed(detail)) => SinkHealthState::Unhealthy {
                reason: SinkUnhealthyReason::AuthenticationFailed,
                detail,
                retry_after: None,
            },
            Err(AgentError::PermissionDenied(detail)) => SinkHealthState::Unhealthy {
                reason: SinkUnhealthyReason::PermissionDenied,
                detail,
                retry_after: None,
            },
            Err(AgentError::ServerError { status, detail }) => SinkHealthState::Indeterminate {
                detail: format!("agent server {}: {}", status, detail),
            },
            Err(other) => SinkHealthState::Indeterminate {
                detail: format!("agent service health: {}", other),
            },
        }
    }

    async fn find_existing(
        &self,
        action: &ClaimedAction,
    ) -> Result<Option<ExistingResult>, DispatcherError> {
        match Self::spec_for_kind(action.kind.as_str()) {
            Some(spec) => dispatch::probe(self.client.as_ref(), spec, action).await,
            None => Err(DispatcherError::Internal(format!(
                "agent runner: no probe for unhandled kind '{}'",
                action.kind
            ))),
        }
    }

    async fn execute(
        &self,
        action: &ClaimedAction,
    ) -> Result<AttemptOutcome, DispatcherError> {
        match Self::spec_for_kind(action.kind.as_str()) {
            Some(spec) => dispatch::execute(self.client.as_ref(), spec, action).await,
            None => Err(DispatcherError::Internal(format!(
                "agent runner: no executor for unhandled kind '{}'",
                action.kind
            ))),
        }
    }
}
