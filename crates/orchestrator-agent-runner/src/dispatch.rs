//! Shared dispatch logic. Each per-agent module specifies (a) the agent
//! type string, (b) how to build the outcome `EventCommand` from the
//! agent's output, (c) how to build the recovered outcome for the probe
//! path. This module orchestrates the rest: client call, error
//! classification, side-event emission, malformed-cost fallback.

use orchestrator_core::{
    AttemptOutcome, ClaimedAction, DispatcherError, EventCommand, ExistingResult,
    SinkUnhealthyReason,
};
use orchestrator_coding_workflow::budget_consumed_event;
use serde_json::Value;
use tracing::{debug, warn};

use crate::client::{fresh_request_id, AgentClient, AgentRunResult, AgentRunStatus};
use crate::errors::AgentError;

/// Per-agent specifics. Each agent module provides a constant of this type
/// to drive `execute` / `probe` through `dispatch`.
pub struct AgentSpec {
    pub agent_type: &'static str,
    pub category: &'static str, // for BudgetConsumed.category
    pub build_outcome: fn(
        &ClaimedAction,
        &Value,
        Option<String>,
    ) -> Result<EventCommand, AgentError>,
}

pub async fn execute<C: AgentClient>(
    client: &C,
    spec: &AgentSpec,
    action: &ClaimedAction,
) -> Result<AttemptOutcome, DispatcherError> {
    let request_id = fresh_request_id();
    let result = client
        .run(spec.agent_type, &action.action_id, &action.payload, &request_id)
        .await;
    match result {
        Ok(AgentRunResult::Finished { output, cost_cents }) => {
            match (spec.build_outcome)(action, &output, Some(request_id.clone())) {
                Ok(outcome_event) => {
                    debug!(agent_type = spec.agent_type, "agent run finished");
                    let side_events = side_events_for_cost(action, spec, cost_cents);
                    Ok(AttemptOutcome::Succeeded {
                        external_ref: Some(request_id),
                        outcome_event,
                        side_events,
                    })
                }
                Err(e) => Ok(AttemptOutcome::PermanentFail {
                    error: format!("agent returned malformed output: {}", e),
                }),
            }
        }
        Ok(AgentRunResult::StillRunning) => Ok(AttemptOutcome::TransientFail {
            error: format!(
                "agent {} still running (protocol violation: contract expects blocking run)",
                spec.agent_type
            ),
        }),
        Err(e) => Ok(map_error(e, spec)),
    }
}

pub async fn probe<C: AgentClient>(
    client: &C,
    spec: &AgentSpec,
    action: &ClaimedAction,
) -> Result<Option<ExistingResult>, DispatcherError> {
    let result = client.status(spec.agent_type, &action.action_id).await;
    match result {
        Ok(AgentRunStatus::NotFound) => Ok(None),
        Ok(AgentRunStatus::Running) => Err(DispatcherError::Sink(format!(
            "agent {} still running on probe; cannot determine completion",
            spec.agent_type
        ))),
        Ok(AgentRunStatus::Finished { output, cost_cents }) => {
            match (spec.build_outcome)(action, &output, None) {
                Ok(outcome_event) => {
                    let side_events = side_events_for_cost(action, spec, cost_cents);
                    Ok(Some(ExistingResult {
                        external_ref: None,
                        outcome_event,
                        side_events,
                    }))
                }
                Err(e) => Err(DispatcherError::Sink(format!(
                    "probe found finished status but output is malformed: {}",
                    e
                ))),
            }
        }
        Err(e) => Err(DispatcherError::Sink(format!(
            "probe transport: {}",
            e
        ))),
    }
}

/// Construct the `BudgetConsumed` side event when cost is reported.
/// Malformed/missing cost degrades gracefully — the primary outcome is
/// still emitted, just without the BudgetConsumed companion (per Codex
/// round-3 G.2: malformed cost metadata shouldn't fail the action).
fn side_events_for_cost(
    action: &ClaimedAction,
    spec: &AgentSpec,
    cost_cents: Option<u64>,
) -> Vec<EventCommand> {
    match cost_cents {
        Some(cents) if cents > 0 => vec![budget_consumed_event(
            &action.workflow_id,
            &action.action_id,
            cents,
            spec.category.into(),
        )],
        _ => vec![],
    }
}

fn map_error(err: AgentError, spec: &AgentSpec) -> AttemptOutcome {
    match err {
        AgentError::AuthenticationFailed(detail) => AttemptOutcome::SinkUnhealthy {
            reason: SinkUnhealthyReason::AuthenticationFailed,
            detail,
        },
        AgentError::PermissionDenied(detail) => AttemptOutcome::SinkUnhealthy {
            reason: SinkUnhealthyReason::PermissionDenied,
            detail,
        },
        AgentError::RateLimit(detail) => AttemptOutcome::TransientFail {
            error: format!("rate limit: {}", detail),
        },
        AgentError::UnknownAgentType(detail) => AttemptOutcome::PermanentFail {
            error: format!("agent service does not know agent_type {}: {}", spec.agent_type, detail),
        },
        AgentError::InvalidInput(detail) => AttemptOutcome::PermanentFail {
            error: format!("agent rejected input: {}", detail),
        },
        AgentError::MalformedOutput(detail) => AttemptOutcome::PermanentFail {
            error: format!("agent malformed output: {}", detail),
        },
        AgentError::Transport(detail) => {
            warn!(detail, "agent transport error → transient");
            AttemptOutcome::TransientFail { error: detail }
        }
        AgentError::ServerError { status, detail } => AttemptOutcome::TransientFail {
            error: format!("agent server {}: {}", status, detail),
        },
    }
}
