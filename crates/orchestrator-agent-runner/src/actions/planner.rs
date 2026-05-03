//! `agent.run_planner` specification.

use orchestrator_coding_workflow::{plan_proposed_event, PlanProposed};
use orchestrator_core::{ClaimedAction, EventCommand};
use serde_json::Value;

use crate::dispatch::AgentSpec;
use crate::errors::AgentError;

pub const SPEC: AgentSpec = AgentSpec {
    agent_type: "planner",
    category: "agent.planner",
    build_outcome: build,
};

fn build(
    action: &ClaimedAction,
    output: &Value,
    request_id: Option<String>,
) -> Result<EventCommand, AgentError> {
    let mut body: PlanProposed = serde_json::from_value(output.clone())
        .map_err(|e| AgentError::MalformedOutput(e.to_string()))?;
    body.action_id = action.action_id.clone();
    Ok(plan_proposed_event(
        &action.workflow_id,
        &action.action_id,
        &body,
        request_id,
    ))
}
