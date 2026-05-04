//! `agent.run_architect` specification.

use orchestrator_coding_workflow::{architecture_proposed_event, ArchitectureProposed};
use orchestrator_core::{ClaimedAction, EventCommand};
use serde_json::Value;

use crate::dispatch::AgentSpec;
use crate::errors::AgentError;

pub const SPEC: AgentSpec = AgentSpec {
    agent_type: "architect",
    category: "agent.architect",
    build_outcome: build,
};

fn build(
    action: &ClaimedAction,
    output: &Value,
    request_id: Option<String>,
) -> Result<EventCommand, AgentError> {
    let mut body: ArchitectureProposed = serde_json::from_value(output.clone())
        .map_err(|e| AgentError::MalformedOutput(e.to_string()))?;
    body.action_id = action.action_id.clone();
    Ok(architecture_proposed_event(
        &action.workflow_id,
        &action.action_id,
        &body,
        request_id,
    ))
}
