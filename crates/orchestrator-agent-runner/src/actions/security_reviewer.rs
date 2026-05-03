//! `agent.run_security_reviewer` specification.

use orchestrator_coding_workflow::{security_reviewer_output_event, SecurityReviewerOutput};
use orchestrator_core::{ClaimedAction, EventCommand};
use serde_json::Value;

use crate::dispatch::AgentSpec;
use crate::errors::AgentError;

pub const SPEC: AgentSpec = AgentSpec {
    agent_type: "security_reviewer",
    category: "agent.security_reviewer",
    build_outcome: build,
};

fn build(
    action: &ClaimedAction,
    output: &Value,
    request_id: Option<String>,
) -> Result<EventCommand, AgentError> {
    let mut body: SecurityReviewerOutput = serde_json::from_value(output.clone())
        .map_err(|e| AgentError::MalformedOutput(e.to_string()))?;
    body.action_id = action.action_id.clone();
    Ok(security_reviewer_output_event(
        &action.workflow_id,
        &action.action_id,
        &body,
        request_id,
    ))
}
