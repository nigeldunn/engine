//! `agent.run_coder` specification.

use orchestrator_coding_workflow::{coder_output_event, CoderOutput};
use orchestrator_core::{ClaimedAction, EventCommand};
use serde_json::Value;

use crate::dispatch::AgentSpec;
use crate::errors::AgentError;

pub const SPEC: AgentSpec = AgentSpec {
    agent_type: "coder",
    category: "agent.coder",
    build_outcome: build,
};

fn build(
    action: &ClaimedAction,
    output: &Value,
    request_id: Option<String>,
) -> Result<EventCommand, AgentError> {
    let mut body: CoderOutput = serde_json::from_value(output.clone())
        .map_err(|e| AgentError::MalformedOutput(e.to_string()))?;
    body.action_id = action.action_id.clone();
    Ok(coder_output_event(
        &action.workflow_id,
        &action.action_id,
        &body,
        request_id,
    ))
}
