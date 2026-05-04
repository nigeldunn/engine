//! Per-agent specifications. Each module declares an `AgentSpec` constant
//! that the sink uses to dispatch execute/probe through the shared
//! `dispatch` module.

pub mod architect;
pub mod coder;
pub mod planner;
pub mod reviewer;
pub mod security_reviewer;
pub mod triage;
