//! Action kind constants the workflow reducer emits.
//!
//! `agent.*` kinds are implemented by M12's agent runner sinks; their
//! contract is: receive a `ClaimedAction`, run the agent, write an
//! outcome event matching the corresponding `agent.*.completed.v1` /
//! `agent.*.output.v1` schema in `events.rs`. M11b just emits these
//! kinds; M12 implements them.
//!
//! `github.*` kinds re-export the constants from `orchestrator-github`
//! so the reducer's `derive_actions` body uses the same string values
//! the GitHub sink advertises in `Sink::handles()`.

pub const KIND_AGENT_TRIAGE: &str = "agent.run_triage";
pub const KIND_AGENT_PLANNER: &str = "agent.run_planner";
pub const KIND_AGENT_CODER: &str = "agent.run_coder";
pub const KIND_AGENT_REVIEWER: &str = "agent.run_reviewer";
pub const KIND_AGENT_SECURITY_REVIEWER: &str = "agent.run_security_reviewer";

pub use orchestrator_github::{
    KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH, KIND_OPEN_PR,
};
