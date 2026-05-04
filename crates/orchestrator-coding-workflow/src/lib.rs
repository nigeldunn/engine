//! Coding-workflow reducer for the orchestrator.
//!
//! Implements the linear single-task happy path described in PLAN.md
//! M11b v1: ingest → triage → plan → ensure_branch → code → commit →
//! review → security review → open PR → await human approval → merged.
//! Any permanent action failure halts the workflow; halting is final
//! (no automatic compensation in v1).
//!
//! Deferred to M11c+: multi-task plans, review iteration loops,
//! architecture step, timeouts, failure compensation beyond halt.

pub mod action_kinds;
pub mod events;
pub mod reducer;
pub mod state;
pub mod webhook;

pub use action_kinds::{
    KIND_AGENT_ARCHITECT, KIND_AGENT_CODER, KIND_AGENT_PLANNER, KIND_AGENT_REVIEWER,
    KIND_AGENT_SECURITY_REVIEWER, KIND_AGENT_TRIAGE, KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH,
    KIND_OPEN_PR,
};
pub use events::{
    architecture_proposed_event, budget_consumed_event, coder_output_event, plan_proposed_event,
    reviewer_output_event, security_reviewer_output_event, triage_completed_event,
    ArchitectureProposed, BudgetConsumed, CoderOutput, FileChangeOutput, PatchOutput,
    PlanProposed, PrMerged, ReviewerOutput, SecurityFinding, SecurityReviewerOutput, Severity,
    TaskSpec, TicketIngested, TicketRef, TriageCompleted, EVT_ARCHITECTURE_PROPOSED,
    EVT_BUDGET_CONSUMED, EVT_CODER_OUTPUT, EVT_PLAN_PROPOSED, EVT_PR_MERGED,
    EVT_REVIEWER_OUTPUT, EVT_SECURITY_REVIEWER_OUTPUT, EVT_TICKET_INGESTED, EVT_TRIAGE_COMPLETED,
};
pub use reducer::WorkflowReducer;
pub use state::{
    ExpectedOutcomeKind, FailureInfo, Plan, WorkflowState, WorkflowStatus,
};
pub use webhook::translate_github_webhook;
