//! Domain event payload types and constants.
//!
//! Two categories:
//! - **Inbound events** (e.g., `TicketIngested`, `BudgetConsumed`,
//!   `PrMerged`) come from outside the reducer: webhook ingestion, ticket
//!   sources, agent sinks reporting cost.
//! - **Agent output events** (e.g., `TriageCompleted`, `PlanProposed`,
//!   `CoderOutput`) come from M12 agent sinks; their schemas are
//!   defined here in M11b so M12 can import them when implementing the
//!   sinks.

use orchestrator_core::{ActionId, Causation, EventCommand, WorkflowId};
use orchestrator_github::RepoRef;
use serde::{Deserialize, Serialize};

// ── event type constants ────────────────────────────────────────────────

pub const EVT_TICKET_INGESTED: &str = "workflow.ticket_ingested.v1";

pub const EVT_TRIAGE_COMPLETED: &str = "agent.triage.completed.v1";
pub const EVT_PLAN_PROPOSED: &str = "agent.plan.proposed.v1";
pub const EVT_CODER_OUTPUT: &str = "agent.coder.output.v1";
pub const EVT_REVIEWER_OUTPUT: &str = "agent.reviewer.output.v1";
pub const EVT_SECURITY_REVIEWER_OUTPUT: &str = "agent.security_reviewer.output.v1";

pub const EVT_BUDGET_CONSUMED: &str = "core.budget.consumed.v1";

pub const EVT_PR_MERGED: &str = "github.pr_merged.v1";

// ── inbound: ticket ingestion ───────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TicketRef {
    pub source: String, // "jira" | "linear" | "manual" | etc.
    pub id: String,     // source-local id (e.g., "ENG-123")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TicketIngested {
    pub ticket: TicketRef,
    pub repo: RepoRef,
    pub base_branch: String,
    pub base_sha: String,
    /// Optional ceiling on cumulative agent cost. `None` means no cap;
    /// `Some(0)` would halt before the first agent runs.
    pub cost_budget_cents: Option<u64>,
}

// ── agent outputs ───────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TriageCompleted {
    pub action_id: ActionId,
    pub accepted: bool,
    /// `Some(_)` when `accepted == false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanProposed {
    pub action_id: ActionId,
    /// One or more tasks; the reducer rejects an empty plan. Tasks are
    /// run sequentially with one commit per task; M11d will add the
    /// reviewer-iteration loop on rejection.
    pub tasks: Vec<TaskSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskSpec {
    pub description: String,
    /// Advisory hint for the coder agent — non-binding.
    #[serde(default)]
    pub files_in_scope: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CoderOutput {
    pub action_id: ActionId,
    pub task_idx: usize,
    pub patch: PatchOutput,
    pub notes: String,
}

/// Coder output describes a patch in agent-domain terms. Deliberately
/// independent of `orchestrator_github::FileChange` so the same coder
/// output could later target a different sink (gitlab, gitea, etc.).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PatchOutput {
    pub files: Vec<FileChangeOutput>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileChangeOutput {
    pub path: String,
    /// `None` defaults to `"100644"`. Same set as the github sink
    /// accepts: `"100644"` or `"100755"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// `None` deletes the file; `Some(s)` upserts the UTF-8 content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReviewerOutput {
    pub action_id: ActionId,
    pub passed: bool,
    /// `Some(_)` when `passed == false`. M11b v1 ignores this and halts
    /// on a rejection (no iteration loops); M11c uses it to drive
    /// re-coder loops.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecurityReviewerOutput {
    pub action_id: ActionId,
    pub passed: bool,
    #[serde(default)]
    pub findings: Vec<SecurityFinding>,
}

/// Codex round 2 disagreed on free-form severity strings: a typed enum
/// fails fast on schema drift instead of letting unknown values flow
/// silently into pure reducer logic. Per CLAUDE.md rule #9, evolving
/// the set bumps the event's `payload_type` version.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Info,
    Warning,
    High,
    Critical,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecurityFinding {
    pub severity: Severity,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
}

// ── budget ──────────────────────────────────────────────────────────────

/// Cost in fixed-point cents (USD) per Codex round-2 pushback: floating-
/// point accumulation breaks deterministic replay because rounding drift
/// makes threshold guards in `derive_actions` non-reproducible. Integer
/// cents accumulate exactly.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BudgetConsumed {
    /// The action whose execution incurred this cost.
    pub action_id: ActionId,
    pub cents: u64,
    /// Free-form category for billing breakdowns ("agent.triage",
    /// "agent.coder", "github.api", etc.).
    pub category: String,
}

// ── github webhook → workflow event ─────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrMerged {
    pub repo: RepoRef,
    pub pr_number: u64,
    pub merge_commit_sha: String,
}

// ── helpers ─────────────────────────────────────────────────────────────

/// Decode a typed payload from an event's JSON value. Centralized so the
/// reducer's pattern-match arms stay tidy.
pub fn decode<T: serde::de::DeserializeOwned>(
    payload: &serde_json::Value,
) -> Result<T, serde_json::Error> {
    serde_json::from_value(payload.clone())
}

// ── event command constructors ─────────────────────────────────────────
//
// Used by the agent-runner sinks (orchestrator-agent-runner crate) to
// build typed `EventCommand`s for the agent output events. Causation is
// always `Action { action_id }` since these events are direct
// consequences of dispatched agent.run_* actions.

fn build_event_command<T: Serialize>(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    payload_type: &'static str,
    body: &T,
    request_id: Option<String>,
) -> EventCommand {
    EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: payload_type.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(body).expect("agent output serializes infallibly"),
        causation: Causation::Action {
            action_id: action_id.clone(),
        },
        // request_id stamped here as the event-level correlation id —
        // per the M12 round-3 contract, this is per-HTTP-attempt
        // correlation, NOT the durable workflow trace.
        trace_id: request_id,
        ingress_dedup_key: None,
    }
}

pub fn triage_completed_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    body: &TriageCompleted,
    request_id: Option<String>,
) -> EventCommand {
    build_event_command(workflow_id, action_id, EVT_TRIAGE_COMPLETED, body, request_id)
}

pub fn plan_proposed_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    body: &PlanProposed,
    request_id: Option<String>,
) -> EventCommand {
    build_event_command(workflow_id, action_id, EVT_PLAN_PROPOSED, body, request_id)
}

pub fn coder_output_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    body: &CoderOutput,
    request_id: Option<String>,
) -> EventCommand {
    build_event_command(workflow_id, action_id, EVT_CODER_OUTPUT, body, request_id)
}

pub fn reviewer_output_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    body: &ReviewerOutput,
    request_id: Option<String>,
) -> EventCommand {
    build_event_command(workflow_id, action_id, EVT_REVIEWER_OUTPUT, body, request_id)
}

pub fn security_reviewer_output_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    body: &SecurityReviewerOutput,
    request_id: Option<String>,
) -> EventCommand {
    build_event_command(
        workflow_id,
        action_id,
        EVT_SECURITY_REVIEWER_OUTPUT,
        body,
        request_id,
    )
}

/// Build a `BudgetConsumed` side event with a kind-prefixed dedup key.
/// The dedup key prevents duplicate writes on dispatcher crash-recovery
/// (one cost report per action attempt).
pub fn budget_consumed_event(
    workflow_id: &WorkflowId,
    action_id: &ActionId,
    cents: u64,
    category: String,
) -> EventCommand {
    let body = BudgetConsumed {
        action_id: action_id.clone(),
        cents,
        category,
    };
    EventCommand {
        workflow_id: workflow_id.clone(),
        payload_type: EVT_BUDGET_CONSUMED.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body).expect("BudgetConsumed serializes infallibly"),
        causation: Causation::Action {
            action_id: action_id.clone(),
        },
        trace_id: None,
        ingress_dedup_key: Some(format!("budget_consumed:{}", action_id.as_str())),
    }
}
