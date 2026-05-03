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

use orchestrator_core::ActionId;
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
    /// In M11b v1 the reducer rejects `tasks.len() != 1`. M11c lifts
    /// this to support multi-task plans.
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
