//! Workflow state — the snapshot the reducer maintains.
//!
//! Wide-and-flat shape (Codex round-1 confirmed): every field is at the
//! top level, statuses are tag-only enum variants. Adding a new optional
//! field is purely additive and doesn't break old snapshots.
//!
//! Field invariants:
//! - `cost_consumed_cents` is monotone non-decreasing.
//! - `pending_action_ids` carries every action_id we've emitted but not
//!   yet observed an outcome for. Failure events are matched against this
//!   set: a failure for an action_id NOT in the set is ignored (it's not
//!   for this workflow, or it's a stale duplicate).
//! - `current_task` is `Some(idx)` only while we're in `Coding` /
//!   `PushingCommit` / `Reviewing` etc., and the index is into
//!   `plan.tasks`.

use orchestrator_core::ActionId;
use orchestrator_github::RepoRef;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::events::TaskSpec;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct WorkflowState {
    pub status: WorkflowStatus,

    // Ticket
    pub ticket: Option<TicketRef>,

    // Repo + branch
    pub repo: Option<RepoRef>,
    pub base_branch: Option<String>,
    pub base_sha: Option<String>,
    pub branch_name: Option<String>,
    pub head_sha: Option<String>,

    // Plan
    pub plan: Option<Plan>,
    pub current_task: Option<usize>,

    // PR
    pub pr_number: Option<u64>,
    pub pr_html_url: Option<String>,
    pub merge_commit_sha: Option<String>,

    // Budget (fixed-point cents — see events::BudgetConsumed for why)
    pub cost_consumed_cents: u64,
    pub cost_budget_cents: Option<u64>,

    // Failure
    pub failure: Option<FailureInfo>,

    /// M11e: cached from `TicketIngested.require_architecture_review`.
    /// When `true`, `apply_plan_proposed` transitions to Architecting
    /// (running an architect agent) instead of EnsuringBranch directly.
    /// Set exactly once at ingestion time; never re-read from the event
    /// log (reducer purity).
    #[serde(default)]
    pub require_architecture_review: bool,

    /// Lifetime count of reviewer rejections — increments on every
    /// `ReviewerOutput { passed: false }`. Compared against
    /// `MAX_REVIEW_ITERATIONS` to halt review thrashing. Not reset on a
    /// reviewer pass; useful for telemetry / post-mortems.
    #[serde(default)]
    pub total_reviewer_rejections: u32,

    /// Feedback from the most recent reviewer rejection. Set on
    /// rejection, threaded into the next coder action's payload, and
    /// cleared once a reviewer pass lands.
    #[serde(default)]
    pub last_review_feedback: Option<String>,

    /// Outstanding action_ids the workflow has emitted, mapped to the
    /// kind it expects to receive an outcome from. Codex round-1 pushback:
    /// failure events must be matched by action_id, not kind, because
    /// multiple instances of the same kind exist over a workflow lifetime.
    #[serde(default)]
    pub pending_action_ids: HashMap<ActionId, ExpectedOutcomeKind>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    #[default]
    Empty, // no event ingested yet
    Triaging,
    Planning,
    Architecting, // M11e: optional review of plan before coding starts
    EnsuringBranch,
    Coding,
    PushingCommit,
    Reviewing,
    SecurityReviewing,
    OpeningPr,
    AwaitingHumanApproval,
    Merged,
    Failed,
}

/// Re-export from events for convenience.
pub use crate::events::TicketRef;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Plan {
    pub tasks: Vec<TaskSpec>,
}

/// Records a non-recoverable workflow halt.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailureInfo {
    pub reason: String,
    /// The action that failed (when known). `None` for non-action-driven
    /// failures (e.g., budget exceeded, validation).
    pub action_id: Option<ActionId>,
    /// Underlying error message from the dispatcher's failure event,
    /// when applicable.
    pub last_error: Option<String>,
}

/// Tag for what kind of outcome event the reducer is waiting on.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedOutcomeKind {
    Triage,
    Planner,
    Architect, // M11e: architect agent output
    EnsureBranch,
    Coder,
    CommitPatch,
    Reviewer,
    SecurityReviewer,
    OpenPr,
}

impl WorkflowState {
    pub fn is_failed(&self) -> bool {
        matches!(self.status, WorkflowStatus::Failed)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self.status, WorkflowStatus::Failed | WorkflowStatus::Merged)
    }

    /// Convenience for budget guard. `true` when a cap is set and we're
    /// at or above it.
    pub fn budget_exhausted(&self) -> bool {
        self.cost_budget_cents
            .is_some_and(|cap| self.cost_consumed_cents >= cap)
    }
}
