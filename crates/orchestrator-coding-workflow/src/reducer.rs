//! `WorkflowReducer` — the pure state transition + action emission logic
//! for the M11b v1 coding workflow.
//!
//! Linear single-task happy path; multi-task plans, review iteration
//! loops, architecture step, timeouts, and failure compensation beyond
//! halt are deferred to M11c+. v1 emits at most one action per event.

use orchestrator_core::{
    decode_action_failed, slug::slugify, Action, ActionId, EventEnvelope, ExecutorError, Reducer,
    EVT_ACTION_FAILED, EVT_ACTION_PROBE_EXHAUSTED,
};
use orchestrator_github::{
    CommitPatchPayload, EnsureBranchPayload, FileChange, OpenPrPayload,
};
use serde_json::Value as Json;

use crate::action_kinds::{
    KIND_AGENT_CODER, KIND_AGENT_PLANNER, KIND_AGENT_REVIEWER, KIND_AGENT_SECURITY_REVIEWER,
    KIND_AGENT_TRIAGE, KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH, KIND_OPEN_PR,
};
use crate::events::{
    decode, BudgetConsumed, CoderOutput, PlanProposed, PrMerged, ReviewerOutput, Severity,
    SecurityReviewerOutput, TicketIngested, TriageCompleted, EVT_BUDGET_CONSUMED,
    EVT_CODER_OUTPUT, EVT_PLAN_PROPOSED, EVT_PR_MERGED, EVT_REVIEWER_OUTPUT,
    EVT_SECURITY_REVIEWER_OUTPUT, EVT_TICKET_INGESTED, EVT_TRIAGE_COMPLETED,
};
use crate::state::{
    ExpectedOutcomeKind, FailureInfo, Plan, WorkflowState, WorkflowStatus,
};

const MAX_BRANCH_SLUG_LEN: usize = 60;
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
/// Matches the schema default and existing fast-sink behavior. M12b's
/// reducer updates bumps this per agent kind for slow-running sinks.
const DEFAULT_MAX_PROBE_ATTEMPTS: u32 = 20;

// We're also subscribed to github outcome events emitted by the github sink.
const EVT_GH_BRANCH_ENSURED: &str = orchestrator_github::EVT_BRANCH_ENSURED;
const EVT_GH_COMMIT_PUSHED: &str = orchestrator_github::EVT_COMMIT_PUSHED;
const EVT_GH_PR_OPENED: &str = orchestrator_github::EVT_PR_OPENED;

pub struct WorkflowReducer;

impl Reducer for WorkflowReducer {
    type State = WorkflowState;

    fn state_version(&self) -> u32 {
        1
    }

    fn reduce(
        &self,
        mut state: Self::State,
        event: &EventEnvelope,
    ) -> Result<Self::State, ExecutorError> {
        // Once the workflow is terminal, ignore further events.
        if state.is_terminal() {
            return Ok(state);
        }

        match event.payload_type.as_str() {
            EVT_TICKET_INGESTED => apply_ticket_ingested(&mut state, event)?,
            EVT_TRIAGE_COMPLETED => apply_triage_completed(&mut state, event)?,
            EVT_PLAN_PROPOSED => apply_plan_proposed(&mut state, event)?,
            EVT_GH_BRANCH_ENSURED => apply_branch_ensured(&mut state, event)?,
            EVT_CODER_OUTPUT => apply_coder_output(&mut state, event)?,
            EVT_GH_COMMIT_PUSHED => apply_commit_pushed(&mut state, event)?,
            EVT_REVIEWER_OUTPUT => apply_reviewer_output(&mut state, event)?,
            EVT_SECURITY_REVIEWER_OUTPUT => apply_security_reviewer_output(&mut state, event)?,
            EVT_GH_PR_OPENED => apply_pr_opened(&mut state, event)?,
            EVT_PR_MERGED => apply_pr_merged(&mut state, event)?,
            EVT_BUDGET_CONSUMED => apply_budget_consumed(&mut state, event)?,
            EVT_ACTION_FAILED | EVT_ACTION_PROBE_EXHAUSTED => apply_action_failed(&mut state, event)?,
            _ => {
                // Unknown event — silently ignore. Per CLAUDE.md rule #9
                // (additive schema), reducers must tolerate unknown event
                // types so future kinds added by the engine don't break
                // in-flight workflows.
            }
        }

        Ok(state)
    }

    fn derive_actions(
        &self,
        new_state: &Self::State,
        triggering_event: &EventEnvelope,
    ) -> Result<Vec<Action>, ExecutorError> {
        // Terminal workflows emit nothing further.
        if new_state.is_terminal() {
            return Ok(vec![]);
        }
        // Budget guard: even if reduce didn't trip it (maybe budget cap was
        // set in this event), refuse to emit further work when over.
        if new_state.budget_exhausted() {
            return Ok(vec![]);
        }

        Ok(match triggering_event.payload_type.as_str() {
            EVT_TICKET_INGESTED if new_state.status == WorkflowStatus::Triaging => {
                vec![build_triage_action(triggering_event, new_state)]
            }
            EVT_TRIAGE_COMPLETED if new_state.status == WorkflowStatus::Planning => {
                vec![build_planner_action(triggering_event, new_state)]
            }
            EVT_PLAN_PROPOSED if new_state.status == WorkflowStatus::EnsuringBranch => {
                vec![build_ensure_branch_action(triggering_event, new_state)?]
            }
            EVT_GH_BRANCH_ENSURED if new_state.status == WorkflowStatus::Coding => {
                vec![build_coder_action(triggering_event, new_state)]
            }
            EVT_CODER_OUTPUT if new_state.status == WorkflowStatus::PushingCommit => {
                vec![build_commit_patch_action(triggering_event, new_state)?]
            }
            EVT_GH_COMMIT_PUSHED if new_state.status == WorkflowStatus::Reviewing => {
                vec![build_reviewer_action(triggering_event, new_state)]
            }
            EVT_REVIEWER_OUTPUT if new_state.status == WorkflowStatus::SecurityReviewing => {
                vec![build_security_reviewer_action(triggering_event, new_state)]
            }
            EVT_SECURITY_REVIEWER_OUTPUT if new_state.status == WorkflowStatus::OpeningPr => {
                vec![build_open_pr_action(triggering_event, new_state)?]
            }
            // AwaitingHumanApproval and Merged: no actions; we wait for
            // the webhook.
            _ => vec![],
        })
    }
}

// ── reduce helpers ──────────────────────────────────────────────────────

fn apply_ticket_ingested(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::Empty {
        // Duplicate ingestion; ignore.
        return Ok(());
    }
    let p: TicketIngested = decode(&event.payload).map_err(decode_err)?;
    state.ticket = Some(p.ticket);
    state.repo = Some(p.repo);
    state.base_branch = Some(p.base_branch);
    state.base_sha = Some(p.base_sha);
    state.cost_budget_cents = p.cost_budget_cents;

    if state.budget_exhausted() {
        halt(state, "budget exhausted at ingestion".into(), None, None);
        return Ok(());
    }
    state.status = WorkflowStatus::Triaging;
    let action_id = ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_AGENT_TRIAGE);
    state
        .pending_action_ids
        .insert(action_id, ExpectedOutcomeKind::Triage);
    Ok(())
}

fn apply_triage_completed(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::Triaging {
        return Ok(());
    }
    let p: TriageCompleted = decode(&event.payload).map_err(decode_err)?;
    state.pending_action_ids.remove(&p.action_id);

    if !p.accepted {
        halt(
            state,
            format!("triage rejected: {}", p.reason.unwrap_or_default()),
            Some(p.action_id),
            None,
        );
        return Ok(());
    }
    state.status = WorkflowStatus::Planning;
    let action_id = ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_AGENT_PLANNER);
    state
        .pending_action_ids
        .insert(action_id, ExpectedOutcomeKind::Planner);
    Ok(())
}

fn apply_plan_proposed(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::Planning {
        return Ok(());
    }
    let p: PlanProposed = decode(&event.payload).map_err(decode_err)?;
    state.pending_action_ids.remove(&p.action_id);

    if p.tasks.len() != 1 {
        halt(
            state,
            format!(
                "M11b v1 supports single-task plans only; got {} tasks",
                p.tasks.len()
            ),
            Some(p.action_id),
            None,
        );
        return Ok(());
    }

    state.plan = Some(Plan { tasks: p.tasks });
    state.current_task = Some(0);
    state.status = WorkflowStatus::EnsuringBranch;
    let action_id =
        ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_ENSURE_BRANCH);
    state
        .pending_action_ids
        .insert(action_id, ExpectedOutcomeKind::EnsureBranch);
    Ok(())
}

fn apply_branch_ensured(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::EnsuringBranch {
        return Ok(());
    }
    // Read branch_name + head_sha from the github outcome.
    let branch_name = event.payload["branch_name"].as_str().map(|s| s.to_string());
    let head_sha = event.payload["head_sha"].as_str().map(|s| s.to_string());
    let action_id = event.payload["action_id"]
        .as_str()
        .map(|s| ActionId(s.to_string()));

    state.branch_name = branch_name;
    state.head_sha = head_sha;
    if let Some(aid) = &action_id {
        state.pending_action_ids.remove(aid);
    }

    state.status = WorkflowStatus::Coding;
    let next_id = ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_AGENT_CODER);
    state
        .pending_action_ids
        .insert(next_id, ExpectedOutcomeKind::Coder);
    Ok(())
}

fn apply_coder_output(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::Coding {
        return Ok(());
    }
    let p: CoderOutput = decode(&event.payload).map_err(decode_err)?;
    state.pending_action_ids.remove(&p.action_id);

    let expected_idx = state.current_task.unwrap_or(0);
    if p.task_idx != expected_idx {
        halt(
            state,
            format!("coder output for task {} but expected {}", p.task_idx, expected_idx),
            Some(p.action_id),
            None,
        );
        return Ok(());
    }

    state.status = WorkflowStatus::PushingCommit;
    let next_id = ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_COMMIT_PATCH);
    state
        .pending_action_ids
        .insert(next_id, ExpectedOutcomeKind::CommitPatch);
    Ok(())
}

fn apply_commit_pushed(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::PushingCommit {
        return Ok(());
    }
    let head_sha = event.payload["head_sha_at_probe"]
        .as_str()
        .or_else(|| event.payload["commit_sha"].as_str())
        .map(|s| s.to_string());
    let action_id = event.payload["action_id"]
        .as_str()
        .map(|s| ActionId(s.to_string()));
    if let Some(s) = head_sha {
        state.head_sha = Some(s);
    }
    if let Some(aid) = &action_id {
        state.pending_action_ids.remove(aid);
    }

    state.status = WorkflowStatus::Reviewing;
    let next_id = ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_AGENT_REVIEWER);
    state
        .pending_action_ids
        .insert(next_id, ExpectedOutcomeKind::Reviewer);
    Ok(())
}

fn apply_reviewer_output(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::Reviewing {
        return Ok(());
    }
    let p: ReviewerOutput = decode(&event.payload).map_err(decode_err)?;
    state.pending_action_ids.remove(&p.action_id);

    if !p.passed {
        halt(
            state,
            format!(
                "reviewer rejected: {}",
                p.feedback.unwrap_or_else(|| "no feedback".into())
            ),
            Some(p.action_id),
            None,
        );
        return Ok(());
    }
    state.status = WorkflowStatus::SecurityReviewing;
    let next_id = ActionId::derive(
        &event.workflow_id,
        event.sequence,
        0,
        KIND_AGENT_SECURITY_REVIEWER,
    );
    state
        .pending_action_ids
        .insert(next_id, ExpectedOutcomeKind::SecurityReviewer);
    Ok(())
}

fn apply_security_reviewer_output(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::SecurityReviewing {
        return Ok(());
    }
    let p: SecurityReviewerOutput = decode(&event.payload).map_err(decode_err)?;
    state.pending_action_ids.remove(&p.action_id);

    let has_blocker = p.findings.iter().any(|f| {
        matches!(f.severity, Severity::High | Severity::Critical)
    });
    if !p.passed || has_blocker {
        halt(
            state,
            "security review blocked merge".into(),
            Some(p.action_id),
            None,
        );
        return Ok(());
    }
    state.status = WorkflowStatus::OpeningPr;
    let next_id = ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_OPEN_PR);
    state
        .pending_action_ids
        .insert(next_id, ExpectedOutcomeKind::OpenPr);
    Ok(())
}

fn apply_pr_opened(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::OpeningPr {
        return Ok(());
    }
    let pr_number = event.payload["pr_number"].as_u64();
    let html_url = event.payload["html_url"].as_str().map(|s| s.to_string());
    let action_id = event.payload["action_id"]
        .as_str()
        .map(|s| ActionId(s.to_string()));

    state.pr_number = pr_number;
    state.pr_html_url = html_url;
    if let Some(aid) = &action_id {
        state.pending_action_ids.remove(aid);
    }
    state.status = WorkflowStatus::AwaitingHumanApproval;
    Ok(())
}

fn apply_pr_merged(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::AwaitingHumanApproval {
        return Ok(());
    }
    let p: PrMerged = decode(&event.payload).map_err(decode_err)?;
    if state.pr_number != Some(p.pr_number) {
        // Wrong PR — ignore.
        return Ok(());
    }
    state.merge_commit_sha = Some(p.merge_commit_sha);
    state.status = WorkflowStatus::Merged;
    Ok(())
}

fn apply_budget_consumed(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    let p: BudgetConsumed = decode(&event.payload).map_err(decode_err)?;
    state.cost_consumed_cents = state.cost_consumed_cents.saturating_add(p.cents);
    if state.budget_exhausted() {
        halt(state, "budget exceeded".into(), Some(p.action_id), None);
    }
    Ok(())
}

fn apply_action_failed(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    let payload = decode_action_failed(&event.payload).ok_or_else(|| {
        ExecutorError::Reducer("malformed action-failed event payload".into())
    })?;
    // Defensive: only halt if this failure is for an action we emitted.
    // The workflow_id boundary already filters at the storage level, so
    // in practice this is always true. The check guards against
    // out-of-band manual events or buggy callers.
    let was_pending = state.pending_action_ids.remove(&payload.action_id).is_some();
    if !was_pending {
        return Ok(());
    }
    halt(
        state,
        format!("action {} failed: {}", payload.kind, payload.last_error),
        Some(payload.action_id),
        Some(payload.last_error),
    );
    Ok(())
}

fn halt(
    state: &mut WorkflowState,
    reason: String,
    action_id: Option<ActionId>,
    last_error: Option<String>,
) {
    state.status = WorkflowStatus::Failed;
    state.failure = Some(FailureInfo {
        reason,
        action_id,
        last_error,
    });
    state.pending_action_ids.clear();
}

fn decode_err(e: serde_json::Error) -> ExecutorError {
    ExecutorError::Reducer(format!("event payload decode failed: {}", e))
}

// ── derive_actions builders ─────────────────────────────────────────────

fn build_triage_action(event: &EventEnvelope, state: &WorkflowState) -> Action {
    let payload = serde_json::json!({
        "ticket": state.ticket,
        "repo": state.repo,
    });
    Action {
        kind: KIND_AGENT_TRIAGE.into(),
        payload,
        delay_seconds: 0,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        max_probe_attempts: DEFAULT_MAX_PROBE_ATTEMPTS,
    }
    .with_event_for_id_check(event)
}

fn build_planner_action(event: &EventEnvelope, state: &WorkflowState) -> Action {
    let payload = serde_json::json!({
        "ticket": state.ticket,
        "repo": state.repo,
    });
    Action {
        kind: KIND_AGENT_PLANNER.into(),
        payload,
        delay_seconds: 0,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        max_probe_attempts: DEFAULT_MAX_PROBE_ATTEMPTS,
    }
    .with_event_for_id_check(event)
}

fn build_ensure_branch_action(
    event: &EventEnvelope,
    state: &WorkflowState,
) -> Result<Action, ExecutorError> {
    let repo = state.repo.clone().ok_or_else(|| {
        ExecutorError::Reducer("ensure_branch requires repo in state".into())
    })?;
    let base_branch = state.base_branch.clone().ok_or_else(|| {
        ExecutorError::Reducer("ensure_branch requires base_branch in state".into())
    })?;
    let base_sha = state.base_sha.clone().ok_or_else(|| {
        ExecutorError::Reducer("ensure_branch requires base_sha in state".into())
    })?;
    let ticket = state.ticket.as_ref().ok_or_else(|| {
        ExecutorError::Reducer("ensure_branch requires ticket in state".into())
    })?;

    // Pre-compute branch_name using the slugify helper. The action_id
    // matches what derive_actions's caller will derive (idx=0).
    let action_id =
        ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_ENSURE_BRANCH);
    let id_short: String = action_id
        .as_str()
        .strip_prefix("act_")
        .unwrap_or(action_id.as_str())
        .chars()
        .take(16)
        .collect();
    let ticket_slug = slugify(&ticket.id, MAX_BRANCH_SLUG_LEN);
    let branch_name = format!("auto/{}/{}", ticket_slug, id_short);

    let payload = EnsureBranchPayload {
        repo,
        base_branch,
        base_sha,
        branch_name,
        ticket_id: ticket.id.clone(),
    };
    Ok(Action {
        kind: KIND_ENSURE_BRANCH.into(),
        payload: serde_json::to_value(&payload).map_err(decode_err)?,
        delay_seconds: 0,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        max_probe_attempts: DEFAULT_MAX_PROBE_ATTEMPTS,
    })
}

fn build_coder_action(_event: &EventEnvelope, state: &WorkflowState) -> Action {
    let task_idx = state.current_task.unwrap_or(0);
    let task = state.plan.as_ref().and_then(|p| p.tasks.get(task_idx));
    let payload = serde_json::json!({
        "ticket": state.ticket,
        "repo": state.repo,
        "task_idx": task_idx,
        "task": task,
    });
    Action {
        kind: KIND_AGENT_CODER.into(),
        payload,
        delay_seconds: 0,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        max_probe_attempts: DEFAULT_MAX_PROBE_ATTEMPTS,
    }
}

fn build_commit_patch_action(
    event: &EventEnvelope,
    state: &WorkflowState,
) -> Result<Action, ExecutorError> {
    let coder_output: CoderOutput = decode(&event.payload).map_err(decode_err)?;
    let repo = state.repo.clone().ok_or_else(|| {
        ExecutorError::Reducer("commit_patch requires repo".into())
    })?;
    let branch = state.branch_name.clone().ok_or_else(|| {
        ExecutorError::Reducer("commit_patch requires branch_name".into())
    })?;
    let head_sha = state.head_sha.clone().ok_or_else(|| {
        ExecutorError::Reducer("commit_patch requires head_sha".into())
    })?;
    let ticket = state.ticket.as_ref().ok_or_else(|| {
        ExecutorError::Reducer("commit_patch requires ticket".into())
    })?;

    let files: Vec<FileChange> = coder_output
        .patch
        .files
        .into_iter()
        .map(|f| FileChange {
            path: f.path,
            mode: f.mode,
            content: f.content,
        })
        .collect();

    let task = state.plan.as_ref().and_then(|p| {
        state.current_task.and_then(|i| p.tasks.get(i))
    });
    let commit_message = match task {
        Some(t) => format!("{}\n\nFor ticket {}.", t.description, ticket.id),
        None => format!("Apply patch for {}.", ticket.id),
    };

    let payload = CommitPatchPayload {
        repo,
        branch,
        expected_parent_sha: head_sha,
        commit_message,
        author: None,
        files,
        ticket_id: ticket.id.clone(),
    };
    Ok(Action {
        kind: KIND_COMMIT_PATCH.into(),
        payload: serde_json::to_value(&payload).map_err(decode_err)?,
        delay_seconds: 0,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        max_probe_attempts: DEFAULT_MAX_PROBE_ATTEMPTS,
    })
}

fn build_reviewer_action(_event: &EventEnvelope, state: &WorkflowState) -> Action {
    let payload = serde_json::json!({
        "ticket": state.ticket,
        "repo": state.repo,
        "branch": state.branch_name,
        "head_sha": state.head_sha,
    });
    Action {
        kind: KIND_AGENT_REVIEWER.into(),
        payload,
        delay_seconds: 0,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        max_probe_attempts: DEFAULT_MAX_PROBE_ATTEMPTS,
    }
}

fn build_security_reviewer_action(_event: &EventEnvelope, state: &WorkflowState) -> Action {
    let payload = serde_json::json!({
        "ticket": state.ticket,
        "repo": state.repo,
        "branch": state.branch_name,
        "head_sha": state.head_sha,
    });
    Action {
        kind: KIND_AGENT_SECURITY_REVIEWER.into(),
        payload,
        delay_seconds: 0,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        max_probe_attempts: DEFAULT_MAX_PROBE_ATTEMPTS,
    }
}

fn build_open_pr_action(
    _event: &EventEnvelope,
    state: &WorkflowState,
) -> Result<Action, ExecutorError> {
    let repo = state.repo.clone().ok_or_else(|| {
        ExecutorError::Reducer("open_pr requires repo".into())
    })?;
    let head_branch = state.branch_name.clone().ok_or_else(|| {
        ExecutorError::Reducer("open_pr requires branch_name".into())
    })?;
    let base_branch = state.base_branch.clone().ok_or_else(|| {
        ExecutorError::Reducer("open_pr requires base_branch".into())
    })?;
    let ticket = state.ticket.as_ref().ok_or_else(|| {
        ExecutorError::Reducer("open_pr requires ticket".into())
    })?;
    let task = state.plan.as_ref().and_then(|p| {
        state.current_task.and_then(|i| p.tasks.get(i))
    });
    let title = match task {
        Some(t) => format!("{}: {}", ticket.id, t.description),
        None => format!("{}: orchestrator-generated changes", ticket.id),
    };
    let body = format!(
        "Generated by the orchestrator for ticket {}.\n\nBranch: `{}`\n",
        ticket.id, head_branch
    );

    let payload = OpenPrPayload {
        repo,
        head_branch,
        base_branch,
        title,
        body,
        draft: false,
        ticket_id: ticket.id.clone(),
    };
    Ok(Action {
        kind: KIND_OPEN_PR.into(),
        payload: serde_json::to_value(&payload).map_err(decode_err)?,
        delay_seconds: 0,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        max_probe_attempts: DEFAULT_MAX_PROBE_ATTEMPTS,
    })
}

// Tiny no-op extension trait for symmetry — keeps the build_* fn signatures
// uniform whether they need the event arg or not.
trait WithEventForIdCheck: Sized {
    fn with_event_for_id_check(self, _event: &EventEnvelope) -> Self {
        self
    }
}
impl WithEventForIdCheck for Action {}

// Avoid unused warnings on `Json` import.
#[allow(dead_code)]
fn _hint_json_used(_v: &Json) {}
