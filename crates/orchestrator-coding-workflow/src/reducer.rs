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
    KIND_AGENT_ARCHITECT, KIND_AGENT_CODER, KIND_AGENT_PLANNER, KIND_AGENT_REVIEWER,
    KIND_AGENT_SECURITY_REVIEWER, KIND_AGENT_TRIAGE, KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH,
    KIND_OPEN_PR,
};
use crate::events::{
    decode, ArchitectureProposed, BudgetConsumed, CoderOutput, PlanProposed, PrMerged,
    ReviewerOutput, Severity, SecurityReviewerOutput, TicketIngested, TriageCompleted,
    EVT_ARCHITECTURE_PROPOSED, EVT_BUDGET_CONSUMED, EVT_CODER_OUTPUT, EVT_PLAN_PROPOSED,
    EVT_PR_MERGED, EVT_REVIEWER_OUTPUT, EVT_SECURITY_REVIEWER_OUTPUT, EVT_TICKET_INGESTED,
    EVT_TRIAGE_COMPLETED,
};
use crate::state::{
    ExpectedOutcomeKind, FailureInfo, Plan, WorkflowState, WorkflowStatus,
};

const MAX_BRANCH_SLUG_LEN: usize = 60;
/// Default retry budget for github.* actions: fast HTTP calls, transient
/// failures usually clear within a few minutes.
const DEFAULT_MAX_ATTEMPTS: u32 = 5;
const DEFAULT_MAX_PROBE_ATTEMPTS: u32 = 20;

/// Coder runs are the slowest agent in the v1 contract — code generation
/// over potentially many files can run 5-15 minutes per attempt. With
/// `max_attempts = 50` and the default exponential backoff capping at
/// 5 minutes, total wait is 4-5 hours before permanent failure. Probe
/// budget similarly extended.
const CODER_MAX_ATTEMPTS: u32 = 50;
const CODER_MAX_PROBE_ATTEMPTS: u32 = 60;

/// Other agents (triage, planner, reviewer, security) are typically
/// faster — single-digit minutes per attempt. 20 attempts gives ~1-2
/// hours of budget under the default backoff schedule.
const AGENT_MAX_ATTEMPTS: u32 = 20;
const AGENT_MAX_PROBE_ATTEMPTS: u32 = 40;

/// Hard cap on reviewer rejection cycles before the workflow halts.
/// Typical workflows clear in 1-2 iterations; the cap is defensive
/// against runaway agent costs from a perpetually-rejecting reviewer.
const MAX_REVIEW_ITERATIONS: u32 = 5;

/// M11f: cap on the per-action-chain compensation depth for agent.*
/// failures. `1` means each fresh agent action gets at most one
/// retry-from-scratch after exhausting its (already generous) attempt
/// budget — single-shot safety net for transient infrastructure
/// failures, not a prolonged second attempt at a genuinely failing
/// plan. The combined effective budget is therefore 2×
/// max_attempts × backoff_cap (~hours per agent action).
///
/// github.* failures do NOT compensate; they halt the workflow
/// immediately, on the assumption that github 4xx/5xx exhaustion
/// reflects either misconfiguration or a sustained outage that a
/// blind retry won't fix.
const MAX_COMPENSATION_DEPTH: u32 = 1;

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
            EVT_ARCHITECTURE_PROPOSED => apply_architecture_proposed(&mut state, event)?,
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
            // M11e: opt-in architecture review path. apply_plan_proposed
            // sets status = Architecting when require_architecture_review,
            // emit run_architect.
            EVT_PLAN_PROPOSED if new_state.status == WorkflowStatus::Architecting => {
                vec![build_architect_action(triggering_event, new_state)]
            }
            // Architecture approved → ensure_branch (same builder as the
            // direct path; the architect doesn't change the branch shape).
            EVT_ARCHITECTURE_PROPOSED if new_state.status == WorkflowStatus::EnsuringBranch => {
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
            // Multi-task plan: commit landed but more tasks remain →
            // run the coder for the next task. apply_commit_pushed has
            // already advanced state.current_task and set status =
            // Coding, so build_coder_action sees the new task index.
            EVT_GH_COMMIT_PUSHED if new_state.status == WorkflowStatus::Coding => {
                vec![build_coder_action(triggering_event, new_state)]
            }
            EVT_REVIEWER_OUTPUT if new_state.status == WorkflowStatus::SecurityReviewing => {
                vec![build_security_reviewer_action(triggering_event, new_state)]
            }
            // Reviewer rejected: apply_reviewer_output transitioned back to
            // Coding{task=0} with feedback in state. Re-run the coder.
            EVT_REVIEWER_OUTPUT if new_state.status == WorkflowStatus::Coding => {
                vec![build_coder_action(triggering_event, new_state)]
            }
            EVT_SECURITY_REVIEWER_OUTPUT if new_state.status == WorkflowStatus::OpeningPr => {
                vec![build_open_pr_action(triggering_event, new_state)?]
            }
            // M11f: failure compensation. apply_action_failed preserves
            // an agent-waiting status when it pre-registered a fresh
            // pending action; halt sets status = Failed (which
            // is_terminal short-circuits above). So reaching this arm
            // with one of the agent-waiting statuses below implies a
            // compensation was activated — re-emit the matching agent
            // action. The arm dispatches by status alone (Codex
            // round-2 G) because each agent-waiting status maps 1:1 to
            // exactly one agent kind in this reducer; decoding `kind`
            // from the failure payload would be redundant.
            EVT_ACTION_FAILED | EVT_ACTION_PROBE_EXHAUSTED => match new_state.status {
                WorkflowStatus::Triaging => {
                    vec![build_triage_action(triggering_event, new_state)]
                }
                WorkflowStatus::Planning => {
                    vec![build_planner_action(triggering_event, new_state)]
                }
                WorkflowStatus::Architecting => {
                    vec![build_architect_action(triggering_event, new_state)]
                }
                WorkflowStatus::Coding => {
                    vec![build_coder_action(triggering_event, new_state)]
                }
                WorkflowStatus::Reviewing => {
                    vec![build_reviewer_action(triggering_event, new_state)]
                }
                WorkflowStatus::SecurityReviewing => {
                    vec![build_security_reviewer_action(triggering_event, new_state)]
                }
                _ => vec![],
            },
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
    state.require_architecture_review = p.require_architecture_review;

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
    complete_pending(state, &p.action_id);

    if p.indeterminate {
        // Third outcome: agent couldn't decide. Pause in a non-terminal
        // status so operators can intervene without the workflow
        // counting as Failed. We still record FailureInfo so the same
        // dashboards / state queries that surface failures also see
        // escalations (the status field disambiguates the two cases).
        state.status = WorkflowStatus::AwaitingTriageClarification;
        state.failure = Some(FailureInfo {
            reason: format!(
                "triage requested clarification: {}",
                p.reason.unwrap_or_default(),
            ),
            action_id: Some(p.action_id),
            last_error: None,
        });
        // Don't clear pending_action_ids / compensation_depths — there
        // are no pending actions at this point (we just removed the
        // triage one above) and we want the maps stable for any future
        // resume mechanism.
        return Ok(());
    }

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
    complete_pending(state, &p.action_id);

    if p.tasks.is_empty() {
        halt(
            state,
            "planner produced an empty plan (zero tasks)".into(),
            Some(p.action_id),
            None,
        );
        return Ok(());
    }

    state.plan = Some(Plan { tasks: p.tasks });
    state.current_task = Some(0);

    // M11e: opt-in architecture review. Branch here, not in derive_actions,
    // so state.status reflects what we're actually waiting on.
    if state.require_architecture_review {
        state.status = WorkflowStatus::Architecting;
        let action_id =
            ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_AGENT_ARCHITECT);
        state
            .pending_action_ids
            .insert(action_id, ExpectedOutcomeKind::Architect);
    } else {
        state.status = WorkflowStatus::EnsuringBranch;
        let action_id =
            ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_ENSURE_BRANCH);
        state
            .pending_action_ids
            .insert(action_id, ExpectedOutcomeKind::EnsureBranch);
    }
    Ok(())
}

fn apply_architecture_proposed(
    state: &mut WorkflowState,
    event: &EventEnvelope,
) -> Result<(), ExecutorError> {
    if state.status != WorkflowStatus::Architecting {
        return Ok(());
    }
    let p: ArchitectureProposed = decode(&event.payload).map_err(decode_err)?;
    complete_pending(state, &p.action_id);

    if !p.accepted {
        // v1: halt on rejection. M11f could iterate (similar to M11d).
        halt(
            state,
            format!(
                "architecture rejected: {}",
                p.feedback.unwrap_or_else(|| "no feedback".into())
            ),
            Some(p.action_id),
            None,
        );
        return Ok(());
    }
    // Approved → proceed to ensure_branch. Pre-register the next
    // action_id (Codex round-1 E) so failure events for ensure_branch
    // route correctly.
    state.status = WorkflowStatus::EnsuringBranch;
    let next_id =
        ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_ENSURE_BRANCH);
    state
        .pending_action_ids
        .insert(next_id, ExpectedOutcomeKind::EnsureBranch);
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
        complete_pending(state, aid);
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
    complete_pending(state, &p.action_id);

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
        complete_pending(state, aid);
    }

    // Branch on whether more tasks remain. Defensive bounds guard
    // (Codex round-1 H): if current_task somehow exceeds plan.tasks.len(),
    // halt rather than silently advance into undefined slots.
    let total_tasks = state.plan.as_ref().map(|p| p.tasks.len()).unwrap_or(0);
    let current = state.current_task.unwrap_or(0);
    if current >= total_tasks {
        halt(
            state,
            format!(
                "current_task {} out of bounds (plan has {} tasks)",
                current, total_tasks
            ),
            None,
            None,
        );
        return Ok(());
    }

    if current + 1 < total_tasks {
        // More tasks remain — advance to the next coder run.
        state.current_task = Some(current + 1);
        state.status = WorkflowStatus::Coding;
        let next_id =
            ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_AGENT_CODER);
        state
            .pending_action_ids
            .insert(next_id, ExpectedOutcomeKind::Coder);
    } else {
        // All tasks committed — proceed to review.
        state.status = WorkflowStatus::Reviewing;
        let next_id =
            ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_AGENT_REVIEWER);
        state
            .pending_action_ids
            .insert(next_id, ExpectedOutcomeKind::Reviewer);
    }
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
    complete_pending(state, &p.action_id);

    if !p.passed {
        // Iterate: re-run the coder with the reviewer's feedback. Capped
        // to MAX_REVIEW_ITERATIONS to bound runaway agent costs.
        state.total_reviewer_rejections =
            state.total_reviewer_rejections.saturating_add(1);
        if state.total_reviewer_rejections >= MAX_REVIEW_ITERATIONS {
            halt(
                state,
                format!(
                    "review budget exhausted after {} iterations",
                    state.total_reviewer_rejections
                ),
                Some(p.action_id),
                None,
            );
            return Ok(());
        }
        // Pre-register the next coder action_id (Codex round-1 H): the
        // failure-event router matches by action_id against
        // pending_action_ids, so a permanent failure on the rerun must
        // resolve to a halt rather than be silently ignored.
        state.last_review_feedback = p.feedback;
        state.current_task = Some(0);
        state.status = WorkflowStatus::Coding;
        let next_id =
            ActionId::derive(&event.workflow_id, event.sequence, 0, KIND_AGENT_CODER);
        state
            .pending_action_ids
            .insert(next_id, ExpectedOutcomeKind::Coder);
        return Ok(());
    }

    // Passed: clear feedback (no longer relevant). total_reviewer_rejections
    // is left as a workflow-lifetime counter for telemetry.
    state.last_review_feedback = None;
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
    complete_pending(state, &p.action_id);

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
        complete_pending(state, aid);
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
    // Match (repo, pr_number) so a webhook for a fork or unrelated repo with
    // a colliding PR number cannot complete this workflow. Repo comparison
    // is case-insensitive: GitHub normalizes owner/name in API responses,
    // and a user-typed `Octo/World` must still match the canonical
    // `octo/world` carried by the webhook payload.
    let same_repo = state
        .repo
        .as_ref()
        .is_some_and(|r| r.eq_ignore_ascii_case(&p.repo));
    if !same_repo || state.pr_number != Some(p.pr_number) {
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
    // Defensive: only act if this failure is for an action we emitted.
    // The workflow_id boundary already filters at the storage level, so
    // in practice this is always true. The check guards against
    // out-of-band manual events or buggy callers.
    let was_pending = state.pending_action_ids.remove(&payload.action_id).is_some();
    let prior_depth = state
        .action_compensation_depths
        .remove(&payload.action_id)
        .unwrap_or(0);
    if !was_pending {
        return Ok(());
    }

    // M11f: compensate agent.* failures up to MAX_COMPENSATION_DEPTH.
    // Status is preserved so derive_actions emits a fresh action of the
    // same kind. github.* failures and depth-exhausted agent failures
    // fall through to halt.
    if is_compensable_kind(&payload.kind) && prior_depth < MAX_COMPENSATION_DEPTH {
        let next_id = ActionId::derive(
            &event.workflow_id,
            event.sequence,
            0,
            &payload.kind,
        );
        let expected = expected_kind_for_action_kind(&payload.kind).ok_or_else(|| {
            ExecutorError::Reducer(format!(
                "compensable kind '{}' has no expected-outcome mapping",
                payload.kind
            ))
        })?;
        state
            .pending_action_ids
            .insert(next_id.clone(), expected);
        state
            .action_compensation_depths
            .insert(next_id, prior_depth + 1);
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

/// Compensable iff the action targets the agent runner. github.*
/// failures halt unconditionally (see `MAX_COMPENSATION_DEPTH` doc).
fn is_compensable_kind(kind: &str) -> bool {
    kind.starts_with("agent.")
}

fn expected_kind_for_action_kind(kind: &str) -> Option<ExpectedOutcomeKind> {
    match kind {
        KIND_AGENT_TRIAGE => Some(ExpectedOutcomeKind::Triage),
        KIND_AGENT_PLANNER => Some(ExpectedOutcomeKind::Planner),
        KIND_AGENT_ARCHITECT => Some(ExpectedOutcomeKind::Architect),
        KIND_AGENT_CODER => Some(ExpectedOutcomeKind::Coder),
        KIND_AGENT_REVIEWER => Some(ExpectedOutcomeKind::Reviewer),
        KIND_AGENT_SECURITY_REVIEWER => Some(ExpectedOutcomeKind::SecurityReviewer),
        _ => None,
    }
}

/// Drop `action_id` from both pending-action tracking maps. Used by
/// success/outcome handlers (so a compensated action's depth entry is
/// freed once its outcome lands) and by the failure handler. Orphaned
/// depth entries would be operationally harmless — fresh action_ids
/// are derived from a unique (workflow_id, sequence, idx, kind) tuple
/// and never collide with prior chains — but cleaning up keeps tests
/// and snapshot inspection honest.
fn complete_pending(state: &mut WorkflowState, action_id: &ActionId) {
    state.pending_action_ids.remove(action_id);
    state.action_compensation_depths.remove(action_id);
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
    state.action_compensation_depths.clear();
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
        max_attempts: AGENT_MAX_ATTEMPTS,
        max_probe_attempts: AGENT_MAX_PROBE_ATTEMPTS,
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
        max_attempts: AGENT_MAX_ATTEMPTS,
        max_probe_attempts: AGENT_MAX_PROBE_ATTEMPTS,
    }
    .with_event_for_id_check(event)
}

fn build_architect_action(event: &EventEnvelope, state: &WorkflowState) -> Action {
    // Pass plan + ticket context. The architect agent sees what's about
    // to be implemented and can flag concerns before any code is written.
    let payload = serde_json::json!({
        "ticket": state.ticket,
        "repo": state.repo,
        "plan": state.plan,
    });
    Action {
        kind: KIND_AGENT_ARCHITECT.into(),
        payload,
        delay_seconds: 0,
        max_attempts: AGENT_MAX_ATTEMPTS,
        max_probe_attempts: AGENT_MAX_PROBE_ATTEMPTS,
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
        // M11d: review-iteration context. Both fields are always present
        // so the agent service can pattern-match unconditionally;
        // total_reviewer_rejections == 0 + null feedback signals
        // first-time coding rather than a rerun.
        "review_feedback": state.last_review_feedback,
        "total_reviewer_rejections": state.total_reviewer_rejections,
    });
    Action {
        kind: KIND_AGENT_CODER.into(),
        payload,
        delay_seconds: 0,
        max_attempts: CODER_MAX_ATTEMPTS,
        max_probe_attempts: CODER_MAX_PROBE_ATTEMPTS,
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
        max_attempts: AGENT_MAX_ATTEMPTS,
        max_probe_attempts: AGENT_MAX_PROBE_ATTEMPTS,
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
        max_attempts: AGENT_MAX_ATTEMPTS,
        max_probe_attempts: AGENT_MAX_PROBE_ATTEMPTS,
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
