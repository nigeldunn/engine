//! `github.open_pr`: open a pull request, idempotent on retries.
//!
//! Flow (per the classification table in PLAN.md):
//!
//! - `execute`:
//!     - `POST /repos/{owner}/{name}/pulls` with `body` rewritten to append
//!       the `<!-- orchestrator-action: {action_id} -->` marker.
//!     - 201 → `Succeeded` with `already_existed: false`.
//!     - 422 carrying "A pull request already exists for ..." → call
//!       `probe(action)` and translate:
//!         - probe finds our marker → `Succeeded { already_existed: true }`
//!         - probe finds nothing    → `PermanentFail` (a different PR
//!           already occupies head:branch — a collision the workflow
//!           must escalate)
//!         - probe transport error  → `TransientFail`
//!     - other 422 / 404 / 401 / 403 / etc. mapped per the classifier.
//!
//! - `probe` (called by the dispatcher's `find_existing` path):
//!     - `GET /pulls?head={owner}:{branch}&state=all&per_page=100` (paginate
//!       up to `MAX_PROBE_PAGES`).
//!     - For each PR, scan `body` for our action's marker.
//!     - 0 matches → `Ok(None)`.
//!     - 1 match   → `Ok(Some(...))`.
//!     - 2+ matches → `Err(...)` (architecturally impossible; markers come
//!       from blake3-deterministic action ids. Returning Err matches the
//!       safety bar set by `ensure_branch::probe` for branch collisions).

use orchestrator_core::{
    AttemptOutcome, ClaimedAction, DispatcherError, ExistingResult, SinkUnhealthyReason,
};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, instrument, warn};

use crate::action::{decode_open_pr, OpenPrPayload};
use crate::auth::GithubAuth;
use crate::client::installation_client;
use crate::errors::{classify_github_error, ErrorClass};
use crate::marker::{append_action_id_marker, extract_action_id_marker};
use crate::outcome::pr_opened_event;

const MAX_PROBE_PAGES: u32 = 2;
const PROBE_PAGE_SIZE: u32 = 100;

#[derive(Debug, Deserialize)]
struct PrResponse {
    number: u64,
    html_url: String,
    head: PrRef,
    base: PrRef,
    draft: Option<bool>,
    state: String,
    body: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PrRef {
    sha: String,
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn execute(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<AttemptOutcome, DispatcherError> {
    let payload = match decode_open_pr(&action.payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "open_pr: payload validation failed → PermanentFail");
            return Ok(AttemptOutcome::PermanentFail {
                error: format!("payload validation: {}", e),
            });
        }
    };

    let octocrab = match installation_client(auth).await {
        Ok(o) => o,
        Err(e) => {
            return Ok(AttemptOutcome::TransientFail {
                error: format!("installation client: {}", e),
            });
        }
    };

    let body_with_marker = append_action_id_marker(&payload.body, &action.action_id);
    let path = format!(
        "/repos/{}/{}/pulls",
        payload.repo.owner, payload.repo.name
    );
    let request = json!({
        "title": payload.title,
        "body": body_with_marker,
        "head": payload.head_branch,
        "base": payload.base_branch,
        "draft": payload.draft,
    });

    let result: octocrab::Result<PrResponse> = octocrab.post(&path, Some(&request)).await;

    match result {
        Ok(pr) => {
            debug!(pr_number = pr.number, "open_pr: created");
            Ok(succeeded(action, &payload, &pr, false))
        }
        Err(e) => {
            if is_pr_already_exists_failure(&e) {
                warn!("open_pr: 422 'pull request already exists' → probing");
                Ok(probe_or_fail(
                    auth,
                    action,
                    format!(
                        "POST /pulls returned 'pull request already exists' but probe found no PR with our marker — collision: {}",
                        e
                    ),
                )
                .await)
            } else {
                Ok(map_class_to_outcome(classify_github_error(&e), &payload))
            }
        }
    }
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn probe(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<Option<ExistingResult>, DispatcherError> {
    let payload = decode_open_pr(&action.payload)
        .map_err(|e| DispatcherError::Sink(format!("payload decode: {}", e)))?;
    let octocrab = installation_client(auth)
        .await
        .map_err(|e| DispatcherError::Sink(format!("installation client: {}", e)))?;

    let mut pulls: Vec<PrResponse> = Vec::new();
    for page in 1..=MAX_PROBE_PAGES {
        let path = format!(
            "/repos/{}/{}/pulls?head={}:{}&state=all&per_page={}&page={}",
            payload.repo.owner,
            payload.repo.name,
            payload.repo.owner,
            payload.head_branch,
            PROBE_PAGE_SIZE,
            page,
        );
        let result: octocrab::Result<Vec<PrResponse>> = octocrab.get(&path, None::<&()>).await;
        let chunk = match result {
            Ok(v) => v,
            Err(e) => match classify_github_error(&e) {
                // 404 here means repo missing — treat as definitively no PR.
                ErrorClass::NotFound { .. } => return Ok(None),
                other => {
                    return Err(DispatcherError::Sink(format!(
                        "probe list-pulls failed: {}",
                        other
                    )));
                }
            },
        };
        let len = chunk.len();
        pulls.extend(chunk);
        if (len as u32) < PROBE_PAGE_SIZE {
            break;
        }
        if page == MAX_PROBE_PAGES {
            warn!(
                pages = MAX_PROBE_PAGES,
                "open_pr probe: exhausted pagination cap; if your repo legitimately has more matching PRs, raise MAX_PROBE_PAGES"
            );
        }
    }

    let action_id_str = action.action_id.as_str();
    let mut matches: Vec<&PrResponse> = Vec::new();
    for pr in &pulls {
        let body = pr.body.as_deref().unwrap_or("");
        if let Some(extracted) = extract_action_id_marker(body) {
            if extracted == action_id_str {
                matches.push(pr);
            }
        }
    }

    match matches.len() {
        0 => Ok(None),
        1 => {
            let pr = matches[0];
            let event = pr_opened_event(
                &action.workflow_id,
                &action.action_id,
                &payload,
                pr.number,
                pr.html_url.clone(),
                pr.head.sha.clone(),
                pr.base.sha.clone(),
                pr.state.clone(),
                pr.draft.unwrap_or(false),
                true,
            );
            Ok(Some(ExistingResult {
                external_ref: Some(external_ref(&payload, pr.number)),
                outcome_event: event,
                side_events: vec![],
            }))
        }
        n => Err(DispatcherError::Sink(format!(
            "probe found {} PRs carrying marker for action_id {} — should be impossible (blake3-deterministic ids); flag for investigation",
            n, action_id_str
        ))),
    }
}

// ── helpers ─────────────────────────────────────────────────────────────

fn succeeded(
    action: &ClaimedAction,
    payload: &OpenPrPayload,
    pr: &PrResponse,
    already_existed: bool,
) -> AttemptOutcome {
    let event = pr_opened_event(
        &action.workflow_id,
        &action.action_id,
        payload,
        pr.number,
        pr.html_url.clone(),
        pr.head.sha.clone(),
        pr.base.sha.clone(),
        pr.state.clone(),
        pr.draft.unwrap_or(false),
        already_existed,
    );
    AttemptOutcome::Succeeded {
        external_ref: Some(external_ref(payload, pr.number)),
        outcome_event: event,
        side_events: vec![],
    }
}

async fn probe_or_fail(
    auth: &GithubAuth,
    action: &ClaimedAction,
    fail_msg: String,
) -> AttemptOutcome {
    match probe(auth, action).await {
        Ok(Some(existing)) => AttemptOutcome::Succeeded {
            external_ref: existing.external_ref,
            outcome_event: existing.outcome_event,
            side_events: existing.side_events,
        },
        Ok(None) => AttemptOutcome::PermanentFail { error: fail_msg },
        Err(probe_err) => AttemptOutcome::TransientFail {
            error: format!("post-failure probe: {}", probe_err),
        },
    }
}

fn map_class_to_outcome(class: ErrorClass, payload: &OpenPrPayload) -> AttemptOutcome {
    match class {
        ErrorClass::AuthenticationFailed { detail } => AttemptOutcome::SinkUnhealthy {
            reason: SinkUnhealthyReason::AuthenticationFailed,
            detail,
        },
        ErrorClass::PermissionDenied { detail } => AttemptOutcome::SinkUnhealthy {
            reason: SinkUnhealthyReason::PermissionDenied,
            detail,
        },
        ErrorClass::RateLimit { detail } => AttemptOutcome::TransientFail {
            error: format!("rate limit: {}", detail),
        },
        ErrorClass::NotFound { detail } => AttemptOutcome::PermanentFail {
            error: format!(
                "not found ({}/{}): {}",
                payload.repo.full(),
                payload.head_branch,
                detail
            ),
        },
        ErrorClass::Conflict { detail } => AttemptOutcome::PermanentFail {
            error: format!("conflict: {}", detail),
        },
        ErrorClass::ReferenceAlreadyExists { detail } => {
            // Not expected on POST /pulls — this classifier variant is for
            // git ref creation. Treat as permanent.
            AttemptOutcome::PermanentFail {
                error: format!("unexpected 'reference already exists' on POST /pulls: {}", detail),
            }
        }
        ErrorClass::Validation { detail } => AttemptOutcome::PermanentFail {
            error: format!("validation: {}", detail),
        },
        ErrorClass::Transient { detail } => AttemptOutcome::TransientFail { error: detail },
        ErrorClass::OtherClient { status, detail } => AttemptOutcome::PermanentFail {
            error: format!("HTTP {}: {}", status, detail),
        },
        ErrorClass::Other { detail } => AttemptOutcome::TransientFail { error: detail },
    }
}

/// Detect "A pull request already exists for ..." 422 responses. GitHub
/// returns this with a top-level "Validation Failed" message and the
/// specific text inside the `errors[].message` array, so we substring-
/// match against the Debug repr of the whole error to be robust regardless
/// of which field carries the text.
fn is_pr_already_exists_failure(err: &octocrab::Error) -> bool {
    let dbg = format!("{:?}", err).to_ascii_lowercase();
    dbg.contains("pull request already exists")
}

fn external_ref(payload: &OpenPrPayload, pr_number: u64) -> String {
    format!("{}#{}", payload.repo.full(), pr_number)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_ref_format_includes_repo_and_number() {
        let p = OpenPrPayload {
            repo: crate::action::RepoRef {
                owner: "octo".into(),
                name: "world".into(),
            },
            head_branch: "auto/x".into(),
            base_branch: "main".into(),
            title: "[orch-test] x".into(),
            body: "".into(),
            draft: false,
            ticket_id: "ENG-X".into(),
        };
        assert_eq!(external_ref(&p, 7), "octo/world#7");
    }
}
