//! `github.set_pr_status`: toggle draft state and/or request reviewers.
//!
//! No probe. Idempotent, last-write-wins (per the M7 contract). The action
//! makes one or two API calls depending on which fields are set:
//!
//! 1. `PATCH /pulls/{n}` with `draft: bool` (only when `draft.is_some()`).
//! 2. `POST /pulls/{n}/requested_reviewers` (only when non-empty).
//!
//! The outcome event records canonical state from the LAST response; this
//! captures the PR after both calls when both are made.
//!
//! Partial-success caveat: if call 1 succeeds and call 2 fails, the action
//! returns `TransientFail` and the dispatcher's retry re-runs both. Call 1
//! is idempotent (PATCH `draft: x`); call 2 is additive on the GitHub side
//! (re-POSTing the same reviewer is a no-op when they're already requested).

use orchestrator_core::{AttemptOutcome, ClaimedAction, DispatcherError, SinkUnhealthyReason};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, instrument, warn};

use crate::action::{decode_set_pr_status, SetPrStatusPayload};
use crate::auth::GithubAuth;
use crate::client::installation_client;
use crate::errors::{classify_github_error, ErrorClass};
use crate::outcome::pr_status_set_event;

#[derive(Debug, Deserialize)]
struct PrFullResponse {
    draft: Option<bool>,
    requested_reviewers: Option<Vec<UserRef>>,
}

#[derive(Debug, Deserialize)]
struct UserRef {
    login: String,
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn execute(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<AttemptOutcome, DispatcherError> {
    let payload = match decode_set_pr_status(&action.payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "set_pr_status: payload validation failed → PermanentFail");
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

    let pr_path = format!(
        "/repos/{}/{}/pulls/{}",
        payload.repo.owner, payload.repo.name, payload.pr_number
    );
    let mut last_response: Option<PrFullResponse> = None;

    // Call 1: PATCH draft.
    if let Some(draft) = payload.draft {
        let body = json!({ "draft": draft });
        let result: octocrab::Result<PrFullResponse> =
            octocrab.patch(&pr_path, Some(&body)).await;
        match result {
            Ok(resp) => {
                debug!(pr_number = payload.pr_number, draft, "set_pr_status: PATCH draft ok");
                last_response = Some(resp);
            }
            Err(e) => {
                return Ok(map_class_to_outcome(classify_github_error(&e), &payload));
            }
        }
    }

    // Call 2: POST reviewers.
    if !payload.requested_reviewers.is_empty() {
        let reviewers_path = format!(
            "/repos/{}/{}/pulls/{}/requested_reviewers",
            payload.repo.owner, payload.repo.name, payload.pr_number
        );
        let body = json!({ "reviewers": payload.requested_reviewers });
        let result: octocrab::Result<PrFullResponse> =
            octocrab.post(&reviewers_path, Some(&body)).await;
        match result {
            Ok(resp) => {
                debug!(
                    pr_number = payload.pr_number,
                    "set_pr_status: POST requested_reviewers ok"
                );
                last_response = Some(resp);
            }
            Err(e) => {
                return Ok(map_class_to_outcome(classify_github_error(&e), &payload));
            }
        }
    }

    let resp = last_response.expect(
        "validation ensures at least one of draft or requested_reviewers is set, \
         so at least one API call ran",
    );
    let observed_draft = resp.draft.unwrap_or(false);
    let observed_reviewers = resp
        .requested_reviewers
        .unwrap_or_default()
        .into_iter()
        .map(|u| u.login)
        .collect();
    let event = pr_status_set_event(
        &action.workflow_id,
        &action.action_id,
        &payload,
        observed_draft,
        observed_reviewers,
    );
    Ok(AttemptOutcome::Succeeded {
        external_ref: Some(format!(
            "{}#{}",
            payload.repo.full(),
            payload.pr_number
        )),
        outcome_event: event,
        side_events: vec![],
    })
}

fn map_class_to_outcome(class: ErrorClass, payload: &SetPrStatusPayload) -> AttemptOutcome {
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
                "PR {}#{} not found: {}",
                payload.repo.full(),
                payload.pr_number,
                detail
            ),
        },
        ErrorClass::Conflict { detail } => AttemptOutcome::PermanentFail {
            error: format!("conflict: {}", detail),
        },
        ErrorClass::Validation { detail } => AttemptOutcome::PermanentFail {
            error: format!("validation: {}", detail),
        },
        ErrorClass::ReferenceAlreadyExists { detail } => AttemptOutcome::PermanentFail {
            error: format!("unexpected 'reference already exists': {}", detail),
        },
        ErrorClass::Transient { detail } => AttemptOutcome::TransientFail { error: detail },
        ErrorClass::OtherClient { status, detail } => AttemptOutcome::PermanentFail {
            error: format!("HTTP {}: {}", status, detail),
        },
        ErrorClass::Other { detail } => AttemptOutcome::TransientFail { error: detail },
    }
}
