//! `github.close_pr`: close a pull request.
//!
//! No probe. Idempotent — closing a PR that's already closed is a no-op
//! on the GitHub side (PATCH state=closed returns the PR with state=closed
//! either way). Last-write-wins per the M7 contract.
//!
//! No optional closing comment — composability via a separate
//! `post_issue_comment` action.

use orchestrator_core::{AttemptOutcome, ClaimedAction, DispatcherError, SinkUnhealthyReason};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, instrument, warn};

use crate::action::{decode_close_pr, ClosePrPayload};
use crate::auth::GithubAuth;
use crate::client::installation_client;
use crate::errors::{classify_github_error, ErrorClass};
use crate::outcome::pr_closed_event;

#[derive(Debug, Deserialize)]
struct PrCloseResponse {
    state: String,
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn execute(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<AttemptOutcome, DispatcherError> {
    let payload = match decode_close_pr(&action.payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "close_pr: payload validation failed → PermanentFail");
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

    let path = format!(
        "/repos/{}/{}/pulls/{}",
        payload.repo.owner, payload.repo.name, payload.pr_number
    );
    let body = json!({ "state": "closed" });
    let result: octocrab::Result<PrCloseResponse> = octocrab.patch(&path, Some(&body)).await;

    match result {
        Ok(resp) => {
            debug!(pr_number = payload.pr_number, state = %resp.state, "close_pr: ok");
            let event = pr_closed_event(
                &action.workflow_id,
                &action.action_id,
                &payload,
                resp.state,
            );
            Ok(AttemptOutcome::Succeeded {
                external_ref: Some(format!(
                    "{}#{}",
                    payload.repo.full(),
                    payload.pr_number
                )),
                outcome_event: event,
            })
        }
        Err(e) => Ok(map_class_to_outcome(classify_github_error(&e), &payload)),
    }
}

fn map_class_to_outcome(class: ErrorClass, payload: &ClosePrPayload) -> AttemptOutcome {
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
