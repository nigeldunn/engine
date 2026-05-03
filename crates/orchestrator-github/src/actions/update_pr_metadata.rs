//! `github.update_pr_metadata`: PATCH a PR's title and/or body.
//!
//! No probe. Idempotent, last-write-wins: the orchestrator owns
//! orchestrator-managed PR metadata (the M6 marker tells humans so), and on
//! retry we re-apply the same intent. If a human edits between our first
//! and second PATCH, our PATCH wins. That trade-off is documented; in
//! exchange we keep the implementation tiny and don't need a "compare
//! desired vs current" probe.
//!
//! The outcome event records the applied state from the PATCH response,
//! matching the convention from `open_pr` and `commit_patch`.

use orchestrator_core::{AttemptOutcome, ClaimedAction, DispatcherError, SinkUnhealthyReason};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, instrument, warn};

use crate::action::{decode_update_pr_metadata, UpdatePrMetadataPayload};
use crate::auth::GithubAuth;
use crate::client::installation_client;
use crate::errors::{classify_github_error, ErrorClass};
use crate::outcome::pr_metadata_updated_event;

#[derive(Debug, Deserialize)]
struct PrPatchResponse {
    title: String,
    body: Option<String>,
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn execute(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<AttemptOutcome, DispatcherError> {
    let payload = match decode_update_pr_metadata(&action.payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "update_pr_metadata: payload validation failed → PermanentFail");
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

    // Build PATCH body with only the fields the reducer wanted to set.
    let mut request = json!({});
    if let Some(t) = &payload.title {
        request["title"] = json!(t);
    }
    if let Some(b) = &payload.body {
        request["body"] = json!(b);
    }

    let path = format!(
        "/repos/{}/{}/pulls/{}",
        payload.repo.owner, payload.repo.name, payload.pr_number
    );
    let result: octocrab::Result<PrPatchResponse> =
        octocrab.patch(&path, Some(&request)).await;

    match result {
        Ok(resp) => {
            debug!(pr_number = payload.pr_number, "update_pr_metadata: PATCH succeeded");
            let event = pr_metadata_updated_event(
                &action.workflow_id,
                &action.action_id,
                &payload,
                resp.title,
                resp.body.unwrap_or_default(),
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

fn map_class_to_outcome(
    class: ErrorClass,
    payload: &UpdatePrMetadataPayload,
) -> AttemptOutcome {
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
