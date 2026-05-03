//! `github.ensure_branch`: create a branch at a known base SHA, idempotently.
//!
//! Flow (per the classification table in PLAN.md):
//!
//! - `execute`:
//!     - `POST /repos/{owner}/{name}/git/refs` with `ref = refs/heads/{branch}`
//!       and `sha = base_sha`.
//!     - 201 → `Succeeded` with `already_existed: false`.
//!     - 422 "Reference already exists" → `read_branch_head` and translate:
//!         - head matches base_sha → `Succeeded` with `already_existed: true`.
//!         - head differs from base_sha → `PermanentFail` (collision).
//!         - 404 from probe (race) → `TransientFail`.
//!         - probe transport error → `TransientFail`.
//!     - everything else → mapped via `classify_github_error`.
//!
//! - `probe` (called by the dispatcher's `find_existing` path on attempt > 0):
//!     - `GET /repos/{owner}/{name}/git/ref/heads/{branch}`.
//!     - 200, head == base_sha → `Ok(Some(BranchEnsured{ already_existed: true }))`.
//!     - 200, head != base_sha → `Err(...)` (collision).
//!         - **TODO(M5/M6)**: extend the probe return type so a definitive
//!           permanent conflict surfaces as a fast `PermanentFail` from this
//!           path too, instead of going through `failed_probe_exhausted`.
//!     - 404 → `Ok(None)` (definitively did not happen).
//!     - 401/403/5xx/network → `Err(...)` (probe failed; do NOT execute).

use orchestrator_core::{
    AttemptOutcome, ClaimedAction, DispatcherError, ExistingResult, SinkUnhealthyReason,
};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, instrument, warn};

use crate::action::{decode_ensure_branch, EnsureBranchPayload};
use crate::auth::GithubAuth;
use crate::client::installation_client;
use crate::errors::{classify_github_error, ErrorClass};
use crate::outcome::branch_ensured_event;

/// Minimal slice of the GitHub `Reference` response we care about.
#[derive(Debug, Deserialize)]
struct RefResponse {
    object: RefObject,
}

#[derive(Debug, Deserialize)]
struct RefObject {
    sha: String,
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn execute(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<AttemptOutcome, DispatcherError> {
    let payload = match decode_ensure_branch(&action.payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "ensure_branch: payload validation failed → PermanentFail");
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
        "/repos/{}/{}/git/refs",
        payload.repo.owner, payload.repo.name
    );
    let body = json!({
        "ref": format!("refs/heads/{}", payload.branch_name),
        "sha": payload.base_sha,
    });

    let result: octocrab::Result<serde_json::Value> = octocrab.post(&path, Some(&body)).await;

    match result {
        Ok(_) => {
            debug!(
                repo = %payload.repo.full(),
                branch = %payload.branch_name,
                "ensure_branch: created"
            );
            Ok(succeeded(
                action,
                &payload,
                payload.base_sha.clone(),
                false,
            ))
        }
        Err(e) => Ok(map_create_error(e, auth, action, &payload).await),
    }
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn probe(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<Option<ExistingResult>, DispatcherError> {
    let payload = decode_ensure_branch(&action.payload)
        .map_err(|e| DispatcherError::Sink(format!("payload decode: {}", e)))?;

    let octocrab = installation_client(auth)
        .await
        .map_err(|e| DispatcherError::Sink(format!("installation client: {}", e)))?;

    match read_branch_head(&octocrab, &payload).await? {
        None => Ok(None),
        Some(head_sha) if head_sha == payload.base_sha => {
            let event =
                branch_ensured_event(&action.workflow_id, &action.action_id, &payload, head_sha, true);
            Ok(Some(ExistingResult {
                external_ref: Some(external_ref(&payload)),
                outcome_event: event,
                side_events: vec![],
            }))
        }
        Some(other_sha) => {
            warn!(
                repo = %payload.repo.full(),
                branch = %payload.branch_name,
                head_sha = %other_sha,
                base_sha = %payload.base_sha,
                "ensure_branch probe: collision — branch exists at different SHA"
            );
            // Per Option 1: collision returns Err from probe. The dispatcher
            // records a probe failure and eventually transitions the action
            // to failed_probe_exhausted. Execute-discovered collisions get
            // fast PermanentFail via map_create_error below.
            Err(DispatcherError::Sink(format!(
                "branch {}/{} exists at SHA {} but base_sha is {} — collision",
                payload.repo.full(),
                payload.branch_name,
                other_sha,
                payload.base_sha
            )))
        }
    }
}

/// Read the current head SHA of the branch. Returns `Ok(None)` on 404 (no
/// branch), `Err` on transport/auth/permission failures.
async fn read_branch_head(
    octocrab: &octocrab::Octocrab,
    payload: &EnsureBranchPayload,
) -> Result<Option<String>, DispatcherError> {
    let path = format!(
        "/repos/{}/{}/git/ref/heads/{}",
        payload.repo.owner, payload.repo.name, payload.branch_name
    );
    let result: octocrab::Result<RefResponse> = octocrab.get(&path, None::<&()>).await;
    match result {
        Ok(r) => Ok(Some(r.object.sha)),
        Err(e) => match classify_github_error(&e) {
            ErrorClass::NotFound { .. } => Ok(None),
            other => Err(DispatcherError::Sink(format!(
                "branch-head read failed: {}",
                other
            ))),
        },
    }
}

/// Map a `POST /git/refs` failure to an `AttemptOutcome`.
///
/// The interesting branch is `ReferenceAlreadyExists`: we re-read the branch
/// head and translate based on whether it matches our `base_sha`. This
/// gives execute fast-PermanentFail on collision (rather than spinning
/// through the dispatcher's slow `failed_probe_exhausted` path).
async fn map_create_error(
    err: octocrab::Error,
    auth: &GithubAuth,
    action: &ClaimedAction,
    payload: &EnsureBranchPayload,
) -> AttemptOutcome {
    match classify_github_error(&err) {
        ErrorClass::ReferenceAlreadyExists { .. } => {
            // We have proof the branch exists; verify it's at our base_sha.
            // Use a fresh client so transient post-write read-after-write
            // delays don't carry forward.
            let octocrab = match installation_client(auth).await {
                Ok(o) => o,
                Err(e) => {
                    return AttemptOutcome::TransientFail {
                        error: format!("post-422 client build: {}", e),
                    };
                }
            };
            match read_branch_head(&octocrab, payload).await {
                Ok(Some(head_sha)) if head_sha == payload.base_sha => {
                    debug!("ensure_branch: 422 then probe match → idempotent recovery");
                    succeeded(action, payload, head_sha, true)
                }
                Ok(Some(head_sha)) => {
                    warn!(
                        head_sha = %head_sha,
                        base_sha = %payload.base_sha,
                        "ensure_branch: 422 then probe mismatch → PermanentFail (collision)"
                    );
                    AttemptOutcome::PermanentFail {
                        error: format!(
                            "branch {}/{} exists at SHA {} but base_sha is {} — collision",
                            payload.repo.full(),
                            payload.branch_name,
                            head_sha,
                            payload.base_sha
                        ),
                    }
                }
                Ok(None) => AttemptOutcome::TransientFail {
                    error: "POST returned 422 'Reference already exists' but probe found no branch — race"
                        .into(),
                },
                Err(probe_err) => AttemptOutcome::TransientFail {
                    error: format!("post-422 probe: {}", probe_err),
                },
            }
        }
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
            // For POST /repos/{owner}/{name}/git/refs, 404 means the repo
            // doesn't exist (workflow precondition). Per the table, that's
            // PermanentFail — needs a human.
            error: format!("repo {} not found: {}", payload.repo.full(), detail),
        },
        ErrorClass::Conflict { detail } => AttemptOutcome::PermanentFail {
            error: format!("conflict: {}", detail),
        },
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

fn succeeded(
    action: &ClaimedAction,
    payload: &EnsureBranchPayload,
    head_sha: String,
    already_existed: bool,
) -> AttemptOutcome {
    let event = branch_ensured_event(
        &action.workflow_id,
        &action.action_id,
        payload,
        head_sha,
        already_existed,
    );
    AttemptOutcome::Succeeded {
        external_ref: Some(external_ref(payload)),
        outcome_event: event,
        side_events: vec![],
    }
}

fn external_ref(payload: &EnsureBranchPayload) -> String {
    format!("{}:{}", payload.repo.full(), payload.branch_name)
}

