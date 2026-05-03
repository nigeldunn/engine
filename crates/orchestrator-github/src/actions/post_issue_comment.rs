//! `github.post_issue_comment`: post a comment on an issue (or PR — same
//! `/issues/{n}/comments` endpoint), idempotent on retries via dual marker
//! identity.
//!
//! Marker strategy (PLAN.md):
//! - HTML primary: `<!-- orchestrator-action: {action_id} -->` appended to
//!   the body before POST.
//! - Plain-text fallback: `[orch:{8 hex}]` footer, where the hex is the
//!   first 4 bytes of `sha256(action_id)`. Defends against the (rare) case
//!   where a markdown renderer or comment editor strips HTML comments —
//!   the plain-text footer is robust.
//!
//! Probe scans the issue's recent comments (paginated up to
//! `MAX_COMMENT_PROBE_PAGES = 3` × 100 = 300 comments) and matches either
//! marker form. Multi-match → `Err` (architecturally impossible since
//! action ids are blake3-deterministic; any duplicate marker is a real
//! bug — surface it loudly, matches the M4/M6 collision safety bar).
//!
//! `comment_id` is captured into `external_ref` via the standard
//! `finalize_succeeded` path, but probe uses scan rather than direct
//! GET-by-id. Path-B (extending `ClaimedAction` with `external_ref`) is
//! a future optimization, not v1-blocking — pagination over 300 comments
//! is acceptable.

use orchestrator_core::{
    AttemptOutcome, ClaimedAction, DispatcherError, ExistingResult, SinkUnhealthyReason,
};
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, instrument, warn};

use crate::action::{decode_post_issue_comment, PostIssueCommentPayload};
use crate::auth::GithubAuth;
use crate::client::installation_client;
use crate::errors::{classify_github_error, ErrorClass};
use crate::marker::{append_comment_markers, comment_carries_marker};
use crate::outcome::issue_comment_posted_event;

const MAX_COMMENT_PROBE_PAGES: u32 = 3;
const PROBE_PAGE_SIZE: u32 = 100;

#[derive(Debug, Deserialize)]
struct CommentResponse {
    id: u64,
    html_url: String,
    body: Option<String>,
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn execute(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<AttemptOutcome, DispatcherError> {
    let payload = match decode_post_issue_comment(&action.payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "post_issue_comment: payload validation failed → PermanentFail");
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

    let body_with_markers = append_comment_markers(&payload.body, &action.action_id);
    let path = format!(
        "/repos/{}/{}/issues/{}/comments",
        payload.repo.owner, payload.repo.name, payload.issue_number
    );
    let request = json!({ "body": body_with_markers });
    let result: octocrab::Result<CommentResponse> =
        octocrab.post(&path, Some(&request)).await;

    match result {
        Ok(c) => {
            debug!(comment_id = c.id, "post_issue_comment: created");
            let event = issue_comment_posted_event(
                &action.workflow_id,
                &action.action_id,
                &payload,
                c.id,
                c.html_url,
                false,
            );
            Ok(AttemptOutcome::Succeeded {
                external_ref: Some(c.id.to_string()),
                outcome_event: event,
                side_events: vec![],
            })
        }
        Err(e) => Ok(map_class_to_outcome(classify_github_error(&e), &payload)),
    }
}

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn probe(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<Option<ExistingResult>, DispatcherError> {
    let payload = decode_post_issue_comment(&action.payload)
        .map_err(|e| DispatcherError::Sink(format!("payload decode: {}", e)))?;
    let octocrab = installation_client(auth)
        .await
        .map_err(|e| DispatcherError::Sink(format!("installation client: {}", e)))?;

    let mut comments: Vec<CommentResponse> = Vec::new();
    for page in 1..=MAX_COMMENT_PROBE_PAGES {
        let path = format!(
            "/repos/{}/{}/issues/{}/comments?per_page={}&page={}",
            payload.repo.owner,
            payload.repo.name,
            payload.issue_number,
            PROBE_PAGE_SIZE,
            page
        );
        let result: octocrab::Result<Vec<CommentResponse>> =
            octocrab.get(&path, None::<&()>).await;
        let chunk = match result {
            Ok(v) => v,
            Err(e) => match classify_github_error(&e) {
                // 404 = issue/repo missing; we know definitively no
                // matching comment exists.
                ErrorClass::NotFound { .. } => return Ok(None),
                other => {
                    return Err(DispatcherError::Sink(format!(
                        "probe list-comments failed: {}",
                        other
                    )));
                }
            },
        };
        let len = chunk.len();
        comments.extend(chunk);
        if (len as u32) < PROBE_PAGE_SIZE {
            break;
        }
        if page == MAX_COMMENT_PROBE_PAGES {
            warn!(
                pages = MAX_COMMENT_PROBE_PAGES,
                "post_issue_comment probe: exhausted pagination cap"
            );
        }
    }

    let mut matches: Vec<&CommentResponse> = Vec::new();
    for c in &comments {
        let body = c.body.as_deref().unwrap_or("");
        if comment_carries_marker(body, &action.action_id) {
            matches.push(c);
        }
    }

    match matches.len() {
        0 => Ok(None),
        1 => {
            let c = matches[0];
            let event = issue_comment_posted_event(
                &action.workflow_id,
                &action.action_id,
                &payload,
                c.id,
                c.html_url.clone(),
                true,
            );
            Ok(Some(ExistingResult {
                external_ref: Some(c.id.to_string()),
                outcome_event: event,
                side_events: vec![],
            }))
        }
        n => Err(DispatcherError::Sink(format!(
            "probe found {} comments carrying marker for action_id {} — should be impossible (blake3-deterministic ids); flag for investigation",
            n,
            action.action_id.as_str()
        ))),
    }
}

fn map_class_to_outcome(
    class: ErrorClass,
    payload: &PostIssueCommentPayload,
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
                "issue {}#{} not found: {}",
                payload.repo.full(),
                payload.issue_number,
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
