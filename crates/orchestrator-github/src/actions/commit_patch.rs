//! `github.commit_patch`: push a multi-file commit onto a branch via the
//! Git Data API, idempotent on retries, recoverable from crashes between
//! step 5 (commit creation) and the outcome event write.
//!
//! Six API calls in sequence — see PLAN.md "Milestone 5". On step 6 fast-
//! forward failure (or on step 1 head-mismatch caused by a previous run
//! having landed before the outcome event was written), we call our own
//! `probe` to translate: a buried `Action-Id`-tagged commit means we did
//! land previously, so the outcome event is reconstructed; otherwise the
//! branch genuinely advanced past us and the reducer must re-derive.
//!
//! Probe: `GET /commits?sha={branch}&per_page=50` and scan the last
//! paragraph of each commit message for our `Action-Id:` trailer.
//! `MAX_HISTORY_DEPTH = 50` is the bounded scan depth from PLAN.md.
//!
//! Step 1 head-mismatch is also routed through probe so a successful
//! step-6-but-no-outcome-write crash recovers cleanly on retry. Without
//! that, the second attempt's step-1 read would mistake our own landed
//! commit for an unrelated branch advance.

use orchestrator_core::{
    AttemptOutcome, ClaimedAction, DispatcherError, ExistingResult, SinkUnhealthyReason,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{debug, instrument, warn};

use crate::action::{decode_commit_patch, CommitPatchPayload, FileChange};
use crate::auth::GithubAuth;
use crate::client::installation_client;
use crate::errors::{classify_github_error, ErrorClass};
use crate::outcome::commit_pushed_event;
use crate::trailer::{append_action_id_trailer, extract_action_id_trailer};

const MAX_HISTORY_DEPTH: u32 = 50;

// ── tiny response shapes ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RefResponse {
    object: RefObject,
}
#[derive(Debug, Deserialize)]
struct RefObject {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct GetCommitResponse {
    tree: TreeRef,
}
#[derive(Debug, Deserialize)]
struct TreeRef {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct ShaResponse {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct ListCommit {
    sha: String,
    commit: ListCommitInner,
    parents: Vec<ParentRef>,
}
#[derive(Debug, Deserialize)]
struct ListCommitInner {
    message: String,
}
#[derive(Debug, Deserialize)]
struct ParentRef {
    sha: String,
}

// ── execute ─────────────────────────────────────────────────────────────

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn execute(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<AttemptOutcome, DispatcherError> {
    let payload = match decode_commit_patch(&action.payload) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "commit_patch: payload validation failed → PermanentFail");
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

    // Step 1: read branch HEAD. If it has moved past expected_parent_sha,
    // route through probe to distinguish "we landed previously" from
    // "branch genuinely advanced past us".
    let head_sha = match get_branch_head(&octocrab, &payload).await {
        Ok(Some(sha)) => sha,
        Ok(None) => {
            return Ok(AttemptOutcome::PermanentFail {
                error: format!(
                    "branch '{}' on {} not found — workflow precondition",
                    payload.branch,
                    payload.repo.full(),
                ),
            });
        }
        Err(class) => return Ok(map_class_to_outcome(class, &payload, "step 1 (read branch)")),
    };

    if head_sha != payload.expected_parent_sha {
        debug!(
            head_sha = %head_sha,
            expected = %payload.expected_parent_sha,
            "step 1: branch HEAD differs from expected_parent_sha; probing"
        );
        return Ok(probe_or_fail(
            auth,
            action,
            format!(
                "branch HEAD {} != expected_parent {} and our commit not found in last {} — branch advanced",
                head_sha, payload.expected_parent_sha, MAX_HISTORY_DEPTH
            ),
        )
        .await);
    }

    // Step 2: read the parent commit to capture its tree SHA (base_tree).
    let base_tree_sha = match get_commit_tree(&octocrab, &payload, &payload.expected_parent_sha)
        .await
    {
        Ok(t) => t,
        Err(class) => return Ok(map_class_to_outcome(class, &payload, "step 2 (read parent commit)")),
    };

    // Step 3: post one blob per upserted file. Sequential for simplicity;
    // optimize via parallelism later if profiling motivates it.
    let mut blob_shas: Vec<Option<String>> = Vec::with_capacity(payload.files.len());
    for file in &payload.files {
        if let Some(content) = &file.content {
            let sha = match post_blob(&octocrab, &payload, content).await {
                Ok(s) => s,
                Err(class) => {
                    return Ok(map_class_to_outcome(class, &payload, "step 3 (create blob)"));
                }
            };
            blob_shas.push(Some(sha));
        } else {
            // Deletion has no blob.
            blob_shas.push(None);
        }
    }

    // Step 4: build the new tree.
    let new_tree_sha =
        match post_tree(&octocrab, &payload, &base_tree_sha, &blob_shas).await {
            Ok(t) => t,
            Err(class) => {
                return Ok(map_class_to_outcome(class, &payload, "step 4 (create tree)"));
            }
        };

    // Step 5: create the commit. Embed Action-Id trailer for probe.
    let new_commit_sha = match post_commit(
        &octocrab,
        &payload,
        action,
        &new_tree_sha,
    )
    .await
    {
        Ok(c) => c,
        Err(class) => {
            return Ok(map_class_to_outcome(class, &payload, "step 5 (create commit)"));
        }
    };

    // Step 6: fast-forward update the ref.
    match patch_ref(&octocrab, &payload, &new_commit_sha).await {
        Ok(()) => {
            debug!(
                commit_sha = %new_commit_sha,
                "commit_patch: ref updated"
            );
            Ok(AttemptOutcome::Succeeded {
                external_ref: Some(external_ref(&payload, &new_commit_sha)),
                outcome_event: commit_pushed_event(
                    &action.workflow_id,
                    &action.action_id,
                    &payload,
                    new_commit_sha.clone(),
                    payload.expected_parent_sha.clone(),
                    true, // we just updated the ref to our commit
                    new_commit_sha,
                ),
            })
        }
        Err(ErrorClass::Validation { detail }) | Err(ErrorClass::Conflict { detail })
            if is_fast_forward_failure(&detail) =>
        {
            // 422 / 409 with "not a fast forward" — branch advanced
            // between step 1 and step 6. Translate via probe.
            warn!(detail, "step 6: fast-forward failed; probing for buried commit");
            Ok(probe_or_fail(
                auth,
                action,
                format!(
                    "fast-forward failed and our commit not found in last {} — branch advanced concurrently: {}",
                    MAX_HISTORY_DEPTH, detail
                ),
            )
            .await)
        }
        Err(class) => Ok(map_class_to_outcome(class, &payload, "step 6 (update ref)")),
    }
}

// ── probe ───────────────────────────────────────────────────────────────

#[instrument(skip(auth, action), fields(action_id = %action.action_id))]
pub async fn probe(
    auth: &GithubAuth,
    action: &ClaimedAction,
) -> Result<Option<ExistingResult>, DispatcherError> {
    let payload = decode_commit_patch(&action.payload)
        .map_err(|e| DispatcherError::Sink(format!("payload decode: {}", e)))?;
    let octocrab = installation_client(auth)
        .await
        .map_err(|e| DispatcherError::Sink(format!("installation client: {}", e)))?;

    let path = format!(
        "/repos/{}/{}/commits?sha={}&per_page={}",
        payload.repo.owner, payload.repo.name, payload.branch, MAX_HISTORY_DEPTH
    );
    let result: octocrab::Result<Vec<ListCommit>> = octocrab.get(&path, None::<&()>).await;
    let commits = match result {
        Ok(c) => c,
        Err(e) => match classify_github_error(&e) {
            // 404 here may be "branch missing" or "repo missing". Either way,
            // we know definitively our commit is not on a branch that
            // doesn't exist.
            ErrorClass::NotFound { .. } => return Ok(None),
            other => {
                return Err(DispatcherError::Sink(format!(
                    "probe list-commits failed: {}",
                    other
                )));
            }
        },
    };

    let head_sha = commits.first().map(|c| c.sha.clone());
    let action_id_str = action.action_id.as_str();

    for commit in &commits {
        if let Some(trailer) = extract_action_id_trailer(&commit.commit.message) {
            if trailer == action_id_str {
                let parent_sha = commit
                    .parents
                    .first()
                    .map(|p| p.sha.clone())
                    .unwrap_or_else(|| payload.expected_parent_sha.clone());
                let head_sha_at_probe = head_sha.clone().unwrap_or_else(|| commit.sha.clone());
                let is_at_head = head_sha.as_deref() == Some(commit.sha.as_str());

                let event = commit_pushed_event(
                    &action.workflow_id,
                    &action.action_id,
                    &payload,
                    commit.sha.clone(),
                    parent_sha,
                    is_at_head,
                    head_sha_at_probe,
                );
                return Ok(Some(ExistingResult {
                    external_ref: Some(external_ref(&payload, &commit.sha)),
                    outcome_event: event,
                }));
            }
        }
    }

    // Not found within MAX_HISTORY_DEPTH commits. Per PLAN.md, treat as
    // "definitively did not happen" within the bound. If we did land but
    // are buried beyond 50 commits, execute will fast-forward-fail and
    // get PermanentFail — also correct in spirit (the branch has moved
    // far enough that the patch needs re-derivation).
    Ok(None)
}

// ── helpers ─────────────────────────────────────────────────────────────

/// Fetch the branch HEAD SHA. `Ok(None)` only on 404; other failures
/// classify via `ErrorClass`.
async fn get_branch_head(
    octocrab: &octocrab::Octocrab,
    payload: &CommitPatchPayload,
) -> Result<Option<String>, ErrorClass> {
    let path = format!(
        "/repos/{}/{}/git/ref/heads/{}",
        payload.repo.owner, payload.repo.name, payload.branch
    );
    let r: octocrab::Result<RefResponse> = octocrab.get(&path, None::<&()>).await;
    match r {
        Ok(r) => Ok(Some(r.object.sha)),
        Err(e) => match classify_github_error(&e) {
            ErrorClass::NotFound { .. } => Ok(None),
            other => Err(other),
        },
    }
}

async fn get_commit_tree(
    octocrab: &octocrab::Octocrab,
    payload: &CommitPatchPayload,
    commit_sha: &str,
) -> Result<String, ErrorClass> {
    let path = format!(
        "/repos/{}/{}/git/commits/{}",
        payload.repo.owner, payload.repo.name, commit_sha
    );
    let r: octocrab::Result<GetCommitResponse> = octocrab.get(&path, None::<&()>).await;
    r.map(|c| c.tree.sha).map_err(|e| classify_github_error(&e))
}

async fn post_blob(
    octocrab: &octocrab::Octocrab,
    payload: &CommitPatchPayload,
    content: &str,
) -> Result<String, ErrorClass> {
    let path = format!(
        "/repos/{}/{}/git/blobs",
        payload.repo.owner, payload.repo.name
    );
    let body = json!({ "content": content, "encoding": "utf-8" });
    let r: octocrab::Result<ShaResponse> = octocrab.post(&path, Some(&body)).await;
    r.map(|s| s.sha).map_err(|e| classify_github_error(&e))
}

async fn post_tree(
    octocrab: &octocrab::Octocrab,
    payload: &CommitPatchPayload,
    base_tree_sha: &str,
    blob_shas: &[Option<String>],
) -> Result<String, ErrorClass> {
    let mut tree_entries: Vec<Value> = Vec::with_capacity(payload.files.len());
    for (file, blob_sha) in payload.files.iter().zip(blob_shas.iter()) {
        tree_entries.push(tree_entry(file, blob_sha.as_deref()));
    }
    let path = format!(
        "/repos/{}/{}/git/trees",
        payload.repo.owner, payload.repo.name
    );
    let body = json!({
        "base_tree": base_tree_sha,
        "tree": tree_entries,
    });
    let r: octocrab::Result<ShaResponse> = octocrab.post(&path, Some(&body)).await;
    r.map(|s| s.sha).map_err(|e| classify_github_error(&e))
}

fn tree_entry(file: &FileChange, blob_sha: Option<&str>) -> Value {
    let mode = file.mode.as_deref().unwrap_or("100644");
    match blob_sha {
        Some(sha) => json!({
            "path": file.path,
            "mode": mode,
            "type": "blob",
            "sha": sha,
        }),
        None => json!({
            "path": file.path,
            // Mode is irrelevant for deletion but the field is required.
            "mode": "100644",
            "type": "blob",
            "sha": Value::Null,
        }),
    }
}

async fn post_commit(
    octocrab: &octocrab::Octocrab,
    payload: &CommitPatchPayload,
    action: &ClaimedAction,
    tree_sha: &str,
) -> Result<String, ErrorClass> {
    let message = append_action_id_trailer(&payload.commit_message, &action.action_id);
    let mut body = json!({
        "message": message,
        "parents": [payload.expected_parent_sha],
        "tree": tree_sha,
    });
    // Insert author conditionally; never serialize `author: null` because
    // that confuses GitHub's API.
    if let Some(author) = &payload.author {
        body["author"] = json!({
            "name": author.name,
            "email": author.email,
        });
    }
    let path = format!(
        "/repos/{}/{}/git/commits",
        payload.repo.owner, payload.repo.name
    );
    let r: octocrab::Result<ShaResponse> = octocrab.post(&path, Some(&body)).await;
    r.map(|s| s.sha).map_err(|e| classify_github_error(&e))
}

async fn patch_ref(
    octocrab: &octocrab::Octocrab,
    payload: &CommitPatchPayload,
    new_commit_sha: &str,
) -> Result<(), ErrorClass> {
    let path = format!(
        "/repos/{}/{}/git/refs/heads/{}",
        payload.repo.owner, payload.repo.name, payload.branch
    );
    let body = json!({ "sha": new_commit_sha, "force": false });
    let r: octocrab::Result<Value> = octocrab.patch(&path, Some(&body)).await;
    r.map(|_| ()).map_err(|e| classify_github_error(&e))
}

/// Probe and translate to `Succeeded` if found, `PermanentFail` if not,
/// `TransientFail` if probe transport fails.
async fn probe_or_fail(
    auth: &GithubAuth,
    action: &ClaimedAction,
    fail_msg: String,
) -> AttemptOutcome {
    match probe(auth, action).await {
        Ok(Some(existing)) => AttemptOutcome::Succeeded {
            external_ref: existing.external_ref,
            outcome_event: existing.outcome_event,
        },
        Ok(None) => AttemptOutcome::PermanentFail { error: fail_msg },
        Err(probe_err) => AttemptOutcome::TransientFail {
            error: format!("post-failure probe: {}", probe_err),
        },
    }
}

fn map_class_to_outcome(
    class: ErrorClass,
    payload: &CommitPatchPayload,
    step: &str,
) -> AttemptOutcome {
    match class {
        ErrorClass::AuthenticationFailed { detail } => AttemptOutcome::SinkUnhealthy {
            reason: SinkUnhealthyReason::AuthenticationFailed,
            detail: format!("{}: {}", step, detail),
        },
        ErrorClass::PermissionDenied { detail } => AttemptOutcome::SinkUnhealthy {
            reason: SinkUnhealthyReason::PermissionDenied,
            detail: format!("{}: {}", step, detail),
        },
        ErrorClass::RateLimit { detail } => AttemptOutcome::TransientFail {
            error: format!("{}: rate limit: {}", step, detail),
        },
        ErrorClass::NotFound { detail } => AttemptOutcome::PermanentFail {
            error: format!("{}: not found ({}): {}", step, payload.repo.full(), detail),
        },
        ErrorClass::Conflict { detail } => AttemptOutcome::PermanentFail {
            error: format!("{}: conflict: {}", step, detail),
        },
        ErrorClass::ReferenceAlreadyExists { detail } => {
            // Unexpected at this point in commit_patch — none of our calls
            // create refs. Treat as permanent.
            AttemptOutcome::PermanentFail {
                error: format!("{}: unexpected 'reference already exists': {}", step, detail),
            }
        }
        ErrorClass::Validation { detail } => AttemptOutcome::PermanentFail {
            error: format!("{}: validation: {}", step, detail),
        },
        ErrorClass::Transient { detail } => AttemptOutcome::TransientFail {
            error: format!("{}: {}", step, detail),
        },
        ErrorClass::OtherClient { status, detail } => AttemptOutcome::PermanentFail {
            error: format!("{}: HTTP {}: {}", step, status, detail),
        },
        ErrorClass::Other { detail } => AttemptOutcome::TransientFail {
            error: format!("{}: {}", step, detail),
        },
    }
}

/// GitHub's PATCH /git/refs returns 422 when a non-force update isn't
/// fast-forward; the response text is "Update is not a fast forward".
/// Detect that specifically so we route only that case through probe.
fn is_fast_forward_failure(detail: &str) -> bool {
    let lower = detail.to_ascii_lowercase();
    lower.contains("fast forward") || lower.contains("not a fast")
}

fn external_ref(payload: &CommitPatchPayload, commit_sha: &str) -> String {
    format!(
        "{}:{}@{}",
        payload.repo.full(),
        payload.branch,
        commit_sha
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::FileChange;

    #[test]
    fn tree_entry_for_upsert_includes_blob_sha() {
        let f = FileChange {
            path: "src/main.rs".into(),
            mode: None,
            content: Some("fn main() {}".into()),
        };
        let entry = tree_entry(&f, Some("blob_sha_1"));
        assert_eq!(entry["path"], json!("src/main.rs"));
        assert_eq!(entry["mode"], json!("100644"));
        assert_eq!(entry["type"], json!("blob"));
        assert_eq!(entry["sha"], json!("blob_sha_1"));
    }

    #[test]
    fn tree_entry_for_upsert_respects_explicit_mode() {
        let f = FileChange {
            path: "scripts/run".into(),
            mode: Some("100755".into()),
            content: Some("#!/bin/sh".into()),
        };
        let entry = tree_entry(&f, Some("blob_x"));
        assert_eq!(entry["mode"], json!("100755"));
    }

    #[test]
    fn tree_entry_for_deletion_has_null_sha() {
        let f = FileChange {
            path: "old.txt".into(),
            mode: None,
            content: None,
        };
        let entry = tree_entry(&f, None);
        assert_eq!(entry["sha"], Value::Null);
        assert_eq!(entry["mode"], json!("100644"));
    }

    #[test]
    fn detects_fast_forward_failure_message() {
        assert!(is_fast_forward_failure("Update is not a fast forward"));
        assert!(is_fast_forward_failure("update is not a fast-forward"));
        assert!(is_fast_forward_failure("Cannot fast forward"));
        assert!(!is_fast_forward_failure("Validation failed"));
        assert!(!is_fast_forward_failure(""));
    }
}
