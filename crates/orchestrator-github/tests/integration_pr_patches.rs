//! Integration tests for the M7 PATCH triple: `update_pr_metadata`,
//! `set_pr_status`, `close_pr`.
//!
//! All tests are `#[ignore]`d. Activate with the standard env vars:
//!
//!   - `GITHUB_APP_ID`
//!   - `GITHUB_PRIVATE_KEY_PEM`
//!   - `GITHUB_INSTALLATION_ID`
//!   - `GITHUB_TEST_REPO_OWNER`
//!   - `GITHUB_TEST_REPO_NAME`
//!
//! ```sh
//! cargo test -p orchestrator-github -- --ignored
//! ```
//!
//! Each test creates a unique branch, lands one commit, opens a PR via raw
//! API, runs the action against that PR, then best-effort closes the PR
//! and deletes the branch.

use chrono::Utc;
use orchestrator_core::{
    ActionId, AttemptOutcome, ClaimedAction, DispatcherId, WorkflowId,
};
use orchestrator_github::actions::{close_pr, set_pr_status, update_pr_metadata};
use orchestrator_github::{
    installation_client, ClosePrPayload, GithubAuth, RepoRef, SetPrStatusPayload,
    UpdatePrMetadataPayload, KIND_CLOSE_PR, KIND_SET_PR_STATUS, KIND_UPDATE_PR_METADATA,
};
use serde_json::{json, Value};
use std::sync::Arc;

struct TestCtx {
    auth: Arc<GithubAuth>,
    repo: RepoRef,
    main_sha: String,
}

async fn setup() -> Option<TestCtx> {
    let _ = tracing_subscriber::fmt::try_init();
    let app_id: u64 = std::env::var("GITHUB_APP_ID").ok()?.parse().ok()?;
    let pem = std::env::var("GITHUB_PRIVATE_KEY_PEM").ok()?;
    let inst: u64 = std::env::var("GITHUB_INSTALLATION_ID").ok()?.parse().ok()?;
    let owner = std::env::var("GITHUB_TEST_REPO_OWNER").ok()?;
    let name = std::env::var("GITHUB_TEST_REPO_NAME").ok()?;
    let auth = Arc::new(GithubAuth::new(app_id, &pem, inst).ok()?);
    let repo = RepoRef { owner, name };
    let octocrab = installation_client(&auth).await.ok()?;
    let main_sha = read_branch_head(&octocrab, &repo, "main").await?;
    Some(TestCtx {
        auth,
        repo,
        main_sha,
    })
}

async fn read_branch_head(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Resp {
        object: Obj,
    }
    #[derive(serde::Deserialize)]
    struct Obj {
        sha: String,
    }
    let path = format!("/repos/{}/{}/git/ref/heads/{}", repo.owner, repo.name, branch);
    let r: octocrab::Result<Resp> = octocrab.get(&path, None::<&()>).await;
    r.ok().map(|x| x.object.sha)
}

async fn create_branch(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
    sha: &str,
) -> Result<(), String> {
    let path = format!("/repos/{}/{}/git/refs", repo.owner, repo.name);
    let body = json!({ "ref": format!("refs/heads/{}", branch), "sha": sha });
    let r: octocrab::Result<Value> = octocrab.post(&path, Some(&body)).await;
    r.map(|_| ()).map_err(|e| e.to_string())
}

async fn delete_branch_best_effort(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
) {
    let path = format!("/repos/{}/{}/git/refs/heads/{}", repo.owner, repo.name, branch);
    let _ = octocrab._delete(&path, None::<&()>).await;
}

async fn close_pr_best_effort(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    pr_number: u64,
) {
    let path = format!("/repos/{}/{}/pulls/{}", repo.owner, repo.name, pr_number);
    let body = json!({ "state": "closed" });
    let _: octocrab::Result<Value> = octocrab.patch(&path, Some(&body)).await;
}

async fn land_one_commit(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
    parent_sha: &str,
    file_path: &str,
    file_content: &str,
) -> Result<String, String> {
    let parent: Value = octocrab
        .get(
            &format!("/repos/{}/{}/git/commits/{}", repo.owner, repo.name, parent_sha),
            None::<&()>,
        )
        .await
        .map_err(|e| e.to_string())?;
    let base_tree = parent["tree"]["sha"].as_str().ok_or("no parent tree")?.to_string();

    let blob: Value = octocrab
        .post(
            format!("/repos/{}/{}/git/blobs", repo.owner, repo.name),
            Some(&json!({ "content": file_content, "encoding": "utf-8" })),
        )
        .await
        .map_err(|e| e.to_string())?;
    let blob_sha = blob["sha"].as_str().ok_or("no blob sha")?.to_string();

    let tree: Value = octocrab
        .post(
            format!("/repos/{}/{}/git/trees", repo.owner, repo.name),
            Some(&json!({
                "base_tree": base_tree,
                "tree": [{ "path": file_path, "mode": "100644", "type": "blob", "sha": blob_sha }],
            })),
        )
        .await
        .map_err(|e| e.to_string())?;
    let tree_sha = tree["sha"].as_str().ok_or("no tree sha")?.to_string();

    let commit: Value = octocrab
        .post(
            format!("/repos/{}/{}/git/commits", repo.owner, repo.name),
            Some(&json!({
                "message": "[orch-test] m7 setup commit",
                "parents": [parent_sha],
                "tree": tree_sha,
            })),
        )
        .await
        .map_err(|e| e.to_string())?;
    let commit_sha = commit["sha"].as_str().ok_or("no commit sha")?.to_string();

    let _: Value = octocrab
        .patch(
            &format!("/repos/{}/{}/git/refs/heads/{}", repo.owner, repo.name, branch),
            Some(&json!({ "sha": commit_sha, "force": false })),
        )
        .await
        .map_err(|e| e.to_string())?;
    Ok(commit_sha)
}

async fn open_pr_raw(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    head_branch: &str,
    base_branch: &str,
    title: &str,
    body: &str,
) -> Result<u64, String> {
    let path = format!("/repos/{}/{}/pulls", repo.owner, repo.name);
    let req = json!({
        "title": title,
        "body": body,
        "head": head_branch,
        "base": base_branch,
    });
    let pr: Value = octocrab.post(&path, Some(&req)).await.map_err(|e| e.to_string())?;
    pr["number"]
        .as_u64()
        .ok_or_else(|| "no pr number in response".to_string())
}

async fn read_pr(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    pr_number: u64,
) -> Option<Value> {
    let path = format!("/repos/{}/{}/pulls/{}", repo.owner, repo.name, pr_number);
    octocrab.get(&path, None::<&()>).await.ok()
}

fn unique_branch(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{}/{}", prefix, &id[..16])
}

async fn open_test_pr(ctx: &TestCtx) -> (octocrab::Octocrab, String, u64) {
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/m7");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();
    let _ = land_one_commit(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        &format!("orch-test/m7-{}.txt", &branch[branch.len() - 8..]),
        "m7\n",
    )
    .await
    .unwrap();
    let pr_number = open_pr_raw(
        &octocrab,
        &ctx.repo,
        &branch,
        "main",
        "[orch-test] m7 fixture",
        "Fixture body — to be modified by tests.",
    )
    .await
    .unwrap();
    (octocrab, branch, pr_number)
}

fn make_action_with_kind(kind: &str, payload: Value) -> ClaimedAction {
    let workflow_id = WorkflowId::new("test-wf-m7");
    let action_id = ActionId::derive(&workflow_id, 0, 0, kind);
    ClaimedAction {
        action_id,
        workflow_id,
        source_sequence: 0,
        kind: kind.into(),
        payload,
        attempt: 0,
        max_attempts: 5,
        probe_attempt: 0,
        max_probe_attempts: 20,
        claimed_by: DispatcherId::new(),
        lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
    }
}

// ── update_pr_metadata ──────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn update_pr_metadata_applies_title_and_body() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = UpdatePrMetadataPayload {
        repo: ctx.repo.clone(),
        pr_number,
        title: Some("[orch-test] updated title".into()),
        body: Some("Updated body content.".into()),
        ticket_id: "ENG-M7-META".into(),
    };
    let action = make_action_with_kind(
        KIND_UPDATE_PR_METADATA,
        serde_json::to_value(&payload).unwrap(),
    );

    let outcome = update_pr_metadata::execute(&ctx.auth, &action).await.unwrap();
    let event = match outcome {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected Succeeded, got {:?}", other),
    };
    assert_eq!(
        event.payload["title"].as_str(),
        Some("[orch-test] updated title")
    );
    assert_eq!(event.payload["body"].as_str(), Some("Updated body content."));

    // Verify via raw read.
    let pr = read_pr(&octocrab, &ctx.repo, pr_number).await.unwrap();
    assert_eq!(pr["title"].as_str(), Some("[orch-test] updated title"));
    assert_eq!(pr["body"].as_str(), Some("Updated body content."));

    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn update_pr_metadata_idempotent_re_apply() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = UpdatePrMetadataPayload {
        repo: ctx.repo.clone(),
        pr_number,
        title: Some("[orch-test] idem title".into()),
        body: None,
        ticket_id: "ENG-M7-IDEM".into(),
    };
    let action = make_action_with_kind(
        KIND_UPDATE_PR_METADATA,
        serde_json::to_value(&payload).unwrap(),
    );

    // First apply.
    let _ = update_pr_metadata::execute(&ctx.auth, &action).await.unwrap();
    // Second apply — same intent, idempotent server-side.
    let outcome = update_pr_metadata::execute(&ctx.auth, &action).await.unwrap();
    assert!(matches!(outcome, AttemptOutcome::Succeeded { .. }));

    let pr = read_pr(&octocrab, &ctx.repo, pr_number).await.unwrap();
    assert_eq!(pr["title"].as_str(), Some("[orch-test] idem title"));

    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

// ── set_pr_status ───────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn set_pr_status_toggles_draft() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    // PR was opened as non-draft (raw open_pr_raw didn't set draft). Mark it draft.
    let payload = SetPrStatusPayload {
        repo: ctx.repo.clone(),
        pr_number,
        draft: Some(true),
        requested_reviewers: vec![],
        ticket_id: "ENG-M7-DRAFT".into(),
    };
    let action = make_action_with_kind(
        KIND_SET_PR_STATUS,
        serde_json::to_value(&payload).unwrap(),
    );

    let outcome = set_pr_status::execute(&ctx.auth, &action).await.unwrap();
    let event = match outcome {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected Succeeded, got {:?}", other),
    };
    assert_eq!(event.payload["draft"].as_bool(), Some(true));

    let pr = read_pr(&octocrab, &ctx.repo, pr_number).await.unwrap();
    assert_eq!(pr["draft"].as_bool(), Some(true));

    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

// ── close_pr ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn close_pr_transitions_state() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = ClosePrPayload {
        repo: ctx.repo.clone(),
        pr_number,
        ticket_id: "ENG-M7-CLOSE".into(),
    };
    let action = make_action_with_kind(KIND_CLOSE_PR, serde_json::to_value(&payload).unwrap());

    let outcome = close_pr::execute(&ctx.auth, &action).await.unwrap();
    let event = match outcome {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected Succeeded, got {:?}", other),
    };
    assert_eq!(event.payload["state"].as_str(), Some("closed"));

    let pr = read_pr(&octocrab, &ctx.repo, pr_number).await.unwrap();
    assert_eq!(pr["state"].as_str(), Some("closed"));

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn close_pr_idempotent_when_already_closed() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = ClosePrPayload {
        repo: ctx.repo.clone(),
        pr_number,
        ticket_id: "ENG-M7-CLOSE-IDEM".into(),
    };
    let action = make_action_with_kind(KIND_CLOSE_PR, serde_json::to_value(&payload).unwrap());

    // First close.
    let _ = close_pr::execute(&ctx.auth, &action).await.unwrap();
    // Second close — already closed, GitHub returns 200 with state=closed.
    let outcome = close_pr::execute(&ctx.auth, &action).await.unwrap();
    let event = match outcome {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected Succeeded, got {:?}", other),
    };
    assert_eq!(event.payload["state"].as_str(), Some("closed"));

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}
