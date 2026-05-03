//! Integration tests for `github.ensure_branch` against real GitHub.
//!
//! All tests are `#[ignore]`d by default. To run them, set:
//!
//!   - `GITHUB_APP_ID`
//!   - `GITHUB_PRIVATE_KEY_PEM` (the full PEM, including header/footer)
//!   - `GITHUB_INSTALLATION_ID`
//!   - `GITHUB_TEST_REPO_OWNER`
//!   - `GITHUB_TEST_REPO_NAME`
//!
//! Then:
//!
//! ```sh
//! cargo test -p orchestrator-github -- --ignored
//! ```
//!
//! Each test creates a uniquely-named branch under `orch-test/<uuid>` and
//! best-effort deletes it on completion. Tests assume the test repo has
//! a `main` branch with at least two commits (for the collision test).

use chrono::Utc;
use orchestrator_core::{
    ActionId, AttemptOutcome, ClaimedAction, DispatcherId, WorkflowId,
};
use orchestrator_github::actions::ensure_branch;
use orchestrator_github::{
    installation_client, EnsureBranchPayload, GithubAuth, RepoRef, KIND_ENSURE_BRANCH,
};
use serde_json::json;
use std::sync::Arc;

struct TestCtx {
    auth: Arc<GithubAuth>,
    repo: RepoRef,
    main_sha: String,
    main_parent_sha: String,
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
    let main_sha = read_branch_head_raw(&octocrab, &repo, "main").await?;
    let main_parent_sha = read_first_parent(&octocrab, &repo, &main_sha).await?;

    Some(TestCtx {
        auth,
        repo,
        main_sha,
        main_parent_sha,
    })
}

async fn read_branch_head_raw(
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

async fn read_first_parent(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    commit_sha: &str,
) -> Option<String> {
    let path = format!("/repos/{}/{}/commits/{}", repo.owner, repo.name, commit_sha);
    let r: octocrab::Result<serde_json::Value> = octocrab.get(&path, None::<&()>).await;
    r.ok()
        .and_then(|v| v["parents"][0]["sha"].as_str().map(|s| s.to_string()))
}

async fn create_branch_raw(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
    sha: &str,
) -> Result<(), String> {
    let path = format!("/repos/{}/{}/git/refs", repo.owner, repo.name);
    let body = json!({
        "ref": format!("refs/heads/{}", branch),
        "sha": sha,
    });
    let r: octocrab::Result<serde_json::Value> = octocrab.post(&path, Some(&body)).await;
    r.map(|_| ()).map_err(|e| e.to_string())
}

async fn delete_branch_best_effort(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
) {
    let path = format!(
        "/repos/{}/{}/git/refs/heads/{}",
        repo.owner, repo.name, branch
    );
    // _delete returns the raw HTTP response; discard it without deserializing.
    let _ = octocrab._delete(&path, None::<&()>).await;
}

fn unique_branch(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{}/{}", prefix, &id[..16])
}

fn make_action(payload: &EnsureBranchPayload) -> ClaimedAction {
    let workflow_id = WorkflowId::new("test-wf");
    let action_id = ActionId::derive(&workflow_id, 0, 0, KIND_ENSURE_BRANCH);
    ClaimedAction {
        action_id,
        workflow_id,
        source_sequence: 0,
        kind: KIND_ENSURE_BRANCH.into(),
        payload: serde_json::to_value(payload).unwrap(),
        attempt: 0,
        max_attempts: 5,
        probe_attempt: 0,
        max_probe_attempts: 20,
        claimed_by: DispatcherId::new(),
        lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
    }
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn happy_path_creates_branch_at_base_sha() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let branch_name = unique_branch("orch-test/happy");
    let payload = EnsureBranchPayload {
        repo: ctx.repo.clone(),
        base_branch: "main".into(),
        base_sha: ctx.main_sha.clone(),
        branch_name: branch_name.clone(),
        ticket_id: "ENG-TEST-HAPPY".into(),
    };
    let action = make_action(&payload);

    let outcome = ensure_branch::execute(&ctx.auth, &action)
        .await
        .expect("execute returns Ok variant");
    assert!(
        matches!(outcome, AttemptOutcome::Succeeded { .. }),
        "expected Succeeded, got {:?}",
        outcome
    );

    // Verify the branch is now at base_sha.
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let head = read_branch_head_raw(&octocrab, &ctx.repo, &branch_name)
        .await
        .expect("branch must exist after execute");
    assert_eq!(head, ctx.main_sha);

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch_name).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn idempotent_second_execute_returns_already_existed() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let branch_name = unique_branch("orch-test/idem");
    let payload = EnsureBranchPayload {
        repo: ctx.repo.clone(),
        base_branch: "main".into(),
        base_sha: ctx.main_sha.clone(),
        branch_name: branch_name.clone(),
        ticket_id: "ENG-TEST-IDEM".into(),
    };
    let action = make_action(&payload);

    // First call: creates.
    let first = ensure_branch::execute(&ctx.auth, &action).await.unwrap();
    assert!(matches!(first, AttemptOutcome::Succeeded { .. }));

    // Second call: 422 → probe → match → Succeeded with already_existed=true.
    let second = ensure_branch::execute(&ctx.auth, &action).await.unwrap();
    let outcome_event = match second {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected Succeeded, got {:?}", other),
    };
    let payload_json = outcome_event.payload;
    assert_eq!(
        payload_json["already_existed"].as_bool(),
        Some(true),
        "second call should report already_existed=true; payload was {}",
        payload_json
    );

    let octocrab = installation_client(&ctx.auth).await.unwrap();
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch_name).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn probe_finds_pre_existing_branch_at_base_sha() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let branch_name = unique_branch("orch-test/probe");
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    create_branch_raw(&octocrab, &ctx.repo, &branch_name, &ctx.main_sha)
        .await
        .expect("pre-create must succeed");

    let payload = EnsureBranchPayload {
        repo: ctx.repo.clone(),
        base_branch: "main".into(),
        base_sha: ctx.main_sha.clone(),
        branch_name: branch_name.clone(),
        ticket_id: "ENG-TEST-PROBE".into(),
    };
    let action = make_action(&payload);
    let result = ensure_branch::probe(&ctx.auth, &action)
        .await
        .expect("probe returns Ok");
    assert!(result.is_some(), "probe should find pre-existing branch");

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch_name).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn collision_returns_permanent_fail() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let branch_name = unique_branch("orch-test/collision");
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    // Pre-create at main_sha; we'll then call execute with main_parent_sha
    // as base_sha → 422 → probe sees mismatch → PermanentFail.
    create_branch_raw(&octocrab, &ctx.repo, &branch_name, &ctx.main_sha)
        .await
        .expect("pre-create must succeed");

    let payload = EnsureBranchPayload {
        repo: ctx.repo.clone(),
        base_branch: "main".into(),
        base_sha: ctx.main_parent_sha.clone(),
        branch_name: branch_name.clone(),
        ticket_id: "ENG-TEST-COLLIDE".into(),
    };
    let action = make_action(&payload);

    let outcome = ensure_branch::execute(&ctx.auth, &action).await.unwrap();
    let err = match outcome {
        AttemptOutcome::PermanentFail { error } => error,
        other => panic!("expected PermanentFail (collision), got {:?}", other),
    };
    assert!(
        err.contains("collision") || err.contains("base_sha"),
        "permanent-fail error should mention collision/base_sha; got: {}",
        err
    );

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch_name).await;
}
