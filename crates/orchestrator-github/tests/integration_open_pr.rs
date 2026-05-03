//! Integration tests for `github.open_pr` against real GitHub.
//!
//! All tests are `#[ignore]`d. Activate with:
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
//! All test PRs use the `[orch-test]` title prefix so they're easy to
//! identify and clean up manually if needed. The REST API does not allow
//! deleting PRs (only closing them), so closed test PRs accumulate in the
//! repo's PR list — this is acknowledged operational pollution.

use chrono::Utc;
use orchestrator_core::{
    ActionId, AttemptOutcome, ClaimedAction, DispatcherId, WorkflowId,
};
use orchestrator_github::actions::open_pr;
use orchestrator_github::marker::append_action_id_marker;
use orchestrator_github::{
    installation_client, GithubAuth, OpenPrPayload, RepoRef, KIND_OPEN_PR,
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

/// Land a single commit on `branch` so the branch differs from `main` and
/// is therefore mergeable into a PR. Returns the new commit SHA.
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
                "message": "[orch-test] setup commit for open_pr test",
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

fn unique_branch(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{}/{}", prefix, &id[..16])
}

fn make_action(payload: &OpenPrPayload) -> ClaimedAction {
    let workflow_id = WorkflowId::new("test-wf-pr");
    let action_id = ActionId::derive(&workflow_id, 0, 0, KIND_OPEN_PR);
    ClaimedAction {
        action_id,
        workflow_id,
        source_sequence: 0,
        kind: KIND_OPEN_PR.into(),
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
async fn happy_path_opens_pr_with_marker() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/pr-happy");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();
    land_one_commit(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        &format!("orch-test/{}.txt", &branch[branch.len() - 8..]),
        "happy\n",
    )
    .await
    .unwrap();

    let payload = OpenPrPayload {
        repo: ctx.repo.clone(),
        head_branch: branch.clone(),
        base_branch: "main".into(),
        title: "[orch-test] happy path".into(),
        body: "Body without marker — sink appends it.".into(),
        draft: true,
        ticket_id: "ENG-PR-HAPPY".into(),
    };
    let action = make_action(&payload);

    let outcome = open_pr::execute(&ctx.auth, &action).await.unwrap();
    let event = match outcome {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected Succeeded, got {:?}", other),
    };
    let payload_json = event.payload;
    let pr_number = payload_json["pr_number"].as_u64().unwrap();
    assert_eq!(payload_json["draft"].as_bool(), Some(true));
    assert_eq!(payload_json["already_existed"].as_bool(), Some(false));
    assert!(payload_json["html_url"]
        .as_str()
        .unwrap()
        .contains("/pull/"));

    // Verify the marker is in the PR body via raw read.
    let path = format!("/repos/{}/{}/pulls/{}", ctx.repo.owner, ctx.repo.name, pr_number);
    let pr_get: Value = octocrab.get(&path, None::<&()>).await.unwrap();
    let body = pr_get["body"].as_str().unwrap();
    assert!(
        body.contains(&format!("orchestrator-action: {}", action.action_id.as_str())),
        "marker missing from PR body: {}",
        body
    );

    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn idempotent_second_call_recovers_via_probe() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/pr-idem");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();
    land_one_commit(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        &format!("orch-test/{}.txt", &branch[branch.len() - 8..]),
        "idem\n",
    )
    .await
    .unwrap();

    let payload = OpenPrPayload {
        repo: ctx.repo.clone(),
        head_branch: branch.clone(),
        base_branch: "main".into(),
        title: "[orch-test] idempotent".into(),
        body: "Idempotency test.".into(),
        draft: false,
        ticket_id: "ENG-PR-IDEM".into(),
    };
    let action = make_action(&payload);

    // First call: creates.
    let first = open_pr::execute(&ctx.auth, &action).await.unwrap();
    let first_event = match first {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected first Succeeded, got {:?}", other),
    };
    let pr_number = first_event.payload["pr_number"].as_u64().unwrap();
    assert_eq!(first_event.payload["already_existed"].as_bool(), Some(false));

    // Second call: 422 → probe → match → Succeeded { already_existed: true }.
    let second = open_pr::execute(&ctx.auth, &action).await.unwrap();
    let second_event = match second {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected second Succeeded, got {:?}", other),
    };
    assert_eq!(second_event.payload["pr_number"].as_u64(), Some(pr_number));
    assert_eq!(
        second_event.payload["already_existed"].as_bool(),
        Some(true)
    );

    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn probe_finds_pre_existing_pr_with_our_marker() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/pr-probe");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();
    land_one_commit(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        &format!("orch-test/{}.txt", &branch[branch.len() - 8..]),
        "probe\n",
    )
    .await
    .unwrap();

    let payload = OpenPrPayload {
        repo: ctx.repo.clone(),
        head_branch: branch.clone(),
        base_branch: "main".into(),
        title: "[orch-test] probe-only".into(),
        body: "Probe test.".into(),
        draft: false,
        ticket_id: "ENG-PR-PROBE".into(),
    };
    let action = make_action(&payload);

    // Pre-create a PR out-of-band, with our marker in the body.
    let pre_body = append_action_id_marker("Pre-created body.", &action.action_id);
    let pr_number = open_pr_raw(
        &octocrab,
        &ctx.repo,
        &branch,
        "main",
        "[orch-test] pre-existing for probe",
        &pre_body,
    )
    .await
    .unwrap();

    let result = open_pr::probe(&ctx.auth, &action).await.unwrap();
    let existing = result.expect("probe should find our pre-existing PR");
    let body = existing.outcome_event.payload;
    assert_eq!(body["pr_number"].as_u64(), Some(pr_number));
    assert_eq!(body["already_existed"].as_bool(), Some(true));

    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn collision_with_non_orchestrator_pr_is_permanent_fail() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/pr-collision");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();
    land_one_commit(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        &format!("orch-test/{}.txt", &branch[branch.len() - 8..]),
        "collide\n",
    )
    .await
    .unwrap();

    // Pre-create a PR without our marker — simulates a human-authored or
    // different-action-id PR sitting on the same head:branch.
    let pr_number = open_pr_raw(
        &octocrab,
        &ctx.repo,
        &branch,
        "main",
        "[orch-test] interloper PR (no marker)",
        "Plain body without an orchestrator-action marker.",
    )
    .await
    .unwrap();

    let payload = OpenPrPayload {
        repo: ctx.repo.clone(),
        head_branch: branch.clone(),
        base_branch: "main".into(),
        title: "[orch-test] would-be PR".into(),
        body: "Body that won't get a chance.".into(),
        draft: false,
        ticket_id: "ENG-PR-COLLIDE".into(),
    };
    let action = make_action(&payload);

    let outcome = open_pr::execute(&ctx.auth, &action).await.unwrap();
    let err = match outcome {
        AttemptOutcome::PermanentFail { error } => error,
        other => panic!("expected PermanentFail, got {:?}", other),
    };
    assert!(
        err.contains("collision") || err.contains("already exists"),
        "expected collision error; got: {}",
        err
    );

    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn probe_finds_closed_pr_via_state_all() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/pr-closed");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();
    land_one_commit(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        &format!("orch-test/{}.txt", &branch[branch.len() - 8..]),
        "closed\n",
    )
    .await
    .unwrap();

    let payload = OpenPrPayload {
        repo: ctx.repo.clone(),
        head_branch: branch.clone(),
        base_branch: "main".into(),
        title: "[orch-test] closed-PR recovery".into(),
        body: "Will be closed before probe.".into(),
        draft: false,
        ticket_id: "ENG-PR-CLOSED".into(),
    };
    let action = make_action(&payload);

    // Open via execute, then close it.
    let outcome = open_pr::execute(&ctx.auth, &action).await.unwrap();
    let pr_number = match outcome {
        AttemptOutcome::Succeeded { outcome_event, .. } => {
            outcome_event.payload["pr_number"].as_u64().unwrap()
        }
        other => panic!("expected Succeeded, got {:?}", other),
    };
    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;

    // Probe with state=all should still find the closed PR.
    let result = open_pr::probe(&ctx.auth, &action).await.unwrap();
    let existing = result.expect("probe should find closed PR");
    let body = existing.outcome_event.payload;
    assert_eq!(body["pr_number"].as_u64(), Some(pr_number));
    assert_eq!(body["state"].as_str(), Some("closed"));
    assert_eq!(body["already_existed"].as_bool(), Some(true));

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}
