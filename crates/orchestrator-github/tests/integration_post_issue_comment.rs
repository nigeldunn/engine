//! Integration tests for `github.post_issue_comment` against real GitHub.
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
//! Each test creates a unique branch + commit + PR via raw API, posts
//! comments to the PR (PR comments live on the `/issues/{n}/comments`
//! endpoint), and cleans up by DELETEing comments + closing PR + deleting
//! branch. Comments support deletion (unlike PRs), so M8 cleanup is full.

use chrono::Utc;
use orchestrator_core::{
    ActionId, AttemptOutcome, ClaimedAction, DispatcherId, WorkflowId,
};
use orchestrator_github::actions::post_issue_comment;
use orchestrator_github::marker::{
    action_id_sha256_short, append_action_id_marker, sha256_footer,
};
use orchestrator_github::{
    installation_client, GithubAuth, PostIssueCommentPayload, RepoRef, KIND_POST_ISSUE_COMMENT,
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

async fn delete_comment_best_effort(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    comment_id: u64,
) {
    let path = format!(
        "/repos/{}/{}/issues/comments/{}",
        repo.owner, repo.name, comment_id
    );
    let _ = octocrab._delete(&path, None::<&()>).await;
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
                "message": "[orch-test] m8 setup commit",
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

async fn post_comment_raw(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    issue_number: u64,
    body: &str,
) -> Result<u64, String> {
    let path = format!(
        "/repos/{}/{}/issues/{}/comments",
        repo.owner, repo.name, issue_number
    );
    let resp: Value = octocrab
        .post(&path, Some(&json!({ "body": body })))
        .await
        .map_err(|e| e.to_string())?;
    resp["id"]
        .as_u64()
        .ok_or_else(|| "no comment id in response".to_string())
}

async fn read_comment(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    comment_id: u64,
) -> Option<Value> {
    let path = format!(
        "/repos/{}/{}/issues/comments/{}",
        repo.owner, repo.name, comment_id
    );
    octocrab.get(&path, None::<&()>).await.ok()
}

fn unique_branch(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{}/{}", prefix, &id[..16])
}

async fn open_test_pr(ctx: &TestCtx) -> (octocrab::Octocrab, String, u64) {
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/m8");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();
    let _ = land_one_commit(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        &format!("orch-test/m8-{}.txt", &branch[branch.len() - 8..]),
        "m8\n",
    )
    .await
    .unwrap();
    let pr_number = open_pr_raw(
        &octocrab,
        &ctx.repo,
        &branch,
        "main",
        "[orch-test] m8 fixture",
        "Comment-target PR.",
    )
    .await
    .unwrap();
    (octocrab, branch, pr_number)
}

fn make_action(payload: &PostIssueCommentPayload) -> ClaimedAction {
    let workflow_id = WorkflowId::new("test-wf-m8");
    let action_id = ActionId::derive(&workflow_id, 0, 0, KIND_POST_ISSUE_COMMENT);
    ClaimedAction {
        action_id,
        workflow_id,
        source_sequence: 0,
        kind: KIND_POST_ISSUE_COMMENT.into(),
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
async fn happy_path_posts_comment_with_both_markers() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = PostIssueCommentPayload {
        repo: ctx.repo.clone(),
        issue_number: pr_number,
        body: "Posted by orch-test happy path.".into(),
        ticket_id: "ENG-M8-HAPPY".into(),
    };
    let action = make_action(&payload);

    let outcome = post_issue_comment::execute(&ctx.auth, &action).await.unwrap();
    let event = match outcome {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected Succeeded, got {:?}", other),
    };
    let comment_id = event.payload["comment_id"].as_u64().unwrap();
    assert_eq!(event.payload["already_existed"].as_bool(), Some(false));

    let comment = read_comment(&octocrab, &ctx.repo, comment_id).await.unwrap();
    let body = comment["body"].as_str().unwrap();
    assert!(
        body.contains(&format!("orchestrator-action: {}", action.action_id.as_str())),
        "HTML marker missing: {}",
        body
    );
    assert!(
        body.contains(&format!("[orch:{}]", action_id_sha256_short(&action.action_id))),
        "sha256 footer missing: {}",
        body
    );

    delete_comment_best_effort(&octocrab, &ctx.repo, comment_id).await;
    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn idempotent_second_call_finds_via_probe_scan() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = PostIssueCommentPayload {
        repo: ctx.repo.clone(),
        issue_number: pr_number,
        body: "Idempotency test comment.".into(),
        ticket_id: "ENG-M8-IDEM".into(),
    };
    let action = make_action(&payload);

    // First call: posts.
    let first = post_issue_comment::execute(&ctx.auth, &action).await.unwrap();
    let first_id = match &first {
        AttemptOutcome::Succeeded { outcome_event, .. } => {
            outcome_event.payload["comment_id"].as_u64().unwrap()
        }
        other => panic!("expected first Succeeded, got {:?}", other),
    };

    // Probe (simulating dispatcher's find_existing on retry): should find
    // the existing comment via marker scan.
    let probe_result = post_issue_comment::probe(&ctx.auth, &action).await.unwrap();
    let existing = probe_result.expect("probe should find our comment");
    let probed_id = existing.outcome_event.payload["comment_id"].as_u64().unwrap();
    assert_eq!(probed_id, first_id);
    assert_eq!(
        existing.outcome_event.payload["already_existed"].as_bool(),
        Some(true)
    );

    delete_comment_best_effort(&octocrab, &ctx.repo, first_id).await;
    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn probe_finds_pre_existing_comment_via_html_marker() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = PostIssueCommentPayload {
        repo: ctx.repo.clone(),
        issue_number: pr_number,
        body: "ignored — marker comes from raw post".into(),
        ticket_id: "ENG-M8-PROBE-HTML".into(),
    };
    let action = make_action(&payload);

    // Pre-create a comment with our HTML marker (no sha256 footer).
    let body_with_html = append_action_id_marker("Pre-created.", &action.action_id);
    let comment_id = post_comment_raw(&octocrab, &ctx.repo, pr_number, &body_with_html)
        .await
        .unwrap();

    let result = post_issue_comment::probe(&ctx.auth, &action).await.unwrap();
    let existing = result.expect("probe should find our HTML-marked comment");
    assert_eq!(
        existing.outcome_event.payload["comment_id"].as_u64(),
        Some(comment_id)
    );

    delete_comment_best_effort(&octocrab, &ctx.repo, comment_id).await;
    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn probe_finds_comment_via_sha256_fallback_when_html_absent() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = PostIssueCommentPayload {
        repo: ctx.repo.clone(),
        issue_number: pr_number,
        body: "irrelevant — only the footer is matchable".into(),
        ticket_id: "ENG-M8-PROBE-SHA".into(),
    };
    let action = make_action(&payload);

    // Pre-create a comment with ONLY the sha256 footer (no HTML marker)
    // — simulates a renderer that stripped the comment.
    let body_with_only_footer = format!(
        "plain comment body\n\n{}",
        sha256_footer(&action.action_id)
    );
    let comment_id =
        post_comment_raw(&octocrab, &ctx.repo, pr_number, &body_with_only_footer)
            .await
            .unwrap();

    let result = post_issue_comment::probe(&ctx.auth, &action).await.unwrap();
    let existing = result.expect("probe should find via sha256 fallback");
    assert_eq!(
        existing.outcome_event.payload["comment_id"].as_u64(),
        Some(comment_id)
    );

    delete_comment_best_effort(&octocrab, &ctx.repo, comment_id).await;
    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn multi_marker_match_returns_err() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let (octocrab, branch, pr_number) = open_test_pr(&ctx).await;

    let payload = PostIssueCommentPayload {
        repo: ctx.repo.clone(),
        issue_number: pr_number,
        body: "ignored".into(),
        ticket_id: "ENG-M8-MULTI".into(),
    };
    let action = make_action(&payload);

    // Pre-create TWO comments with our marker.
    let body1 = append_action_id_marker("First.", &action.action_id);
    let body2 = append_action_id_marker("Second.", &action.action_id);
    let id1 = post_comment_raw(&octocrab, &ctx.repo, pr_number, &body1).await.unwrap();
    let id2 = post_comment_raw(&octocrab, &ctx.repo, pr_number, &body2).await.unwrap();

    let result = post_issue_comment::probe(&ctx.auth, &action).await;
    assert!(
        result.is_err(),
        "expected Err for multi-match, got {:?}",
        result
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("found 2") || err.contains("should be impossible"),
        "expected multi-match error message; got: {}",
        err
    );

    delete_comment_best_effort(&octocrab, &ctx.repo, id1).await;
    delete_comment_best_effort(&octocrab, &ctx.repo, id2).await;
    close_pr_best_effort(&octocrab, &ctx.repo, pr_number).await;
    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}
