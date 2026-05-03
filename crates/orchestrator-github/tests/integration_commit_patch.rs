//! Integration tests for `github.commit_patch` against real GitHub.
//!
//! All tests are `#[ignore]`d. To run:
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
//! Each test creates a uniquely-named branch off `main`, runs the test
//! against it, and best-effort deletes the branch on completion. The test
//! repo must have `main` with at least two commits (for the
//! `concurrent_advance` test to read `main^`).

use chrono::Utc;
use orchestrator_core::{
    ActionId, AttemptOutcome, ClaimedAction, DispatcherId, WorkflowId,
};
use orchestrator_github::actions::commit_patch;
use orchestrator_github::trailer::append_action_id_trailer;
use orchestrator_github::{
    installation_client, CommitPatchPayload, FileChange, GithubAuth, RepoRef, KIND_COMMIT_PATCH,
};
use serde_json::{json, Value};
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
    let main_sha = read_branch_head(&octocrab, &repo, "main").await?;
    let main_parent_sha = read_first_parent(&octocrab, &repo, &main_sha).await?;

    Some(TestCtx {
        auth,
        repo,
        main_sha,
        main_parent_sha,
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

async fn read_first_parent(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    commit_sha: &str,
) -> Option<String> {
    let path = format!("/repos/{}/{}/commits/{}", repo.owner, repo.name, commit_sha);
    let r: octocrab::Result<Value> = octocrab.get(&path, None::<&()>).await;
    r.ok()
        .and_then(|v| v["parents"][0]["sha"].as_str().map(|s| s.to_string()))
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

async fn read_commit_message(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    commit_sha: &str,
) -> Option<String> {
    let path = format!("/repos/{}/{}/commits/{}", repo.owner, repo.name, commit_sha);
    let r: octocrab::Result<Value> = octocrab.get(&path, None::<&()>).await;
    r.ok()
        .and_then(|v| v["commit"]["message"].as_str().map(|s| s.to_string()))
}

async fn read_file_content(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
    path: &str,
) -> Option<String> {
    let api = format!(
        "/repos/{}/{}/contents/{}?ref={}",
        repo.owner, repo.name, path, branch
    );
    #[derive(serde::Deserialize)]
    struct ContentResp {
        content: String,
        encoding: String,
    }
    let r: octocrab::Result<ContentResp> = octocrab.get(&api, None::<&()>).await;
    r.ok().and_then(|c| {
        if c.encoding == "base64" {
            // GitHub embeds line breaks in base64. Strip whitespace.
            let cleaned: String = c.content.chars().filter(|ch| !ch.is_whitespace()).collect();
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(cleaned)
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
        } else {
            None
        }
    })
}

async fn file_exists(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
    path: &str,
) -> bool {
    let api = format!(
        "/repos/{}/{}/contents/{}?ref={}",
        repo.owner, repo.name, path, branch
    );
    let r: octocrab::Result<Value> = octocrab.get(&api, None::<&()>).await;
    r.is_ok()
}

/// Build a real commit on `branch` whose message includes the given
/// `Action-Id` trailer, simulating either a previous successful execute
/// or a third-party landing. Updates the ref to point at the new commit.
#[allow(clippy::too_many_arguments)]
async fn land_commit_with_trailer(
    octocrab: &octocrab::Octocrab,
    repo: &RepoRef,
    branch: &str,
    parent_sha: &str,
    msg_subject: &str,
    action_id: &ActionId,
    file_path: &str,
    file_content: &str,
) -> Result<String, String> {
    // Get parent's tree.
    let path = format!("/repos/{}/{}/git/commits/{}", repo.owner, repo.name, parent_sha);
    let parent: Value = octocrab
        .get(&path, None::<&()>)
        .await
        .map_err(|e| e.to_string())?;
    let base_tree = parent["tree"]["sha"]
        .as_str()
        .ok_or("missing parent tree")?
        .to_string();

    // Create blob.
    let blob_body = json!({ "content": file_content, "encoding": "utf-8" });
    let blob: Value = octocrab
        .post(format!("/repos/{}/{}/git/blobs", repo.owner, repo.name), Some(&blob_body))
        .await
        .map_err(|e| e.to_string())?;
    let blob_sha = blob["sha"].as_str().ok_or("missing blob sha")?.to_string();

    // Create tree.
    let tree_body = json!({
        "base_tree": base_tree,
        "tree": [{ "path": file_path, "mode": "100644", "type": "blob", "sha": blob_sha }],
    });
    let tree: Value = octocrab
        .post(format!("/repos/{}/{}/git/trees", repo.owner, repo.name), Some(&tree_body))
        .await
        .map_err(|e| e.to_string())?;
    let tree_sha = tree["sha"].as_str().ok_or("missing tree sha")?.to_string();

    // Create commit with the trailer.
    let message = append_action_id_trailer(msg_subject, action_id);
    let commit_body = json!({
        "message": message,
        "parents": [parent_sha],
        "tree": tree_sha,
    });
    let commit: Value = octocrab
        .post(
            format!("/repos/{}/{}/git/commits", repo.owner, repo.name),
            Some(&commit_body),
        )
        .await
        .map_err(|e| e.to_string())?;
    let commit_sha = commit["sha"].as_str().ok_or("missing commit sha")?.to_string();

    // Update ref.
    let ref_path = format!("/repos/{}/{}/git/refs/heads/{}", repo.owner, repo.name, branch);
    let ref_body = json!({ "sha": commit_sha, "force": false });
    let _: Value = octocrab
        .patch(&ref_path, Some(&ref_body))
        .await
        .map_err(|e| e.to_string())?;

    Ok(commit_sha)
}

fn unique_branch(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{}/{}", prefix, &id[..16])
}

fn make_action(payload: &CommitPatchPayload) -> ClaimedAction {
    let workflow_id = WorkflowId::new("test-wf-cp");
    let action_id = ActionId::derive(&workflow_id, 0, 0, KIND_COMMIT_PATCH);
    ClaimedAction {
        action_id,
        workflow_id,
        source_sequence: 0,
        kind: KIND_COMMIT_PATCH.into(),
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
async fn happy_path_single_file_commit() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/cp-happy");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();

    let unique_path = format!(
        "orch-test/{}.txt",
        uuid::Uuid::new_v4().simple().to_string().chars().take(12).collect::<String>()
    );
    let payload = CommitPatchPayload {
        repo: ctx.repo.clone(),
        branch: branch.clone(),
        expected_parent_sha: ctx.main_sha.clone(),
        commit_message: "test: single-file happy path".into(),
        author: None,
        files: vec![FileChange {
            path: unique_path.clone(),
            mode: None,
            content: Some("hello from commit_patch happy path\n".into()),
        }],
        ticket_id: "ENG-CP-HAPPY".into(),
    };
    let action = make_action(&payload);

    let outcome = commit_patch::execute(&ctx.auth, &action).await.unwrap();
    let event = match outcome {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected Succeeded, got {:?}", other),
    };

    let body = event.payload;
    assert_eq!(body["is_at_head"].as_bool(), Some(true));
    let commit_sha = body["commit_sha"].as_str().unwrap().to_string();

    // Branch HEAD should now be our commit.
    let head_after = read_branch_head(&octocrab, &ctx.repo, &branch).await.unwrap();
    assert_eq!(head_after, commit_sha);

    // Commit message should carry the Action-Id trailer.
    let msg = read_commit_message(&octocrab, &ctx.repo, &commit_sha).await.unwrap();
    assert!(msg.contains(&format!("Action-Id: {}", action.action_id.as_str())));

    // File should exist with expected content.
    let read = read_file_content(&octocrab, &ctx.repo, &branch, &unique_path).await;
    assert_eq!(read.as_deref(), Some("hello from commit_patch happy path\n"));

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn multi_file_commit_with_upsert_modify_and_delete() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/cp-multi");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();

    // Pre-populate two files via raw API so we can modify+delete them.
    let prefix = uuid::Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(12)
        .collect::<String>();
    let pre_modify = format!("orch-test/{}-modify.txt", prefix);
    let pre_delete = format!("orch-test/{}-delete.txt", prefix);
    let post_setup_sha = land_commit_with_trailer(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        "test: setup multi-file fixture",
        &ActionId("act_setup_multi".into()),
        &pre_modify,
        "v1\n",
    )
    .await
    .unwrap();
    let post_setup_sha = land_commit_with_trailer(
        &octocrab,
        &ctx.repo,
        &branch,
        &post_setup_sha,
        "test: setup second file",
        &ActionId("act_setup_multi2".into()),
        &pre_delete,
        "to be deleted\n",
    )
    .await
    .unwrap();

    let new_path = format!("orch-test/{}-create.txt", prefix);
    let payload = CommitPatchPayload {
        repo: ctx.repo.clone(),
        branch: branch.clone(),
        expected_parent_sha: post_setup_sha.clone(),
        commit_message: "test: multi-file (upsert/modify/delete)".into(),
        author: None,
        files: vec![
            FileChange { path: new_path.clone(), mode: None, content: Some("brand new\n".into()) },
            FileChange { path: pre_modify.clone(), mode: None, content: Some("v2\n".into()) },
            FileChange { path: pre_delete.clone(), mode: None, content: None },
        ],
        ticket_id: "ENG-CP-MULTI".into(),
    };
    let action = make_action(&payload);

    let outcome = commit_patch::execute(&ctx.auth, &action).await.unwrap();
    assert!(matches!(outcome, AttemptOutcome::Succeeded { .. }));

    assert_eq!(
        read_file_content(&octocrab, &ctx.repo, &branch, &new_path)
            .await
            .as_deref(),
        Some("brand new\n")
    );
    assert_eq!(
        read_file_content(&octocrab, &ctx.repo, &branch, &pre_modify)
            .await
            .as_deref(),
        Some("v2\n")
    );
    assert!(
        !file_exists(&octocrab, &ctx.repo, &branch, &pre_delete).await,
        "deleted file should not exist"
    );

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn idempotent_recovery_via_probe_after_step_one_head_mismatch() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/cp-idem");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();

    let unique_path = format!(
        "orch-test/{}.txt",
        uuid::Uuid::new_v4().simple().to_string().chars().take(12).collect::<String>()
    );
    let payload = CommitPatchPayload {
        repo: ctx.repo.clone(),
        branch: branch.clone(),
        expected_parent_sha: ctx.main_sha.clone(),
        commit_message: "test: idempotent recovery".into(),
        author: None,
        files: vec![FileChange {
            path: unique_path.clone(),
            mode: None,
            content: Some("idempotency test\n".into()),
        }],
        ticket_id: "ENG-CP-IDEM".into(),
    };
    let action = make_action(&payload);

    // First call: succeeds, lands at HEAD.
    let first = commit_patch::execute(&ctx.auth, &action).await.unwrap();
    let first_event = match first {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected first Succeeded, got {:?}", other),
    };
    let landed_sha = first_event.payload["commit_sha"].as_str().unwrap().to_string();

    // Second call: branch HEAD has moved past expected_parent_sha (because
    // we just landed). Step 1 detects the mismatch, probe finds our commit
    // at HEAD, outcome event reports is_at_head: true.
    let second = commit_patch::execute(&ctx.auth, &action).await.unwrap();
    let second_event = match second {
        AttemptOutcome::Succeeded { outcome_event, .. } => outcome_event,
        other => panic!("expected second Succeeded, got {:?}", other),
    };
    assert_eq!(second_event.payload["commit_sha"].as_str().unwrap(), landed_sha);
    assert_eq!(second_event.payload["is_at_head"].as_bool(), Some(true));

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn probe_finds_buried_commit_with_is_at_head_false() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/cp-buried");
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_sha)
        .await
        .unwrap();

    let payload = CommitPatchPayload {
        repo: ctx.repo.clone(),
        branch: branch.clone(),
        expected_parent_sha: ctx.main_sha.clone(),
        commit_message: "test: buried commit".into(),
        author: None,
        files: vec![FileChange {
            path: format!("orch-test/{}.txt", &branch[branch.len() - 8..]),
            mode: None,
            content: Some("buried\n".into()),
        }],
        ticket_id: "ENG-CP-BURIED".into(),
    };
    let action = make_action(&payload);

    // Simulate "we landed previously" by raw API: build a commit with our
    // Action-Id trailer at expected_parent_sha. Then land an unrelated
    // commit on top so ours is buried at depth 1.
    let our_path = format!("orch-test/{}-ours.txt", &branch[branch.len() - 8..]);
    let our_sha = land_commit_with_trailer(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_sha,
        "test: simulated previous run",
        &action.action_id,
        &our_path,
        "ours\n",
    )
    .await
    .unwrap();

    let other_path = format!("orch-test/{}-other.txt", &branch[branch.len() - 8..]);
    let _other_sha = land_commit_with_trailer(
        &octocrab,
        &ctx.repo,
        &branch,
        &our_sha,
        "test: third-party landing on top",
        &ActionId("act_unrelated".into()),
        &other_path,
        "other\n",
    )
    .await
    .unwrap();

    // Calling probe directly should find our commit and report is_at_head = false.
    let result = commit_patch::probe(&ctx.auth, &action).await.unwrap();
    let existing = result.expect("probe should find our buried commit");
    let body = existing.outcome_event.payload;
    assert_eq!(body["commit_sha"].as_str().unwrap(), our_sha);
    assert_eq!(body["is_at_head"].as_bool(), Some(false));

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials + test repo env vars"]
async fn concurrent_advance_returns_permanent_fail() {
    let Some(ctx) = setup().await else {
        eprintln!("skipping: env vars not set");
        return;
    };
    let octocrab = installation_client(&ctx.auth).await.unwrap();
    let branch = unique_branch("orch-test/cp-advance");
    // Start the test branch at main^ so we can advance it to main without
    // touching the protected `main` branch.
    create_branch(&octocrab, &ctx.repo, &branch, &ctx.main_parent_sha)
        .await
        .unwrap();

    // Land an out-of-band commit on the test branch — moves HEAD past
    // expected_parent_sha (which we'll set to main_parent_sha below).
    let prefix = uuid::Uuid::new_v4().simple().to_string().chars().take(12).collect::<String>();
    let interloper_path = format!("orch-test/{}-interloper.txt", prefix);
    let _interloper_sha = land_commit_with_trailer(
        &octocrab,
        &ctx.repo,
        &branch,
        &ctx.main_parent_sha,
        "test: interloper",
        &ActionId("act_interloper".into()),
        &interloper_path,
        "interloper\n",
    )
    .await
    .unwrap();

    let payload = CommitPatchPayload {
        repo: ctx.repo.clone(),
        branch: branch.clone(),
        // Stale expected_parent_sha — branch has moved on.
        expected_parent_sha: ctx.main_parent_sha.clone(),
        commit_message: "test: stale parent".into(),
        author: None,
        files: vec![FileChange {
            path: format!("orch-test/{}-stale.txt", prefix),
            mode: None,
            content: Some("never lands\n".into()),
        }],
        ticket_id: "ENG-CP-STALE".into(),
    };
    let action = make_action(&payload);

    let outcome = commit_patch::execute(&ctx.auth, &action).await.unwrap();
    let err = match outcome {
        AttemptOutcome::PermanentFail { error } => error,
        other => panic!("expected PermanentFail, got {:?}", other),
    };
    assert!(
        err.contains("branch advanced") || err.contains("expected_parent"),
        "expected branch-advance error; got: {}",
        err
    );

    delete_branch_best_effort(&octocrab, &ctx.repo, &branch).await;
}
