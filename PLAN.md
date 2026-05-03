# PLAN.md

Roadmap and current state. Update this as you go so the next session can pick up cleanly.

## Where we are

**Milestone 2 of the GitHub sink plan: COMPLETE.**

The reducer-side helpers are in place. On top of the v2 contract from M1, `orchestrator-core` now exposes:

- `slugify(input, max_len) -> String` (`src/slug.rs`) — branch-safe slugs with deterministic blake3 hash-suffix truncation. Empty / all-stripped input falls back to an opaque hash so output is always non-empty and deterministic.
- `ActionBuilder` + `ActionRef` (`src/action_builder.rs`) — kind-coupled builder. `push(action)` derives the `ActionId` from the action's own `kind` and its index in the builder's vec, returning a ref whose embedded id cannot drift from the outbox row `Storage::advance` will create. `peek_id(kind)` for the case where the id must be known before payload construction.
- Both re-exported from `lib.rs`.
- 26 tests pass: 17 unit + 2 new integration (round-tripping through `Storage::advance`) + 7 existing E2E. `cargo clippy -- -D warnings` clean.

## What's next

Implementation order, each item independently mergeable:

### Milestone 3: `orchestrator-github` crate skeleton

Create a new crate (sibling, in a workspace).

**Cargo.toml workspace setup:**

```toml
# /Cargo.toml (workspace root)
[workspace]
members = ["crates/orchestrator-core", "crates/orchestrator-github"]
```

This requires moving the existing crate to `crates/orchestrator-core/`. Be careful with `Cargo.lock` and the test binary path.

**`orchestrator-github` initial shape:**

```
crates/orchestrator-github/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── auth.rs           # GitHub App auth helper
    ├── sink.rs           # GithubSink struct, Sink impl skeleton
    ├── health.rs         # check_health implementation
    ├── extractor.rs      # GithubHintExtractor for SinkHealthScope
    ├── action.rs         # GithubAction enum and payload types
    └── outcome.rs        # outcome event types
```

**Dependencies:**
- `orchestrator-core = { path = "../orchestrator-core" }`
- `octocrab = "0.x"` (latest)
- `tokio`, `serde`, `serde_json`, `chrono`, `tracing`, `async-trait`, `thiserror`
- `blake3`, `hex`, `sha2` (for body digests)

**Initial sink:** registers no action kinds yet. `check_health` does the global App auth probe but no per-repo probe (no kinds → no scope → nothing to probe). Health implementation in §6.2 of the v5 design (in conversation history).

**Acceptance:** crate compiles, registers with the dispatcher, `check_health` works, nothing else implemented.

### Milestone 4: `github.ensure_branch`

First real action kind. The simplest one to implement and a good test of the pattern.

**Action payload:**

```rust
pub struct EnsureBranch {
    pub repo: RepoRef,
    pub base_branch: String,
    pub base_sha: String,    // required - probe checks branch.head == base_sha
    pub branch_name: String, // pre-computed by reducer using slugify + ActionBuilder
    pub ticket_id: String,
}
```

**Outcome event:** `github.branch_ensured.v1`

**Execute:**
- `POST /repos/{repo}/git/refs` with `ref = refs/heads/{branch_name}` and `sha = base_sha`
- 422 "Reference already exists" → run probe to confirm it's our branch (head matches base_sha)
- 200/201 → success

**Probe (`find_existing`):**
- `GET /repos/{repo}/git/ref/heads/{branch_name}`
- 404 → `Ok(None)` (branch doesn't exist; execute can proceed)
- 200, head_sha == base_sha → `Ok(Some(...))` (already created)
- 200, head_sha != base_sha → `Err(...)` (branch exists with different content; this is a collision and the workflow needs to escalate; treat probe as definitively failed-conflict)
- transient errors → `Err(...)`

**Tests (against a real test repo, gated behind a feature flag):**
- Happy path: create new branch, verify it exists at `base_sha`.
- Idempotent: run twice, verify only one branch operation observable.
- Chaos: crash after 201 response, verify probe finds it on retry.
- Collision: pre-create a branch with the same name pointing elsewhere; verify probe correctly errors.

For the integration tests, set up a dedicated test repo (e.g. `your-org/orchestrator-test`) with a GitHub App installation. Credentials via env vars: `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY_PEM`, `GITHUB_INSTALLATION_ID`, `GITHUB_TEST_REPO_OWNER`, `GITHUB_TEST_REPO_NAME`. Skip integration tests if these aren't set.

**Acceptance:** unit tests with mocked octocrab pass; integration tests pass against the real repo.

### Milestone 5: `github.commit_patch` via Git Data API

The most complex action. Use the Git Data API flow:

```
1. GET /repos/{repo}/git/ref/heads/{branch}
   - Verify head == expected_parent_sha (else PermanentFail)
2. GET /repos/{repo}/git/commits/{ref_sha}
   - Capture tree SHA
3. POST /repos/{repo}/git/blobs (one per file upsert)
4. POST /repos/{repo}/git/trees
   - base_tree = tree from step 2
   - tree[].mode = "100644" for files, sha = blob SHA from step 3
   - sha = null for deletions
5. POST /repos/{repo}/git/commits
   - parents = [expected_parent_sha]
   - message = "{commit_message}\n\nAction-Id: {action_id}"
6. PATCH /repos/{repo}/git/refs/heads/{branch}
   - sha = new commit SHA
   - force = false (refuse non-fast-forward)
```

Step 6 is the transactional unit. Crashes between 3 and 6 leave dangling blobs/trees/commits that GitHub GCs.

**Probe** scans recent branch commits up to `MAX_HISTORY_DEPTH = 50` looking for the `Action-Id` trailer. Returns:
- `Ok(Some(...))` if found, with `is_at_head: bool` indicating whether our commit is currently HEAD
- `Ok(None)` if branch HEAD == expected_parent_sha (commit didn't land) OR scanned MAX_HISTORY_DEPTH without finding (indeterminate within bound — execute will discover via fast-forward failure if our commit was actually buried)
- `Err(...)` if a probe API call failed

**Outcome event:** `github.commit_pushed.v1` with `commit_sha`, `parent_sha`, `is_at_head`, `head_sha_at_probe`, `files_changed`, `action_id`.

**Tests:**
- Single-file commit happy path
- Multi-file commit (3+ files)
- File deletion (sha = null)
- Crash between commit creation and ref update → probe finds our commit at HEAD on retry
- Concurrent commit lands on top → probe finds our commit buried; outcome has `is_at_head: false`
- Branch advanced beyond our `expected_parent_sha` → execute fails 422 fast-forward; classified permanent

### Milestone 6: `github.open_pr`

Action payload includes title, body, head/base branches, draft flag.

**Marker:** HTML comment in PR body: `<!-- orchestrator-action: {action_id} -->`. The sink appends this; the reducer's body input doesn't include it.

**Probe:** `GET /repos/{repo}/pulls?head={owner}:{branch}&state=all`, paginate, scan bodies for marker.

**Outcome event:** `github.pr_opened.v1` with PR number, URL, head/base SHAs, draft state, `action_id`.

### Milestone 7-9: Remaining v1 actions

In order of complexity:
- `github.update_pr_metadata` (title/body only, no commits)
- `github.set_pr_status` (draft↔ready, request reviewers)
- `github.close_pr`
- `github.post_issue_comment` (with HTML marker + body-sha256 marker fallback; capture comment_id to external_ref on first success)

Deferred from v1:
- `github.post_review_comment` (inline comments — diff position validity adds complexity)
- `github.update_pr_branch` (atomic multi-commit — reducer emits separate `commit_patch` actions instead)
- `github.merge_pr` (humans merge; orchestrator observes via webhook)

### Milestone 10: Webhook ingestion

Separate concern from the sink. An HTTP server (axum or similar) that:
- Receives GitHub webhook deliveries
- Validates HMAC signatures
- Translates webhook events into `EventCommand`s with `ingress_dedup_key = delivery_id`
- Calls `executor.advance(...)`

This is what closes the loop: orchestrator opens PR → human merges → webhook arrives → workflow advances to "merged" state.

### Milestone 11: The actual coding workflow reducer

Replace the counter test reducer with the real ticket workflow. Domain events for:
- Ticket ingested (from Jira/Linear/etc.)
- Triage complete
- Plan proposed (planner agent output)
- Architecture proposed (architect agent output)
- Task started/completed (per task in the plan DAG)
- Review requested/passed/rejected
- Security review requested/passed/rejected
- PR opened
- Awaiting human approval
- Merged or closed

This is a much bigger piece of work and likely warrants its own design document before implementation. The reducer alone might be 500-1000 lines.

### Milestone 12: Agent runner sinks

The agents themselves are external services. The orchestrator's view of them is through sinks like:
- `agent.run_planner` action → calls planner service → produces `planner_output.v1` event
- `agent.run_coder` action → calls coder service → produces `coder_output.v1` event with patches
- `agent.run_reviewer` action → calls reviewer service → produces `reviewer_output.v1` event with `RejectionKind`
- etc.

Each of these is structurally similar to the GitHub sink: typed payloads, idempotency via action_id, health checks. They're simpler in some ways (the external system is easier to control than GitHub) and harder in others (responses are unbounded JSON, latency is high).

## Open design questions for later

These don't block current work but should be revisited when the relevant milestone arrives.

### Q: Multi-installation GitHub deployments

Current sink_key is just `"github"`. For deployments where the orchestrator works across multiple GitHub installations (different orgs, etc.), we'd want `sink_key` to be `"github:{installation_id}"` and instantiate one GithubSink per installation. The data model already supports this — no schema change needed. Defer until a real deployment needs it.

### Q: Snapshot compaction

When event logs get long, replay-on-restart becomes slow. We should compact: snapshot every N events and truncate older events from disk. Out of scope for v1 but should be designed before v1 deployment grows substantial logs.

### Q: Multi-process coordination

Current design assumes single-instance dispatcher. If we ever want HA, we need leader election (raft via openraft, or Postgres advisory locks if we move off SQLite). Not v1.

### Q: Cost / token budget enforcement

Designed but not implemented: per-ticket budget caps that prevent runaway agent costs. Belongs in the workflow reducer as an event type (`BudgetConsumed`, `BudgetExceeded`) and a guard in `derive_actions` that refuses to emit more agent actions when over budget. Build alongside Milestone 11.

### Q: Observability beyond tracing

We have `#[instrument]` everywhere. We should also expose:
- Per-workflow timeline view (UI on top of event log)
- Per-agent metrics (latency histogram, failure rate)
- Per-sink metrics (queue depth, retry rate, health flap rate)
- Cost dashboard

A separate component, not in `orchestrator-core`. Probably reads the SQLite database directly.

## How to use this document

When starting a session, read this top-to-bottom to know where we are.

When ending a session, update the "Where we are" section to reflect what was completed and what's next. Move completed milestones to a "Done" section if it gets long. Add open questions as they arise.

## Done

- Milestone 1 (orchestrator-core v2 contract): complete with 7 passing tests.
- Milestone 2 (slugify + ActionBuilder helpers): kind-coupled builder eliminates the embedded-id-vs-outbox-row drift footgun; round-trip test verifies multi-action ordering through `Storage::advance`. 26 tests pass.
