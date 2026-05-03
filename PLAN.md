# PLAN.md

Roadmap and current state. Update this as you go so the next session can pick up cleanly.

## Where we are

**Milestone 4 of the GitHub sink plan: COMPLETE.**

`github.ensure_branch` is the first real action kind. The skeleton from M3 is now wired:

- `action.rs` — `RepoRef`, `EnsureBranchPayload`, `decode_ensure_branch` (serde + structural validation). Validation rejects malformed owners/repo names/branch names/SHAs at the dispatcher boundary so a buggy reducer becomes a fast `PermanentFail`, not a confusing GitHub 422 down the line.
- `outcome.rs` — `BranchEnsured` event payload (`github.branch_ensured.v1`) with `already_existed: bool` to distinguish "we created it" from "we observed prior partial success".
- `client.rs` — shared `installation_client` / `app_client` builders so M5+ doesn't grow a parallel auth path.
- `errors.rs` — `ErrorClass` enum + pure `classify_response(status, message)` matching the classification table; `octocrab::Error` adapter wraps it.
- `actions/ensure_branch.rs` — `execute()` does `POST /repos/{}/{}/git/refs`, with the 422-fallback flow: `read_branch_head` translates "exists at base_sha" → `Succeeded { already_existed: true }`, "exists at different SHA" → `PermanentFail` (collision). `probe()` (called by the dispatcher's `find_existing` path) shares `read_branch_head` and returns `Err` on collision per Option 1 — fast PermanentFail via execute, slower `failed_probe_exhausted` via the dispatcher path. Documented `TODO(M5/M6)` for extending the probe return type.
- `extractor.rs` — extracts `EndpointHint::GithubRepo` from `KIND_ENSURE_BRANCH` payloads.
- `sink.rs` — `handles()` returns `ALL_KINDS = [KIND_ENSURE_BRANCH]`; `execute`/`find_existing` dispatch on kind; unknown kinds return `DispatcherError::Internal`.
- 70 tests pass workspace-wide (38 in github lib unit + 6 github integration + 26 in core); `cargo clippy --all-targets -- -D warnings` clean.
- 5 `#[ignore]`d integration tests (1 health from M3, 4 ensure_branch covering happy / idempotent / probe-only / collision) gated on `GITHUB_APP_ID` / `GITHUB_PRIVATE_KEY_PEM` / `GITHUB_INSTALLATION_ID` / `GITHUB_TEST_REPO_OWNER` / `GITHUB_TEST_REPO_NAME`.

## GitHub sink HTTP-status classification

Shared policy for all `github.*` action kinds (M4-M9). Per-action deviations must be documented in their milestone section.

**Important:** `Sink::find_existing` is the *only* place where the distinction between "probe says it didn't happen" (`Ok(None)`) and "probe couldn't tell" (`Err(...)`) is encoded. The dispatcher's CLAUDE.md rule #4 forbids executing on `Err(...)`. Conflate them and we silently double-execute side effects on real outages. When in doubt, return `Err(...)`.

### `Sink::execute` mapping

| HTTP outcome                          | `AttemptOutcome`                                                |
|---------------------------------------|-----------------------------------------------------------------|
| 200 / 201 / 204 (action-specific)     | `Succeeded { ... }`                                             |
| 401 Unauthorized                      | `SinkUnhealthy { AuthenticationFailed }`                        |
| 403 (rate-limit / abuse detection)    | `TransientFail` — honour `Retry-After` / `X-RateLimit-Reset`    |
| 403 (permission denied)               | `SinkUnhealthy { PermissionDenied }`                            |
| 404 on workflow precondition          | `PermanentFail` (e.g., base branch missing — needs human)       |
| 404 on a target we just created       | `TransientFail` (eventual consistency)                          |
| 409 Conflict                          | `PermanentFail`                                                 |
| 422 "Reference already exists"        | Per-action: `execute` invokes `find_existing` and translates    |
| 422 Validation / non-fast-forward     | `PermanentFail`                                                 |
| 429 Too Many Requests                 | `TransientFail` — honour `Retry-After`                          |
| 5xx / network / timeout               | `TransientFail`                                                 |

### `Sink::find_existing` mapping (probe)

| HTTP outcome                          | Result                                                          |
|---------------------------------------|-----------------------------------------------------------------|
| 200, marker matches                   | `Ok(Some(ExistingResult { ... }))`                              |
| 200, marker absent                    | `Ok(None)` — execute may proceed                                |
| 200, conflicting state                | `Err(...)` — probe failed; do NOT execute                       |
| 404                                   | `Ok(None)` — definitively did not happen                        |
| 401 / 403 / 5xx / network             | `Err(...)` — probe failed; do NOT execute                       |

### `Sink::check_health` mapping

| HTTP outcome on `GET /app`            | `SinkHealthState`                                               |
|---------------------------------------|-----------------------------------------------------------------|
| 200                                   | `Healthy`                                                       |
| 401                                   | `Unhealthy { AuthenticationFailed }`                            |
| 403                                   | `Unhealthy { PermissionDenied }`                                |
| Other 4xx                             | `Unhealthy { ConfigurationInvalid }`                            |
| 5xx / network / timeout               | `Indeterminate`                                                 |

### Notes / deferred work

- **Secondary rate limits** (abuse detection): GitHub returns 403 with a back-off hint. Treat as `TransientFail`, not unhealthy — they're per-token throttling, not auth issues.
- **Rate-limit-aware backoff**: `Retry-After` and `X-RateLimit-Reset` should drive the transient backoff schedule. M4 may ship with the default exponential backoff; rate-limit-aware backoff is a deferred TODO before significant production load.
- **422 "Reference already exists"** specifically: `ensure_branch::execute` interprets this as "branch exists; verify via probe" and immediately calls its own `find_existing` to translate the result. Other 422s are `PermanentFail`.

## What's next

Implementation order, each item independently mergeable:

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
- Milestone 3 (orchestrator-github skeleton): workspace split, `GithubAuth` with PEM validation + Mutex token cache, `GithubSink` registers cleanly, `check_health` probes `GET /app` per the classification table. 32 tests pass; integration test `#[ignore]`d behind real GitHub credentials. action.rs/outcome.rs deferred to M4.
- Milestone 4 (github.ensure_branch): first real action kind. POST /git/refs with 422-fallback probe; collision returns Err from probe (Option 1) and PermanentFail from execute. Shared error classifier + client builder for M5+. 70 tests workspace-wide; 4 #[ignore]d integration tests gated on test-repo env vars.
