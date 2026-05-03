# PLAN.md

Roadmap and current state. Update this as you go so the next session can pick up cleanly.

## Where we are

**Milestone 6 of the GitHub sink plan: COMPLETE.**

`github.open_pr` is the third action kind — open a pull request, idempotent on retries via an HTML-comment marker in the PR body.

- `action.rs` — `KIND_OPEN_PR`, `OpenPrPayload`, `decode_open_pr` with structural validation (title length, body cap, branch-name format under `head_branch`/`base_branch`, head ≠ base, ticket_id, missing-body defaults to empty).
- `marker.rs` — sibling to `trailer.rs`; `append_action_id_marker` always emits the marker as a trailing paragraph; `extract_action_id_marker` finds the first marker in a body. Multi-PR multi-match detection is the probe's responsibility, not the extractor's.
- `outcome.rs` — `PrOpened` event (`github.pr_opened.v1`) carries pr_number, html_url, head/base SHAs, draft, state (observed-at-probe-time), and `already_existed` flag.
- `actions/open_pr.rs` — `POST /repos/.../pulls` with marker-rewritten body. On 422 carrying "pull request already exists" (detected via Debug-string substring match — robust to whether GitHub puts the message in `source.message` or `source.errors[].message`), routes through `probe`. Probe paginates `GET /pulls?head={owner}:{branch}&state=all&per_page=100` up to `MAX_PROBE_PAGES = 2`, scans bodies, and **returns `Err` if multiple PRs carry our marker** — matches the M4 ensure_branch collision safety bar.
- `extractor.rs` — third match arm produces `EndpointHint::GithubRepo` from `open_pr` payloads.
- `sink.rs` — dispatches `KIND_OPEN_PR` to `actions::open_pr::*`; `ALL_KINDS = [KIND_ENSURE_BRANCH, KIND_COMMIT_PATCH, KIND_OPEN_PR]`.
- 135 tests pass workspace-wide (103 lib unit + 6 github integration smoke + 26 core); `cargo clippy --all-targets -- -D warnings` clean.
- 15 `#[ignore]`d integration tests gated on real GitHub creds: 1 health (M3), 4 ensure_branch (M4), 5 commit_patch (M5), 5 open_pr (M6: happy, idempotent, probe-only, collision with non-orchestrator PR, closed-PR recovery via `state=all`). All test PRs use `[orch-test]` title prefix; closed PRs accumulate (REST API doesn't support PR deletion).

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
- Milestone 5 (github.commit_patch): six-step Git Data flow with Action-Id trailer for probe identity. Step-1 head-mismatch and step-6 fast-forward failure both route through probe to recover from "we landed but the outcome event didn't write" crash cases. is_at_head distinguishes "still HEAD" from "buried under a later commit". 108 tests workspace-wide; 5 #[ignore]d integration tests covering single-file, multi-file (upsert/modify/delete), idempotent recovery, buried-commit, and concurrent-advance.
- Milestone 6 (github.open_pr): POST /pulls with marker-rewritten body (HTML comment `<!-- orchestrator-action: {id} -->` always appended at end of body). 422-fallback probe paginates list-pulls filtered by `head={owner}:{branch}&state=all`. Multi-marker matches → Err per the collision safety bar. state field on outcome captures observed-at-probe-time so closed-PR recovery is visible to reducers. 135 tests workspace-wide; 5 #[ignore]d integration tests covering happy, idempotent, probe-only, non-orchestrator collision, and closed-PR recovery.
