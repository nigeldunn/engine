# PLAN.md

Roadmap and current state. Update this as you go so the next session can pick up cleanly.

## Where we are

**Milestone 11b of the GitHub sink plan: COMPLETE.**

The coding workflow reducer is alive. New crate `orchestrator-coding-workflow` implements the linear single-task happy path: ingest → triage → plan → ensure_branch → code → commit → review → security review → open PR → await human approval → merged. Halt-on-failure (matched against `pending_action_ids`); budget tracking with fixed-point cents; webhook translation for `pull_request.merged`.

- `events.rs` — domain event types: `TicketIngested`, `TriageCompleted`, `PlanProposed`, `CoderOutput`, `ReviewerOutput`, `SecurityReviewerOutput`, `BudgetConsumed`, `PrMerged`. `Severity` is a typed enum (Codex round-2: free-form strings let unknown values flow into pure reducer logic; typed enum fails fast on schema drift).
- `state.rs` — wide-flat `WorkflowState` with `WorkflowStatus` tag-only enum. `pending_action_ids: HashMap<ActionId, ExpectedOutcomeKind>` lets failure events be matched by `action_id` (Codex round-2: matching by kind alone misattributes failures across multiple instances over a workflow lifetime).
- `reducer.rs` — pure-function `reduce` + `derive_actions`. Each event produces 0-1 actions in v1. Action ids computed via `ActionId::derive` with idx=0; this matches what `Storage::advance` derives from the returned `Vec<Action>`. Halt-on-rejection paths for triage / reviewer / security findings (high+critical severities are blockers). Budget guard: cumulative cents (u64 — fixed-point for deterministic replay per Codex round-2) checked against optional cap.
- `webhook.rs` — `translate_github_webhook` filters to `pull_request.closed` with `merged: true`, producing a `PrMerged` event. Workflow id resolution is the consumer's closure (per M10's transport-only contract).
- 16 new tests (5 webhook + 11 reducer happy-path + halt-path coverage). `cargo clippy --all-targets -- -D warnings` clean.

**Deferred to M11c+** (deliberate scope cut from Codex round-1):
- Multi-task plans (currently halts when `tasks.len() != 1`).
- Review iteration loops (rejected → re-code with feedback).
- Architecture review step.
- Triage `Indeterminate` paths.
- Failure compensation beyond halt.
- Cost-from-agents wiring (M12 territory).

230 tests pass workspace-wide (was 214 after M11a; +16). cargo build / clippy --all-targets -- -D warnings clean.

**Milestone 11a of the GitHub sink plan: COMPLETE.**

Failure events + state-version migration. Two small core changes that together unblock M11b (the workflow reducer): the reducer can now observe permanent action failures, and reducers can evolve their state schema without breaking in-flight workflows.

- `crates/orchestrator-core/src/failure.rs` — new module. `EVT_ACTION_FAILED` / `EVT_ACTION_PROBE_EXHAUSTED` constants, `ActionFailedPayload` struct, `build_failure_event_command` helper. Original payload embedded up to `MAX_ORIGINAL_PAYLOAD_BYTES = 64 KiB` with `payload_truncated: bool` flag for larger ones (commit_patch can be 5 MiB). `Causation::Action { action_id }` matches the rest of the engine's outcome-event causation pattern.
- Dispatcher (`dispatcher.rs`) writes failure events via `executor.advance` BEFORE the state-transition write. Distinct dedup-key prefixes (`action_failed:{id}` vs `probe_exhausted:{id}`) prevent collision under `events.ingress_dedup_key`'s unique index. The dedup makes event-then-state ordering crash-safe: on dispatcher restart, action reclaim retries lead to dedup'd no-op event-write, then state transition completes.
- Storage (`storage.rs`) `advance` extends to discard-and-replay: when the snapshot's `state_version` doesn't match the reducer's current `state_version()`, the snapshot is dropped and state is rebuilt by replaying the event log inside the same transaction. Snapshots are a cache; the event log is authoritative.
- 14 new tests workspace-wide (8 failure-module unit + 4 failure-event E2E covering permanent-fail, transient-exhaustion, dedup-key uniqueness, and write-idempotency + 2 state-migration tests covering version-match snapshot reuse and version-mismatch replay).
- Existing reducers must acknowledge `EVT_ACTION_FAILED` and `EVT_ACTION_PROBE_EXHAUSTED` (return state unchanged) or `executor.advance` will fail when the dispatcher writes them. The test `CounterReducer` was updated; the future workflow reducer in M11b handles them as the trigger for compensating actions.
- 214 tests pass workspace-wide (was 200 after M10; +14). `cargo clippy --all-targets -- -D warnings` clean.

**Milestone 10 of the GitHub sink plan: COMPLETE.**

Webhook ingestion closes the loop. New crate `orchestrator-github-webhook` provides HMAC-validated GitHub webhook receipt; the consumer's handler closure translates a `GithubWebhookDelivery` into an `EventCommand` (using `delivery_id` as `ingress_dedup_key`) and calls `executor.advance(...)`.

- `crates/orchestrator-github-webhook/` — new sibling crate, library only (no binary).
- `error.rs` — `WebhookError` with mapped HTTP statuses: 400 missing-header / malformed-signature, 403 signature-mismatch, 422 JSON-parse, 500 handler-error. axum's `DefaultBodyLimit` middleware returns 413 for over-cap bodies before our handler runs.
- `hmac.rs` — `validate_hmac_sha256(secret, raw_body, header)` with constant-time compare via `subtle::ConstantTimeEq`. Strict `sha256=` prefix; hex-decode; HMAC over **raw body bytes** (router uses axum's `Bytes` extractor, never `Json`).
- `delivery.rs` — `GithubWebhookDelivery { event_type, delivery_id, action, payload }`. Payload kept opaque (`serde_json::Value`); `action` pre-extracted from `payload.action` for ergonomic `(event_type, action)` pattern matching by the consumer. Typing out 30+ GitHub event variants would be premature; M11 reducer decodes what it needs.
- `router.rs` — `router(GithubWebhookConfig, handler) -> axum::Router`. Single `POST /` endpoint; mountable under any prefix via `Router::nest`. Handler closure is opaque-error (`E: Display`) — surfaces as 500 with the error logged.
- 29 tests pass (17 unit on hmac + delivery, 12 integration via `tower::ServiceExt::oneshot` covering all status code paths). No `#[ignore]`d tests — fully self-contained, no real network bind.
- 200 tests pass workspace-wide (was 171 after M8; +29). `cargo clippy --all-targets -- -D warnings` clean.

**Workflow routing scope.** The webhook crate is transport-only. Mapping a delivery to a `workflow_id` lives in the consumer's translation closure — the crate has no opinion about which workflow a given delivery belongs to. M11 (the workflow reducer) and the eventual app binary own that wiring.

**v1 GitHub surface complete.** Action surface (M3-M8) + ingress (M10). Outstanding: M11 (real workflow reducer) and M12 (agent runner sinks).

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

Deferred from v1:
- `github.post_review_comment` (inline comments — diff position validity adds complexity)
- `github.update_pr_branch` (atomic multi-commit — reducer emits separate `commit_patch` actions instead)
- `github.merge_pr` (humans merge; orchestrator observes via webhook)

### Milestone 11c+: Workflow reducer extensions (deferred from M11b)

Each item is independently mergeable. M11b's linear single-task design accommodates these as additive changes.

- **Multi-task plans.** Lift `tasks.len() != 1` halt; cycle through tasks with explicit task_idx routing. Reducer adds a `Coding { task_idx }`-style state distinguishing tasks. Adds the per-task DAG.
- **Review iteration loops.** On `ReviewerOutput { passed: false, feedback }`, instead of halting, transition back to `Coding` with the feedback in state. Track iteration count; cap to prevent infinite loops.
- **Architecture review step.** Optional intermediate state between Planning and EnsuringBranch where an architecture agent runs.
- **Triage `Indeterminate` paths.** Currently triage is binary accept/reject; add a third "needs_more_info" path that emits a human-escalation action.
- **Failure compensation beyond halt.** On certain failure types (e.g., reviewer agent unreachable), retry a fresh agent run rather than halting outright.
- **Cost-from-agents wiring.** M12 agent sinks emit `BudgetConsumed` events; this is the M12 contract, not M11c.

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
- Milestone 7 (PATCH triple — update_pr_metadata + set_pr_status + close_pr): three orchestrator-owned, last-write-wins actions. No probe (find_existing returns Ok(None)); idempotent via PATCH semantics on the GitHub side. Outcome events record applied state from response, not echoed intent. set_pr_status makes up to two calls (PATCH draft, POST /requested_reviewers); last response is canonical. 152 tests workspace-wide; 5 #[ignore]d integration tests covering update + idem, set draft, close + idem.
- Milestone 8 (github.post_issue_comment): seventh and final v1 action. Dual-marker idempotency — HTML primary `<!-- orchestrator-action: {id} -->`, plain-text sha256 footer `[orch:{8 hex}]` as stripped-HTML-tolerant fallback. Probe scans `GET /issues/{n}/comments` paginated up to MAX_COMMENT_PROBE_PAGES=3 and matches either marker form; multi-match → Err per collision safety bar. comment_id flows to external_ref via finalize_succeeded but probe uses scan (Path-B direct lookup deferred). 171 tests workspace-wide; 5 #[ignore]d integration tests covering happy with both markers, idempotent via probe scan, probe-only via HTML marker, probe-only via sha256 fallback, and multi-match Err. **v1 GitHub action surface complete (all 7 kinds wired).**
- Milestone 10 (orchestrator-github-webhook crate): HMAC-validated webhook ingestion. New library crate provides `router(config, handler) -> axum::Router` with HMAC-SHA256 over raw body bytes (Bytes extractor, never Json), constant-time compare via subtle, GithubWebhookDelivery handed to consumer's translation closure. HTTP status codes: 400 missing-header / malformed-signature, 403 signature-mismatch, 422 JSON-parse, 500 handler-error (413 from axum's DefaultBodyLimit). Workflow routing is the consumer's concern. 200 tests workspace-wide; 29 new (17 unit + 12 integration via tower::ServiceExt::oneshot, no real network bind). Closes the loop: orchestrator opens PR → human merges → webhook → workflow advances.
- Milestone 11a (failure events + state-version migration): two small core changes that unblock the workflow reducer. New EVT_ACTION_FAILED / EVT_ACTION_PROBE_EXHAUSTED events written via executor.advance with distinct dedup-key prefixes; event-then-state ordering with dedup makes crash-recovery safe. Storage::advance discards stale snapshots and replays from event log when state_version mismatches. 214 tests workspace-wide; +14 new (failure unit + E2E + migration tests).
- Milestone 11b (orchestrator-coding-workflow crate): linear single-task happy path with halt-on-failure + budget tracking + webhook translation. Severity is a typed enum; budget cents as u64 (deterministic replay); failure events matched by action_id (not kind). Wide-flat WorkflowState evolves via state_version. Pure-function reducer tests cover happy path + each halt path + budget guard + action_failed routing. 230 tests workspace-wide; +16 new (5 webhook + 11 reducer). Multi-task plans, review iteration, architecture step, and failure compensation deferred to M11c+.
