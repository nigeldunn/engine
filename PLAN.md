# PLAN.md

Roadmap and current state. Update this as you go so the next session can pick up cleanly.

## Where we are

**Milestone 11f of the GitHub sink plan: COMPLETE.**

Failure compensation for agent actions. When an `agent.*` action permanently fails (`EVT_ACTION_FAILED` or `EVT_ACTION_PROBE_EXHAUSTED`), the reducer emits a fresh action of the same kind at the same status, instead of halting. Cap is `MAX_COMPENSATION_DEPTH = 1` — single-shot safety net for transient infrastructure failures, not a prolonged second attempt at a genuinely failing plan. github.* failures still halt unconditionally.

- State addition (additive, `#[serde(default)]`): `WorkflowState.action_compensation_depths: HashMap<ActionId, u32>`. Per-action-chain depth (Codex round-2 B): unrelated coder runs across tasks/review iterations don't share a budget. Fresh actions are not recorded here; lookup defaults to depth 0.
- `apply_action_failed`: on a pending failure, look up depth. If kind `starts_with("agent.")` and depth < cap, derive a new action_id at `(workflow_id, event.sequence, 0, kind)`, register with depth + 1, preserve status. Else halt as before.
- `derive_actions`: new `EVT_ACTION_FAILED | EVT_ACTION_PROBE_EXHAUSTED` arm dispatches by status alone (Codex round-2 G — each agent-waiting status maps 1:1 to one agent kind). Halted workflows have status = Failed which is_terminal short-circuits, so reaching the arm always means compensation.
- `complete_pending` helper drops both pending_action_ids and action_compensation_depths together; halt clears both maps.
- Why cap = 1: agent retry budgets are already huge (CODER 50 attempts × 5min cap ≈ hours, AGENT 20 × ≈ 1-2h). A second compensation gives 4× — excessive (Codex round-2 C).
- 5 new happy_path tests + 2 existing tests rewritten (architect_action_failure & failure_mid_multi_task now exercise compensate-then-halt). Tests cover: first-failure compensation, depth-exhausted halt, github halt unconditional, probe_exhausted compensates, post-compensation success, per-chain isolation.
- 27 reducer tests pass (was 22 after M11e; +5). Workspace clippy --all-targets -- -D warnings clean. No state_version bump.

**Milestone 11e of the GitHub sink plan: COMPLETE.**

Architecture review step. Optional architect agent runs between Planning and EnsuringBranch when the workflow opts in via `TicketIngested.require_architecture_review = true`. The architect sees the proposed plan and gates it; pass advances to EnsuringBranch as before, rejection halts (iteration deferred to a future milestone).

- `KIND_AGENT_ARCHITECT = "agent.run_architect"` in action_kinds.rs.
- `ArchitectureProposed { action_id, accepted, feedback }` event + `architecture_proposed_event` constructor in events.rs. v1 is a pass/fail gate — feedback is used only in the halt reason, not threaded into downstream coder payloads.
- `WorkflowStatus::Architecting` new state variant; `ExpectedOutcomeKind::Architect` for failure-event routing; `WorkflowState.require_architecture_review: bool` cached at ingestion (Codex round-1 C: set exactly once, never re-read from log).
- `apply_ticket_ingested` reads the new field. `apply_plan_proposed` branches on it to choose Architecting vs EnsuringBranch. `apply_architecture_proposed` halts on rejection or pre-registers ensure_branch action_id and advances on pass (Codex round-1 E).
- `derive_actions` adds two arms: `EVT_PLAN_PROPOSED if status == Architecting` → run_architect; `EVT_ARCHITECTURE_PROPOSED if status == EnsuringBranch` → ensure_branch.
- `build_architect_action` includes the plan in the payload so the architect can review the proposed approach. Uses AGENT_MAX_* (20/40) — architect is a non-coder agent.
- `orchestrator-agent-runner`: new `actions/architect.rs` spec; ALL_KINDS and spec_for_kind extended.
- 5 new happy_path tests + 1 architect-failure-routing test (Codex round-1 F): runs architect after plan, passes through to ensure_branch on accept, halts on reject, skips when not required, fails through EVT_ACTION_FAILED routing.
- 260 tests pass workspace-wide (was 255 after M11d; +5). cargo build / clippy --all-targets -- -D warnings clean. No state_version bump (purely additive: new optional field, new enum variants by name, additive event type).

**Milestone 11d of the GitHub sink plan: COMPLETE.**

Review iteration loops. Reviewer rejection no longer halts the workflow — instead, the reducer transitions back to `Coding{task=0}` with the reviewer's feedback in state, and the next coder action's payload includes both the feedback and the iteration count. Capped at `MAX_REVIEW_ITERATIONS = 5` to bound runaway agent costs.

- State additions (purely additive, `#[serde(default)]` on each): `total_reviewer_rejections: u32` workflow-lifetime counter; `last_review_feedback: Option<String>` cleared on pass.
- `apply_reviewer_output` now branches on `passed`: false → increment counter, halt at cap, otherwise loop back to Coding{task=0} with feedback and pre-register the next coder action_id (Codex round-1 H — failure events match by action_id, so the rerun must be in pending_action_ids).
- `derive_actions` extra `EVT_REVIEWER_OUTPUT if status == Coding` arm emits the next coder action.
- `build_coder_action` payload threads `review_feedback` and `total_reviewer_rejections` so the agent can adjust prompting for retries.
- Multi-task plans correctly reset `current_task = 0` on rejection — each iteration is a full pass through the plan.
- Security reviewer rejection still halts (out of scope for M11d).
- 4 new tests + 1 renamed test (`reviewer_rejection_halts_workflow` → `reviewer_rejection_loops_back_to_coding`).
- 255 tests pass workspace-wide (was 251 after M11c; +4). cargo build / clippy --all-targets -- -D warnings clean. No state_version bump.

**Milestone 11c of the GitHub sink plan: COMPLETE.**

Multi-task plans. The single-task constraint from M11b is lifted; reducer now runs N tasks sequentially with one commit per task. Each commit chains on the previous via `state.head_sha`; on the last commit the reducer transitions to `Reviewing`.

- `apply_plan_proposed`: `tasks.len() != 1` halt removed; empty plan (`tasks.len() == 0`) still halts (planner bug). Plans with N >= 1 tasks are accepted.
- `apply_commit_pushed`: branches on `current_task + 1 < total_tasks` — increments task index and re-emits `agent.run_coder` if more remain, else advances to `Reviewing`. Defensive bounds guard halts on out-of-range `current_task`.
- `derive_actions`: extra `EVT_GH_COMMIT_PUSHED if status == Coding` arm emits the next-task coder action; the existing `Reviewing` arm still emits the reviewer.
- 2 new tests: `three_task_plan_runs_through_all_tasks_to_review` (verifies each task's run_coder + commit_patch + correct status transition + final review) and `failure_mid_multi_task_halts` (task 0 commits; agent.run_coder for task 1 fails → workflow halts with last_error preserved).
- 1 test renamed: `multi_task_plan_halts_in_v1` → `empty_plan_halts` (now triggered by `tasks: []` instead of `tasks.len() == 2`).
- Stale `PlanProposed` doc comment updated.
- 251 tests pass workspace-wide (was 249 after M12c; +2). cargo build / clippy --all-targets -- -D warnings clean. No state_version bump needed — change is purely additive.

**Milestone 12c of the GitHub sink plan: COMPLETE.**

Agent runner sinks. New crate `orchestrator-agent-runner` connects the workflow reducer's `agent.run_*` actions to actual agent services. With M12a's side-event mechanism + M12b's per-agent retry budgets + M12c's sinks, the engine can now drive a coding workflow end-to-end: reducer emits agent actions → sinks call agent services → outcome events update the workflow state → next agent action emitted.

- `client.rs` — `AgentClient` trait (`run`, `status`, `health`) abstracts the agent service. Default `HttpAgentClient` POSTs `/run/{agent_type}`, GETs `/status/{agent_type}/{action_id}`, GETs `/healthz`. Optional bearer-token auth via constructor. `fresh_request_id()` generates per-HTTP UUID v7 ids.
- `dispatch.rs` — shared `execute` / `probe` logic. Each per-agent module supplies an `AgentSpec { agent_type, category, build_outcome }` constant; dispatch handles the rest (client call, error classification, side-event emission with malformed-cost graceful fallback, request-id stamping onto `outcome_event.trace_id`).
- `actions/{triage,planner,coder,reviewer,security_reviewer}.rs` — five per-agent specs that decode the agent's output JSON into the corresponding workflow event type and overwrite `action_id` with the dispatcher's value (so the sink can't accidentally produce an event for a different action).
- `errors.rs` — `AgentError` enum with HTTP-status-aware classification per the M12 round-3 table.
- `sink.rs` — `AgentRunnerSink<C: AgentClient>` generic over the client type for testability. Routes by kind to the appropriate spec; unknown kinds error defensively.
- 18 new tests via mock `AgentClient`: happy path with cost+side-event, no-cost variants, still-running fallback, all error classifications, probe NotFound/Running/Finished, health Healthy/Unhealthy/Indeterminate, kind routing, request-id propagation.
- 249 tests pass workspace-wide (was 231 after M12b; +18). cargo build / clippy --all-targets -- -D warnings clean.

**v1 contract complete.** All five workspace crates wired:
1. `orchestrator-core` — engine.
2. `orchestrator-github` — outbound action surface (7 kinds).
3. `orchestrator-github-webhook` — inbound webhook ingestion.
4. `orchestrator-coding-workflow` — workflow reducer + event types.
5. `orchestrator-agent-runner` — agent-service sink.

The workspace can run a complete ticket-to-merged-PR cycle with real GitHub + an agent service implementing the M12 HTTP contract.

**Milestone 12a of the GitHub sink plan: COMPLETE.**

Core extensions to support agent-runner sinks (M12c). Two small additions:

- `AttemptOutcome::Succeeded` and `ExistingResult` gain `side_events: Vec<EventCommand>`. Sinks that want auxiliary events (e.g., `BudgetConsumed` for cost reporting) populate the field; the dispatcher's `finalize_success` writes the outcome event first, then iterates `side_events` and writes each via `executor.advance` (using the side event's own `ingress_dedup_key` for crash safety).
- `Action` gains `max_probe_attempts: u32` (default 20 via serde, matching the schema default and existing fast-sink behavior). Slow-running sinks (agent runners blocking minutes) need a higher probe budget than the default.

The dispatcher reads `action.max_probe_attempts` via `Storage::insert_outbox_row` instead of relying on the SQL default.

All existing sinks updated to construct `Succeeded { ..., side_events: vec![] }` and Action constructions with the new field. Workspace-wide changes: 13 outcome construction sites + 11 Action construction sites updated.

1 new test added (`side_events_are_written_after_primary_outcome`) verifying that the dispatcher writes side events after the primary outcome event in sequence order. 231 tests pass workspace-wide (was 230 after M11b; +1).

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

The five workspace crates are libraries. There is no binary that wires them together, no configuration story, no entry point for ingesting tickets. That's M13. Everything below M13 is post-v1 polish and can be picked up opportunistically.

### Milestone 13: app binary + end-to-end runtime

Goal: a single binary that boots the whole workspace and runs a real ticket through to a real merged PR. The architecture is done; what's missing is the operational shake-out — cold starts, real network flakes, schema migration on a long-running workflow, dashboards.

**Scope of the binary** (new crate, e.g. `crates/orchestrator-app`):

- Read configuration (TOML or env) for SQLite path, GitHub App credentials (app_id, install_id, private_key_path, webhook_secret, sink_key), agent service base URL + optional bearer token, listen addresses for the webhook + ticket-ingest endpoints.
- Open `Storage`, build `Executor::new(storage, WorkflowReducer)`, build `Dispatcher` with reasonable production config (longer poll/health intervals than tests).
- Register both sinks: `GithubSink` (with its `HintExtractor`) and `AgentRunnerSink<HttpAgentClient>`.
- Spawn the dispatcher loop and a graceful-shutdown handler that uses the `Notify`-based shutdown handle (don't try to `abort()` it).
- Mount the `orchestrator-github-webhook::router(...)` HTTP server. The handler closure translates `pull_request.closed{merged: true}` deliveries into `PrMerged` events via `executor.advance(...)`. **Workflow-id resolution is the critical wiring decision** — see open questions.
- Expose a small ticket-ingest endpoint: POST a `TicketIngested` payload, the handler builds an `EventCommand` (with a fresh `WorkflowId` and an `ingress_dedup_key` derived from the ticket id) and calls `executor.advance(...)`. Either an HTTP endpoint or a CLI subcommand is fine; HTTP is more flexible for production.

**External services the operator must provide**:

- A **GitHub App** with `pull_request` webhook subscription and contents/PRs/issues write. App ID, Installation ID, PEM private key, webhook secret.
- A **publicly reachable URL** for the webhook endpoint. Local dev: ngrok or cloudflared tunnel. Production: deploy somewhere with a public IP and TLS.
- An **agent service** that implements the M12 HTTP contract (`POST /run/{type}`, `GET /status/{type}/{id}`, `GET /healthz`) and produces output JSON matching the schemas in `orchestrator-coding-workflow/src/events.rs`. **This is the brain of the system** and the largest piece outside this repo's scope.

**Recommended bring-up path**:

1. Land the binary with both sinks pluggable but configurable to no-op stubs.
2. Stub agent service: a minimal HTTP server returning canned responses (accept triage, single-task plan, hardcoded patch, pass review, pass security review). Lets us watch the engine drive the state machine without an LLM in the loop.
3. Real GitHub App against a throwaway test repo. Run a ticket end-to-end with the stub agent. Confirm crash recovery by killing the binary mid-workflow and restarting.
4. Swap the stub agent for a real LLM-backed implementation.

**Open questions for M13** (resolve during design, not now):

- *Workflow-id from webhooks.* The webhook crate is transport-only. The translation closure needs to map `(repo, pr_number)` → `WorkflowId`. Options: (a) `WorkflowId::new(format!("ticket:{}", ticket_id))` with a sidecar `pr_locator` table that maps `(repo, pr_number)` → `WorkflowId`, populated when `apply_pr_opened` fires; (b) embed the workflow id in the PR body marker so it can be parsed back; (c) store the mapping on the github sink's outcome events. (a) is the most explicit; the sidecar table is small.
- *Ticket-ingest API shape.* `POST /tickets` taking a `TicketIngested` JSON body is the simplest. Authentication on this endpoint is out of scope for v1 — assume internal network or a reverse proxy enforces auth.
- *Configuration loading.* TOML via `serde` + `figment` (or similar) covers all config types cleanly. Avoid env-only because the GitHub PEM doesn't fit in env vars comfortably.
- *Logging shape.* `tracing-subscriber` with `RUST_LOG`-controlled filters and JSON output for production. The crates already use `#[instrument]` extensively; we just need the subscriber wiring.

This milestone is mostly plumbing — half a day of focused work for the binary itself. The real cost is providing the agent service and the GitHub App; those are operator concerns, not engineering ones.

### Deferred github.* action kinds (post-v1)
- `github.post_review_comment` (inline comments — diff position validity adds complexity)
- `github.update_pr_branch` (atomic multi-commit — reducer emits separate `commit_patch` actions instead)
- `github.merge_pr` (humans merge; orchestrator observes via webhook)

### Milestone 11f+: Workflow reducer extensions (remaining)

Each item is independently mergeable. The reducer accommodates these as additive changes.

- **Triage `Indeterminate` paths.** Currently triage is binary accept/reject; add a third "needs_more_info" path that emits a human-escalation action.
- **Compensation telemetry event.** M11f tracks compensation depth in state but emits no synthetic event marking "compensation activated". Operators currently infer it from a failure event followed by a fresh action of the same kind. A dedicated `core.compensation.activated.v1` (or similar) would simplify dashboards and alerting; not load-bearing for correctness.
- **Higher compensation depths or per-kind tuning.** `MAX_COMPENSATION_DEPTH = 1` is one-size-fits-all. If operational data shows certain agents (e.g., reviewer) benefit from a second compensation, lift the cap to a per-kind config field.
- **Security review iteration.** Currently security findings halt; could iterate similarly to M11d's reviewer loop.
- **Architecture review iteration.** Currently architect rejection halts; could iterate similar to M11d. Add when concrete need emerges.
- **Architecture approach summary threading.** v1 architect is a pure gate; threading structured architectural decisions into downstream coder payloads would require a new field on state and event-payload changes.
- **Task DAG.** Currently tasks are an ordered linear list; lifting to a true DAG with declared dependencies would let independent tasks run in parallel.

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
- Milestone 12a (side events on AttemptOutcome + per-action max_probe_attempts): two small core extensions. AttemptOutcome::Succeeded and ExistingResult gain side_events: Vec<EventCommand>; dispatcher writes outcome event first then iterates side events. Action gains max_probe_attempts (default 20). 13 outcome construction sites + 11 Action construction sites updated workspace-wide. 1 new test verifying outcome-then-side ordering. 231 tests workspace-wide.
- Milestone 12b (per-agent-kind retry budgets): coding-workflow reducer sets max_attempts=50 / max_probe_attempts=60 for coder; 20/40 for other agents; github.* unchanged. Three named constant pairs; five builder updates. No new tests (covered by existing reducer suite). 231 tests workspace-wide.
- Milestone 12c (orchestrator-agent-runner crate): 5 Sink impls for agent.run_* kinds via shared AgentSpec dispatch. AgentClient trait + HttpAgentClient default impl (POST /run/{type}, GET /status/{type}/{id}, GET /healthz). Per-call request_id (UUID v7) sent as X-Request-Id and stamped on outcome trace_id for local correlation. BudgetConsumed side events emitted when cost reported (zero-cost or missing-cost graceful no-op). 18 new tests via mock AgentClient cover happy/error paths, probe states, health classification, kind routing, request-id propagation. 249 tests workspace-wide.
- Milestone 11c (multi-task plans): single-task constraint lifted. Reducer accepts plans with N >= 1 tasks; each task runs sequentially with one commit per task, chained on state.head_sha. apply_commit_pushed branches on current_task + 1 < total_tasks; defensive bounds guard halts on overflow. Empty plan still halts. Existing single-task tests pass unchanged; 2 new tests for 3-task happy path and mid-multi-task failure halt. 251 tests workspace-wide.
- Milestone 11d (review iteration loops): reviewer rejection no longer halts; transitions back to Coding{task=0} with feedback in state. MAX_REVIEW_ITERATIONS = 5 cap prevents runaway. State adds total_reviewer_rejections (lifetime telemetry counter) + last_review_feedback (cleared on pass). build_coder_action threads both into the payload. Multi-task plans restart from task 0 on rejection (full re-pass). Security reviewer unchanged. Pre-registers rerun coder action_id in pending_action_ids per Codex round-1 H. 4 new tests + 1 rename. 255 tests workspace-wide.
- Milestone 11e (architecture review step): optional architect agent gate between Planning and EnsuringBranch via TicketIngested.require_architecture_review opt-in. New WorkflowStatus::Architecting + ExpectedOutcomeKind::Architect + ArchitectureProposed event. v1 is pass/fail (no iteration); architect's plan-context payload mirrors planner's. Agent runner gets a 6th spec via actions/architect.rs. 5 new happy-path tests + 1 architect-failure-routing test (Codex round-1 F). 260 tests workspace-wide.
