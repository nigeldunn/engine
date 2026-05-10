# orchestrator

A durable, event-sourced workflow engine in Rust. The execution backbone for an autonomous coding system that ingests tickets, runs agents (planner, coder, reviewer, etc.) against them, and produces GitHub PRs for human approval.

This repo is a Cargo workspace of six crates: the orchestration core, two GitHub I/O crates (outbound action surface + inbound webhook ingestion), the coding workflow reducer, the agent-runner sink, and the app binary that wires them together.

## Status

**v1 GitHub-driven coding workflow is end-to-end runnable.** A single `orchestrator-app --config config.toml` boots the engine; `POST /tickets` (or the `ingest` CLI subcommand) starts a workflow; the dispatcher drives it through GitHub + agent service interactions; `pull_request.closed{merged:true}` webhooks complete the cycle. 338 tests pass workspace-wide; clippy clean.

| Crate | What it does |
|---|---|
| `orchestrator-core` | Engine: storage, executor, dispatcher, sink trait, sink health, failure events, state-version migration. |
| `orchestrator-github` | Outbound action surface — 7 GitHub action kinds (ensure_branch, commit_patch, open_pr, update_pr_metadata, set_pr_status, close_pr, post_issue_comment). |
| `orchestrator-github-webhook` | Inbound webhook ingestion with HMAC-SHA256 validation. Transport-only; consumer's translation closure builds the event. |
| `orchestrator-coding-workflow` | Pure-function workflow reducer: triage → plan → (optional architecture review) → ensure_branch → code → commit (per task) → review (with iteration loop) → security review → open PR → await human merge. Triage `Indeterminate` outcome + failure compensation for agent.* actions. |
| `orchestrator-agent-runner` | Sink that connects `agent.run_*` actions to an external HTTP agent service implementing a small request/status/health contract. |
| `orchestrator-app` | The binary that wires everything together: TOML config, dispatcher loop, webhook server, ticket-ingest endpoint + CLI, bounded graceful shutdown. |

**Operator docs:** [`docs/RUNBOOK.md`](docs/RUNBOOK.md) covers deployment, configuration, GitHub App setup, exit codes, and troubleshooting. [`docs/AGENT_SERVICE.md`](docs/AGENT_SERVICE.md) specifies the HTTP contract your agent service must implement. `PLAN.md` covers history and what's next.

## What this workspace is

A small, opinionated workflow engine with these properties:

- **Event-sourced.** Every state change is an immutable event. State is a fold over the event log; snapshots are a cache.
- **Transactional outbox.** The event, snapshot update, and side-effect intentions (outbox rows) commit in a single database transaction. Either all of it happened or none of it did.
- **Idempotent side effects.** Each outbox action has a deterministic ID derived from `(workflow_id, sequence, action_index, kind)`. Sinks use the action ID to probe external systems for prior partial successes.
- **Sink health is persisted.** When a sink (e.g. GitHub) becomes unauthenticated or otherwise can't reach its targets, it's marked unhealthy in a database table. Subsequent claim cycles filter out actions for unhealthy sinks. Health survives process restarts so a crash during an outage doesn't burn another action attempt to rediscover the problem.
- **Probe failure does not authorize execute.** If a sink's `find_existing` probe can't determine whether the side effect previously happened, the dispatcher schedules a probe-only retry rather than risking a duplicate execute call.
- **Pure reducers.** Workflow logic is a pure function `(state, event) -> (new state, actions)`. No I/O, no clocks, no randomness — that all lives in the executor and dispatcher.

## What this workspace is not

- A general-purpose workflow engine like Temporal. It's small, opinionated, and self-hosted.
- A queue. The outbox is durable but the dispatcher polls; there's no broker.
- An agent runtime. Agents are external services the agent-runner sink calls into; the LLM-backed work that triages, plans, codes, and reviews lives outside this repo.
- An auth/permission layer. Multi-tenant and access control are out of scope for v1.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          orchestrator-core                           │
│                                                                      │
│  ┌────────────┐   advance(cmd)   ┌────────────┐   transactional      │
│  │  Caller    ├──────────────────►  Executor  ├──┐  commit:          │
│  │ (webhook,  │                  └─────┬──────┘  │  - event          │
│  │  agent,    │                        │         │  - snapshot       │
│  │  human)    │                        │         │  - outbox rows    │
│  └────────────┘                        ▼         │                   │
│                                  ┌──────────┐    │                   │
│                                  │ Reducer  │    │                   │
│                                  │ (pure)   │    │                   │
│                                  └──────────┘    │                   │
│                                                  ▼                   │
│                                            ┌──────────┐              │
│                                            │ Postgres │              │
│                                            │ Storage  │              │
│                                            └────┬─────┘              │
│                                                 │                    │
│  ┌────────────┐                                 │                    │
│  │ Dispatcher │◄────────── claims ──────────────┘                    │
│  │  - claim   │                                                      │
│  │  - probe   │                                                      │
│  │  - execute ├────► Sink (GitHub, Jira, agent, ...)                 │
│  │  - health  │              │                                       │
│  │    loop    │              │  outcome event                        │
│  └─────┬──────┘              ▼                                       │
│        │             ┌────────────────┐                              │
│        └────────────►│ executor.advance(outcome)                     │
│                      └────────────────┘                              │
└──────────────────────────────────────────────────────────────────────┘
```

The executor is the only thing that writes events. The dispatcher is the only thing that calls sinks. Sinks never touch storage directly — they receive context (a `ClaimedAction` for execute/probe, a `SinkHealthScope` for health checks).

The five crates compose around this picture:

```
                       orchestrator-app
                      (config + binary)
                            │
       ┌────────────────────┼─────────────────────┐
       ▼                    ▼                     ▼
orchestrator-coding-workflow                orchestrator-agent-runner
  (pure reducer + event types)                (HTTP client → agent service)
                │                                            │
                ▼                                            ▼
          orchestrator-core (storage / executor / dispatcher / sink trait)
                ▲                                            ▲
                │                                            │
orchestrator-github-webhook                       orchestrator-github
  (HMAC-validated ingress)                          (7 action kinds outbound)
```

## Quick start

```rust
use orchestrator_core::*;

// 1. Define a reducer (pure logic).
struct MyReducer;
impl Reducer for MyReducer {
    type State = MyState;
    fn state_version(&self) -> u32 { 1 }
    fn reduce(&self, state: Self::State, event: &EventEnvelope)
        -> Result<Self::State, ExecutorError> { /* ... */ }
    fn derive_actions(&self, new_state: &Self::State, evt: &EventEnvelope)
        -> Result<Vec<Action>, ExecutorError> { /* ... */ }
}

// 2. Implement a sink (the side-effect adapter).
struct MySink { /* ... */ }
#[async_trait::async_trait]
impl Sink for MySink {
    fn handles(&self) -> &[&'static str] { &["my.action"] }
    fn sink_key(&self) -> &str { "my-sink" }
    async fn execute(&self, action: &ClaimedAction)
        -> Result<AttemptOutcome, DispatcherError> { /* ... */ }
}

// 3. Wire it up.
let storage = Storage::open("postgres://orch:orch@localhost:5432/orch").await?;
let executor = std::sync::Arc::new(Executor::new(storage, MyReducer));
let mut dispatcher = Dispatcher::new(executor.clone(), DispatcherConfig::default());
dispatcher.register(MySink::new());
let shutdown = dispatcher.shutdown_handle();
let dispatcher_task = tokio::spawn(dispatcher.run());

// 4. Send commands.
executor.advance(EventCommand {
    workflow_id: WorkflowId::new("ticket-123"),
    payload_type: "my.event.v1".into(),
    payload_schema_version: 1,
    payload: serde_json::json!({}),
    causation: Causation::External {
        source: "webhook".into(),
        request_id: "delivery-abc".into(),
    },
    trace_id: None,
    ingress_dedup_key: Some("delivery-abc".into()),
}).await?;
```

## Storage layout

Six tables, all in PostgreSQL:

| Table | Purpose |
|---|---|
| `events` | Append-only event log. Primary key `(workflow_id, sequence)`. |
| `snapshots` | State cache, derivable from events. One row per workflow. |
| `actions_outbox` | Side-effect intentions. Drained by dispatchers. |
| `action_attempts` | Audit trail of every dispatch attempt. |
| `sink_health` | Persisted sink health, keyed on `sink_key`. |
| `workflow_configs` | Content-addressed config snapshots, for replay fidelity. |

Schema lives in `crates/orchestrator-core/migrations/`, applied via `sqlx::migrate!` at `Storage::open`. Payloads are JSONB; query with `payload->'field'` / `payload->>'field'` for debugging.

## Idempotency in three places

1. **Outbox row** — one row per `ActionId`, retries reuse the row.
2. **External system** — sinks override `find_existing` to probe by `ActionId` (e.g., HTML comment marker in PR body, branch named after action_id, commit trailer).
3. **Outcome event** — reducer can dedup on `causation.action_id` if needed.

## What's tested

338 tests workspace-wide. Highlights:

- **Engine** (`crates/orchestrator-core/tests/`) — happy path, ingress dedup, deterministic action IDs, persisted sink health surviving reopens, `SinkUnhealthy` not burning attempts, failure events with crash-safe event-then-state ordering, state-version migration via discard-and-replay, side-event ordering after primary outcomes, `ActionBuilder` round-tripping with `Storage::advance`'s id derivation.
- **GitHub action surface** (`crates/orchestrator-github/`) — full HTTP-status classification table per action; idempotent execution probes (HTML markers, sha256 footers, branch markers); 422-fallback recovery for partial successes. Plus `#[ignore]`d integration tests gated on real GitHub credentials.
- **Webhook ingestion** (`crates/orchestrator-github-webhook/`) — HMAC-SHA256 validation over raw bytes, status code mapping (400 / 403 / 422 / 500), router behavior via `tower::ServiceExt::oneshot` (no real network bind).
- **Coding workflow reducer** (`crates/orchestrator-coding-workflow/tests/`) — linear happy path, multi-task plans, review iteration loops with cap, optional architecture review step, halt paths (triage rejection, security blockers, budget exhaustion, multi-task failure), failure compensation for agent.* actions with per-chain depth tracking.
- **Agent runner** (`crates/orchestrator-agent-runner/`) — mock `AgentClient` exercises happy/error paths, probe states, health classification, kind routing, request-id propagation, side-event emission for cost.
- **App binary** (`crates/orchestrator-app/`) — config schema (every reject-case + every accept-case), runtime boot/shutdown (drain timing, abort on grace timeout, partial-boot leak guard), webhook routing (HMAC, 200/401/403/500, race-recovery via in-handler retry), ticket ingest (201/200/409/401/400, override workflow_id, dedup races), end-to-end smoke that drives a full ingest → triage → plan → ensure_branch → code → commit → review → security → open_pr → AwaitingHumanApproval → PrMerged → Merged cycle through the real dispatcher with stub sinks.

The integration tests under `crates/orchestrator-core/tests/end_to_end.rs` and `crates/orchestrator-app/tests/end_to_end_smoke.rs` are the canonical reference for how to wire the pieces together.

## Building and testing

Requires Rust 1.75+ and Docker.

The test suite needs a running Postgres. The repo ships a `docker-compose.yml`
that brings up Postgres 16 with the credentials the test harness expects:

```sh
docker compose up -d
export TEST_DATABASE_URL=postgres://orch:orch@localhost:5432/orch
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Each test creates and uses its own `test_<uuid>` database against the
admin connection in `TEST_DATABASE_URL`, so suites are isolated even
under `cargo test`'s default parallelism. Orphaned test databases are
not cleaned up automatically (see `orchestrator_core::test_support::DbGuard`
docs for why); reset with `docker compose down -v` when the admin
database starts feeling crowded.

For verbose tracing during tests:

```sh
RUST_LOG=debug cargo test --workspace -- --nocapture
```

Real-GitHub integration tests are `#[ignore]`d and gated behind environment variables. See `crates/orchestrator-github/tests/` for the variable names.

## Running end-to-end

```sh
cargo build --release --bin orchestrator-app
./target/release/orchestrator-app --config /path/to/orchestrator.toml
```

You need three external pieces:

1. **A GitHub App** with `pull_request` webhook subscription, contents/PRs/issues write, an Installation, and a PEM private key.
2. **A publicly reachable URL** for the webhook endpoint (ngrok / cloudflared / a real deployment).
3. **An agent service** implementing the HTTP contract in [`docs/AGENT_SERVICE.md`](docs/AGENT_SERVICE.md) — this is the LLM-backed brain of the system and lives outside this repo.

Full deployment instructions, configuration reference, exit codes, troubleshooting: [`docs/RUNBOOK.md`](docs/RUNBOOK.md).

## License

TBD.

## Reading order for new contributors

1. `crates/orchestrator-core/src/schema.sql` — what's in the database.
2. `crates/orchestrator-core/src/event.rs` and `action.rs` — the type vocabulary.
3. `crates/orchestrator-core/src/reducer.rs` — the trait you implement for workflow logic.
4. `crates/orchestrator-core/src/storage.rs` — `advance()` is the most important method; read it first. The transactional invariant (event + snapshot + outbox in one tx) is the load-bearing contract of the engine.
5. `crates/orchestrator-core/src/sink.rs` and `health.rs` — the trait sinks implement.
6. `crates/orchestrator-core/src/dispatcher.rs` — claim, probe, execute, finalize loop.
7. `crates/orchestrator-core/src/failure.rs` — failure events let reducers observe permanent action failures.
8. `crates/orchestrator-core/tests/end_to_end.rs` — see how it all fits together at the engine layer.
9. `crates/orchestrator-coding-workflow/src/reducer.rs` — the actual workflow state machine.
10. `crates/orchestrator-coding-workflow/tests/happy_path.rs` — the workflow's behavior in scenarios.
11. `crates/orchestrator-app/src/runtime.rs` — how config + sinks + dispatcher boot together; the binary's entry point.
12. `crates/orchestrator-app/tests/end_to_end_smoke.rs` — the canonical "real dispatcher driving a full happy path" test.

`CLAUDE.md` documents architectural rules that are non-negotiable. `PLAN.md` covers history and what's next. [`docs/RUNBOOK.md`](docs/RUNBOOK.md) and [`docs/AGENT_SERVICE.md`](docs/AGENT_SERVICE.md) are the operator-facing manuals.
