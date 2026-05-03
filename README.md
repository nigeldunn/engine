# orchestrator-core

A durable, event-sourced workflow engine in Rust. The execution backbone for an autonomous coding system that ingests tickets, runs agents (planner, coder, reviewer, etc.) against them, and produces GitHub PRs for human approval.

This crate provides the **orchestration plane only**: storage, the executor, the dispatcher, the sink trait, and sink health. Agents, the GitHub sink, the ticket-provider integrations, and the actual coding workflow reducer are built on top.

## Status

- **v2 contract:** complete and tested. Persisted sink health, separate probe-attempt counter, transactional outbox with claim-doesn't-burn-attempt discipline, ingress idempotency, sink-health-aware claim filtering, hint-extractor scope building.
- **GitHub sink:** designed, not yet implemented. See `PLAN.md`.
- **Coding workflow reducer:** not yet started. The current test workflow is a simple counter for validating the engine.

## What this crate is

A small, opinionated workflow engine with these properties:

- **Event-sourced.** Every state change is an immutable event. State is a fold over the event log; snapshots are a cache.
- **Transactional outbox.** The event, snapshot update, and side-effect intentions (outbox rows) commit in a single database transaction. Either all of it happened or none of it did.
- **Idempotent side effects.** Each outbox action has a deterministic ID derived from `(workflow_id, sequence, action_index, kind)`. Sinks use the action ID to probe external systems for prior partial successes.
- **Sink health is persisted.** When a sink (e.g. GitHub) becomes unauthenticated or otherwise can't reach its targets, it's marked unhealthy in a database table. Subsequent claim cycles filter out actions for unhealthy sinks. Health survives process restarts so a crash during an outage doesn't burn another action attempt to rediscover the problem.
- **Probe failure does not authorize execute.** If a sink's `find_existing` probe can't determine whether the side effect previously happened, the dispatcher schedules a probe-only retry rather than risking a duplicate execute call.
- **Pure reducers.** Workflow logic is a pure function `(state, event) -> (new state, actions)`. No I/O, no clocks, no randomness — that all lives in the executor and dispatcher.

## What this crate is not

- A general-purpose workflow engine like Temporal. It's small, opinionated, and self-hosted.
- A queue. The outbox is durable but the dispatcher polls; there's no broker.
- An agent runtime. Agents are external services that sinks call into.
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
│                                            │ SQLite   │              │
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
let storage = Storage::open("sqlite:///var/lib/orchestrator.db").await?;
let executor = std::sync::Arc::new(Executor::new(storage, MyReducer));
let mut dispatcher = Dispatcher::new(executor.clone(), DispatcherConfig::default());
dispatcher.register(MySink::new());
tokio::spawn(dispatcher.run());

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

Six tables, all in SQLite:

| Table | Purpose |
|---|---|
| `events` | Append-only event log. Primary key `(workflow_id, sequence)`. |
| `snapshots` | State cache, derivable from events. One row per workflow. |
| `actions_outbox` | Side-effect intentions. Drained by dispatchers. |
| `action_attempts` | Audit trail of every dispatch attempt. |
| `sink_health` | Persisted sink health, keyed on `sink_key`. |
| `workflow_configs` | Content-addressed config snapshots, for replay fidelity. |

Schema in `src/schema.sql`. JSON payloads in TEXT columns for v1 — query with SQLite's JSON functions for debugging.

## Idempotency in three places

1. **Outbox row** — one row per `ActionId`, retries reuse the row.
2. **External system** — sinks override `find_existing` to probe by `ActionId` (e.g., HTML comment marker in PR body, branch named after action_id, commit trailer).
3. **Outcome event** — reducer can dedup on `causation.action_id` if needed.

## What's tested

The `tests/end_to_end.rs` suite covers:

- Happy path: command in → action runs → outcome event written → workflow advances.
- Ingress dedup: same `ingress_dedup_key` returns the prior outcome without re-running.
- Deterministic action IDs.
- Persisted sink health: unhealthy state survives storage reopens.
- `SinkUnhealthy` outcome: doesn't burn `attempt`, action recovers cleanly when health is restored.
- Indeterminate health: doesn't change persisted state.
- `SinkHealthState` serde round-trip.

About 2,800 lines of Rust total.

## Building and testing

Requires Rust 1.75+.

```sh
cargo build
cargo test
```

For verbose tracing during tests:

```sh
RUST_LOG=debug cargo test -- --nocapture
```

## License

TBD.

## Reading order for new contributors

1. `src/schema.sql` — what's in the database
2. `src/event.rs` and `src/action.rs` — the type vocabulary
3. `src/reducer.rs` — the trait you implement for workflow logic
4. `src/storage.rs` — `advance()` is the most important method; read it first
5. `src/sink.rs` and `src/health.rs` — the trait sinks implement
6. `src/dispatcher.rs` — claim, probe, execute, finalize loop
7. `tests/end_to_end.rs` — see how it all fits together

`PLAN.md` covers what comes next.
