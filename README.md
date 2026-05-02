# orchestrator-core

The durable workflow engine for an autonomous coding system. Pure-functional reducers, event-sourced state, transactional outbox for side effects, lease-based dispatching.

## What's in here

```
src/
  ids.rs         WorkflowId, EventId, ActionId, DispatcherId
  event.rs       EventEnvelope, EventCommand, Causation
  action.rs      Action, ActionState, ClaimedAction, AttemptOutcome
  reducer.rs     Reducer trait - pure (state, event) -> (state, actions)
  sink.rs        Sink trait - the dispatcher's interface to the world
  storage.rs     Storage struct - holds the transactional invariant
  executor.rs    Executor - retries advance() on sequence conflicts
  dispatcher.rs  Dispatcher - claims actions under lease, runs sinks
  schema.sql    SQLite schema (events, snapshots, outbox, attempts, configs)

tests/
  end_to_end.rs  Counter workflow exercising the full loop
```

## The core invariant

`Storage::advance` is the only place that writes events. In a single transaction it:

1. Checks ingress dedup
2. Reads current `MAX(sequence)` for the workflow
3. Loads the prior snapshot (or default state)
4. Runs the reducer (pure)
5. Inserts the new event (PK collision = `SequenceConflict`, retry)
6. Updates the snapshot
7. Inserts outbox rows for derived actions

If the process dies anywhere in here, the transaction rolls back. **Either all of it happened, or none of it did.**

## Idempotency containment

Three places, same `ActionId`:

| Layer            | Where                                  | What it does                                                |
|------------------|----------------------------------------|-------------------------------------------------------------|
| Outbox row       | `actions_outbox.action_id` (PK)        | One row per action. Retries reuse the row.                  |
| External system  | Sink's `find_existing` probe           | Detects "succeeded but response lost" cases.                |
| Outcome event    | Reducer rejects duplicate outcomes     | Optional - reducer can dedup using `causation.action_id`.   |

The `ActionId` is deterministic: `blake3(workflow_id || sequence || index || kind)`. Same inputs always produce the same ID.

## Lease mechanics

A dispatcher claims an action with `claimed_by` and `lease_expires_at`. Long-running actions renew the lease at 1/3 of the lease duration. If the dispatcher dies, the lease expires and another dispatcher reclaims it. The reclaimed attempt's first step is `find_existing` — which is what catches the "GitHub PR was created but we crashed before recording it" case.

If the renewer fails (lease lost, network partition), the in-flight attempt does NOT write an outcome. Another dispatcher will own the action and the existence probe will reconcile.

## What's NOT in here yet

This is the executor + dispatcher + storage layer. Out of scope for this milestone:

- Concrete sinks (GitHub, Jira, agent runners) — `Sink` trait only
- Domain events and reducer for the actual coding workflow
- Tracing exporter (spans are emitted; collector configuration is the user's problem)
- Snapshot compaction / event log truncation
- Multi-process coordination beyond per-action leases (single-instance is fine for v1)

## Build

Requires Rust 1.75+. `cargo build`, `cargo test`.

## Design discipline

Things this code is careful about:

- **Reducer purity.** No clock, no I/O, no randomness. The executor passes `recorded_at` into the envelope so reducers can read it deterministically.
- **`DateTime<Utc>` for persisted state, never `Instant`.**
- **Strings for all IDs and paths in the event log.** Cross-platform, cross-version safe.
- **JSON payloads in v1.** Easier debugging with `sqlite3` CLI; switch to msgpack later if size matters.
- **Optimistic concurrency on event append.** Two callers racing on the same workflow get one `SequenceConflict`; the executor retries with backoff.
- **Outcome event before outbox finalize.** If we crash between them, the outcome is durable and the action's lease will simply expire and re-run, where the existence probe absorbs it.
