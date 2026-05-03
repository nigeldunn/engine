# CLAUDE.md

Operational guidance for Claude Code (and similar agents) working on this codebase.

## What this project is

A durable workflow engine in Rust, designed to be the orchestration plane for an autonomous coding system. See `README.md` for context and `PLAN.md` for what's planned next.

## Read these first, in order

Before making any non-trivial change:

1. `README.md` — project context
2. `PLAN.md` — what we're building toward and where we are in that plan
3. `src/storage.rs::advance` — the single most important method in the codebase; understand the transactional invariant it enforces before touching anything storage-related
4. `tests/end_to_end.rs` — the working examples of how everything fits together

## Architectural rules — non-negotiable

These rules came out of a long design discussion. Violating them silently will reintroduce correctness bugs we already fixed.

### 1. Reducers are pure

`Reducer::reduce` and `Reducer::derive_actions` MUST be pure functions. No I/O, no clock reads (use `event.recorded_at`), no randomness, no global state. The executor depends on this for replay and crash recovery to work correctly.

If you find yourself wanting to make a reducer impure, the right answer is almost always to emit an action that does the impure work and produces an outcome event the reducer can react to.

### 2. Only `Storage::advance` writes events

All event writes go through `Executor::advance` → `Storage::advance`. No other code path inserts into the `events` table. This is what makes the transactional invariant (event + snapshot + outbox in one tx) hold.

### 3. Claim does not increment `attempt`

`attempt` and `probe_attempt` are incremented by the storage methods that record outcomes (`finalize_succeeded`, `record_transient_failure`, `record_permanent_failure`, `record_probe_failure`). Never by `claim_actions`. Never by ad-hoc UPDATE statements.

The reason: a dispatcher crash between claim and execute should not burn an attempt. The claim only sets the lease.

### 4. Probe failure does NOT authorize execute

`Sink::find_existing` returns:

- `Ok(Some(result))` → finalize as success
- `Ok(None)` → execute may proceed (definitively no prior side effect)
- `Err(...)` → dispatcher records a probe failure and waits; **must not call execute**

If you change the dispatcher's probe handling, this contract is the thing you cannot break. Re-read the relevant section of the design conversation if you're tempted to.

### 5. Sinks do not touch storage

Sinks receive `ClaimedAction` for execute/probe and `SinkHealthScope` for health checks. They never query the database, never call into other sinks, never invoke the executor. The boundary is clean and must stay clean.

If a sink needs queue-derived context (e.g., "which repos have queued actions"), the dispatcher provides it via `SinkHealthScope` and `HintExtractor`. Add a new variant to `EndpointHint` (or use `EndpointHint::Custom`) rather than punching through the abstraction.

### 6. Public APIs use `std::time::Duration`

`chrono::Duration` is a storage-internal detail (used for `DateTime<Utc>` arithmetic). Public method signatures, config fields, and trait method parameters use `std::time::Duration`. The `to_chrono()` helper in `storage.rs` does the conversion at the boundary.

### 7. Persisted timestamps are `DateTime<Utc>`

Never store `Instant` in the database or in serializable state. `Instant` is process-local and meaningless across restarts. Use `chrono::DateTime<chrono::Utc>` for everything that crosses a serialization boundary.

### 8. Sink health is persisted

When a sink reports unhealthy, the dispatcher writes to the `sink_health` table. In-memory health state alone is insufficient — a process restart during an outage would forget the unhealthy state and burn another action attempt rediscovering it.

### 9. Schema additions are additive only

Existing columns and tables don't change. New columns are added with defaults. New tables go alongside the old ones. The reducer-input event schema evolves via `payload_type` versioning (e.g., `my_event.v1` → `my_event.v2`), not by mutation.

## Type system conventions

- IDs are typed wrappers around `String` (`WorkflowId`, `EventId`, `ActionId`, `DispatcherId`). They all impl `Display`. Never use bare strings for IDs.
- Paths in events are `String`, never `PathBuf`. Cross-platform safety.
- Event payloads at the storage boundary are `serde_json::Value`. Typed decoding happens at the reducer.
- Action payloads at the dispatcher boundary are `serde_json::Value`. Typed decoding happens inside the sink.
- All public enums that map to database strings have `as_str()` and `from_str()` methods. Don't use `Debug` formatting for persistence.

## Coding conventions

- **Errors:** thiserror-derived enums per layer (`ExecutorError`, `DispatcherError`). Never `Box<dyn Error>` or `anyhow::Error` in public APIs.
- **Logging:** `tracing` only, never `println!` or `eprintln!`. Use `#[instrument]` on public async methods. Field bindings use `%` for `Display` and `?` for `Debug`.
- **Async:** Tokio runtime. Use `async-trait` for trait methods. Spawned tasks should always be reaped by their owner; no detached `tokio::spawn` in long-lived loops.
- **SQLite:** sqlx with `runtime-tokio`, `sqlite`, `chrono`, `json` features. Use `BEGIN/COMMIT` via `pool.begin()`, never raw SQL transactions. Bind values explicitly; never format SQL with user input.
- **Tests:** integration tests in `tests/`, unit tests inline. Tests use `sqlite::memory:` for fast isolated databases, or `tempfile::TempDir` when persistence across reopens is being tested.
- **Imports:** Group by std → external → crate. Don't `use crate::*`.

## Things that look like they should work but don't

A few learned-the-hard-way notes:

- `chrono::Duration::seconds()` on a `const` doesn't work — `Duration::seconds` is not const. Use config fields, not constants, or initialize lazily.
- `#[serde(other)]` on internally-tagged enums (`#[serde(tag = "type")]`) does NOT preserve the unknown payload — it only marks the variant. For event payloads we use `payload_type: String` + `payload: serde_json::Value` instead.
- sqlx's `query_as!` macros require a live database at compile time. We use the runtime variants (`query`, `query_as`) to avoid that dependency.
- `tokio::sync::Notify::notified()` requires being polled inside `tokio::select!` (which pins it). Don't store it in a local and `.await` later.

## How to add a new feature

The general shape:

1. **Read `PLAN.md` first.** If your feature isn't on the plan, decide whether it should be added there before writing code.
2. **Sketch the storage changes.** Schema additions go in `src/schema.sql` first. Add migrations for live deployments later (not v1 concern).
3. **Add types in the smallest scope.** New event payload types, new action kinds, new outcome shapes. Keep them in the module that owns them.
4. **Update the trait if necessary.** Trait additions get default implementations so existing impls don't break.
5. **Wire it through.** Storage method → dispatcher logic → tests.
6. **Test the failure modes, not just the happy path.** Every interesting feature should have a test that deliberately fails and verifies recovery.

## How to add a new sink

1. Pick a `sink_key`. Stable string, unique across registered sinks.
2. Decide on action kinds. Stable strings, namespace prefix recommended (e.g., `github.commit_patch`).
3. Define typed payload structs serializable to JSON.
4. Implement the `Sink` trait:
   - `handles()` returns the action kinds.
   - `sink_key()` returns the chosen key.
   - `find_existing()` probes the external system using the action_id as a marker. Returns `Err` if it can't determine state.
   - `execute()` does the work. Returns `Succeeded`, `TransientFail`, `PermanentFail`, or `SinkUnhealthy`.
   - Override `check_health()` if the sink can fail in ways that affect every action (auth issues, target system down).
5. Implement a `HintExtractor` if `check_health` needs queue-derived context.
6. Register both with the dispatcher: `dispatcher.register(sink)` and `dispatcher.register_extractor(extractor)`.
7. Write tests, including a chaos test that crashes between API call and outcome event write to verify `find_existing` recovery.

The GitHub sink design (in `PLAN.md`) is the canonical example.

## Common gotchas when modifying the dispatcher

- The dispatcher loop is a single `async fn run(self)` that consumes the dispatcher. The shutdown handle is obtained via `shutdown_handle()` before calling `run`. Don't try to control the dispatcher after spawning it; control it via the `Notify`.
- `handle_action` is a free function, not a method. It receives clones of the executor, sinks map, and config values. Don't try to capture `&self.config` into a `tokio::spawn`.
- The lease renewer is a separate spawned task that must be `abort()`ed before finalize, otherwise it might extend the lease while we're trying to release it.
- `record_attempt_start` is called at execute-start, NOT at claim. Don't move it.

## Test discipline

- Tests should be deterministic. Use atomic counters for invocation tracking, not random delays.
- Use short timeouts in test configs so tests fail fast on hangs. The standard setup uses 50ms poll, 100ms unhealthy retry, 200ms health check interval.
- Tests that expect retry/recovery behavior should poll on observable state (events, sink health table, invocation counts) with a generous bound (e.g., 5s of retries). Don't rely on fixed sleeps.
- Tests that need persistence across reopens use `tempfile::TempDir` for an on-disk database.

## When to ask the human vs proceed

Proceed without asking:
- Bug fixes for code already covered by failing tests.
- Refactors that don't change public APIs or storage schema.
- Adding test coverage for existing behavior.
- Documentation improvements.

Ask first:
- Schema changes (anything in `schema.sql`).
- Public API changes (trait methods, struct fields, function signatures in `lib.rs`).
- Anything in `PLAN.md` that's marked as a design decision needing sign-off.
- Adding new dependencies to `Cargo.toml`.
- Anything that touches the architectural rules above.

## What success looks like

- All tests pass: `cargo test`.
- No new warnings: `cargo build` produces clean output.
- Clippy is happy: `cargo clippy -- -D warnings` (run before declaring done on a feature).
- Public APIs are documented with doc comments.
- The architectural rules above are upheld.
