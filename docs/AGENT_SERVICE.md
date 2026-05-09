# Agent service contract

The orchestrator's `agent_runner` sink calls into an external HTTP
service for every `agent.*` action (triage, planner, architect, coder,
reviewer, security_reviewer). This document specifies that contract.

The agent service is the LLM-backed brain of the system and lives
**outside** this repo. Build it however you like — Python, Node, Rust,
a hosted API, anything. As long as it speaks this protocol.

## Endpoints

The sink talks to three endpoints, all rooted at the configured
`agent_runner.base_url`.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/run/{agent_type}` | Run an agent and (synchronously) return its output. |
| `GET` | `/status/{agent_type}/{action_id}` | Probe whether a previously-run action is still in flight. Used for crash recovery. |
| `GET` | `/healthz` | Liveness check. Drives the dispatcher's sink-health loop. |

`{agent_type}` is one of: `triage`, `planner`, `architect`, `coder`,
`reviewer`, `security_reviewer`.

`{action_id}` is the orchestrator's deterministic action id —
opaque to your service; just echo it back.

## Authentication

Configurable bearer token: when `agent_runner.bearer_token` is set in
the orchestrator's config, every request carries
`Authorization: Bearer <token>`. Reject unauthenticated calls with
`401 Unauthorized` and your service will be marked unhealthy by the
dispatcher.

When no token is configured, the orchestrator sends no auth header.

## Per-call correlation id

Each `POST /run` carries an `X-Request-Id` header (UUID v7, prefixed
`req_`). Include it in your logs for cross-system tracing — the
orchestrator stamps the same id onto the resulting outcome event's
`trace_id` field.

`GET /status` and `GET /healthz` do NOT carry the header (status is
keyed off `action_id`; healthz is unattributed).

## `POST /run/{agent_type}`

Request body:

```json
{
  "action_id": "01J9...XYZ",
  "payload": { /* agent-type-specific, see below */ }
}
```

Headers: `Content-Type: application/json`, `X-Request-Id: <uuid>`,
optional `Authorization: Bearer <token>`.

Successful response — `200 OK` or `201 Created`, same body shape:

```json
{
  "status": "finished",            // OPTIONAL; default "finished" when omitted
  "output": { /* must match the output schema for this agent_type */ },
  "cost_cents": 47                 // OPTIONAL, fixed-point USD cents
}
```

Still-running response (the v1 contract expects `/run` to block until
finished, but the client tolerates this for protocol-violation
recovery):

```json
{ "status": "running" }
```

When the body's `status` is `"running"` the action is treated as
`TransientFail` — the dispatcher retries after backoff. When `status`
is `"finished"` (or omitted), `output` is required; missing `output`
is a `PermanentFail` (malformed response).

`output` is an opaque JSON value the orchestrator deserializes into a
typed event. The exact schema depends on `agent_type` — see [Output
schemas](#output-schemas).

`cost_cents` is optional. When present, the orchestrator emits a
`core.budget.consumed.v1` side event so per-workflow cost caps can be
enforced. Use **fixed-point USD cents** (integer) — float
accumulation breaks deterministic replay.

## `GET /status/{agent_type}/{action_id}`

Used when the dispatcher restarts mid-action (crash recovery): if the
action was claimed and started but the outcome event was never
written, the dispatcher probes here to find out whether your service
already did the work.

Response by HTTP status:

| Status | Body | Meaning |
|---|---|---|
| `200 OK` | `{"status": "running"}` | In flight; dispatcher waits for the next probe interval. |
| `200 OK` | `{"status": "finished", "output": {...}, "cost_cents": 47}` (or `status` omitted, defaults to finished) | Done; dispatcher writes the outcome event without re-running. |
| `404 Not Found` | (any) | Never heard of this `action_id`; dispatcher re-runs. |

If you don't have a way to track in-progress action ids, returning
`404` is acceptable but means the dispatcher will re-execute on crash
recovery. In that case make sure your `/run` is idempotent (same
`(action_id, payload)` → same `output`).

There is **no `{"status": "not_found"}` body shape** — the wire
signal for "I don't know this action" is HTTP 404, not a JSON status
string. Returning a 200 without `output` is treated as malformed.

## `GET /healthz`

Dispatcher's sink-health loop calls this when the agent sink is
considered unhealthy, on the configured `health_check_interval_ms`.

Response: **HTTP 200 specifically → healthy** (not generic 2xx). Body
is ignored; keep it cheap — this fires regularly.

Other statuses:

| Status | Health outcome |
|---|---|
| `200` | Healthy. |
| `401` | Unhealthy: `AuthenticationFailed`. |
| `403` | Unhealthy: `PermissionDenied`. |
| `5xx` | Unhealthy: `Indeterminate` (transient infrastructure). |
| Anything else | Unhealthy: `Indeterminate` (treated as transport error). |

## HTTP status semantics

The orchestrator's classification of your responses to `/run` and
`/status`:

| Status | Outcome | Notes |
|---|---|---|
| `200 OK`, `201 Created` (`/run`) | Body decides — `finished`/omitted → `Succeeded`; `running` → `TransientFail`. | `output` required when finished. |
| `200 OK` (`/status`) | Body decides as above. | |
| `404 Not Found` (`/status`) | Treated as "no record"; dispatcher re-runs. | Only `/status` interprets 404 specifically. |
| `401 Unauthorized` | `SinkUnhealthy { AuthenticationFailed }` — sink stops draining until `/healthz` returns 200. | |
| `403 Forbidden` | `SinkUnhealthy { PermissionDenied }`. | |
| `404 Not Found` (`/run`) | `PermanentFail { UnknownAgentType }`. | The agent service doesn't know this `agent_type`. |
| `422 Unprocessable Entity` (`/run`) | `PermanentFail { InvalidInput }`. | The agent rejected the action's payload. |
| `429 Too Many Requests` | `TransientFail` — honour any `Retry-After` header. | |
| `5xx`, network error, timeout | `TransientFail` — automatic retry with backoff up to the action's `max_attempts`. | |
| Malformed JSON / missing `output` when finished / unknown `status` string | `PermanentFail { MalformedOutput }`. | |

For the agent runner specifically, `coder` actions get a higher retry
budget (50 attempts × 5min cap) because real coder runs can be long;
the other agents use 20 attempts.

## Output schemas

Each agent type has a specific JSON output schema. The orchestrator
deserializes `output` into a strongly-typed Rust struct; unknown
fields are tolerated (`#[serde(default)]` on optional fields), but
missing required fields produce a typed decode error and the
action is marked `PermanentFail`.

`action_id` MUST match the action_id in the request — the
orchestrator overwrites the field defensively after decoding so you
can't accidentally produce an event for a different action.

### `triage` output

Decides whether the ticket is in scope.

```json
{
  "action_id": "01J9...XYZ",
  "accepted": true,            // true → proceed to planning
                               // false → halt OR clarification (see below)
  "indeterminate": false,      // OPTIONAL; default false
  "reason": "..."              // OPTIONAL; required when accepted == false
}
```

Three semantic outcomes:

| `accepted` | `indeterminate` | Workflow result |
|---|---|---|
| `true` | (any) | Proceeds to `Planning`. |
| `false` | `false` (default) | `Failed` with reason "triage rejected: {reason}". |
| `false` | `true` | `AwaitingTriageClarification` (non-terminal) with the reason as the clarification question. Operator-resumable. |

### `planner` output

```json
{
  "action_id": "01J9...XYZ",
  "tasks": [
    {
      "description": "Add a public method to Foo",
      "files_in_scope": ["src/foo.rs"]   // OPTIONAL hint, non-binding
    },
    { "description": "Wire it into Bar", "files_in_scope": [] }
  ]
}
```

Tasks run sequentially with one git commit per task. An empty `tasks`
array halts the workflow with "planner produced an empty plan".

### `architect` output

Only invoked when the ticket was ingested with
`require_architecture_review = true`.

```json
{
  "action_id": "01J9...XYZ",
  "accepted": true,           // false → halt with feedback
  "feedback": "..."           // OPTIONAL; typically present when accepted == false
}
```

v1 is a pure pass/fail gate — feedback is logged in the halt reason
but not threaded into downstream coder payloads.

### `coder` output

```json
{
  "action_id": "01J9...XYZ",
  "task_idx": 0,                       // index into the plan's tasks
  "patch": {
    "files": [
      {
        "path": "src/foo.rs",
        "mode": "100644",              // OPTIONAL; "100644" or "100755"; default "100644"
        "content": "pub fn foo() {}\n" // null/missing → delete the file; string → upsert UTF-8
      }
    ]
  },
  "notes": "Added foo() and a quick smoke test."
}
```

The orchestrator turns this into a `github.commit_patch` action that
applies the patch as a single commit on the workflow's branch.

`task_idx` should match the current task. The reducer doesn't enforce
strict equality but downstream stages use it for sanity checks.

### `reviewer` output

```json
{
  "action_id": "01J9...XYZ",
  "passed": true,
  "feedback": "..."                    // OPTIONAL; typically present when passed == false
}
```

Behavior:

- `passed: true` → workflow advances to `SecurityReviewing`.
- `passed: false` → workflow loops back to `Coding{task=0}` with
  `feedback` threaded into the next coder action's payload (M11d).
  Capped at `MAX_REVIEW_ITERATIONS = 5`; further rejections halt.

### `security_reviewer` output

```json
{
  "action_id": "01J9...XYZ",
  "passed": true,
  "findings": [
    {
      "severity": "warning",       // "info" | "warning" | "high" | "critical"
      "message": "Potential SSRF in fetch_url()",
      "file_path": "src/fetch.rs", // OPTIONAL
      "line": 42                   // OPTIONAL
    }
  ]
}
```

`severity` is a typed enum — unknown values cause a decode error
(intentional; v1 chose typed-enum over free-string for fail-fast on
schema drift). Bumping the set requires a `payload_type` version bump
per the engine's schema-evolution rule.

`high` or `critical` findings halt the workflow; `info` and `warning`
are advisory.

## Request payloads

What `payload` you'll see in `POST /run/{agent_type}` requests.

### `triage`

```json
{
  "ticket": {"source": "manual", "id": "ENG-123"},
  "repo": {"owner": "octo", "name": "world"}
}
```

### `planner`

```json
{
  "ticket": {"source": "manual", "id": "ENG-123"},
  "repo": {"owner": "octo", "name": "world"}
}
```

### `architect`

```json
{
  "ticket": {"source": "manual", "id": "ENG-123"},
  "repo": {"owner": "octo", "name": "world"},
  "plan": {
    "tasks": [
      {"description": "...", "files_in_scope": [...]}
    ]
  }
}
```

### `coder`

```json
{
  "ticket": {"source": "manual", "id": "ENG-123"},
  "repo": {"owner": "octo", "name": "world"},
  "task_idx": 0,
  "task": {"description": "...", "files_in_scope": [...]},
  "review_feedback": null,           // string when this is a re-coder iteration; null on first pass
  "total_reviewer_rejections": 0     // count for telemetry; non-zero means this is a rerun
}
```

### `reviewer`

```json
{
  "ticket": {"source": "manual", "id": "ENG-123"},
  "repo": {"owner": "octo", "name": "world"},
  "branch": "auto/eng-123/abcdef0123456789",
  "head_sha": "abcdef0123456789abcdef0123456789abcdef01"
}
```

### `security_reviewer`

```json
{
  "ticket": {"source": "manual", "id": "ENG-123"},
  "repo": {"owner": "octo", "name": "world"},
  "branch": "auto/eng-123/abcdef0123456789",
  "head_sha": "abcdef0123456789abcdef0123456789abcdef01"
}
```

## Idempotency

The dispatcher's transactional outbox guarantees each `action_id` is
claimed once at a time, but crashes between execute and finalize can
result in re-execution. To make recovery clean:

- **If you can persist by `action_id`**: implement `/status` properly
  and your service will avoid duplicate work on recovery.
- **If you can't**: ensure your `/run` is naturally idempotent for the
  same `(action_id, payload)` — same input → same output.

The orchestrator never re-issues the same `action_id` with different
inputs.

## Side events: cost reporting

When you include `cost_cents` in a `finished` response, the
orchestrator emits a `core.budget.consumed.v1` event alongside the
agent's outcome event. Both events commit in the same transaction.

Per-workflow caps via `cost_budget_cents` on `TicketIngested` halt the
workflow when the running total exceeds the cap (in `derive_actions`,
the next action emission is suppressed and the workflow goes to
`Failed` with `budget exceeded` reason).

Reporting cost is optional. Omit `cost_cents` (or set to `null`) when
you don't have it.

## Reference implementation

A minimal stub used for the engine's smoke test lives in the
in-repo test:

> `crates/orchestrator-app/tests/end_to_end_smoke.rs::StubAgentClient`

It implements the trait directly without HTTP, but the canned
responses match the JSON shapes specified above. Useful as a starting
point for a Python / Node / Rust stub server during local development
of your real agent service.

## Versioning

The output schemas above are version 1 (every event constant in
`crates/orchestrator-coding-workflow/src/events.rs` ends in `.v1`).
Breaking schema changes will introduce `.v2` constants and the
agent-runner sink will route by content-type or another mechanism
when that day comes. For now, target the v1 schemas.

Per the engine's schema-evolution rule (CLAUDE.md rule #9), additions
that are purely additive (new optional fields with `#[serde(default)]`)
do not require a version bump — they're backward compatible.
