# Operator runbook

Everything you need to deploy and operate `orchestrator-app`. Sister
docs:

- [`AGENT_SERVICE.md`](AGENT_SERVICE.md) — the HTTP contract your
  agent service must implement.
- [`../README.md`](../README.md) — high-level overview + architecture.
- [`../PLAN.md`](../PLAN.md) — what we built and why; design history.

## TL;DR

```sh
# 1. Build
cargo build --release --bin orchestrator-app

# 2. Configure (see "Configuration" below for the full schema)
cat > orchestrator.toml <<'EOF'
[storage]
sqlite_path = "/var/lib/orchestrator/orch.sqlite"

[github]
app_id = 12345
install_id = 67890
private_key = { path = "/etc/orchestrator/github-app.pem" }
webhook_secret = { path = "/etc/orchestrator/webhook-secret" }

[agent_runner]
base_url = "http://agent-svc.internal:8080"

[server.webhook]
listen = "0.0.0.0:8080"

[server.ingest]
# Defaults to 127.0.0.1:8081. Set non-loopback only with bearer_token.

[dispatcher]
poll_interval_ms = 250
health_check_interval_ms = 30000
unhealthy_retry_interval_ms = 5000
EOF

# 3. Run
./target/release/orchestrator-app --config orchestrator.toml

# 4. Ingest a ticket (over HTTP, from another shell)
curl -X POST http://127.0.0.1:8081/tickets \
  -H 'content-type: application/json' \
  -d '{
    "ticket": {"source": "manual", "id": "ENG-123"},
    "repo": {"owner": "octo", "name": "world"},
    "base_branch": "main",
    "base_sha": "0123456789abcdef0123456789abcdef01234567"
  }'

# … or via the CLI subcommand (no running engine required):
./target/release/orchestrator-app --config orchestrator.toml ingest \
  --source manual --id ENG-123 \
  --repo-owner octo --repo-name world \
  --base-branch main \
  --base-sha 0123456789abcdef0123456789abcdef01234567
```

The engine then drives: triage → plan → ensure_branch → code → commit
→ review → security → open PR → wait for human merge → on
`pull_request.closed{merged:true}` webhook → workflow `Merged`.

## Required external services

The binary needs three things you must provide:

1. **A GitHub App** — see [GitHub App setup](#github-app-setup).
2. **A publicly reachable webhook URL** — local dev: ngrok / cloudflared.
   Production: a load balancer or reverse proxy fronting the binary
   on `[server.webhook].listen`.
3. **An agent service** that implements the HTTP contract in
   [`AGENT_SERVICE.md`](AGENT_SERVICE.md). This is the LLM-backed
   brain of the system and lives outside this repo.

The engine boots even without (3) — but no agent action will succeed
until the service is reachable, and the dispatcher will mark the
agent sink unhealthy and stop draining agent actions after enough
failed health checks.

## Configuration

TOML file passed via `--config <PATH>`. **No implicit search path** —
the path is required.

Layered overlay: file → environment variables (prefix `ORCH_`,
`__` as section separator). Example: `ORCH_STORAGE__SQLITE_PATH=/var/db.sqlite`.

Unknown fields are rejected at parse time — typos surface as a clean
error, not a silent ignored value.

### `[storage]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `sqlite_path` | path | required | SQLite database file. Relative paths resolve against the directory containing the config file (not the process cwd). The DB is created if missing. WAL mode is enabled automatically. |

### `[github]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `app_id` | u64 | required | GitHub App ID from the App settings page. |
| `install_id` | u64 | required | Installation ID for the org/user the App is installed on. |
| `private_key` | secret | required | RSA private key (PEM) issued by GitHub for the App. See [Secrets](#secrets). |
| `webhook_secret` | secret | required | Shared secret configured on the GitHub App for HMAC-SHA256 validation of incoming webhooks. See [Secrets](#secrets). |

### `[agent_runner]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `base_url` | string | required | Base URL of your agent service (no trailing slash). The sink calls `{base_url}/run/{type}`, `/status/{type}/{id}`, `/healthz`. |
| `bearer_token` | secret | absent | Optional `Authorization: Bearer <token>` header for every call. Empty inline values are rejected at config-load. |

### `[server.webhook]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `listen` | socket addr | required | e.g. `"0.0.0.0:8080"`. Bind happens at startup; bind failure aborts boot loudly. |
| `path_prefix` | string | `"/webhook"` | Mount prefix. Must be empty, `"/"`, or `"/segments"` where each segment matches `[A-Za-z0-9._-]+`. Strict allow-list to prevent axum router panics on operator typos. |
| `lookup_retry_budget_ms` | u64 | `5000` | Total time the webhook handler will spend retrying the workflow-id lookup before giving up. Sized to absorb the open-then-merge race window between `open_pr.execute` and `executor.advance` writing `PrOpened`. MUST be strictly less than `[dispatcher].shutdown_grace_period_ms` — validated at config load. |
| `lookup_retry_backoff_ms` | u64 | `200` | Backoff between lookup retries. |

### `[server.ingest]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `listen` | socket addr | `"127.0.0.1:8081"` | Defaults to loopback for safety. **Non-loopback addresses require `bearer_token`** — validated at config load. |
| `bearer_token` | secret | absent | `Authorization: Bearer <token>` enforced via constant-time compare. Required for any non-loopback `listen`. |

### `[dispatcher]`

| Field | Type | Default | Notes |
|---|---|---|---|
| `poll_interval_ms` | u64 | required | How often the dispatcher claims new actions. Tests use 50; production 250–500 is fine. |
| `health_check_interval_ms` | u64 | required | How often the health loop re-probes unhealthy sinks. Production: 30000. |
| `unhealthy_retry_interval_ms` | u64 | required | Delay before re-claiming an action after the sink reported `SinkUnhealthy`. Production: 5000. |
| `shutdown_grace_period_ms` | u64 | `25000` | Maximum time `Runtime::shutdown` will wait for each subsystem to drain before aborting. Sized at 25s to fit comfortably under k8s' 30s SIGKILL window. **Must exceed `lookup_retry_budget_ms`.** |

### Secrets

Every secret-typed field accepts one of two shapes:

```toml
# Inline — convenient for dev, fine for short tokens.
webhook_secret = { inline = "shared-secret" }

# File path — for production. Relative paths resolve against the
# config file's directory (not the process cwd). k8s secret mounts,
# /etc files, etc.
webhook_secret = { path = "/etc/orchestrator/webhook-secret" }
```

Validation, applied at config-load time:

- Exactly one of `inline` / `path` must be set. Both → error. Neither → error.
- Empty contents are rejected: `inline = ""` errors, and a `path` pointing
  to an empty file errors. Empty bearer tokens silently disable auth, so
  they're caught early.
- For `path`, the file is read at startup. A missing file or read error
  surfaces as a typed error rather than a deep-stack failure later.

## GitHub App setup

You need a GitHub App, not a personal access token. Steps:

1. **Create the App.** Settings → Developer settings → GitHub Apps → New.
   - Webhook URL: your public URL ending in the `path_prefix` you
     configured (default `/webhook`).
   - Webhook secret: generate a strong random value; put the same
     value in your config's `[github].webhook_secret`.
   - Events to subscribe to: **Pull request**.
   - Permissions:
     - **Repository permissions**:
       - Contents: Read & write (commit, push)
       - Pull requests: Read & write (open, comment)
       - Issues: Read & write (post comments)
       - Metadata: Read (default)

2. **Generate a private key.** App settings → "Generate a private key".
   Download the `.pem`. Put it where `[github].private_key` points.

3. **Install the App** on the org / user / repos you want the engine
   to touch. Note the Installation ID from the URL after install
   (e.g. `https://github.com/settings/installations/<INSTALL_ID>`).

4. **Configure the binary** with `app_id` (App settings → "App ID"),
   `install_id` (from step 3), `private_key`, and `webhook_secret`.

5. **Verify**: start the binary, then send a test webhook from the App
   settings page. Look for a `webhook server received` log line and a
   200 response. Bad HMAC → 403; missing headers → 400; everything else
   that doesn't match the merge filter → 200 (ignored, which is fine).

## Ticket ingest

Two ways to start a workflow:

### `POST /tickets` (HTTP)

Request body:

```json
{
  "workflow_id": "manual:ENG-123#run-2",   // OPTIONAL override; default is "{source}:{id}"
  "ticket": {"source": "manual", "id": "ENG-123"},
  "repo": {"owner": "octo", "name": "world"},
  "base_branch": "main",
  "base_sha": "0123456789abcdef0123456789abcdef01234567",
  "cost_budget_cents": 100000,             // OPTIONAL cumulative cap on agent cost
  "require_architecture_review": false     // OPTIONAL opt-in to the architect step
}
```

Response codes:

| Status | Meaning |
|---|---|
| `201 Created` | New workflow started; body has `{"workflow_id": "...", "status": "created"}`. |
| `200 OK` | Idempotent re-post (same dedup key, byte-identical payload). Body: `{"workflow_id": "...", "status": "already_exists"}`. |
| `409 Conflict` | Same dedup key, **different** payload — config drift on your side. Body has `error`, `dedup_key`, `detail`. |
| `400 Bad Request` | Malformed body. |
| `401 Unauthorized` | Missing or wrong bearer token (only when configured). |
| `500 Internal Server Error` | Lookup or advance failed; safe to retry. |

Auth: see `[server.ingest].bearer_token`. Loopback bind requires no
auth; non-loopback bind requires the token at config-validate time.

### `orchestrator-app ingest` (CLI)

```sh
orchestrator-app --config /path/to/config.toml ingest \
  --source manual \
  --id ENG-123 \
  --repo-owner octo \
  --repo-name world \
  --base-branch main \
  --base-sha 0123456789abcdef0123456789abcdef01234567 \
  --cost-budget-cents 100000 \
  --require-architecture-review            # boolean flag (presence = true)
  --workflow-id manual:ENG-123#run-2       # optional override
```

The CLI opens the configured SQLite directly — no running server
needed. Safe concurrent with a running engine: SQLite WAL handles the
writer contention and `Storage::advance` is transactional.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Workflow created (or idempotent re-post of identical payload). Stdout has the workflow id. |
| `1` | Storage / advance failure. Logs have detail. |
| `2` | Dedup conflict (same key, different payload). Stderr names the conflicting key. |

### How `workflow_id` and dedup work

By default `workflow_id = "{source}:{id}"` and `ingress_dedup_key = workflow_id`.
Re-POSTing the same ticket with byte-identical payload is idempotent
(returns `200 already_exists`). Re-POSTing with a *different* payload
under the same key returns `409` — that's a config drift signal.

Want a fresh workflow for the same ticket (e.g., to retry after a
halt)? Pass an explicit `workflow_id` like `"manual:ENG-123#run-2"`.
The dedup key follows the override, so the new workflow is independent
of the prior one.

## Process lifecycle

### Startup

The boot sequence is:

1. Parse CLI, load config, validate.
2. Initialize tracing (TTY-aware: pretty for terminal, JSON otherwise).
3. Open SQLite (creates file if missing, applies schema, sets WAL).
4. Resolve every secret (PEM, webhook secret, bearer tokens) — fails
   loudly if any is unreadable or empty.
5. Bind the webhook listener and the ingest listener — fails loudly if
   ports are taken.
6. Spawn the dispatcher loop, the webhook server, and the ingest
   server.
7. Wait on Ctrl+C / SIGTERM (Unix) or Ctrl+C (Windows).

If steps 1–5 fail, exit code is 1 with a logged error. From step 6
onward the binary is "running" and only exits on signal.

### Shutdown

A signal triggers graceful shutdown of all three subsystems
concurrently. Each subsystem has up to `shutdown_grace_period_ms` to
drain (default 25s, sized below k8s' 30s SIGKILL).

Exit code mapping:

| Code | Meaning | Operator action |
|---|---|---|
| `0` | All subsystems drained cleanly. | None. |
| `1` | At least one subsystem returned a typed error during drain. | Check logs for the error. |
| `2` | At least one subsystem did not drain within the grace period (was aborted). | Investigate stuck handler. K8s deployments: alert. |
| `3` | At least one subsystem task panicked. | Code defect. File a bug. |

The "worst" outcome wins (Drained < DrainErrored < TimedOut < Panicked).

### Signal handling

| Signal | Behavior |
|---|---|
| `SIGINT` (Ctrl+C) | Triggers graceful shutdown. |
| `SIGTERM` (Unix only) | Same as SIGINT. |
| `SIGKILL` | No grace period. The dispatcher's claimed-but-not-finalized actions become reclaimable when their lease expires (default 5 min); recovery is automatic on restart via probes. |

## Webhook ingestion

GitHub posts to `https://your-host/{path_prefix}/`. The webhook handler:

1. Validates HMAC-SHA256 over the raw body using `[github].webhook_secret`.
2. Filters: only `pull_request.closed` with `merged: true` is acted on;
   everything else returns 200 (accepted, no-op).
3. Resolves the workflow: queries the events table for the prior
   `github.pr_opened.v1` event matching `(repo.owner, repo.name, pr_number)`.
   Owner/name comparison is case-insensitive — GitHub canonicalizes.
4. Translates to a `PrMerged` event and calls `executor.advance(...)`.

### The open-then-merge race

If a human merges a PR very quickly after the engine opened it, the
webhook may arrive before the dispatcher has finished writing the
`PrOpened` outcome event. The handler retries the lookup for
`lookup_retry_budget_ms` (default 5s) before giving up. After the
budget elapses with no resolution:

- If we **only ever saw "no row"** during the budget → 200 OK
  (we treat the PR as genuinely untracked; the operator's GitHub App
  may receive merges for PRs the engine didn't open).
- If we **ever saw a query error** during the budget (DB hiccup) →
  500. **GitHub does NOT auto-retry failed webhook deliveries**, so
  the operator must manually redeliver via the App's deliveries UI.

### Manual webhook redelivery

When a delivery 500s, GitHub records it as failed but does not retry.
Recovery options:

1. **GitHub UI**: App settings → Advanced → Recent Deliveries. Find
   the failed delivery and click "Redeliver".
2. **API**: `POST /app/hook/deliveries/{delivery_id}/attempts` (use
   App-level auth; not Installation auth).

The redelivery has the same `X-GitHub-Delivery` id as the original,
which means `events.ingress_dedup_key` correctly dedups duplicates if
the original somehow succeeded after the 500.

## Operations runbook

### A workflow is stuck

Workflow status (read from the `snapshots` table or your monitoring):

| Status | Meaning | Action |
|---|---|---|
| `Triaging`, `Planning`, `Coding`, `Reviewing`, etc. | In flight; agent action pending. | Check the agent sink's health (look for `unhealthy` rows in `sink_health`). Check the agent service is reachable. |
| `EnsuringBranch`, `PushingCommit`, `OpeningPr` | In flight; github action pending. | Check the github sink's health. Check the App's Installation is still valid. |
| `AwaitingHumanApproval` | Engine done; waiting for someone to merge the PR. | Normal. |
| `AwaitingTriageClarification` | Triage agent returned `indeterminate`. Needs human input. | Read `failure.reason` for the clarification question; respond out-of-band; current v1 has no resume mechanism, future versions may. |
| `Failed` | Halted. `failure` field has the reason. | Investigate `failure.reason` and `failure.action_id`. If recoverable (e.g., infra was down), no automatic retry — you'd start a fresh workflow with `workflow_id` override. |
| `Merged` | Done. | None. |

### A sink keeps going unhealthy

Look at `sink_health` in the database. The `state`, `reason`, and
`detail` columns describe why. Common cases:

- **`AuthenticationFailed`** (auth-unhealthy): GitHub App PEM expired
  or installation revoked. Rotate the PEM / re-install the App; the
  health loop will pick up the recovery on the next probe interval.
- **`PermissionDenied`**: the App's permissions don't cover the
  action. Check the App settings; redeploy with the right permissions.
- **`RateLimit`** (transient): GitHub is throttling us. The dispatcher
  honours the back-off automatically.
- **`ConfigurationInvalid`** (4xx other): something's wrong with our
  request. Check the logs for the failing request and the response
  body.

### The DB is down

`sqlx`'s pool surfaces this as `BUSY` or connection errors. The
webhook handler responds 500; ingest endpoints respond 500. The
dispatcher's claim cycle errors and backs off 2s before retrying.
Once the DB is back, normal operation resumes.

For webhook deliveries that 500'd during the outage: redeliver from
GitHub manually (see [Manual webhook redelivery](#manual-webhook-redelivery)).
The unique index on `events.ingress_dedup_key` means a re-deliver
that arrives after the DB is back will just dedup if any prior event
already landed.

### The dispatcher is logging "sequence conflict"

A second writer was racing ours on the same `(workflow_id, sequence)`
PK. The Executor retries automatically (with exponential backoff up
to `max_retries`). If you see `RetryBudgetExhausted`, you have
multiple processes writing to the same workflow — likely from
running two `orchestrator-app` instances against the same SQLite. v1
is single-instance; multi-process coordination is a deferred design
question (see PLAN.md).

### A workflow halted with `Failed`; how do I recover?

The cleanest way is to start a fresh workflow with the same ticket
but a different `workflow_id`:

```sh
orchestrator-app --config orch.toml ingest \
  --source manual --id ENG-123 \
  ... \
  --workflow-id manual:ENG-123#retry-1
```

This creates a new workflow that doesn't share state with the
halted one. The halted one stays in the database for forensics.

## Logging

`tracing-subscriber` initialized at startup. Format depends on stdout:

- **Terminal (TTY)**: pretty, colored, multi-line.
- **Pipe (production)**: JSON, one event per line. Pipe to your log
  shipper of choice.

Filter via `RUST_LOG`. Default: `info,orchestrator=debug`. Examples:

```sh
# More chatty:
RUST_LOG=debug orchestrator-app --config orch.toml

# Quiet but keep SQL queries:
RUST_LOG=warn,sqlx=debug orchestrator-app --config orch.toml
```

`#[instrument]` is used liberally — span fields surface as JSON
attributes for correlation. Notable spans: `dispatcher::run`,
`Storage::advance`, `serve_webhook`, `handle_delivery`,
`ingest_ticket`, `Runtime::boot`.

## Storage layout cheat sheet

Six tables, all in SQLite, schema in
`crates/orchestrator-core/src/schema.sql`:

| Table | What's in it | Operator queries |
|---|---|---|
| `events` | The append-only log. Everything that happened. | `SELECT payload_type, recorded_at FROM events WHERE workflow_id = ? ORDER BY sequence;` |
| `snapshots` | Current state per workflow (cache). | `SELECT json_extract(state_blob, '$.status') FROM snapshots WHERE workflow_id = ?;` |
| `actions_outbox` | Pending side effects. | `SELECT action_kind, state, attempt FROM actions_outbox WHERE workflow_id = ?;` |
| `action_attempts` | Audit trail of dispatch attempts. | `SELECT * FROM action_attempts WHERE action_id = ? ORDER BY attempt;` |
| `sink_health` | Persisted sink status. | `SELECT * FROM sink_health WHERE state != 'healthy';` |
| `workflow_configs` | Content-addressed config snapshots. | Mostly internal. |

JSON payloads are TEXT — query with SQLite's `json_extract`.

## Backups and DR

Standard SQLite practice:

- **Live backup**: use `sqlite3 orch.sqlite ".backup '/path/to/backup.sqlite'"`
  (works concurrently with a running engine, takes a checkpoint).
- **WAL files**: `*.sqlite-wal` and `*.sqlite-shm` should live alongside
  the main file. Don't restore just the `.sqlite` without them — you'll
  miss recent writes.
- **Filesystem snapshots** (LVM, ZFS, etc.) are also fine for SQLite WAL.

Restoring from backup is a clean swap. The engine on startup will
replay any events the snapshot was missing (snapshots are a cache;
event log is authoritative).

## Architecture quick reference

See [`../README.md`](../README.md) for the full picture. The
operator-relevant invariants:

- The **executor** is the only thing that writes events.
- The **dispatcher** is the only thing that calls sinks.
- Sinks **never touch storage**; they receive context.
- Side effects are idempotent via deterministic action IDs.
- Sink health is persisted, so a crash during an outage doesn't burn
  another action attempt rediscovering the problem.

For deeper architectural context see `../CLAUDE.md` (non-negotiable
rules) and `../PLAN.md` (history and what's next).
