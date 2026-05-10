# AWS ECS Express and Aurora Serverless deployment plan

> **Status (2026-05-10):** the application changes in §"Application
> changes" are **complete** — the codebase is now Postgres-only and
> ships with a `docker-compose.yml` for local development. This
> document is preserved for the infrastructure / cost analysis
> sections that drove the migration. Sections describing the SQLite
> baseline are historical.

Prepared: 2026-05-09
Target work date: 2026-05-10

This document captures the work needed to move `orchestrator-app` from
the current single-binary SQLite deployment model to AWS ECS Express
Mode with Aurora Serverless PostgreSQL.

## Recommendation

ECS Express Mode is a reasonable managed hosting target for
`orchestrator-app` once storage is moved out of the container. Aurora
Serverless PostgreSQL is the right managed database target if we want
the orchestrator to survive task replacement, support ECS scheduling,
and eventually support more than one running task.

The main application change is not ECS. It is replacing the concrete
SQLite storage layer with a Postgres/Aurora-compatible storage backend.

Recommended first production shape:

- ECS Express service running one `orchestrator-app` task.
- Public ALB for GitHub webhooks and authenticated ticket ingest.
- Aurora Serverless v2 PostgreSQL, single writer, no RDS Proxy at first.
- Secrets Manager for GitHub App private key, webhook secret, ingest
  bearer token, agent bearer token, and DB credentials.
- CloudWatch Logs for JSON application logs.
- Agents remain external behind the existing HTTP `agent_runner`
  contract.

Avoid running the long-lived coder agents inside the same ECS Express
service unless the agent service becomes async and durable. The current
agent contract expects synchronous `/run/{agent_type}` calls, while
real coder work can take long enough to fight ALB/client timeouts.

## Current app constraints

The current app assumes local SQLite in several important places:

- Config exposes `[storage].sqlite_path`, and runtime constructs a
  `sqlite:` URL directly.
- `orchestrator-core::Storage` owns a concrete `SqlitePool`.
- The schema is one embedded SQLite SQL file.
- Queries use SQLite placeholders (`?`), SQLite date handling, and
  SQLite JSON functions such as `json_extract`.
- Tests use `sqlite::memory:` throughout.
- The dispatcher polls the DB frequently (`poll_interval_ms = 250` in
  the example runbook), which would keep Aurora active and prevent
  scale-to-zero savings.

These are all manageable changes, but they are real code changes rather
than pure infrastructure work.

## Application changes

### Storage backend

Add Postgres support in `orchestrator-core`.

- Introduce a database backend abstraction or make `Storage` generic
  enough to support both SQLite and Postgres.
- Add SQLx Postgres dependencies and runtime features.
- Keep SQLite support for local tests and cheap development unless it
  becomes too costly to maintain.
- Add a Postgres schema/migration set for the six durable tables:
  `events`, `snapshots`, `actions_outbox`, `action_attempts`,
  `workflow_configs`, and `sink_health`.
- Use `jsonb` for event/action payload blobs in Postgres while keeping
  deterministic JSON serialization in Rust.
- Convert SQL placeholders from `?` to `$1`, `$2`, etc. in the Postgres
  path.
- Replace SQLite `json_extract` queries with Postgres JSONB operators.
- Preserve the current transactional invariant: event, snapshot, and
  outbox rows must commit in one transaction.

The highest-risk storage method is action claiming. The current
SQLite implementation selects candidates and then updates them inside a
transaction. In Postgres, prefer a single claim query using
`FOR UPDATE SKIP LOCKED` so future multi-task deployments do not race.

### Configuration

Replace or extend `[storage]`.

Initial target:

```toml
[storage]
database_url = { env = "ORCH_DATABASE_URL" }
backend = "postgres"
```

Implementation notes:

- Keep `sqlite_path` available for local/dev mode if SQLite remains
  supported.
- Read production secrets from ECS task environment variables populated
  from Secrets Manager.
- Do not put DB passwords in the config file or container image.
- Set SQLx pool options deliberately: small max connections, zero or
  low min connections, short idle timeout.

### Aurora idle behavior

Aurora Serverless v2 can scale to 0 ACUs on supported Aurora
PostgreSQL versions, but only when there are no user connections. The
current app will usually keep Aurora awake because:

- the dispatcher polls frequently;
- SQLx may keep idle pooled connections open;
- any DB-backed health check would also wake the DB.

To get real scale-to-zero savings:

- Change dispatcher behavior from fixed frequent polling to mostly
  event-driven wakeups.
- Use an in-process `Notify` to wake the dispatcher after ingest,
  webhook handling, and action finalization.
- Keep a slow fallback poll, for example every 30-60 seconds, to recover
  missed in-process wakeups in the single-task deployment.
- Configure the DB pool with `min_connections = 0` and a short
  `idle_timeout`.
- Make ECS/ALB health checks process-only. Do not hit Aurora from the
  health endpoint.
- Add connection retry logic for Aurora resume latency. AWS documents
  typical resume around 15 seconds, and longer after deep idle.

If we keep the 250 ms DB poll, budget as though Aurora is always at the
minimum active capacity.

### HTTP server shape

ECS Express exposes a container through one service/load balancer
shape. The app currently has separate webhook and ingest listeners.

Simplify to one public HTTP listener:

- `/webhook` for GitHub webhooks.
- `/tickets` for authenticated ticket ingest.
- `/healthz` for ECS/ALB health checks.

Keep bearer-token validation for `/tickets` when network reachable.
Keep GitHub HMAC validation for `/webhook`.

### Container and ECS

Add deployment artifacts:

- Multi-stage Dockerfile that builds the Rust release binary.
- ECS task definition compatible with ECS Express.
- Health check endpoint and container health check.
- `RUST_LOG` default suitable for JSON logs.
- Graceful shutdown support remains important because the app already
  maps subsystem drain results to process exit codes.

Start with desired count 1. Only raise desired count after the Postgres
storage path uses `FOR UPDATE SKIP LOCKED` and the workflow write paths
have been tested under concurrent tasks.

## Implementation sequence

1. Add Postgres dependencies and a second schema/migration path.
2. Refactor `Storage` so SQLite and Postgres can share the public API.
3. Port `advance`, event reads, action claiming, lease renewal,
   finalization, failure recording, and sink health methods.
4. Port app-level SQL in ingest and webhook lookup.
5. Add Postgres integration tests using a disposable local Postgres.
6. Add unified HTTP listener and `/healthz`.
7. Add idle-friendly dispatcher wakeups and DB pool tuning.
8. Add Dockerfile and ECS task/service config.
9. Deploy to a dev ECS service with Aurora min 0 ACUs.
10. Run crash recovery, webhook redelivery, and agent failure tests.

## Test checklist

- Ingest creates the first event and derived agent action in Postgres.
- Re-posting an identical ticket returns idempotent success.
- Re-posting the same dedup key with different payload returns conflict.
- Dispatcher claims one action exactly once under concurrent claimers.
- A task killed mid-agent-run recovers through `/status`.
- Webhook lookup resolves `github.pr_opened.v1` using Postgres JSONB.
- GitHub webhook HMAC validation still rejects bad signatures.
- Aurora resume from pause is tolerated by connection retry settings.
- ECS SIGTERM drains dispatcher, webhook, and ingest paths within the
  configured grace period.
- CloudWatch logs contain enough fields to follow `workflow_id`,
  `action_id`, and `request_id`.

## Rough monthly cost

Region assumed: `ap-southeast-2` (Sydney).
Currency conversion used: `1 USD ~= 1.386 AUD`.
Month assumed: 730 hours.

These estimates exclude GST, LLM/model calls, GitHub traffic, domain
name costs, and any AgentCore or external agent runtime spend.

| Scenario | Rough USD/month | Rough AUD/month |
| --- | ---: | ---: |
| ECS Express default x86 task, public ALB, Aurora active at 0.5 ACU | 150 | 208 |
| Same, but private task uses one NAT Gateway | 190 | 263 |
| Three default ECS tasks, public ALB, Aurora active at 0.5 ACU | 244 | 338 |
| Tuned small ARM task, public ALB, Aurora active at 0.5 ACU | 116 | 160 |
| ECS Express default x86 task, public ALB, Aurora mostly paused | 77 | 107 |
| Tuned small ARM task, public ALB, Aurora mostly paused | 43 | 60 |

Key unit prices observed from AWS public pricing data for Sydney:

- Fargate x86 vCPU: USD 0.04856 per vCPU-hour.
- Fargate x86 memory: USD 0.00532 per GB-hour.
- Fargate ARM vCPU: USD 0.03885 per vCPU-hour.
- Fargate ARM memory: USD 0.00426 per GB-hour.
- Aurora Serverless v2 PostgreSQL standard: USD 0.20 per ACU-hour.
- Aurora Serverless v2 PostgreSQL I/O optimized: USD 0.26 per ACU-hour.
- Aurora storage: USD 0.11 per GB-month.
- Aurora I/O: USD 0.22 per million requests.
- Application Load Balancer: USD 0.0252 per hour plus LCU usage.
- Public IPv4 address: USD 0.005 per hour.
- NAT Gateway: USD 0.059 per hour plus data processing.
- CloudWatch Logs ingest: USD 0.67 per GB.

Cost interpretation:

- Aurora is the dominant cost if it is kept awake at 0.5 ACU.
- NAT Gateway is expensive relative to this workload. Prefer public
  subnet tasks with tight security groups or VPC endpoints where
  acceptable, or accept the NAT cost for stricter private networking.
- The ALB is a fixed baseline. For very low-traffic personal use, it
  can cost more than the app container.
- Scale-to-zero Aurora only helps if the app stops polling and releases
  idle DB connections.

## Open decisions for tomorrow

- Keep SQLite as a supported local backend, or cut over fully to
  Postgres?
- Use direct Aurora connections first, or add RDS Proxy? Direct is
  cheaper and allows auto-pause. RDS Proxy keeps connections open and
  can prevent auto-pause.
- Keep desired count at 1 for v1, or design for multi-task ECS from the
  start?
- Use public ECS tasks behind ALB, or private tasks plus NAT/VPC
  endpoints?
- Put agents on AgentCore Runtime, ECS, or another service?

## References

- ECS Express Mode overview:
  https://docs.aws.amazon.com/AmazonECS/latest/developerguide/express-service-overview.html
- Fargate pricing:
  https://aws.amazon.com/fargate/pricing/
- Aurora pricing:
  https://aws.amazon.com/rds/aurora/pricing/
- Aurora Serverless v2 auto-pause:
  https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2-auto-pause.html
- Elastic Load Balancing pricing:
  https://aws.amazon.com/elasticloadbalancing/pricing/
- VPC and public IPv4 pricing:
  https://aws.amazon.com/vpc/pricing/
- CloudWatch pricing:
  https://aws.amazon.com/cloudwatch/pricing/
