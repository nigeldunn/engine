//! Test-only helpers that need privileged access to `Storage` internals
//! (the pool, raw SQL). Keep this module thin: anything genuinely
//! production-shaped belongs on `Storage`'s public surface.
//!
//! Marked `#[doc(hidden)]` at the lib root: this is **not a stable public
//! API**. It is `pub` purely so cross-crate integration tests can call
//! the helpers; production code MUST NOT import from this module.

use std::env;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde_json::Value as Json;
use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection, Row};
use uuid::Uuid;

use crate::ids::WorkflowId;
use crate::storage::Storage;

const ADMIN_URL_VAR: &str = "TEST_DATABASE_URL";

/// Token that the caller binds to keep the per-test database alive for
/// the test scope. The current implementation does NOT clean up on
/// Drop: a sync Drop cannot await pool closure, and a `tokio::spawn` of
/// `DROP DATABASE WITH (FORCE)` races against the still-open Storage
/// pool (which is dropped *after* this binding under tuple destructuring
/// order, so its connections would be torn down by FORCE while the
/// owning test is still using them).
///
/// Orphan `test_*` databases therefore accumulate across local
/// `cargo test` invocations. They are reset by either of:
///  - `docker compose down -v` (drops the volume),
///  - `docker exec orch-pg psql -U orch -c
///     "DO $$ DECLARE r record; BEGIN FOR r IN
///        SELECT datname FROM pg_database WHERE datname LIKE 'test_%'
///        LOOP EXECUTE format('DROP DATABASE %I WITH (FORCE)', r.datname);
///        END LOOP; END $$;"`.
///
/// CI runs against an ephemeral service container, so orphans are
/// reset between jobs without operator action.
pub struct DbGuard {
    db_name: String,
    admin_opts: PgConnectOptions,
}

impl DbGuard {
    pub fn db_name(&self) -> &str {
        &self.db_name
    }
}

/// Create a fresh, empty database against the admin connection in
/// `TEST_DATABASE_URL`, run migrations, and return the resulting
/// `Storage` plus a binding token.
///
/// Panics if `TEST_DATABASE_URL` is unset — the docker-compose Postgres
/// must be running. CI sets this env var via the service-container
/// configuration; local development runs `docker compose up -d`.
pub async fn fresh_storage() -> (Storage, DbGuard) {
    let admin_url = env::var(ADMIN_URL_VAR).unwrap_or_else(|_| {
        panic!(
            "{ADMIN_URL_VAR} not set; start docker-compose Postgres and \
             export TEST_DATABASE_URL \
             (e.g. postgres://orch:orch@localhost:5432/postgres)"
        )
    });
    let admin_opts = PgConnectOptions::from_str(&admin_url)
        .expect("TEST_DATABASE_URL is not a valid Postgres URL");

    // UUIDv7 is monotonic + collision-free across parallel test threads;
    // the `simple` form gives a 32-char hex string that's a valid SQL
    // identifier without quoting concerns.
    let db_name = format!("test_{}", Uuid::now_v7().simple());

    {
        let mut conn = PgConnection::connect_with(&admin_opts)
            .await
            .expect("connect to admin DB");
        sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
            .execute(&mut conn)
            .await
            .expect("CREATE DATABASE");
    }

    let test_opts = admin_opts.clone().database(&db_name);
    let storage = Storage::open_with_options(test_opts)
        .await
        .expect("open Storage on fresh test DB");

    (storage, DbGuard { db_name, admin_opts })
}

/// Open a fresh `Storage` against an existing test database. Used by
/// tests that need to verify cross-restart behaviour (e.g. snapshot
/// state-version migration), where a single test reopens the same
/// database under a new reducer and asserts replay semantics.
pub async fn reopen(guard: &DbGuard) -> Storage {
    let opts = guard.admin_opts.clone().database(&guard.db_name);
    Storage::open_with_options(opts)
        .await
        .expect("reopen Storage")
}

/// Insert a synthetic `github.pr_opened.v1` event directly into the
/// `events` table for routing tests. Bypasses the reducer (which would
/// reject the event without prior workflow state); used to seed
/// the resolver query without standing up a full workflow lifecycle.
pub async fn insert_pr_opened_event(
    storage: &Storage,
    workflow_id: &str,
    owner: &str,
    name: &str,
    pr_number: u64,
) {
    let payload: Json = serde_json::json!({
        "action_id": "test-action",
        "repo": { "owner": owner, "name": name },
        "pr_number": pr_number,
        "html_url": format!("https://github.com/{owner}/{name}/pull/{pr_number}"),
        "state": "open",
    });
    let recorded_at: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
    sqlx::query(
        r#"
        INSERT INTO events (
            workflow_id, sequence, event_id, recorded_at,
            payload_type, payload_schema_version,
            causation_kind, causation_ref, payload, ingress_dedup_key
        ) VALUES ($1, $2, $3, $4, 'github.pr_opened.v1', 1, 'system', NULL, $5, NULL)
        "#,
    )
    .bind(workflow_id)
    .bind(0_i64)
    .bind(format!("ev-{workflow_id}-{pr_number}"))
    .bind(recorded_at)
    .bind(&payload)
    .execute(storage.pool())
    .await
    .unwrap();
}

/// Read the cached workflow state JSON directly from the `snapshots`
/// table. There is no production read path for this — snapshots are an
/// internal cache — but tests use the snapshot as a deterministic
/// observable for reducer outcomes (e.g. `status`, `merge_commit_sha`)
/// that aren't otherwise visible from the event log alone.
pub async fn read_snapshot_state(
    storage: &Storage,
    workflow_id: &WorkflowId,
) -> Option<Json> {
    let row = sqlx::query("SELECT state_blob FROM snapshots WHERE workflow_id = $1")
        .bind(workflow_id.as_str())
        .fetch_optional(storage.pool())
        .await
        .expect("snapshot query failed")?;
    Some(row.try_get::<Json, _>("state_blob").expect("state_blob column"))
}
