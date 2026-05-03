//! Storage layer. Holds the transactional invariant that events, snapshot
//! updates, and outbox rows commit together or not at all.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::Value as Json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool, Transaction};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, instrument};

use crate::action::{Action, ClaimedAction};
use crate::error::ExecutorError;
use crate::event::{AdvanceOutcome, Causation, EventCommand, EventEnvelope};
use crate::health::{
    EndpointHint, HintExtractor, PersistedHealthState, SinkHealthRecord, SinkHealthScope,
    SinkUnhealthyReason,
};
use crate::ids::{ActionId, DispatcherId, EventId, WorkflowId};
use crate::reducer::{state_from_json, state_to_json, Reducer};

const SCHEMA_SQL: &str = include_str!("schema.sql");

#[derive(Clone)]
pub struct Storage {
    pool: SqlitePool,
}

impl Storage {
    pub async fn open(database_url: &str) -> Result<Self, ExecutorError> {
        let opts = SqliteConnectOptions::from_str(database_url)
            .map_err(|e| ExecutorError::Internal(e.to_string()))?
            .create_if_missing(true)
            .pragma("journal_mode", "WAL")
            .pragma("synchronous", "NORMAL")
            .pragma("foreign_keys", "ON")
            .pragma("busy_timeout", "5000");

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await?;

        sqlx::query(SCHEMA_SQL).execute(&pool).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool { &self.pool }

    /// The core method. Atomically:
    ///   1. Checks ingress dedup
    ///   2. Reads current sequence
    ///   3. Loads prior state
    ///   4. Applies reducer → new state, derived actions
    ///   5. Inserts event + snapshot + outbox rows
    ///
    /// On sequence conflict, returns SequenceConflict so the caller can retry.
    #[instrument(skip(self, reducer, cmd), fields(
        workflow_id = %cmd.workflow_id,
        payload_type = %cmd.payload_type,
    ))]
    pub async fn advance<R: Reducer>(
        &self,
        reducer: &R,
        cmd: &EventCommand,
    ) -> Result<AdvanceOutcome, ExecutorError> {
        let mut tx = self.pool.begin().await?;

        // Ingress dedup: if we've already processed this command, return prior outcome.
        if let Some(key) = &cmd.ingress_dedup_key {
            if let Some(prior) = lookup_by_dedup_key(&mut tx, key).await? {
                tx.rollback().await?;
                debug!(?prior, "ingress dedup hit");
                return Ok(AdvanceOutcome {
                    event_id: prior.event_id,
                    sequence: prior.sequence,
                    actions_enqueued: vec![],
                    deduplicated: true,
                });
            }
        }

        // Read current head sequence for this workflow.
        let current: Option<i64> = sqlx::query_scalar(
            "SELECT MAX(sequence) FROM events WHERE workflow_id = ?",
        )
        .bind(cmd.workflow_id.as_str())
        .fetch_one(&mut *tx)
        .await?;

        let next_seq: u64 = match current {
            Some(s) => (s + 1) as u64,
            None => 0,
        };

        let event_id = EventId::new();
        let recorded_at = Utc::now();

        // Build the envelope so we can pass it to the reducer.
        let envelope = EventEnvelope {
            event_id: event_id.clone(),
            workflow_id: cmd.workflow_id.clone(),
            sequence: next_seq,
            recorded_at,
            payload_type: cmd.payload_type.clone(),
            payload_schema_version: cmd.payload_schema_version,
            causation: cmd.causation.clone(),
            trace_id: cmd.trace_id.clone(),
            payload: cmd.payload.clone(),
        };

        // Load prior state (or default). If the snapshot's state_version
        // doesn't match the reducer's current state_version, the snapshot
        // is from an older schema and would deserialize wrong (or
        // silently misinterpret). Discard it and replay the event log to
        // rebuild — snapshots are a cache, the event log is authoritative.
        let prior_state: R::State = load_prior_state(&mut tx, reducer, &cmd.workflow_id).await?;

        // Pure reduction.
        let new_state = reducer.reduce(prior_state, &envelope)?;
        let actions = reducer.derive_actions(&new_state, &envelope)?;

        // Insert the event. PK collision = SequenceConflict.
        let payload_str = serde_json::to_string(&cmd.payload)?;
        let causation_kind = cmd.causation.kind();
        let causation_ref = cmd.causation.ref_id();

        let result = sqlx::query(
            r#"
            INSERT INTO events (
                workflow_id, sequence, event_id, recorded_at,
                payload_type, payload_schema_version,
                causation_kind, causation_ref, payload, ingress_dedup_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(cmd.workflow_id.as_str())
        .bind(next_seq as i64)
        .bind(event_id.0.as_str())
        .bind(recorded_at)
        .bind(&cmd.payload_type)
        .bind(cmd.payload_schema_version as i64)
        .bind(causation_kind)
        .bind(causation_ref)
        .bind(&payload_str)
        .bind(cmd.ingress_dedup_key.as_deref())
        .execute(&mut *tx)
        .await;

        if let Err(sqlx::Error::Database(db_err)) = &result {
            if db_err.is_unique_violation() {
                tx.rollback().await?;
                return Err(ExecutorError::SequenceConflict);
            }
        }
        result?;

        // Update snapshot.
        let state_json = state_to_json(&new_state)?;
        let state_str = serde_json::to_string(&state_json)?;
        sqlx::query(
            r#"
            INSERT INTO snapshots (workflow_id, sequence, state_blob, state_version, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(workflow_id) DO UPDATE SET
                sequence      = excluded.sequence,
                state_blob    = excluded.state_blob,
                state_version = excluded.state_version,
                updated_at    = excluded.updated_at
            "#,
        )
        .bind(cmd.workflow_id.as_str())
        .bind(next_seq as i64)
        .bind(&state_str)
        .bind(reducer.state_version() as i64)
        .bind(recorded_at)
        .execute(&mut *tx)
        .await?;

        // Insert outbox rows. ActionId is deterministic from (workflow, seq, idx, kind).
        let mut enqueued = Vec::with_capacity(actions.len());
        for (idx, action) in actions.iter().enumerate() {
            let action_id =
                ActionId::derive(&cmd.workflow_id, next_seq, idx as u32, &action.kind);
            insert_outbox_row(&mut tx, &action_id, &cmd.workflow_id, next_seq, action, recorded_at)
                .await?;
            enqueued.push(action_id);
        }

        tx.commit().await?;

        Ok(AdvanceOutcome {
            event_id,
            sequence: next_seq,
            actions_enqueued: enqueued,
            deduplicated: false,
        })
    }

    /// Atomically claim up to `batch_size` actions for this dispatcher.
    /// Picks up genuinely-pending rows AND reclaims expired leases.
    ///
    /// Contract change from v1: claim does NOT increment `attempt`.
    /// `attempt` is only incremented when an execute returns a real outcome.
    /// This means crashes between claim and execute do not burn attempts.
    ///
    /// `kinds_filter`: if non-empty, only actions whose kind is in this set
    /// are claimed. Used by the dispatcher to skip kinds belonging to
    /// unhealthy sinks.
    #[instrument(skip(self, kinds_filter), fields(dispatcher = %dispatcher_id))]
    pub async fn claim_actions(
        &self,
        dispatcher_id: &DispatcherId,
        batch_size: u32,
        lease_duration: Duration,
        kinds_filter: &[&str],
    ) -> Result<Vec<ClaimedAction>, ExecutorError> {
        let now = Utc::now();
        let lease_expires = now + to_chrono(lease_duration);

        if kinds_filter.is_empty() {
            // No healthy sinks - nothing to claim.
            return Ok(vec![]);
        }

        let mut tx = self.pool.begin().await?;

        // Build a parameterized IN clause for kinds.
        let placeholders: String = (0..kinds_filter.len())
            .map(|_| "?".to_string())
            .collect::<Vec<_>>()
            .join(",");
        let candidate_sql = format!(
            r#"
            SELECT action_id FROM actions_outbox
            WHERE state = 'pending'
              AND next_attempt_at <= ?
              AND action_kind IN ({})
            UNION ALL
            SELECT action_id FROM actions_outbox
            WHERE state = 'in_progress'
              AND lease_expires_at <= ?
              AND action_kind IN ({})
            LIMIT ?
            "#,
            placeholders, placeholders
        );

        let mut q = sqlx::query_scalar::<_, String>(&candidate_sql).bind(now);
        for k in kinds_filter {
            q = q.bind(*k);
        }
        q = q.bind(now);
        for k in kinds_filter {
            q = q.bind(*k);
        }
        q = q.bind(batch_size as i64);

        let candidates: Vec<String> = q.fetch_all(&mut *tx).await?;

        if candidates.is_empty() {
            tx.rollback().await?;
            return Ok(vec![]);
        }

        let mut claimed = Vec::with_capacity(candidates.len());
        for action_id_str in candidates {
            // Set lease. Do NOT increment attempt - that happens on outcome only.
            let row = sqlx::query(
                r#"
                UPDATE actions_outbox
                SET state            = 'in_progress',
                    claimed_by       = ?,
                    lease_expires_at = ?,
                    updated_at       = ?
                WHERE action_id = ?
                  AND (state = 'pending'
                       OR (state = 'in_progress' AND lease_expires_at <= ?))
                RETURNING workflow_id, source_sequence, action_kind, payload,
                          attempt, max_attempts, probe_attempt, max_probe_attempts
                "#,
            )
            .bind(dispatcher_id.as_str())
            .bind(lease_expires)
            .bind(now)
            .bind(&action_id_str)
            .bind(now)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = row {
                let workflow_id: String = row.get("workflow_id");
                let source_sequence: i64 = row.get("source_sequence");
                let kind: String = row.get("action_kind");
                let payload_str: String = row.get("payload");
                let attempt: i64 = row.get("attempt");
                let max_attempts: i64 = row.get("max_attempts");
                let probe_attempt: i64 = row.get("probe_attempt");
                let max_probe_attempts: i64 = row.get("max_probe_attempts");

                let payload: Json = serde_json::from_str(&payload_str)?;

                claimed.push(ClaimedAction {
                    action_id: ActionId(action_id_str),
                    workflow_id: WorkflowId(workflow_id),
                    source_sequence: source_sequence as u64,
                    kind,
                    payload,
                    attempt: attempt as u32,
                    max_attempts: max_attempts as u32,
                    probe_attempt: probe_attempt as u32,
                    max_probe_attempts: max_probe_attempts as u32,
                    claimed_by: dispatcher_id.clone(),
                    lease_expires_at: lease_expires,
                });
            }
            // If row is None, someone else claimed it between our SELECT and UPDATE.
        }

        tx.commit().await?;
        Ok(claimed)
    }

    /// Record the start of an execute attempt. Called by the dispatcher
    /// immediately before invoking `Sink::execute`. Inserts an audit row
    /// with the next attempt number; the corresponding outcome will be
    /// recorded by `finalize_succeeded`, `record_transient_failure`,
    /// or `record_permanent_failure`.
    pub async fn record_attempt_start(
        &self,
        action_id: &ActionId,
        next_attempt: u32,
    ) -> Result<(), ExecutorError> {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO action_attempts (action_id, attempt, started_at)
            VALUES (?, ?, ?)
            ON CONFLICT(action_id, attempt) DO NOTHING
            "#,
        )
        .bind(action_id.as_str())
        .bind(next_attempt as i64)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Renew an in-progress lease. Returns Err if the lease has been lost
    /// (action no longer claimed by us, or no longer in_progress).
    #[instrument(skip(self), fields(action_id = %action_id, dispatcher = %dispatcher_id))]
    pub async fn renew_lease(
        &self,
        action_id: &ActionId,
        dispatcher_id: &DispatcherId,
        lease_duration: Duration,
    ) -> Result<(), ExecutorError> {
        let now = Utc::now();
        let new_expiry = now + to_chrono(lease_duration);

        let rows = sqlx::query(
            r#"
            UPDATE actions_outbox
            SET lease_expires_at = ?, updated_at = ?
            WHERE action_id = ? AND claimed_by = ? AND state = 'in_progress'
            "#,
        )
        .bind(new_expiry)
        .bind(now)
        .bind(action_id.as_str())
        .bind(dispatcher_id.as_str())
        .execute(&self.pool)
        .await?
        .rows_affected();

        if rows == 0 {
            return Err(ExecutorError::Internal("lease lost".into()));
        }
        Ok(())
    }

    /// Mark an action succeeded and finalize the audit row.
    /// Caller must ensure the outcome event has already been written via `advance`.
    ///
    /// Increments `attempt` (since claim no longer does) and writes the audit
    /// row outcome.
    #[instrument(skip(self), fields(action_id = %action_id))]
    pub async fn finalize_succeeded(
        &self,
        action_id: &ActionId,
        dispatcher_id: &DispatcherId,
        external_ref: Option<String>,
        outcome_event_id: Option<EventId>,
    ) -> Result<(), ExecutorError> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;

        // Verify lease and read current attempt.
        let row = sqlx::query(
            r#"
            SELECT attempt FROM actions_outbox
            WHERE action_id = ? AND claimed_by = ? AND state = 'in_progress'
            "#,
        )
        .bind(action_id.as_str())
        .bind(dispatcher_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.rollback().await?;
            return Err(ExecutorError::Internal("lease lost during finalize".into()));
        };
        let prior_attempt: i64 = row.get("attempt");
        let new_attempt = prior_attempt + 1;

        sqlx::query(
            r#"
            UPDATE actions_outbox
            SET state            = 'succeeded',
                attempt          = ?,
                external_ref     = COALESCE(?, external_ref),
                outcome_event_id = ?,
                claimed_by       = NULL,
                lease_expires_at = NULL,
                updated_at       = ?
            WHERE action_id = ?
            "#,
        )
        .bind(new_attempt)
        .bind(external_ref.as_deref())
        .bind(outcome_event_id.as_ref().map(|e| e.0.as_str()))
        .bind(now)
        .bind(action_id.as_str())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE action_attempts
            SET finished_at = ?, outcome = 'success', external_ref = ?
            WHERE action_id = ? AND attempt = ?
            "#,
        )
        .bind(now)
        .bind(external_ref.as_deref())
        .bind(action_id.as_str())
        .bind(new_attempt)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Mark a transient failure; schedule a retry with exponential backoff
    /// (unless max attempts reached, in which case mark failed).
    /// Increments `attempt`.
    /// Returns true if scheduled for retry, false if exhausted.
    #[instrument(skip(self), fields(action_id = %action_id))]
    pub async fn record_transient_failure(
        &self,
        action_id: &ActionId,
        dispatcher_id: &DispatcherId,
        error: &str,
    ) -> Result<bool, ExecutorError> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            SELECT attempt, max_attempts FROM actions_outbox
            WHERE action_id = ? AND claimed_by = ? AND state = 'in_progress'
            "#,
        )
        .bind(action_id.as_str())
        .bind(dispatcher_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.rollback().await?;
            return Err(ExecutorError::Internal("lease lost during failure record".into()));
        };

        let prior_attempt: i64 = row.get("attempt");
        let max_attempts: i64 = row.get("max_attempts");
        let new_attempt = prior_attempt + 1;

        if new_attempt >= max_attempts {
            // Permanent failure - this attempt is the last one.
            sqlx::query(
                r#"
                UPDATE actions_outbox
                SET state            = 'failed',
                    attempt          = ?,
                    last_error       = ?,
                    claimed_by       = NULL,
                    lease_expires_at = NULL,
                    updated_at       = ?
                WHERE action_id = ?
                "#,
            )
            .bind(new_attempt)
            .bind(error)
            .bind(now)
            .bind(action_id.as_str())
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                UPDATE action_attempts
                SET finished_at = ?, outcome = 'permanent_fail',
                    error_kind = 'budget_exhausted', error_message = ?
                WHERE action_id = ? AND attempt = ?
                "#,
            )
            .bind(now)
            .bind(error)
            .bind(action_id.as_str())
            .bind(new_attempt)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            return Ok(false);
        }

        // Schedule retry with exponential backoff + jitter.
        let backoff = backoff_duration(new_attempt as u32);
        let next_at = now + backoff;

        sqlx::query(
            r#"
            UPDATE actions_outbox
            SET state            = 'pending',
                attempt          = ?,
                next_attempt_at  = ?,
                last_error       = ?,
                claimed_by       = NULL,
                lease_expires_at = NULL,
                updated_at       = ?
            WHERE action_id = ?
            "#,
        )
        .bind(new_attempt)
        .bind(next_at)
        .bind(error)
        .bind(now)
        .bind(action_id.as_str())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE action_attempts
            SET finished_at = ?, outcome = 'transient_fail', error_message = ?
            WHERE action_id = ? AND attempt = ?
            "#,
        )
        .bind(now)
        .bind(error)
        .bind(action_id.as_str())
        .bind(new_attempt)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(true)
    }

    /// Mark a permanent failure - no retry. Increments `attempt`.
    #[instrument(skip(self), fields(action_id = %action_id))]
    pub async fn record_permanent_failure(
        &self,
        action_id: &ActionId,
        dispatcher_id: &DispatcherId,
        error: &str,
    ) -> Result<(), ExecutorError> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;

        let prior_attempt: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT attempt FROM actions_outbox
            WHERE action_id = ? AND claimed_by = ? AND state = 'in_progress'
            "#,
        )
        .bind(action_id.as_str())
        .bind(dispatcher_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;

        let Some(prior_attempt) = prior_attempt else {
            tx.rollback().await?;
            return Err(ExecutorError::Internal("lease lost during failure record".into()));
        };
        let new_attempt = prior_attempt + 1;

        sqlx::query(
            r#"
            UPDATE actions_outbox
            SET state            = 'failed',
                attempt          = ?,
                last_error       = ?,
                claimed_by       = NULL,
                lease_expires_at = NULL,
                updated_at       = ?
            WHERE action_id = ?
            "#,
        )
        .bind(new_attempt)
        .bind(error)
        .bind(now)
        .bind(action_id.as_str())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            UPDATE action_attempts
            SET finished_at = ?, outcome = 'permanent_fail', error_message = ?
            WHERE action_id = ? AND attempt = ?
            "#,
        )
        .bind(now)
        .bind(error)
        .bind(action_id.as_str())
        .bind(new_attempt)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    /// Return an in-progress action to pending without incrementing `attempt`.
    /// Used when the sink reports unhealthy: the action never really had a
    /// chance to run, so it should not burn a retry. Schedules a short
    /// retry delay so the dispatcher polls the queue again soon.
    #[instrument(skip(self), fields(action_id = %action_id))]
    pub async fn return_to_pending(
        &self,
        action_id: &ActionId,
        dispatcher_id: &DispatcherId,
        retry_delay: Duration,
        reason: &str,
    ) -> Result<(), ExecutorError> {
        let now = Utc::now();
        let next_at = now + to_chrono(retry_delay);

        let rows = sqlx::query(
            r#"
            UPDATE actions_outbox
            SET state            = 'pending',
                next_attempt_at  = ?,
                last_error       = ?,
                claimed_by       = NULL,
                lease_expires_at = NULL,
                updated_at       = ?
            WHERE action_id = ? AND claimed_by = ? AND state = 'in_progress'
            "#,
        )
        .bind(next_at)
        .bind(reason)
        .bind(now)
        .bind(action_id.as_str())
        .bind(dispatcher_id.as_str())
        .execute(&self.pool)
        .await?
        .rows_affected();

        if rows == 0 {
            return Err(ExecutorError::Internal(
                "return_to_pending: lease lost or row not in_progress".into(),
            ));
        }
        Ok(())
    }

    /// Record a probe failure. Increments `probe_attempt` (NOT `attempt`)
    /// and schedules a retry. If `probe_attempt` exhausts `max_probe_attempts`,
    /// transitions the action to `failed_probe_exhausted` (operationally
    /// distinct from `failed`).
    /// Returns true if scheduled for retry, false if exhausted.
    #[instrument(skip(self), fields(action_id = %action_id))]
    pub async fn record_probe_failure(
        &self,
        action_id: &ActionId,
        dispatcher_id: &DispatcherId,
        error: &str,
    ) -> Result<bool, ExecutorError> {
        let now = Utc::now();
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            SELECT probe_attempt, max_probe_attempts FROM actions_outbox
            WHERE action_id = ? AND claimed_by = ? AND state = 'in_progress'
            "#,
        )
        .bind(action_id.as_str())
        .bind(dispatcher_id.as_str())
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.rollback().await?;
            return Err(ExecutorError::Internal(
                "lease lost during probe failure record".into(),
            ));
        };
        let prior_probe: i64 = row.get("probe_attempt");
        let max_probe: i64 = row.get("max_probe_attempts");
        let new_probe = prior_probe + 1;

        if new_probe >= max_probe {
            sqlx::query(
                r#"
                UPDATE actions_outbox
                SET state            = 'failed_probe_exhausted',
                    probe_attempt    = ?,
                    last_error       = ?,
                    claimed_by       = NULL,
                    lease_expires_at = NULL,
                    updated_at       = ?
                WHERE action_id = ?
                "#,
            )
            .bind(new_probe)
            .bind(error)
            .bind(now)
            .bind(action_id.as_str())
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            return Ok(false);
        }

        // Probe-specific backoff: gentler than execute backoff.
        // 30s, 60s, 120s, 240s, capped at 5min.
        let backoff_secs = (30u64.saturating_mul(2u64.saturating_pow(new_probe.min(8) as u32)))
            .min(300);
        let next_at = now + ChronoDuration::seconds(backoff_secs as i64);

        sqlx::query(
            r#"
            UPDATE actions_outbox
            SET state            = 'pending',
                probe_attempt    = ?,
                next_attempt_at  = ?,
                last_error       = ?,
                claimed_by       = NULL,
                lease_expires_at = NULL,
                updated_at       = ?
            WHERE action_id = ?
            "#,
        )
        .bind(new_probe)
        .bind(next_at)
        .bind(error)
        .bind(now)
        .bind(action_id.as_str())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(true)
    }

    /// Mark a sink as unhealthy. Idempotent UPSERT.
    #[instrument(skip(self))]
    pub async fn mark_sink_unhealthy(
        &self,
        sink_key: &str,
        reason: SinkUnhealthyReason,
        detail: &str,
    ) -> Result<(), ExecutorError> {
        let now = Utc::now();
        // Default next_check 60s from now; the dispatcher will probe at this cadence.
        let next_check = now + ChronoDuration::seconds(60);
        sqlx::query(
            r#"
            INSERT INTO sink_health (
                sink_key, state, reason, detail,
                updated_at, last_check_at, next_check_at
            ) VALUES (?, 'unhealthy', ?, ?, ?, ?, ?)
            ON CONFLICT(sink_key) DO UPDATE SET
                state         = 'unhealthy',
                reason        = excluded.reason,
                detail        = excluded.detail,
                updated_at    = excluded.updated_at,
                last_check_at = excluded.last_check_at,
                next_check_at = excluded.next_check_at
            "#,
        )
        .bind(sink_key)
        .bind(reason.as_str())
        .bind(detail)
        .bind(now)
        .bind(now)
        .bind(next_check)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Mark a sink as healthy. Idempotent UPSERT.
    #[instrument(skip(self))]
    pub async fn mark_sink_healthy(&self, sink_key: &str) -> Result<(), ExecutorError> {
        let now = Utc::now();
        sqlx::query(
            r#"
            INSERT INTO sink_health (
                sink_key, state, reason, detail,
                updated_at, last_check_at, next_check_at
            ) VALUES (?, 'healthy', NULL, NULL, ?, ?, NULL)
            ON CONFLICT(sink_key) DO UPDATE SET
                state         = 'healthy',
                reason        = NULL,
                detail        = NULL,
                updated_at    = excluded.updated_at,
                last_check_at = excluded.last_check_at,
                next_check_at = NULL
            "#,
        )
        .bind(sink_key)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Read a sink's persisted health record.
    pub async fn get_sink_health(
        &self,
        sink_key: &str,
    ) -> Result<Option<SinkHealthRecord>, ExecutorError> {
        let row = sqlx::query(
            r#"
            SELECT sink_key, state, reason, detail,
                   updated_at, last_check_at, next_check_at
            FROM sink_health
            WHERE sink_key = ?
            "#,
        )
        .bind(sink_key)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            let state_str: String = r.get("state");
            let reason_str: Option<String> = r.get("reason");
            SinkHealthRecord {
                sink_key: r.get("sink_key"),
                state: PersistedHealthState::from_str(&state_str)
                    .unwrap_or(PersistedHealthState::Healthy),
                reason: reason_str.and_then(|s| SinkUnhealthyReason::from_str(&s)),
                detail: r.get("detail"),
                updated_at: r.get("updated_at"),
                last_check_at: r.get("last_check_at"),
                next_check_at: r.get("next_check_at"),
            }
        }))
    }

    /// List all unhealthy sinks. Used by the dispatcher's health-check loop.
    pub async fn list_unhealthy_sinks(&self) -> Result<Vec<SinkHealthRecord>, ExecutorError> {
        let rows = sqlx::query(
            r#"
            SELECT sink_key, state, reason, detail,
                   updated_at, last_check_at, next_check_at
            FROM sink_health
            WHERE state = 'unhealthy'
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let state_str: String = r.get("state");
            let reason_str: Option<String> = r.get("reason");
            out.push(SinkHealthRecord {
                sink_key: r.get("sink_key"),
                state: PersistedHealthState::from_str(&state_str)
                    .unwrap_or(PersistedHealthState::Healthy),
                reason: reason_str.and_then(|s| SinkUnhealthyReason::from_str(&s)),
                detail: r.get("detail"),
                updated_at: r.get("updated_at"),
                last_check_at: r.get("last_check_at"),
                next_check_at: r.get("next_check_at"),
            });
        }
        Ok(out)
    }

    /// Return the set of sink_keys that are currently persisted unhealthy.
    /// Used to filter claim queries.
    pub async fn unhealthy_sink_keys(&self) -> Result<Vec<String>, ExecutorError> {
        let keys: Vec<String> = sqlx::query_scalar(
            "SELECT sink_key FROM sink_health WHERE state = 'unhealthy'",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(keys)
    }

    /// Build a health-check scope for a sink: read up to `max_hints` recent
    /// pending or in_progress actions for the given kinds, run each registered
    /// extractor over the payloads, deduplicate, and return.
    ///
    /// `extractors` is the list of registered extractors; the caller (dispatcher)
    /// is responsible for supplying the right ones.
    pub async fn build_health_scope(
        &self,
        active_kinds: &[&str],
        extractors: &[std::sync::Arc<dyn HintExtractor>],
        max_hints: u32,
    ) -> Result<SinkHealthScope, ExecutorError> {
        if active_kinds.is_empty() {
            return Ok(SinkHealthScope::default());
        }

        let placeholders: String = (0..active_kinds.len())
            .map(|_| "?".to_string())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            r#"
            SELECT action_kind, payload FROM actions_outbox
            WHERE state IN ('pending', 'in_progress')
              AND action_kind IN ({})
            ORDER BY created_at DESC
            LIMIT ?
            "#,
            placeholders
        );

        let mut q = sqlx::query(&sql);
        for k in active_kinds {
            q = q.bind(*k);
        }
        // Read 4x the hint cap to give dedup room.
        q = q.bind((max_hints as i64).saturating_mul(4).max(40));

        let rows = q.fetch_all(&self.pool).await?;

        let mut hints: Vec<EndpointHint> = Vec::new();
        for row in rows {
            let kind: String = row.get("action_kind");
            let payload_str: String = row.get("payload");
            let payload: Json = match serde_json::from_str(&payload_str) {
                Ok(v) => v,
                Err(_) => continue,
            };
            for ex in extractors {
                if let Some(hint) = ex.extract(&kind, &payload) {
                    if !hints.contains(&hint) {
                        hints.push(hint);
                        if hints.len() >= max_hints as usize {
                            break;
                        }
                    }
                }
            }
            if hints.len() >= max_hints as usize {
                break;
            }
        }

        Ok(SinkHealthScope {
            active_kinds: active_kinds.iter().map(|s| s.to_string()).collect(),
            endpoint_hints: hints,
        })
    }

    /// Read all events for a workflow, in order. For replay and debugging.
    pub async fn read_events(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<EventEnvelope>, ExecutorError> {
        let rows = sqlx::query(
            r#"
            SELECT event_id, workflow_id, sequence, recorded_at,
                   payload_type, payload_schema_version,
                   causation_kind, causation_ref, payload
            FROM events
            WHERE workflow_id = ?
            ORDER BY sequence ASC
            "#,
        )
        .bind(workflow_id.as_str())
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let payload_str: String = row.get("payload");
            let payload: Json = serde_json::from_str(&payload_str)?;
            let causation = decode_causation(
                row.get::<&str, _>("causation_kind"),
                row.get::<Option<String>, _>("causation_ref"),
            );
            out.push(EventEnvelope {
                event_id: EventId(row.get::<String, _>("event_id")),
                workflow_id: WorkflowId(row.get::<String, _>("workflow_id")),
                sequence: row.get::<i64, _>("sequence") as u64,
                recorded_at: row.get("recorded_at"),
                payload_type: row.get("payload_type"),
                payload_schema_version: row.get::<i64, _>("payload_schema_version") as u32,
                causation,
                trace_id: None,
                payload,
            });
        }
        Ok(out)
    }
}

// ── helpers ──────────────────────────────────────────────────────────

/// Load prior state for a workflow inside the advance transaction.
///
/// Reads `snapshots` and compares `state_version`. If the snapshot's
/// version matches the reducer's current `state_version`, deserialize
/// directly. Otherwise (snapshot missing OR version mismatch from a
/// reducer schema bump) replay the event log to reconstruct state from
/// scratch. Snapshots are a cache; the event log is authoritative.
async fn load_prior_state<R: Reducer>(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    reducer: &R,
    workflow_id: &WorkflowId,
) -> Result<R::State, ExecutorError> {
    let row = sqlx::query(
        "SELECT state_blob, state_version FROM snapshots WHERE workflow_id = ?",
    )
    .bind(workflow_id.as_str())
    .fetch_optional(&mut **tx)
    .await?;

    if let Some(row) = row {
        let blob: String = row.get("state_blob");
        let version: i64 = row.get("state_version");
        if version as u32 == reducer.state_version() {
            let parsed: Json = serde_json::from_str(&blob)?;
            return state_from_json(Some(parsed));
        }
        // Version mismatch — fall through to replay.
        tracing::info!(
            workflow_id = %workflow_id,
            snapshot_version = version,
            reducer_version = reducer.state_version(),
            "snapshot state_version stale; replaying event log"
        );
    }

    replay_state_in_tx(tx, reducer, workflow_id).await
}

/// Rebuild state by replaying every event for a workflow through the
/// reducer in order. Used when no usable snapshot exists.
async fn replay_state_in_tx<R: Reducer>(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    reducer: &R,
    workflow_id: &WorkflowId,
) -> Result<R::State, ExecutorError> {
    let rows = sqlx::query(
        r#"
        SELECT event_id, sequence, recorded_at,
               payload_type, payload_schema_version,
               causation_kind, causation_ref, payload
        FROM events
        WHERE workflow_id = ?
        ORDER BY sequence ASC
        "#,
    )
    .bind(workflow_id.as_str())
    .fetch_all(&mut **tx)
    .await?;

    let mut state: R::State = state_from_json(None)?;
    for row in rows {
        let payload_str: String = row.get("payload");
        let payload: Json = serde_json::from_str(&payload_str)?;
        let causation = decode_causation(
            row.get::<&str, _>("causation_kind"),
            row.get::<Option<String>, _>("causation_ref"),
        );
        let envelope = EventEnvelope {
            event_id: EventId(row.get::<String, _>("event_id")),
            workflow_id: workflow_id.clone(),
            sequence: row.get::<i64, _>("sequence") as u64,
            recorded_at: row.get("recorded_at"),
            payload_type: row.get("payload_type"),
            payload_schema_version: row.get::<i64, _>("payload_schema_version") as u32,
            causation,
            trace_id: None,
            payload,
        };
        state = reducer.reduce(state, &envelope)?;
    }
    Ok(state)
}

#[derive(Debug)]
struct PriorOutcome {
    event_id: EventId,
    sequence: u64,
}

async fn lookup_by_dedup_key(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    key: &str,
) -> Result<Option<PriorOutcome>, ExecutorError> {
    let row = sqlx::query(
        "SELECT event_id, sequence FROM events WHERE ingress_dedup_key = ? LIMIT 1",
    )
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;

    Ok(row.map(|r| PriorOutcome {
        event_id: EventId(r.get::<String, _>("event_id")),
        sequence: r.get::<i64, _>("sequence") as u64,
    }))
}

async fn insert_outbox_row(
    tx: &mut Transaction<'_, sqlx::Sqlite>,
    action_id: &ActionId,
    workflow_id: &WorkflowId,
    source_sequence: u64,
    action: &Action,
    now: DateTime<Utc>,
) -> Result<(), ExecutorError> {
    let payload_str = serde_json::to_string(&action.payload)?;
    let next_at = now + ChronoDuration::seconds(action.delay_seconds as i64);

    sqlx::query(
        r#"
        INSERT INTO actions_outbox (
            action_id, workflow_id, source_sequence, action_kind, payload,
            state, attempt, max_attempts, probe_attempt, max_probe_attempts,
            next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, 0, ?, ?, ?, ?)
        ON CONFLICT(action_id) DO NOTHING
        "#,
    )
    .bind(action_id.as_str())
    .bind(workflow_id.as_str())
    .bind(source_sequence as i64)
    .bind(&action.kind)
    .bind(&payload_str)
    .bind(action.max_attempts as i64)
    .bind(action.max_probe_attempts as i64)
    .bind(next_at)
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

fn decode_causation(kind: &str, ref_id: Option<String>) -> Causation {
    match kind {
        "external" => Causation::External {
            source: "unknown".into(),
            request_id: ref_id.unwrap_or_default(),
        },
        "action" => Causation::Action {
            action_id: ActionId(ref_id.unwrap_or_default()),
        },
        "timer" => Causation::Timer {
            timer_id: ref_id.unwrap_or_default(),
        },
        "human" => Causation::Human {
            user_id: "unknown".into(),
            action_id: ref_id.map(ActionId),
        },
        _ => Causation::System {
            reason: "unknown".into(),
        },
    }
}

/// Convert a std::time::Duration to chrono::Duration. Saturates on overflow,
/// which won't happen for the values we use (minutes at most).
fn to_chrono(d: Duration) -> ChronoDuration {
    ChronoDuration::from_std(d).unwrap_or_else(|_| ChronoDuration::seconds(i64::MAX / 1000))
}

/// Exponential backoff with jitter. Capped at ~5 minutes. Internal helper
/// returning chrono::Duration for direct addition to DateTime<Utc>.
fn backoff_duration(attempt: u32) -> ChronoDuration {
    use rand::Rng;
    let base_ms: u64 = 500;
    let max_ms: u64 = 300_000;
    let exp = base_ms.saturating_mul(2u64.saturating_pow(attempt.min(10)));
    let capped = exp.min(max_ms);
    let jitter = rand::thread_rng().gen_range(0..=(capped / 4));
    ChronoDuration::milliseconds((capped + jitter) as i64)
}