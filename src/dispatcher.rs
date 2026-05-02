//! The dispatcher claims pending actions under a lease, executes them via
//! sinks, and writes outcome events back through the executor.
//!
//! v2 changes:
//!   - Claim does NOT increment `attempt`; outcome methods do.
//!   - Probe failure is `Err(...)` not `Ok(None)`; recorded as probe attempt,
//!     does NOT authorize execute.
//!   - Sink health is persisted; unhealthy sinks have their kinds excluded
//!     from claim queries.
//!   - Background health-check loop probes unhealthy sinks via `check_health`
//!     with a queue-derived scope.

use chrono::Duration as ChronoDuration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn};

use crate::action::{AttemptOutcome, ClaimedAction};
use crate::error::DispatcherError;
use crate::executor::Executor;
use crate::health::{HintExtractor, SinkHealthState};
use crate::ids::DispatcherId;
use crate::reducer::Reducer;
use crate::sink::Sink;

const RETURN_TO_PENDING_DELAY: ChronoDuration = ChronoDuration::seconds(30);
const MAX_HEALTH_PROBE_ENDPOINTS: u32 = 10;

pub struct DispatcherConfig {
    pub batch_size: u32,
    pub poll_interval: Duration,
    pub lease_duration: ChronoDuration,
    pub max_concurrent_attempts: usize,
    pub health_check_interval: Duration,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            batch_size: 16,
            poll_interval: Duration::from_millis(500),
            lease_duration: ChronoDuration::seconds(300),
            max_concurrent_attempts: 32,
            health_check_interval: Duration::from_secs(60),
        }
    }
}

pub struct Dispatcher<R: Reducer> {
    id: DispatcherId,
    executor: Arc<Executor<R>>,
    /// kind -> sink
    sinks_by_kind: HashMap<&'static str, Arc<dyn Sink>>,
    /// sink_key -> sink (for health-check loop lookup)
    sinks_by_key: HashMap<String, Arc<dyn Sink>>,
    /// Hint extractors run during health-scope construction.
    extractors: Vec<Arc<dyn HintExtractor>>,
    config: DispatcherConfig,
    shutdown: Arc<Notify>,
}

impl<R: Reducer> Dispatcher<R> {
    pub fn new(executor: Arc<Executor<R>>, config: DispatcherConfig) -> Self {
        Self {
            id: DispatcherId::new(),
            executor,
            sinks_by_kind: HashMap::new(),
            sinks_by_key: HashMap::new(),
            extractors: Vec::new(),
            config,
            shutdown: Arc::new(Notify::new()),
        }
    }

    pub fn id(&self) -> &DispatcherId {
        &self.id
    }

    /// Register a sink. Panics if two sinks claim the same action kind or
    /// the same sink_key.
    pub fn register<S: Sink>(&mut self, sink: S) {
        let sink: Arc<dyn Sink> = Arc::new(sink);
        let key = sink.sink_key().to_string();
        if self.sinks_by_key.insert(key.clone(), sink.clone()).is_some() {
            panic!("two sinks registered with sink_key '{}'", key);
        }
        for kind in sink.handles() {
            if self.sinks_by_kind.insert(*kind, sink.clone()).is_some() {
                panic!("two sinks registered for action kind '{}'", kind);
            }
        }
    }

    /// Register a hint extractor. Multiple extractors may apply to the same
    /// action; each is consulted during scope building.
    pub fn register_extractor<E: HintExtractor>(&mut self, extractor: E) {
        self.extractors.push(Arc::new(extractor));
    }

    pub fn shutdown_handle(&self) -> Arc<Notify> {
        self.shutdown.clone()
    }

    /// Run the dispatcher loop until shutdown.
    #[instrument(skip(self), fields(dispatcher = %self.id))]
    pub async fn run(self) -> Result<(), DispatcherError> {
        info!(dispatcher = %self.id, "dispatcher starting");
        let semaphore =
            Arc::new(tokio::sync::Semaphore::new(self.config.max_concurrent_attempts));
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        // Spawn health-check loop.
        let health_handle = {
            let executor = self.executor.clone();
            let sinks_by_key = self.sinks_by_key.clone();
            let extractors = self.extractors.clone();
            let interval = self.config.health_check_interval;
            let shutdown = self.shutdown.clone();
            tokio::spawn(async move {
                health_check_loop(executor, sinks_by_key, extractors, interval, shutdown).await;
            })
        };

        loop {
            handles.retain(|h| !h.is_finished());

            tokio::select! {
                _ = self.shutdown.notified() => {
                    info!("shutdown signal received, draining {} in-flight", handles.len());
                    for h in handles {
                        let _ = h.await;
                    }
                    let _ = health_handle.await;
                    info!(dispatcher = %self.id, "dispatcher stopped");
                    return Ok(());
                }
                _ = sleep(self.config.poll_interval) => {}
            }

            // Compute the set of healthy action kinds for this cycle.
            let healthy_kinds = match self.compute_healthy_kinds().await {
                Ok(k) => k,
                Err(e) => {
                    error!(error = %e, "failed to compute healthy kinds, backing off");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            if healthy_kinds.is_empty() {
                // All sinks unhealthy or none registered. Back off.
                debug!("no healthy sinks; idle");
                continue;
            }

            let kinds_filter: Vec<&str> = healthy_kinds.iter().map(|s| s.as_str()).collect();

            let claimed = match self
                .executor
                .storage()
                .claim_actions(
                    &self.id,
                    self.config.batch_size,
                    self.config.lease_duration,
                    &kinds_filter,
                )
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    error!(error = %e, "claim_actions failed, backing off");
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            if claimed.is_empty() {
                continue;
            }

            debug!(count = claimed.len(), "claimed actions");

            for action in claimed {
                let permit = match semaphore.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let executor = self.executor.clone();
                let sinks = self.sinks_by_kind.clone();
                let dispatcher_id = self.id.clone();
                let lease = self.config.lease_duration;

                let handle = tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(e) =
                        handle_action(executor, sinks, dispatcher_id, lease, action).await
                    {
                        error!(error = %e, "action handling failed");
                    }
                });
                handles.push(handle);
            }
        }
    }

    /// Computes the action kinds for which the responsible sink is currently
    /// healthy in persisted storage.
    async fn compute_healthy_kinds(&self) -> Result<Vec<String>, DispatcherError> {
        let unhealthy_keys = self.executor.storage().unhealthy_sink_keys().await?;
        let mut kinds = Vec::new();
        for (kind, sink) in &self.sinks_by_kind {
            if !unhealthy_keys.iter().any(|k| k == sink.sink_key()) {
                kinds.push(kind.to_string());
            }
        }
        Ok(kinds)
    }
}

#[instrument(skip(executor, sinks, action), fields(
    action_id = %action.action_id,
    workflow_id = %action.workflow_id,
    kind = %action.kind,
    attempt = action.attempt,
    probe_attempt = action.probe_attempt,
))]
async fn handle_action<R: Reducer>(
    executor: Arc<Executor<R>>,
    sinks: HashMap<&'static str, Arc<dyn Sink>>,
    dispatcher_id: DispatcherId,
    lease_duration: ChronoDuration,
    action: ClaimedAction,
) -> Result<(), DispatcherError> {
    let storage = executor.storage().clone();

    let Some(sink) = sinks.get(action.kind.as_str()) else {
        error!(kind = %action.kind, "no sink registered for action kind");
        let err = format!("no sink for kind '{}'", action.kind);
        storage
            .record_permanent_failure(&action.action_id, &dispatcher_id, &err)
            .await?;
        return Ok(());
    };
    let sink = sink.clone();

    // Step 1: Probe path. Run only if at least one prior execute attempt happened.
    if action.attempt > 0 {
        match sink.find_existing(&action).await {
            Ok(Some(existing)) => {
                debug!(
                    external_ref = ?existing.external_ref,
                    "existence probe found prior success"
                );
                return finalize_success(
                    &executor,
                    &dispatcher_id,
                    &action,
                    existing.outcome_event,
                    existing.external_ref,
                )
                .await;
            }
            Ok(None) => {
                // Probe definitively says: not yet done. Proceed to execute.
            }
            Err(e) => {
                // Probe could not determine state. MUST NOT execute.
                warn!(error = %e, "probe failed, recording probe failure");
                let scheduled = storage
                    .record_probe_failure(&action.action_id, &dispatcher_id, &e.to_string())
                    .await?;
                if !scheduled {
                    error!("probe attempts exhausted; action moved to failed_probe_exhausted");
                }
                return Ok(());
            }
        }
    }

    // Step 2: Record attempt start in audit log.
    let next_attempt = action.attempt + 1;
    storage
        .record_attempt_start(&action.action_id, next_attempt)
        .await?;

    // Step 3: Spawn lease renewer.
    let renewer_handle = spawn_lease_renewer(
        storage.clone(),
        action.action_id.clone(),
        dispatcher_id.clone(),
        lease_duration,
    );

    // Step 4: Execute.
    let outcome = sink.execute(&action).await;

    // Step 5: Stop renewer before finalizing.
    renewer_handle.abort();

    match outcome {
        Ok(AttemptOutcome::Succeeded {
            external_ref,
            outcome_event,
        }) => {
            finalize_success(&executor, &dispatcher_id, &action, outcome_event, external_ref).await
        }
        Ok(AttemptOutcome::TransientFail { error }) => {
            let scheduled = storage
                .record_transient_failure(&action.action_id, &dispatcher_id, &error)
                .await?;
            if scheduled {
                debug!(error, "scheduled retry");
            } else {
                warn!(error, "retry budget exhausted, action failed permanently");
            }
            Ok(())
        }
        Ok(AttemptOutcome::PermanentFail { error }) => {
            warn!(error, "permanent failure recorded");
            storage
                .record_permanent_failure(&action.action_id, &dispatcher_id, &error)
                .await?;
            Ok(())
        }
        Ok(AttemptOutcome::SinkUnhealthy { reason, detail }) => {
            warn!(?reason, %detail, "sink unhealthy; action returns to pending");
            storage
                .mark_sink_unhealthy(sink.sink_key(), reason, &detail)
                .await?;
            // Roll the action back: clear lease, reset to pending, no attempt increment.
            // Note: record_attempt_start added an audit row for `next_attempt`. That's
            // fine - it's a started-but-not-finished audit record indicating an aborted
            // attempt. The next claim won't insert a duplicate due to ON CONFLICT DO NOTHING.
            storage
                .return_to_pending(
                    &action.action_id,
                    &dispatcher_id,
                    RETURN_TO_PENDING_DELAY,
                    &format!("sink unhealthy: {}: {}", reason.as_str(), detail),
                )
                .await?;
            Ok(())
        }
        Err(e) => {
            warn!(error = %e, "sink errored, treating as transient");
            let err_str = e.to_string();
            storage
                .record_transient_failure(&action.action_id, &dispatcher_id, &err_str)
                .await?;
            Ok(())
        }
    }
}

async fn finalize_success<R: Reducer>(
    executor: &Executor<R>,
    dispatcher_id: &DispatcherId,
    action: &ClaimedAction,
    outcome_event: crate::event::EventCommand,
    external_ref: Option<String>,
) -> Result<(), DispatcherError> {
    // Write outcome event first - the durable record that the side effect happened.
    let advance_outcome = executor.advance(outcome_event).await?;
    // Mark the outbox row succeeded.
    executor
        .storage()
        .finalize_succeeded(
            &action.action_id,
            dispatcher_id,
            external_ref,
            Some(advance_outcome.event_id),
        )
        .await?;
    Ok(())
}

fn spawn_lease_renewer(
    storage: crate::storage::Storage,
    action_id: crate::ids::ActionId,
    dispatcher_id: DispatcherId,
    lease_duration: ChronoDuration,
) -> JoinHandle<()> {
    let renew_interval =
        Duration::from_secs(((lease_duration.num_seconds() / 3).max(10)) as u64);
    tokio::spawn(async move {
        loop {
            sleep(renew_interval).await;
            match storage
                .renew_lease(&action_id, &dispatcher_id, lease_duration)
                .await
            {
                Ok(()) => debug!(action_id = %action_id, "lease renewed"),
                Err(e) => {
                    warn!(
                        action_id = %action_id, error = %e,
                        "lease renewal failed - abandoning"
                    );
                    return;
                }
            }
        }
    })
}

async fn health_check_loop<R: Reducer>(
    executor: Arc<Executor<R>>,
    sinks_by_key: HashMap<String, Arc<dyn Sink>>,
    extractors: Vec<Arc<dyn HintExtractor>>,
    interval: Duration,
    shutdown: Arc<Notify>,
) {
    loop {
        tokio::select! {
            _ = shutdown.notified() => {
                debug!("health check loop stopping");
                return;
            }
            _ = sleep(interval) => {}
        }

        let unhealthy = match executor.storage().list_unhealthy_sinks().await {
            Ok(list) => list,
            Err(e) => {
                warn!(error = %e, "list_unhealthy_sinks failed");
                continue;
            }
        };

        for record in unhealthy {
            let Some(sink) = sinks_by_key.get(&record.sink_key) else {
                continue;
            };

            // Build a scope from active kinds for this sink.
            let active_kinds: Vec<&str> = sink.handles().iter().copied().collect();
            let scope = match executor
                .storage()
                .build_health_scope(&active_kinds, &extractors, MAX_HEALTH_PROBE_ENDPOINTS)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    warn!(error = %e, sink = %record.sink_key, "scope build failed");
                    continue;
                }
            };

            match sink.check_health(scope).await {
                SinkHealthState::Healthy => {
                    info!(sink = %record.sink_key, "sink recovered");
                    if let Err(e) = executor.storage().mark_sink_healthy(&record.sink_key).await {
                        warn!(error = %e, "mark_sink_healthy failed");
                    }
                }
                SinkHealthState::Unhealthy {
                    reason,
                    detail,
                    ..
                } => {
                    debug!(sink = %record.sink_key, ?reason, %detail, "still unhealthy");
                    if let Err(e) = executor
                        .storage()
                        .mark_sink_unhealthy(&record.sink_key, reason, &detail)
                        .await
                    {
                        warn!(error = %e, "mark_sink_unhealthy failed");
                    }
                }
                SinkHealthState::Indeterminate { detail } => {
                    debug!(sink = %record.sink_key, %detail, "health indeterminate, no change");
                }
            }
        }
    }
}

/// Force a health re-check for a specific sink. Used by operator tools.
pub async fn force_health_recheck<R: Reducer>(
    executor: &Executor<R>,
    sinks_by_key: &HashMap<String, Arc<dyn Sink>>,
    extractors: &[Arc<dyn HintExtractor>],
    sink_key: &str,
) -> Result<SinkHealthState, DispatcherError> {
    let Some(sink) = sinks_by_key.get(sink_key) else {
        return Err(DispatcherError::Internal(format!(
            "no sink registered with key '{}'",
            sink_key
        )));
    };
    let active_kinds: Vec<&str> = sink.handles().iter().copied().collect();
    let scope = executor
        .storage()
        .build_health_scope(&active_kinds, extractors, MAX_HEALTH_PROBE_ENDPOINTS)
        .await?;
    let state = sink.check_health(scope).await;

    match &state {
        SinkHealthState::Healthy => {
            executor.storage().mark_sink_healthy(sink_key).await?;
        }
        SinkHealthState::Unhealthy { reason, detail, .. } => {
            executor
                .storage()
                .mark_sink_unhealthy(sink_key, *reason, detail)
                .await?;
        }
        SinkHealthState::Indeterminate { .. } => {}
    }
    Ok(state)
}
