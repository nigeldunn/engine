//! Boot the engine: open `Storage`, build `Executor` + `Dispatcher`,
//! register both production sinks (`GithubSink`, `AgentRunnerSink`),
//! spawn the dispatcher loop and the webhook HTTP server. Returns
//! handles the caller uses to drive graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use orchestrator_agent_runner::{AgentRunnerSink, HttpAgentClient};
use orchestrator_coding_workflow::WorkflowReducer;
use orchestrator_core::{
    Dispatcher, DispatcherConfig as CoreDispatcherConfig, DispatcherError, Executor,
    ExecutorError, Storage,
};
use orchestrator_github::{GithubAuth, GithubAuthError, GithubHintExtractor, GithubSink};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{info, instrument};

use crate::config::{ConfigError, LoadedConfig};
use crate::server::{self, ServerError};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("config error: {0}")]
    Config(#[from] ConfigError),

    #[error("storage open failed: {0}")]
    Storage(#[from] ExecutorError),

    #[error("github auth init failed: {0}")]
    GithubAuth(#[from] GithubAuthError),

    /// HTTP server failed to come up — currently only surfaced when
    /// the webhook listener fails to bind. Spawning the server task
    /// before bind succeeds would make this failure invisible.
    #[error("server startup failed: {0}")]
    Server(#[from] ServerError),
}

/// Outcome of a graceful shutdown attempt across all subsystems. The
/// "worst" per-subsystem outcome wins so the binary's exit code
/// surfaces the most severe failure mode. Severity order:
/// `Drained < DrainErrored < TimedOut < Panicked`.
#[derive(Debug)]
pub enum ShutdownOutcome {
    Drained,
    DrainErrored,
    TimedOut,
    Panicked,
}

/// Per-subsystem drain result, used internally before merging into the
/// public `ShutdownOutcome`.
#[derive(Debug, Clone, Copy)]
enum SubsystemDrain {
    Drained,
    Errored,
    TimedOut,
    Panicked,
}

/// A spawned subsystem with its own shutdown notify and join handle.
/// Generic over the error type so each subsystem keeps its typed
/// boundary error (DispatcherError, ServerError, ...). Adding a new
/// subsystem to `Runtime` is one field + one spawn into a handle, with
/// no per-subsystem drain helper.
///
/// `join` is wrapped in `Option` so `drain` can `take()` it before the
/// `Drop` guard runs — the guard exists to abort stranded tasks if a
/// partially-built `Runtime` is dropped (e.g., a fallible boot step
/// fails after the dispatcher has been spawned), so it must not also
/// fire on already-drained handles.
struct SubsystemHandle<E> {
    name: &'static str,
    shutdown: Arc<Notify>,
    join: Option<JoinHandle<Result<(), E>>>,
}

impl<E: std::fmt::Display + Send + 'static> SubsystemHandle<E> {
    fn new(
        name: &'static str,
        shutdown: Arc<Notify>,
        join: JoinHandle<Result<(), E>>,
    ) -> Self {
        Self { name, shutdown, join: Some(join) }
    }

    /// Fire the shutdown notify. Does NOT wait for the task to drain —
    /// the caller does that via [`drain`]. Splitting the two lets
    /// `Runtime::shutdown` signal every subsystem first, then drain
    /// them in parallel.
    fn signal_shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Wait up to `grace` for the spawned task to finish. On timeout
    /// the task is aborted so the binary can still exit. Errors and
    /// panics are logged with the subsystem name attached. Takes the
    /// JoinHandle out of `self` so the `Drop` abort-guard becomes a
    /// no-op when this handle is later dropped.
    async fn drain(mut self, grace: Duration) -> SubsystemDrain {
        let join = self
            .join
            .take()
            .expect("drain consumes the handle exactly once");
        let abort_handle = join.abort_handle();
        match tokio::time::timeout(grace, join).await {
            Ok(Ok(Ok(()))) => SubsystemDrain::Drained,
            Ok(Ok(Err(e))) => {
                tracing::error!(name = self.name, error = %e, "subsystem drained with error");
                SubsystemDrain::Errored
            }
            Ok(Err(join_err)) => {
                tracing::error!(name = self.name, error = %join_err, "subsystem task panicked");
                SubsystemDrain::Panicked
            }
            Err(_elapsed) => {
                tracing::error!(
                    name = self.name,
                    grace_ms = grace.as_millis(),
                    "subsystem did not drain within grace period; aborting"
                );
                abort_handle.abort();
                SubsystemDrain::TimedOut
            }
        }
    }
}

impl<E> Drop for SubsystemHandle<E> {
    /// Defense in depth against partial-boot leaks. If the handle is
    /// dropped without going through `drain` (e.g., a later fallible
    /// step in `Runtime::boot` errors out after this subsystem has
    /// been spawned), abort the task so it does not keep running with
    /// no observer. `drain` calls `take()` first, so on the normal
    /// path this is a no-op.
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            tracing::warn!(
                name = self.name,
                "subsystem dropped without drain; aborting"
            );
            join.abort();
        }
    }
}

pub struct Runtime {
    dispatcher: SubsystemHandle<DispatcherError>,
    webhook_server: SubsystemHandle<ServerError>,
    ingest_server: SubsystemHandle<ServerError>,
    grace_period: Duration,
}

impl Runtime {
    /// Boot the engine end-to-end: storage + sinks + dispatcher + webhook
    /// HTTP server. Both subsystems are running by the time this returns.
    /// The `LoadedConfig` carries the validated base directory so secret
    /// and sqlite paths resolve consistently across callers.
    #[instrument(skip(cfg), fields(install_id = cfg.github.install_id))]
    pub async fn boot(cfg: &LoadedConfig) -> Result<Self, RuntimeError> {
        let base_dir = cfg.base_dir();
        // Storage. Relative sqlite_path resolves against the config dir
        // so a relative `data/orch.sqlite` lands beside the config file.
        let sqlite_path = cfg.storage.resolved_sqlite_path(base_dir);
        let database_url = format!("sqlite:{}", sqlite_path.display());
        info!(%database_url, "opening storage");
        let storage = Storage::open(&database_url).await?;

        // Executor wraps storage + the workflow reducer.
        let executor = Arc::new(Executor::new(storage, WorkflowReducer));

        // Dispatcher config: poll/health/unhealthy delays come from TOML;
        // batch_size, lease, and concurrency stay at the core defaults
        // until we have telemetry to tune them per-deployment.
        let dispatcher_config = CoreDispatcherConfig {
            poll_interval: Duration::from_millis(cfg.dispatcher.poll_interval_ms),
            health_check_interval: Duration::from_millis(
                cfg.dispatcher.health_check_interval_ms,
            ),
            sink_unhealthy_retry_delay: Duration::from_millis(
                cfg.dispatcher.unhealthy_retry_interval_ms,
            ),
            ..CoreDispatcherConfig::default()
        };
        // Resolve all fallible inputs (PEM, agent token, webhook secret)
        // and bind every listener BEFORE spawning anything. A failure
        // partway through boot must not leave a spawned subsystem
        // running with no observer (Codex stop-gate round-14).
        let pem = cfg.github.private_key.resolve("github.private_key", base_dir)?;
        let auth = GithubAuth::new(cfg.github.app_id, &pem, cfg.github.install_id)?;
        let agent_token = match &cfg.agent_runner.bearer_token {
            Some(s) => Some(s.resolve("agent_runner.bearer_token", base_dir)?),
            None => None,
        };
        let webhook_secret = cfg
            .github
            .webhook_secret
            .resolve("github.webhook_secret", base_dir)?;
        let webhook_listener =
            server::bind_webhook_listener(cfg.server.webhook.listen).await?;

        let ingest_bearer_token = match &cfg.server.ingest.bearer_token {
            Some(s) => Some(s.resolve("server.ingest.bearer_token", base_dir)?),
            None => None,
        };
        let ingest_listener =
            server::bind_ingest_listener(cfg.server.ingest.listen).await?;

        // From here on the boot is infallible — only spawns. Each
        // subsystem owns its own Notify so we don't repeat the
        // multi-waiter race fixed in dispatcher.rs (round-6).
        let mut dispatcher = Dispatcher::new(executor.clone(), dispatcher_config);
        dispatcher.register(GithubSink::new(auth));
        dispatcher.register_extractor(GithubHintExtractor);
        let agent_client =
            HttpAgentClient::new(cfg.agent_runner.base_url.clone(), agent_token);
        dispatcher.register(AgentRunnerSink::new(agent_client));

        let dispatcher_shutdown = dispatcher.shutdown_handle();
        let dispatcher = SubsystemHandle::new(
            "dispatcher",
            dispatcher_shutdown,
            tokio::spawn(dispatcher.run()),
        );

        let webhook_shutdown = Arc::new(Notify::new());
        let webhook_join = {
            let prefix = cfg.server.webhook.path_prefix.clone();
            let executor = executor.clone();
            let shutdown = webhook_shutdown.clone();
            let retry_budget =
                Duration::from_millis(cfg.server.webhook.lookup_retry_budget_ms);
            let retry_backoff =
                Duration::from_millis(cfg.server.webhook.lookup_retry_backoff_ms);
            tokio::spawn(async move {
                server::run_webhook(
                    webhook_listener,
                    prefix,
                    webhook_secret,
                    executor,
                    retry_budget,
                    retry_backoff,
                    shutdown,
                )
                .await
            })
        };
        let webhook_server =
            SubsystemHandle::new("webhook_server", webhook_shutdown, webhook_join);

        let ingest_shutdown = Arc::new(Notify::new());
        let ingest_join = {
            let executor = executor.clone();
            let shutdown = ingest_shutdown.clone();
            tokio::spawn(async move {
                server::run_ingest(ingest_listener, ingest_bearer_token, executor, shutdown)
                    .await
            })
        };
        let ingest_server =
            SubsystemHandle::new("ingest_server", ingest_shutdown, ingest_join);

        let grace_period = Duration::from_millis(cfg.dispatcher.shutdown_grace_period_ms);
        info!(grace_ms = cfg.dispatcher.shutdown_grace_period_ms, "runtime booted");

        Ok(Self {
            dispatcher,
            webhook_server,
            ingest_server,
            grace_period,
        })
    }

    /// Fire shutdown for every subsystem and wait for them to drain in
    /// parallel, bounded by the grace period. Drain hangs are surfaced
    /// as `TimedOut` and the binary exits non-zero so monitoring catches
    /// stuck deployments. Panics surface as `Panicked` (severity wins
    /// over TimedOut — a panic is a code defect that needs immediate
    /// attention, not a transient slow handler).
    pub async fn shutdown(self) -> ShutdownOutcome {
        self.dispatcher.signal_shutdown();
        self.webhook_server.signal_shutdown();
        self.ingest_server.signal_shutdown();
        let grace = self.grace_period;
        let (d, w, i) = tokio::join!(
            self.dispatcher.drain(grace),
            self.webhook_server.drain(grace),
            self.ingest_server.drain(grace),
        );
        merge_outcomes([d, w, i])
    }
}

/// Combine per-subsystem outcomes into a single shutdown outcome. The
/// most severe outcome wins so callers can map directly to a non-zero
/// exit code that surfaces the worst failure across all subsystems.
fn merge_outcomes<I: IntoIterator<Item = SubsystemDrain>>(outcomes: I) -> ShutdownOutcome {
    outcomes
        .into_iter()
        .map(per_subsystem_outcome)
        .max_by_key(severity_rank)
        .unwrap_or(ShutdownOutcome::Drained)
}

fn per_subsystem_outcome(d: SubsystemDrain) -> ShutdownOutcome {
    match d {
        SubsystemDrain::Drained => ShutdownOutcome::Drained,
        SubsystemDrain::Errored => ShutdownOutcome::DrainErrored,
        SubsystemDrain::TimedOut => ShutdownOutcome::TimedOut,
        SubsystemDrain::Panicked => ShutdownOutcome::Panicked,
    }
}

fn severity_rank(o: &ShutdownOutcome) -> u8 {
    match o {
        ShutdownOutcome::Drained => 0,
        ShutdownOutcome::DrainErrored => 1,
        ShutdownOutcome::TimedOut => 2,
        ShutdownOutcome::Panicked => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    /// Build a `SubsystemHandle` whose join is a synthetic future. The
    /// shutdown notify is never wired into the future so tests can
    /// directly observe the drain behavior without simulating a real
    /// subsystem loop.
    fn handle_for_test<E: std::fmt::Display + Send + 'static>(
        name: &'static str,
        join: JoinHandle<Result<(), E>>,
    ) -> SubsystemHandle<E> {
        SubsystemHandle::new(name, Arc::new(Notify::new()), join)
    }

    #[tokio::test]
    async fn drain_completes_within_grace_period() {
        let handle: SubsystemHandle<DispatcherError> = handle_for_test(
            "test",
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(())
            }),
        );
        let outcome = handle.drain(Duration::from_secs(5)).await;
        assert!(matches!(outcome, SubsystemDrain::Drained), "got: {outcome:?}");
    }

    #[tokio::test]
    async fn drain_exceeding_grace_period_aborts_and_reports_timeout() {
        let handle: SubsystemHandle<DispatcherError> = handle_for_test(
            "test",
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(())
            }),
        );
        let started = Instant::now();
        let outcome = handle.drain(Duration::from_millis(50)).await;
        let elapsed = started.elapsed();
        assert!(matches!(outcome, SubsystemDrain::TimedOut), "got: {outcome:?}");
        assert!(elapsed < Duration::from_secs(1), "shutdown took {elapsed:?}");
    }

    #[tokio::test]
    async fn dropping_handle_without_drain_aborts_task() {
        // Codex stop-gate round-14: a partial-boot failure must not
        // leave a stranded task running. The Drop guard aborts the
        // join handle when the SubsystemHandle is dropped without
        // having been drained.
        let join = tokio::spawn(async {
            // Long sleep so the task is still running when we drop.
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<(), DispatcherError>(())
        });
        let abort_handle = join.abort_handle();
        let handle: SubsystemHandle<DispatcherError> =
            handle_for_test("test", join);
        assert!(!abort_handle.is_finished(), "task should still be running");

        drop(handle);

        // The abort propagates on the next yield. A short sleep is
        // enough; we don't need to busy-loop.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            abort_handle.is_finished(),
            "Drop must abort the task; it is still running"
        );
    }

    #[tokio::test]
    async fn drained_handle_does_not_warn_on_drop() {
        // After drain, the join is `None`, so Drop's abort branch is
        // skipped and no warning fires.
        let handle: SubsystemHandle<DispatcherError> = handle_for_test(
            "test",
            tokio::spawn(async { Ok(()) }),
        );
        let _ = handle.drain(Duration::from_secs(1)).await;
        // Nothing to assert beyond "no panic / no double-abort"; the
        // is_some()/take() pattern is what guards this.
    }

    #[test]
    fn merge_outcomes_returns_drained_when_no_subsystems() {
        let r = merge_outcomes(std::iter::empty());
        assert!(matches!(r, ShutdownOutcome::Drained), "got: {r:?}");
    }

    #[test]
    fn merge_outcomes_picks_highest_severity() {
        // Severity ranking: Drained < Errored < TimedOut < Panicked.
        // Verify each pairwise upgrade.
        for (inputs, expected) in [
            (vec![SubsystemDrain::Drained, SubsystemDrain::Drained], 0u8),
            (vec![SubsystemDrain::Drained, SubsystemDrain::Errored], 1),
            (vec![SubsystemDrain::Errored, SubsystemDrain::TimedOut], 2),
            (vec![SubsystemDrain::TimedOut, SubsystemDrain::Panicked], 3),
            (
                vec![
                    SubsystemDrain::Drained,
                    SubsystemDrain::Errored,
                    SubsystemDrain::Panicked,
                    SubsystemDrain::TimedOut,
                ],
                3,
            ),
        ] {
            let outcome = merge_outcomes(inputs.iter().copied());
            assert_eq!(severity_rank(&outcome), expected, "inputs: {inputs:?}");
        }
    }
}
