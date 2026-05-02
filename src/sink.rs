//! Sinks are how the dispatcher reaches the outside world. Each sink
//! handles one or more action kinds.
//!
//! Three methods that matter:
//!   - `execute`: do the side effect.
//!   - `find_existing`: probe for prior partial success (idempotency containment).
//!   - `check_health`: report whether the sink can currently reach its targets.
//!
//! Sinks must be pure adapters to external systems; they MUST NOT touch
//! storage directly. The dispatcher supplies any context they need (via
//! `SinkHealthScope` for health checks, via `ClaimedAction` for execute/probe).

use async_trait::async_trait;

use crate::action::{AttemptOutcome, ClaimedAction};
use crate::error::DispatcherError;
use crate::health::{SinkHealthScope, SinkHealthState};

#[async_trait]
pub trait Sink: Send + Sync + 'static {
    /// The action kinds this sink handles.
    fn handles(&self) -> &[&'static str];

    /// Stable identifier for this sink instance. Used as the primary key in
    /// the persisted `sink_health` table. Examples: "github",
    /// "github:installation-12345", "jira:acme-corp".
    fn sink_key(&self) -> &str;

    /// Probe the external system for prior partial success. Contract:
    ///
    /// - `Ok(Some(result))`: prior side effect found; dispatcher finalizes.
    /// - `Ok(None)`: definitively did not happen; dispatcher proceeds to execute.
    /// - `Err(...)`: probe could not determine state; dispatcher MUST NOT execute,
    ///   records a probe failure (incrementing `probe_attempt`), and will retry later.
    ///
    /// Default: no probing. Override for sinks where partial success is possible.
    async fn find_existing(
        &self,
        _action: &ClaimedAction,
    ) -> Result<Option<ExistingResult>, DispatcherError> {
        Ok(None)
    }

    /// Probe the sink's health. Called by the dispatcher's health-check loop
    /// while the sink is unhealthy, or on operator-triggered force-recheck.
    ///
    /// The `scope` parameter provides queue-derived context (active action
    /// kinds and endpoint hints) so the sink can probe relevant endpoints
    /// without needing storage access.
    ///
    /// Default: always healthy.
    async fn check_health(&self, _scope: SinkHealthScope) -> SinkHealthState {
        SinkHealthState::Healthy
    }

    /// Execute the side effect.
    async fn execute(&self, action: &ClaimedAction) -> Result<AttemptOutcome, DispatcherError>;
}

/// Returned from `find_existing` when a prior attempt already succeeded
/// on the external system.
#[derive(Clone, Debug)]
pub struct ExistingResult {
    pub external_ref: Option<String>,
    pub outcome_event: crate::event::EventCommand,
}
