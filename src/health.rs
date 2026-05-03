//! Sink health: persisted, scoped, with explicit indeterminate state.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Reported by `Sink::check_health`. The dispatcher persists `Healthy` and
/// `Unhealthy` to the `sink_health` table; `Indeterminate` leaves the
/// persisted state unchanged.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SinkHealthState {
    Healthy,
    Unhealthy {
        reason: SinkUnhealthyReason,
        detail: String,
        retry_after: Option<Duration>,
    },
    /// Could not determine health (e.g., all probe calls had transient errors).
    /// The dispatcher leaves persisted state unchanged.
    Indeterminate { detail: String },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SinkUnhealthyReason {
    AuthenticationFailed,
    PermissionDenied,
    ConfigurationInvalid,
    ExternalSystemDown,
}

impl SinkUnhealthyReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AuthenticationFailed => "authentication_failed",
            Self::PermissionDenied => "permission_denied",
            Self::ConfigurationInvalid => "configuration_invalid",
            Self::ExternalSystemDown => "external_system_down",
        }
    }

    #[allow(clippy::should_implement_trait)] // intentional: returns Option, per CLAUDE.md convention
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "authentication_failed" => Some(Self::AuthenticationFailed),
            "permission_denied" => Some(Self::PermissionDenied),
            "configuration_invalid" => Some(Self::ConfigurationInvalid),
            "external_system_down" => Some(Self::ExternalSystemDown),
            _ => None,
        }
    }
}

/// Scope passed to `Sink::check_health` so the sink can probe relevant
/// endpoints without needing storage access.
#[derive(Clone, Debug, Default)]
pub struct SinkHealthScope {
    /// Action kinds this sink handles that have queued work.
    pub active_kinds: Vec<String>,
    /// Endpoint hints derived from queued action payloads. May be empty if
    /// no actions are queued, in which case the sink should return Healthy
    /// after passing whatever global checks it has.
    pub endpoint_hints: Vec<EndpointHint>,
}

/// Hints about external endpoints relevant to a sink, derived from queued
/// action payloads. Extensible for OSS sinks via the `Custom` variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EndpointHint {
    GithubRepo { owner: String, name: String },
    JiraProject { key: String },
    LinearTeam { id: String },
    /// Catch-all for sinks not in core. `sink_key` identifies which sink
    /// produced this hint; `value` is sink-defined.
    Custom {
        sink_key: String,
        kind: String,
        value: serde_json::Value,
    },
}

/// Persisted record of a sink's health state.
#[derive(Clone, Debug)]
pub struct SinkHealthRecord {
    pub sink_key: String,
    pub state: PersistedHealthState,
    pub reason: Option<SinkUnhealthyReason>,
    pub detail: Option<String>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_check_at: chrono::DateTime<chrono::Utc>,
    pub next_check_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistedHealthState {
    Healthy,
    Unhealthy,
}

impl PersistedHealthState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Unhealthy => "unhealthy",
        }
    }
    #[allow(clippy::should_implement_trait)] // intentional: returns Option, per CLAUDE.md convention
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "healthy" => Some(Self::Healthy),
            "unhealthy" => Some(Self::Unhealthy),
            _ => None,
        }
    }
}

/// Implemented per sink type to extract endpoint hints from queued action
/// payloads. Registered with the dispatcher at startup.
pub trait HintExtractor: Send + Sync + 'static {
    /// Inspect an action and produce a hint, if applicable.
    fn extract(
        &self,
        action_kind: &str,
        payload: &serde_json::Value,
    ) -> Option<EndpointHint>;
}
