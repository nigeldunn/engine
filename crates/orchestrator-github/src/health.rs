//! GitHub sink health probe. M3 scope: global App-auth check via `GET /app`.
//!
//! Per the classification table in PLAN.md:
//! - 200             → `Healthy`
//! - 401             → `Unhealthy { AuthenticationFailed }`
//! - 403             → `Unhealthy { PermissionDenied }`
//! - other 4xx       → `Unhealthy { ConfigurationInvalid }`
//! - 5xx / network   → `Indeterminate`

use orchestrator_core::{SinkHealthScope, SinkHealthState, SinkUnhealthyReason};
use tracing::{debug, warn};

use crate::auth::GithubAuth;

/// Probe GitHub App auth. M3: ignores `scope.endpoint_hints` because no
/// action kinds are registered; per-repo probes land in M4+.
pub async fn check_health(auth: &GithubAuth, _scope: SinkHealthScope) -> SinkHealthState {
    let jwt = match auth.app_jwt() {
        Ok(j) => j,
        Err(e) => {
            warn!(error = %e, "github sink: jwt creation failed");
            return SinkHealthState::Unhealthy {
                reason: SinkUnhealthyReason::ConfigurationInvalid,
                detail: format!("jwt creation failed: {}", e),
                retry_after: None,
            };
        }
    };

    let octocrab = match octocrab::Octocrab::builder().personal_token(jwt).build() {
        Ok(o) => o,
        Err(e) => {
            return SinkHealthState::Indeterminate {
                detail: format!("octocrab build failed: {}", e),
            };
        }
    };

    // GET /app: returns the authenticated App's metadata. 200 means JWT is valid.
    let result: Result<serde_json::Value, _> = octocrab.get("/app", None::<&()>).await;
    match result {
        Ok(_) => {
            debug!("github sink: GET /app ok, healthy");
            SinkHealthState::Healthy
        }
        Err(e) => {
            warn!(error = %e, "github sink: GET /app failed");
            classify_error(&e)
        }
    }
}

fn classify_error(err: &octocrab::Error) -> SinkHealthState {
    match err {
        octocrab::Error::GitHub { source, .. } => {
            let code = source.status_code.as_u16();
            match code {
                401 => SinkHealthState::Unhealthy {
                    reason: SinkUnhealthyReason::AuthenticationFailed,
                    detail: source.message.clone(),
                    retry_after: None,
                },
                403 => SinkHealthState::Unhealthy {
                    reason: SinkUnhealthyReason::PermissionDenied,
                    detail: source.message.clone(),
                    retry_after: None,
                },
                400..=499 => SinkHealthState::Unhealthy {
                    reason: SinkUnhealthyReason::ConfigurationInvalid,
                    detail: format!("HTTP {}: {}", code, source.message),
                    retry_after: None,
                },
                _ => SinkHealthState::Indeterminate {
                    detail: format!("HTTP {}: {}", code, source.message),
                },
            }
        }
        other => SinkHealthState::Indeterminate {
            detail: format!("transport error: {}", other),
        },
    }
}
