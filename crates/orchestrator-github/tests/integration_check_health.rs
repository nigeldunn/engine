//! Integration test: `check_health` against real GitHub.
//!
//! Skipped unless `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY_PEM`, and
//! `GITHUB_INSTALLATION_ID` are all set in the environment. Run with:
//!
//! ```sh
//! GITHUB_APP_ID=… GITHUB_PRIVATE_KEY_PEM="$(cat key.pem)" \
//!   GITHUB_INSTALLATION_ID=… cargo test -p orchestrator-github -- --ignored
//! ```
//!
//! Marked `#[ignore]` so it doesn't run by default in CI/dev — opt-in only.

use orchestrator_core::{SinkHealthScope, SinkHealthState};
use orchestrator_github::{health, GithubAuth};

fn load_auth() -> Option<GithubAuth> {
    let app_id: u64 = std::env::var("GITHUB_APP_ID").ok()?.parse().ok()?;
    let pem = std::env::var("GITHUB_PRIVATE_KEY_PEM").ok()?;
    let inst: u64 = std::env::var("GITHUB_INSTALLATION_ID").ok()?.parse().ok()?;
    GithubAuth::new(app_id, &pem, inst).ok()
}

#[tokio::test]
#[ignore = "requires real GitHub App credentials in env"]
async fn check_health_returns_healthy_with_valid_creds() {
    let _ = tracing_subscriber::fmt::try_init();
    let auth = load_auth().expect("set GITHUB_APP_ID, GITHUB_PRIVATE_KEY_PEM, GITHUB_INSTALLATION_ID");
    let state = health::check_health(&auth, SinkHealthScope::default()).await;
    assert!(
        matches!(state, SinkHealthState::Healthy),
        "expected Healthy, got {:?}",
        state
    );
}
