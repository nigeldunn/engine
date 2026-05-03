//! Shared helper for building an authenticated `octocrab::Octocrab` client.
//!
//! Centralized so every GitHub action goes through the same auth + cache
//! path. The token comes from `GithubAuth::installation_token`, which
//! refreshes 60s before expiry.

use crate::auth::{GithubAuth, GithubAuthError};

/// Build an `Octocrab` authenticated with the App's installation token.
/// Cheap to call repeatedly — the token cache lives on `GithubAuth`.
pub async fn installation_client(
    auth: &GithubAuth,
) -> Result<octocrab::Octocrab, GithubAuthError> {
    let token = auth.installation_token().await?;
    octocrab::Octocrab::builder()
        .personal_token(token)
        .build()
        .map_err(|e| GithubAuthError::TokenFetch(format!("octocrab build failed: {}", e)))
}

/// Build an `Octocrab` authenticated with an App-level JWT (no installation
/// scope). Used for endpoints like `GET /app` that the App identifies itself
/// to, not endpoints that act on behalf of an installation.
pub fn app_client(auth: &GithubAuth) -> Result<octocrab::Octocrab, GithubAuthError> {
    let jwt = auth.app_jwt()?;
    octocrab::Octocrab::builder()
        .personal_token(jwt)
        .build()
        .map_err(|e| GithubAuthError::TokenFetch(format!("octocrab build failed: {}", e)))
}
