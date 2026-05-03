//! GitHub App authentication: JWT generation and installation-token caching.
//!
//! GitHub App auth is a two-step flow:
//! 1. Sign a short-lived (≤10 min) JWT with the App's RSA private key.
//! 2. Exchange the JWT for an installation access token (typically valid 1h)
//!    by POST `/app/installations/{id}/access_tokens`.
//!
//! The installation token is what's used as a Bearer for repo-level API
//! calls. Tokens are cached and refreshed when within
//! `REFRESH_THRESHOLD_SECS` of expiry.

use chrono::{DateTime, Utc};
use jsonwebtoken::EncodingKey;
use octocrab::models::{AppId, InstallationToken};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

const REFRESH_THRESHOLD_SECS: i64 = 60;
const DEFAULT_TOKEN_LIFETIME_SECS: i64 = 3600;

#[derive(Debug, Error)]
pub enum GithubAuthError {
    #[error("invalid private key PEM: {0}")]
    InvalidPem(String),
    #[error("JWT creation failed: {0}")]
    JwtCreation(String),
    #[error("installation token fetch failed: {0}")]
    TokenFetch(String),
}

/// Cached GitHub App credentials with on-demand installation-token refresh.
pub struct GithubAuth {
    app_id: u64,
    encoding_key: EncodingKey,
    installation_id: u64,
    cache: Mutex<Option<CachedToken>>,
}

#[derive(Clone)]
struct CachedToken {
    token: String,
    expires_at: DateTime<Utc>,
}

impl GithubAuth {
    /// Construct a new auth helper. Validates the PEM at construction time.
    pub fn new(
        app_id: u64,
        private_key_pem: &str,
        installation_id: u64,
    ) -> Result<Self, GithubAuthError> {
        if private_key_pem.trim().is_empty() {
            return Err(GithubAuthError::InvalidPem("empty PEM".into()));
        }
        let encoding_key = EncodingKey::from_rsa_pem(private_key_pem.as_bytes())
            .map_err(|e| GithubAuthError::InvalidPem(e.to_string()))?;
        Ok(Self {
            app_id,
            encoding_key,
            installation_id,
            cache: Mutex::new(None),
        })
    }

    pub fn app_id(&self) -> u64 {
        self.app_id
    }

    pub fn installation_id(&self) -> u64 {
        self.installation_id
    }

    /// Generate a fresh App-level JWT. Cheap; signs with the cached key.
    pub fn app_jwt(&self) -> Result<String, GithubAuthError> {
        octocrab::auth::create_jwt(AppId(self.app_id), &self.encoding_key)
            .map_err(|e| GithubAuthError::JwtCreation(e.to_string()))
    }

    /// Return a valid installation token. Refreshes if the cache is empty
    /// or the cached token is within `REFRESH_THRESHOLD_SECS` of expiry.
    pub async fn installation_token(&self) -> Result<String, GithubAuthError> {
        let mut cache = self.cache.lock().await;
        let now = Utc::now();
        if let Some(t) = cache.as_ref() {
            if t.expires_at - now > chrono::Duration::seconds(REFRESH_THRESHOLD_SECS) {
                return Ok(t.token.clone());
            }
        }
        let fresh = self.fetch_installation_token().await?;
        let token = fresh.token.clone();
        *cache = Some(fresh);
        Ok(token)
    }

    async fn fetch_installation_token(&self) -> Result<CachedToken, GithubAuthError> {
        let jwt = self.app_jwt()?;
        let octocrab = octocrab::Octocrab::builder()
            .personal_token(jwt)
            .build()
            .map_err(|e| GithubAuthError::TokenFetch(e.to_string()))?;
        let url = format!("/app/installations/{}/access_tokens", self.installation_id);
        let resp: InstallationToken = octocrab
            .post(url, None::<&()>)
            .await
            .map_err(|e| GithubAuthError::TokenFetch(e.to_string()))?;

        let expires_at = resp
            .expires_at
            .as_deref()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|| Utc::now() + chrono::Duration::seconds(DEFAULT_TOKEN_LIFETIME_SECS));

        Ok(CachedToken {
            token: resp.token,
            expires_at,
        })
    }
}

/// Implementing Debug manually since EncodingKey doesn't impl Debug.
impl std::fmt::Debug for GithubAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GithubAuth")
            .field("app_id", &self.app_id)
            .field("installation_id", &self.installation_id)
            .field("cache", &"<redacted>")
            .finish()
    }
}

/// Convenience: a thread-safe handle to GithubAuth shared across tasks.
pub type SharedAuth = Arc<GithubAuth>;
