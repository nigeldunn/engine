//! Config schema and loader for the orchestrator app binary.
//!
//! Layered via figment: TOML file → environment variables (prefix
//! `ORCH_`, double underscore = section separator). The CLI passes the
//! config path explicitly — there is no implicit search path (PLAN.md
//! M13: cwd-vs-/etc precedence is a footgun).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config load failed: {0}")]
    Load(Box<figment::Error>),

    #[error("ingest server bound to non-loopback address {addr} requires `[server.ingest].bearer_token`")]
    IngestNeedsAuth { addr: SocketAddr },

    #[error("secret `{field}` is empty")]
    SecretEmpty { field: &'static str },

    #[error("secret `{field}` could not be read from {path}: {source}")]
    SecretIo {
        field: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "`server.webhook.path_prefix` must be empty, \"/\", or start with \"/\"; got {value:?}"
    )]
    InvalidPathPrefix { value: String },

    #[error(
        "`dispatcher.shutdown_grace_period_ms` ({grace_ms}) must exceed \
         `server.webhook.lookup_retry_budget_ms` ({retry_ms}) so an in-flight \
         retry can drain before the grace timer fires"
    )]
    ShutdownGraceTooShort { grace_ms: u64, retry_ms: u64 },
}

impl From<figment::Error> for ConfigError {
    fn from(e: figment::Error) -> Self {
        Self::Load(Box::new(e))
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub storage: StorageConfig,
    pub github: GithubConfig,
    pub agent_runner: AgentRunnerConfig,
    pub server: ServerConfig,
    pub dispatcher: DispatcherConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub sqlite_path: PathBuf,
}

impl StorageConfig {
    /// Resolve `sqlite_path` against `base_dir` if it is relative.
    /// Same rationale as `Secret::resolve`: a relative path in TOML
    /// should refer to a sibling of the config file, not whatever
    /// directory the binary happened to be launched from.
    pub fn resolved_sqlite_path(&self, base_dir: &Path) -> PathBuf {
        if self.sqlite_path.is_absolute() {
            self.sqlite_path.clone()
        } else {
            base_dir.join(&self.sqlite_path)
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GithubConfig {
    pub app_id: u64,
    pub install_id: u64,
    pub private_key: Secret,
    pub webhook_secret: Secret,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRunnerConfig {
    pub base_url: String,
    #[serde(default)]
    pub bearer_token: Option<Secret>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub webhook: WebhookServerConfig,
    pub ingest: IngestServerConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookServerConfig {
    pub listen: SocketAddr,
    #[serde(default = "default_webhook_path_prefix")]
    pub path_prefix: String,
    /// Total time the handler will spend retrying a workflow lookup
    /// before giving up. Sized to absorb the open-then-merge race
    /// window between `open_pr.execute` (PR exists on GitHub) and
    /// `executor.advance` (PrOpened event recorded). Default 5000ms.
    /// MUST stay strictly below `dispatcher.shutdown_grace_period_ms`
    /// — see `Config::validate`.
    #[serde(default = "default_lookup_retry_budget_ms")]
    pub lookup_retry_budget_ms: u64,
    /// Backoff between lookup retries. Default 200ms.
    #[serde(default = "default_lookup_retry_backoff_ms")]
    pub lookup_retry_backoff_ms: u64,
}

fn default_webhook_path_prefix() -> String {
    "/webhook".into()
}

fn default_lookup_retry_budget_ms() -> u64 {
    5_000
}

fn default_lookup_retry_backoff_ms() -> u64 {
    200
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestServerConfig {
    /// Defaults to loopback per M13 design — operators must explicitly
    /// opt in to a network-reachable bind address AND provide
    /// `bearer_token`, enforced in `Config::validate`.
    #[serde(default = "default_ingest_listen")]
    pub listen: SocketAddr,
    #[serde(default)]
    pub bearer_token: Option<Secret>,
}

fn default_ingest_listen() -> SocketAddr {
    "127.0.0.1:8081".parse().expect("hardcoded loopback addr is valid")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatcherConfig {
    pub poll_interval_ms: u64,
    pub health_check_interval_ms: u64,
    pub unhealthy_retry_interval_ms: u64,
    /// Maximum time the runtime will wait for the dispatcher loop to
    /// drain after a shutdown signal before aborting the task. Bounds
    /// SIGTERM-to-exit so a stuck sink handler can't hold the binary
    /// open indefinitely (k8s sends SIGKILL after 30s by default).
    #[serde(default = "default_shutdown_grace_period_ms")]
    pub shutdown_grace_period_ms: u64,
}

fn default_shutdown_grace_period_ms() -> u64 {
    25_000
}

/// A secret value sourced from either an inline string (dev convenience)
/// or a file path (production: k8s secret mounts, /etc files). Exactly
/// one of `inline` / `path` must be set; the untagged-enum match enforces
/// that at deserialize time.
#[derive(Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Secret {
    Inline { inline: String },
    Path { path: PathBuf },
}

impl Secret {
    /// Materialize the secret value. For `Inline`, returns the literal
    /// string. For `Path`, reads the file. Relative paths resolve
    /// against `base_dir` (the directory containing the config file) —
    /// not the process cwd, which depends on where the binary was
    /// launched (systemd, docker, etc.). Absolute paths are used as-is.
    /// Whitespace is preserved (PEM files have meaningful trailing
    /// newlines). Empty contents are rejected.
    pub fn resolve(&self, field: &'static str, base_dir: &Path) -> Result<String, ConfigError> {
        let value = match self {
            Secret::Inline { inline } => inline.clone(),
            Secret::Path { path } => {
                let resolved = if path.is_absolute() {
                    path.clone()
                } else {
                    base_dir.join(path)
                };
                std::fs::read_to_string(&resolved).map_err(|e| ConfigError::SecretIo {
                    field,
                    path: resolved,
                    source: e,
                })?
            }
        };
        if value.is_empty() {
            return Err(ConfigError::SecretEmpty { field });
        }
        Ok(value)
    }
}

/// A `Config` paired with the directory used to resolve relative
/// secret/storage paths. `Config::load` returns this so the validated
/// `base_dir` travels with the config — eliminating the drift risk
/// where one caller validates against `/etc/orch/` and another later
/// resolves the same paths against the process cwd. Pass it to
/// `Runtime::boot` instead of two separate values.
#[derive(Debug)]
pub struct LoadedConfig {
    pub config: Config,
    base_dir: PathBuf,
}

impl LoadedConfig {
    /// Directory the config file lives in; relative paths in the
    /// config resolve against this.
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }
}

impl std::ops::Deref for LoadedConfig {
    type Target = Config;
    fn deref(&self) -> &Config {
        &self.config
    }
}

impl Config {
    /// Load the config from a TOML file with environment overrides.
    /// Env vars use the prefix `ORCH_` and `__` as section separator,
    /// e.g. `ORCH_STORAGE__SQLITE_PATH=/var/lib/orch/db.sqlite`.
    /// Returns a [`LoadedConfig`] that carries the directory used to
    /// resolve relative secret / sqlite paths.
    pub fn load(path: &Path) -> Result<LoadedConfig, ConfigError> {
        let cfg: Self = Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("ORCH_").split("__"))
            .extract()?;
        // Resolve relative secret paths against the config file's
        // directory, not the process cwd. Empty parent (bare-filename
        // config) falls back to "" which `Path::join` treats as cwd —
        // matching user intent for `orchestrator-app --config foo.toml`.
        let base_dir = path.parent().unwrap_or(Path::new("")).to_path_buf();
        cfg.validate(&base_dir)?;
        Ok(LoadedConfig {
            config: cfg,
            base_dir,
        })
    }

    fn validate(&self, base_dir: &Path) -> Result<(), ConfigError> {
        // Required secrets must materialize to non-empty strings. Eager
        // resolution at config-load surfaces typo'd paths and empty
        // files at startup rather than deep inside the dispatcher.
        self.github.private_key.resolve("github.private_key", base_dir)?;
        self.github
            .webhook_secret
            .resolve("github.webhook_secret", base_dir)?;

        // Optional secrets, when set, must also materialize to non-empty.
        // An empty value is never what an operator meant; treat it as a
        // configuration error.
        if let Some(s) = &self.agent_runner.bearer_token {
            s.resolve("agent_runner.bearer_token", base_dir)?;
        }
        let ingest_has_token = match &self.server.ingest.bearer_token {
            Some(s) => {
                s.resolve("server.ingest.bearer_token", base_dir)?;
                true
            }
            None => false,
        };
        if !is_loopback(&self.server.ingest.listen) && !ingest_has_token {
            return Err(ConfigError::IngestNeedsAuth {
                addr: self.server.ingest.listen,
            });
        }

        // axum's `Router::nest` panics on any of: missing leading slash,
        // trailing slash, path parameters (`:id` in axum ≤ 0.7,
        // `{id}` in axum ≥ 0.8), or wildcards (`*rest` / `{*rest}`).
        // Rather than chase the syntax flavor du jour, allow-list a
        // small safe character set in each segment. Anything special
        // (braces, colons, asterisks, query/fragment markers,
        // whitespace, double slashes) is rejected at config load so
        // the operator sees a typed error instead of a thread panic
        // deep inside the spawned server task.
        let prefix = &self.server.webhook.path_prefix;
        if !is_valid_path_prefix(prefix) {
            return Err(ConfigError::InvalidPathPrefix {
                value: prefix.clone(),
            });
        }

        // The shutdown grace must outlast an in-flight webhook lookup
        // retry; otherwise a normal in-handler retry held during
        // shutdown gets aborted as TimedOut and the operator sees a
        // false alarm. Strict inequality — equal values still race.
        let grace_ms = self.dispatcher.shutdown_grace_period_ms;
        let retry_ms = self.server.webhook.lookup_retry_budget_ms;
        if grace_ms <= retry_ms {
            return Err(ConfigError::ShutdownGraceTooShort { grace_ms, retry_ms });
        }
        Ok(())
    }
}

fn is_loopback(addr: &SocketAddr) -> bool {
    addr.ip().is_loopback()
}

/// True for a path prefix safe to hand to `axum::Router::nest`. Empty
/// and "/" are accepted (the caller skips nest in those cases). Any
/// other prefix must:
///   - start with `/`
///   - not end with `/`
///   - consist of one or more `/segment` parts where each segment is
///     non-empty and contains only ASCII alphanumerics, `-`, `_`, or `.`.
///
/// This is deliberately stricter than RFC 3986 path syntax: braces,
/// colons, asterisks, query / fragment markers, percent-encoding, and
/// whitespace are all rejected because none of them belong in a static
/// mount prefix and several of them trigger axum panics.
fn is_valid_path_prefix(prefix: &str) -> bool {
    if prefix.is_empty() || prefix == "/" {
        return true;
    }
    if !prefix.starts_with('/') || prefix.ends_with('/') {
        return false;
    }
    // First piece is the empty leading segment from the leading '/';
    // every subsequent segment must be non-empty + allowed chars.
    let mut parts = prefix.split('/');
    let leading = parts.next().expect("split always yields at least one piece");
    if !leading.is_empty() {
        return false;
    }
    for segment in parts {
        if segment.is_empty() {
            return false;
        }
        if !segment
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp_toml(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    fn full_config_toml() -> String {
        // Inline secrets keep tests self-contained — path variants are
        // covered by their own tests with tempfiles.
        r#"
[storage]
sqlite_path = "/tmp/orch.sqlite"

[github]
app_id = 12345
install_id = 67890
private_key = { inline = "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n" }
webhook_secret = { inline = "super-secret" }

[agent_runner]
base_url = "http://localhost:8080"

[server.webhook]
listen = "0.0.0.0:8080"

[server.ingest]
listen = "127.0.0.1:8081"

[dispatcher]
poll_interval_ms = 250
health_check_interval_ms = 30000
unhealthy_retry_interval_ms = 5000
"#
        .to_string()
    }

    #[test]
    fn loads_full_config_with_default_webhook_path_prefix() {
        let f = write_tmp_toml(&full_config_toml());
        let cfg = Config::load(f.path()).expect("must load");
        assert_eq!(cfg.storage.sqlite_path.to_str(), Some("/tmp/orch.sqlite"));
        assert_eq!(cfg.github.app_id, 12345);
        assert_eq!(cfg.server.webhook.path_prefix, "/webhook");
        assert!(cfg.agent_runner.bearer_token.is_none());
    }

    #[test]
    fn relative_sqlite_path_resolves_against_config_directory() {
        let dir = tempfile::tempdir().unwrap();
        let toml = full_config_toml().replace(
            r#"sqlite_path = "/tmp/orch.sqlite""#,
            r#"sqlite_path = "data/orch.sqlite""#,
        );
        let config_path = dir.path().join("orch.toml");
        std::fs::write(&config_path, toml).unwrap();

        let cfg = Config::load(&config_path).unwrap();
        let resolved = cfg.storage.resolved_sqlite_path(dir.path());
        assert_eq!(resolved, dir.path().join("data/orch.sqlite"));
    }

    #[test]
    fn invalid_path_prefix_is_rejected_at_validate() {
        // Allow-list approach (Codex stop-gate round-13): any character
        // outside [A-Za-z0-9._-] in a segment is rejected, plus
        // structural problems (no leading slash, trailing slash, empty
        // segments). Each row covers a different axum-panic-trigger or
        // operator footgun.
        for bad in [
            "webhook",          // missing leading /
            "/webhook/",        // trailing slash
            "/webhook/:id",     // axum ≤ 0.7 path param
            "/webhook/*rest",   // axum wildcard
            "/webhook/{id}",    // axum 0.8 path param
            "/webhook/{*x}",    // axum 0.8 catch-all
            "/web hook",        // whitespace
            "/webhook//foo",    // empty segment
            "/webhook?q=1",     // query marker
            "/webhook#frag",    // fragment marker
            "/webhook%20foo",   // percent-encoding
            "/webhook/(group)", // parens
        ] {
            let toml = full_config_toml().replace(
                "[server.webhook]\nlisten = \"0.0.0.0:8080\"",
                &format!(
                    "[server.webhook]\nlisten = \"0.0.0.0:8080\"\npath_prefix = \"{bad}\"",
                ),
            );
            let f = write_tmp_toml(&toml);
            let err = Config::load(f.path())
                .err()
                .unwrap_or_else(|| panic!("prefix {bad:?} must be rejected"));
            assert!(
                matches!(&err, ConfigError::InvalidPathPrefix { value } if value == bad),
                "prefix {bad:?} should reject as InvalidPathPrefix; got: {err:?}"
            );
        }
    }

    #[test]
    fn ingest_listen_defaults_to_loopback_when_omitted() {
        // Slice-4 prep: M13 design says ingest binds 127.0.0.1 by
        // default. Operators have to explicitly opt in to a network
        // address (which then triggers the bearer-token requirement).
        let toml = full_config_toml().replace(
            "[server.ingest]\nlisten = \"127.0.0.1:8081\"",
            "[server.ingest]",
        );
        let f = write_tmp_toml(&toml);
        let cfg = Config::load(f.path()).expect("must load with default listen");
        assert_eq!(
            cfg.server.ingest.listen.to_string(),
            "127.0.0.1:8081"
        );
        assert!(cfg.server.ingest.bearer_token.is_none());
    }

    #[test]
    fn valid_path_prefixes_are_accepted() {
        // Each of these is a sane operator choice that must NOT be
        // caught by the allow-list — keep the check from drifting
        // into over-rejection over time.
        for ok in [
            "",
            "/",
            "/webhook",
            "/api/v1/webhook",
            "/api/v1.0/webhook",
            "/orchestrator-webhook",
            "/orch_webhook",
        ] {
            let prefix_toml = if ok.is_empty() {
                String::new()
            } else {
                format!("\npath_prefix = \"{ok}\"")
            };
            let toml = full_config_toml().replace(
                "[server.webhook]\nlisten = \"0.0.0.0:8080\"",
                &format!("[server.webhook]\nlisten = \"0.0.0.0:8080\"{prefix_toml}"),
            );
            let f = write_tmp_toml(&toml);
            Config::load(f.path())
                .unwrap_or_else(|e| panic!("prefix {ok:?} should load; got: {e:?}"));
        }
    }

    #[test]
    fn absolute_sqlite_path_is_unchanged_by_resolve() {
        let cfg = StorageConfig {
            sqlite_path: PathBuf::from("/var/lib/orch/db.sqlite"),
        };
        assert_eq!(
            cfg.resolved_sqlite_path(Path::new("/anywhere/else")),
            PathBuf::from("/var/lib/orch/db.sqlite"),
        );
    }

    #[test]
    fn secret_inline_resolves_directly() {
        let s = Secret::Inline { inline: "hello".into() };
        assert_eq!(s.resolve("test", Path::new("/tmp")).unwrap(), "hello");
    }

    #[test]
    fn secret_path_resolves_file_contents() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"file-secret\n").unwrap();
        f.flush().unwrap();
        let s = Secret::Path { path: f.path().to_path_buf() };
        assert_eq!(s.resolve("test", Path::new("/")).unwrap(), "file-secret\n");
    }

    #[test]
    fn secret_relative_path_resolves_against_config_directory() {
        // Place the config and the secret file in the same tempdir, then
        // reference the secret via a bare relative filename. Resolution
        // must succeed regardless of the test process's cwd.
        let dir = tempfile::tempdir().unwrap();
        let secret_file = dir.path().join("github-app.pem");
        std::fs::write(&secret_file, b"-----BEGIN FAKE PEM-----\n").unwrap();

        let toml = full_config_toml().replace(
            r#"private_key = { inline = "-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----\n" }"#,
            r#"private_key = { path = "github-app.pem" }"#,
        );
        let config_path = dir.path().join("orch.toml");
        std::fs::write(&config_path, toml).unwrap();

        let cfg = Config::load(&config_path).expect("relative path must resolve against config dir");
        // Sanity: the parsed Secret value still carries the relative path
        // exactly as written; resolution happens at load time.
        match &cfg.github.private_key {
            Secret::Path { path } => assert_eq!(path, Path::new("github-app.pem")),
            other => panic!("expected Secret::Path, got {other:?}"),
        }
    }

    #[test]
    fn secret_inline_empty_is_rejected_at_validate() {
        let toml = full_config_toml().replace(
            r#"webhook_secret = { inline = "super-secret" }"#,
            r#"webhook_secret = { inline = "" }"#,
        );
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("empty inline secret must reject");
        assert!(
            matches!(err, ConfigError::SecretEmpty { field } if field == "github.webhook_secret"),
            "got: {err:?}"
        );
    }

    #[test]
    fn secret_path_empty_file_is_rejected() {
        let empty = tempfile::NamedTempFile::new().unwrap();
        let toml = full_config_toml().replace(
            r#"webhook_secret = { inline = "super-secret" }"#,
            &format!(
                r#"webhook_secret = {{ path = "{}" }}"#,
                empty.path().display()
            ),
        );
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("empty file must reject");
        assert!(
            matches!(err, ConfigError::SecretEmpty { field } if field == "github.webhook_secret"),
            "got: {err:?}"
        );
    }

    #[test]
    fn secret_path_missing_file_is_rejected_with_io_error() {
        let toml = full_config_toml().replace(
            r#"webhook_secret = { inline = "super-secret" }"#,
            r#"webhook_secret = { path = "/this/path/does/not/exist/orch.secret" }"#,
        );
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("missing file must reject");
        assert!(matches!(err, ConfigError::SecretIo { .. }), "got: {err:?}");
    }

    #[test]
    fn empty_optional_bearer_token_is_rejected_even_on_loopback() {
        // Codex stop-gate round-3: a non-loopback ingest with bearer_token
        // = { inline = "" } previously passed the is_some() check while
        // providing zero auth. Empty optional tokens must reject regardless
        // of the listen address.
        let toml = full_config_toml().replace(
            "[server.ingest]\nlisten = \"127.0.0.1:8081\"",
            "[server.ingest]\nlisten = \"127.0.0.1:8081\"\nbearer_token = { inline = \"\" }",
        );
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("empty bearer token must reject");
        assert!(
            matches!(err, ConfigError::SecretEmpty { field } if field == "server.ingest.bearer_token"),
            "got: {err:?}"
        );
    }

    #[test]
    fn empty_agent_runner_bearer_token_is_rejected() {
        let toml = full_config_toml().replace(
            "[agent_runner]\nbase_url = \"http://localhost:8080\"",
            "[agent_runner]\nbase_url = \"http://localhost:8080\"\nbearer_token = { inline = \"\" }",
        );
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("empty agent token must reject");
        assert!(
            matches!(err, ConfigError::SecretEmpty { field } if field == "agent_runner.bearer_token"),
            "got: {err:?}"
        );
    }

    #[test]
    fn secret_with_both_inline_and_path_is_rejected() {
        let toml = full_config_toml().replace(
            r#"webhook_secret = { inline = "super-secret" }"#,
            r#"webhook_secret = { inline = "x", path = "/y" }"#,
        );
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("both fields must reject");
        let msg = format!("{err}");
        assert!(msg.contains("config load failed"), "{msg}");
    }

    #[test]
    fn secret_with_neither_field_is_rejected() {
        let toml = full_config_toml().replace(
            r#"webhook_secret = { inline = "super-secret" }"#,
            r#"webhook_secret = {}"#,
        );
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("empty secret must reject");
        let msg = format!("{err}");
        assert!(msg.contains("config load failed"), "{msg}");
    }

    #[test]
    fn unknown_top_level_field_is_rejected() {
        let toml = format!("{}\nsurprise = true\n", full_config_toml());
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("unknown field must reject");
        let msg = format!("{err}");
        assert!(msg.contains("config load failed"), "{msg}");
    }

    #[test]
    fn ingest_on_non_loopback_without_token_is_rejected() {
        let toml = full_config_toml().replace(
            r#"listen = "127.0.0.1:8081""#,
            r#"listen = "0.0.0.0:8081""#,
        );
        let f = write_tmp_toml(&toml);
        let err = Config::load(f.path()).expect_err("non-loopback ingest must require auth");
        assert!(matches!(err, ConfigError::IngestNeedsAuth { .. }));
    }

    #[test]
    fn ingest_on_non_loopback_with_token_is_accepted() {
        let toml = full_config_toml().replace(
            "[server.ingest]\nlisten = \"127.0.0.1:8081\"",
            "[server.ingest]\nlisten = \"0.0.0.0:8081\"\nbearer_token = { inline = \"t\" }",
        );
        let f = write_tmp_toml(&toml);
        Config::load(f.path()).expect("non-loopback ingest with token must load");
    }
}
