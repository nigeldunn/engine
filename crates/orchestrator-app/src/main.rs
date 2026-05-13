//! Orchestrator app binary. Two operating modes:
//!
//! - Default (no subcommand) — load config, boot the engine runtime
//!   (Storage + Executor + Dispatcher + webhook server + ingest server),
//!   wait on Ctrl+C / SIGTERM, then trigger graceful shutdown.
//! - `ingest` — open the configured database directly and write a
//!   single `TicketIngested` event without spinning up the engine.
//!   Mirrors the HTTP `POST /tickets` semantics (default workflow_id =
//!   `{source}:{id}`, 409-equivalent on payload conflict, idempotent
//!   on identical re-ingest). Useful for ops shells, automation, and
//!   integration tests.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};

use orchestrator_app::{
    config::{Config, LoadedConfig},
    ingest::{ingest_ticket, IngestError, IngestOutcome, IngestRequest},
    logging,
    runtime::{Runtime, ShutdownOutcome},
};
use orchestrator_coding_workflow::{
    events::{TicketIngested, TicketRef},
    WorkflowReducer,
};
use orchestrator_core::{Executor, Storage};
use orchestrator_github::RepoRef;

#[derive(Debug, Parser)]
#[command(name = "orchestrator-app", about = "Durable workflow engine for autonomous coding")]
struct Cli {
    /// Path to the TOML config file (no implicit search path).
    /// Required for `run` and `ingest`; ignored by `health` so the
    /// container liveness probe doesn't need config / secrets loaded.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the engine (default). Boots all subsystems and waits on a
    /// shutdown signal.
    Run,
    /// Ingest a single ticket directly into the configured database.
    /// Does NOT boot the engine; the next time the server starts (or
    /// if it's already running and shares the DB), the dispatcher
    /// will pick up the resulting actions.
    Ingest(IngestArgs),
    /// Container liveness probe. HTTP-GETs `/healthz` on the local
    /// webhook port and exits 0 on 200, non-zero otherwise. Designed
    /// to be invoked by ECS `healthCheck` from inside a distroless
    /// image (no shell, no wget) — the binary is its own probe.
    /// Loads no config and contacts no external services, so a
    /// missing PEM or unreachable Aurora cannot cause a false-negative
    /// liveness signal.
    Health(HealthArgs),
}

#[derive(Debug, Args)]
struct HealthArgs {
    /// Port the webhook listener is bound to. Defaults to 8080 to match
    /// the orchestrator config example. Override with `--port` when the
    /// task definition / config use a non-default port.
    #[arg(long, default_value_t = 8080)]
    port: u16,
}

#[derive(Debug, Args)]
struct IngestArgs {
    /// Ticket source — e.g., "manual", "linear", "jira".
    #[arg(long)]
    source: String,
    /// Ticket id within the source — e.g., "ENG-123".
    #[arg(long)]
    id: String,
    /// Repository owner.
    #[arg(long)]
    repo_owner: String,
    /// Repository name.
    #[arg(long)]
    repo_name: String,
    /// Branch to start from.
    #[arg(long)]
    base_branch: String,
    /// Commit SHA the workflow starts from.
    #[arg(long)]
    base_sha: String,
    /// Optional cumulative cost cap in cents.
    #[arg(long)]
    cost_budget_cents: Option<u64>,
    /// Run the architect agent before coding starts.
    #[arg(long, default_value_t = false)]
    require_architecture_review: bool,
    /// Override the derived workflow id. Default is "{source}:{id}".
    #[arg(long)]
    workflow_id: Option<String>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let command = cli.command.unwrap_or(Command::Run);

    // The health probe runs INSIDE the container as the ECS liveness
    // check (compute.tf). It must not load config or initialize tracing
    // — config-load failures would mask a healthy main process, and
    // tracing setup would log to stderr on every probe (noisy in
    // CloudWatch). Handle it before any of that runs.
    if let Command::Health(args) = &command {
        return health_probe_exit(args.port);
    }

    let _guard = logging::init();

    let config_path = match cli.config.as_ref() {
        Some(p) => p,
        None => {
            tracing::error!("missing required --config <PATH> for this subcommand");
            return ExitCode::from(1);
        }
    };
    let cfg = match Config::load(config_path) {
        Ok(c) => c,
        Err(err) => {
            tracing::error!(%err, config_path = %config_path.display(), "config load failed");
            return ExitCode::from(1);
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            tracing::error!(%err, "tokio runtime build failed");
            return ExitCode::from(1);
        }
    };

    match command {
        Command::Run => rt.block_on(run_engine(cfg)),
        Command::Ingest(args) => rt.block_on(run_ingest_subcommand(cfg, args)),
        Command::Health(_) => unreachable!("handled before tokio init above"),
    }
}

/// Process-level liveness probe. Opens a TCP connection to
/// `127.0.0.1:<port>`, sends a minimal HTTP/1.1 GET for `/healthz`,
/// and inspects the status line.
///
/// Implementation note: deliberately uses raw `std::net::TcpStream`
/// rather than reqwest so the probe (a) keeps the binary's dependency
/// surface unchanged for this single shell-out, (b) starts in
/// microseconds with no async runtime, and (c) cannot fail-open via a
/// crate-level bug introduced elsewhere.
fn health_probe_exit(port: u16) -> ExitCode {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    let connect_result = TcpStream::connect_timeout(
        &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_secs(2),
    );
    let mut stream = match connect_result {
        Ok(s) => s,
        Err(_) => return ExitCode::from(1),
    };
    if stream.set_read_timeout(Some(Duration::from_secs(2))).is_err()
        || stream.set_write_timeout(Some(Duration::from_secs(2))).is_err()
    {
        return ExitCode::from(1);
    }
    let request = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if stream.write_all(request).is_err() {
        return ExitCode::from(1);
    }
    // Read just the status line — we don't care about headers or body
    // for a liveness check. 128 bytes is plenty.
    let mut buf = [0u8; 128];
    let n = match stream.read(&mut buf) {
        Ok(n) => n,
        Err(_) => return ExitCode::from(1),
    };
    let head = &buf[..n];
    if head.starts_with(b"HTTP/1.1 200") || head.starts_with(b"HTTP/1.0 200") {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

async fn run_engine(cfg: LoadedConfig) -> ExitCode {
    let runtime = match Runtime::boot(&cfg).await {
        Ok(r) => r,
        Err(err) => {
            tracing::error!(%err, "runtime boot failed");
            return ExitCode::from(1);
        }
    };

    match wait_for_shutdown_signal().await {
        Ok(sig) => tracing::info!(signal = sig, "shutdown signal received"),
        Err(err) => tracing::error!(%err, "signal wait failed; shutting down anyway"),
    }

    // Exit code mapping (see operator runbook):
    //   0 — clean drain
    //   1 — at least one subsystem returned a typed error during drain
    //   2 — at least one subsystem did not drain within the grace
    //       period and was aborted (k8s alarm: pod likely SIGKILLed)
    //   3 — at least one subsystem task panicked
    match runtime.shutdown().await {
        ShutdownOutcome::Drained => ExitCode::SUCCESS,
        ShutdownOutcome::DrainErrored => ExitCode::from(1),
        ShutdownOutcome::TimedOut => ExitCode::from(2),
        ShutdownOutcome::Panicked => ExitCode::from(3),
    }
}

async fn run_ingest_subcommand(cfg: LoadedConfig, args: IngestArgs) -> ExitCode {
    // Open Storage directly — no Dispatcher / sinks. Concurrent with a
    // running engine instance is safe: `Storage::advance` is transactional
    // and Postgres serialises any contention through the per-row locks.
    let storage = match Storage::open(&cfg.storage.database_url).await {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(%err, "storage open failed");
            return ExitCode::from(1);
        }
    };
    let executor = Arc::new(Executor::new(storage, WorkflowReducer));

    let request = IngestRequest {
        workflow_id: args.workflow_id,
        ticket_ingested: TicketIngested {
            ticket: TicketRef {
                source: args.source,
                id: args.id,
            },
            repo: RepoRef {
                owner: args.repo_owner,
                name: args.repo_name,
            },
            base_branch: args.base_branch,
            base_sha: args.base_sha,
            cost_budget_cents: args.cost_budget_cents,
            require_architecture_review: args.require_architecture_review,
        },
    };

    match ingest_ticket(&executor, request).await {
        Ok(IngestOutcome::Created { workflow_id }) => {
            println!("created workflow {}", workflow_id.as_str());
            ExitCode::SUCCESS
        }
        Ok(IngestOutcome::AlreadyExists { workflow_id }) => {
            println!("workflow {} already exists (idempotent)", workflow_id.as_str());
            ExitCode::SUCCESS
        }
        Err(IngestError::Conflict { dedup_key, .. }) => {
            eprintln!("conflict: dedup key {dedup_key} already exists with a different payload");
            ExitCode::from(2)
        }
        Err(err) => {
            tracing::error!(%err, "ingest failed");
            ExitCode::from(1)
        }
    }
}

/// Block until either Ctrl+C or SIGTERM. Returns the signal name.
async fn wait_for_shutdown_signal() -> std::io::Result<&'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate())?;
        tokio::select! {
            res = tokio::signal::ctrl_c() => res.map(|_| "SIGINT"),
            _ = term.recv() => Ok("SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.map(|_| "SIGINT")
    }
}
