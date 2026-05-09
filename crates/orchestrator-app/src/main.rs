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
    #[arg(long, value_name = "PATH")]
    config: PathBuf,

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
    let _guard = logging::init();

    let cfg = match Config::load(&cli.config) {
        Ok(c) => c,
        Err(err) => {
            tracing::error!(%err, config_path = %cli.config.display(), "config load failed");
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

    match cli.command.unwrap_or(Command::Run) {
        Command::Run => rt.block_on(run_engine(cfg)),
        Command::Ingest(args) => rt.block_on(run_ingest_subcommand(cfg, args)),
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
    // running engine instance is safe (SQLite WAL handles the writer
    // contention; Storage::advance is transactional).
    let sqlite_path = cfg.storage.resolved_sqlite_path(cfg.base_dir());
    let database_url = format!("sqlite:{}", sqlite_path.display());
    let storage = match Storage::open(&database_url).await {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(%err, %database_url, "storage open failed");
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
