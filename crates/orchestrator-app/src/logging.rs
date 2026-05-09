//! Tracing-subscriber bootstrap.
//!
//! Pretty formatter when stdout is a TTY (dev), JSON otherwise (prod
//! pipes stdout to a log shipper). `tracing-appender::non_blocking` keeps
//! log writes off the async runtime hot path. Filter is `RUST_LOG`-driven,
//! defaulting to `info,orchestrator=debug`.

use std::io::IsTerminal;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Initialize the global tracing subscriber. Returns a `WorkerGuard`
/// that must be held by `main` for the lifetime of the process —
/// dropping it flushes the non-blocking writer.
pub fn init() -> WorkerGuard {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,orchestrator=debug"));

    let (non_blocking_writer, guard) = tracing_appender::non_blocking(std::io::stdout());

    let is_tty = std::io::stdout().is_terminal();
    let registry = tracing_subscriber::registry().with(env_filter);
    if is_tty {
        registry
            .with(fmt::layer().with_writer(non_blocking_writer).pretty())
            .init();
    } else {
        registry
            .with(fmt::layer().with_writer(non_blocking_writer).json())
            .init();
    }

    guard
}
