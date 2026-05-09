//! Library crate backing the `orchestrator-app` binary. The binary is a
//! thin wrapper so the config + logging + runtime + routing modules
//! stay testable.

pub mod config;
pub mod ingest;
pub mod logging;
pub mod runtime;
pub mod server;
pub mod webhook;
