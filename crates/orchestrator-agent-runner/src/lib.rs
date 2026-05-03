//! Agent runner sinks for the orchestrator.
//!
//! Implements 5 `Sink`s for the `agent.run_*` action kinds emitted by
//! the coding-workflow reducer (M11b). Each sink calls an external agent
//! service via the `AgentClient` trait — default `HttpAgentClient` over
//! reqwest; tests substitute a mock implementation.
//!
//! Cost reporting flows via `AttemptOutcome::Succeeded.side_events`:
//! when the agent service returns `cost_cents`, the sink emits a
//! `BudgetConsumed` side event alongside the agent output event. The
//! dispatcher writes both atomically (per the M12a contract).
//!
//! Per-HTTP-attempt correlation: a fresh `request_id` (UUID v7) is
//! generated for each `AgentClient::run` call, sent as `X-Request-Id`,
//! and stamped onto the outcome event's `trace_id` field. This is NOT
//! the durable workflow trace — propagating `EventEnvelope.trace_id`
//! through `ClaimedAction` is M12c+ work.

pub mod actions;
pub mod client;
pub mod dispatch;
pub mod errors;
pub mod sink;

pub use client::{
    fresh_request_id, AgentClient, AgentRunResult, AgentRunStatus, HttpAgentClient,
};
pub use errors::AgentError;
pub use sink::AgentRunnerSink;
