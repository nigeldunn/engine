//! Orchestrator core: durable workflow executor with transactional outbox.

pub mod action;
pub mod dispatcher;
pub mod error;
pub mod event;
pub mod executor;
pub mod health;
pub mod ids;
pub mod reducer;
pub mod sink;
pub mod storage;

pub use action::{Action, ActionState, AttemptOutcome, ClaimedAction};
pub use dispatcher::{Dispatcher, DispatcherConfig};
pub use error::{DispatcherError, ExecutorError};
pub use event::{AdvanceOutcome, Causation, EventCommand, EventEnvelope};
pub use executor::Executor;
pub use health::{
    EndpointHint, HintExtractor, PersistedHealthState, SinkHealthRecord, SinkHealthScope,
    SinkHealthState, SinkUnhealthyReason,
};
pub use ids::{ActionId, DispatcherId, EventId, WorkflowId};
pub use reducer::Reducer;
pub use sink::{ExistingResult, Sink};
pub use storage::Storage;
