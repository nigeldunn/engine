//! Orchestrator core: durable workflow executor with transactional outbox.

pub mod action;
pub mod action_builder;
pub mod dispatcher;
pub mod error;
pub mod event;
pub mod executor;
pub mod failure;
pub mod health;
pub mod ids;
pub mod reducer;
pub mod sink;
pub mod slug;
pub mod storage;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support;

pub use action::{Action, ActionState, AttemptOutcome, ClaimedAction};
pub use action_builder::{ActionBuilder, ActionRef};
pub use failure::{
    build_failure_event_command, decode_action_failed, ActionFailedPayload,
    EVT_ACTION_FAILED, EVT_ACTION_PROBE_EXHAUSTED, MAX_ORIGINAL_PAYLOAD_BYTES,
};
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
pub use slug::slugify;
pub use storage::Storage;
