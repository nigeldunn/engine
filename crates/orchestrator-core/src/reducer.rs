//! The Reducer trait. Implementations of this are where workflow logic lives.
//!
//! Reducers MUST be pure: same inputs always produce same outputs, no I/O,
//! no clock reads, no randomness. The executor wraps this purity with
//! durable persistence and side-effect dispatch.

use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value as Json;

use crate::action::Action;
use crate::event::EventEnvelope;
use crate::error::ExecutorError;

/// A reducer maps (state, event) to (new state, actions).
pub trait Reducer: Send + Sync + 'static {
    /// The state type. Must be JSON-serializable for snapshotting.
    type State: Serialize + DeserializeOwned + Clone + Default + Send + Sync;

    /// Schema version of `State`. Bump when the state shape changes
    /// (and provide a migration).
    fn state_version(&self) -> u32;

    /// Apply an event to the state. Pure function.
    fn reduce(
        &self,
        state: Self::State,
        event: &EventEnvelope,
    ) -> Result<Self::State, ExecutorError>;

    /// Determine actions the new state demands. Pure function.
    /// Called exactly once per advance, after `reduce`.
    fn derive_actions(
        &self,
        new_state: &Self::State,
        triggering_event: &EventEnvelope,
    ) -> Result<Vec<Action>, ExecutorError>;
}

/// Helper for reducers that store state as JSON.
pub fn state_to_json<S: Serialize>(state: &S) -> Result<Json, ExecutorError> {
    serde_json::to_value(state).map_err(ExecutorError::from)
}

pub fn state_from_json<S: DeserializeOwned + Default>(
    json: Option<Json>,
) -> Result<S, ExecutorError> {
    match json {
        Some(j) => serde_json::from_value(j).map_err(ExecutorError::from),
        None => Ok(S::default()),
    }
}
