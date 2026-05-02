//! The executor wraps Storage::advance with optimistic-concurrency retry.

use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{instrument, warn};

use crate::error::ExecutorError;
use crate::event::{AdvanceOutcome, EventCommand};
use crate::reducer::Reducer;
use crate::storage::Storage;

pub struct Executor<R: Reducer> {
    storage: Storage,
    reducer: Arc<R>,
    max_retries: u32,
}

impl<R: Reducer> Executor<R> {
    pub fn new(storage: Storage, reducer: R) -> Self {
        Self {
            storage,
            reducer: Arc::new(reducer),
            max_retries: 5,
        }
    }

    pub fn with_max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    pub fn storage(&self) -> &Storage { &self.storage }
    pub fn reducer(&self) -> &Arc<R> { &self.reducer }

    /// Advance the workflow. Retries on sequence conflicts with backoff.
    #[instrument(skip(self, cmd), fields(
        workflow_id = %cmd.workflow_id,
        payload_type = %cmd.payload_type,
    ))]
    pub async fn advance(&self, cmd: EventCommand) -> Result<AdvanceOutcome, ExecutorError> {
        let mut attempt = 0;
        loop {
            match self.storage.advance(&*self.reducer, &cmd).await {
                Ok(outcome) => return Ok(outcome),
                Err(ExecutorError::SequenceConflict) if attempt < self.max_retries => {
                    attempt += 1;
                    let backoff_ms = 10u64 * 2u64.pow(attempt);
                    warn!(attempt, backoff_ms, "sequence conflict, retrying");
                    sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(ExecutorError::SequenceConflict) => {
                    return Err(ExecutorError::RetryBudgetExhausted { attempts: attempt });
                }
                Err(e) => return Err(e),
            }
        }
    }
}
