use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("sequence conflict on workflow advance - retry")]
    SequenceConflict,

    #[error("reducer failed: {0}")]
    Reducer(String),

    #[error("storage error: {0}")]
    Storage(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("retry budget exhausted after {attempts} attempts")]
    RetryBudgetExhausted { attempts: u32 },

    #[error("internal: {0}")]
    Internal(String),
}

#[derive(Debug, Error)]
pub enum DispatcherError {
    #[error("lease lost - another dispatcher took over")]
    LeaseLost,

    #[error("storage error: {0}")]
    Storage(#[from] sqlx::Error),

    #[error("executor error: {0}")]
    Executor(#[from] ExecutorError),

    #[error("sink error: {0}")]
    Sink(String),

    #[error("internal: {0}")]
    Internal(String),
}
