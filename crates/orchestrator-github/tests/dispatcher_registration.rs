//! Verify GithubSink can be constructed and registered with a Dispatcher.

use orchestrator_core::*;
use orchestrator_github::{
    GithubAuth, GithubHintExtractor, GithubSink, KIND_CLOSE_PR, KIND_COMMIT_PATCH,
    KIND_ENSURE_BRANCH, KIND_OPEN_PR, KIND_POST_ISSUE_COMMENT, KIND_SET_PR_STATUS,
    KIND_UPDATE_PR_METADATA,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Default, Clone, Serialize, Deserialize, Debug)]
struct NoopState;

struct NoopReducer;

impl Reducer for NoopReducer {
    type State = NoopState;
    fn state_version(&self) -> u32 {
        1
    }
    fn reduce(
        &self,
        s: Self::State,
        _: &EventEnvelope,
    ) -> Result<Self::State, ExecutorError> {
        Ok(s)
    }
    fn derive_actions(
        &self,
        _: &Self::State,
        _: &EventEnvelope,
    ) -> Result<Vec<Action>, ExecutorError> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn github_sink_registers_with_dispatcher() {
    let pem = include_str!("fixtures/test_app_key.pem");
    let auth = GithubAuth::new(12345, pem, 67890).expect("fixture must parse");
    let sink = GithubSink::new(auth);

    // Sanity: trait surface. handles() is ALL_KINDS — extends as more
    // action kinds land.
    assert_eq!(sink.sink_key(), "github");
    assert_eq!(
        sink.handles(),
        &[
            KIND_ENSURE_BRANCH,
            KIND_COMMIT_PATCH,
            KIND_OPEN_PR,
            KIND_UPDATE_PR_METADATA,
            KIND_SET_PR_STATUS,
            KIND_CLOSE_PR,
            KIND_POST_ISSUE_COMMENT,
        ]
    );

    // Register with a real Dispatcher. If the trait shape didn't match,
    // this wouldn't compile.
    let storage = Storage::open("sqlite::memory:").await.unwrap();
    let executor = Arc::new(Executor::new(storage, NoopReducer));
    let mut dispatcher = Dispatcher::new(executor, DispatcherConfig::default());
    dispatcher.register(sink);
    dispatcher.register_extractor(GithubHintExtractor);
}
