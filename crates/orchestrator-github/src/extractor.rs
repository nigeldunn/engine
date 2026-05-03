//! `HintExtractor` for github action payloads. M3 stub: no kinds registered,
//! so nothing to extract. M4 (`github.ensure_branch`) wires up the first
//! match arm to produce `EndpointHint::GithubRepo` from the action payload.

use orchestrator_core::{EndpointHint, HintExtractor};
use serde_json::Value;

pub struct GithubHintExtractor;

impl HintExtractor for GithubHintExtractor {
    fn extract(&self, _action_kind: &str, _payload: &Value) -> Option<EndpointHint> {
        None
    }
}
