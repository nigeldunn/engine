//! `HintExtractor` for github action payloads. Produces `EndpointHint::GithubRepo`
//! from any kind whose payload includes a repo reference, so the dispatcher
//! can build a meaningful health-check scope.
//!
//! Best-effort: if the payload is malformed, the extractor returns `None`
//! rather than failing. Validation runs at execute/probe time; this is a
//! hint, not a contract.

use orchestrator_core::{EndpointHint, HintExtractor};
use serde_json::Value;

use crate::action::{
    CommitPatchPayload, EnsureBranchPayload, KIND_COMMIT_PATCH, KIND_ENSURE_BRANCH,
};

pub struct GithubHintExtractor;

impl HintExtractor for GithubHintExtractor {
    fn extract(&self, action_kind: &str, payload: &Value) -> Option<EndpointHint> {
        match action_kind {
            KIND_ENSURE_BRANCH => {
                let p: EnsureBranchPayload = serde_json::from_value(payload.clone()).ok()?;
                Some(EndpointHint::GithubRepo {
                    owner: p.repo.owner,
                    name: p.repo.name,
                })
            }
            KIND_COMMIT_PATCH => {
                let p: CommitPatchPayload = serde_json::from_value(payload.clone()).ok()?;
                Some(EndpointHint::GithubRepo {
                    owner: p.repo.owner,
                    name: p.repo.name,
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_repo_hint_from_ensure_branch_payload() {
        let payload = json!({
            "repo": { "owner": "octo", "name": "world" },
            "base_branch": "main",
            "base_sha": "0123456789abcdef0123456789abcdef01234567",
            "branch_name": "auto/eng-1/ab",
            "ticket_id": "ENG-1",
        });
        let hint = GithubHintExtractor.extract(KIND_ENSURE_BRANCH, &payload);
        assert!(matches!(
            hint,
            Some(EndpointHint::GithubRepo { ref owner, ref name })
                if owner == "octo" && name == "world"
        ));
    }

    #[test]
    fn returns_none_for_unknown_kind() {
        let payload = json!({"repo": {"owner": "o", "name": "n"}});
        assert!(GithubHintExtractor
            .extract("some.other.kind", &payload)
            .is_none());
    }

    #[test]
    fn returns_none_for_malformed_payload() {
        let payload = json!({"not": "what we expect"});
        assert!(GithubHintExtractor
            .extract(KIND_ENSURE_BRANCH, &payload)
            .is_none());
    }

    #[test]
    fn extracts_repo_hint_from_commit_patch_payload() {
        let payload = json!({
            "repo": { "owner": "octo", "name": "world" },
            "branch": "auto/eng-1/abc",
            "expected_parent_sha": "0123456789abcdef0123456789abcdef01234567",
            "commit_message": "fix",
            "files": [{ "path": "x", "content": "y" }],
            "ticket_id": "ENG-1",
        });
        let hint = GithubHintExtractor.extract(KIND_COMMIT_PATCH, &payload);
        assert!(matches!(
            hint,
            Some(EndpointHint::GithubRepo { ref owner, ref name })
                if owner == "octo" && name == "world"
        ));
    }
}
