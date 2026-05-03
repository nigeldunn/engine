//! Translate GitHub webhook deliveries into workflow `EventCommand`s.
//!
//! M11b v1 cares about exactly one webhook event: `pull_request.merged`.
//! It maps to a `github.pr_merged.v1` event in the workflow's log so the
//! reducer can transition `AwaitingHumanApproval` → `Merged`.
//!
//! The `delivery_id` becomes the `ingress_dedup_key`, so if GitHub
//! retries the delivery, `Storage::advance` absorbs the duplicate.
//!
//! `workflow_id` resolution is the consumer's concern (PLAN.md M10
//! addendum: webhook crate is transport-only). This translator takes
//! a workflow_id resolver closure so the consumer can map a PR or
//! repo back to its workflow ID via whatever index it maintains
//! externally.

use orchestrator_core::{Causation, EventCommand, WorkflowId};
use orchestrator_github_webhook::GithubWebhookDelivery;

use crate::events::{PrMerged, EVT_PR_MERGED};

/// Translate a GitHub webhook delivery into an `EventCommand`. Returns
/// `None` for events the workflow doesn't react to.
///
/// Caller supplies a `resolve_workflow_id` closure that maps the PR
/// payload to a `WorkflowId`. Typically it's a database lookup keyed
/// on (repo, pr_number) → workflow_id, indexed at PR-open time.
pub fn translate_github_webhook(
    delivery: &GithubWebhookDelivery,
    resolve_workflow_id: impl Fn(&serde_json::Value) -> Option<WorkflowId>,
) -> Option<EventCommand> {
    if delivery.event_type != "pull_request" {
        return None;
    }
    if delivery.action.as_deref() != Some("closed") {
        return None;
    }
    // closed-with-merge vs closed-without-merge: GitHub sets
    // `pull_request.merged: bool` on the close action.
    if delivery.payload["pull_request"]["merged"].as_bool() != Some(true) {
        return None;
    }

    let workflow_id = resolve_workflow_id(&delivery.payload)?;
    let pr_number = delivery.payload["pull_request"]["number"].as_u64()?;
    let merge_commit_sha = delivery.payload["pull_request"]["merge_commit_sha"]
        .as_str()?
        .to_string();
    let repo_owner = delivery.payload["repository"]["owner"]["login"]
        .as_str()?
        .to_string();
    let repo_name = delivery.payload["repository"]["name"]
        .as_str()?
        .to_string();

    let body = PrMerged {
        repo: orchestrator_github::RepoRef {
            owner: repo_owner,
            name: repo_name,
        },
        pr_number,
        merge_commit_sha,
    };

    Some(EventCommand {
        workflow_id,
        payload_type: EVT_PR_MERGED.into(),
        payload_schema_version: 1,
        payload: serde_json::to_value(&body).expect("PrMerged serializes infallibly"),
        causation: Causation::External {
            source: "github_webhook".into(),
            request_id: delivery.delivery_id.clone(),
        },
        trace_id: None,
        ingress_dedup_key: Some(delivery.delivery_id.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn delivery(event_type: &str, payload: serde_json::Value) -> GithubWebhookDelivery {
        let action = payload.get("action").and_then(|a| a.as_str()).map(String::from);
        GithubWebhookDelivery {
            event_type: event_type.into(),
            delivery_id: "delivery-1".into(),
            action,
            payload,
        }
    }

    #[test]
    fn translates_pull_request_merged_to_pr_merged_event() {
        let d = delivery(
            "pull_request",
            json!({
                "action": "closed",
                "pull_request": {
                    "number": 42,
                    "merged": true,
                    "merge_commit_sha": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
                },
                "repository": {
                    "owner": { "login": "octo" },
                    "name": "world",
                },
            }),
        );
        let cmd = translate_github_webhook(&d, |_| Some(WorkflowId::new("wf-1"))).unwrap();
        assert_eq!(cmd.payload_type, EVT_PR_MERGED);
        assert_eq!(cmd.workflow_id.as_str(), "wf-1");
        assert_eq!(cmd.ingress_dedup_key.as_deref(), Some("delivery-1"));
        let body: PrMerged = serde_json::from_value(cmd.payload).unwrap();
        assert_eq!(body.pr_number, 42);
        assert_eq!(body.merge_commit_sha, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef");
    }

    #[test]
    fn ignores_pull_request_closed_without_merge() {
        let d = delivery(
            "pull_request",
            json!({
                "action": "closed",
                "pull_request": { "number": 42, "merged": false },
                "repository": { "owner": { "login": "o" }, "name": "n" },
            }),
        );
        assert!(translate_github_webhook(&d, |_| Some(WorkflowId::new("wf-1"))).is_none());
    }

    #[test]
    fn ignores_pull_request_other_actions() {
        for action in ["opened", "edited", "reopened", "synchronize"] {
            let d = delivery(
                "pull_request",
                json!({
                    "action": action,
                    "pull_request": { "number": 42, "merged": false },
                }),
            );
            assert!(
                translate_github_webhook(&d, |_| Some(WorkflowId::new("wf-1"))).is_none(),
                "should ignore action={}",
                action
            );
        }
    }

    #[test]
    fn ignores_unrelated_event_types() {
        let d = delivery(
            "issue_comment",
            json!({"action": "created"}),
        );
        assert!(translate_github_webhook(&d, |_| Some(WorkflowId::new("wf-1"))).is_none());
    }

    #[test]
    fn returns_none_when_resolver_cannot_find_workflow() {
        let d = delivery(
            "pull_request",
            json!({
                "action": "closed",
                "pull_request": {
                    "number": 42, "merged": true,
                    "merge_commit_sha": "0".repeat(40),
                },
                "repository": { "owner": { "login": "o" }, "name": "n" },
            }),
        );
        assert!(translate_github_webhook(&d, |_| None).is_none());
    }
}
