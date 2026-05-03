//! Reducer-side helper for computing deterministic action IDs while building
//! the `Vec<Action>` that the executor will enqueue.
//!
//! Reducers often need to know an action's id *before* the action is enqueued
//! — for example, to embed it as a marker inside the payload itself
//! (branch names like `auto/{ticket_slug}/{action_short}`, commit trailers
//! like `Action-Id: {action_id}`, HTML markers in PR bodies). The builder
//! couples id derivation to the action being pushed so the embedded id
//! cannot drift from the outbox row that `Storage::advance` will create.

use crate::action::Action;
use crate::ids::{ActionId, WorkflowId};

/// Build a list of actions and receive each one's deterministic identity.
///
/// **Critical contract:** the reducer MUST return `builder.into_actions()`
/// directly to the executor without reordering, filtering, or appending.
/// `Storage::advance` derives each action's id from its index in the
/// returned vector; mutating the vector after `push` invalidates every
/// `ActionRef` already handed out.
pub struct ActionBuilder<'a> {
    workflow_id: &'a WorkflowId,
    sequence: u64,
    actions: Vec<Action>,
}

/// The deterministic identity of an action, as it will be persisted by
/// `Storage::advance`.
#[derive(Clone, Debug)]
pub struct ActionRef {
    /// The full action ID, e.g. `act_abcdefghij...`. Embed in payloads when
    /// the sink probe matches on the full id (commit trailers, HTML markers).
    pub action_id: ActionId,
    /// First 16 base32 chars of the action_id body (the portion after the
    /// `act_` prefix). Use for human-visible short markers like branch names.
    /// 80 bits of collision resistance.
    pub short: String,
    /// Mirrors the `kind` of the pushed `Action`. Carried here so the reducer
    /// cannot accidentally embed a ref derived from a different kind.
    pub kind: String,
}

impl<'a> ActionBuilder<'a> {
    pub fn new(workflow_id: &'a WorkflowId, sequence: u64) -> Self {
        Self {
            workflow_id,
            sequence,
            actions: Vec::new(),
        }
    }

    /// Push an `Action` onto the builder and return its deterministic
    /// identity. The returned ref's `kind` always equals `action.kind`.
    pub fn push(&mut self, action: Action) -> ActionRef {
        let idx = self.actions.len() as u32;
        let action_id =
            ActionId::derive(self.workflow_id, self.sequence, idx, &action.kind);
        let short = short_form(&action_id);
        let kind = action.kind.clone();
        self.actions.push(action);
        ActionRef {
            action_id,
            short,
            kind,
        }
    }

    /// Compute the id `push` *would* produce for an action of `kind`,
    /// without actually pushing.
    ///
    /// Use this when the reducer must know the id before constructing the
    /// payload (e.g. embedding the action_id inside the payload). The
    /// returned id is only valid if the very next call to `push` uses the
    /// same `kind`, with no intervening `push` between them.
    pub fn peek_id(&self, kind: &str) -> ActionId {
        let idx = self.actions.len() as u32;
        ActionId::derive(self.workflow_id, self.sequence, idx, kind)
    }

    /// Consume the builder and return the actions in push order. The reducer
    /// MUST return this `Vec` to the executor unchanged.
    pub fn into_actions(self) -> Vec<Action> {
        self.actions
    }
}

/// Slice the 16-char base32 short form out of an `ActionId`. Internal helper.
///
/// `ActionId` is always `"act_" + 26 base32 chars` (see `ids::ActionId::derive`),
/// pure ASCII, so byte indexing is safe.
fn short_form(id: &ActionId) -> String {
    let s = id.as_str();
    debug_assert!(
        s.starts_with("act_") && s.len() >= 20,
        "malformed ActionId: {}",
        s
    );
    s[4..20].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dummy(kind: &str) -> Action {
        Action {
            kind: kind.into(),
            payload: json!({}),
            delay_seconds: 0,
            max_attempts: 5,
        }
    }

    #[test]
    fn push_returns_id_matching_action_id_derive() {
        let wf = WorkflowId::new("wf-x");
        let mut b = ActionBuilder::new(&wf, 7);
        let r0 = b.push(dummy("a.kind"));
        let r1 = b.push(dummy("b.kind"));
        assert_eq!(r0.action_id, ActionId::derive(&wf, 7, 0, "a.kind"));
        assert_eq!(r1.action_id, ActionId::derive(&wf, 7, 1, "b.kind"));
    }

    #[test]
    fn ref_kind_matches_pushed_action() {
        let wf = WorkflowId::new("wf-x");
        let mut b = ActionBuilder::new(&wf, 0);
        let r = b.push(dummy("notify"));
        assert_eq!(r.kind, "notify");
    }

    #[test]
    fn short_is_16_chars_and_equals_id_body_prefix() {
        let wf = WorkflowId::new("wf-x");
        let mut b = ActionBuilder::new(&wf, 1);
        let r = b.push(dummy("k"));
        assert_eq!(r.short.len(), 16);
        assert_eq!(r.short, &r.action_id.as_str()[4..20]);
        // base32 lowercase alphabet: a-z and 2-7.
        assert!(r
            .short
            .chars()
            .all(|c| c.is_ascii_lowercase() || ('2'..='7').contains(&c)));
    }

    #[test]
    fn changing_only_the_kind_changes_the_id() {
        let wf = WorkflowId::new("wf-x");
        let id_a = ActionId::derive(&wf, 0, 0, "k.a");
        let id_b = ActionId::derive(&wf, 0, 0, "k.b");
        assert_ne!(id_a, id_b);

        let mut b = ActionBuilder::new(&wf, 0);
        let ra = b.push(dummy("k.a"));
        // Reset for fairness — second builder, fresh index.
        let mut b2 = ActionBuilder::new(&wf, 0);
        let rb = b2.push(dummy("k.b"));
        assert_ne!(ra.action_id, rb.action_id);
    }

    #[test]
    fn peek_id_matches_subsequent_push_of_same_kind() {
        let wf = WorkflowId::new("wf-x");
        let mut b = ActionBuilder::new(&wf, 3);
        let peek = b.peek_id("kind");
        let r = b.push(dummy("kind"));
        assert_eq!(peek, r.action_id);
    }

    #[test]
    fn determinism_across_builder_instances() {
        let wf = WorkflowId::new("wf-x");
        let mut b1 = ActionBuilder::new(&wf, 11);
        let mut b2 = ActionBuilder::new(&wf, 11);
        let r1 = b1.push(dummy("k"));
        let r2 = b2.push(dummy("k"));
        assert_eq!(r1.action_id, r2.action_id);
        assert_eq!(r1.short, r2.short);
    }

    #[test]
    fn into_actions_preserves_push_order_and_kinds() {
        let wf = WorkflowId::new("wf-x");
        let mut b = ActionBuilder::new(&wf, 0);
        b.push(dummy("a"));
        b.push(dummy("b"));
        b.push(dummy("c"));
        let acts = b.into_actions();
        assert_eq!(acts.len(), 3);
        assert_eq!(acts[0].kind, "a");
        assert_eq!(acts[1].kind, "b");
        assert_eq!(acts[2].kind, "c");
    }
}
