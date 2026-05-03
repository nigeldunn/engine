//! HTML-comment marker for PR-body identity. Sibling to `trailer.rs`.
//!
//! Format: `<!-- orchestrator-action: {action_id} -->`. Always appended at
//! the end of the body in its own paragraph; the reducer's body input does
//! NOT include the marker, so its presence on the GitHub side is proof the
//! sink emitted it.
//!
//! Probe extracts the marker substring from a PR body; the caller (e.g.
//! `actions::open_pr::probe`) compares the extracted id to the action's id
//! and additionally checks for multiple markers across PRs (which would
//! indicate a real bug — `ActionId` collisions are blake3-prevented).

use orchestrator_core::ActionId;

const MARKER_PREFIX: &str = "<!-- orchestrator-action: ";
const MARKER_SUFFIX: &str = " -->";

/// Append the action-id marker as a fresh trailing paragraph.
///
/// `body` is the user-supplied text. If empty, we emit just the marker.
/// Trailing whitespace is trimmed so we never produce three consecutive
/// newlines.
pub fn append_action_id_marker(body: &str, action_id: &ActionId) -> String {
    let trimmed = body.trim_end();
    let marker = format!("{}{}{}", MARKER_PREFIX, action_id, MARKER_SUFFIX);
    if trimmed.is_empty() {
        marker
    } else {
        format!("{}\n\n{}", trimmed, marker)
    }
}

/// Find the first `<!-- orchestrator-action: <id> -->` marker in `body`
/// and return its `<id>` value (trimmed). Returns `None` if no marker
/// is present.
///
/// The probe scans every PR's body looking for a match against the
/// action's own id; it's the probe's responsibility to flag duplicate
/// matches across PRs.
pub fn extract_action_id_marker(body: &str) -> Option<String> {
    let start = body.find(MARKER_PREFIX)?;
    let after_prefix = &body[start + MARKER_PREFIX.len()..];
    let end = after_prefix.find(MARKER_SUFFIX)?;
    let value = after_prefix[..end].trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(id: &str) -> ActionId {
        ActionId(id.into())
    }

    // ── append ────────────────────────────────────────────────────────

    #[test]
    fn append_to_empty_body_emits_marker_only() {
        let s = append_action_id_marker("", &aid("act_abc"));
        assert_eq!(s, "<!-- orchestrator-action: act_abc -->");
    }

    #[test]
    fn append_to_simple_body_uses_blank_line_separator() {
        let s = append_action_id_marker("Closes ENG-1.", &aid("act_abc"));
        assert_eq!(
            s,
            "Closes ENG-1.\n\n<!-- orchestrator-action: act_abc -->"
        );
    }

    #[test]
    fn append_trims_trailing_whitespace_before_separator() {
        let s = append_action_id_marker("Closes ENG-1.\n\n", &aid("act_abc"));
        assert_eq!(
            s,
            "Closes ENG-1.\n\n<!-- orchestrator-action: act_abc -->"
        );
    }

    #[test]
    fn append_then_extract_round_trips() {
        let id = aid("act_5lkfsbcuh4abcd");
        let body = append_action_id_marker("Some PR description.", &id);
        assert_eq!(extract_action_id_marker(&body), Some(id.0));
    }

    // ── extract ───────────────────────────────────────────────────────

    #[test]
    fn extract_from_body_without_marker_returns_none() {
        assert_eq!(extract_action_id_marker("just a description"), None);
    }

    #[test]
    fn extract_from_empty_body_returns_none() {
        assert_eq!(extract_action_id_marker(""), None);
    }

    #[test]
    fn extract_finds_marker_anywhere_in_body() {
        let body = "Some text\n\n<!-- orchestrator-action: act_xyz -->\n\nMore text";
        assert_eq!(extract_action_id_marker(body), Some("act_xyz".into()));
    }

    #[test]
    fn extract_handles_inner_whitespace() {
        let body = "<!-- orchestrator-action:   act_xyz   -->";
        assert_eq!(extract_action_id_marker(body), Some("act_xyz".into()));
    }

    #[test]
    fn extract_skips_other_html_comments() {
        let body = "<!-- not our marker --><!-- orchestrator-action: act_xyz -->";
        assert_eq!(extract_action_id_marker(body), Some("act_xyz".into()));
    }

    #[test]
    fn extract_skips_marker_with_empty_id() {
        let body = "<!-- orchestrator-action:  -->";
        assert_eq!(extract_action_id_marker(body), None);
    }

    #[test]
    fn extract_returns_first_match_when_multiple_present() {
        // Marker collisions across PRs are detected at the probe level.
        // Within a single body, first match wins — the probe only uses
        // this to determine whether a body matches at all.
        let body = "<!-- orchestrator-action: act_first -->\n\n<!-- orchestrator-action: act_second -->";
        assert_eq!(extract_action_id_marker(body), Some("act_first".into()));
    }

    #[test]
    fn extract_does_not_match_malformed_marker_missing_suffix() {
        let body = "<!-- orchestrator-action: act_xyz";
        assert_eq!(extract_action_id_marker(body), None);
    }
}
