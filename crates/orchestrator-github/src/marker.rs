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
use sha2::{Digest, Sha256};

const MARKER_PREFIX: &str = "<!-- orchestrator-action: ";
const MARKER_SUFFIX: &str = " -->";

const SHA_FOOTER_PREFIX: &str = "[orch:";
const SHA_FOOTER_SUFFIX: &str = "]";

/// Length of the hex prefix used in the sha256 footer marker. 8 hex chars
/// (32 bits) — enough collision resistance within a single issue's
/// comment list since action ids are blake3-deterministic upstream.
const SHA_FOOTER_HEX_LEN: usize = 8;

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

// ── sha256 footer (post_issue_comment fallback marker) ────────────────────

/// Return the short identity hash for an action: first 8 hex chars of
/// `sha256(action_id)`. Used as a plain-text fallback marker when an HTML
/// comment marker may have been stripped (defensive — GitHub doesn't
/// usually strip them, but renderers vary).
pub fn action_id_sha256_short(action_id: &ActionId) -> String {
    let mut h = Sha256::new();
    h.update(action_id.as_str().as_bytes());
    let result = h.finalize();
    let mut hex = String::with_capacity(SHA_FOOTER_HEX_LEN);
    for byte in result.iter().take(SHA_FOOTER_HEX_LEN / 2) {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}

/// Format the sha256 footer marker — `[orch:{8 hex}]`.
pub fn sha256_footer(action_id: &ActionId) -> String {
    format!(
        "{}{}{}",
        SHA_FOOTER_PREFIX,
        action_id_sha256_short(action_id),
        SHA_FOOTER_SUFFIX
    )
}

/// Append the sha256 footer to `body` as a fresh trailing paragraph, unless
/// the footer is already present (so calling twice is a no-op).
pub fn append_sha256_footer(body: &str, action_id: &ActionId) -> String {
    let footer = sha256_footer(action_id);
    if body.contains(&footer) {
        return body.to_string();
    }
    let trimmed = body.trim_end();
    if trimmed.is_empty() {
        footer
    } else {
        format!("{}\n\n{}", trimmed, footer)
    }
}

/// True when `body` contains our sha256 footer for `action_id`.
pub fn matches_sha256_footer(body: &str, action_id: &ActionId) -> bool {
    body.contains(&sha256_footer(action_id))
}

/// Composed marker for `post_issue_comment` bodies — HTML primary +
/// sha256 footer fallback, in that order. Matches the dual-marker
/// strategy described in PLAN.md.
pub fn append_comment_markers(body: &str, action_id: &ActionId) -> String {
    let with_html = append_action_id_marker(body, action_id);
    append_sha256_footer(&with_html, action_id)
}

/// True when `body` carries either the HTML marker for `action_id` or
/// the sha256 footer fallback. Used by `post_issue_comment::probe`.
pub fn comment_carries_marker(body: &str, action_id: &ActionId) -> bool {
    if let Some(extracted) = extract_action_id_marker(body) {
        if extracted == action_id.as_str() {
            return true;
        }
    }
    matches_sha256_footer(body, action_id)
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

    // ── sha256 footer ──────────────────────────────────────────────────

    #[test]
    fn sha256_short_is_8_hex_chars_and_deterministic() {
        let id = aid("act_abc");
        let s1 = action_id_sha256_short(&id);
        let s2 = action_id_sha256_short(&id);
        assert_eq!(s1, s2, "deterministic");
        assert_eq!(s1.len(), 8);
        assert!(s1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn sha256_short_differs_for_different_action_ids() {
        let a = action_id_sha256_short(&aid("act_first"));
        let b = action_id_sha256_short(&aid("act_second"));
        assert_ne!(a, b);
    }

    #[test]
    fn append_sha256_footer_emits_plain_text_marker() {
        let id = aid("act_xyz");
        let s = append_sha256_footer("hello", &id);
        let expected = format!("hello\n\n[orch:{}]", action_id_sha256_short(&id));
        assert_eq!(s, expected);
    }

    #[test]
    fn append_sha256_footer_is_idempotent() {
        let id = aid("act_xyz");
        let once = append_sha256_footer("hello", &id);
        let twice = append_sha256_footer(&once, &id);
        assert_eq!(once, twice);
    }

    #[test]
    fn append_sha256_footer_to_empty_emits_just_footer() {
        let id = aid("act_xyz");
        let s = append_sha256_footer("", &id);
        assert_eq!(s, format!("[orch:{}]", action_id_sha256_short(&id)));
    }

    #[test]
    fn matches_sha256_footer_substring() {
        let id = aid("act_xyz");
        let body = format!("some text [orch:{}] more text", action_id_sha256_short(&id));
        assert!(matches_sha256_footer(&body, &id));
        assert!(!matches_sha256_footer("no footer here", &id));
    }

    // ── composed comment markers ───────────────────────────────────────

    #[test]
    fn append_comment_markers_emits_both() {
        let id = aid("act_xyz");
        let s = append_comment_markers("Comment.", &id);
        assert!(s.contains("<!-- orchestrator-action: act_xyz -->"));
        assert!(s.contains(&format!("[orch:{}]", action_id_sha256_short(&id))));
    }

    #[test]
    fn comment_carries_marker_via_html() {
        let id = aid("act_xyz");
        let body = "Some comment.\n\n<!-- orchestrator-action: act_xyz -->";
        assert!(comment_carries_marker(body, &id));
    }

    #[test]
    fn comment_carries_marker_via_sha256_fallback() {
        let id = aid("act_xyz");
        // Note: NO HTML marker — only the sha256 footer.
        let body = format!("plain body\n\n[orch:{}]", action_id_sha256_short(&id));
        assert!(comment_carries_marker(&body, &id));
    }

    #[test]
    fn comment_carries_marker_returns_false_for_unrelated_body() {
        let id = aid("act_xyz");
        assert!(!comment_carries_marker("nothing here", &id));
    }

    #[test]
    fn comment_carries_marker_does_not_match_other_action_id() {
        let id_ours = aid("act_ours");
        let id_other = aid("act_other");
        let body = append_comment_markers("hi", &id_other);
        assert!(!comment_carries_marker(&body, &id_ours));
    }
}
