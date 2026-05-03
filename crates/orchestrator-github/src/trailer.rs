//! `Action-Id` commit trailer: append at format time, extract at probe time.
//!
//! Per PLAN.md the format is the literal `{commit_message}\n\nAction-Id: {action_id}` —
//! always a new trailer paragraph. Probe scans the last paragraph of a
//! commit message for an `Action-Id:` line and returns the value verbatim;
//! the caller compares it to the action's id.

use orchestrator_core::ActionId;

const TRAILER_KEY: &str = "Action-Id: ";

/// Append `Action-Id: {action_id}` as a fresh trailer paragraph. Trims
/// trailing whitespace from `message` first so we don't end up with three
/// consecutive newlines.
pub fn append_action_id_trailer(message: &str, action_id: &ActionId) -> String {
    let trimmed = message.trim_end();
    if trimmed.is_empty() {
        return format!("{}{}", TRAILER_KEY, action_id);
    }
    format!("{}\n\n{}{}", trimmed, TRAILER_KEY, action_id)
}

/// Look for an `Action-Id:` line in the last paragraph of `message`.
/// Returns the trailer value (e.g. `act_abcdef...`) on a hit. Multiple
/// trailers in the same paragraph are supported — if more than one
/// `Action-Id:` appears, the first match wins.
pub fn extract_action_id_trailer(message: &str) -> Option<String> {
    let trimmed = message.trim_end();
    let last_para = match trimmed.rsplit_once("\n\n") {
        Some((_, last)) => last,
        None => trimmed,
    };
    for line in last_para.lines() {
        if let Some(value) = line.strip_prefix(TRAILER_KEY) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(id: &str) -> ActionId {
        ActionId(id.into())
    }

    // ── append ─────────────────────────────────────────────────────────

    #[test]
    fn append_to_empty_message_emits_trailer_only() {
        let s = append_action_id_trailer("", &aid("act_abc"));
        assert_eq!(s, "Action-Id: act_abc");
    }

    #[test]
    fn append_to_simple_message() {
        let s = append_action_id_trailer("fix the thing", &aid("act_abc"));
        assert_eq!(s, "fix the thing\n\nAction-Id: act_abc");
    }

    #[test]
    fn append_trims_trailing_whitespace_first() {
        let s = append_action_id_trailer("fix\n\n", &aid("act_abc"));
        assert_eq!(s, "fix\n\nAction-Id: act_abc");
    }

    #[test]
    fn append_then_extract_round_trips() {
        let id = aid("act_5lkfsbcuh4abcd");
        let msg = append_action_id_trailer("subject\n\nbody", &id);
        assert_eq!(extract_action_id_trailer(&msg), Some(id.0));
    }

    // ── extract ────────────────────────────────────────────────────────

    #[test]
    fn extract_from_message_without_trailer_returns_none() {
        assert_eq!(extract_action_id_trailer("just a subject"), None);
    }

    #[test]
    fn extract_from_empty_message_returns_none() {
        assert_eq!(extract_action_id_trailer(""), None);
    }

    #[test]
    fn extract_finds_in_single_paragraph() {
        let msg = "Action-Id: act_xyz";
        assert_eq!(extract_action_id_trailer(msg), Some("act_xyz".into()));
    }

    #[test]
    fn extract_finds_in_last_paragraph_after_blank() {
        let msg = "subject\n\nbody text\n\nReviewed-by: Alice\nAction-Id: act_xyz";
        assert_eq!(extract_action_id_trailer(msg), Some("act_xyz".into()));
    }

    #[test]
    fn extract_ignores_action_id_in_earlier_paragraph() {
        // Probe scans only the last paragraph. An "Action-Id:" line in the
        // body but not in the trailer block is intentionally not matched.
        let msg = "Subject\n\nAction-Id: act_in_body_not_trailer\n\nReviewed-by: bob";
        assert_eq!(extract_action_id_trailer(msg), None);
    }

    #[test]
    fn extract_handles_trailing_whitespace_around_value() {
        let msg = "subject\n\nAction-Id:  act_xyz   \n";
        assert_eq!(extract_action_id_trailer(msg), Some("act_xyz".into()));
    }

    #[test]
    fn extract_skips_empty_action_id_value() {
        let msg = "subject\n\nAction-Id: \nOther-Trailer: y";
        assert_eq!(extract_action_id_trailer(msg), None);
    }

    #[test]
    fn extract_first_match_wins_when_multiple() {
        let msg = "subject\n\nAction-Id: act_first\nAction-Id: act_second";
        assert_eq!(extract_action_id_trailer(msg), Some("act_first".into()));
    }

    #[test]
    fn extract_works_with_crlf_inside_paragraph() {
        // Robust to messages with mixed line endings within the trailer block.
        let msg = "subject\n\nReviewed-by: bob\r\nAction-Id: act_crlf";
        assert_eq!(extract_action_id_trailer(msg), Some("act_crlf".into()));
    }
}
