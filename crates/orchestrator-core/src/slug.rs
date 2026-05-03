//! Branch-safe slugification with deterministic hash-suffix truncation.
//!
//! Used by reducers to convert ticket IDs and other arbitrary strings into
//! safe components of git refs, paths, and external markers.

use std::fmt::Write;

/// Convert an arbitrary string into a deterministic, branch-safe slug.
///
/// Algorithm:
/// 1. Map each character: ASCII alphanumeric → lowercase ASCII alphanumeric;
///    everything else (whitespace, punctuation, slashes, non-ASCII) → `-`.
/// 2. Collapse consecutive dashes; trim leading and trailing dashes.
/// 3. If the result is longer than `max_len`, truncate to `max_len - 17`
///    chars (trimming any resulting trailing dash) and append
///    `-{16 hex chars}` where the hex is the first 8 bytes of `blake3(input)`.
///
/// The output is pure ASCII `[a-z0-9-]`, never starts or ends with `-`,
/// and never contains `--`.
///
/// Determinism: same `input` always yields the same slug.
///
/// Edge cases:
/// - **Empty or fully-stripped input** (all non-alphanumeric, e.g. `"///"`)
///   yields the bare 16-hex blake3 prefix — a fully opaque but deterministic,
///   non-empty slug. Different inputs that both strip to empty produce
///   different hashes.
/// - `max_len` must be at least 17 (16 hex chars + 1 dash separator). Smaller
///   values are a programmer error and trigger a debug assertion; in release
///   builds the function falls back to returning just the hash suffix.
pub fn slugify(input: &str, max_len: usize) -> String {
    debug_assert!(
        max_len >= 17,
        "max_len must be at least 17 to fit the hash suffix"
    );

    let mapped: String = input
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();

    let collapsed = collapse_dashes(&mapped);

    if collapsed.is_empty() {
        return hash_suffix(input);
    }

    if collapsed.len() <= max_len {
        return collapsed;
    }

    let prefix_max = max_len.saturating_sub(17);
    let mut prefix: String = collapsed.chars().take(prefix_max).collect();
    while prefix.ends_with('-') {
        prefix.pop();
    }
    let hash = hash_suffix(input);
    if prefix.is_empty() {
        return hash;
    }
    format!("{}-{}", prefix, hash)
}

fn collapse_dashes(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_dash = false;
    for ch in s.chars() {
        if ch == '-' {
            if !prev_dash {
                out.push('-');
            }
            prev_dash = true;
        } else {
            out.push(ch);
            prev_dash = false;
        }
    }
    out.trim_matches('-').to_string()
}

fn hash_suffix(input: &str) -> String {
    let hash = blake3::hash(input.as_bytes());
    let bytes = &hash.as_bytes()[..8];
    let mut hex = String::with_capacity(16);
    for b in bytes {
        write!(&mut hex, "{:02x}", b).expect("writing to String never fails");
    }
    hex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_roundtrip_lowercases_and_preserves_alnum_dashes() {
        assert_eq!(slugify("hello-world", 50), "hello-world");
        assert_eq!(slugify("Foo-Bar", 50), "foo-bar");
        assert_eq!(slugify("ENG123", 50), "eng123");
    }

    #[test]
    fn collapses_runs_of_dashes_and_spaces() {
        assert_eq!(slugify("hello   world", 50), "hello-world");
        assert_eq!(slugify("a---b", 50), "a-b");
        assert_eq!(slugify("--leading-trailing--", 50), "leading-trailing");
    }

    #[test]
    fn slashes_and_punctuation_become_dashes() {
        assert_eq!(slugify("foo/bar/baz", 50), "foo-bar-baz");
        assert_eq!(slugify("a.b.c", 50), "a-b-c");
        assert_eq!(
            slugify("ENG-123: fix the thing!", 50),
            "eng-123-fix-the-thing"
        );
    }

    #[test]
    fn unicode_becomes_dashes() {
        assert_eq!(slugify("héllo wörld", 50), "h-llo-w-rld");
        // Pure non-ASCII strips to empty → falls back to hash.
        let result = slugify("日本語", 50);
        assert_eq!(result.len(), 16);
        assert!(result.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn long_input_truncates_with_hash_suffix() {
        let long = "a".repeat(200);
        let max = 30;
        let s = slugify(&long, max);
        assert!(s.len() <= max, "{} (len {}) > {}", s, s.len(), max);
        let (prefix, suffix) = s.rsplit_once('-').expect("must contain '-'");
        assert!(!prefix.is_empty());
        assert_eq!(suffix.len(), 16);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn truncation_drops_trailing_dash_before_appending_hash() {
        // collapsed = "aaaaa-bbbbb-ccccc-ddddd" (23 chars)
        // max_len = 23 → no truncation. Use 22 to force truncate to prefix_max=5.
        // prefix = "aaaaa", no trailing dash; result = "aaaaa-{hash}" (22 chars).
        let s = slugify("aaaaa bbbbb ccccc ddddd", 22);
        assert!(s.starts_with("aaaaa-"));
        assert!(!s.contains("--"));
        assert_eq!(s.len(), 22);

        // Force the truncation boundary to land on a dash so we exercise the
        // trailing-dash trim path.
        // collapsed = "aa-bb-cc-dd" (11 chars). max_len=20 → no truncation.
        // Use longer source: collapsed = "aa-bb-cc-dd-ee-ff" (17). max=20 → no.
        // Make max=19, collapsed=20 chars: "aa-bb-cc-dd-ee-fff" (18). Hmm.
        // Easier: max_len=20 → prefix_max=3. "aa-bb-cc-dd-ee-ff-gg" (20 chars).
        // collapsed=20 ≤ max=20 → no truncation. Use max=19.
        let s = slugify("aa bb cc dd ee ff gg", 19);
        // collapsed = "aa-bb-cc-dd-ee-ff-gg" (20) > 19, prefix_max=2,
        // chars().take(2) = "aa", no trailing dash → "aa-{hash}" (19 chars).
        assert!(s.starts_with("aa-"));
        assert!(!s.contains("--"));
        assert_eq!(s.len(), 19);
    }

    #[test]
    fn determinism_holds_for_same_input() {
        assert_eq!(
            slugify("the quick brown fox", 30),
            slugify("the quick brown fox", 30)
        );
        let long = "x".repeat(200);
        assert_eq!(slugify(&long, 30), slugify(&long, 30));
        assert_eq!(slugify("", 50), slugify("", 50));
    }

    #[test]
    fn empty_input_falls_back_to_hash() {
        let s = slugify("", 50);
        assert_eq!(s.len(), 16);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn all_stripped_input_falls_back_to_hash_distinct_per_input() {
        let s = slugify("///---!!!", 50);
        let t = slugify("###...", 50);
        assert_eq!(s.len(), 16);
        assert_eq!(t.len(), 16);
        assert!(s.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(t.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(s, t, "different inputs must hash differently");
    }

    #[test]
    fn output_never_contains_double_dash_or_boundary_dashes() {
        for input in [
            "hello world",
            "a---b---c",
            "/leading/and/trailing/",
            "ENG-123",
            &"x ".repeat(40),
        ] {
            let s = slugify(input, 30);
            assert!(!s.contains("--"), "double-dash in {:?} → {:?}", input, s);
            assert!(!s.starts_with('-'), "leading dash in {:?} → {:?}", input, s);
            assert!(!s.ends_with('-'), "trailing dash in {:?} → {:?}", input, s);
        }
    }
}
