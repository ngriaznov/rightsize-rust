//! Parses `msb ls --format json`, the msb backend's only way to learn which sandboxes
//! are currently `Running`.
//!
//! msb 0.6.2's shape is a flat JSON array of objects with keys `created_at`, `image`,
//! `name`, `status` (status capitalized, e.g. `"Running"`). This deserializes with
//! `serde_json` into a struct carrying only `name`/`status` — both typed `Option`, so
//! an object missing one (or a whole object shape a future msb release changes
//! further) deserializes to `None` for the missing field rather than failing that
//! entry outright; unrecognized extra fields are ignored by default. An entry missing
//! `name` or `status` is then filtered out in [`running_names`] and treated as "not
//! Running" rather than propagated, since one malformed record from `msb ls` should
//! never make readiness-polling itself error out.
//!
//! **Pin note:** the real-CLI shape is guarded by an integration test
//! (`running_sandbox_names` against a live `msb ls`) — this module's own tests exercise
//! this parse against the documented shape and its edge cases (extra fields, a missing
//! key, non-`Running` statuses), which a live-CLI test can't easily probe without
//! hand-crafting msb output itself.

use std::collections::HashSet;

use serde::Deserialize;

/// One entry of `msb ls --format json`'s output — only the two fields this backend
/// reads. `created_at`/`image` (and anything a future msb version adds) are ignored.
/// Both fields are `Option`, not defaulted to an empty string: an object missing
/// either one must be skipped outright, not treated as an entry named `""`.
#[derive(Deserialize, Default)]
struct LsEntry {
    name: Option<String>,
    status: Option<String>,
}

/// Returns the `name` of every object in `json` whose `status` field is exactly
/// `"Running"` (capitalized, per msb's own casing — not ours to normalize). An object
/// missing either `name` or `status` is skipped, not counted.
///
/// A `json` that fails to parse as an array at all (msb printed something other than
/// its documented shape) yields an empty set rather than an error — the same
/// best-effort posture the hand-rolled parser this replaced had, since callers treat
/// "no sandboxes found Running yet" as an ordinary polling outcome, not a hard failure.
pub(crate) fn running_names(json: &str) -> HashSet<String> {
    let entries: Vec<LsEntry> = serde_json::from_str(json).unwrap_or_default();
    entries
        .into_iter()
        .filter_map(|e| match (e.name, e.status) {
            (Some(name), Some(status)) if status == "Running" => Some(name),
            _ => None,
        })
        .collect()
}

/// Returns whether `json`'s entry named `name` has `status` exactly equal to
/// `wanted` (msb's own casing, e.g. `"Stopped"`) — the msb backend's fast-exit
/// post-mortem classification uses this to confirm a sandbox that exited before
/// `Running` was observed actually finished cleanly rather than dying mid-boot.
///
/// The same tolerant-failure posture as [`running_names`]: no entry named `name`, an
/// entry missing `name` or `status`, or a `json` that fails to parse as an array at
/// all, all yield `false` rather than an error — a malformed/absent `msb ls` result
/// must read as "not confirmed Stopped", never crash the classification.
pub(crate) fn status_is(json: &str, name: &str, wanted: &str) -> bool {
    let entries: Vec<LsEntry> = serde_json::from_str(json).unwrap_or_default();
    entries.into_iter().any(|e| match (e.name, e.status) {
        (Some(n), Some(s)) => n == name && s == wanted,
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(json: &str) -> HashSet<String> {
        running_names(json)
    }

    #[test]
    fn parses_the_documented_flat_shape_keys_in_spec_order() {
        let json = r#"[
              {"created_at":"2024-01-01T00:00:00Z","image":"alpine:3.19","name":"rz-abc-1","status":"Running"},
              {"created_at":"2024-01-01T00:00:01Z","image":"alpine:3.19","name":"rz-abc-2","status":"Stopped"}
            ]"#;
        assert_eq!(names(json), ["rz-abc-1".to_string()].into());
    }

    #[test]
    fn key_order_within_an_object_does_not_matter() {
        let json = r#"[{"status":"Running","name":"rz-xyz-1","image":"x","created_at":"t"}]"#;
        assert_eq!(names(json), ["rz-xyz-1".to_string()].into());
    }

    #[test]
    fn extra_unknown_fields_are_ignored() {
        let json = r#"[{"name":"rz-1","status":"Running","cpu_percent":12.5,"labels":{"a":"b"}}]"#;
        assert_eq!(names(json), ["rz-1".to_string()].into());
    }

    #[test]
    fn object_missing_status_or_name_is_skipped_not_thrown() {
        let json = r#"[
              {"name":"rz-no-status"},
              {"status":"Running"},
              {"name":"rz-both","status":"Running"}
            ]"#;
        assert_eq!(names(json), ["rz-both".to_string()].into());
    }

    #[test]
    fn non_running_statuses_are_excluded() {
        let json = r#"[{"name":"a","status":"Stopped"},{"name":"b","status":"running"},{"name":"c","status":"Running"}]"#;
        assert_eq!(names(json), ["c".to_string()].into());
    }

    #[test]
    fn empty_array_yields_empty_set() {
        assert_eq!(names("[]"), HashSet::new());
    }

    #[test]
    fn malformed_json_yields_empty_set_rather_than_panicking() {
        assert_eq!(names("not json at all"), HashSet::new());
    }

    #[test]
    fn braces_and_colons_inside_string_values_do_not_confuse_parsing() {
        let json = r#"[{"name":"rz-brace","status":"Running","image":"repo/{tag}:v1"},{"name":"rz-2","status":"Running"}]"#;
        assert_eq!(
            names(json),
            ["rz-brace".to_string(), "rz-2".to_string()].into()
        );
    }

    #[test]
    fn escaped_quote_inside_a_string_value_does_not_break_parsing() {
        let json = r#"[{"name":"rz-esc","status":"Running","image":"a\"b"},{"name":"rz-after","status":"Running"}]"#;
        assert_eq!(
            names(json),
            ["rz-esc".to_string(), "rz-after".to_string()].into()
        );
    }

    #[test]
    fn nested_object_values_do_not_throw_off_name_status_extraction() {
        let json = r#"[{"name":"rz-nested","status":"Running","meta":{"nested":"{not a name}"}}]"#;
        assert_eq!(names(json), ["rz-nested".to_string()].into());
    }

    #[test]
    fn no_running_sandboxes_at_all_yields_empty_set() {
        let json = r#"[{"name":"a","status":"Stopped"}]"#;
        assert!(names(json).is_empty());
    }

    #[test]
    fn status_is_matches_the_named_entrys_exact_status() {
        let json = r#"[
              {"name":"rz-a","status":"Stopped"},
              {"name":"rz-b","status":"Running"}
            ]"#;
        assert!(status_is(json, "rz-a", "Stopped"));
        assert!(!status_is(json, "rz-a", "Running"));
        assert!(status_is(json, "rz-b", "Running"));
    }

    #[test]
    fn status_is_false_when_the_name_is_not_present_at_all() {
        let json = r#"[{"name":"rz-a","status":"Stopped"}]"#;
        assert!(!status_is(json, "rz-does-not-exist", "Stopped"));
    }

    #[test]
    fn status_is_false_when_the_entry_is_missing_name_or_status() {
        let json = r#"[{"name":"rz-a"},{"status":"Stopped"}]"#;
        assert!(!status_is(json, "rz-a", "Stopped"));
    }

    #[test]
    fn status_is_false_on_malformed_json_rather_than_panicking() {
        assert!(!status_is("not json at all", "rz-a", "Stopped"));
    }
}
