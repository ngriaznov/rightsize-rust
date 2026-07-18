//! Parses `msb snapshot list --format json`, this backend's only way to confirm an
//! imported checkpoint's DIGEST-DIR NAME is actually registered (see
//! `MsbCliBackend::import_checkpoint`).
//!
//! Per the verified contract: `msb snapshot import <archive>` unpacks under a
//! DIGEST-DERIVED directory name (e.g. `sha256-b9c0448ee9d54e33`) that is NOT the
//! full digest and does NOT preserve the archive's original snapshot name. The
//! full `sha256:<64hex>` digest does NOT resolve as a snapshot ref at all —
//! `msb snapshot inspect sha256:<full>` fails "snapshot not found" (msb treats it
//! as a literal path) — only the digest-dir name resolves for `inspect`, `rm`, and
//! `run --snapshot`. So the effective ref this backend must return is the
//! digest-dir name itself; this module's job is only to confirm it via `msb
//! snapshot list --format json` (matching it against an entry's `name` or
//! `artifact_path`) and hand it back unchanged.
//!
//! Same tolerant-parse posture as `crate::ls_json`: an entry missing a field this
//! module reads is skipped for that field's purposes rather than failing the whole
//! parse, and a `json` that doesn't parse as an array at all yields no match rather
//! than propagating a parse error — `MsbCliBackend::import_checkpoint` turns "no
//! match found" into its own actionable error, naming the digest-dir name it was
//! looking for.

use serde::Deserialize;

/// One entry of `msb snapshot list --format json`'s output — only the two fields
/// this module reads. Anything else msb's real output carries (`digest`,
/// `image_ref`, etc.) is ignored by `serde_json`'s default "unknown fields are
/// dropped" behavior.
#[derive(Deserialize, Default)]
struct SnapshotEntry {
    name: Option<String>,
    artifact_path: Option<String>,
}

/// Confirms `digest_dir_name` (the basename of the path `msb snapshot import`
/// printed — see `MsbCliBackend::import_checkpoint`) is present in `msb snapshot
/// list --format json`'s output, matching it against an entry's `name` (an exact
/// match) or `artifact_path` (matched as one of the path's own components — the
/// entry's artifact can sit directly under a directory named `digest_dir_name`, or
/// be a file inside one). Returns `digest_dir_name` itself, unchanged, on a match
/// — never an entry's `digest` field, which msb does not accept as a snapshot ref
/// (see module docs). `None` when `json` doesn't parse, or no entry matches.
pub(crate) fn confirm_digest_dir_name(json: &str, digest_dir_name: &str) -> Option<String> {
    let entries: Vec<SnapshotEntry> = serde_json::from_str(json).ok()?;
    let found = entries.into_iter().any(|entry| {
        let name_matches = entry.name.as_deref() == Some(digest_dir_name);
        let path_matches = entry
            .artifact_path
            .as_deref()
            .is_some_and(|p| path_has_component(p, digest_dir_name));
        name_matches || path_matches
    });
    found.then(|| digest_dir_name.to_string())
}

/// True if any component of `path` is exactly `component` — covers both an
/// `artifact_path` that IS the digest directory itself and one that's a file
/// nested inside it, without caring which OS path separator msb printed.
fn path_has_component(path: &str, component: &str) -> bool {
    std::path::Path::new(path)
        .components()
        .any(|c| c.as_os_str() == component)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_by_exact_name_and_returns_the_digest_dir_name_unchanged() {
        let json = r#"[{"digest":"sha256:full64hexdigest","name":"sha256-b9c0448ee9d54e33","artifact_path":"/home/u/.microsandbox/snapshots/sha256-b9c0448ee9d54e33"}]"#;
        assert_eq!(
            confirm_digest_dir_name(json, "sha256-b9c0448ee9d54e33"),
            Some("sha256-b9c0448ee9d54e33".to_string())
        );
    }

    #[test]
    fn matches_when_the_digest_dir_is_a_path_component_of_artifact_path() {
        let json = r#"[{"digest":"sha256:full64hexdigest","name":"some-other-name","artifact_path":"/home/u/.microsandbox/snapshots/sha256-b9c0448ee9d54e33/artifact.tar.zst"}]"#;
        assert_eq!(
            confirm_digest_dir_name(json, "sha256-b9c0448ee9d54e33"),
            Some("sha256-b9c0448ee9d54e33".to_string())
        );
    }

    #[test]
    fn picks_the_matching_entry_among_several() {
        let json = r#"[
            {"digest":"sha256:aaa","name":"sha256-aaa","artifact_path":"/x/sha256-aaa"},
            {"digest":"sha256:bbb","name":"sha256-bbb","artifact_path":"/x/sha256-bbb"}
        ]"#;
        assert_eq!(
            confirm_digest_dir_name(json, "sha256-bbb"),
            Some("sha256-bbb".to_string())
        );
    }

    #[test]
    fn no_match_yields_none() {
        let json =
            r#"[{"digest":"sha256:aaa","name":"sha256-aaa","artifact_path":"/x/sha256-aaa"}]"#;
        assert_eq!(confirm_digest_dir_name(json, "sha256-zzz"), None);
    }

    #[test]
    fn empty_array_yields_none() {
        assert_eq!(confirm_digest_dir_name("[]", "sha256-aaa"), None);
    }

    #[test]
    fn malformed_json_yields_none_rather_than_panicking() {
        assert_eq!(
            confirm_digest_dir_name("not json at all", "sha256-aaa"),
            None
        );
    }

    #[test]
    fn unknown_extra_fields_are_ignored() {
        let json = r#"[{"digest":"sha256:aaa","name":"sha256-aaa","artifact_path":"/x/sha256-aaa","image_ref":"floci/floci:1.5.30","created_at":"t"}]"#;
        assert_eq!(
            confirm_digest_dir_name(json, "sha256-aaa"),
            Some("sha256-aaa".to_string())
        );
    }

    #[test]
    fn an_entry_missing_both_matchable_fields_never_matches() {
        let json = r#"[{"digest":"sha256:aaa"}]"#;
        assert_eq!(confirm_digest_dir_name(json, "sha256-aaa"), None);
    }
}
