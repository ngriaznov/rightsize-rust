//! Container reuse: a `.reuse(true)`-marked container survives process exit and is
//! ADOPTED by a later, equivalent `start()` instead of being re-created — see
//! [`crate::Container::reuse`] and `Container::start`'s own wiring for the full
//! adopt/create/stop flow. This module owns the parts of that flow that are pure
//! I/O/hashing and don't need [`crate::ContainerGuard`]'s private fields: the double
//! opt-in env gate, the canonical cross-language identity hash, and the
//! `<cacheDir>/reuse/<hash>.json` registry file. `crate::container` owns the
//! orchestration itself (registry lookup -> adopt-or-create -> guard construction),
//! since only that module can build a `ContainerGuard` directly.
//!
//! **Double opt-in**: reuse only activates when BOTH `Container::reuse(true)` was
//! called AND `RIGHTSIZE_REUSE` is exactly `"true"` or `"1"` in the real process
//! environment — an API-marked-but-env-disabled container runs as an ordinary
//! ephemeral one (Testcontainers semantics), with a single stderr note. See
//! [`env_parse`].
//!
//! **Identity**: `sha256` over a canonical (stable key order, no whitespace) JSON
//! serialization of `{image, env (sorted by key), command, exposedPorts (sorted),
//! memoryLimitMb, copies: [{guestPath, sha256(content)}] sorted by guestPath}` — this
//! exact shape and key order is a CROSS-LANGUAGE CONTRACT: the same logical spec
//! must hash identically in the Kotlin/Node/Rust implementations, pinned by a fixed
//! vector (see the `pinned_cross_language_vector_hashes_to_the_pinned_value` test
//! below). The sandbox name is `rz-reuse-<first 12 hex chars of the hash>`. See
//! [`compute_identity`].

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Result, RightsizeError};
use crate::model::FileMount;

/// Parses `RIGHTSIZE_REUSE`'s value: reuse is enabled by exactly `"true"` or `"1"`
/// (case-sensitive, per the cross-language contract's exact-string requirement —
/// unlike `RIGHTSIZE_REAPER`'s case-insensitive `on`/`sweep`/`off`, this one has no
/// "unknown value falls back to a default" case: anything else means disabled).
pub(crate) fn env_parse(value: Option<&str>) -> bool {
    matches!(value, Some("true") | Some("1"))
}

/// Reads `RIGHTSIZE_REUSE` from the real process environment.
pub(crate) fn env_enabled() -> bool {
    env_parse(std::env::var("RIGHTSIZE_REUSE").ok().as_deref())
}

/// A reuse container's derived identity: the full 64-hex-char sha256 (also the
/// registry file's own name, `<hash>.json`) and the `rz-reuse-<12hex>` sandbox name
/// derived from its first 12 characters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identity {
    /// The full lowercase-hex sha256 digest of the canonical spec JSON.
    pub hash_hex: String,
    /// `rz-reuse-<first 12 hex chars of hash_hex>`.
    pub name: String,
}

/// Computes a reuse container's [`Identity`] from the reuse-relevant subset of its
/// spec. Reads every mount's host-side file content to hash it (`copies`) — a
/// genuine I/O error here (an unreadable mount) is propagated rather than swallowed,
/// since the same file would fail the container's own start moments later anyway.
pub(crate) fn compute_identity(
    image: &str,
    env: &[(String, String)],
    command: &Option<Vec<String>>,
    exposed_ports: &[u16],
    memory_limit_mb: Option<u64>,
    mounts: &[FileMount],
) -> Result<Identity> {
    let mut env_sorted = env.to_vec();
    env_sorted.sort_by(|a, b| a.0.cmp(&b.0));

    let mut ports_sorted = exposed_ports.to_vec();
    ports_sorted.sort_unstable();

    let mut copies = Vec::with_capacity(mounts.len());
    for mount in mounts {
        let content = fs::read(&mount.host_path)?;
        copies.push((mount.guest_path.clone(), hex_sha256(&content)));
    }
    copies.sort_by(|a, b| a.0.cmp(&b.0));

    let canonical = canonical_json(
        image,
        &env_sorted,
        command.as_deref().unwrap_or(&[]),
        &ports_sorted,
        memory_limit_mb,
        &copies,
    );
    let hash_hex = hex_sha256(canonical.as_bytes());
    let name = format!("rz-reuse-{}", &hash_hex[..12]);
    Ok(Identity { hash_hex, name })
}

/// Builds the exact canonical JSON string the identity hash is computed over —
/// hand-assembled (not a `serde_json::Map`, whose default `BTreeMap` backing would
/// sort keys alphabetically instead of the spec's fixed order) so the top-level key
/// order (`image`, `env`, `command`, `exposedPorts`, `memoryLimitMb`, `copies`)
/// matches the cross-language contract exactly, with every scalar/string
/// individually JSON-encoded via `serde_json::to_string` for correct escaping. No
/// whitespace anywhere — that's the "canonical" half of the contract.
fn canonical_json(
    image: &str,
    env_sorted: &[(String, String)],
    command: &[String],
    ports_sorted: &[u16],
    memory_limit_mb: Option<u64>,
    copies_sorted: &[(String, String)],
) -> String {
    let mut out = String::new();
    out.push_str("{\"image\":");
    out.push_str(&json_string(image));

    out.push_str(",\"env\":{");
    for (i, (k, v)) in env_sorted.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&json_string(k));
        out.push(':');
        out.push_str(&json_string(v));
    }
    out.push('}');

    out.push_str(",\"command\":[");
    for (i, c) in command.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&json_string(c));
    }
    out.push(']');

    out.push_str(",\"exposedPorts\":[");
    for (i, p) in ports_sorted.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&p.to_string());
    }
    out.push(']');

    out.push_str(",\"memoryLimitMb\":");
    match memory_limit_mb {
        Some(mb) => out.push_str(&mb.to_string()),
        None => out.push_str("null"),
    }

    out.push_str(",\"copies\":[");
    for (i, (guest_path, sha)) in copies_sorted.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"guestPath\":");
        out.push_str(&json_string(guest_path));
        out.push_str(",\"sha256\":");
        out.push_str(&json_string(sha));
        out.push('}');
    }
    out.push_str("]}");
    out
}

/// JSON-encodes a single string value (quoting + escaping) via `serde_json`, so this
/// module never hand-rolls escaping rules.
fn json_string(s: &str) -> String {
    serde_json::to_string(s).expect("a &str always serializes")
}

/// Lowercase-hex sha256 of `bytes`. Same `fold`+`write!` shape as
/// `rightsize_msb::provisioner`'s own `hex_sha256` (clippy's `format_collect` lint
/// rejects the more obvious `map(|b| format!(...)).collect()`).
fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    digest.iter().fold(String::new(), |mut hex, b| {
        let _ = write!(hex, "{b:02x}");
        hex
    })
}

/// `<cacheDir>/reuse/<hash>.json`'s payload — the cross-language contract's exact
/// field names (`camelCase`, since Kotlin/Node write and read this same file shape).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RegistryEntry {
    /// The reuse sandbox's name (`rz-reuse-<12hex>`).
    pub name: String,
    /// The image reference it was created from.
    pub image: String,
    /// Guest port (as a string — JSON object keys are always strings) -> host port.
    pub ports: BTreeMap<String, u16>,
    /// ISO-8601 UTC instant this entry was written.
    #[serde(rename = "createdIso")]
    pub created_iso: String,
    /// The backend's registered name (e.g. `"microsandbox"`, `"docker"`) that
    /// created it.
    pub backend: String,
}

/// `<cacheDir>/reuse/<hash>.json` for one reuse identity.
pub(crate) struct Registry {
    path: PathBuf,
}

impl Registry {
    /// Builds a registry handle for `hash_hex`'s identity. Does not itself create
    /// the directory or any file.
    pub(crate) fn new(cache_dir: &Path, hash_hex: &str) -> Self {
        Registry {
            path: cache_dir.join("reuse").join(format!("{hash_hex}.json")),
        }
    }

    /// True if a registry file exists at all — distinguishes "no registry" (nothing
    /// to remove, go straight to a fresh create) from "registry exists but is
    /// corrupt" (best-effort removal of the identity-derived name first — see
    /// `crate::container`'s adopt orchestration, which is the only reason this is
    /// a separate method from [`Self::read`]).
    pub(crate) fn exists(&self) -> bool {
        self.path.exists()
    }

    /// Reads and parses the registry file. `None` for a missing OR unparseable file
    /// — both are the caller's "not adoptable" case; [`Self::exists`] is what
    /// distinguishes them when that matters.
    pub(crate) fn read(&self) -> Option<RegistryEntry> {
        let raw = fs::read(&self.path).ok()?;
        serde_json::from_slice(&raw).ok()
    }

    /// Writes the registry file atomically (temp file + rename), creating
    /// `<cacheDir>/reuse/` if needed. Called only after the reused container's own
    /// wait strategy has already succeeded.
    pub(crate) fn write_atomic(&self, entry: &RegistryEntry) -> std::io::Result<()> {
        let dir = self
            .path
            .parent()
            .expect("registry path always has a parent (cache_dir/reuse/)");
        fs::create_dir_all(dir)?;
        let json =
            serde_json::to_vec_pretty(entry).expect("RegistryEntry has no non-serializable fields");
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&json)?;
        }
        fs::rename(&tmp, &self.path)
    }

    /// Best-effort deletes the registry file. Safe to call more than once.
    pub(crate) fn delete(&self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// True if `e` represents a sandbox-name collision on create (another process won
/// the reuse-create race) — the reuse start flow's cue to re-enter the adopt path
/// once. Prefers the typed [`RightsizeError::NameConflict`], walking the `source`
/// chain; falls back to matching "already exists" (case-insensitive) for a backend
/// that doesn't throw the typed variant. Mirrors
/// `crate::container::is_port_bind_conflict`'s typed-first/string-fallback shape.
pub(crate) fn is_name_conflict(e: &RightsizeError) -> bool {
    let mut current: Option<&RightsizeError> = Some(e);
    while let Some(err) = current {
        if matches!(err, RightsizeError::NameConflict { .. }) {
            return true;
        }
        if err.to_string().to_lowercase().contains("already exists") {
            return true;
        }
        current = match err {
            RightsizeError::NameConflict {
                source: Some(s), ..
            } => Some(s.as_ref()),
            _ => None,
        };
    }
    false
}

/// The current UTC instant as `YYYY-MM-DDTHH:MM:SSZ` — a general-purpose "now" ISO-
/// 8601 formatter using pure integer arithmetic (Howard Hinnant's `civil_from_days`,
/// public domain), needing no OS call and no `#[cfg(windows)]` split. Unlike
/// `crate::reaper::liveness`'s formatter (unix-only there because it's paired with
/// `ps`-based probing of ANOTHER process's start time), this one only ever asks for
/// THIS process's own clock, which `SystemTime::now()` already gives on every
/// platform — small deliberate duplication of the calendar math rather than reaching
/// into a sibling module's private, differently-scoped helper.
pub(crate) fn now_iso8601() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let hh = rem / 3600;
    let mi = (rem % 3600) / 60;
    let ss = rem % 60;
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mi:02}:{ss:02}Z")
}

/// Howard Hinnant's `civil_from_days` (public domain): the proleptic-Gregorian
/// `(year, month, day)` for `days` days since the Unix epoch.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- env gate ----------------------------------------------------------

    #[test]
    fn env_parse_accepts_only_the_exact_strings_true_or_1() {
        assert!(env_parse(Some("true")));
        assert!(env_parse(Some("1")));
        assert!(!env_parse(Some("TRUE")));
        assert!(!env_parse(Some("yes")));
        assert!(!env_parse(Some("on")));
        assert!(!env_parse(Some("")));
        assert!(!env_parse(None));
    }

    // -- canonical hash / pinned cross-language vector ----------------------

    /// THE cross-language contract vector: the exact spec from the reuse feature
    /// spec's "Identity" section must hash to this exact sha256, identically in the
    /// Kotlin/Node/Rust implementations. Computed once (sha256 of the canonical
    /// JSON below) and pinned here so a change to `canonical_json`'s shape that
    /// breaks cross-language parity fails loudly.
    ///
    /// Canonical JSON this hashes:
    /// `{"image":"redis:7-alpine","env":{"A":"1","B":"2"},"command":[],"exposedPorts":[6379],"memoryLimitMb":null,"copies":[]}`
    const PINNED_VECTOR_HASH: &str =
        "799aad5a3338ce3d36999c7ff2733d4673c0592d417563f334544693ec1907a5";

    fn pinned_vector_identity() -> Identity {
        compute_identity(
            "redis:7-alpine",
            &[
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "2".to_string()),
            ],
            &None,
            &[6379],
            None,
            &[],
        )
        .unwrap()
    }

    #[test]
    fn pinned_cross_language_vector_hashes_to_the_pinned_value() {
        let identity = pinned_vector_identity();
        assert_eq!(identity.hash_hex, PINNED_VECTOR_HASH, "{identity:?}");
        assert_eq!(
            identity.name,
            format!("rz-reuse-{}", &PINNED_VECTOR_HASH[..12])
        );
    }

    #[test]
    fn canonical_json_matches_the_pinned_shape_exactly() {
        let json = canonical_json(
            "redis:7-alpine",
            &[
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "2".to_string()),
            ],
            &[],
            &[6379],
            None,
            &[],
        );
        assert_eq!(
            json,
            r#"{"image":"redis:7-alpine","env":{"A":"1","B":"2"},"command":[],"exposedPorts":[6379],"memoryLimitMb":null,"copies":[]}"#
        );
    }

    #[test]
    fn env_key_order_at_the_call_site_does_not_affect_the_hash() {
        let a = compute_identity(
            "redis:7-alpine",
            &[
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "2".to_string()),
            ],
            &None,
            &[6379],
            None,
            &[],
        )
        .unwrap();
        let b = compute_identity(
            "redis:7-alpine",
            &[
                ("B".to_string(), "2".to_string()),
                ("A".to_string(), "1".to_string()),
            ],
            &None,
            &[6379],
            None,
            &[],
        )
        .unwrap();
        assert_eq!(
            a.hash_hex, b.hash_hex,
            "env insertion order must not matter"
        );
    }

    #[test]
    fn a_different_image_changes_the_hash() {
        let a = pinned_vector_identity();
        let b = compute_identity(
            "redis:7",
            &[
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "2".to_string()),
            ],
            &None,
            &[6379],
            None,
            &[],
        )
        .unwrap();
        assert_ne!(a.hash_hex, b.hash_hex);
    }

    #[test]
    fn copy_content_change_changes_the_hash() {
        let dir = std::env::temp_dir().join(format!(
            "rz-reuse-identity-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("f.txt");

        std::fs::write(&file, b"one").unwrap();
        let mount = FileMount::new(&file, "/guest/f.txt");
        let a = compute_identity(
            "redis:7-alpine",
            &[],
            &None,
            &[],
            None,
            std::slice::from_ref(&mount),
        )
        .unwrap();

        std::fs::write(&file, b"two").unwrap();
        let b = compute_identity(
            "redis:7-alpine",
            &[],
            &None,
            &[],
            None,
            std::slice::from_ref(&mount),
        )
        .unwrap();

        assert_ne!(a.hash_hex, b.hash_hex);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compute_identity_propagates_an_unreadable_mount_as_a_real_error() {
        let mount = FileMount::new("/definitely/does/not/exist/rightsize-reuse", "/guest/f.txt");
        let err = compute_identity("redis:7-alpine", &[], &None, &[], None, &[mount]).unwrap_err();
        // std::io::Error via RightsizeError::Io — just prove it propagates rather
        // than silently hashing to a wrong/empty value.
        assert!(matches!(err, RightsizeError::Io(_)), "{err}");
    }

    // -- registry -------------------------------------------------------------

    fn temp_cache_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rz-reuse-registry-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_entry() -> RegistryEntry {
        RegistryEntry {
            name: "rz-reuse-799aad5a3338".to_string(),
            image: "redis:7-alpine".to_string(),
            ports: BTreeMap::from([("6379".to_string(), 32768)]),
            created_iso: "2025-01-01T00:00:00Z".to_string(),
            backend: "docker".to_string(),
        }
    }

    #[test]
    fn write_then_read_round_trips() {
        let cache = temp_cache_dir("round-trip");
        let registry = Registry::new(&cache, "deadbeef");
        registry.write_atomic(&sample_entry()).unwrap();
        assert!(registry.exists());
        assert_eq!(registry.read(), Some(sample_entry()));
    }

    #[test]
    fn missing_registry_neither_exists_nor_reads() {
        let cache = temp_cache_dir("missing");
        let registry = Registry::new(&cache, "deadbeef");
        assert!(!registry.exists());
        assert!(registry.read().is_none());
    }

    #[test]
    fn corrupt_registry_exists_but_does_not_parse() {
        let cache = temp_cache_dir("corrupt");
        let registry = Registry::new(&cache, "deadbeef");
        std::fs::create_dir_all(cache.join("reuse")).unwrap();
        std::fs::write(cache.join("reuse").join("deadbeef.json"), b"not json").unwrap();
        assert!(registry.exists(), "a corrupt file still exists on disk");
        assert!(registry.read().is_none(), "but it must not parse");
    }

    #[test]
    fn delete_is_idempotent_and_best_effort() {
        let cache = temp_cache_dir("delete");
        let registry = Registry::new(&cache, "deadbeef");
        registry.write_atomic(&sample_entry()).unwrap();
        registry.delete();
        assert!(!registry.exists());
        registry.delete(); // second call: must not panic.
    }

    #[test]
    fn registry_json_uses_the_camel_case_created_iso_field_name() {
        let cache = temp_cache_dir("camel-case");
        let registry = Registry::new(&cache, "deadbeef");
        registry.write_atomic(&sample_entry()).unwrap();
        let raw = std::fs::read_to_string(cache.join("reuse").join("deadbeef.json")).unwrap();
        assert!(raw.contains("\"createdIso\""), "{raw}");
        assert!(!raw.contains("created_iso"), "{raw}");
    }

    #[test]
    fn write_atomic_overwrites_an_existing_entry() {
        let cache = temp_cache_dir("overwrite");
        let registry = Registry::new(&cache, "deadbeef");
        registry.write_atomic(&sample_entry()).unwrap();
        let mut updated = sample_entry();
        updated.ports.insert("9999".to_string(), 40000);
        registry.write_atomic(&updated).unwrap();
        assert_eq!(registry.read(), Some(updated));
    }

    // -- name conflict classifier ---------------------------------------------

    #[test]
    fn is_name_conflict_matches_the_typed_variant() {
        let e = RightsizeError::NameConflict {
            message: "boom".to_string(),
            source: None,
        };
        assert!(is_name_conflict(&e));
    }

    #[test]
    fn is_name_conflict_matches_the_typed_variant_nested_under_itself() {
        let e = RightsizeError::NameConflict {
            message: "outer".to_string(),
            source: Some(Box::new(RightsizeError::NameConflict {
                message: "sandbox 'rz-reuse-abc' already exists".to_string(),
                source: None,
            })),
        };
        assert!(is_name_conflict(&e));
    }

    #[test]
    fn is_name_conflict_matches_an_already_exists_message_fallback() {
        let e = RightsizeError::Backend("sandbox 'rz-reuse-abc' already exists".to_string());
        assert!(is_name_conflict(&e));
    }

    #[test]
    fn is_name_conflict_negative_case_does_not_match() {
        let e = RightsizeError::Backend("connection refused".to_string());
        assert!(!is_name_conflict(&e));
    }

    // -- now_iso8601 -----------------------------------------------------------

    #[test]
    fn now_iso8601_has_the_expected_shape() {
        let iso = now_iso8601();
        assert_eq!(iso.len(), 20, "{iso}");
        assert!(iso.ends_with('Z'), "{iso}");
        assert_eq!(iso.as_bytes()[4], b'-');
        assert_eq!(iso.as_bytes()[7], b'-');
        assert_eq!(iso.as_bytes()[10], b'T');
        assert_eq!(iso.as_bytes()[13], b':');
        assert_eq!(iso.as_bytes()[16], b':');
    }
}
