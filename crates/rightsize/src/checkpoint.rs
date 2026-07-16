//! Checkpoint/restore: `ContainerGuard::checkpoint()`/`checkpoint_named()` and
//! `Container::from_checkpoint` (see `crate::container`, which owns all three, plus
//! the backend-touching halves of `Checkpoint::find`/`list`/`remove` — this module
//! owns only the plain [`Checkpoint`] value, its ref-nonce generator, name
//! validation, and the named-checkpoint registry file discipline, the same split
//! `crate::reuse` uses for the reuse feature).
//!
//! **Be honest about what this is**: a checkpoint is a FILESYSTEM capture of a
//! running container (via the active backend's own checkpoint primitive — an image
//! commit on docker, a disk snapshot on microsandbox), not a memory snapshot —
//! restore boots a container whose filesystem starts where the checkpoint left off
//! (a FRESH container on docker; the SAME sandbox, restarted, on microsandbox), but
//! every process restarts from scratch. True microVM memory snapshots need upstream
//! microsandbox support and stay on the roadmap (see `docs/roadmap.md`).
//!
//! **Named checkpoints** extend this with durable identity: an UNNAMED
//! `checkpoint()` behaves exactly as above (random ref, no registry entry,
//! ephemeral). A NAMED `checkpoint_named(name)` additionally writes
//! `<cacheDir>/checkpoints/<name>.json` — one small JSON file per name, in the
//! same atomic tmp-then-rename shape as `crate::reuse::Registry` — so a LATER
//! process (not just this one) can rediscover it via `Checkpoint::find`/`list`,
//! and `Checkpoint::remove` can tear it down explicitly. The registry's field
//! names are a cross-language contract (see the parity docs), pinned exactly:
//! `name`, `ref`, `backend`, `createdIso`, and a reduced `spec` (only the four
//! fields `Container::from_checkpoint` actually reads back: `env`, `command`,
//! `exposedPorts`, `memoryLimitMb` — never the full `ContainerSpec`, since
//! mounts/network/aliases/name/run_id have no meaning surviving a process
//! boundary and `from_checkpoint` never reads them anyway).

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::cache_dir::unique_tmp_suffix;
use crate::error::{Result, RightsizeError};
use crate::model::ContainerSpec;

/// The outcome of [`crate::ContainerGuard::checkpoint`]: the backend-native ref it
/// was stored under, which backend created it, and the source container's full spec,
/// so [`crate::Container::from_checkpoint`] has everything it needs to boot an
/// equivalent container from it.
#[derive(Clone, Debug)]
pub struct Checkpoint {
    /// The backend-native checkpoint reference — random per checkpoint (see
    /// `generate_ref_nonce`), formatted by each backend into its own shape: a
    /// docker image tag (`rightsize/checkpoint:<12-hex>`) or a microsandbox disk
    /// snapshot name (`rz-ckpt-<12-hex>`). NOT auto-reaped: this is a checkpoint
    /// artifact, not a container, so it survives process exit and every own-run
    /// cleanup path in this crate. See the checkpoints docs for the cleanup
    /// one-liner (or `SandboxBackend::remove_checkpoint` for tests).
    pub checkpoint_ref: String,
    /// The registered name of the backend that created this checkpoint (e.g.
    /// `"docker"`, `"microsandbox"`) — `Container::from_checkpoint` refuses to
    /// restore under a different active backend (see
    /// `RightsizeError::CheckpointBackendMismatch`), since a ref from one backend's
    /// checkpoint mechanism has no meaning to the other's.
    pub backend: String,
    /// The source container's spec at the moment it was checkpointed — env,
    /// command, exposed ports, memory limit. `Container::from_checkpoint` reads
    /// exactly those four from this; it deliberately does NOT re-apply `mounts`
    /// (the checkpoint already has whatever those mounts wrote, baked in) or
    /// `network_id`/`aliases` (network topology never survives a restore).
    ///
    /// This is the FULL original `ContainerSpec` only when the `Checkpoint` came
    /// directly back from [`crate::ContainerGuard::checkpoint`] or
    /// [`crate::ContainerGuard::checkpoint_named`]. A `Checkpoint` obtained via
    /// [`Checkpoint::find`]/[`Checkpoint::list`] (a NAMED checkpoint, rediscovered
    /// from its registry entry, possibly in a later process) instead carries a
    /// RECONSTRUCTED spec: only `env`/`command`/`exposed_ports`/
    /// `memory_limit_mb` are real — every other field (`name`, `mounts`,
    /// `network_id`, `aliases`, `run_id`, `keep_alive`, `checkpoint_ref`, host
    /// ports) is a placeholder, since the registry never persisted them and
    /// `from_checkpoint` never reads them anyway. Don't read anything off a
    /// `find`/`list`-obtained checkpoint's `spec` beyond those four fields.
    pub spec: ContainerSpec,
}

/// A named checkpoint's name must match this pattern — pinned identically across
/// every port of this library (Kotlin/Node/Rust), enforced before any backend call
/// or registry I/O. Anchored at both ends: a leading `[a-z0-9]` plus up to 40 more
/// `[a-z0-9-]` characters (41 characters total, at most).
const NAME_PATTERN: &str = "^[a-z0-9][a-z0-9-]{0,40}$";

fn name_regex() -> &'static regex_lite::Regex {
    static RE: OnceLock<regex_lite::Regex> = OnceLock::new();
    RE.get_or_init(|| regex_lite::Regex::new(NAME_PATTERN).expect("NAME_PATTERN is a valid regex"))
}

/// Validates a named checkpoint's `name` against [`NAME_PATTERN`], fast, before any
/// backend call or registry I/O — every entry point that takes a checkpoint name
/// (`ContainerGuard::checkpoint_named`, `Checkpoint::find`, `Checkpoint::remove`)
/// calls this first.
pub(crate) fn validate_name(name: &str) -> Result<()> {
    if name_regex().is_match(name) {
        Ok(())
    } else {
        Err(RightsizeError::InvalidCheckpointName {
            name: name.to_string(),
        })
    }
}

/// The reduced slice of a source `ContainerSpec` a named checkpoint registry entry
/// persists — exactly the fields [`crate::Container::from_checkpoint`] reads back
/// (env, command, exposed/guest-side ports, memory limit), never the full spec.
/// Field names and order are a cross-language contract (see the parity docs) —
/// `env` as a JSON object (not the core `Vec<(String, String)>` pair list), `command`
/// as an array or `null`, `exposedPorts` as an array of guest-side ports, and
/// `memoryLimitMb` as a number or `null`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct NamedRegistrySpec {
    pub env: BTreeMap<String, String>,
    pub command: Option<Vec<String>>,
    #[serde(rename = "exposedPorts")]
    pub exposed_ports: Vec<u16>,
    #[serde(rename = "memoryLimitMb")]
    pub memory_limit_mb: Option<u64>,
}

/// `<cacheDir>/checkpoints/<name>.json`'s payload — the cross-language contract's
/// exact field names and order, pinned identically across the Kotlin/Node/Rust
/// implementations (parity-testable, see the parity docs page).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct NamedRegistryEntry {
    /// The checkpoint's name — also this registry file's own stem, `<name>.json`.
    pub name: String,
    /// The backend-native checkpoint ref this name currently points at.
    #[serde(rename = "ref")]
    pub checkpoint_ref: String,
    /// The backend that created it, e.g. `"docker"` / `"microsandbox"`.
    pub backend: String,
    /// ISO-8601 UTC instant this entry was (re)written — a re-checkpoint under the
    /// same name overwrites this along with everything else (latest wins).
    #[serde(rename = "createdIso")]
    pub created_iso: String,
    /// The reduced source spec — see [`NamedRegistrySpec`]'s own doc.
    pub spec: NamedRegistrySpec,
}

/// `<cacheDir>/checkpoints/<name>.json` for one named checkpoint — the same
/// atomic-write, corrupt-tolerant-read discipline as `crate::reuse::Registry`,
/// keyed by NAME rather than an identity hash.
pub(crate) struct Registry {
    path: PathBuf,
}

impl Registry {
    /// Builds a registry handle for `name`. Does not itself create the directory or
    /// any file.
    pub(crate) fn new(cache_dir: &Path, name: &str) -> Self {
        Registry {
            path: cache_dir.join("checkpoints").join(format!("{name}.json")),
        }
    }

    /// True if a registry file exists at all, distinguishing "no registry" from "a
    /// registry file exists but is corrupt" — [`Self::read`] collapses both to
    /// `None`, which is fine for `find`/`checkpoint_named`'s own purposes, but
    /// `remove`'s "did anything exist" contract needs this finer distinction.
    pub(crate) fn exists(&self) -> bool {
        self.path.exists()
    }

    /// Reads and parses the registry file. `None` for a missing OR unparseable file
    /// — both are the caller's "not usable" case (see `Checkpoint::find`'s own
    /// stale/corrupt handling, which treats either the same way: absent).
    pub(crate) fn read(&self) -> Option<NamedRegistryEntry> {
        let raw = fs::read(&self.path).ok()?;
        serde_json::from_slice(&raw).ok()
    }

    /// Writes the registry file atomically (temp file + rename), creating
    /// `<cacheDir>/checkpoints/` if needed. Called only after the backend
    /// checkpoint itself has already succeeded. The temp file's name is unique
    /// per write (see [`unique_tmp_suffix`]) rather than the fixed
    /// `<name>.json.tmp`, so two processes checkpointing under the same name at
    /// once write into distinct temp files instead of interleaving into one,
    /// and a temp file left behind by a crashed writer can never collide with
    /// (or get clobbered by) the next write's own temp file.
    pub(crate) fn write_atomic(&self, entry: &NamedRegistryEntry) -> std::io::Result<()> {
        let dir = self
            .path
            .parent()
            .expect("registry path always has a parent (cache_dir/checkpoints/)");
        fs::create_dir_all(dir)?;
        let json = serde_json::to_vec_pretty(entry)
            .expect("NamedRegistryEntry has no non-serializable fields");
        let tmp = self
            .path
            .with_extension(format!("json.tmp.{}", unique_tmp_suffix()));
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

/// Every parseable named-checkpoint registry entry under
/// `<cacheDir>/checkpoints/` — `Checkpoint::list`'s own primitive. A missing
/// directory is an empty list, not an error (no checkpoint has ever been named on
/// this host); a corrupt/unreadable entry is silently skipped (`list`'s documented
/// contract — only [`Checkpoint::find`] resolves a stale-vs-corrupt entry, and only
/// for the one name it was asked about).
pub(crate) fn list_registry_entries(cache_dir: &Path) -> std::io::Result<Vec<NamedRegistryEntry>> {
    let dir = cache_dir.join("checkpoints");
    let read_dir = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut entries = Vec::new();
    for dir_entry in read_dir {
        let path = dir_entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if let Ok(raw) = fs::read(&path) {
            if let Ok(parsed) = serde_json::from_slice::<NamedRegistryEntry>(&raw) {
                entries.push(parsed);
            }
        }
    }
    Ok(entries)
}

/// Generates a fresh random 12-lowercase-hex nonce for one checkpoint. The 12 hex
/// characters (48 bits) are mixed from the current time, this process's id, and a
/// process-wide monotonic counter — the same dependency-free mixing shape
/// `crate::run_id::generate` uses for the per-process run id, extended with a
/// counter so two checkpoints taken back-to-back in the same process (even within
/// the same timer tick) never collide. Not cryptographic — just enough spread that a
/// random per-checkpoint nonce never collides with another one on the same host in
/// practice.
///
/// Each backend's own `create_checkpoint` formats this bare nonce into its own ref
/// shape (a docker image tag, a microsandbox snapshot name) — this module no longer
/// decides the full ref shape itself, since that's backend-specific.
pub(crate) fn generate_ref_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::SeqCst) as u128;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    let mixed = nanos
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(pid)
        .wrapping_add(seq);
    let low48 = (mixed & 0xFFFF_FFFF_FFFF) as u64;
    format!("{low48:012x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_twelve_lowercase_hex(s: &str) -> bool {
        s.len() == 12
            && s.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    }

    #[test]
    fn generate_ref_nonce_is_twelve_lowercase_hex_chars() {
        let n = generate_ref_nonce();
        assert!(is_twelve_lowercase_hex(&n), "{n}");
    }

    #[test]
    fn generate_ref_nonce_differs_across_back_to_back_calls() {
        let a = generate_ref_nonce();
        let b = generate_ref_nonce();
        assert_ne!(a, b, "two checkpoints must never share a ref nonce");
    }

    // -- name validation --------------------------------------------------------

    #[test]
    fn validate_name_accepts_the_pinned_pattern() {
        assert!(validate_name("a").is_ok());
        assert!(validate_name("seeded-db").is_ok());
        assert!(validate_name("a0-1-2-3").is_ok());
        // Exactly 41 characters: the pattern's upper bound.
        let max_len = format!("a{}", "0".repeat(40));
        assert_eq!(max_len.len(), 41);
        assert!(validate_name(&max_len).is_ok());
    }

    #[test]
    fn validate_name_rejects_anything_outside_the_pattern() {
        for bad in [
            "",
            "-leading-dash",
            "Uppercase",
            "has_underscore",
            "has space",
            "a.b",
            &format!("a{}", "0".repeat(41)), // 42 chars: one over the limit
        ] {
            let err = validate_name(bad).expect_err(&format!("{bad:?} must be rejected"));
            assert!(
                matches!(err, RightsizeError::InvalidCheckpointName { ref name } if name == bad),
                "{err}"
            );
        }
    }

    // -- registry -----------------------------------------------------------

    fn temp_cache_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rz-checkpoint-registry-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_entry() -> NamedRegistryEntry {
        NamedRegistryEntry {
            name: "seeded-db".to_string(),
            checkpoint_ref: "rz-ckpt-seeded-db".to_string(),
            backend: "microsandbox".to_string(),
            created_iso: "2025-01-01T00:00:00Z".to_string(),
            spec: NamedRegistrySpec {
                env: BTreeMap::from([("A".to_string(), "1".to_string())]),
                command: Some(vec!["redis-server".to_string()]),
                exposed_ports: vec![6379],
                memory_limit_mb: Some(256),
            },
        }
    }

    #[test]
    fn write_then_read_round_trips() {
        let cache = temp_cache_dir("round-trip");
        let registry = Registry::new(&cache, "seeded-db");
        registry.write_atomic(&sample_entry()).unwrap();
        assert!(registry.exists());
        assert_eq!(registry.read(), Some(sample_entry()));
    }

    #[test]
    fn missing_registry_neither_exists_nor_reads() {
        let cache = temp_cache_dir("missing");
        let registry = Registry::new(&cache, "seeded-db");
        assert!(!registry.exists());
        assert!(registry.read().is_none());
    }

    #[test]
    fn corrupt_registry_exists_but_does_not_parse() {
        let cache = temp_cache_dir("corrupt");
        let registry = Registry::new(&cache, "seeded-db");
        fs::create_dir_all(cache.join("checkpoints")).unwrap();
        fs::write(
            cache.join("checkpoints").join("seeded-db.json"),
            b"not json",
        )
        .unwrap();
        assert!(registry.exists(), "a corrupt file still exists on disk");
        assert!(registry.read().is_none(), "but it must not parse");
    }

    #[test]
    fn delete_is_idempotent_and_best_effort() {
        let cache = temp_cache_dir("delete");
        let registry = Registry::new(&cache, "seeded-db");
        registry.write_atomic(&sample_entry()).unwrap();
        registry.delete();
        assert!(!registry.exists());
        registry.delete(); // second call: must not panic.
    }

    #[test]
    fn write_atomic_survives_a_stale_leftover_tmp_file() {
        let cache = temp_cache_dir("stale-tmp");
        let registry = Registry::new(&cache, "seeded-db");

        // A writer that crashed between creating its temp file and renaming it
        // would leave exactly this behind. It must not collide with (or be
        // mistaken for) a later write's own uniquely-named temp file.
        fs::create_dir_all(cache.join("checkpoints")).unwrap();
        fs::write(
            cache.join("checkpoints").join("seeded-db.json.tmp.stale"),
            b"leftover from a crashed writer",
        )
        .unwrap();

        registry.write_atomic(&sample_entry()).unwrap();
        assert_eq!(registry.read(), Some(sample_entry()));

        let mut replaced = sample_entry();
        replaced.checkpoint_ref = "rz-ckpt-seeded-db-2".to_string();
        registry.write_atomic(&replaced).unwrap();
        assert_eq!(
            registry.read(),
            Some(replaced),
            "a second write must still land cleanly with the stale tmp file still sitting next to it"
        );
    }

    #[test]
    fn write_atomic_overwrites_an_existing_entry() {
        let cache = temp_cache_dir("overwrite");
        let registry = Registry::new(&cache, "seeded-db");
        registry.write_atomic(&sample_entry()).unwrap();
        let mut replaced = sample_entry();
        replaced.checkpoint_ref = "rz-ckpt-seeded-db-2".to_string();
        registry.write_atomic(&replaced).unwrap();
        assert_eq!(registry.read(), Some(replaced));
    }

    #[test]
    fn registry_json_uses_the_pinned_field_names() {
        let cache = temp_cache_dir("field-names");
        let registry = Registry::new(&cache, "seeded-db");
        registry.write_atomic(&sample_entry()).unwrap();
        let raw = fs::read_to_string(cache.join("checkpoints").join("seeded-db.json")).unwrap();
        for pinned in [
            "\"name\"",
            "\"ref\"",
            "\"backend\"",
            "\"createdIso\"",
            "\"spec\"",
            "\"env\"",
            "\"command\"",
            "\"exposedPorts\"",
            "\"memoryLimitMb\"",
        ] {
            assert!(raw.contains(pinned), "{pinned} missing from {raw}");
        }
        assert!(!raw.contains("checkpoint_ref"), "{raw}");
        assert!(!raw.contains("created_iso"), "{raw}");
        assert!(!raw.contains("exposed_ports"), "{raw}");
        assert!(!raw.contains("memory_limit_mb"), "{raw}");
    }

    #[test]
    fn list_registry_entries_returns_empty_for_a_missing_directory() {
        let cache = temp_cache_dir("list-missing-dir");
        assert_eq!(list_registry_entries(&cache).unwrap(), Vec::new());
    }

    #[test]
    fn list_registry_entries_skips_corrupt_files_and_returns_valid_ones() {
        let cache = temp_cache_dir("list-mixed");
        Registry::new(&cache, "good-one")
            .write_atomic(&sample_entry())
            .unwrap();
        fs::create_dir_all(cache.join("checkpoints")).unwrap();
        fs::write(cache.join("checkpoints").join("corrupt.json"), b"not json").unwrap();
        fs::write(cache.join("checkpoints").join("ignored.txt"), b"irrelevant").unwrap();

        let entries = list_registry_entries(&cache).unwrap();
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].name, "seeded-db");
    }
}
