//! The checkpoint export/import archive format — see [`crate::Checkpoint::export_to`]
//! and [`crate::Checkpoint::import_from`] for the public API, `crate::container`'s
//! `impl Checkpoint` block for the orchestration, and `docs/checkpoints.md` for the
//! user-facing story.
//!
//! An archive is a plain tar with exactly two members at its root: `checkpoint.json`
//! (this module's [`ArchiveManifest`], pinned identically across every port of this
//! library) and `artifact` (the backend payload, byte-for-byte what the backend CLI
//! produced — msb's `.tar.zst` snapshot export, docker's `docker save` tar). No
//! container-level compression: msb's payload is already zstd, and docker's
//! compresses poorly enough not to matter.
//!
//! The host `tar` binary is the archive container tool (present on Linux, macOS, and
//! Windows 10+ as bsdtar's `tar.exe`) — this module shells out to it rather than
//! hand-rolling or depending on a tar implementation, the same call as the pinned
//! `docs/checkpoints.md`/spec contract for every language port.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::checkpoint::NamedRegistrySpec;
use crate::error::{Result, RightsizeError};

/// The only `rightsizeArchive` value this port understands. A different value in an
/// archive being imported is a typed [`RightsizeError::MalformedArchive`] naming the
/// value found — see [`crate::Checkpoint::import_from`].
pub(crate) const FORMAT_VERSION: u32 = 1;

/// The two member names an archive's tar contains, at its root, in creation order —
/// [`tar_create`]'s own argument, and what [`tar_extract`]'s caller looks for
/// afterward.
const MANIFEST_MEMBER: &str = "checkpoint.json";
const ARTIFACT_MEMBER: &str = "artifact";

/// `checkpoint.json`'s exact shape — the registry-entry shape
/// (`crate::checkpoint::NamedRegistryEntry`) plus a format version and an optional
/// name (an archive from an UNNAMED checkpoint carries `name: null`). Field names,
/// order, and the `rightsizeArchive`/`ref` renames are a cross-language contract,
/// pinned identically in every port of this library.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ArchiveManifest {
    #[serde(rename = "rightsizeArchive")]
    pub rightsize_archive: u32,
    pub name: Option<String>,
    #[serde(rename = "ref")]
    pub checkpoint_ref: String,
    pub backend: String,
    #[serde(rename = "createdIso")]
    pub created_iso: String,
    pub spec: NamedRegistrySpec,
}

/// A fresh, uniquely-named temp directory, removed on drop regardless of whether the
/// work done inside it succeeded — the "temp staging in a fresh unique temp dir,
/// removed in a finally/defer/guard" requirement both `export_to` and `import_from`
/// share, expressed as a plain RAII guard so every `?` early return still cleans up.
pub(crate) struct TempStagingDir {
    path: PathBuf,
}

impl TempStagingDir {
    /// Creates `<host temp dir>/rightsize-archive-<label>-<unique suffix>` and
    /// returns a guard that removes it (recursively) on drop. Delegates to
    /// [`Self::create_in`] with `std::env::temp_dir()` as the parent — the
    /// same default every production caller now passes explicitly (see
    /// [`crate::Checkpoint::export_to`]/`import_from`), so this shorthand is
    /// only reached from tests that don't care which parent their staging
    /// dir lands under.
    #[cfg(test)]
    pub(crate) fn create(label: &str) -> Result<Self> {
        Self::create_in(&std::env::temp_dir(), label)
    }

    /// Creates `<parent>/rightsize-archive-<label>-<unique suffix>` and returns
    /// a guard that removes it (recursively) on drop. Exists so a test can
    /// point `parent` at a private, per-test directory instead of the shared
    /// host temp dir — a staging-cleanup assertion that scans `parent`
    /// afterward is then immune to sibling tests racing identically-prefixed
    /// staging dirs into the same shared directory.
    pub(crate) fn create_in(parent: &Path, label: &str) -> Result<Self> {
        let path = parent.join(format!(
            "rightsize-archive-{label}-{}",
            crate::cache_dir::unique_tmp_suffix()
        ));
        std::fs::create_dir_all(&path)?;
        Ok(TempStagingDir { path })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempStagingDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Writes `manifest` as pretty JSON to `path` (always `<staging>/checkpoint.json`).
pub(crate) fn write_manifest(path: &Path, manifest: &ArchiveManifest) -> Result<()> {
    let json = serde_json::to_vec_pretty(manifest)
        .expect("ArchiveManifest has no non-serializable fields");
    std::fs::write(path, json)?;
    Ok(())
}

/// Reads and parses `<staging>/checkpoint.json`, extracted from `archive_path` by an
/// earlier [`tar_extract`] call — `archive_path` is only used for this function's own
/// error messages, naming the archive a caller actually gave
/// `Checkpoint::import_from`, not the throwaway staging path. A missing member or
/// unparseable JSON is a typed [`RightsizeError::MalformedArchive`] either way — the
/// version/name/backend checks that follow live in `crate::container`'s import
/// orchestration, not here, since they need context (the active backend, the name
/// regex) this module doesn't have.
pub(crate) fn read_manifest(
    archive_path: &Path,
    checkpoint_json_path: &Path,
) -> Result<ArchiveManifest> {
    let raw =
        std::fs::read(checkpoint_json_path).map_err(|_| RightsizeError::MalformedArchive {
            path: archive_path.to_path_buf(),
            reason: format!("missing the required '{MANIFEST_MEMBER}' member"),
        })?;
    serde_json::from_slice(&raw).map_err(|e| RightsizeError::MalformedArchive {
        path: archive_path.to_path_buf(),
        reason: format!("'{MANIFEST_MEMBER}' could not be parsed as JSON: {e}"),
    })
}

/// The staging directory's `checkpoint.json` path — the pinned member name, one
/// place, used by both the export writer and the import reader's caller.
pub(crate) fn manifest_path(staging_dir: &Path) -> PathBuf {
    staging_dir.join(MANIFEST_MEMBER)
}

/// The staging directory's `artifact` path — the pinned member name.
pub(crate) fn artifact_path(staging_dir: &Path) -> PathBuf {
    staging_dir.join(ARTIFACT_MEMBER)
}

/// `tar -cf <dest basename> -C <staging_dir> checkpoint.json artifact`, run with the
/// child's working directory set to `dest`'s parent — the `-f` argument stays a bare
/// BASENAME because an absolute Windows path there (`C:\...`) is parsed by GNU tar as
/// a `host:path` remote-archive spec ("Cannot connect to C"), and which flavor `tar`
/// resolves to on Windows depends on PATH order (System32's bsdtar accepts drive
/// letters, Git's GNU tar does not); basename-plus-cwd behaves identically under
/// both. Both members must already exist directly under `staging_dir` (as
/// [`manifest_path`]/[`artifact_path`] name them) before this is called. `dest`'s
/// parent directory must already exist (the caller's responsibility, matching the
/// spec's "parent directories created" at the `export_to` call site, not here); a
/// pre-existing `dest` is overwritten, `tar -c`'s ordinary behavior.
pub(crate) async fn tar_create(dest: &Path, staging_dir: &Path) -> Result<()> {
    let (dest_dir, dest_name) = split_for_tar(dest)?;
    let output = tokio::process::Command::new("tar")
        .current_dir(dest_dir)
        .arg("-cf")
        .arg(dest_name)
        .arg("-C")
        .arg(tar_dir_arg(staging_dir))
        .arg(MANIFEST_MEMBER)
        .arg(ARTIFACT_MEMBER)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .map_err(|e| {
            RightsizeError::Backend(format!("failed to spawn tar -cf {}: {e}", dest.display()))
        })?;
    if !output.status.success() {
        return Err(RightsizeError::Backend(format!(
            "tar -cf {} failed (exit {}): {}",
            dest.display(),
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

/// `tar -xf <archive_path> -C <dest_dir>` — a typed [`RightsizeError::MalformedArchive`]
/// when `archive_path` doesn't exist at all (checked up front, for a clear message
/// independent of the local `tar` flavor's own wording) or when the extraction itself
/// fails (a file that exists but isn't a tar archive, a truncated download, etc.).
pub(crate) async fn tar_extract(archive_path: &Path, dest_dir: &Path) -> Result<()> {
    if !archive_path.is_file() {
        return Err(RightsizeError::MalformedArchive {
            path: archive_path.to_path_buf(),
            reason: "no such archive file".to_string(),
        });
    }
    let (archive_dir, archive_name) = split_for_tar(archive_path)?;
    let output = tokio::process::Command::new("tar")
        .current_dir(archive_dir)
        .arg("-xf")
        .arg(archive_name)
        .arg("-C")
        .arg(tar_dir_arg(dest_dir))
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .map_err(|e| {
            RightsizeError::Backend(format!(
                "failed to spawn tar -xf {}: {e}",
                archive_path.display()
            ))
        })?;
    if !output.status.success() {
        return Err(RightsizeError::MalformedArchive {
            path: archive_path.to_path_buf(),
            reason: format!(
                "tar could not extract it (exit {}): {}",
                output.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        });
    }
    Ok(())
}

/// Normalizes a directory for tar's `-C` argument: on Windows, Git's GNU (MSYS)
/// tar mangles backslash paths ("Cannot open"), while both it and System32's
/// bsdtar accept the same path with forward slashes. Elsewhere the path passes
/// through untouched — a backslash is a legal filename character on POSIX.
fn tar_dir_arg(dir: &Path) -> std::ffi::OsString {
    if cfg!(windows) {
        dir.to_string_lossy().replace('\\', "/").into()
    } else {
        dir.as_os_str().to_os_string()
    }
}

/// Splits a path into (parent-or-cwd, file name) for a tar `-f` argument — see
/// [`tar_create`]'s doc for why the archive is always addressed by basename with the
/// parent as the child's working directory.
fn split_for_tar(path: &Path) -> Result<(std::path::PathBuf, std::ffi::OsString)> {
    let name = path
        .file_name()
        .ok_or_else(|| RightsizeError::MalformedArchive {
            path: path.to_path_buf(),
            reason: "archive path has no file name".to_string(),
        })?
        .to_os_string();
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    };
    Ok((parent, name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sample_manifest() -> ArchiveManifest {
        ArchiveManifest {
            rightsize_archive: FORMAT_VERSION,
            name: Some("seeded-db".to_string()),
            checkpoint_ref: "rz-ckpt-deadbeefcafe".to_string(),
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
    fn temp_staging_dir_creates_and_removes_itself() {
        let path = {
            let staging = TempStagingDir::create("test").expect("create must succeed");
            let path = staging.path().to_path_buf();
            assert!(path.is_dir(), "the staging dir must exist while held");
            path
        };
        assert!(!path.exists(), "the staging dir must be gone once dropped");
    }

    #[test]
    fn two_staging_dirs_never_collide() {
        let a = TempStagingDir::create("dup").unwrap();
        let b = TempStagingDir::create("dup").unwrap();
        assert_ne!(a.path(), b.path());
    }

    #[test]
    fn write_then_read_manifest_round_trips_with_the_pinned_field_names() {
        let staging = TempStagingDir::create("manifest-roundtrip").unwrap();
        let path = manifest_path(staging.path());
        write_manifest(&path, &sample_manifest()).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        for pinned in [
            "\"rightsizeArchive\": 1",
            "\"name\": \"seeded-db\"",
            "\"ref\": \"rz-ckpt-deadbeefcafe\"",
            "\"backend\": \"microsandbox\"",
            "\"createdIso\"",
            "\"spec\"",
        ] {
            assert!(raw.contains(pinned), "{pinned} missing from {raw}");
        }
        assert!(!raw.contains("rightsize_archive"), "{raw}");
        assert!(!raw.contains("checkpoint_ref"), "{raw}");

        let parsed = read_manifest(std::path::Path::new("archive.tar"), &path).unwrap();
        assert_eq!(parsed, sample_manifest());
    }

    #[test]
    fn write_manifest_serializes_a_null_name_for_an_unnamed_checkpoint() {
        let staging = TempStagingDir::create("manifest-unnamed").unwrap();
        let mut manifest = sample_manifest();
        manifest.name = None;
        let path = manifest_path(staging.path());
        write_manifest(&path, &manifest).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("\"name\": null"), "{raw}");

        let parsed = read_manifest(std::path::Path::new("archive.tar"), &path).unwrap();
        assert_eq!(parsed.name, None);
    }

    #[test]
    fn read_manifest_on_a_missing_member_is_a_typed_malformed_archive_error() {
        let staging = TempStagingDir::create("manifest-missing").unwrap();
        let err = read_manifest(
            std::path::Path::new("archive.tar"),
            &manifest_path(staging.path()), // never written
        )
        .expect_err("a missing checkpoint.json must be a typed error");
        match err {
            RightsizeError::MalformedArchive { path, reason } => {
                assert_eq!(path, std::path::PathBuf::from("archive.tar"));
                assert!(reason.contains("checkpoint.json"), "{reason}");
            }
            other => panic!("expected MalformedArchive, got {other:?}"),
        }
    }

    #[test]
    fn read_manifest_on_malformed_json_is_a_typed_malformed_archive_error() {
        let staging = TempStagingDir::create("manifest-malformed").unwrap();
        let path = manifest_path(staging.path());
        std::fs::write(&path, b"not json").unwrap();

        let err = read_manifest(std::path::Path::new("archive.tar"), &path)
            .expect_err("malformed JSON must be a typed error");
        assert!(
            matches!(err, RightsizeError::MalformedArchive { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn tar_create_then_tar_extract_round_trips_both_members_byte_for_byte() {
        let staging = TempStagingDir::create("tar-roundtrip-src").unwrap();
        std::fs::write(manifest_path(staging.path()), b"{\"a\":1}").unwrap();
        std::fs::write(artifact_path(staging.path()), b"\x00\x01payload-bytes\xff").unwrap();

        let archive_dir = TempStagingDir::create("tar-roundtrip-archive").unwrap();
        let archive = archive_dir.path().join("cp.archive");
        tar_create(&archive, staging.path()).await.unwrap();
        assert!(archive.is_file());

        let dest = TempStagingDir::create("tar-roundtrip-dest").unwrap();
        tar_extract(&archive, dest.path()).await.unwrap();

        assert_eq!(
            std::fs::read(manifest_path(dest.path())).unwrap(),
            b"{\"a\":1}"
        );
        assert_eq!(
            std::fs::read(artifact_path(dest.path())).unwrap(),
            b"\x00\x01payload-bytes\xff"
        );
    }

    #[tokio::test]
    async fn tar_create_overwrites_a_pre_existing_destination_file() {
        let staging = TempStagingDir::create("tar-overwrite-src").unwrap();
        std::fs::write(manifest_path(staging.path()), b"new-manifest").unwrap();
        std::fs::write(artifact_path(staging.path()), b"new-artifact").unwrap();

        let archive_dir = TempStagingDir::create("tar-overwrite-archive").unwrap();
        let archive = archive_dir.path().join("cp.archive");
        std::fs::write(&archive, b"stale bytes from a previous export").unwrap();

        tar_create(&archive, staging.path()).await.unwrap();

        let dest = TempStagingDir::create("tar-overwrite-dest").unwrap();
        tar_extract(&archive, dest.path()).await.unwrap();
        assert_eq!(
            std::fs::read(manifest_path(dest.path())).unwrap(),
            b"new-manifest"
        );
    }

    #[tokio::test]
    async fn tar_extract_on_a_missing_file_is_a_typed_malformed_archive_error() {
        let dest = TempStagingDir::create("tar-missing-dest").unwrap();
        let err = tar_extract(
            std::path::Path::new("/definitely/not/a/real/archive"),
            dest.path(),
        )
        .await
        .expect_err("a missing archive file must be a typed error");
        match err {
            RightsizeError::MalformedArchive { reason, .. } => {
                assert!(reason.contains("no such archive"), "{reason}");
            }
            other => panic!("expected MalformedArchive, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tar_extract_on_a_non_tar_file_is_a_typed_malformed_archive_error() {
        let src_dir = TempStagingDir::create("tar-bad-src").unwrap();
        let not_a_tar = src_dir.path().join("not-a-tar");
        std::fs::write(
            &not_a_tar,
            b"just some plain bytes, not a tar archive at all",
        )
        .unwrap();

        let dest = TempStagingDir::create("tar-bad-dest").unwrap();
        let err = tar_extract(&not_a_tar, dest.path())
            .await
            .expect_err("a non-tar file must be a typed error");
        assert!(
            matches!(err, RightsizeError::MalformedArchive { .. }),
            "{err}"
        );
    }
}
