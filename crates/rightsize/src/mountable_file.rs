//! A file to be mounted into a container via `Container::with_copy_file_to_container`.
//!
//! Rust has no classpath, so [`MountableFile::for_classpath_resource`] resolves
//! "a bundled resource to a real host path" via a documented convention instead:
//! resource paths resolve relative to `CARGO_MANIFEST_DIR`, matching how a Rust
//! crate actually bundles test/data files.

use std::path::{Path, PathBuf};

use crate::error::{Result, RightsizeError};

/// A host file or directory to mount into a container.
#[derive(Debug, Clone)]
pub struct MountableFile {
    path: PathBuf,
}

impl MountableFile {
    /// A file already on the host filesystem, resolved to an absolute path.
    pub fn for_host_path(path: &str) -> MountableFile {
        let p = Path::new(path);
        let absolute = if p.is_absolute() {
            p.to_path_buf()
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(p)
        };
        MountableFile { path: absolute }
    }

    /// Resolves `resource` relative to `CARGO_MANIFEST_DIR` — this crate's Rust
    /// analogue of a JVM classpath resource, since Rust has no classpath to resolve
    /// against. The path must exist and be readable; a caller bundling data under e.g.
    /// `tests/fixtures/` passes that relative path here.
    pub fn for_classpath_resource(resource: &str) -> Result<MountableFile> {
        let normalized = resource.strip_prefix('/').unwrap_or(resource);
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").map_err(|_| {
            RightsizeError::Backend(
                "CARGO_MANIFEST_DIR is not set — for_classpath_resource can only resolve \
                 bundled resources when run under `cargo test`/`cargo run`"
                    .to_string(),
            )
        })?;
        let candidate = Path::new(&manifest_dir).join(normalized);
        if !candidate.exists() {
            return Err(RightsizeError::Backend(format!(
                "Bundled resource not found: '{normalized}' under {manifest_dir} (original \
                 request: '{resource}'). Verify the resource ships with the crate (e.g. under \
                 a `resources/` or `tests/fixtures/` directory) and the path has no leading \
                 slash issues."
            )));
        }
        Ok(MountableFile { path: candidate })
    }

    /// The resolved absolute host path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn for_host_path_absolutizes_a_relative_path() {
        let mf = MountableFile::for_host_path("some/relative/file.txt");
        assert!(mf.path().is_absolute(), "{:?}", mf.path());
        assert!(mf.path().ends_with("some/relative/file.txt"));
    }

    #[test]
    fn for_host_path_leaves_an_absolute_path_untouched() {
        let mf = MountableFile::for_host_path("/etc/hosts");
        assert_eq!(mf.path(), Path::new("/etc/hosts"));
    }

    #[test]
    fn for_classpath_resource_round_trips_a_bundled_fixture() {
        // The gate names a round-trip against a bundled `rightsize-fixture.txt`: write
        // one under a scratch dir inside CARGO_MANIFEST_DIR/target so the test is
        // self-contained and doesn't require a checked-in fixture just for this unit test.
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let fixture_dir = Path::new(&manifest_dir)
            .join("target")
            .join("mountable_file_test_fixtures");
        std::fs::create_dir_all(&fixture_dir).unwrap();
        let fixture_path = fixture_dir.join("rightsize-fixture.txt");
        let mut f = std::fs::File::create(&fixture_path).unwrap();
        writeln!(f, "hello from the fixture").unwrap();
        drop(f);

        let relative = fixture_path
            .strip_prefix(&manifest_dir)
            .unwrap()
            .to_str()
            .unwrap();
        let mf =
            MountableFile::for_classpath_resource(relative).expect("bundled fixture must resolve");
        let contents = std::fs::read_to_string(mf.path()).unwrap();
        assert_eq!(contents.trim(), "hello from the fixture");

        std::fs::remove_file(&fixture_path).ok();
    }

    #[test]
    fn for_classpath_resource_errors_naming_the_resource_when_missing() {
        let err =
            MountableFile::for_classpath_resource("definitely/does/not/exist.txt").unwrap_err();
        assert!(
            err.to_string().contains("definitely/does/not/exist.txt"),
            "{err}"
        );
    }
}
