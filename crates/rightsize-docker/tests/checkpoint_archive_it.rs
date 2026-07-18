//! `sandbox-it` integration test for checkpoint export/import
//! (`rightsize::Checkpoint::export_to`/`Checkpoint::import_from`) against the
//! docker backend — the same portable-archive round trip
//! `rightsize-msb`'s `checkpoint_archive_it.rs` exercises, but with docker's own
//! ref-preservation contract: `docker load` preserves the tag baked into the
//! `docker save` archive, so the imported checkpoint's ref round-trips UNCHANGED
//! (unlike msb's content-addressed import, whose effective ref is always a
//! resolved digest).
//!
//! Run for real against a real Docker daemon:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-docker --features sandbox-it --test checkpoint_archive_it
//! ```
//!
//! Cleanup: the two ordinary containers this test starts clean themselves up
//! automatically via the crate's own two-tier cleanup story (`ContainerGuard`'s
//! own `Drop`), even if a mid-test assertion panics. The archive file, the
//! imported checkpoint image, and the named-checkpoint registry entry have no
//! automatic cleanup path at all (checkpoint artifacts are never auto-reaped —
//! see the checkpoints docs), so all three are wired through an
//! [`ArchiveCleanup`] `Drop` guard, the same discipline as the msb IT's own.

#![cfg(feature = "sandbox-it")]

use std::cell::RefCell;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Once;
use std::time::Duration;

use rightsize::backend::BackendProvider;
use rightsize::{Checkpoint, Container, Wait};
use rightsize_docker::DockerBackendProvider;

static REGISTER: Once = Once::new();

fn docker_runtime_available() -> bool {
    if std::env::var("RIGHTSIZE_BACKEND")
        .map(|v| v.eq_ignore_ascii_case("microsandbox") || v.eq_ignore_ascii_case("msb"))
        .unwrap_or(false)
    {
        return false;
    }
    DockerBackendProvider.is_supported()
}

/// Registers the docker provider exactly once per process and forces
/// `RIGHTSIZE_BACKEND=docker` for this process unless the caller already set it —
/// the same set-once-at-startup shape the msb ITs use for their own provider.
fn ensure_registered() {
    REGISTER.call_once(|| {
        rightsize::backends::register_provider(Box::new(DockerBackendProvider));
        if std::env::var("RIGHTSIZE_BACKEND").is_err() {
            // SAFETY-of-intent note: `std::env::set_var` is unsafe as of the 2024
            // edition; this only runs once, before any other thread in this test
            // binary has started touching Container/backends::active().
            unsafe { std::env::set_var("RIGHTSIZE_BACKEND", "docker") };
        }
    });
}

macro_rules! require_docker {
    () => {
        ensure_registered();
        if !docker_runtime_available() {
            eprintln!(
                "skipping: no reachable Docker daemon on this host (or RIGHTSIZE_BACKEND=microsandbox)"
            );
            return;
        }
    };
}

/// A per-process random-enough hex nonce, folded into this test's checkpoint NAME
/// and archive filename so a leftover from a previous crashed run can never
/// collide with this run's own.
fn nonce() -> &'static str {
    rightsize::RunId::value()
}

/// Best-effort, SYNCHRONOUS `docker rmi -f <ref>` — shells the `docker` CLI
/// directly rather than the async `SandboxBackend::remove_checkpoint` SPI, since
/// this is called from a `Drop` impl, which cannot `.await`.
fn remove_checkpoint_image_blocking(checkpoint_ref: &str) {
    let _ = Command::new("docker")
        .args(["rmi", "-f", checkpoint_ref])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Best-effort, SYNCHRONOUS cleanup for one archive round trip: removes the
/// archive file, the imported checkpoint image (once known — set only after a
/// successful import), and the named-checkpoint registry entry. Wired through a
/// `Drop` guard, not a bare trailing call, so all three still run if an assertion
/// partway through the test panics.
struct ArchiveCleanup {
    archive_path: PathBuf,
    name: String,
    imported_ref: RefCell<Option<String>>,
}

impl ArchiveCleanup {
    fn new(archive_path: PathBuf, name: String) -> Self {
        ArchiveCleanup {
            archive_path,
            name,
            imported_ref: RefCell::new(None),
        }
    }

    fn set_imported_ref(&self, effective_ref: String) {
        *self.imported_ref.borrow_mut() = Some(effective_ref);
    }
}

impl Drop for ArchiveCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.archive_path);
        if let Some(effective_ref) = self.imported_ref.borrow().clone() {
            remove_checkpoint_image_blocking(&effective_ref);
        }
        let registry_path = rightsize::cache_dir::dir()
            .join("checkpoints")
            .join(format!("{}.json", self.name));
        let _ = std::fs::remove_file(registry_path);
    }
}

/// Boot alpine + sleep, exec-write a marker file, `checkpoint_named("<nonce>-
/// archive")` it, `exportTo` an archive file, then remove the checkpoint (both the
/// committed image AND the registry entry) so the archive is the ONLY surviving
/// copy. `importFrom` that archive: docker's `docker load` preserves the tag
/// baked into the save file, so the effective ref round-trips UNCHANGED — the
/// opposite of msb's content-addressed import — and
/// `Container::from_checkpoint` on the result must still restore the marker.
#[tokio::test]
async fn checkpoint_archive_survives_removal_of_the_original_and_restores_the_marker() {
    require_docker!();

    let name = format!("{}-archive", nonce());
    let archive_path = std::env::temp_dir().join(format!("rz-archive-it-{}.archive", nonce()));
    let cleanup = ArchiveCleanup::new(archive_path.clone(), name.clone());

    let original = Container::new("alpine:3.19")
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(60)));
    let original_guard = original.start().await.expect("original must start");

    let write = original_guard
        .exec(&[
            "sh",
            "-c",
            "echo rz-archive-marker > /root/rz-archive-marker.txt && sync",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);

    let cp = original_guard
        .checkpoint_named(&name)
        .await
        .expect("checkpoint_named must succeed on docker via image commit");
    assert!(
        cp.checkpoint_ref.starts_with("rightsize/checkpoint:"),
        "{}",
        cp.checkpoint_ref
    );
    assert_eq!(cp.backend, "docker");

    original_guard
        .stop()
        .await
        .expect("stop the original container");

    cp.export_to(&archive_path)
        .await
        .expect("export_to must succeed while the checkpoint still exists");
    assert!(archive_path.is_file(), "export_to must produce a file");

    // Remove the checkpoint entirely — both the committed image and the registry
    // entry. From here on, the archive is the ONLY surviving copy.
    let removed = Checkpoint::remove(&name)
        .await
        .expect("remove must not error");
    assert!(removed, "remove must report the checkpoint existed");
    assert!(
        Checkpoint::find(&name)
            .await
            .expect("find must not error after removal")
            .is_none(),
        "the checkpoint must be genuinely gone before the import below"
    );

    let imported = Checkpoint::import_from(&archive_path)
        .await
        .expect("import_from must succeed from the surviving archive alone");
    cleanup.set_imported_ref(imported.checkpoint_ref.clone());
    assert_eq!(
        imported.checkpoint_ref, cp.checkpoint_ref,
        "docker's import preserves the original tag baked into the save file — the effective \
         ref round-trips unchanged"
    );
    assert_eq!(imported.backend, "docker");

    let found = Checkpoint::find(&name)
        .await
        .expect("find must not error")
        .expect("import_from must have re-registered the named checkpoint");
    assert_eq!(found.checkpoint_ref, imported.checkpoint_ref);

    let restored = Container::from_checkpoint(&imported)
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(60)));
    let restored_guard = restored
        .start()
        .await
        .expect("a container restored from an imported archive must start");

    let read = restored_guard
        .exec(&["cat", "/root/rz-archive-marker.txt"])
        .await
        .expect("exec must run in the restored container");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert!(
        read.stdout.contains("rz-archive-marker"),
        "the restored container's committed image must contain the marker file written before \
         checkpointing: stdout was {:?}",
        read.stdout
    );

    restored_guard
        .stop()
        .await
        .expect("stop the restored container");

    // `cleanup` fires on drop regardless — a harmless no-op for the registry entry
    // now that this run never re-registered under `name` again, but still the one
    // thing that removes the imported checkpoint image and the archive file.
}
