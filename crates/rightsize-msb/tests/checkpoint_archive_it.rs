//! `sandbox-it` integration test for checkpoint export/import
//! (`rightsize::Checkpoint::export_to`/`Checkpoint::import_from`) against the msb
//! backend — the portable-archive story this feature exists for: a named
//! checkpoint is exported to a plain archive file, the ORIGINAL disk snapshot and
//! registry entry are torn down entirely, and the archive alone is enough to bring
//! it back — proving the archive, not the original snapshot, is what a restore
//! actually depends on.
//!
//! Run for real:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test checkpoint_archive_it
//! ```
//!
//! Cleanup: the two ordinary (non-`keep_alive`) sandboxes this test starts clean
//! themselves up automatically via the crate's own two-tier cleanup story
//! (`ContainerGuard`'s own `Drop`), even if a mid-test assertion panics. The
//! archive file, the imported digest snapshot, and the named-checkpoint registry
//! entry have no automatic cleanup path at all (checkpoint artifacts are never
//! auto-reaped — see the checkpoints docs), so all three are wired through an
//! [`ArchiveCleanup`] `Drop` guard, same discipline as `checkpoint_it.rs`'s
//! `SnapshotCleanup` and `named_checkpoint_it.rs`'s `NamedCheckpointCleanup` — it
//! still runs if an assertion partway through the test panics.

#![cfg(feature = "sandbox-it")]

use std::cell::RefCell;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Once;
use std::time::Duration;

use rightsize::backend::BackendProvider;
use rightsize::{Checkpoint, Container, Wait};
use rightsize_msb::MsbBackendProvider;

static REGISTER: Once = Once::new();

fn msb_runtime_available() -> bool {
    if std::env::var("RIGHTSIZE_BACKEND")
        .map(|v| v.eq_ignore_ascii_case("docker"))
        .unwrap_or(false)
    {
        return false;
    }
    MsbBackendProvider.is_supported()
}

/// Registers the msb provider exactly once per process and forces
/// `RIGHTSIZE_BACKEND=microsandbox` for this process unless the caller already set
/// it — the same set-once-at-startup shape every other msb IT in this crate uses.
fn ensure_registered() {
    REGISTER.call_once(|| {
        rightsize::backends::register_provider(Box::new(MsbBackendProvider));
        if std::env::var("RIGHTSIZE_BACKEND").is_err() {
            // SAFETY-of-intent note: `std::env::set_var` is unsafe as of the 2024
            // edition; this only runs once, before any other thread in this test
            // binary has started touching Container/backends::active().
            unsafe { std::env::set_var("RIGHTSIZE_BACKEND", "microsandbox") };
        }
    });
}

macro_rules! require_msb {
    () => {
        ensure_registered();
        if !msb_runtime_available() {
            eprintln!(
                "skipping: no supported msb runtime on this host (or RIGHTSIZE_BACKEND=docker)"
            );
            return;
        }
    };
}

/// A per-process random-enough hex nonce, folded into this test's checkpoint NAME
/// and archive filename — the same `RZ_TEST_NONCE` discipline every other msb IT
/// in this crate applies, so a leftover from a previous crashed run can never
/// collide with this run's own.
fn nonce() -> &'static str {
    rightsize::RunId::value()
}

/// Best-effort, SYNCHRONOUS cleanup for one archive round trip: removes the
/// archive file, the imported digest snapshot (once known — set only after a
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
            if let Ok(msb_path) = rightsize_msb::provisioner::ensure_installed() {
                let _ = Command::new(msb_path)
                    .args(["snapshot", "rm", &effective_ref])
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
        }
        let registry_path = rightsize::cache_dir::dir()
            .join("checkpoints")
            .join(format!("{}.json", self.name));
        let _ = std::fs::remove_file(registry_path);
    }
}

/// Boot alpine + sleep, exec-write a marker to `/srv` (never `/tmp` — the
/// microsandbox guest mounts it as tmpfs, which a disk snapshot cannot capture),
/// `checkpoint_named("<nonce>-archive")` it, `exportTo` an archive file, then
/// remove the checkpoint (both the disk snapshot AND the registry entry) so the
/// archive is the ONLY surviving copy. `importFrom` that archive: msb's import is
/// content-addressed, so the effective ref is a resolved digest — never the
/// original `rz-ckpt-` name — and `Container::from_checkpoint` on the result must
/// still restore the marker.
#[tokio::test]
async fn checkpoint_archive_survives_removal_of_the_original_and_restores_the_marker() {
    require_msb!();

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
            "echo rz-archive-marker > /srv/rz-archive-marker.txt && sync",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);

    let cp = original_guard
        .checkpoint_named(&name)
        .await
        .expect("checkpoint_named must succeed on msb via disk snapshot");
    // The msb ref is the absolute artifact path under <cache_dir>/checkpoints —
    // created there via --dest-dir and restored by path.
    let ref_path = std::path::Path::new(&cp.checkpoint_ref);
    assert!(ref_path.is_absolute(), "{}", cp.checkpoint_ref);
    assert!(
        ref_path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("rz-ckpt-")),
        "{}",
        cp.checkpoint_ref
    );
    assert_eq!(cp.backend, "microsandbox");

    original_guard
        .stop()
        .await
        .expect("stop the original sandbox");

    cp.export_to(&archive_path)
        .await
        .expect("export_to must succeed while the checkpoint still exists");
    assert!(archive_path.is_file(), "export_to must produce a file");

    // Remove the checkpoint entirely — both the disk snapshot artifact and the
    // registry entry. From here on, the archive is the ONLY surviving copy.
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
    assert_ne!(
        imported.checkpoint_ref, cp.checkpoint_ref,
        "msb's load is content-addressed — the effective ref is a resolved digest, never the \
         original rz-ckpt- name"
    );
    // Digest-shaped, not merely different: msb has published both `sha256-<16hex>` (0.6.6)
    // and a bare 64-hex digest (0.6.8) for a loaded snapshot, so the prefix is optional —
    // what must hold is that the ref is a content digest. Asserting only "differs from the
    // original" would accept any renaming msb ever adopts.
    let digest_body = imported
        .checkpoint_ref
        .strip_prefix("sha256-")
        .unwrap_or(&imported.checkpoint_ref);
    assert!(
        (16..=64).contains(&digest_body.len())
            && digest_body.bytes().all(|b| b.is_ascii_hexdigit()),
        "expected msb's digest-shaped effective ref, got '{}'",
        imported.checkpoint_ref
    );
    assert_eq!(imported.backend, "microsandbox");

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
        .expect("a sandbox restored from an imported archive must start");

    let read = restored_guard
        .exec(&["cat", "/srv/rz-archive-marker.txt"])
        .await
        .expect("exec must run in the restored sandbox");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert!(
        read.stdout.contains("rz-archive-marker"),
        "the restored sandbox's disk snapshot must contain the marker file written before \
         checkpointing: stdout was {:?}",
        read.stdout
    );

    restored_guard
        .stop()
        .await
        .expect("stop the restored sandbox");

    // `cleanup` fires on drop regardless — a harmless no-op for the registry entry
    // now that this run never re-registered under `name` again, but still the one
    // thing that removes the imported digest snapshot and the archive file.
}
