//! `sandbox-it` integration test for NAMED checkpoints
//! (`rightsize::ContainerGuard::checkpoint_named`, `rightsize::Checkpoint::find`/
//! `remove`) against the msb backend — the cross-run story this feature exists for:
//! a checkpoint taken by one guard is restored by a LATER caller using only its
//! `name`, never the `Checkpoint` value the original `checkpoint_named` call
//! returned.
//!
//! Run for real:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test named_checkpoint_it
//! ```
//!
//! Cleanup: the original (non-`keep_alive`) sandbox this test starts cleans itself
//! up automatically via the crate's own two-tier cleanup story
//! (`ContainerGuard`'s `Drop`), same as `checkpoint_it.rs`. The named checkpoint's
//! disk snapshot AND its registry entry have no automatic cleanup path (checkpoint
//! artifacts are never auto-reaped — see the checkpoints docs), so both are wired
//! through a [`NamedCheckpointCleanup`] `Drop` guard, same discipline as
//! `checkpoint_it.rs`'s `SnapshotCleanup` and `reuse_it.rs`'s `ReuseCleanup` — it
//! still runs if an assertion partway through the test panics.

#![cfg(feature = "sandbox-it")]

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
/// it — the same set-once-at-startup shape `checkpoint_it.rs`/`reuse_it.rs` already
/// use.
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

/// A per-process random-enough hex nonce (reuses `rightsize::RunId`'s own
/// time+pid-mixed generator rather than hand-rolling a second one), folded into
/// this test's checkpoint NAME — the same `RZ_TEST_NONCE` discipline
/// `reuse_it.rs`/`checkpoint_it.rs` apply to sandbox/reuse identity, applied here to
/// a named checkpoint's own identity instead: without it, a snapshot/registry entry
/// left behind by a previous crashed run could collide with this run's name.
fn nonce() -> &'static str {
    rightsize::RunId::value()
}

/// Best-effort, SYNCHRONOUS cleanup for one named checkpoint: removes the disk
/// snapshot via a direct `msb snapshot rm` (mirrors `checkpoint_it.rs`'s
/// `remove_snapshot_blocking` — a `Drop` impl cannot `.await` the async
/// `Checkpoint::remove`) and deletes the registry file directly. Wired through a
/// `Drop` guard, not a bare trailing call, so both still run if an assertion
/// partway through the test panics.
fn remove_named_checkpoint_blocking(name: &str, checkpoint_ref: &str) {
    if let Ok(msb_path) = rightsize_msb::provisioner::ensure_installed() {
        let _ = Command::new(msb_path)
            .args(["snapshot", "rm", checkpoint_ref])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    let path = rightsize::cache_dir::dir()
        .join("checkpoints")
        .join(format!("{name}.json"));
    let _ = std::fs::remove_file(path);
}

struct NamedCheckpointCleanup {
    name: String,
    checkpoint_ref: String,
}

impl Drop for NamedCheckpointCleanup {
    fn drop(&mut self) {
        remove_named_checkpoint_blocking(&self.name, &self.checkpoint_ref);
    }
}

/// Boot alpine + sleep, exec-write a marker to `/srv` (never `/tmp` — the
/// microsandbox guest mounts it as tmpfs, which a disk snapshot cannot capture),
/// `checkpoint_named("<nonce>-golden")` it, then stop the original sandbox
/// entirely. From here on, the ONLY thing this test uses to get back to that
/// checkpoint is `Checkpoint::find(name)` — never the `Checkpoint` value
/// `checkpoint_named` already returned — proving a checkpoint taken by one guard is
/// genuinely rediscoverable by a caller that never saw it, the whole point of a
/// NAMED checkpoint over an anonymous one.
#[tokio::test]
async fn a_named_checkpoint_is_rediscoverable_via_find_and_restores_the_marker() {
    require_msb!();

    let name = format!("{}-golden", nonce());

    let original = Container::new("alpine:3.19")
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(60)));
    let original_guard = original.start().await.expect("original must start");

    let write = original_guard
        .exec(&[
            "sh",
            "-c",
            "echo rz-named-checkpoint-marker > /srv/rz-named-checkpoint-marker.txt && sync",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);

    let cp = original_guard
        .checkpoint_named(&name)
        .await
        .expect("checkpoint_named must succeed on msb via disk snapshot");
    assert!(
        cp.checkpoint_ref.starts_with("rz-ckpt-"),
        "{}",
        cp.checkpoint_ref
    );
    assert_eq!(cp.backend, "microsandbox");
    // From here on, both the disk snapshot and the registry entry must be removed
    // even if a later assertion panics.
    let _cleanup = NamedCheckpointCleanup {
        name: name.clone(),
        checkpoint_ref: cp.checkpoint_ref.clone(),
    };

    original_guard
        .stop()
        .await
        .expect("stop the original sandbox");

    // The cross-run story: rediscover the checkpoint via `Checkpoint::find`, not
    // the `cp` value already in hand.
    let found = Checkpoint::find(&name)
        .await
        .expect("find must not error")
        .expect("a checkpoint just written under this name must be found");
    assert_eq!(found.checkpoint_ref, cp.checkpoint_ref);
    assert_eq!(found.backend, "microsandbox");

    let restored = Container::from_checkpoint(&found)
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(60)));
    let restored_guard = restored
        .start()
        .await
        .expect("a sandbox restored via Checkpoint::find must start");

    let read = restored_guard
        .exec(&["cat", "/srv/rz-named-checkpoint-marker.txt"])
        .await
        .expect("exec must run in the restored sandbox");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert!(
        read.stdout.contains("rz-named-checkpoint-marker"),
        "the restored sandbox's disk snapshot must contain the marker file written before \
         checkpointing: stdout was {:?}",
        read.stdout
    );

    restored_guard
        .stop()
        .await
        .expect("stop the restored sandbox");

    // `Checkpoint::remove` is the public cleanup affordance this feature adds —
    // exercise it for real here rather than relying solely on the Drop guard's own
    // direct-CLI fallback, which becomes a harmless no-op once this succeeds.
    let removed = Checkpoint::remove(&name)
        .await
        .expect("remove must not error");
    assert!(
        removed,
        "remove must report that a checkpoint existed for this name"
    );
    assert!(
        Checkpoint::find(&name)
            .await
            .expect("find must not error after removal")
            .is_none(),
        "after remove, find must report absent"
    );

    // `_cleanup` fires on drop regardless — a harmless no-op now that `remove`
    // above already tore everything down, whether we reach here normally or
    // unwound through a panic earlier.
}
