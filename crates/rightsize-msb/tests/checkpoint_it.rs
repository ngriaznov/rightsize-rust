//! `sandbox-it` integration test for microsandbox's checkpoint/restore
//! (`rightsize::ContainerGuard::checkpoint`, `rightsize::Container::from_checkpoint`)
//! against the msb backend: the stop/snapshot/rm/reboot cycle, the post-checkpoint
//! wait-strategy re-run it requires, and a disk-snapshot restore into a fresh
//! sandbox.
//!
//! Run for real:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test checkpoint_it
//! ```
//!
//! Cleanup: the two ordinary (non-`keep_alive`) sandboxes this test starts clean
//! themselves up automatically via the crate's own two-tier cleanup story
//! (`ContainerGuard`'s own `Drop`), even if a mid-test assertion panics. The
//! snapshot artifact this test takes has no automatic cleanup path at all
//! (checkpoint artifacts are never auto-reaped — see the checkpoints docs), so it's
//! wired through a [`SnapshotCleanup`] `Drop` guard, same discipline as
//! `reuse_it.rs`'s `ReuseCleanup` — it still runs if an assertion partway through
//! the test panics.

#![cfg(feature = "sandbox-it")]

use std::process::{Command, Stdio};
use std::sync::Once;
use std::time::Duration;

use rightsize::backend::BackendProvider;
use rightsize::{Container, Wait};
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
/// it — the same set-once-at-startup shape `reuse_it.rs`/`network_links_it.rs`
/// already use.
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

/// Best-effort, SYNCHRONOUS `msb snapshot rm <ref>` — shells the `msb` CLI directly
/// rather than the async `SandboxBackend::remove_checkpoint` SPI, since this is
/// called from a `Drop` impl, which cannot `.await`. Mirrors the shared contract
/// suite's own `docker_rmi` helper (same rationale: a synchronous cleanup path
/// needs a synchronous tool invocation).
fn remove_snapshot_blocking(snapshot_ref: &str) {
    if let Ok(msb_path) = rightsize_msb::provisioner::ensure_installed() {
        let _ = Command::new(msb_path)
            .args(["snapshot", "rm", snapshot_ref])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// Best-effort teardown for the snapshot artifact this test takes — a `Drop` guard,
/// not a bare trailing call, so it still runs if an assertion partway through the
/// test panics (see this module's own doc for why the two sandboxes don't need the
/// same treatment).
struct SnapshotCleanup {
    snapshot_ref: String,
}

impl Drop for SnapshotCleanup {
    fn drop(&mut self) {
        remove_snapshot_blocking(&self.snapshot_ref);
    }
}

/// Boot alpine + sleep, exec-write a marker file, `checkpoint()` it — assert the
/// SAME sandbox still works afterward (exec succeeds, proving the internal
/// stop/snapshot/start cycle plus the post-checkpoint wait re-run), assert the
/// reaper ledger is untouched by that cycle, then `Container::from_checkpoint(...)`
/// a restored sandbox and confirm the marker file survived on its disk snapshot.
#[tokio::test]
async fn checkpoint_restarts_the_sandbox_and_restore_recovers_the_marker_file() {
    require_msb!();

    let original = Container::new("alpine:3.19")
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(60)));
    let original_guard = original.start().await.expect("original must start");
    let original_name = original_guard.name().to_string();

    let write = original_guard
        .exec(&[
            "sh",
            "-c",
            // /srv, not /tmp: the microsandbox guest mounts /tmp as tmpfs, which a disk snapshot cannot capture.
            "echo msb-checkpoint-marker > /srv/rz-msb-checkpoint-marker.txt && sync",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);

    // The ledger contract: same sandbox name, still owned by this run, before and
    // after the stop/snapshot/start cycle — read the raw on-disk `.sandboxes` file
    // directly (the same cross-process contract `reaper_it.rs` reads), since this
    // external test crate has no access to `rightsize`'s own private ledger API.
    let sandboxes_path = rightsize::cache_dir::dir()
        .join("runs")
        .join(format!("{}.sandboxes", rightsize::RunId::value()));
    let ledger_before =
        std::fs::read_to_string(&sandboxes_path).expect("this run's ledger file must exist");
    assert!(ledger_before.contains(&original_name), "{ledger_before}");

    let cp = original_guard
        .checkpoint()
        .await
        .expect("checkpoint must succeed on msb via disk snapshot");
    assert!(
        cp.checkpoint_ref.starts_with("rz-ckpt-"),
        "{}",
        cp.checkpoint_ref
    );
    assert_eq!(cp.backend, "microsandbox");
    // From here on, the snapshot must be removed even if a later assertion panics.
    let _snapshot_cleanup = SnapshotCleanup {
        snapshot_ref: cp.checkpoint_ref.clone(),
    };

    let ledger_after =
        std::fs::read_to_string(&sandboxes_path).expect("this run's ledger file must exist");
    assert_eq!(
        ledger_before, ledger_after,
        "the stop/snapshot/start cycle must not touch the reaper ledger — same sandbox name, \
         still owned by this run"
    );

    // The SAME sandbox: still exec-able under its original name — proves the
    // internal rm + attached re-boot from the snapshot brought it back up under
    // the same name, and `checkpoint()`'s own post-restart wait re-run already
    // confirmed it reached ready again before returning (checkpoint() would have
    // errored otherwise).
    assert_eq!(original_guard.name(), original_name);
    let still_alive = original_guard
        .exec(&["cat", "/srv/rz-msb-checkpoint-marker.txt"])
        .await
        .expect("exec must run against the SAME sandbox after checkpointing");
    assert_eq!(still_alive.exit_code, 0, "{}", still_alive.stderr);
    assert!(
        still_alive.stdout.contains("msb-checkpoint-marker"),
        "{}",
        still_alive.stdout
    );

    let restored = Container::from_checkpoint(&cp)
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(60)));
    let restored_guard = restored
        .start()
        .await
        .expect("a sandbox restored from the msb snapshot must start");
    assert_ne!(
        restored_guard.name(),
        original_name,
        "a restored container gets a fresh name, like any other start()"
    );

    let read = restored_guard
        .exec(&["cat", "/srv/rz-msb-checkpoint-marker.txt"])
        .await
        .expect("exec must run in the restored sandbox");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert!(
        read.stdout.contains("msb-checkpoint-marker"),
        "the restored sandbox's disk snapshot must contain the marker file written before \
         checkpointing: stdout was {:?}",
        read.stdout
    );

    restored_guard
        .stop()
        .await
        .expect("stop the restored sandbox");
    original_guard
        .stop()
        .await
        .expect("stop the original sandbox");

    // `_snapshot_cleanup` fires on drop, whether we reach here normally or unwound
    // through a panic above.
}
