//! Orphan-reaping `sandbox-it` integration tests against the real msb backend: a
//! SIGKILL end-to-end test (the watchdog path) and a sweep end-to-end test (the
//! init-time sweep path). See docs/reaping.md for the two-layer design.
//!
//! Rust has no `ServiceLoader`-style helper-process mechanism, so both tests re-exec
//! the CURRENT test binary as a fresh OS process (`std::env::current_exe()`), gated by
//! `RIGHTSIZE_HELPER_CHILD=1` + `--exact <helper test name>` — the same
//! spawn-a-fresh-process shape a Kotlin `java` main or a Node `node` script gives
//! those languages' equivalents of this test, adapted to how a Rust test binary is
//! actually invoked.
//!
//! Run for real:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test reaper_it
//! ```

#![cfg(feature = "sandbox-it")]

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use rightsize::backend::{BackendProvider, SandboxBackend};
use rightsize::error::Result as RightsizeResult;
use rightsize::wait::{WaitStrategy, WaitTarget};
use rightsize::{Container, RunId};
use rightsize_msb::MsbBackendProvider;

/// Generous per the addendum's "robust on slow runners" requirement — this runs on
/// both the msb-linux and msb-windows CI lanes.
const REAP_CEILING: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

fn msb_runtime_available() -> bool {
    if std::env::var("RIGHTSIZE_BACKEND")
        .map(|v| v.eq_ignore_ascii_case("docker"))
        .unwrap_or(false)
    {
        return false;
    }
    MsbBackendProvider.is_supported()
}

/// A wait strategy that's immediately satisfied — these tests only care that a
/// sandbox reaches `Running` and gets a real ledger entry, not about any workload
/// readiness signal.
struct ReadyImmediately;
#[async_trait::async_trait]
impl WaitStrategy for ReadyImmediately {
    async fn wait_until_ready(&self, _target: &dyn WaitTarget) -> RightsizeResult<()> {
        Ok(())
    }
    fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
        self
    }
}

fn unique_id(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{label}-{:x}", nanos)
}

// ---------------------------------------------------------------------------------
// SIGKILL end-to-end: a helper child process starts a real sandbox through the real
// core `Container` builder (so the reaper's ledger/watchdog wiring in
// `create_started_container` actually runs), signals readiness by writing the
// sandbox name to a file, then blocks forever. The parent SIGKILLs it and asserts the
// watchdog reaps the sandbox and deletes the run record files within `REAP_CEILING`.
// ---------------------------------------------------------------------------------

/// The helper entry point. A normal `cargo test` run treats this as a no-op (the
/// `RIGHTSIZE_HELPER_CHILD` guard) — it only does real work when re-exec'd by
/// [`sigkill_end_to_end_reaps_via_the_watchdog`] below.
#[tokio::test]
async fn helper_boots_a_sandbox_then_blocks_until_killed() {
    if std::env::var("RIGHTSIZE_HELPER_CHILD").as_deref() != Ok("1") {
        return;
    }
    rightsize::backends::register_provider(Box::new(MsbBackendProvider));

    let ready_file =
        std::env::var("RIGHTSIZE_IT_READY_FILE").expect("RIGHTSIZE_IT_READY_FILE must be set");

    let guard = Container::new("alpine:3.19")
        .with_command(&["sleep", "300"])
        .waiting_for(ReadyImmediately)
        .start()
        .await
        .expect("helper child must boot a real sandbox");

    // Signal readiness to the parent: the sandbox (and this run's ledger/watchdog)
    // now exist for real. The parent SIGKILLs this process right after seeing this
    // file, so nothing past this point matters — including `guard`'s own Drop, which
    // never gets a chance to run under a real SIGKILL.
    std::fs::write(&ready_file, RunId::value()).expect("write readiness marker");

    // Block "forever" (bounded so a leaked/un-killed helper doesn't hang CI forever
    // on its own if the parent's SIGKILL step itself never runs) — the parent kills
    // this process well within this window.
    tokio::time::sleep(Duration::from_secs(600)).await;
    drop(guard); // unreachable in the real test; keeps `guard` from an unused warning.
}

#[test]
fn sigkill_end_to_end_reaps_via_the_watchdog() {
    if !msb_runtime_available() {
        eprintln!("skipping: no supported msb runtime on this host (or RIGHTSIZE_BACKEND=docker)");
        return;
    }

    let cache_dir = std::env::temp_dir().join(format!("rz-reaper-sigkill-{}", unique_id("c")));
    std::fs::create_dir_all(&cache_dir).unwrap();
    let ready_file = cache_dir.join("ready");

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(&exe)
        .args([
            "--exact",
            "helper_boots_a_sandbox_then_blocks_until_killed",
            "--nocapture",
        ])
        .env("RIGHTSIZE_HELPER_CHILD", "1")
        .env("RIGHTSIZE_IT_READY_FILE", &ready_file)
        .env("RIGHTSIZE_CACHE_DIR", &cache_dir)
        .env("RIGHTSIZE_REAPER", "on")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn helper child process");

    let deadline = Instant::now() + REAP_CEILING;
    let mut run_id = None;
    while Instant::now() < deadline {
        if let Ok(contents) = std::fs::read_to_string(&ready_file) {
            if !contents.trim().is_empty() {
                run_id = Some(contents.trim().to_string());
                break;
            }
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    let run_id = run_id.expect("helper child never signaled readiness within the ceiling");

    // The kill itself: SIGKILL, the exact signal a bare crash/OOM-kill delivers,
    // which skips this process's own Drop-path cleanup entirely — only the
    // detached watchdog (which does NOT share this process's fate) can reap it.
    kill_process(child.id());
    let _ = child.wait();

    let record_path = cache_dir.join("runs").join(format!("{run_id}.json"));
    let sandboxes_path = cache_dir.join("runs").join(format!("{run_id}.sandboxes"));

    let deadline = Instant::now() + REAP_CEILING;
    let mut reaped = false;
    while Instant::now() < deadline {
        if !record_path.exists() && !sandboxes_path.exists() {
            reaped = true;
            break;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    assert!(
        reaped,
        "the watchdog must remove the run record files within {}s of the SIGKILL \
         (record still present: {}, sandboxes still present: {})",
        REAP_CEILING.as_secs(),
        record_path.exists(),
        sandboxes_path.exists()
    );

    let _ = std::fs::remove_dir_all(&cache_dir);
}

#[cfg(unix)]
fn kill_process(pid: u32) {
    // A real SIGKILL, not `Child::kill` (which sends SIGKILL too, but this test wants
    // the exact same external, no-Rust-cooperation kill a crashed/OOM-killed process
    // would receive — shelling out to `kill -9` matches that shape precisely, and
    // needs no `unsafe`/`libc` in this crate-free workspace).
    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
}

#[cfg(windows)]
fn kill_process(pid: u32) {
    let _ = Command::new("taskkill")
        .args(["/F", "/PID", &pid.to_string()])
        .status();
}

// ---------------------------------------------------------------------------------
// Sweep end-to-end: this test process creates a REAL sandbox directly (bypassing the
// ledger — it plays the role of "a crashed prior run"), fabricates a dead run record
// pointing at it under a fake run-id with a pid that is not running, then spawns a
// FRESH process (a real "fresh library init", since `rightsize::backends::active()`
// caches its resolution per-process) whose first backend use must trigger the sweep
// and reap the fabricated dead run's sandbox.
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn sweep_end_to_end_reaps_a_fabricated_dead_run() {
    if !msb_runtime_available() {
        eprintln!("skipping: no supported msb runtime on this host (or RIGHTSIZE_BACKEND=docker)");
        return;
    }

    let cache_dir = std::env::temp_dir().join(format!("rz-reaper-sweep-{}", unique_id("c")));
    std::fs::create_dir_all(&cache_dir).unwrap();

    let msb_path = rightsize_msb::provisioner::ensure_installed().expect("msb must be provisioned");
    let backend = rightsize_msb::MsbCliBackend::new(msb_path.clone());
    let name = unique_id("rz-deadbeef");
    let spec = rightsize::model::ContainerSpec {
        command: Some(vec!["sleep".to_string(), "300".to_string()]),
        ..rightsize::model::ContainerSpec::new(&name, "alpine:3.19", "sweep-it")
    };
    let handle = backend.create(spec).await.expect("create");
    backend
        .start(handle.as_ref())
        .await
        .expect("start the sandbox this test hand-registers as a dead run's leftover");

    // Fabricate the dead run's ledger by hand, matching the on-disk contract exactly
    // (see docs/reaping.md) — this is deliberately raw file I/O, not
    // `rightsize`'s own (private) ledger API, mirroring how a DIFFERENT process (or
    // language) would have actually produced these files.
    let runs_dir = cache_dir.join("runs");
    std::fs::create_dir_all(&runs_dir).unwrap();
    let dead_run_id = "deadrun1";
    std::fs::write(
        runs_dir.join(format!("{dead_run_id}.json")),
        format!(
            r#"{{"pid":4294967294,"startedIso":"1999-01-01T00:00:00Z","backend":"microsandbox","msbPath":{}}}"#,
            serde_json::to_string(&msb_path.display().to_string()).unwrap()
        ),
    )
    .unwrap();
    std::fs::write(
        runs_dir.join(format!("{dead_run_id}.sandboxes")),
        format!("{name}\n"),
    )
    .unwrap();

    // The fresh library init: a real child process whose first container start
    // triggers `rightsize::backends::active()` — and therefore the sweep — from
    // scratch, pointed at the SAME cache dir so it sees the fabricated dead run.
    let exe = std::env::current_exe().expect("current_exe");
    let child_cache_dir = cache_dir.clone();
    let status = Command::new(&exe)
        .args([
            "--exact",
            "helper_triggers_a_fresh_backend_resolution",
            "--nocapture",
        ])
        .env("RIGHTSIZE_HELPER_CHILD", "1")
        .env("RIGHTSIZE_CACHE_DIR", &child_cache_dir)
        .env("RIGHTSIZE_REAPER", "sweep") // sweep only: no watchdog needed for this test.
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .status()
        .expect("spawn fresh-init child process");
    assert!(status.success(), "fresh-init child must exit cleanly");

    let deadline = Instant::now() + REAP_CEILING;
    let mut reaped = false;
    while Instant::now() < deadline {
        if !runs_dir.join(format!("{dead_run_id}.json")).exists() {
            reaped = true;
            break;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    assert!(
        reaped,
        "a fresh process's init-time sweep must reap the fabricated dead run within {}s",
        REAP_CEILING.as_secs()
    );

    // Best-effort cleanup of the sandbox this test created directly, in case the
    // sweep somehow didn't reach it (assertion above already failed in that case).
    backend.remove_by_name(&name);
    let _ = std::fs::remove_dir_all(&cache_dir);
}

/// The "fresh library init" helper for the sweep test: registers the msb provider and
/// starts one trivial sandbox of its own, which is what actually triggers
/// `rightsize::backends::active()` (and therefore the sweep) inside a brand-new
/// process. Cleans up its own sandbox before exiting.
#[tokio::test]
async fn helper_triggers_a_fresh_backend_resolution() {
    if std::env::var("RIGHTSIZE_HELPER_CHILD").as_deref() != Ok("1") {
        return;
    }
    rightsize::backends::register_provider(Box::new(MsbBackendProvider));

    let guard = Container::new("alpine:3.19")
        .with_command(&["true"])
        .waiting_for(ReadyImmediately)
        .start()
        .await
        .expect("fresh-init helper must boot its own trivial sandbox");
    guard.stop().await.expect("stop the helper's own sandbox");
}
