//! The shared backend contract suite — every `SandboxBackend` must satisfy these eleven
//! behaviors identically. Parameterized by `RIGHTSIZE_BACKEND` so the same test bodies
//! run against whichever backend is active.
//!
//! Run for real, once per backend (each is its own process — `rightsize::backends::active()`
//! caches its resolution for the process lifetime, so running both backends means two
//! separate `cargo test` invocations, never two branches inside one test binary):
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker       cargo test -p rightsize-modules --features sandbox-it --test contract
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test contract
//! ```
//!
//! The eleven contract behaviors:
//!
//! - Container publishes TCP port to host loopback: [`container_publishes_tcp_port_to_host_loopback`]
//! - Env vars are visible to the workload: [`env_vars_are_visible_to_the_workload`]
//! - Exec returns real exit codes and stderr: [`exec_returns_real_exit_codes_and_stderr`]
//! - Logs capture workload stdout and forLogMessage waits on them: [`logs_capture_workload_stdout_and_for_log_message_waits_on_them`]
//! - Stop terminates and frees the container: [`stop_terminates_and_frees_the_container`]
//! - withCopyFileToContainer round-trips a bundled resource and a host path: [`with_copy_file_to_container_round_trips_a_bundled_resource_and_a_host_path`]
//! - A builder mount is read-write by default and the guest write reaches the host file: [`a_mount_is_read_write_by_default_and_the_guest_write_reaches_the_host_file`]
//! - followOutput streams lines in order and close halts delivery: [`follow_output_streams_lines_in_order_and_close_halts_delivery`]
//! - followOutput delivers a final unterminated line after the workload exits: [`follow_output_delivers_a_final_unterminated_line_after_the_workload_exits_exactly_once`]
//! - Starting a container writes the reaping run record: [`starting_a_container_writes_the_reaping_run_record`]
//! - A fabricated dead run's sandbox is reaped by a fresh process's sweep, provably
//!   gone at the backend level: [`sweep_reaps_a_fabricated_dead_run_at_the_backend_level`]
//!
//! Plus four container-reuse behaviors (`Container::reuse`), each run in a spawned
//! child process so its `RIGHTSIZE_REUSE`/`RIGHTSIZE_CACHE_DIR` env is deterministic
//! and never races this binary's other parallel tests — no `RIGHTSIZE_REUSE` needs
//! to be set for the `cargo test` invocation itself, each scenario sets its own:
//!
//! - Double opt-in gating: env-disabled falls through to an ordinary container: [`reuse_double_opt_in_gating_env_disabled_runs_as_an_ordinary_ephemeral_container`]
//! - The pinned cross-language identity hash vector produces the contract sandbox name: [`reuse_pinned_cross_language_hash_vector_produces_the_contract_sandbox_name`]
//! - Adopt: a second equivalent container reuses the first one's live sandbox and mapped port: [`reuse_adopt_reuses_the_same_sandbox_and_mapped_port_across_two_starts`]
//! - Stop semantics: the sandbox is left running and never ledgered: [`reuse_stop_semantics_leaves_the_sandbox_running_and_never_ledgers_it`]
//!
//! Plus three failure-diagnostics / isolation-requirement behaviors:
//!
//! - Diagnostics report format: a live container's report section renders in the
//!   pinned cross-language shape: [`diagnostics_report_reflects_a_live_container_in_the_pinned_cross_language_format`]
//! - Capabilities: each backend reports its pinned `hardware_isolated`/`checkpoint`
//!   values: [`capabilities_report_the_pinned_per_backend_values`]
//! - requireIsolation gating: refuses to start on a non-isolated backend, starts
//!   normally on a hardware-isolated one: [`require_isolation_gates_start_per_backend_hardware_isolation`]
//!
//! Plus two checkpoint/restore behaviors:
//!
//! - Both backends succeed with a well-formed, backend-specific ref:
//!   [`checkpoint_succeeds_on_both_backends_with_a_well_formed_backend_specific_ref`]
//! - Restore (docker-only — microsandbox has its own dedicated test in
//!   `rightsize-msb/tests/checkpoint_it.rs`): a marker file written after boot
//!   survives checkpoint -> stop -> restore:
//!   [`checkpoint_restore_round_trips_a_marker_file_written_after_boot`]
//!
//! Plus five runtime-copy behaviors (`ContainerGuard::copy_file_to_container` /
//! `copy_content_to_container` / `copy_file_from_container`):
//!
//! - Copy a host file in, to an absent parent: [`copy_file_to_container_round_trips_a_host_file_into_an_absent_parent`]
//! - Copy in-memory content in: [`copy_content_to_container_round_trips_in_memory_bytes`]
//! - Copy a directory in: [`copy_file_to_container_round_trips_a_directory`]
//! - Copy a guest file out, to an absent parent: [`copy_file_from_container_round_trips_a_guest_file`]
//! - Copy a guest directory out: [`copy_file_from_container_round_trips_a_guest_directory`]
//!
//! Mounts are read-write by default, on both backends alike, and a guest write reaches
//! the host file behind the mount: see
//! [`a_mount_is_read_write_by_default_and_the_guest_write_reaches_the_host_file`].
//!
//! Plus five behaviors for the msb-0.6.8 spec options (`with_disk_limit`,
//! `with_tmpfs_root`, `with_network_disabled`) — docker ignores all three and runs
//! with its normal disk-backed rootfs and normal networking, proven by these same
//! tests running unmodified on both backends:
//!
//! - A network-disabled container still serves its published port, and on msb
//!   cannot reach the public internet: [`a_network_disabled_container_serves_its_published_port`]
//! - A tmpfs-root container boots and accepts writes: [`a_tmpfs_root_container_boots_and_accepts_writes`]
//! - A disk-limited container boots: [`a_disk_limited_container_boots`]
//! - Checkpointing a tmpfs-root container is refused (msb-only — docker ignores
//!   `tmpfs_root_mb`): [`checkpointing_a_tmpfs_root_container_is_refused`]
//! - A checkpoint stores its artifact under the cache dir and restores from it
//!   (msb-only — docker checkpoints are image tags, not filesystem artifacts):
//!   [`a_checkpoint_stores_its_artifact_under_the_cache_dir_and_restores_from_it`]

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rightsize::backend::{BackendProvider, SandboxBackend};
use rightsize::model::{ContainerSpec, PortBinding};
use rightsize::{Container, MountableFile, Wait};
// Feature-gated like the lib's own registration: `rightsize-docker` is unix-only, and
// a Windows build of this suite selects `--no-default-features --features
// backend-msb,sandbox-it` (see the msb-windows CI lane), so the crate is never linked
// there and only the msb arm of `raw_backend_for` exists.
#[cfg(feature = "backend-docker")]
use rightsize_docker::DockerBackendProvider;
use rightsize_msb::MsbBackendProvider;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

/// Container publishes a TCP port to host loopback (a Python http.server, `for_http`).
#[tokio::test]
async fn container_publishes_tcp_port_to_host_loopback() {
    require_backend!();
    let c = Container::new("python:3.12-alpine")
        .with_command(&["python", "-m", "http.server", "8000"])
        .with_exposed_ports(&[8000])
        .waiting_for(
            // 120s: shared CI runners boot a microVM + python noticeably slower
            // than dev hardware; the default 60s flakes there.
            Wait::for_http("/")
                .for_port(8000)
                .with_startup_timeout(Duration::from_secs(120)),
        );
    let guard = c.start().await.expect("container must start");

    let url = format!("http://127.0.0.1:{}/", guard.get_mapped_port(8000).unwrap());
    let status = support::http_agent()
        .get(&url)
        .call()
        .expect("GET / must succeed")
        .status();
    assert_eq!(status.as_u16(), 200);

    guard.stop().await.unwrap();
}

/// Env vars set with `with_env` are visible to the workload via exec.
#[tokio::test]
async fn env_vars_are_visible_to_the_workload() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_env("RZ_PROBE", "hello-rz")
        .with_command(&["sh", "-c", "sleep 120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("container must start");

    let r = guard
        .exec(&["sh", "-c", "echo $RZ_PROBE"])
        .await
        .expect("exec must run");
    assert_eq!(r.exit_code, 0);
    assert!(r.stdout.contains("hello-rz"), "stdout was: {}", r.stdout);

    guard.stop().await.unwrap();
}

/// Exec surfaces real exit codes and stderr.
#[tokio::test]
async fn exec_returns_real_exit_codes_and_stderr() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("container must start");

    let r = guard
        .exec(&["sh", "-c", "echo oops >&2; exit 7"])
        .await
        .expect("exec must run");
    assert_eq!(r.exit_code, 7);
    assert!(r.stderr.contains("oops"), "stderr was: {}", r.stderr);

    guard.stop().await.unwrap();
}

/// Logs capture workload stdout, and `for_log_message` waits on them.
#[tokio::test]
async fn logs_capture_workload_stdout_and_for_log_message_waits_on_them() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&["sh", "-c", "echo BOOT-MARKER; sleep 120"])
        .waiting_for(Wait::for_log_message(".*BOOT-MARKER.*", 1));
    let guard = c.start().await.expect("container must start");

    let logs = guard.logs().await.expect("logs must be readable");
    assert!(logs.contains("BOOT-MARKER"), "logs were: {logs}");

    guard.stop().await.unwrap();
}

/// Stop terminates the container and frees it — exec after stop errors.
///
/// `ContainerGuard::stop(self)` takes the guard by value and consumes it, so reuse
/// after stop is a compile error rather than a runtime exception. That ownership-level
/// guarantee is exactly what
/// U3/U7/U8 unit-test in `crates/rightsize/src/container.rs` against a fake backend
/// (exec/`get_mapped_port` on a not-yet-started guard errors naming the cause; stop is
/// idempotent). This IT instead proves the **real-backend** side of "stop frees the
/// container": the published port genuinely stops accepting connections once the
/// backend has actually torn the container down, on both real runtimes.
#[tokio::test]
async fn stop_terminates_and_frees_the_container() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&[
            "sh",
            "-c",
            "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\nok' | nc -l -p 8000; done",
        ])
        .with_exposed_ports(&[8000])
        .waiting_for(
            // 120s: shared CI runners boot a microVM + python noticeably slower
            // than dev hardware; the default 60s flakes there.
            Wait::for_http("/").for_port(8000).with_startup_timeout(Duration::from_secs(120)),
        );
    let guard = c.start().await.expect("container must start");
    assert!(guard.is_running());
    let host_port = guard.get_mapped_port(8000).unwrap();
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", host_port)).is_ok(),
        "port must be reachable while running"
    );

    guard.stop().await.expect("stop must succeed");

    // Give the backend a moment to actually tear the container/sandbox down, then
    // prove the port is genuinely gone — not merely that this guard is unusable.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", host_port)).is_err(),
        "port must no longer be reachable after stop() — container/sandbox must be torn down"
    );
}

/// The reaping ledger's run record ([`docs/reaping.md`](../../../docs/reaping.md))
/// exists once any container has started in this process, naming this process's real
/// pid — the cross-language contract both backends satisfy identically (the record's
/// `backend` field is what lets the init-time sweep tell an msb run's leftovers apart
/// from a docker run's; see `crate::reaper::sweep` in the core crate).
#[tokio::test]
async fn starting_a_container_writes_the_reaping_run_record() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("container must start");

    let record_path = rightsize::cache_dir::dir()
        .join("runs")
        .join(format!("{}.json", rightsize::RunId::value()));
    let raw = std::fs::read_to_string(&record_path)
        .unwrap_or_else(|e| panic!("run record must exist at {record_path:?}: {e}"));
    let parsed: serde_json::Value = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("run record must be valid JSON: {e}\n{raw}"));
    assert_eq!(
        parsed.get("pid").and_then(serde_json::Value::as_u64),
        Some(u64::from(std::process::id())),
        "run record must name this process's own pid: {raw}"
    );

    guard.stop().await.unwrap();
}

/// `with_copy_file_to_container` round-trips a bundled resource and a host path into
/// the guest, at distinct parent directories (msb 0.6.2 silently fails to wire up two
/// `--mount-file` targets sharing a parent directory — routed around here rather than
/// pinned as a known limitation).
#[tokio::test]
async fn with_copy_file_to_container_round_trips_a_bundled_resource_and_a_host_path() {
    require_backend!();
    let bundled_bytes = "rightsize-mount-fixture-payload\n";
    let host_file =
        std::env::temp_dir().join(format!("rightsize-hostpath-{}.txt", std::process::id()));
    let host_bytes = "rightsize-host-path-payload\n";
    std::fs::write(&host_file, host_bytes).unwrap();

    let bundled = MountableFile::for_classpath_resource("tests/fixtures/rightsize-fixture.txt")
        .expect("bundled fixture must resolve");
    let host_mount = MountableFile::for_host_path(host_file.to_str().unwrap());

    let c = Container::new("alpine:3.19")
        .with_copy_file_to_container(bundled, "/mnt-a/from-classpath.txt")
        .with_copy_file_to_container(host_mount, "/mnt-b/from-host.txt")
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("container must start");

    let from_bundled = guard
        .exec(&["cat", "/mnt-a/from-classpath.txt"])
        .await
        .expect("exec must run");
    assert_eq!(
        from_bundled.exit_code, 0,
        "cat bundled mount failed: {}",
        from_bundled.stderr
    );
    assert_eq!(from_bundled.stdout, bundled_bytes);

    let from_host = guard
        .exec(&["cat", "/mnt-b/from-host.txt"])
        .await
        .expect("exec must run");
    assert_eq!(
        from_host.exit_code, 0,
        "cat host-path mount failed: {}",
        from_host.stderr
    );
    assert_eq!(from_host.stdout, host_bytes);

    guard.stop().await.unwrap();
    std::fs::remove_file(&host_file).ok();
}

/// A mount made through the builder is read-write (`FileMount::new`'s default), and the
/// guest write lands on the host file behind it — identically on both backends. The
/// mount is a view of the host file, not a copy of it: docker binds the host path
/// directly and msb hard-links it into its staging directory, so the same inode is on
/// both sides. Callers who need the host copy protected build the mount with
/// `FileMount::read_only`.
#[tokio::test]
async fn a_mount_is_read_write_by_default_and_the_guest_write_reaches_the_host_file() {
    require_backend!();
    let host_file = std::env::temp_dir().join(format!("rightsize-rw-{}.txt", std::process::id()));
    std::fs::write(&host_file, "seed\n").unwrap();
    // Two fixture choices keep this test measuring the mount's access mode and nothing
    // else — both denials below are EACCES, where a read-only mount denies with EROFS,
    // so leaving either in place would let a permission failure impersonate `:ro`
    // (exactly what this test's predecessor did).
    //
    // World-writable host file: a docker daemon running rootless or with userns-remap
    // maps container root to an unprivileged host uid, which mode 0644 would block.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&host_file, std::fs::Permissions::from_mode(0o666)).unwrap();
    }

    // Guest path outside /tmp: alpine's /tmp is a world-writable sticky directory, and
    // Ubuntu kernels ship fs.protected_regular, which denies the shell redirection's
    // O_CREAT open of another uid's file in such a directory — for root too, mode bits
    // notwithstanding. Reproduced directly: root, a 0666 file owned by another uid,
    // sysctl 2 — `> /tmp/f` is EACCES while the same write to /srv succeeds.
    let c = Container::new("alpine:3.19")
        .with_copy_file_to_container(
            MountableFile::for_host_path(host_file.to_str().unwrap()),
            "/srv/mounted.txt",
        )
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("container must start");

    let write = guard
        .exec(&["sh", "-c", "echo overwritten > /srv/mounted.txt && sync"])
        .await
        .expect("exec must run");
    assert_eq!(
        write.exit_code, 0,
        "a default mount must accept an in-guest write; stderr={}",
        write.stderr
    );

    let read_back = guard
        .exec(&["cat", "/srv/mounted.txt"])
        .await
        .expect("exec must run");
    assert!(
        read_back.stdout.contains("overwritten"),
        "the write must be visible in the guest: {:?}",
        read_back.stdout
    );

    guard.stop().await.unwrap();
    std::fs::remove_file(&host_file).ok();
}

/// `follow_output` streams live lines in order, and `close()` halts delivery.
#[tokio::test]
async fn follow_output_streams_lines_in_order_and_close_halts_delivery() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&[
            "sh",
            "-c",
            "i=1; while [ $i -le 20 ]; do echo LINE-$i; i=$((i+1)); sleep 0.2; done; sleep 120",
        ])
        .waiting_for(Wait::for_log_message("LINE-1", 0));
    let guard = c.start().await.expect("container must start");

    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let saw_three = Arc::new(tokio::sync::Notify::new());
    let saw_three_notify = saw_three.clone();
    let saw_three_fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw_three_fired_writer = saw_three_fired.clone();
    let follow = guard
        .follow_output(move |line| {
            let mut lines = received_clone.lock().unwrap();
            lines.push(line);
            if lines.iter().filter(|l| l.contains("LINE-")).count() >= 3
                && !saw_three_fired_writer.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                saw_three_notify.notify_one();
            }
        })
        .await
        .expect("follow_output must start");

    let waited = tokio::time::timeout(Duration::from_secs(20), saw_three.notified()).await;
    assert!(
        waited.is_ok() || saw_three_fired.load(std::sync::atomic::Ordering::SeqCst),
        "did not observe 3 streamed lines in time; received so far: {:?}",
        received.lock().unwrap()
    );

    let line_numbers: Vec<i32> = received
        .lock()
        .unwrap()
        .iter()
        .filter(|l| l.contains("LINE-"))
        .filter_map(|l| l.strip_prefix("LINE-").and_then(|n| n.parse().ok()))
        .collect();
    let mut sorted = line_numbers.clone();
    sorted.sort_unstable();
    assert_eq!(
        line_numbers, sorted,
        "streamed lines arrived out of order: {line_numbers:?}"
    );

    follow.close();

    let count_after_close = received.lock().unwrap().len();
    tokio::time::sleep(Duration::from_secs(1)).await; // workload keeps emitting
    assert_eq!(
        count_after_close,
        received.lock().unwrap().len(),
        "follow_output kept delivering after close()"
    );

    guard.stop().await.unwrap();
}

/// A workload whose final output is an unterminated line (no trailing
/// `\n`) must have that line delivered exactly once once the workload — and therefore
/// the follow stream — ends on its own (not via `close()`), on both backends' natural
/// stream-end path (docker's frame-stream end; msb's watchdog quiescing the stuck
/// `logs -f` and doing an at-most-once tail replay). This is the duplicate-delivery
/// regression guard: the msb watchdog races a live reader against the replay, and an
/// unsynchronized snapshot of `delivered` can double-deliver lines still sitting in
/// the pipe at stop-time.
///
/// The `sleep 2` between the boot marker and the final fragment exists because msb's
/// `start()` polls `msb ls` for the sandbox to reach Running before returning, so a
/// workload that exits immediately can race that poll.
#[tokio::test]
async fn follow_output_delivers_a_final_unterminated_line_after_the_workload_exits_exactly_once() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&[
            "sh",
            "-c",
            "echo BOOT-MARKER; sleep 2; \
             i=1; while [ $i -le 5 ]; do echo LINE-$i; i=$((i+1)); done; \
             printf 'LINE-END-NO-NEWLINE'",
        ])
        .waiting_for(Wait::for_log_message(".*BOOT-MARKER.*", 1));
    let guard = c.start().await.expect("container must start");

    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let saw_final = Arc::new(tokio::sync::Notify::new());
    let saw_final_notify = saw_final.clone();
    let saw_final_fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let saw_final_fired_writer = saw_final_fired.clone();
    let follow = guard
        .follow_output(move |line| {
            let mut lines = received_clone.lock().unwrap();
            let is_final = line.contains("LINE-END-NO-NEWLINE");
            lines.push(line);
            if is_final && !saw_final_fired_writer.swap(true, std::sync::atomic::Ordering::SeqCst) {
                saw_final_notify.notify_one();
            }
        })
        .await
        .expect("follow_output must start");

    let waited = tokio::time::timeout(Duration::from_secs(20), saw_final.notified()).await;
    assert!(
        waited.is_ok() || saw_final_fired.load(std::sync::atomic::Ordering::SeqCst),
        "final unterminated line was never delivered; received so far: {:?}",
        received.lock().unwrap()
    );

    // Settle window: let any duplicate/late delivery surface.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let lines = received.lock().unwrap().clone();
    let final_count = lines
        .iter()
        .filter(|l| l.contains("LINE-END-NO-NEWLINE"))
        .count();
    assert_eq!(
        final_count, 1,
        "final unterminated line was delivered more than once: {lines:?}"
    );
    for n in 1..=5 {
        let marker = format!("LINE-{n}");
        let count = lines.iter().filter(|l| *l == &marker).count();
        assert_eq!(
            count, 1,
            "LINE-{n} was not delivered exactly once: {lines:?}"
        );
    }

    follow.close();
    guard.stop().await.unwrap();
}

/// Generous per the addendum's "robust on slow runners" requirement (same ceiling
/// `crates/rightsize-msb/tests/reaper_it.rs` uses for its own msb-only sweep test).
const SWEEP_REAP_CEILING: Duration = Duration::from_secs(60);
const SWEEP_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Constructs a raw backend instance directly via its `BackendProvider`, bypassing
/// `rightsize::backends::active()` entirely — the same "hand-register a real
/// sandbox as though a different (dead) process created it" shape
/// `crates/rightsize-msb/tests/reaper_it.rs`'s sweep test uses, generalized across
/// both backends so this test can run through the shared contract suite.
fn raw_backend_for(name: &str) -> Box<dyn SandboxBackend> {
    if name == "docker" {
        #[cfg(feature = "backend-docker")]
        {
            return DockerBackendProvider
                .create()
                .expect("docker backend must construct");
        }
        #[cfg(not(feature = "backend-docker"))]
        // Unreachable in practice: without the feature, `requested_backend_available`
        // reports docker unsupported and every test gated on it has already skipped.
        panic!("docker requested but the backend-docker feature is not compiled in");
    }
    MsbBackendProvider.create().expect(
        "msb backend must construct — provisioning must already have \
                 happened via an earlier test in this binary",
    )
}

/// Sweep behavior, run through the shared contract suite so BOTH backends get
/// equivalent coverage (the addendum's "common trap" note: msb's own coverage in
/// `reaper_it.rs` only proves the ledger's bookkeeping is cleaned up, never docker's
/// real sweep path against a real daemon). A fabricated dead run record pointing at
/// a real, running sandbox is reaped by a FRESH process's init-time sweep — proven
/// gone at the BACKEND level (the port it was serving on stops accepting
/// connections), not just that the ledger's own files were deleted.
#[tokio::test]
async fn sweep_reaps_a_fabricated_dead_run_at_the_backend_level() {
    require_backend!();

    let backend_name = rightsize::backends::active_name();
    let raw_backend = raw_backend_for(&backend_name);

    let cache_dir = std::env::temp_dir().join(format!(
        "rz-contract-sweep-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&cache_dir).unwrap();

    // A host port this test picks for itself (raw backend calls bypass the core
    // free-port allocator entirely — see `ContainerSpec::ports`'s doc: a backend
    // binds already-chosen ports, it never allocates).
    let host_port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port to reserve one")
        .local_addr()
        .unwrap()
        .port();
    let name = format!(
        "rz-contractsweep-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let spec = ContainerSpec {
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\nok' | \
             nc -l -p 8000; done"
                .to_string(),
        ]),
        ports: vec![PortBinding {
            host_port,
            guest_port: 8000,
        }],
        ..ContainerSpec::new(&name, "alpine:3.19", "contract-sweep-it")
    };
    let handle = raw_backend
        .create(spec)
        .await
        .expect("create a real sandbox to fabricate as a dead run's leftover");
    raw_backend
        .start(handle.as_ref())
        .await
        .expect("start the sandbox this test hand-registers as a dead run's leftover");

    // Give the sandbox a real chance to boot before fabricating the dead run around
    // it — 120s: shared CI runners boot a microVM + alpine noticeably slower than
    // dev hardware, same budget `container_publishes_tcp_port_to_host_loopback` uses.
    let boot_deadline = Instant::now() + Duration::from_secs(120);
    let mut booted = false;
    while Instant::now() < boot_deadline {
        if std::net::TcpStream::connect(("127.0.0.1", host_port)).is_ok() {
            booted = true;
            break;
        }
        std::thread::sleep(SWEEP_POLL_INTERVAL);
    }
    assert!(
        booted,
        "the fabricated sandbox must actually boot and accept connections on port \
         {host_port} before the sweep test proceeds"
    );

    // Fabricate the dead run's ledger by hand, matching the on-disk contract exactly
    // (see docs/reaping.md) — deliberately raw file I/O, not rightsize's own
    // (private) ledger API, mirroring how a DIFFERENT process (or language) would
    // actually have produced these files.
    let runs_dir = cache_dir.join("runs");
    std::fs::create_dir_all(&runs_dir).unwrap();
    let dead_run_id = "contractdeadrun1";
    std::fs::write(
        runs_dir.join(format!("{dead_run_id}.json")),
        format!(
            r#"{{"pid":4294967294,"startedIso":"1999-01-01T00:00:00Z","backend":"{backend_name}"}}"#
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
    // scratch, pointed at the SAME cache dir (so it finds the fabricated dead run)
    // and the SAME backend (the sweep only reaps a dead run whose recorded backend
    // matches its own).
    let exe = std::env::current_exe().expect("current_exe");
    let status = Command::new(&exe)
        .args([
            "--exact",
            "helper_triggers_a_fresh_backend_resolution_for_the_sweep_contract_test",
            "--nocapture",
        ])
        .env("RIGHTSIZE_HELPER_CHILD", "1")
        .env("RIGHTSIZE_CACHE_DIR", &cache_dir)
        .env("RIGHTSIZE_REAPER", "sweep") // sweep only: no watchdog needed for this test.
        .env("RIGHTSIZE_BACKEND", &backend_name)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .status()
        .expect("spawn fresh-init child process");
    assert!(status.success(), "fresh-init child must exit cleanly");

    // Prove the sandbox is gone AT THE BACKEND LEVEL, not just that the ledger's own
    // bookkeeping was cleaned up: the port it was serving on must stop accepting
    // connections.
    let reap_deadline = Instant::now() + SWEEP_REAP_CEILING;
    let mut gone = false;
    while Instant::now() < reap_deadline {
        if std::net::TcpStream::connect(("127.0.0.1", host_port)).is_err() {
            gone = true;
            break;
        }
        std::thread::sleep(SWEEP_POLL_INTERVAL);
    }
    assert!(
        gone,
        "a fresh process's init-time sweep must reap the fabricated dead run's sandbox at \
         the backend level (port {host_port} must stop accepting connections) within {}s",
        SWEEP_REAP_CEILING.as_secs()
    );

    // The ledger's own bookkeeping must be cleaned up too.
    assert!(
        !runs_dir.join(format!("{dead_run_id}.json")).exists(),
        "the sweep must also delete the fabricated dead run's ledger files"
    );

    // Best-effort cleanup of the sandbox this test created directly, in case the
    // sweep somehow didn't reach it (the assertion above already failed in that case).
    raw_backend.remove_by_name(&name);
    let _ = std::fs::remove_dir_all(&cache_dir);
}

/// The "fresh library init" helper for the sweep contract test: registers both
/// backend providers and starts one trivial sandbox of its own — on whichever
/// backend `RIGHTSIZE_BACKEND` names, the same one the parent resolved — which is
/// what actually triggers `rightsize::backends::active()` (and therefore the sweep)
/// inside a brand-new process pointed at the parent's cache dir. Cleans up its own
/// sandbox before exiting. A normal `cargo test` run treats this as a no-op (the
/// `RIGHTSIZE_HELPER_CHILD` guard) — it only does real work when re-exec'd by
/// [`sweep_reaps_a_fabricated_dead_run_at_the_backend_level`] above.
#[tokio::test]
async fn helper_triggers_a_fresh_backend_resolution_for_the_sweep_contract_test() {
    if std::env::var("RIGHTSIZE_HELPER_CHILD").as_deref() != Ok("1") {
        return;
    }
    support::ensure_registered();

    let guard = Container::new("alpine:3.19")
        .with_command(&["true"])
        .start()
        .await
        .expect("fresh-init helper must boot its own trivial sandbox");
    guard.stop().await.expect("stop the helper's own sandbox");
}

// =================== Failure diagnostics / isolation requirement ==================

/// Diagnostics report format contract: a container this process has registered as
/// running appears in `rightsize::diagnostics()`'s report in the exact
/// cross-language format pinned by `crates/rightsize/src/diagnostics.rs`'s golden
/// fixture. That fixture itself is Rust-internal — `render`/`register`/`deregister`
/// are all `pub(crate)`, unreachable from this external contract crate — so this
/// proves the same contract at the observable-behavior level instead: start a real
/// container, check ITS OWN report section renders with the pinned header/state-line
/// shape, then stop it and check that section is gone. Other tests in this binary
/// register their own containers into the same process-wide registry concurrently, so
/// this asserts only on this container's own lines, never on exact report equality.
#[tokio::test]
async fn diagnostics_report_reflects_a_live_container_in_the_pinned_cross_language_format() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&["sh", "-c", "echo DIAG-CONTRACT-MARKER; sleep 120"])
        .waiting_for(Wait::for_log_message(".*DIAG-CONTRACT-MARKER.*", 1));
    let guard = c.start().await.expect("container must start");
    let name = guard.name().to_string();

    let report = rightsize::diagnostics().await;
    assert!(
        report.contains(&format!("-- {name} (alpine:3.19) --")),
        "report must contain this container's pinned header line: {report}"
    );
    assert!(
        report.contains("state: running   host: 127.0.0.1   ports:"),
        "report must contain the pinned state/host/ports line: {report}"
    );
    assert!(
        report.contains("last 50 log lines:") || report.contains("logs: unavailable ("),
        "report must contain either a log tail or a degraded-logs line: {report}"
    );

    guard.stop().await.unwrap();

    let after_stop = rightsize::diagnostics().await;
    assert!(
        !after_stop.contains(&format!("-- {name} (alpine:3.19) --")),
        "a stopped container must be deregistered from the report: {after_stop}"
    );
}

/// Capabilities contract: the ACTIVE backend's `capabilities()` reports the pinned
/// per-backend values — msb (`"microsandbox"`) is hardware-isolated (its own kernel
/// per sandbox) AND checkpoint-capable via disk snapshots, whose mechanism restarts
/// the sandbox's workload; docker is not hardware-isolated (shared host kernel) but
/// is checkpoint-capable via image commit, which leaves the container undisturbed.
/// Proven against the real backend instance this contract suite is parameterized
/// over.
#[tokio::test]
async fn capabilities_report_the_pinned_per_backend_values() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();
    let backend = raw_backend_for(&backend_name);
    let caps = backend.capabilities();

    assert!(caps.checkpoint, "both real backends support checkpointing");
    if backend_name == "docker" {
        assert!(
            !caps.hardware_isolated,
            "docker must not report hardware isolation"
        );
        assert!(
            !caps.checkpoint_restarts_workload,
            "an image commit leaves the container undisturbed"
        );
    } else {
        assert!(caps.hardware_isolated, "msb must report hardware isolation");
        assert!(
            caps.checkpoint_restarts_workload,
            "the stop/snapshot/start cycle reboots the guest"
        );
    }
}

/// requireIsolation gating contract: `.require_isolation(true)` refuses to start
/// (`RightsizeError::IsolationRequired`, naming the active backend and the
/// `RIGHTSIZE_BACKEND=microsandbox` remedy) on a backend without hardware isolation, and
/// starts normally on one that has it — proven against the real backend this
/// contract suite is parameterized over, not the fake backend
/// `crates/rightsize/src/container.rs`'s own unit tests use.
#[tokio::test]
async fn require_isolation_gates_start_per_backend_hardware_isolation() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();

    let c = Container::new("alpine:3.19")
        .with_command(&["sleep", "30"])
        .require_isolation(true);

    if backend_name == "docker" {
        // `ContainerGuard` (the `Ok` side) has no `Debug` impl, so `Result::expect_err`
        // is unavailable here — match explicitly instead.
        let err = match c.start().await {
            Ok(_) => {
                panic!("require_isolation(true) must refuse to start on a non-isolated backend")
            }
            Err(e) => e,
        };
        assert!(
            matches!(err, rightsize::RightsizeError::IsolationRequired { .. }),
            "{err}"
        );
        let msg = err.to_string();
        assert!(msg.contains("docker"), "{msg}");
        assert!(msg.contains("RIGHTSIZE_BACKEND=microsandbox"), "{msg}");
    } else {
        let guard = c
            .start()
            .await
            .expect("require_isolation(true) must start normally on a hardware-isolated backend");
        guard.stop().await.unwrap();
    }
}

// ============================ checkpoint / restore =================================

/// checkpoint contract: both real backends succeed and return a well-formed,
/// backend-specific ref — docker tags `rightsize/checkpoint:<12hex>`, microsandbox
/// mints an absolute `<cache_dir>/checkpoints/rz-ckpt-<12hex>` artifact path (the
/// snapshot is created there via `--dest-dir` and restored by path) — and
/// `Checkpoint::backend` names the backend that created it. Cleans up via
/// `SandboxBackend::remove_checkpoint` (SPI-only — no shelling out to either CLI
/// directly), keeping shared CI backend state clean.
#[tokio::test]
async fn checkpoint_succeeds_on_both_backends_with_a_well_formed_backend_specific_ref() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();

    let c = Container::new("alpine:3.19")
        .with_command(&["sleep", "60"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("container must start");

    let cp = guard
        .checkpoint()
        .await
        .expect("checkpoint must succeed on both real backends");
    assert_eq!(cp.backend, backend_name);

    let artifact_name = if backend_name == "docker" {
        cp.checkpoint_ref
            .strip_prefix("rightsize/checkpoint:")
            .unwrap_or_else(|| {
                panic!(
                    "expected the rightsize/checkpoint: prefix, got {}",
                    cp.checkpoint_ref
                )
            })
            .to_string()
    } else {
        let ref_path = std::path::Path::new(&cp.checkpoint_ref);
        assert!(
            ref_path.is_absolute(),
            "expected an absolute artifact path ref, got {}",
            cp.checkpoint_ref
        );
        assert_eq!(
            ref_path.parent().and_then(|p| p.file_name()),
            Some(std::ffi::OsStr::new("checkpoints")),
            "expected the ref to sit in a 'checkpoints' dir: {}",
            cp.checkpoint_ref
        );
        let name = ref_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_else(|| panic!("unreadable artifact name in {}", cp.checkpoint_ref));
        name.strip_prefix("rz-ckpt-")
            .unwrap_or_else(|| panic!("expected the rz-ckpt- prefix, got {}", cp.checkpoint_ref))
            .to_string()
    };
    assert_eq!(artifact_name.len(), 12, "{}", cp.checkpoint_ref);
    assert!(
        artifact_name.chars().all(|c| c.is_ascii_hexdigit()),
        "{}",
        cp.checkpoint_ref
    );

    let backend = raw_backend_for(&backend_name);
    let _ = backend.remove_checkpoint(&cp.checkpoint_ref).await;
    guard.stop().await.unwrap();
}

/// checkpoint gating contract: a backend without `capabilities().checkpoint` (a
/// test double only — both real backends have it) refuses `checkpoint()` with
/// `RightsizeError::CheckpointUnsupported`, naming the backend, BEFORE any backend
/// call. Proven at the unit level with a fake backend
/// (`crates/rightsize/src/container.rs`'s own tests) — nothing left to prove here
/// against a real backend, since both real ones now succeed (see the test above).
///
/// restoring under a different backend than the creator, and reuse combined with
/// `from_checkpoint`, are also unit-tested with fakes — see
/// `from_checkpoint_under_a_different_backend_refuses_before_any_backend_call` and
/// `reuse_combined_with_from_checkpoint_is_a_config_error` in that same module; both
/// checks run entirely before any backend call, so a real-backend contract test
/// would add no further coverage.
///
/// The dedicated checkpoint/restore integration test (docker-only — microsandbox
/// gets its own dedicated test in `rightsize-msb`, since its restore proves a
/// different mechanism: the stop/snapshot/start cycle and the post-checkpoint
/// wait-rerun, not merely a filesystem diff): boot a small image, exec-write a
/// marker file, checkpoint it, stop the original, restore a new container from the
/// checkpoint via `Container::from_checkpoint`, and assert the marker file is
/// present in the restored container's filesystem — proving the "filesystem
/// capture, not memory" semantics end to end. Cleans up the committed image at the
/// end (checkpoint images are never auto-reaped — see the checkpoints docs).
#[tokio::test]
async fn checkpoint_restore_round_trips_a_marker_file_written_after_boot() {
    require_backend!();
    if rightsize::backends::active_name() != "docker" {
        eprintln!(
            "skipping: this dedicated restore test is docker-only — microsandbox has its own \
             equivalent in rightsize-msb/tests/checkpoint_it.rs"
        );
        return;
    }

    let original = Container::new("alpine:3.19")
        .with_command(&["sleep", "60"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let original_guard = original.start().await.expect("original must start");

    let write = original_guard
        .exec(&[
            "sh",
            "-c",
            "echo checkpoint-restore-marker > /tmp/rz-checkpoint-marker.txt",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);

    let cp = original_guard
        .checkpoint()
        .await
        .expect("checkpoint must succeed");
    original_guard
        .stop()
        .await
        .expect("stop the original container");

    let restored = Container::from_checkpoint(&cp)
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let restored_guard = restored
        .start()
        .await
        .expect("a container restored from the checkpoint image must start");

    let read = restored_guard
        .exec(&["cat", "/tmp/rz-checkpoint-marker.txt"])
        .await
        .expect("exec must run in the restored container");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert!(
        read.stdout.contains("checkpoint-restore-marker"),
        "the restored container's filesystem must contain the marker file written before \
         checkpointing: stdout was {:?}",
        read.stdout
    );

    restored_guard
        .stop()
        .await
        .expect("stop the restored container");
    let backend = raw_backend_for("docker");
    let _ = backend.remove_checkpoint(&cp.checkpoint_ref).await;
}

// ================================ runtime copy =====================================

/// Boots a plain alpine sandbox with a long-lived `sleep` workload — the shared
/// fixture for every runtime-copy contract test below; a log-anchored wait would be
/// pointless here (alpine's `sleep` prints nothing), so a listening-port wait would
/// also be wrong (nothing listens either) — `exec`-based readiness (a successful
/// `true`) is the honest fit, mirroring how the msb loopback forwarder's
/// accept-before-listen quirk is a non-issue when nothing is ever dialed.
async fn boot_alpine_sleep() -> rightsize::ContainerGuard {
    Container::new("alpine:3.19")
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)))
        .start()
        .await
        .expect("container must start")
}

/// copy host file in -> `exec cat` returns exact content; the destination's parent
/// did not pre-exist, proving the generic layer's mkdir-p step.
#[tokio::test]
async fn copy_file_to_container_round_trips_a_host_file_into_an_absent_parent() {
    require_backend!();
    let guard = boot_alpine_sleep().await;

    let host_file =
        std::env::temp_dir().join(format!("rightsize-copyin-file-{}.txt", std::process::id()));
    let payload = "rightsize-runtime-copy-file-payload\n";
    std::fs::write(&host_file, payload).unwrap();

    guard
        .copy_file_to_container(&host_file, "/copyin-file/nested/dst.txt")
        .await
        .expect("copy in must succeed");

    let read = guard
        .exec(&["cat", "/copyin-file/nested/dst.txt"])
        .await
        .expect("exec must run");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert_eq!(read.stdout, payload);

    std::fs::remove_file(&host_file).ok();
    guard.stop().await.unwrap();
}

/// copy in-memory content in -> the same exec-cat round trip, no host file involved.
#[tokio::test]
async fn copy_content_to_container_round_trips_in_memory_bytes() {
    require_backend!();
    let guard = boot_alpine_sleep().await;

    let payload = "rightsize-runtime-copy-content-payload\n";
    guard
        .copy_content_to_container(payload.as_bytes(), "/copyin-content/nested/dst.txt")
        .await
        .expect("copy content must succeed");

    let read = guard
        .exec(&["cat", "/copyin-content/nested/dst.txt"])
        .await
        .expect("exec must run");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert_eq!(read.stdout, payload);

    guard.stop().await.unwrap();
}

/// copy directory in -> nested file readable at `<dst>/<nested>` — `cp -r`-style
/// destination naming: an absent destination becomes a copy of the source, not a
/// copy nested one level deeper under it.
#[tokio::test]
async fn copy_file_to_container_round_trips_a_directory() {
    require_backend!();
    let guard = boot_alpine_sleep().await;

    let host_dir =
        std::env::temp_dir().join(format!("rightsize-copyin-dir-{}", std::process::id()));
    std::fs::create_dir_all(host_dir.join("nested")).unwrap();
    let payload = "rightsize-runtime-copy-dir-payload\n";
    std::fs::write(host_dir.join("nested").join("f.txt"), payload).unwrap();

    guard
        .copy_file_to_container(&host_dir, "/copyin-dir")
        .await
        .expect("directory copy in must succeed");

    let read = guard
        .exec(&["cat", "/copyin-dir/nested/f.txt"])
        .await
        .expect("exec must run");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert_eq!(read.stdout, payload);

    std::fs::remove_dir_all(&host_dir).ok();
    guard.stop().await.unwrap();
}

/// exec-write a file in the guest -> copy it out -> host file content matches; the
/// host destination's parent did not pre-exist, proving the stdlib
/// `create_dir_all` step on the copy-out side.
#[tokio::test]
async fn copy_file_from_container_round_trips_a_guest_file() {
    require_backend!();
    let guard = boot_alpine_sleep().await;

    let write = guard
        .exec(&[
            "sh",
            "-c",
            "echo rightsize-runtime-copy-out-payload > /copyout-src.txt",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);

    let host_dest = std::env::temp_dir()
        .join(format!("rightsize-copyout-{}", std::process::id()))
        .join("nested")
        .join("f.txt");

    guard
        .copy_file_from_container("/copyout-src.txt", &host_dest)
        .await
        .expect("copy out must succeed");

    let content = std::fs::read_to_string(&host_dest).expect("host file must exist");
    assert_eq!(content, "rightsize-runtime-copy-out-payload\n");

    std::fs::remove_dir_all(host_dest.parent().unwrap().parent().unwrap()).ok();
    guard.stop().await.unwrap();
}

/// exec-write a nested directory in the guest -> copy the directory out -> the
/// nested host file matches, same `cp -r`-style naming as the copy-in direction.
#[tokio::test]
async fn copy_file_from_container_round_trips_a_guest_directory() {
    require_backend!();
    let guard = boot_alpine_sleep().await;

    let write = guard
        .exec(&[
            "sh",
            "-c",
            "mkdir -p /copyout-dir/nested && echo rightsize-runtime-copy-out-dir-payload > \
             /copyout-dir/nested/f.txt",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);

    let host_dest =
        std::env::temp_dir().join(format!("rightsize-copyout-dir-{}", std::process::id()));

    guard
        .copy_file_from_container("/copyout-dir", &host_dest)
        .await
        .expect("directory copy out must succeed");

    let content = std::fs::read_to_string(host_dest.join("nested").join("f.txt"))
        .expect("host file must exist");
    assert_eq!(content, "rightsize-runtime-copy-out-dir-payload\n");

    std::fs::remove_dir_all(&host_dest).ok();
    guard.stop().await.unwrap();
}

// ======================= Container reuse (double opt-in) ==========================
//
// Reuse's gating is `RIGHTSIZE_REUSE`-env-sensitive, and this binary's OTHER
// contract tests above run in parallel threads within the same process — mutating
// that real env var here would race them (and `Container::reuse`'s own env read has
// no per-call override at this crate's public surface; `Container::with_reuse_env_override`
// is `pub(crate)` inside `rightsize` itself). Every scenario below therefore spawns a
// fresh child process (same "re-exec this test binary with `--exact` + controlled
// env" shape `sweep_reaps_a_fabricated_dead_run_at_the_backend_level` already uses
// for `RIGHTSIZE_REAPER`) so its own `RIGHTSIZE_REUSE`/`RIGHTSIZE_CACHE_DIR` are
// deterministic regardless of what's ambient in this process's environment or what
// any other test in this binary is doing concurrently.

/// A fresh, empty scratch cache dir for one reuse scenario, so its registry file
/// (and any run-record ledger files) never collide with another test's — same shape
/// `sweep_reaps_a_fabricated_dead_run_at_the_backend_level` uses for its own
/// `cache_dir`.
fn fresh_scratch_cache_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "rz-contract-reuse-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Removes a scratch cache dir (and the registry file it may hold) via `Drop`
/// rather than a bare trailing `remove_dir_all` call, so it still runs if a
/// `status.success()`/other assertion earlier in the test panics — a bare trailing
/// call is simply never reached on that path, leaking the scratch dir (and, if the
/// child itself failed after writing one, its reuse registry file) into the shared
/// CI temp dir run after run.
struct ScratchCacheDirGuard(PathBuf);

impl Drop for ScratchCacheDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A random-enough-per-run hex nonce (reuses `rightsize::RunId`'s own
/// time+pid-mixed generator rather than hand-rolling a second one), injected as
/// `RZ_TEST_NONCE` into a reuse scenario's spec so its identity — and therefore its
/// `rz-reuse-<hash>` sandbox name — can never collide with a sandbox left behind by
/// an earlier, differently-timed run of the same scenario. Without this, a helper
/// that always builds the exact same `{image, env, ports}` spec (e.g.
/// `RZ_CONTRACT_ADOPT=1` every run) would reuse the identical identity forever, and
/// a sandbox orphaned by a previous crashed run — the crash-mid-boot scenario
/// `docs/reuse.md#crash-mid-boot-orphan-recovery` describes — could still be sitting
/// there under that exact name. The library's own orphan recovery (see that doc)
/// now makes that collision safe either way, but nonce'ing the identity here means
/// these scenarios test what they say they test (adoption, stop semantics) without
/// ever depending on that recovery path firing.
///
/// NOT used by [`helper_reuse_pinned_vector`] — nonce'ing that spec would change its
/// hash and defeat the one thing it exists to prove: the exact pinned
/// cross-language identity vector.
fn contract_reuse_nonce() -> &'static str {
    rightsize::RunId::value()
}

/// Re-execs this test binary with only `helper_name` selected, pointed at
/// `cache_dir` and `backend_name`, with `RIGHTSIZE_REUSE` set to `reuse_env` — `None`
/// removes it from the child's environment entirely (rather than merely not setting
/// it) so a developer's own exported `RIGHTSIZE_REUSE=true` can never leak into the
/// "env-disabled" scenario. Blocks until the child exits and returns its status;
/// child stderr is surfaced on failure so a helper assertion failure is visible in
/// the parent's own test output.
fn spawn_reuse_helper(
    helper_name: &str,
    cache_dir: &Path,
    backend_name: &str,
    reuse_env: Option<&str>,
) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("current_exe");
    let mut cmd = Command::new(&exe);
    cmd.args(["--exact", helper_name, "--nocapture"])
        .env("RIGHTSIZE_HELPER_CHILD", "1")
        .env("RIGHTSIZE_CACHE_DIR", cache_dir)
        .env("RIGHTSIZE_BACKEND", backend_name)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    match reuse_env {
        Some(v) => {
            cmd.env("RIGHTSIZE_REUSE", v);
        }
        None => {
            cmd.env_remove("RIGHTSIZE_REUSE");
        }
    }
    let output = cmd.output().expect("spawn reuse helper child process");
    if !output.status.success() {
        eprintln!(
            "reuse helper {helper_name} stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    output.status
}

/// Double opt-in gating: `.reuse(true)` with `RIGHTSIZE_REUSE` NOT set must fall
/// straight through to an ordinary, non-reused container (Testcontainers semantics)
/// on a REAL backend — proven two ways in the child helper: the sandbox name is an
/// ordinary `rz-<runid>-<seq>`, never `rz-reuse-...`, and `stop()` genuinely tears it
/// down (the reuse `keep_alive` short-circuit never engages). Also checked here in
/// the parent: no reuse registry file was ever written.
#[tokio::test]
async fn reuse_double_opt_in_gating_env_disabled_runs_as_an_ordinary_ephemeral_container() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();
    let cache_dir = fresh_scratch_cache_dir("gating");
    let _cache_dir_guard = ScratchCacheDirGuard(cache_dir.clone());

    let status = spawn_reuse_helper(
        "helper_reuse_gating_env_disabled",
        &cache_dir,
        &backend_name,
        None,
    );
    assert!(status.success(), "gating helper child must exit cleanly");
    assert!(
        !cache_dir.join("reuse").exists(),
        "an env-disabled reuse request must never write a registry entry"
    );
}

/// Helper for
/// [`reuse_double_opt_in_gating_env_disabled_runs_as_an_ordinary_ephemeral_container`].
/// A normal `cargo test` run treats this as a no-op (the `RIGHTSIZE_HELPER_CHILD`
/// guard) — it only does real work when re-exec'd by that test.
#[tokio::test]
async fn helper_reuse_gating_env_disabled() {
    if std::env::var("RIGHTSIZE_HELPER_CHILD").as_deref() != Ok("1") {
        return;
    }
    support::ensure_registered();
    assert!(
        std::env::var("RIGHTSIZE_REUSE").is_err(),
        "this helper must run with RIGHTSIZE_REUSE unset"
    );

    let c = Container::new("python:3.12-alpine")
        .with_command(&["python", "-m", "http.server", "8000"])
        .with_exposed_ports(&[8000])
        .reuse(true)
        .waiting_for(
            Wait::for_http("/")
                .for_port(8000)
                .with_startup_timeout(Duration::from_secs(120)),
        );
    let guard = c
        .start()
        .await
        .expect("gating helper must start as an ordinary container despite .reuse(true)");
    let name = guard.name().to_string();
    assert!(
        !name.starts_with("rz-reuse-"),
        "env-disabled reuse must produce an ordinary sandbox name, got {name}"
    );
    let port = guard
        .get_mapped_port(8000)
        .expect("ordinary container must have a mapped port");

    guard
        .stop()
        .await
        .expect("an env-disabled reuse container's stop() must behave like any ordinary stop()");

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut gone = false;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
            gone = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        gone,
        "stop() on an env-disabled reuse container must actually tear it down at the backend \
         level (port {port} must stop accepting connections)"
    );
}

/// Best-effort teardown for one reuse scenario's sandbox: removes it at the backend
/// level by name. Built as a `Drop` guard — not a bare trailing call — specifically
/// so it still runs if an assertion partway through a helper's body panics; a reuse
/// sandbox is never torn down by `stop()` (that's the feature), so a panic between
/// `start()` and the old trailing `remove_by_name` call used to leak it into shared
/// CI backend state for good. (The registry FILE itself needs no separate handling
/// here — it lives under this child's `RIGHTSIZE_CACHE_DIR`, which the PARENT's own
/// [`ScratchCacheDirGuard`] removes wholesale regardless of how this child exits.)
struct ReuseSandboxCleanup {
    raw_backend: Box<dyn SandboxBackend>,
    name: String,
}

impl Drop for ReuseSandboxCleanup {
    fn drop(&mut self) {
        self.raw_backend.remove_by_name(&self.name);
    }
}

/// `rz-reuse-` + the first 12 hex chars of the reuse feature spec's pinned
/// cross-language vector hash (see `crates/rightsize/src/reuse.rs`'s
/// `PINNED_VECTOR_HASH` / `pinned_cross_language_vector_hashes_to_the_pinned_value`)
/// — the exact spec `{image: "redis:7-alpine", env: {A: "1", B: "2"}, command: [],
/// exposedPorts: [6379], memoryLimitMb: null, copies: []}` MUST hash (and therefore
/// name) identically across every language's implementation.
const PINNED_VECTOR_SANDBOX_NAME: &str = "rz-reuse-799aad5a3338";

/// The cross-language identity hash contract, proven at the observable-behavior
/// level on a REAL backend: `crate::reuse::compute_identity` is `pub(crate)` inside
/// `rightsize` and unreachable from this external contract crate, so the only way
/// this suite can prove the pinned vector holds end to end is to build the exact
/// pinned spec, start it for real, and check the sandbox name the backend actually
/// received — [`PINNED_VECTOR_SANDBOX_NAME`].
#[tokio::test]
async fn reuse_pinned_cross_language_hash_vector_produces_the_contract_sandbox_name() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();
    let cache_dir = fresh_scratch_cache_dir("hashvec");
    let _cache_dir_guard = ScratchCacheDirGuard(cache_dir.clone());

    let status = spawn_reuse_helper(
        "helper_reuse_pinned_vector",
        &cache_dir,
        &backend_name,
        Some("true"),
    );
    assert!(
        status.success(),
        "hash-vector helper child must exit cleanly"
    );
}

/// Helper for
/// [`reuse_pinned_cross_language_hash_vector_produces_the_contract_sandbox_name`].
/// Explicitly removes the sandbox it creates before exiting (a reuse sandbox is
/// never torn down by `stop()` — that's the feature) so nothing leaks into shared
/// CI backend state; the scratch cache dir itself is discarded by the parent right
/// after this process exits.
#[tokio::test]
async fn helper_reuse_pinned_vector() {
    if std::env::var("RIGHTSIZE_HELPER_CHILD").as_deref() != Ok("1") {
        return;
    }
    support::ensure_registered();

    let backend_name = rightsize::backends::active_name();
    let raw_backend = raw_backend_for(&backend_name);

    let c = Container::new("redis:7-alpine")
        .with_env("A", "1")
        .with_env("B", "2")
        .with_exposed_ports(&[6379])
        .reuse(true);
    let guard = c
        .start()
        .await
        .expect("pinned cross-language vector reuse container must start");
    // From here on, cleanup must run even if a later assertion panics — this spec
    // is deliberately NOT nonce'd (see `contract_reuse_nonce`'s doc), so its
    // identity is the same every run.
    let _cleanup = ReuseSandboxCleanup {
        raw_backend,
        name: PINNED_VECTOR_SANDBOX_NAME.to_string(),
    };
    assert_eq!(
        guard.name(),
        PINNED_VECTOR_SANDBOX_NAME,
        "the pinned cross-language vector must produce the pinned sandbox name"
    );
    guard.stop().await.unwrap(); // reuse stop(): leaves the sandbox running.

    // `_cleanup` fires on drop, whether we reach here normally or unwound through
    // a panic above.
}

/// Adopt: a second, equivalent `.reuse(true)` container built AFTER the first one's
/// `stop()` (which — being reuse — leaves the sandbox running) ADOPTS the exact same
/// live sandbox instead of creating a new one: identical name, identical mapped
/// port, and a real round trip against it proves it's genuinely the same live
/// sandbox, not a coincidentally-matching new one. Generalizes
/// `rightsize-msb/tests/reuse_it.rs`'s msb-only adoption proof across BOTH backends
/// through the shared contract harness — msb-only coverage never proves docker's
/// real `find_running` path against a real daemon.
#[tokio::test]
async fn reuse_adopt_reuses_the_same_sandbox_and_mapped_port_across_two_starts() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();
    let cache_dir = fresh_scratch_cache_dir("adopt");
    let _cache_dir_guard = ScratchCacheDirGuard(cache_dir.clone());

    let status = spawn_reuse_helper(
        "helper_reuse_adopt_across_two_starts",
        &cache_dir,
        &backend_name,
        Some("true"),
    );
    assert!(status.success(), "adopt helper child must exit cleanly");
}

/// Helper for
/// [`reuse_adopt_reuses_the_same_sandbox_and_mapped_port_across_two_starts`].
#[tokio::test]
async fn helper_reuse_adopt_across_two_starts() {
    if std::env::var("RIGHTSIZE_HELPER_CHILD").as_deref() != Ok("1") {
        return;
    }
    support::ensure_registered();

    let backend_name = rightsize::backends::active_name();
    let raw_backend = raw_backend_for(&backend_name);

    let nonce = contract_reuse_nonce();
    let build = move || {
        Container::new("redis:7-alpine")
            .with_env("RZ_CONTRACT_ADOPT", "1")
            .with_env("RZ_TEST_NONCE", nonce)
            .with_exposed_ports(&[6379])
            .reuse(true)
            // A bare listening-port check (this crate's default `WaitStrategy`) is not
            // enough here: microsandbox's host-side port forwarder can accept — and
            // silently hold open — a TCP connection before Redis has actually bound and
            // started serving inside the guest (exactly the race `rightsize_modules::
            // RedisContainer`'s own doc documents and works around the same way). Under
            // this suite's own concurrent sandbox load that window was measured up to
            // ~900ms, wide enough for this test's very next step — an immediate,
            // unretried `redis-cli PING` against the ADOPTED sandbox — to race ahead of
            // Redis actually being ready and observe "Connection refused" even though
            // the wait strategy just reported success. Anchoring on Redis's own "Ready
            // to accept connections" log line (as `RedisContainer` does) instead of a
            // bare port probe closes that window for both the fresh-create wait AND the
            // adopt-path's re-verification (`try_adopt` reuses this same
            // `wait_strategy`), since a real log line can only appear once Redis is
            // genuinely serving.
            .waiting_for(Wait::for_log_message(".*Ready to accept connections.*", 1))
    };

    let first = build()
        .start()
        .await
        .expect("first start() must create+start a fresh reuse sandbox");
    let name = first.name().to_string();
    // From here on, cleanup must run even if a later assertion panics.
    let _cleanup = ReuseSandboxCleanup {
        raw_backend,
        name: name.clone(),
    };
    assert!(name.starts_with("rz-reuse-"), "{name}");
    let first_port = first
        .get_mapped_port(6379)
        .expect("first sandbox must have a mapped port");

    first.stop().await.unwrap(); // reuse stop(): leaves the sandbox running.

    let second = build()
        .start()
        .await
        .expect("second start() must ADOPT the first sandbox, not re-create one");
    assert_eq!(
        second.name(),
        name,
        "adoption must produce the identical sandbox name"
    );
    assert_eq!(
        second.get_mapped_port(6379).unwrap(),
        first_port,
        "adoption must reuse the SAME mapped port, not allocate a fresh one"
    );

    let ping = second
        .exec(&["redis-cli", "PING"])
        .await
        .expect("exec against the adopted sandbox must run");
    assert_eq!(
        ping.stdout.trim(),
        "PONG",
        "adopted sandbox must actually be alive and serving: {ping:?}"
    );

    second.stop().await.unwrap();

    // `_cleanup` fires on drop, whether we reach here normally or unwound through
    // a panic above.
}

/// Stop semantics: `stop()` on a reuse guard leaves the sandbox genuinely running
/// (proven at the TCP level, not just an in-process flag) and never appears in the
/// run's `.sandboxes` reaping ledger file, at any point — the reuse start/stop path
/// never calls `crate::reaper::before_create`/`after_stop` at all (see
/// `crate::container::create_fresh_reuse`), which this reads back from the raw
/// on-disk ledger file to prove externally, since `crate::reaper::Ledger` itself is
/// `pub(crate)` and unreachable from this contract crate.
#[tokio::test]
async fn reuse_stop_semantics_leaves_the_sandbox_running_and_never_ledgers_it() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();
    let cache_dir = fresh_scratch_cache_dir("stopsem");
    let _cache_dir_guard = ScratchCacheDirGuard(cache_dir.clone());

    let status = spawn_reuse_helper(
        "helper_reuse_stop_semantics",
        &cache_dir,
        &backend_name,
        Some("true"),
    );
    assert!(
        status.success(),
        "stop-semantics helper child must exit cleanly"
    );
}

/// Helper for
/// [`reuse_stop_semantics_leaves_the_sandbox_running_and_never_ledgers_it`].
#[tokio::test]
async fn helper_reuse_stop_semantics() {
    if std::env::var("RIGHTSIZE_HELPER_CHILD").as_deref() != Ok("1") {
        return;
    }
    support::ensure_registered();

    let backend_name = rightsize::backends::active_name();
    let raw_backend = raw_backend_for(&backend_name);
    let cache_dir = rightsize::cache_dir::dir();

    let c = Container::new("redis:7-alpine")
        .with_env("RZ_CONTRACT_STOPSEM", "1")
        .with_env("RZ_TEST_NONCE", contract_reuse_nonce())
        .with_exposed_ports(&[6379])
        .reuse(true);
    let guard = c
        .start()
        .await
        .expect("stop-semantics reuse container must start");
    let name = guard.name().to_string();
    // From here on, cleanup must run even if a later assertion panics.
    let _cleanup = ReuseSandboxCleanup {
        raw_backend,
        name: name.clone(),
    };
    let port = guard
        .get_mapped_port(6379)
        .expect("reuse sandbox must have a mapped port");

    guard
        .stop()
        .await
        .expect("stop() on a reuse guard must return Ok even though it leaves the sandbox running");

    assert!(
        std::net::TcpStream::connect(("127.0.0.1", port)).is_ok(),
        "a reuse container's stop() must leave the sandbox genuinely running, not tear it down"
    );

    // Raw file read (see this test's module doc): the run's own `.sandboxes` ledger
    // file, if it exists at all, must never contain the reuse sandbox's name.
    let sandboxes_path = cache_dir
        .join("runs")
        .join(format!("{}.sandboxes", rightsize::RunId::value()));
    if let Ok(contents) = std::fs::read_to_string(&sandboxes_path) {
        assert!(
            !contents.lines().any(|l| l == name),
            "a reuse sandbox must never appear in the run's .sandboxes ledger file: {contents:?}"
        );
    }

    // `_cleanup` fires on drop, whether we reach here normally or unwound through
    // a panic above.
}

// ==================== msb-0.6.8 spec options: disk limit, tmpfs root, ============
// ==================== network disabled ============================================

/// `with_network_disabled` contract: on BOTH backends the container still starts and
/// serves its published port (the http.server/`for_http` pattern used at the top of
/// this file) — a network-disabled container is not an unreachable one. On msb only,
/// additionally proves the flip side: an exec'd request to the public internet fails,
/// since docker has no `--net private` equivalent and simply ignores the field.
#[tokio::test]
async fn a_network_disabled_container_serves_its_published_port() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();

    let c = Container::new("python:3.12-alpine")
        .with_command(&["python", "-m", "http.server", "8000"])
        .with_exposed_ports(&[8000])
        .with_network_disabled()
        .waiting_for(
            Wait::for_http("/")
                .for_port(8000)
                .with_startup_timeout(Duration::from_secs(120)),
        );
    let guard = c
        .start()
        .await
        .expect("a network-disabled container must still start and serve its published port");

    let url = format!("http://127.0.0.1:{}/", guard.get_mapped_port(8000).unwrap());
    let status = support::http_agent()
        .get(&url)
        .call()
        .expect("GET / must succeed")
        .status();
    assert_eq!(status.as_u16(), 200);

    if backend_name != "docker" {
        let probe = guard
            .exec(&[
                "python",
                "-c",
                "import urllib.request; urllib.request.urlopen('http://example.com', timeout=5)",
            ])
            .await
            .expect("exec must run");
        assert_ne!(
            probe.exit_code, 0,
            "a network-disabled msb container must not reach the public internet: {}",
            probe.stderr
        );
    }

    guard.stop().await.unwrap();
}

/// `with_tmpfs_root` contract: on BOTH backends the container boots and a guest
/// write/read round-trips under `/root` — msb backs the root disk with RAM, docker
/// ignores the field and runs its normal disk-backed rootfs, but a write succeeds
/// either way.
#[tokio::test]
async fn a_tmpfs_root_container_boots_and_accepts_writes() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&["sleep", "60"])
        .with_tmpfs_root(256)
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c
        .start()
        .await
        .expect("a tmpfs-root container must start on both backends");

    let write = guard
        .exec(&[
            "sh",
            "-c",
            "echo rz-tmpfs-probe > /root/rz-tmpfs-probe.txt && cat /root/rz-tmpfs-probe.txt",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);
    assert!(write.stdout.contains("rz-tmpfs-probe"), "{}", write.stdout);

    guard.stop().await.unwrap();
}

/// `with_disk_limit` contract: on BOTH backends the container simply boots — msb
/// caps the writable root disk at the given size, docker ignores the field entirely.
#[tokio::test]
async fn a_disk_limited_container_boots() {
    require_backend!();
    let c = Container::new("alpine:3.19")
        .with_command(&["sleep", "30"])
        .with_disk_limit(1024)
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c
        .start()
        .await
        .expect("a disk-limited container must start on both backends");
    guard.stop().await.unwrap();
}

/// Checkpoint guard contract (msb-only — docker ignores `tmpfs_root_mb` and has no
/// notion of a RAM-backed root to refuse checkpointing): `checkpoint()` on a
/// tmpfs-root container refuses with `RightsizeError::TmpfsRootCheckpoint`, before
/// touching the backend's real checkpoint machinery.
#[tokio::test]
async fn checkpointing_a_tmpfs_root_container_is_refused() {
    require_backend!();
    if rightsize::backends::active_name() == "docker" {
        eprintln!(
            "skipping: tmpfs-root checkpoint refusal is msb-only — docker has no notion of a \
             RAM-backed root to refuse checkpointing"
        );
        return;
    }

    let c = Container::new("alpine:3.19")
        .with_command(&["sleep", "60"])
        .with_tmpfs_root(256)
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("tmpfs-root container must start");

    let err = match guard.checkpoint().await {
        Ok(_) => panic!("checkpoint() must refuse a tmpfs-root container"),
        Err(e) => e,
    };
    assert!(
        matches!(err, rightsize::RightsizeError::TmpfsRootCheckpoint),
        "{err}"
    );

    guard.stop().await.unwrap();
}

/// Checkpoint dest-dir contract (msb-only — docker checkpoints are image tags, not
/// filesystem artifacts, so there is no cache-dir path to prove): the ref minted by
/// `checkpoint()` is an absolute path under `<cache dir>/checkpoints`, that path is a
/// real directory containing `snapshot.json`, `Container::from_checkpoint` restores a
/// marker file written before the checkpoint, and `remove_checkpoint` deletes the
/// artifact directory.
#[tokio::test]
async fn a_checkpoint_stores_its_artifact_under_the_cache_dir_and_restores_from_it() {
    require_backend!();
    let backend_name = rightsize::backends::active_name();
    if backend_name == "docker" {
        eprintln!(
            "skipping: checkpoint dest-dir storage is an msb-only mechanic — docker checkpoints \
             are image tags, not filesystem artifacts"
        );
        return;
    }

    let c = Container::new("alpine:3.19")
        .with_command(&["sleep", "60"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("container must start");

    let write = guard
        .exec(&[
            "sh",
            "-c",
            "echo rz-destdir-marker > /root/rz-destdir-marker.txt",
        ])
        .await
        .expect("exec must run");
    assert_eq!(write.exit_code, 0, "{}", write.stderr);

    let cp = guard.checkpoint().await.expect("checkpoint must succeed");

    let ref_path = Path::new(&cp.checkpoint_ref);
    assert!(ref_path.is_absolute(), "{}", cp.checkpoint_ref);
    let expected_parent = rightsize::cache_dir::dir().join("checkpoints");
    assert_eq!(ref_path.parent(), Some(expected_parent.as_path()));
    assert!(ref_path.is_dir(), "{}", cp.checkpoint_ref);
    assert!(
        ref_path.join("snapshot.json").is_file(),
        "{}",
        cp.checkpoint_ref
    );

    guard.stop().await.expect("stop the original container");

    let restored = Container::from_checkpoint(&cp)
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let restored_guard = restored
        .start()
        .await
        .expect("restore from the dest-dir checkpoint must succeed");

    let read = restored_guard
        .exec(&["cat", "/root/rz-destdir-marker.txt"])
        .await
        .expect("exec must run");
    assert_eq!(read.exit_code, 0, "{}", read.stderr);
    assert!(read.stdout.contains("rz-destdir-marker"), "{}", read.stdout);

    let raw_backend = raw_backend_for(&backend_name);
    raw_backend
        .remove_checkpoint(&cp.checkpoint_ref)
        .await
        .expect("remove_checkpoint must succeed");
    assert!(
        !ref_path.exists(),
        "remove_checkpoint must delete the dest-dir artifact directory"
    );

    restored_guard.stop().await.unwrap();
}
