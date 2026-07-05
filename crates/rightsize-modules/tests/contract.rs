//! The shared backend contract suite — every `SandboxBackend` must satisfy these nine
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
//! The nine contract behaviors:
//!
//! - Container publishes TCP port to host loopback: [`container_publishes_tcp_port_to_host_loopback`]
//! - Env vars are visible to the workload: [`env_vars_are_visible_to_the_workload`]
//! - Exec returns real exit codes and stderr: [`exec_returns_real_exit_codes_and_stderr`]
//! - Logs capture workload stdout and forLogMessage waits on them: [`logs_capture_workload_stdout_and_for_log_message_waits_on_them`]
//! - Stop terminates and frees the container: [`stop_terminates_and_frees_the_container`]
//! - withCopyFileToContainer round-trips a bundled resource and a host path: [`with_copy_file_to_container_round_trips_a_bundled_resource_and_a_host_path`]
//! - withCopyFileToContainer default read-only mount rejects an in-guest write: [`default_read_only_mount_rejects_an_in_guest_write`]
//! - followOutput streams lines in order and close halts delivery: [`follow_output_streams_lines_in_order_and_close_halts_delivery`]
//! - followOutput delivers a final unterminated line after the workload exits: [`follow_output_delivers_a_final_unterminated_line_after_the_workload_exits_exactly_once`]
//!
//! `read_only_mount_enforced` is a per-backend override: true on docker (enforced),
//! false on msb (advisory only on 0.6.2).

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rightsize::{Container, MountableFile, Wait};

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

/// Whether the currently-requested backend enforces `FileMount::read_only` as a
/// guest-side write block. `false` only for msb (see
/// `default_read_only_mount_rejects_an_in_guest_write` below); everything else
/// (docker, and an unset `RIGHTSIZE_BACKEND` that resolves to docker by priority)
/// enforces it.
fn read_only_mount_enforced() -> bool {
    !matches!(
        std::env::var("RIGHTSIZE_BACKEND"),
        Ok(v) if v.eq_ignore_ascii_case("microsandbox") || v.eq_ignore_ascii_case("msb")
    )
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
            Wait::for_http("/").for_port(8000).with_startup_timeout(Duration::from_secs(120)),
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

/// Default read-only mount (`FileMount::read_only == true` unless overridden) rejects
/// an in-guest write — enforced on docker; advisory-only (write succeeds) on msb.
#[tokio::test]
async fn default_read_only_mount_rejects_an_in_guest_write() {
    require_backend!();
    let host_file = std::env::temp_dir().join(format!("rightsize-ro-{}.txt", std::process::id()));
    std::fs::write(&host_file, "seed\n").unwrap();

    let c = Container::new("alpine:3.19")
        .with_copy_file_to_container(
            MountableFile::for_host_path(host_file.to_str().unwrap()),
            "/tmp/ro.txt",
        )
        .with_command(&["sleep", "120"])
        .waiting_for(Wait::for_log_message(".*", 0).with_startup_timeout(Duration::from_secs(30)));
    let guard = c.start().await.expect("container must start");

    let write = guard
        .exec(&["sh", "-c", "echo overwritten > /tmp/ro.txt"])
        .await
        .expect("exec must run");
    if read_only_mount_enforced() {
        assert_ne!(
            write.exit_code, 0,
            "expected read-only mount to reject the write; stderr={}",
            write.stderr
        );
    } else {
        assert_eq!(
            write.exit_code, 0,
            "msb read-only-mount write behavior changed — update read_only_mount_enforced pin"
        );
    }

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
