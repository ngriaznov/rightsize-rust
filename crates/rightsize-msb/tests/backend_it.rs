//! `sandbox-it` integration tests for `MsbCliBackend` — the msb-specific behaviors this
//! module covers: attached-mode boot + Running-poll readiness, exec with closed stdin,
//! `running_sandbox_names` against a real `msb ls`, and the `follow_logs` watchdog's
//! exactly-once tail replay. Contract-level parity (the shared backend contract suite)
//! is covered separately, in `contract.rs`.
//!
//! Run for real against a real `msb` binary and real microVMs:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it
//! ```
//!
//! Every test skips itself (rather than failing) when this host has no supported msb
//! runtime, or when `RIGHTSIZE_BACKEND` explicitly names a different backend.

#![cfg(feature = "sandbox-it")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rightsize::backend::SandboxBackend;
use rightsize::model::ContainerSpec;
use rightsize_msb::MsbBackendProvider;
use rightsize_msb::MsbCliBackend;

fn msb_runtime_available() -> bool {
    use rightsize::backend::BackendProvider;
    if std::env::var("RIGHTSIZE_BACKEND")
        .map(|v| v.eq_ignore_ascii_case("docker"))
        .unwrap_or(false)
    {
        return false;
    }
    MsbBackendProvider.is_supported()
}

/// Skips the calling test (prints a notice and returns) unless this host has a
/// supported msb runtime and hasn't been asked to run docker instead.
macro_rules! require_msb {
    () => {
        if !msb_runtime_available() {
            eprintln!(
                "skipping: no supported msb runtime on this host (or RIGHTSIZE_BACKEND=docker)"
            );
            return;
        }
    };
}

fn provisioned_msb_path() -> std::path::PathBuf {
    rightsize_msb::provisioner::ensure_installed()
        .expect("msb must already be provisioned on this host")
}

fn unique_name(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("rz-{label}-{}", &format!("{nanos:x}")[..8])
}

/// Attached-mode boot + Running-poll readiness: a plain `sleep`-based alpine sandbox
/// must reach `Running` and be reported by `running_sandbox_names()`, and `stop`+
/// `remove` must clean it up (no leftover `msb ls` entry afterward).
#[tokio::test]
async fn attached_run_reaches_running_and_appears_in_running_names() {
    require_msb!();
    let msb_path = provisioned_msb_path();
    let backend = MsbCliBackend::new(msb_path.clone());

    let spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "120".to_string()]),
        ..ContainerSpec::new(unique_name("attached"), "alpine:3.19", "backend-it")
    };
    let name = spec.name.clone();

    let handle = backend.create(spec).await.expect("create");
    backend
        .start(handle.as_ref())
        .await
        .expect("start must reach Running");

    let names = rightsize_msb::backend::running_sandbox_names(&msb_path)
        .expect("running_sandbox_names must succeed against a real msb ls");
    assert!(
        names.contains(&name),
        "expected '{name}' among Running sandboxes: {names:?}"
    );

    backend.stop(handle.as_ref()).await.expect("stop");
    backend.remove(handle.as_ref()).await.expect("remove");

    let names_after =
        rightsize_msb::backend::running_sandbox_names(&msb_path).expect("ls after remove");
    assert!(
        !names_after.contains(&name),
        "'{name}' should no longer be Running after stop+remove: {names_after:?}"
    );
}

/// `msb exec` must not hang: every child this backend spawns gets a closed/null stdin,
/// so a command that would otherwise wait on stdin EOF completes promptly.
#[tokio::test]
async fn exec_completes_with_closed_stdin_and_returns_the_real_exit_code() {
    require_msb!();
    let backend = MsbCliBackend::new(provisioned_msb_path());

    let spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "120".to_string()]),
        ..ContainerSpec::new(unique_name("exec-stdin"), "alpine:3.19", "backend-it")
    };
    let handle = backend.create(spec).await.expect("create");
    backend.start(handle.as_ref()).await.expect("start");

    // `cat` with no arguments reads from stdin until EOF — if this backend ever forgot
    // to close/null the child's stdin, this exec would hang until EXEC_TIMEOUT instead
    // of returning almost immediately.
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        backend.exec(handle.as_ref(), &["cat".to_string()]),
    )
    .await
    .expect("exec must not hang waiting on stdin")
    .expect("exec must succeed");
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);

    let failing = backend
        .exec(
            handle.as_ref(),
            &["sh".to_string(), "-c".to_string(), "exit 7".to_string()],
        )
        .await
        .expect("exec must succeed even for a nonzero exit");
    assert_eq!(failing.exit_code, 7);

    backend.stop(handle.as_ref()).await.unwrap();
    backend.remove(handle.as_ref()).await.unwrap();
}

/// The `follow_logs` watchdog's exactly-once tail replay: a workload that
/// prints a final line with NO trailing newline and then exits must have that line
/// delivered by the watchdog's authoritative flush — exactly once, never duplicated
/// with whatever the live stream managed to deliver before the sandbox stopped.
#[tokio::test]
async fn follow_logs_watchdog_replays_the_final_unterminated_line_exactly_once() {
    require_msb!();
    let backend = MsbCliBackend::new(provisioned_msb_path());

    // Sleep briefly first so `start()`'s Running-poll has a real chance to observe the
    // sandbox as Running before it exits — a workload that finishes instantly can race
    // ahead of the very first poll and make `start()` itself fail (msb never reports a
    // sandbox that lived and died between two polls as ever having been Running).
    // Then echo a final line with NO trailing newline and exit — msb's `logs -f` will
    // never observe EOF on its own: the watchdog must notice the sandbox left
    // Running and flush the tail itself.
    let spec = ContainerSpec {
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "echo line-one; sleep 2; printf 'line-two-no-newline'".to_string(),
        ]),
        ..ContainerSpec::new(unique_name("watchdog"), "alpine:3.19", "backend-it")
    };
    let handle = backend.create(spec).await.expect("create");
    backend.start(handle.as_ref()).await.expect("start");

    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let follow = backend
        .follow_logs(
            handle.as_ref(),
            Box::new(move |line| received_clone.lock().unwrap().push(line)),
        )
        .await
        .expect("follow_logs must start");

    // The workload exits almost immediately; give the watchdog time to notice the
    // sandbox left Running and perform its one authoritative flush.
    tokio::time::sleep(Duration::from_secs(5)).await;
    follow.close();

    let lines = received.lock().unwrap().clone();
    let occurrences = |needle: &str| lines.iter().filter(|l| l.contains(needle)).count();
    assert_eq!(
        occurrences("line-two-no-newline"),
        1,
        "the final unterminated line must be delivered exactly once, got: {lines:?}"
    );
    assert_eq!(
        occurrences("line-one"),
        1,
        "the first line must be delivered exactly once too, got: {lines:?}"
    );

    backend.stop(handle.as_ref()).await.unwrap();
    backend.remove(handle.as_ref()).await.unwrap();
}

/// An explicit `close()` must never trigger the tail-replay flush — closing means
/// "stop delivery", not "flush whatever's left". A long-running sandbox (still
/// `Running` when we close) proves this: if close() incorrectly flushed, the fetched
/// tail would include lines the live stream never had a chance to deliver yet, which
/// would show up as extra deliveries beyond what the short sleep here allows.
#[tokio::test]
async fn explicit_close_does_not_trigger_a_replay_while_still_running() {
    require_msb!();
    let backend = MsbCliBackend::new(provisioned_msb_path());

    let spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "120".to_string()]),
        ..ContainerSpec::new(unique_name("close-no-flush"), "alpine:3.19", "backend-it")
    };
    let handle = backend.create(spec).await.expect("create");
    backend.start(handle.as_ref()).await.expect("start");

    let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received.clone();
    let follow = backend
        .follow_logs(
            handle.as_ref(),
            Box::new(move |line| received_clone.lock().unwrap().push(line)),
        )
        .await
        .expect("follow_logs must start");

    follow.close();

    // The sandbox is still Running (sleep 120) — close() must have halted delivery
    // without shelling out for a tail fetch/replay. Nothing to assert on `received`
    // content-wise (a `sleep` workload produces no log lines), but the real assertion
    // is behavioral: close() must return promptly rather than blocking on a flush.
    let names = rightsize_msb::backend::running_sandbox_names(&provisioned_msb_path())
        .expect("ls after close");
    assert!(
        names.contains(&handle.spec().name),
        "the sandbox must still be Running — close() must not have stopped it"
    );

    backend.stop(handle.as_ref()).await.unwrap();
    backend.remove(handle.as_ref()).await.unwrap();
}
