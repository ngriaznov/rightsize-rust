//! `sandbox-it` integration tests for `DockerBackend` — the docker-specific surface
//! covered here: container run/exec/logs/stop lifecycle, port publish to
//! `127.0.0.1`, bind-conflict classification, label-scoped cleanup, native network +
//! alias connect, and `follow_logs` exactly-once delivery with the final-fragment
//! flush. Contract-level parity (the shared backend contract suite run on both
//! backends) is covered separately, in `contract.rs`.
//!
//! Run for real against a real Docker daemon:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-docker --features sandbox-it
//! ```
//!
//! Every test skips itself (rather than failing) when this host has no reachable
//! Docker daemon, or when `RIGHTSIZE_BACKEND` explicitly names a different backend —
//! the same guard shape the msb ITs use.

#![cfg(feature = "sandbox-it")]

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rightsize::backend::{BackendProvider, SandboxBackend};
use rightsize::model::ContainerSpec;
use rightsize_docker::{DockerBackend, DockerBackendProvider};

fn docker_runtime_available() -> bool {
    if std::env::var("RIGHTSIZE_BACKEND")
        .map(|v| v.eq_ignore_ascii_case("microsandbox") || v.eq_ignore_ascii_case("msb"))
        .unwrap_or(false)
    {
        return false;
    }
    DockerBackendProvider.is_supported()
}

/// Skips the calling test (prints a notice and returns) unless this host has a
/// reachable Docker daemon and hasn't been asked to run msb instead.
macro_rules! require_docker {
    () => {
        if !docker_runtime_available() {
            eprintln!(
                "skipping: no reachable Docker daemon on this host (or RIGHTSIZE_BACKEND=microsandbox)"
            );
            return;
        }
    };
}

fn unique_name(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("rz-{label}-{}", &format!("{nanos:x}")[..8])
}

/// Binds an ephemeral loopback port and immediately releases it — good enough for a
/// short-lived test's own port-choosing (this crate has no access to core's
/// crate-private `FreePorts`, nor should it: backends never allocate ports — only
/// *test* code picking a port to hand a `ContainerSpec` is doing anything analogous
/// to what `rightsize`'s own allocator does internally).
fn free_host_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral loopback port")
        .local_addr()
        .unwrap()
        .port()
}

async fn cleanup(backend: &DockerBackend, handle: &dyn rightsize::backend::SandboxHandle) {
    let _ = backend.stop(handle).await;
    let _ = backend.remove(handle).await;
}

/// Basic container lifecycle: create, start, exec with a real exit code, logs capture
/// stdout, stop+remove tears it down (a post-remove exec errors).
#[tokio::test]
async fn container_lifecycle_create_exec_logs_stop_remove() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let spec = ContainerSpec {
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "echo hello-from-docker-it; sleep 120".to_string(),
        ]),
        ..ContainerSpec::new(unique_name("lifecycle"), "alpine:3.19", "docker-backend-it")
    };
    let handle = backend.create(spec).await.expect("create");
    backend.start(handle.as_ref()).await.expect("start");

    // Give the entrypoint a moment to print its line before logs/exec race it.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let logs = backend.logs(handle.as_ref()).await.expect("logs");
    assert!(
        logs.contains("hello-from-docker-it"),
        "expected stdout in logs, got: {logs:?}"
    );

    let result = backend
        .exec(
            handle.as_ref(),
            &["sh".to_string(), "-c".to_string(), "exit 7".to_string()],
        )
        .await
        .expect("exec must succeed even for a nonzero exit");
    assert_eq!(result.exit_code, 7);

    let ok = backend
        .exec(
            handle.as_ref(),
            &["echo".to_string(), "exec-ok".to_string()],
        )
        .await
        .expect("exec");
    assert_eq!(ok.exit_code, 0);
    assert!(ok.stdout.contains("exec-ok"), "stdout: {:?}", ok.stdout);

    backend.stop(handle.as_ref()).await.expect("stop");
    backend.remove(handle.as_ref()).await.expect("remove");

    // Exec against a removed container must error, not hang or silently succeed.
    let after_remove = backend
        .exec(
            handle.as_ref(),
            &["echo".to_string(), "should-fail".to_string()],
        )
        .await;
    assert!(
        after_remove.is_err(),
        "exec after remove must error, got: {after_remove:?}"
    );
}

/// Port publish to host loopback: a container listening on a guest port must be
/// reachable at `127.0.0.1:<host_port>` after start.
#[tokio::test]
async fn port_publish_binds_to_host_loopback() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let host_port = free_host_port();
    let mut spec = ContainerSpec {
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\n\\r\\nok' | nc -l -p 8000; done".to_string(),
        ]),
        ..ContainerSpec::new(unique_name("port-publish"), "alpine:3.19", "docker-backend-it")
    };
    spec.ports.push(rightsize::model::PortBinding {
        host_port,
        guest_port: 8000,
    });
    let handle = backend.create(spec).await.expect("create");
    backend.start(handle.as_ref()).await.expect("start");

    // `nc`/busybox startup + first-listen needs a beat.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let connected = std::net::TcpStream::connect(("127.0.0.1", host_port));
    assert!(
        connected.is_ok(),
        "expected 127.0.0.1:{host_port} to be reachable after publish, got: {connected:?}"
    );

    cleanup(&backend, handle.as_ref()).await;
}

/// A host-port bind conflict from the daemon (a 500 with "address already in
/// use"/"port is already allocated" in the message) must classify as a typed
/// `PortBindConflict`, not a generic `Backend` error — grab the port first with a
/// plain listener so the daemon's own bind genuinely collides.
#[tokio::test]
async fn start_classifies_a_host_port_conflict_as_a_typed_error() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let host_port = free_host_port();
    // Hold the port open on the host so the daemon's own bind attempt collides with a
    // real occupant, not a race against this test's own port-choosing.
    let _holder = TcpListener::bind(("127.0.0.1", host_port)).expect("hold the port");

    let mut spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "60".to_string()]),
        ..ContainerSpec::new(
            unique_name("port-conflict"),
            "alpine:3.19",
            "docker-backend-it",
        )
    };
    spec.ports.push(rightsize::model::PortBinding {
        host_port,
        guest_port: 8000,
    });
    let handle = backend.create(spec).await.expect("create");
    let start_result = backend.start(handle.as_ref()).await;

    assert!(
        matches!(
            start_result,
            Err(rightsize::error::RightsizeError::PortBindConflict { .. })
        ),
        "expected a typed PortBindConflict, got: {start_result:?}"
    );

    let _ = backend.remove(handle.as_ref()).await;
}

/// Containers this backend creates carry the `dev.rightsize.run_id` label, and
/// `close()` force-removes exactly this run's labeled containers.
#[tokio::test]
async fn close_removes_this_runs_labeled_containers() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "60".to_string()]),
        ..ContainerSpec::new(
            unique_name("label-cleanup"),
            "alpine:3.19",
            rightsize::RunId::value(),
        )
    };
    let handle = backend.create(spec).await.expect("create");
    backend.start(handle.as_ref()).await.expect("start");

    backend.close().await.expect("close");

    // A removed container's exec must error — proving close() actually reached it.
    let after_close = backend
        .exec(handle.as_ref(), &["echo".to_string(), "x".to_string()])
        .await;
    assert!(
        after_close.is_err(),
        "container should have been removed by close(), exec got: {after_close:?}"
    );
}

/// Native networks + alias connect: two containers on the same `rightsize` network
/// must resolve each other by alias (docker's own DNS, not msb's emulated aliasing).
#[tokio::test]
async fn native_network_connects_containers_by_alias() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let network_id = format!("rz-net-it-{}", &unique_name("net")[3..11]);
    backend
        .ensure_network(&network_id)
        .await
        .expect("ensure_network");

    let mut server_spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "60".to_string()]),
        ..ContainerSpec::new(
            unique_name("net-server"),
            "alpine:3.19",
            "docker-backend-it",
        )
    };
    server_spec.network_id = Some(network_id.clone());
    server_spec.aliases = vec!["net-server-alias".to_string()];
    let server = backend.create(server_spec).await.expect("create server");
    backend.start(server.as_ref()).await.expect("start server");

    let mut client_spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "60".to_string()]),
        ..ContainerSpec::new(
            unique_name("net-client"),
            "alpine:3.19",
            "docker-backend-it",
        )
    };
    client_spec.network_id = Some(network_id.clone());
    let client = backend.create(client_spec).await.expect("create client");
    backend.start(client.as_ref()).await.expect("start client");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let resolved = backend
        .exec(
            client.as_ref(),
            &[
                "getent".to_string(),
                "hosts".to_string(),
                "net-server-alias".to_string(),
            ],
        )
        .await
        .expect("exec getent");
    assert_eq!(
        resolved.exit_code, 0,
        "expected the alias to resolve inside the client container, stderr: {}",
        resolved.stderr
    );

    cleanup(&backend, client.as_ref()).await;
    cleanup(&backend, server.as_ref()).await;
    backend
        .remove_network(&network_id)
        .await
        .expect("remove_network");
}

/// `follow_logs` must deliver lines in order and stop delivering once closed, and the
/// final unterminated line must arrive exactly once.
#[tokio::test]
async fn follow_logs_delivers_in_order_and_flushes_the_final_fragment_exactly_once() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let spec = ContainerSpec {
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "echo line-one; echo line-two; sleep 1; printf 'line-three-no-newline'".to_string(),
        ]),
        ..ContainerSpec::new(unique_name("follow"), "alpine:3.19", "docker-backend-it")
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

    // Let the workload finish and the stream close on its own.
    tokio::time::sleep(Duration::from_secs(3)).await;
    follow.close();

    let lines = received.lock().unwrap().clone();
    assert_eq!(
        lines,
        vec![
            "line-one".to_string(),
            "line-two".to_string(),
            "line-three-no-newline".to_string(),
        ],
        "lines must arrive in order with the final unterminated fragment exactly once, \
         got: {lines:?}"
    );

    cleanup(&backend, handle.as_ref()).await;
}

/// An explicit `close()` on a still-running follow stream must halt delivery promptly
/// without hanging — proving the cooperative `close_requested` check (the P1 forward
/// flag this task resolves) actually lets the streaming task wind down rather than
/// only relying on `abort()`.
#[tokio::test]
async fn follow_logs_close_halts_delivery_promptly_while_still_running() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let spec = ContainerSpec {
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "i=0; while true; do echo line-$i; i=$((i+1)); sleep 0.1; done".to_string(),
        ]),
        ..ContainerSpec::new(
            unique_name("follow-close"),
            "alpine:3.19",
            "docker-backend-it",
        )
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

    tokio::time::sleep(Duration::from_millis(500)).await;

    let closed = tokio::time::timeout(Duration::from_secs(5), async { follow.close() }).await;
    assert!(
        closed.is_ok(),
        "close() must return promptly, not hang waiting on the still-running workload"
    );

    let count_at_close = received.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let count_after_wait = received.lock().unwrap().len();
    assert_eq!(
        count_at_close, count_after_wait,
        "no more lines should be delivered after close() returns"
    );

    cleanup(&backend, handle.as_ref()).await;
}
