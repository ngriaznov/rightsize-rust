//! `sandbox-it` integration tests for UDP support (UDP Phase 1) against a real
//! Docker daemon: container-to-container UDP on a native user-defined network
//! (docker carries it natively, with no per-port declaration — see the FACTS this
//! phase's design was verified against), and host-to-container UDP through the
//! `<guest>/udp` `ExposedPorts`/`PortBindings` keys `build_create_body` now emits.
//!
//! Run for real against a real Docker daemon:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-docker --features sandbox-it --test udp_it
//! ```
//!
//! Every test skips itself (rather than failing) when this host has no reachable
//! Docker daemon, or when `RIGHTSIZE_BACKEND` explicitly names a different backend
//! — the same guard shape `backend_it.rs` uses.
//!
//! **UDP is lossy, so every datagram send here is wrapped in a short, bounded
//! resend loop** rather than trusting a single send to land — the receiving
//! `nc -u -l` may not have started listening yet the first (or third) time this
//! sends, and neither docker's network nor a raw UDP socket gives any delivery
//! guarantee. A resend loop is the correct tolerance for that, not a flake.

#![cfg(feature = "sandbox-it")]

use std::net::UdpSocket;
use std::time::Duration;

use rightsize::backend::{BackendProvider, SandboxBackend};
use rightsize::model::{ContainerSpec, PortBinding, Protocol};
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

/// Skips the calling test (rather than failing) unless this host has a reachable
/// Docker daemon and hasn't been asked to run msb instead — same shape as
/// `backend_it.rs`'s own `require_docker!`.
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

/// Binds an ephemeral loopback UDP port and immediately releases it — the UDP
/// counterpart to `backend_it.rs`'s own `free_host_port` (a `TcpListener` probe
/// proves nothing about UDP's independent port table — see `rightsize::model::
/// Protocol`'s own doc).
fn free_host_udp_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0")
        .expect("bind an ephemeral loopback udp port")
        .local_addr()
        .unwrap()
        .port()
}

async fn cleanup(backend: &DockerBackend, handle: &dyn rightsize::backend::SandboxHandle) {
    let _ = backend.stop(handle).await;
    let _ = backend.remove(handle).await;
}

/// Container-to-container UDP on a shared native network: FACTS (verified
/// 2026-09-19) says docker's user-defined networks carry UDP between members
/// freely, with no per-port declaration at all — this proves it against a real
/// daemon. A server captures whatever it receives to a file via busybox
/// `nc -u -l`; a client on the same network sends it a datagram by alias with
/// `nc -u`; the server's own captured file is the proof of delivery.
#[tokio::test]
async fn udp_datagrams_flow_freely_between_containers_on_a_shared_network() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let network_id = format!("rz-net-it-{}", &unique_name("udp-net")[3..11]);
    backend
        .ensure_network(&network_id)
        .await
        .expect("ensure_network");

    let server_spec = ContainerSpec {
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "nc -u -l -p 9200 > /tmp/got.txt".to_string(),
        ]),
        network_id: Some(network_id.clone()),
        aliases: vec!["udp-server".to_string()],
        ..ContainerSpec::new(
            unique_name("udp-server"),
            "alpine:3.19",
            "docker-backend-it",
        )
    };
    let server = backend.create(server_spec).await.expect("create server");
    backend.start(server.as_ref()).await.expect("start server");

    let client_spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "60".to_string()]),
        network_id: Some(network_id.clone()),
        ..ContainerSpec::new(
            unique_name("udp-client"),
            "alpine:3.19",
            "docker-backend-it",
        )
    };
    let client = backend.create(client_spec).await.expect("create client");
    backend.start(client.as_ref()).await.expect("start client");

    // Let both entrypoints (and docker's own DNS for the alias) settle before the
    // first send attempt.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut delivered = false;
    for attempt in 0..10 {
        let send = backend
            .exec(
                client.as_ref(),
                &[
                    "sh".to_string(),
                    "-c".to_string(),
                    "echo -n hello-udp-echo | nc -u -w1 udp-server 9200".to_string(),
                ],
            )
            .await
            .expect("exec nc -u on client");
        assert_eq!(
            send.exit_code, 0,
            "client-side nc -u must itself succeed (attempt {attempt}): {}",
            send.stderr
        );

        tokio::time::sleep(Duration::from_millis(300)).await;

        let check = backend
            .exec(
                server.as_ref(),
                &["cat".to_string(), "/tmp/got.txt".to_string()],
            )
            .await
            .expect("exec cat on server");
        if check.exit_code == 0 && check.stdout.contains("hello-udp-echo") {
            delivered = true;
            break;
        }
        eprintln!("attempt {attempt}: datagram not observed on the server yet, resending");
    }
    assert!(
        delivered,
        "the udp datagram never reached the server container after 10 resend attempts"
    );

    cleanup(&backend, client.as_ref()).await;
    cleanup(&backend, server.as_ref()).await;
    let _ = backend.remove_network(&network_id).await;
}

/// Host -> container UDP through the mapped host port: the `<guest>/udp`
/// `ExposedPorts`/`PortBindings` keys `build_create_body` emits for a
/// [`Protocol::Udp`] [`PortBinding`] must actually work end to end against a real
/// daemon, not just serialize correctly (that half is covered by this crate's own
/// unit tests).
#[tokio::test]
async fn host_reaches_a_container_over_its_mapped_udp_port() {
    require_docker!();
    let backend = DockerBackend::connecting_to_env();

    let host_port = free_host_udp_port();
    let mut spec = ContainerSpec {
        command: Some(vec![
            "sh".to_string(),
            "-c".to_string(),
            "nc -u -l -p 9201 > /tmp/got.txt".to_string(),
        ]),
        ..ContainerSpec::new(
            unique_name("udp-host-map"),
            "alpine:3.19",
            "docker-backend-it",
        )
    };
    spec.ports.push(PortBinding {
        host_port,
        guest_port: 9201,
        protocol: Protocol::Udp,
    });
    let handle = backend.create(spec).await.expect("create");
    backend.start(handle.as_ref()).await.expect("start");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let socket = UdpSocket::bind("0.0.0.0:0").expect("bind an ephemeral udp socket");
    let dest = format!("127.0.0.1:{host_port}");
    let payload = b"hello-from-host";

    let mut delivered = false;
    for attempt in 0..10 {
        socket
            .send_to(payload, &dest)
            .expect("send_to must not fail for a connectionless udp socket");
        tokio::time::sleep(Duration::from_millis(300)).await;

        let check = backend
            .exec(
                handle.as_ref(),
                &["cat".to_string(), "/tmp/got.txt".to_string()],
            )
            .await
            .expect("exec cat");
        if check.exit_code == 0 && check.stdout.contains("hello-from-host") {
            delivered = true;
            break;
        }
        eprintln!("attempt {attempt}: datagram not observed in the container yet, resending");
    }
    assert!(
        delivered,
        "the host-sent udp datagram never reached the container after 10 resend attempts"
    );

    cleanup(&backend, handle.as_ref()).await;
}
