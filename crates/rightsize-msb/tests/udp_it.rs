//! `sandbox-it` integration tests for UDP support against a real msb runtime.
//!
//! Two things, in one file since both are UDP-specific and share the resend-loop
//! tolerance and fixtures below:
//!
//! - **Host <-> guest UDP** (Phase 1): host -> guest delivery through a
//!   `with_exposed_udp_ports`-declared, `get_mapped_udp_port`-read-back host port —
//!   msb's own `-p HOST:GUEST/udp` published-port path
//!   (`rightsize_msb::commands::run`'s `/udp` suffix), live-verified end to end
//!   rather than just at the argv-construction level (that half is covered by this
//!   crate's own unit tests in `commands.rs`).
//! - **Guest-to-guest UDP (network links)**: a consumer sandbox reaches a
//!   udp-exposed sibling by alias through the in-guest forwarder
//!   (`install_udp_forwarder`), the msb-only-not-a-published-port mirror of
//!   `network_links_it.rs`'s own TCP exec-tunnel scenarios — reachability, several
//!   clients through the same link, and the no-capable-`nc` fail-fast.
//!
//! Run for real:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test udp_it
//! ```
//!
//! **UDP is lossy, and msb's own emulated networking adds its own translation
//! hop**, so every datagram send below is wrapped in a short, bounded resend loop
//! rather than trusting a single send to land — the guest's `nc -u -l`/forwarder
//! may not have finished starting the first (or third) time this sends. A guest
//! that has already captured a datagram and exited (or, for a link, a forwarder
//! mid-relay-swap) simply ignores a later resend, so over-sending is harmless; a
//! resend loop is the correct tolerance here, not a flake.

#![cfg(feature = "sandbox-it")]

use std::net::UdpSocket;
use std::sync::{Arc, Once};
use std::time::Duration;

use rightsize::backend::BackendProvider;
use rightsize::{Container, Network, Wait};
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
/// it — same shape as `network_links_it.rs`'s own `ensure_registered`.
fn ensure_registered() {
    REGISTER.call_once(|| {
        rightsize::backends::register_provider(Box::new(MsbBackendProvider));
        if std::env::var("RIGHTSIZE_BACKEND").is_err() {
            // SAFETY-of-intent note: `std::env::set_var` is unsafe as of the 2024
            // edition; this only runs once, before any other thread in this test
            // binary has started touching Container/backends::active(), and every
            // test in this binary wants the same backend forced.
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

/// `ContainerGuard` deliberately isn't `Debug`, so `Result::expect_err` doesn't
/// work directly on a failed `start()` — same pulled-by-hand pattern
/// `network_links_it.rs` uses for its own TCP no-nc test.
fn expect_start_err(
    result: rightsize::Result<rightsize::ContainerGuard>,
    msg: &str,
) -> rightsize::RightsizeError {
    match result {
        Ok(_) => panic!("{msg}: expected an error, got Ok"),
        Err(e) => e,
    }
}

/// Binds an ephemeral loopback UDP port purely to mint a distinct payload nonce
/// per run (`RZ_TEST_NONCE`-style uniqueness, see `reuse_it.rs`'s own nonce
/// rationale) — not used as a network port itself.
fn payload_nonce() -> String {
    format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// Host -> guest UDP datagram via a udp-exposed mapped port: send from the test
/// process with a plain UDP socket, the guest writes whatever it receives to a
/// file via `nc -u -l -p <port> > /srv/got.txt`, polled via `exec cat` after each
/// resend attempt.
#[tokio::test]
async fn host_reaches_a_udp_exposed_guest_port_via_the_mapped_port() {
    require_msb!();

    let guest_port: u16 = 9153;
    let payload = format!("hello-udp-{}", payload_nonce());
    let guest_cmd = format!("nc -u -l -p {guest_port} > /srv/got.txt");

    // A udp-only container is vacuously ready under the default wait (see
    // `Container::with_exposed_udp_ports`'s own doc), and that is exactly what
    // this test wants: the guest's `nc -u -l` listener produces NO stdout, so
    // any log-message wait (`times >= 1`) can only time out against a
    // permanently empty stream — CI proved it, 120s against zero log lines.
    // Readiness is instead proven the way UDP itself demands: the bounded
    // resend loop below keeps sending until the guest observably received a
    // datagram, which also absorbs the listener-still-booting window and
    // ordinary datagram loss. The Kotlin twin's green CI runs use this shape.
    let container = Container::new("alpine:3.19")
        .with_exposed_udp_ports(&[guest_port])
        .with_command(&["sh", "-c", &guest_cmd]);
    let guard = container.start().await.expect("container must start");

    let host_port = guard
        .get_mapped_udp_port(guest_port)
        .expect("the udp guest port must have a mapped host port");

    let socket = UdpSocket::bind("0.0.0.0:0").expect("bind an ephemeral udp socket");
    let dest = format!("127.0.0.1:{host_port}");

    let mut delivered = false;
    for attempt in 0..10 {
        socket
            .send_to(payload.as_bytes(), &dest)
            .expect("send_to must not fail for a connectionless udp socket");
        tokio::time::sleep(Duration::from_millis(400)).await;

        let check = guard
            .exec(&["cat", "/srv/got.txt"])
            .await
            .expect("exec cat must run");
        if check.exit_code == 0 && check.stdout.contains(&payload) {
            delivered = true;
            break;
        }
        eprintln!("attempt {attempt}: datagram not observed in the guest yet, resending");
    }

    assert!(
        delivered,
        "/srv/got.txt in the guest never observed the udp payload after 10 resend attempts"
    );

    guard.stop().await.unwrap();
}

/// Guest-to-guest UDP (network links), scenario 1: a consumer reaches a
/// udp-exposed sibling through its alias, over the in-guest forwarder rather
/// than a published host port. Server: `alpine/socat:1.8.1.3`'s own
/// entrypoint kept, args only (`-T5 UDP4-RECVFROM:9153,fork EXEC:cat`) — a UDP
/// echo, started FIRST so its mapped host UDP port is known before the
/// consumer's own `start()` builds its `--net-rule` policy.
#[tokio::test]
async fn consumer_reaches_a_udp_exposed_sibling_via_alias_through_the_forwarder() {
    require_msb!();
    let net = Arc::new(Network::new_network());

    let server = Container::new("alpine/socat:1.8.1.3")
        .with_command(&["-T5", "UDP4-RECVFROM:9153,fork", "EXEC:cat"])
        .with_exposed_udp_ports(&[9153])
        .with_network(&net)
        .with_network_aliases(&["udp-echo"]);
    let server_guard = server.start().await.expect("server must start");

    let consumer = Container::new("alpine:3.19")
        .with_command(&["sleep", "3600"])
        .with_network(&net)
        .waiting_for(Wait::for_log_message(".*", 0));
    let consumer_guard = consumer.start().await.expect("consumer must start");

    let payload = format!("hello-udp-link-{}", payload_nonce());
    let mut delivered = false;
    for attempt in 0..10 {
        let result = consumer_guard
            .exec(&[
                "sh",
                "-c",
                &format!("echo {payload} | nc -u -w2 udp-echo 9153"),
            ])
            .await
            .expect("exec must run");
        if result.exit_code == 0 && result.stdout.contains(&payload) {
            delivered = true;
            break;
        }
        eprintln!("attempt {attempt}: echo not observed yet through the forwarder, retrying");
    }
    assert!(
        delivered,
        "the consumer never observed its own payload echoed back through udp-echo:9153"
    );

    consumer_guard.stop().await.unwrap();
    server_guard.stop().await.unwrap();
}

/// Scenario 2: three separate `nc` invocations from the same consumer (each
/// its own ephemeral client source port) each get their own payload back —
/// proves the forwarder serves more than one client, unlike a single locked
/// `nc -u -l`, which would only ever answer the first.
#[tokio::test]
async fn several_clients_through_the_same_udp_link_each_get_their_own_payload_back() {
    require_msb!();
    let net = Arc::new(Network::new_network());

    let server = Container::new("alpine/socat:1.8.1.3")
        .with_command(&["-T5", "UDP4-RECVFROM:9153,fork", "EXEC:cat"])
        .with_exposed_udp_ports(&[9153])
        .with_network(&net)
        .with_network_aliases(&["udp-echo-multi"]);
    let server_guard = server.start().await.expect("server must start");

    let consumer = Container::new("alpine:3.19")
        .with_command(&["sleep", "3600"])
        .with_network(&net)
        .waiting_for(Wait::for_log_message(".*", 0));
    let consumer_guard = consumer.start().await.expect("consumer must start");

    let nonce = payload_nonce();
    for client in 0..3 {
        let payload = format!("hello-udp-link-{nonce}-client-{client}");
        let mut delivered = false;
        for attempt in 0..10 {
            let result = consumer_guard
                .exec(&[
                    "sh",
                    "-c",
                    &format!("echo {payload} | nc -u -w2 udp-echo-multi 9153"),
                ])
                .await
                .expect("exec must run");
            if result.exit_code == 0 && result.stdout.contains(&payload) {
                delivered = true;
                break;
            }
            eprintln!("client {client} attempt {attempt}: echo not observed yet, retrying");
        }
        assert!(
            delivered,
            "client {client} never observed its own payload echoed back through udp-echo-multi:9153"
        );
    }

    consumer_guard.stop().await.unwrap();
    server_guard.stop().await.unwrap();
}

/// Scenario 3: a consumer image with no capable `nc` must fail `start()` fast
/// with the UDP-specific typed unsupported error, naming docker as the
/// remedy — reusing `mongo:8.0`, the same fixture `network_links_it.rs`'s own
/// TCP no-nc test uses (chosen there after finding hello-world fails to boot
/// as a microVM on this msb build).
#[tokio::test]
async fn consumer_image_without_capable_nc_fails_the_udp_link_install_fast_with_docker_hint() {
    require_msb!();
    let net = Arc::new(Network::new_network());

    let server = Container::new("alpine/socat:1.8.1.3")
        .with_command(&["-T5", "UDP4-RECVFROM:9153,fork", "EXEC:cat"])
        .with_exposed_udp_ports(&[9153])
        .with_network(&net)
        .with_network_aliases(&["udp-echo-no-nc"]);
    let server_guard = server.start().await.expect("server must start");

    let consumer = Container::new("mongo:8.0")
        .with_command(&["sleep", "300"])
        .with_network(&net)
        .waiting_for(Wait::for_log_message(".*", 0));
    let err = expect_start_err(
        consumer.start().await,
        "a consumer image with no capable nc must fail a udp link's start() fast",
    );
    assert!(
        err.to_string().to_lowercase().contains("docker"),
        "error should point at the docker backend: {err}"
    );

    server_guard.stop().await.unwrap();
}

/// Sends `payload` to `alias:9153` from inside `guard`'s sandbox, up to 10
/// times, and reports whether the echo came back.
async fn udp_echo_round_trip(
    guard: &rightsize::ContainerGuard,
    alias: &str,
    payload: &str,
) -> bool {
    for attempt in 0..10 {
        let result = guard
            .exec(&[
                "sh",
                "-c",
                &format!("echo {payload} | nc -u -w2 {alias} 9153"),
            ])
            .await
            .expect("exec must run");
        if result.exit_code == 0 && result.stdout.contains(payload) {
            return true;
        }
        eprintln!("attempt {attempt}: echo not observed yet, retrying");
    }
    false
}

/// Scenario 4: a UDP link survives a checkpoint of the consumer. msb's checkpoint
/// reboots the consumer through `msb restore`, which does not carry the run-time
/// network policy over, so the restore argv has to re-grant the host UDP port, and
/// the reboot wipes `/tmp`, so the link replay has to reinstall the forwarder.
#[tokio::test]
async fn a_checkpointed_consumer_keeps_its_udp_link() {
    require_msb!();
    let net = Arc::new(Network::new_network());
    let server_guard = Container::new("alpine/socat:1.8.1.3")
        .with_command(&["-T5", "UDP4-RECVFROM:9153,fork", "EXEC:cat"])
        .with_exposed_udp_ports(&[9153])
        .with_network(&net)
        .with_network_aliases(&["udp-echo-ckpt"])
        .start()
        .await
        .expect("server must start");
    let consumer_guard = Container::new("alpine:3.19")
        .with_command(&["sleep", "3600"])
        .with_network(&net)
        .waiting_for(Wait::for_log_message(".*", 0))
        .start()
        .await
        .expect("consumer must start");

    let nonce = payload_nonce();
    assert!(
        udp_echo_round_trip(&consumer_guard, "udp-echo-ckpt", &format!("before-{nonce}")).await,
        "the udp link must work before the checkpoint"
    );
    consumer_guard
        .checkpoint()
        .await
        .expect("checkpoint must succeed");
    assert!(
        udp_echo_round_trip(&consumer_guard, "udp-echo-ckpt", &format!("after-{nonce}")).await,
        "the udp link must work again after the checkpoint reboot"
    );

    consumer_guard.stop().await.unwrap();
    server_guard.stop().await.unwrap();
}
