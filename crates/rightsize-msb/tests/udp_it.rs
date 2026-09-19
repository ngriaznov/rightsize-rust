//! `sandbox-it` integration test for UDP support (UDP Phase 1) against a real msb
//! runtime: host -> guest UDP delivery through a `with_exposed_udp_ports`-declared,
//! `get_mapped_udp_port`-read-back host port — msb's own "-p HOST:GUEST/udp"
//! published-port path (`rightsize_msb::commands::run`'s new `/udp` suffix),
//! live-verified end to end rather than just at the argv-construction level (that
//! half is covered by this crate's own unit tests in `commands.rs`).
//!
//! Run for real:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test udp_it
//! ```
//!
//! **Guest-to-guest UDP (network links) is deliberately NOT exercised here**: msb
//! has no guest-to-guest networking in Phase 1, so a udp `NetworkLink` is refused
//! before install even attempts it (see `backend.rs`'s own
//! `require_no_udp_links_*` unit tests) — there is nothing left to integration-test
//! on that path.
//!
//! **UDP is lossy, and msb's own emulated networking adds its own translation
//! hop**, so the datagram send below is wrapped in a short, bounded resend loop
//! rather than trusting a single send to land — the guest's `nc -u -l` may not have
//! finished starting the first (or third) time this sends. A guest that has
//! already captured a datagram and exited simply ignores every later resend
//! (nothing is listening any more), so over-sending is harmless; a resend loop is
//! the correct tolerance here, not a flake.

#![cfg(feature = "sandbox-it")]

use std::net::UdpSocket;
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

    let container = Container::new("alpine:3.19")
        .with_exposed_udp_ports(&[guest_port])
        .with_command(&["sh", "-c", &guest_cmd])
        // A udp-only container is vacuously ready under the default wait (see
        // `Container::with_exposed_udp_ports`'s own doc) — an explicit
        // log-message wait is this test's own readiness signal instead, exactly
        // as that doc recommends for a real UDP-only service. `times = 0` would
        // be a no-op (ready immediately, before the guest has produced any
        // output at all — see `wait.rs`'s own doc comment and
        // `for_log_message_times_zero_succeeds_immediately` test), so this uses
        // `times = 1` to genuinely block until the guest agent has emitted its
        // first line of boot chatter (".*" matches any line) before the host
        // starts sending datagrams.
        .waiting_for(Wait::for_log_message(".*", 1));
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
