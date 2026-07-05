//! `sandbox-it` integration tests for `install_network_links`/`ExecTunnel` — the msb
//! network-link emulation this backend layers over `/etc/hosts` + `exec --stream` byte
//! pumps, since a microVM has no real bridge/subnet on this msb build. Covers alias
//! round-trip reachability, dup-guest-port fail-fast, nc-less fail-fast with a docker
//! remedy, and a shell-quoting-breaking alias rejected before the hosts-file shell-out.
//!
//! Run for real:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test network_links_it
//! ```
//!
//! These tests drive the full public `rightsize` core API (`Container` + `Network`),
//! not `MsbCliBackend` directly — registering this crate's provider once via
//! `rightsize::backends::register_provider` and letting `Container::start()` resolve it
//! through `RIGHTSIZE_BACKEND=microsandbox`, exactly the path a real caller uses. This
//! is also what exercises the exec-tunnel/hosts-alias plumbing end-to-end, since that
//! logic lives behind `SandboxBackend::install_network_links`, which only `Container`'s
//! network-join path calls.

#![cfg(feature = "sandbox-it")]

use std::sync::Arc;
use std::sync::Once;

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

/// Registers the msb provider exactly once per process (a second `register_provider`
/// call would just double the registry entry — harmless, but pointless) and forces
/// `RIGHTSIZE_BACKEND=microsandbox` for this process unless the caller already set it,
/// so `cargo test` run without the env var still exercises this backend rather than
/// silently resolving to whatever else might be registered/supported.
fn ensure_registered() {
    REGISTER.call_once(|| {
        rightsize::backends::register_provider(Box::new(MsbBackendProvider));
        if std::env::var("RIGHTSIZE_BACKEND").is_err() {
            // SAFETY-of-intent note: `std::env::set_var` is unsafe as of the 2024
            // edition; this only runs once, before any other thread in this test
            // binary has started touching Container/backends::active(), and every
            // test in this binary wants the same backend forced — the same
            // process-wide, set-once-at-startup shape `RIGHTSIZE_BACKEND` already has
            // for a real caller exporting it before running their suite.
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

/// `ContainerGuard` deliberately isn't `Debug` (it holds a `Box<dyn SandboxHandle>` and
/// friends), so `Result::expect_err` doesn't work directly on a failed `start()` — this
/// pulls the error out by hand, same pattern the core crate's own tests use.
fn expect_start_err(
    result: rightsize::Result<rightsize::ContainerGuard>,
    msg: &str,
) -> rightsize::RightsizeError {
    match result {
        Ok(_) => panic!("{msg}: expected an error, got Ok"),
        Err(e) => e,
    }
}

/// Alias round-trip reachability: a consumer sandbox reaches a sibling server by
/// `alias:port` over the exec-tunnel (the mirage `configuration-stub:8888` pattern).
#[tokio::test]
async fn consumer_reaches_sibling_via_alias_over_exec_tunnel() {
    require_msb!();
    let net = Arc::new(Network::new_network());

    let server = Container::new("python:3.12-alpine")
        .with_command(&["python", "-m", "http.server", "8888"])
        .with_exposed_ports(&[8888])
        .with_network(&net)
        .with_network_aliases(&["configuration-stub"])
        .waiting_for(Wait::for_http("/").for_port(8888));
    let server_guard = server.start().await.expect("server must start");

    let client = Container::new("alpine:3.19")
        .with_network(&net)
        .with_command(&["sleep", "300"])
        .waiting_for(Wait::for_log_message(".*", 0));
    let client_guard = client.start().await.expect("client must start");

    let target = net
        .resolve("configuration-stub", 8888)
        .expect("alias must resolve");
    let result = client_guard
        .exec(&["wget", "-qO-", "-T", "10", &format!("http://{target}/")])
        .await
        .expect("exec must run");
    assert_eq!(result.exit_code, 0, "wget failed: {}", result.stderr);
    assert!(
        result.stdout.contains("Directory listing"),
        "unexpected body: {}",
        result.stdout
    );

    client_guard.stop().await.unwrap();
    server_guard.stop().await.unwrap();
}

/// A consumer image with no `nc`/busybox must fail `start()` fast, naming docker as
/// the remedy, and self-clean — no leftover half-started container.
#[tokio::test]
async fn consumer_image_without_nc_fails_fast_with_docker_hint() {
    require_msb!();
    let net = Arc::new(Network::new_network());

    let server = Container::new("python:3.12-alpine")
        .with_command(&["python", "-m", "http.server", "8000"])
        .with_exposed_ports(&[8000])
        .with_network(&net)
        .with_network_aliases(&["svc"])
        .waiting_for(Wait::for_http("/").for_port(8000));
    let server_guard = server.start().await.expect("server must start");

    // mongo:8.0 boots as a sandbox but has no nc (busybox) in the image — chosen
    // after finding hello-world fails to boot as a microVM on this msb build
    // ("failed to resolve guest uid 0").
    let consumer = Container::new("mongo:8.0")
        .with_command(&["sleep", "300"])
        .with_network(&net)
        .waiting_for(Wait::for_log_message(".*", 0));
    let err = expect_start_err(
        consumer.start().await,
        "a consumer image with no nc must fail start() fast",
    );
    assert!(
        err.to_string().to_lowercase().contains("docker"),
        "error should point at the docker backend: {err}"
    );

    server_guard.stop().await.unwrap();
}

/// Two siblings exposing the same guest port on one network must fail the consumer's
/// `start()` fast, naming the offending port, before any tunnel/hosts work.
#[tokio::test]
async fn duplicate_guest_port_across_siblings_fails_fast_naming_the_port() {
    require_msb!();
    let net = Arc::new(Network::new_network());

    // server2 declares guest port 8000 (so the Network bookkeeping sees a genuine
    // duplicate with server1's alias) but actually listens on 8001 — binding its real
    // workload on 8000 too would race the consumer's own tunnel `nc -l -p 8000`
    // inside server2's guest once THAT container joins the network, which is a real
    // but orthogonal race to the fail-fast behavior under test here.
    let server1 = Container::new("python:3.12-alpine")
        .with_command(&["python", "-m", "http.server", "8000"])
        .with_exposed_ports(&[8000])
        .with_network(&net)
        .with_network_aliases(&["svc-a"])
        .waiting_for(Wait::for_http("/").for_port(8000));
    let server2 = Container::new("python:3.12-alpine")
        .with_command(&["python", "-m", "http.server", "8001"])
        .with_exposed_ports(&[8000, 8001])
        .with_network(&net)
        .with_network_aliases(&["svc-b"])
        .waiting_for(Wait::for_http("/").for_port(8001));
    let server1_guard = server1.start().await.expect("server1 must start");
    let server2_guard = server2.start().await.expect("server2 must start");

    let consumer = Container::new("alpine:3.19")
        .with_command(&["sleep", "300"])
        .with_network(&net)
        .waiting_for(Wait::for_log_message(".*", 0));
    let err = expect_start_err(
        consumer.start().await,
        "duplicate guest port must fail start() fast",
    );
    assert!(err.to_string().contains("8000"), "{err}");

    server1_guard.stop().await.unwrap();
    server2_guard.stop().await.unwrap();
}

/// A sibling alias that would break the `sh -c "echo '127.0.0.1 $alias' >>
/// /etc/hosts"` quoting must be rejected up front, naming the offending alias, never
/// reaching the hosts-install shell-out.
#[tokio::test]
async fn sibling_alias_that_breaks_hosts_file_shell_quoting_is_rejected_before_install() {
    require_msb!();
    let net = Arc::new(Network::new_network());

    let server = Container::new("python:3.12-alpine")
        .with_command(&["python", "-m", "http.server", "8001"])
        .with_exposed_ports(&[8001])
        .with_network(&net)
        .with_network_aliases(&["bad'alias"])
        .waiting_for(Wait::for_http("/").for_port(8001));
    let server_guard = server.start().await.expect("server must start");

    let consumer = Container::new("alpine:3.19")
        .with_command(&["sleep", "300"])
        .with_network(&net)
        .waiting_for(Wait::for_log_message(".*", 0));
    let err = expect_start_err(
        consumer.start().await,
        "a shell-quoting-breaking alias must be rejected before install",
    );
    assert!(err.to_string().contains("bad'alias"), "{err}");

    server_guard.stop().await.unwrap();
}
