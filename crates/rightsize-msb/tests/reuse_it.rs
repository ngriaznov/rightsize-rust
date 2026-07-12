//! `sandbox-it` integration test for container reuse (`rightsize::Container::reuse`)
//! against the msb backend: same-process adoption — a second, equivalent `Container`
//! built in the SAME process adopts the sandbox the first one created, rather than
//! booting a fresh one (same name, same mapped port, exercised via a real `exec`
//! round trip against the live sandbox to prove it's genuinely the same one, not a
//! lookalike that merely reports matching metadata).
//!
//! Run for real:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test reuse_it
//! ```
//!
//! Cleanup: a reuse sandbox is never torn down by `stop()` — that's the whole
//! feature — so this test removes it for real via a freshly built `MsbCliBackend`'s
//! own `remove_by_name`, the same call the reaping sweep/watchdog use, so nothing
//! leaks into the msb-linux/msb-windows CI lanes' shared image/sandbox state. Wired
//! through a [`ReuseCleanup`] `Drop` guard rather than a bare trailing call, so it
//! still runs if an assertion partway through the test panics.
//!
//! **Identity is nonce'd per run** (`RZ_TEST_NONCE`, see [`nonce`]): without it, this
//! test's identity would be the same fixed `{image, exposed_ports}` spec every run,
//! so a sandbox left behind by a previous crashed run — the exact orphan
//! `docs/reuse.md#crash-mid-boot-orphan-recovery` describes — could collide with
//! this run's own fresh-create. The library's own orphan recovery (see that doc)
//! now handles that collision safely either way, but nonce'ing the identity here
//! means this test never even exercises that path by accident — it's testing
//! same-process adoption, not crash recovery.

#![cfg(feature = "sandbox-it")]

use std::sync::Once;
use std::time::Duration;

use rightsize::backend::{BackendProvider, SandboxBackend};
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

/// Registers the msb provider exactly once per process and forces both
/// `RIGHTSIZE_BACKEND=microsandbox` and `RIGHTSIZE_REUSE=true` for this process
/// unless the caller already set them — this whole suite is ABOUT reuse, so the
/// second half of the double opt-in needs to be on for every test in it, the same
/// process-wide, set-once-at-startup shape `network_links_it.rs` already uses for
/// `RIGHTSIZE_BACKEND`.
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
        if std::env::var("RIGHTSIZE_REUSE").is_err() {
            // SAFETY-of-intent note: same reasoning as the `RIGHTSIZE_BACKEND` call
            // just above — set once, before any other thread reads it.
            unsafe { std::env::set_var("RIGHTSIZE_REUSE", "true") };
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

/// A per-process random-enough hex nonce (reuses `rightsize::RunId`'s own
/// time+pid-mixed generator rather than hand-rolling a second one) injected as
/// `RZ_TEST_NONCE` into every reused spec this file builds — see this file's module
/// doc for why a leftover sandbox from an earlier run must never collide with this
/// run's identity.
fn nonce() -> &'static str {
    rightsize::RunId::value()
}

/// Best-effort teardown for one reuse sandbox: removes it at the backend level and
/// deletes its registry file. Built as a `Drop` guard — not a bare trailing
/// call — specifically so it still runs if an assertion partway through the test
/// panics; a reuse sandbox is never torn down by `stop()` (that's the feature), so
/// a panic between `start()` and the old trailing cleanup call used to leak it into
/// shared CI backend state for good.
struct ReuseCleanup {
    name: String,
}

impl Drop for ReuseCleanup {
    fn drop(&mut self) {
        if let Ok(msb_path) = rightsize_msb::provisioner::ensure_installed() {
            let cleanup_backend = rightsize_msb::MsbCliBackend::new(msb_path);
            cleanup_backend.remove_by_name(&self.name);
        }
        remove_reuse_registry_file_for(&self.name);
    }
}

/// Same-process adoption: a second, equivalent `Container` adopts the sandbox the
/// first one created — identical name, identical mapped port, and a real `PING`
/// round trip against the sandbox `second` resolves to proves it's the exact same
/// live sandbox `first` booted, not a coincidentally-matching new one.
#[tokio::test]
async fn a_second_equivalent_container_adopts_the_first_ones_sandbox() {
    require_msb!();

    let build = || {
        // Log-anchored readiness, not a bare port wait: microsandbox's loopback
        // forwarder accepts connections before the guest workload listens, so only
        // Redis's own "Ready to accept connections" line proves the PING below can
        // succeed — the same rationale as RedisContainer's default wait strategy.
        Container::new("redis:7-alpine")
            .with_exposed_ports(&[6379])
            .with_env("RZ_TEST_NONCE", nonce())
            .waiting_for(
                Wait::for_log_message(".*Ready to accept connections.*", 1)
                    .with_startup_timeout(Duration::from_secs(60)),
            )
            .reuse(true)
    };

    let first = build()
        .start()
        .await
        .expect("first start() must create+start a fresh reuse sandbox");
    let name = first.name().to_string();
    // From here on, cleanup must run even if a later assertion panics.
    let _cleanup = ReuseCleanup { name: name.clone() };
    assert!(name.starts_with("rz-reuse-"), "{name}");
    let first_port = first
        .get_mapped_port(6379)
        .expect("first sandbox must have a mapped port");

    // stop() on a reuse guard leaves the sandbox running — the feature itself.
    first.stop().await.unwrap();

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

    // `_cleanup` fires on drop, whether we reach here normally or unwound
    // through a panic above.
}

/// Removes this test's own `<cacheDir>/reuse/<hash>.json` registry file (see
/// `docs/reuse.md#the-registry`) — written the moment the reused sandbox first
/// starts and never removed by `stop()` (that's the feature), so leaving it behind
/// would accumulate one file per run under the shared msb-linux/msb-windows CI
/// lanes' cache dir. The file's own on-disk name is keyed by the FULL identity
/// hash, not the 12-hex prefix `name` carries, so this matches by the registry
/// entry's own recorded `name` field instead of guessing the filename.
fn remove_reuse_registry_file_for(name: &str) {
    let reuse_dir = rightsize::cache_dir::dir().join("reuse");
    let Ok(entries) = std::fs::read_dir(&reuse_dir) else {
        return; // no registry directory at all: nothing to clean up.
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(raw) = std::fs::read(&path) else {
            continue;
        };
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&raw) else {
            continue;
        };
        if value.get("name").and_then(serde_json::Value::as_str) == Some(name) {
            let _ = std::fs::remove_file(&path);
        }
    }
}
