//! Shared `sandbox-it` test scaffolding for the module integration tests: registers
//! both backend providers once per test binary and lets `RIGHTSIZE_BACKEND` (set by
//! the caller — see each test file's module doc for the invocation) pick which one
//! `Container::start()` resolves to, exactly the path a real caller uses.
//!
//! Each test binary is its own process, and `rightsize::backends::active()` caches
//! its resolution for the lifetime of that process — so running the same suite twice
//! with different `RIGHTSIZE_BACKEND` values (once per backend, to cover both) means
//! two separate `cargo test` invocations, not two branches inside one test.
//!
//! `tests/support/mod.rs` is compiled fresh into every `tests/*.rs` binary via `mod
//! support;` (each integration-test file is its own crate root), so a helper only
//! some of those binaries call is genuinely dead code from any one binary's own
//! point of view — each call site's `mod support;` carries `#[allow(dead_code)]` for
//! exactly that reason, rather than chasing per-binary warnings for a file that's
//! shared by construction.

use rightsize::backend::BackendProvider;
use rightsize_docker::DockerBackendProvider;
use rightsize_msb::MsbBackendProvider;

/// Registers both providers through the crate's own public entry point — the same
/// call every module `start` makes — so these tests exercise the exact registration
/// path a real caller gets. Does NOT force a default `RIGHTSIZE_BACKEND` — unlike
/// the backend crates' own single-backend ITs, module ITs are meant to run once per
/// backend via an explicit `RIGHTSIZE_BACKEND=...` invocation (see each test file's
/// module doc), so silently defaulting here would mask a caller forgetting to set it.
pub fn ensure_registered() {
    rightsize_modules::register_default_backends();
}

/// True if the backend named by `RIGHTSIZE_BACKEND` (or the highest-priority
/// supported one, if unset) is actually usable on this host. Every module IT test
/// should open with `require_backend!()` so it skips itself (rather than failing)
/// when this host has neither runtime available.
pub fn requested_backend_available() -> bool {
    ensure_registered();
    match std::env::var("RIGHTSIZE_BACKEND") {
        Ok(name) if name.eq_ignore_ascii_case("docker") => DockerBackendProvider.is_supported(),
        Ok(name)
            if name.eq_ignore_ascii_case("microsandbox") || name.eq_ignore_ascii_case("msb") =>
        {
            MsbBackendProvider.is_supported()
        }
        Ok(_) => false, // an explicitly unknown name: let Container::start() surface that error.
        Err(_) => DockerBackendProvider.is_supported() || MsbBackendProvider.is_supported(),
    }
}

/// Skips the calling test (prints a notice and returns) unless the requested backend
/// is available on this host. Expects `mod support;` (or an equivalent path prefix)
/// to be in scope at the call site — each `tests/*.rs` file is its own crate root, so
/// this can't be `#[macro_export]`-ed the way an in-crate macro would be; each test
/// file defines its own thin `require_backend!()` wrapper calling
/// `support::requested_backend_available()` instead.
pub fn skip_notice() {
    eprintln!(
        "skipping: no supported backend for RIGHTSIZE_BACKEND={:?} on this host",
        std::env::var("RIGHTSIZE_BACKEND")
    );
}

/// A `ureq` agent with an explicit whole-request timeout, for module ITs that poll an
/// HTTP endpoint. `ureq`'s own default has no global timeout at all — a container
/// that accepts the connection but replies slowly (or a chunked body that never
/// finishes) can otherwise hang a single `.call()` well past any surrounding retry
/// loop's intended budget, which is exactly what happened diagnosing the Redpanda
/// schema-registry smoke (a real, reproducible 503 on this image/version, not a
/// timing fluke — see `broker_modules_it.rs`).
pub fn http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(5)))
        .build()
        .into()
}

/// A minimal, dependency-free base64 encoder — just enough for a `Basic` auth header
/// value. `base64` is only a *transitive* dependency of this workspace (pulled in by
/// other crates' own deps), and Rust doesn't let a crate `use` a dependency it hasn't
/// declared directly, so hand-rolling ~10 lines here keeps every module IT's own
/// `Cargo.toml` footprint at zero, matching the house preference for small
/// hand-rolled encoders over a new direct dependency (see `rightsize-docker/src/json.rs`).
/// Shared here rather than copy-pasted per-file (was duplicated verbatim across
/// `rabbitmq_it.rs`, `clickhouse_it.rs`, and `neo4j_it.rs`).
pub fn basic_auth_header(username: &str, password: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let input = format!("{username}:{password}");
    let bytes = input.as_bytes();
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(b2 & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    format!("Basic {out}")
}
