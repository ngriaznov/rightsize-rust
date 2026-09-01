//! [`DockerBackendProvider`]: the discoverable factory `rightsize::backends::resolve`
//! picks among. Supported exactly when `GET /_ping` succeeds against the daemon this
//! process would otherwise talk to — no Docker/Podman/Colima running is the common
//! "not supported" case on a microVM-capable host that just prefers msb anyway.

use rightsize::backend::{BackendProvider, SandboxBackend};
use rightsize::error::Result;

use crate::backend::DockerBackend;
use crate::client::DockerClient;

/// The Docker backend's [`BackendProvider`]. Priority 10 — lower than msb's 20, so a
/// microVM-capable host prefers the microVM by default; Docker is the fallback for
/// hosts without one (Intel Macs, Windows, no `/dev/kvm`), and doubles as the
/// contract suite's correctness oracle.
pub struct DockerBackendProvider;

impl BackendProvider for DockerBackendProvider {
    fn name(&self) -> &str {
        "docker"
    }

    fn priority(&self) -> u32 {
        10
    }

    fn is_supported(&self) -> bool {
        // `BackendProvider::is_supported` is a plain synchronous fn — it can run
        // before any Tokio runtime exists yet (early process startup), or from
        // *inside* one already (a `#[tokio::test]` resolving a backend), and
        // `Runtime::block_on` panics in the latter case ("cannot start a runtime from
        // within a runtime"). Rather than juggle "is there already a runtime" logic,
        // this probe is a plain blocking GET /_ping over
        // [`crate::stream::BlockingDockerStream`] (a unix socket on unix, Docker
        // Desktop's named pipe on Windows) — the same blocking-transport shape
        // `DockerBackend::cleanup_sync` already uses for its own no-Tokio-in-context
        // constraint, and the honest fit for a synchronous trait method that must work
        // in either context. Windows runs the same real connect + `GET /_ping` as
        // unix, not merely a check that the pipe path exists (a stale or orphaned pipe
        // existing without a daemon actually answering behind it would otherwise
        // report "supported" for a daemon that isn't really there) — bounded to the
        // same 2-second budget on both platforms, though by different means: unix gets
        // it from the socket's own `SO_RCVTIMEO`/`SO_SNDTIMEO`, Windows from
        // `crate::stream::run_with_deadline` wrapping the whole round trip, since a
        // Windows named-pipe handle has no per-read/write deadline knob of its own.
        blocking_ping(DockerClient::from_env().socket_path())
    }

    fn unsupported_reason(&self) -> String {
        "no reachable Docker-API socket (Docker/Podman/Colima not running?)".to_string()
    }

    fn create(&self) -> Result<Box<dyn SandboxBackend>> {
        Ok(Box::new(DockerBackend::connecting_to_env()))
    }
}

/// A minimal blocking `GET /_ping`, true only on a 2xx response — see
/// [`DockerBackendProvider::is_supported`]'s doc for why this is blocking std I/O
/// rather than a reused async request. `target` is a unix socket path on unix, a
/// named pipe path on Windows — [`crate::stream::connect_blocking`] is what actually
/// dispatches on that.
///
/// On Windows this runs [`blocking_ping_once`] under [`crate::stream::run_with_deadline`]
/// rather than calling it directly: a Windows named-pipe handle has no per-read/write
/// deadline of its own (see `connect_blocking`'s Windows doc), so without this wrapper
/// a pipe that accepts the connection but sits behind a wedged daemon would hang this
/// probe — and so `is_supported()` — indefinitely. Unix already gets an equivalent
/// bound for free from `connect_blocking`'s `SO_RCVTIMEO`/`SO_SNDTIMEO`, so it calls
/// `blocking_ping_once` straight, unchanged from before this module existed.
fn blocking_ping(target: &std::path::Path) -> bool {
    #[cfg(unix)]
    {
        blocking_ping_once(target).unwrap_or(false)
    }
    #[cfg(windows)]
    {
        let target = target.to_path_buf();
        crate::stream::run_with_deadline(std::time::Duration::from_secs(2), move || {
            blocking_ping_once(&target)
        })
        .unwrap_or(false)
    }
}

/// The actual connect-plus-`GET /_ping`-plus-parse round trip behind [`blocking_ping`],
/// factored out so both the direct (unix) and deadline-bounded (Windows) call sites
/// run identical logic.
fn blocking_ping_once(target: &std::path::Path) -> std::io::Result<bool> {
    use std::io::{Read, Write};
    use std::time::Duration;

    let mut stream = crate::stream::connect_blocking(target, Duration::from_secs(2))?;
    stream.write_all(b"GET /_ping HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n")?;
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    let text = String::from_utf8_lossy(&response);
    let status_line = text.lines().next().unwrap_or_default();
    Ok(status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .map(|code| (200..300).contains(&code))
        .unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_priority_are_pinned() {
        let provider = DockerBackendProvider;
        assert_eq!(provider.name(), "docker");
        assert_eq!(provider.priority(), 10);
    }

    #[test]
    fn unsupported_reason_names_the_daemon_socket() {
        let provider = DockerBackendProvider;
        let reason = provider.unsupported_reason();
        assert!(reason.to_lowercase().contains("docker"));
    }
}
