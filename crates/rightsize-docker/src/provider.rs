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
        // this probe is a plain blocking `std::os::unix::net::UnixStream` GET
        // /_ping — the same blocking-transport shape `DockerBackend::cleanup_sync`
        // already uses for its own no-Tokio-in-context constraint, and the honest fit
        // for a synchronous trait method that must work in either context.
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
/// rather than a reused async request.
fn blocking_ping(socket_path: &std::path::Path) -> bool {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    let probe = || -> std::io::Result<bool> {
        let mut stream = UnixStream::connect(socket_path)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
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
    };
    probe().unwrap_or(false)
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
