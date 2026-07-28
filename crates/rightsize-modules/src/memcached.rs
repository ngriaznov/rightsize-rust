//! A single-node Memcached container, ready-checked with a protocol-level `version`
//! probe instead of the bare listening-port wait.
//!
//! ### Compatibility checking
//!
//! [`MemcachedContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `memcached` (registry
//! host, tag, and digest stripped) before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("memcached")` to override for
//! a verified drop-in replacement. [`MemcachedContainer::new`] goes through the same
//! check against its own floating reference, so it can never fail in practice.
//!
//! ### `new()` floats to `memcached:latest` — now the Debian-based image, not Alpine
//!
//! This module used to pin `memcached:1.6-alpine`; `new()` now floats to
//! `memcached:latest`, which Docker Hub publishes as a Debian-based image rather than
//! Alpine. Functionally equivalent for this module's own use, just a larger pull.

use std::time::Duration;

use rightsize::wait::{WaitStrategy, WaitTarget};
use rightsize::{Container, ContainerGuard, ImageName, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "memcached";

/// A single-node Memcached container.
pub struct MemcachedContainer {
    container: Container,
    image: ImageName,
}

impl MemcachedContainer {
    /// The guest port memcached listens on.
    const PORT: u16 = 11211;

    /// Builds a container from the floating default image (`memcached:latest`) — see
    /// the module doc for the Alpine-to-Debian shift.
    pub fn new() -> Self {
        Self::with_image("memcached:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`MemcachedContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        Self {
            container: Container::new(image.as_str())
                .with_exposed_ports(&[Self::PORT])
                .waiting_for(MemcachedResponds),
            image,
        }
    }

    /// Boots the container, after checking the image is one this module understands.
    ///
    /// The compatibility check runs here rather than in the constructors so those stay
    /// infallible and match every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server.
    pub async fn start(self) -> Result<MemcachedGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(MemcachedGuard(self.container.start().await?))
    }
}

impl Default for MemcachedContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`MemcachedContainer`].
pub struct MemcachedGuard(ContainerGuard);

impl MemcachedGuard {
    /// The `host:port` address of the running container.
    pub fn address(&self) -> String {
        format!(
            "{}:{}",
            self.0.host(),
            self.0.get_mapped_port(MemcachedContainer::PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for MemcachedGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.0
    }
}

/// Memcached logs nothing on startup and the docker userland proxy (or msb's loopback
/// forwarder) binds the host port before the server inside is accepting, so a bare
/// TCP-connect wait can pass while the first real client connection still gets a dead
/// stream. This strategy proves readiness by speaking the protocol: it sends
/// `version\r\n` and expects a reply starting with `VERSION`.
struct MemcachedResponds;

impl MemcachedResponds {
    async fn probe_once(host: &str, port: u16) -> bool {
        let Ok(connect) = tokio::time::timeout(
            Duration::from_millis(1000),
            TcpStream::connect((host, port)),
        )
        .await
        else {
            return false;
        };
        let Ok(mut stream) = connect else {
            return false;
        };
        if tokio::time::timeout(
            Duration::from_millis(1000),
            stream.write_all(b"version\r\n"),
        )
        .await
        .is_err()
        {
            return false;
        }
        let mut buf = [0u8; 64];
        let Ok(Ok(n)) =
            tokio::time::timeout(Duration::from_millis(1000), stream.read(&mut buf)).await
        else {
            return false;
        };
        if n == 0 {
            return false;
        }
        String::from_utf8_lossy(&buf[..n]).starts_with("VERSION")
    }
}

#[async_trait::async_trait]
impl WaitStrategy for MemcachedResponds {
    async fn wait_until_ready(&self, target: &dyn WaitTarget) -> Result<()> {
        let guest_port = target
            .exposed_guest_ports()
            .first()
            .copied()
            .unwrap_or(MemcachedContainer::PORT);
        let port = target.mapped_port(guest_port);
        let host = target.host().to_string();
        rightsize::wait::poll_until_ready(
            target,
            Duration::from_secs(60),
            "a VERSION reply",
            || {
                let host = host.clone();
                async move { Self::probe_once(&host, port).await }
            },
        )
        .await
    }

    fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
        // This probe doesn't expose a timeout override; keep the fixed poll budget.
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn with_image_smoke() {
        let _ = MemcachedContainer::new();
        let _ = MemcachedContainer::with_image("memcached:1.6-alpine");
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        MemcachedContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = MemcachedContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not memcached");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("memcached"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/memcached-hardened:1.6")
            .as_compatible_substitute_for("memcached");
        MemcachedContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }

    /// A fake `VERSION`-replying socket — proves the wait strategy recognizes a real
    /// memcached protocol reply and (via `probe_once`) rejects a peer that isn't
    /// speaking memcached at all.
    #[tokio::test]
    async fn probe_once_true_on_a_version_reply_false_otherwise() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = ready.clone();
        std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            loop {
                if let Ok((mut stream, _)) = listener.accept() {
                    use std::io::{Read, Write};
                    let mut buf = [0u8; 64];
                    let _ = stream.read(&mut buf);
                    if ready_clone.load(Ordering::SeqCst) {
                        let _ = stream.write_all(b"VERSION 1.6.31\r\n");
                    } else {
                        let _ = stream.write_all(b"ERROR\r\n");
                    }
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });

        assert!(!MemcachedResponds::probe_once("127.0.0.1", port).await);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        ready.store(true, Ordering::SeqCst);
        std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            loop {
                if let Ok((mut stream, _)) = listener.accept() {
                    use std::io::{Read, Write};
                    let mut buf = [0u8; 64];
                    let _ = stream.read(&mut buf);
                    let _ = stream.write_all(b"VERSION 1.6.31\r\n");
                    return;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        assert!(MemcachedResponds::probe_once("127.0.0.1", port).await);
    }
}
