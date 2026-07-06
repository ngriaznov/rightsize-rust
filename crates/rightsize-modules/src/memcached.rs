//! A single-node Memcached container, ready-checked with a protocol-level `version`
//! probe instead of the bare listening-port wait.

use std::time::Duration;

use rightsize::wait::{WaitStrategy, WaitTarget};
use rightsize::{Container, ContainerGuard, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A single-node Memcached container.
pub struct MemcachedContainer(Container);

impl MemcachedContainer {
    /// The guest port memcached listens on.
    const PORT: u16 = 11211;

    /// Builds a container from the pinned default image (`memcached:1.6-alpine`).
    pub fn new() -> Self {
        Self::with_image("memcached:1.6-alpine")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        Self(
            Container::new(image)
                .with_exposed_ports(&[Self::PORT])
                .waiting_for(MemcachedResponds),
        )
    }

    /// Boots the container.
    pub async fn start(self) -> Result<MemcachedGuard> {
        crate::register_default_backends();
        Ok(MemcachedGuard(self.0.start().await?))
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
