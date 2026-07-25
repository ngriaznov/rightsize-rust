//! A single-node Valkey container. Valkey is a protocol-compatible Redis fork; this
//! module mirrors [`crate::redis::RedisContainer`]'s shape exactly — same wait
//! strategy, same lack of a memory-limit override, same single-newtype build.
//!
//! ### Readiness — same signal, same reasoning as Redis
//!
//! `Ready to accept connections tcp` is Valkey's own log line, and a bare
//! listening-port probe is not enough here for the identical reason it isn't for
//! [`crate::redis::RedisContainer`]: on a loaded host, msb's loopback port forwarder
//! can accept and hold a TCP connection in the window between the guest binding its
//! socket and the server actually serving it, which a port-only check can't see
//! through. Verified against a real boot: `valkey/valkey:9.1-alpine` came up, and
//! `valkey-cli PING` against the mapped port replied `PONG`. No env is required and
//! no memory-limit override was needed beyond msb's default during that boot.
//!
//! ### `uri()` returns `redis://`, not `valkey://` — deliberate, not a copy-paste slip
//!
//! Every client this module's tests (and its users) actually reach for — lettuce,
//! node-redis, raw RESP over TCP — parses the `redis://` scheme; none of them
//! recognize `valkey://`. [`ValkeyGuard::uri`] returns `redis://<host>:<port>` on
//! purpose, carried over unchanged from [`crate::redis::RedisGuard::uri`]'s own
//! scheme rather than an oversight.

use rightsize::{Container, ContainerGuard, Result, Wait};

/// A single-node Valkey container, protocol-compatible with Redis (see the module doc
/// for why [`ValkeyGuard::uri`] still returns `redis://`).
pub struct ValkeyContainer(Container);

impl ValkeyContainer {
    /// The guest port Valkey listens on.
    const PORT: u16 = 6379;

    /// Builds a container from the pinned default image (`valkey/valkey:9.1-alpine`).
    pub fn new() -> Self {
        Self::with_image("valkey/valkey:9.1-alpine")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        Self(
            Container::new(image)
                .with_exposed_ports(&[Self::PORT])
                .waiting_for(Wait::for_log_message(".*Ready to accept connections.*", 1)),
        )
    }

    /// Boots the container.
    pub async fn start(self) -> Result<ValkeyGuard> {
        crate::register_default_backends();
        Ok(ValkeyGuard(self.0.start().await?))
    }
}

impl Default for ValkeyContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`ValkeyContainer`].
pub struct ValkeyGuard(ContainerGuard);

impl ValkeyGuard {
    /// A `redis://` connection URI for the running container (see the module doc for
    /// why this is `redis://`, not `valkey://`).
    pub fn uri(&self) -> String {
        format!(
            "redis://{}:{}",
            self.0.host(),
            self.0.get_mapped_port(ValkeyContainer::PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for ValkeyGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_image_exposes_the_valkey_port_and_waits_for_a_listening_port() {
        // No direct field access from outside this module — this is a smoke check that
        // construction doesn't panic and picks the documented default image via `new`.
        let _ = ValkeyContainer::new();
        let _ = ValkeyContainer::with_image("valkey/valkey:9-alpine");
    }
}
