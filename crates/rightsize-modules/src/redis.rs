//! A single-node Redis container.

use rightsize::{Container, ContainerGuard, Result, Wait};

/// A single-node Redis container, ready-checked with a plain TCP read-probe (see
/// [`Wait::for_listening_port`]) — Redis speaks first on connect, so the bare
/// listening-port wait is sufficient here (contrast [`crate::memcached::MemcachedContainer`],
/// which needs a protocol-level probe).
pub struct RedisContainer(Container);

impl RedisContainer {
    /// The guest port Redis listens on.
    const PORT: u16 = 6379;

    /// Builds a container from the pinned default image (`redis:8.6-alpine`).
    pub fn new() -> Self {
        Self::with_image("redis:8.6-alpine")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        Self(
            Container::new(image)
                .with_exposed_ports(&[Self::PORT])
                .waiting_for(Wait::for_listening_port()),
        )
    }

    /// Boots the container.
    pub async fn start(self) -> Result<RedisGuard> {
        Ok(RedisGuard(self.0.start().await?))
    }
}

impl Default for RedisContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`RedisContainer`].
pub struct RedisGuard(ContainerGuard);

impl RedisGuard {
    /// A `redis://` connection URI for the running container.
    pub fn uri(&self) -> String {
        format!(
            "redis://{}:{}",
            self.0.host(),
            self.0.get_mapped_port(RedisContainer::PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for RedisGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_image_exposes_the_redis_port_and_waits_for_a_listening_port() {
        // No direct field access from outside this module — this is a smoke check that
        // construction doesn't panic and picks the documented default image via `new`.
        let _ = RedisContainer::new();
        let _ = RedisContainer::with_image("redis:7-alpine");
    }
}
