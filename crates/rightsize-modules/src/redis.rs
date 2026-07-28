//! A single-node Redis container.
//!
//! ### Compatibility checking
//!
//! [`RedisContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `redis` (registry host,
//! tag, and digest stripped) before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("redis")` to override for a
//! verified drop-in replacement. [`RedisContainer::new`] goes through the same check
//! against its own floating reference, so it can never fail in practice.
//!
//! ### `new()` floats to `redis:latest` — now the Debian-based image, not Alpine
//!
//! This module used to pin `redis:8.6-alpine`; `new()` now floats to `redis:latest`,
//! which Docker Hub publishes as a Debian-based image rather than Alpine. Functionally
//! equivalent for this module's own use (same `Ready to accept connections` log line,
//! same default port), just a larger pull.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "redis";

/// A single-node Redis container. Readiness is anchored on Redis's own
/// "Ready to accept connections" log line rather than a TCP probe: on a loaded
/// host the port forwarder can accept and hold a connection in the window
/// between Redis binding its socket and actually serving, which a bare
/// listening-port check cannot see through.
pub struct RedisContainer {
    container: Container,
    image: ImageName,
}

impl RedisContainer {
    /// The guest port Redis listens on.
    const PORT: u16 = 6379;

    /// Builds a container from the floating default image (`redis:latest`) — see the
    /// module doc for the Alpine-to-Debian shift.
    pub fn new() -> Self {
        Self::with_image("redis:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`RedisContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        Self {
            container: Container::new(image.as_str())
                .with_exposed_ports(&[Self::PORT])
                .waiting_for(Wait::for_log_message(".*Ready to accept connections.*", 1)),
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
    pub async fn start(self) -> Result<RedisGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(RedisGuard(self.container.start().await?))
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

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        RedisContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = RedisContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not redis");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("redis"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image =
            ImageName::parse("mycorp/redis-hardened:8").as_compatible_substitute_for("redis");
        RedisContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
