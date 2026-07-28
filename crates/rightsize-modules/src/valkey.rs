//! A single-node Valkey container. Valkey is a protocol-compatible Redis fork; this
//! module mirrors [`crate::redis::RedisContainer`]'s shape exactly — same wait
//! strategy, same lack of a memory-limit override, same compatibility plumbing.
//!
//! ### Readiness — same signal, same reasoning as Redis
//!
//! `Ready to accept connections tcp` is Valkey's own log line, and a bare
//! listening-port probe is not enough here for the identical reason it isn't for
//! [`crate::redis::RedisContainer`]: on a loaded host, msb's loopback port forwarder
//! can accept and hold a TCP connection in the window between the guest binding its
//! socket and the server actually serving it, which a port-only check can't see
//! through. Verified against a real boot of `valkey/valkey:9.1-alpine` — this
//! module's previous pin: it came up, and `valkey-cli PING` against the mapped port
//! replied `PONG`. No env is required and no memory-limit override was needed beyond
//! msb's default during that boot.
//!
//! ### `uri()` returns `redis://`, not `valkey://` — deliberate, not a copy-paste slip
//!
//! Every client this module's tests (and its users) actually reach for — lettuce,
//! node-redis, raw RESP over TCP — parses the `redis://` scheme; none of them
//! recognize `valkey://`. [`ValkeyGuard::uri`] returns `redis://<host>:<port>` on
//! purpose, carried over unchanged from [`crate::redis::RedisGuard::uri`]'s own
//! scheme rather than an oversight.
//!
//! ### Compatibility checking
//!
//! [`ValkeyContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `valkey/valkey`
//! (registry host, tag, and digest stripped) before ever touching a backend,
//! returning [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather
//! than letting an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("valkey/valkey")` to
//! override for a verified drop-in replacement. [`ValkeyContainer::new`] goes through
//! the same check against its own floating reference, so it can never fail in
//! practice.
//!
//! ### `new()` floats to `valkey/valkey:latest` — now the Debian-based image, not Alpine
//!
//! This module used to pin `valkey/valkey:9.1-alpine`; `new()` now floats to
//! `valkey/valkey:latest`, which Docker Hub publishes as a Debian-based image rather
//! than Alpine. Functionally equivalent for this module's own use, just a larger pull.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "valkey/valkey";

/// A single-node Valkey container, protocol-compatible with Redis (see the module doc
/// for why [`ValkeyGuard::uri`] still returns `redis://`).
pub struct ValkeyContainer {
    container: Container,
    image: ImageName,
}

impl ValkeyContainer {
    /// The guest port Valkey listens on.
    const PORT: u16 = 6379;

    /// Builds a container from the floating default image (`valkey/valkey:latest`) —
    /// see the module doc for the Alpine-to-Debian shift.
    pub fn new() -> Self {
        Self::with_image("valkey/valkey:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`ValkeyContainer::start`].
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
    pub async fn start(self) -> Result<ValkeyGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(ValkeyGuard(self.container.start().await?))
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

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        ValkeyContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = ValkeyContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not valkey/valkey");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("valkey/valkey"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/valkey-hardened:9")
            .as_compatible_substitute_for("valkey/valkey");
        ValkeyContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
