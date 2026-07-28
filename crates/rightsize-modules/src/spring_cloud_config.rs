//! A Spring Cloud Config Server container, ready-checked via its actuator health
//! endpoint.
//!
//! Callers wanting the classpath/filesystem-backed environment repository (no
//! external git remote required — the default, git-backed repository needs a
//! configured URI to boot at all) should chain
//! `.with_env("SPRING_PROFILES_ACTIVE", "native")` before `start()`. That decision
//! belongs to the caller/test, not this module: the module itself does not set this
//! profile.
//!
//! ### Compatibility checking
//!
//! [`SpringCloudConfigContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against
//! `hyness/spring-cloud-config-server` (registry host, tag, and digest stripped)
//! before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("hyness/spring-cloud-config-server")`
//! to override for a verified drop-in replacement. [`SpringCloudConfigContainer::new`]
//! goes through the same check against its own floating reference, so it can never
//! fail in practice.
//!
//! `new()` already floated to `hyness/spring-cloud-config-server:latest` before this
//! crate's compatibility checking existed, so its default image is unchanged here —
//! only the compatibility plumbing is new.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "hyness/spring-cloud-config-server";

/// A Spring Cloud Config Server container.
pub struct SpringCloudConfigContainer {
    container: Container,
    image: ImageName,
}

impl SpringCloudConfigContainer {
    const PORT: u16 = 8888;

    /// Builds a container from the floating default image
    /// (`hyness/spring-cloud-config-server:latest`).
    pub fn new() -> Self {
        Self::with_image("hyness/spring-cloud-config-server:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`SpringCloudConfigContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        Self {
            container: Container::new(image.as_str())
                .with_exposed_ports(&[Self::PORT])
                .waiting_for(Wait::for_http("/actuator/health").for_port(Self::PORT))
                // Paketo's memory calculator sizes this JVM image's fixed regions
                // (~688M) above microsandbox's default microVM RAM (~450M);
                // this is the reason with_memory_limit exists on the module.
                .with_memory_limit(1024),
            image,
        }
    }

    /// Sets a single environment variable for the container process — a thin
    /// passthrough to [`Container::with_env`], since this newtype wraps `Container`
    /// rather than exposing it directly, so this passthrough is how a caller reaches
    /// the same builder surface. Callers needing the classpath-backed `native`
    /// profile call this before `start()` — see the module doc.
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.container = self.container.with_env(key, value);
        self
    }

    /// Boots the container, after checking the image is one this module understands.
    ///
    /// The compatibility check runs here rather than in the constructors so those stay
    /// infallible and match every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server.
    pub async fn start(self) -> Result<SpringCloudConfigGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(SpringCloudConfigGuard(self.container.start().await?))
    }
}

impl Default for SpringCloudConfigContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`SpringCloudConfigContainer`].
pub struct SpringCloudConfigGuard(ContainerGuard);

impl SpringCloudConfigGuard {
    /// The config server's base URI for the running container.
    pub fn uri(&self) -> String {
        format!(
            "http://{}:{}",
            self.0.host(),
            self.0
                .get_mapped_port(SpringCloudConfigContainer::PORT)
                .unwrap()
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for SpringCloudConfigGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_image_smoke() {
        let _ = SpringCloudConfigContainer::new();
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        SpringCloudConfigContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = SpringCloudConfigContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not hyness/spring-cloud-config-server");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("hyness/spring-cloud-config-server"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/spring-cloud-config-server-hardened:latest")
            .as_compatible_substitute_for("hyness/spring-cloud-config-server");
        SpringCloudConfigContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
