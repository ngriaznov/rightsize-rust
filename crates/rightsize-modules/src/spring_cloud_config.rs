//! A Spring Cloud Config Server container, ready-checked via its actuator health
//! endpoint.
//!
//! Callers wanting the classpath/filesystem-backed environment repository (no
//! external git remote required — the default, git-backed repository needs a
//! configured URI to boot at all) should chain
//! `.with_env("SPRING_PROFILES_ACTIVE", "native")` before `start()`. That decision
//! belongs to the caller/test, not this module: the module itself does not set this
//! profile.

use rightsize::{Container, ContainerGuard, Result, Wait};

/// A Spring Cloud Config Server container.
pub struct SpringCloudConfigContainer(Container);

impl SpringCloudConfigContainer {
    const PORT: u16 = 8888;

    /// Builds a container from the pinned default image
    /// (`hyness/spring-cloud-config-server:latest`).
    pub fn new() -> Self {
        Self::with_image("hyness/spring-cloud-config-server:latest")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        Self(
            Container::new(image)
                .with_exposed_ports(&[Self::PORT])
                .waiting_for(Wait::for_http("/actuator/health").for_port(Self::PORT))
                // Paketo's memory calculator sizes this JVM image's fixed regions
                // (~688M) above microsandbox's default microVM RAM (~450M);
                // this is the reason with_memory_limit exists on the module.
                .with_memory_limit(1024),
        )
    }

    /// Sets a single environment variable for the container process — a thin
    /// passthrough to [`Container::with_env`], since this newtype wraps `Container`
    /// rather than exposing it directly, so this passthrough is how a caller reaches
    /// the same builder surface. Callers needing the classpath-backed `native`
    /// profile call this before `start()` — see the module doc.
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.0 = self.0.with_env(key, value);
        self
    }

    /// Boots the container.
    pub async fn start(self) -> Result<SpringCloudConfigGuard> {
        crate::register_default_backends();
        Ok(SpringCloudConfigGuard(self.0.start().await?))
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
}
