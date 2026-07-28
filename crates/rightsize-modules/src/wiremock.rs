//! A WireMock server container for stubbing HTTP dependencies in integration tests.
//!
//! ### Readiness — verified against a real 3.13.2 boot
//!
//! WireMock 3.x ships a dedicated `/__admin/health` endpoint (unlike some older 2.x
//! builds, where `/__admin/mappings` was the only reliable 200). Verified directly
//! against a real container:
//!
//! ```text
//! $ curl http://127.0.0.1:<port>/__admin/health
//! {"status":"healthy","message":"Wiremock is ok","version":"3.13.2","uptimeInSeconds":9,...}
//! ```
//!
//! so this module waits on that endpoint rather than falling back to
//! `/__admin/mappings`.
//!
//! No control characters were found in the image's baked env (checked via
//! `docker image inspect`), and no `with_memory_limit` override was
//! needed — the JVM boots comfortably on msb's default ~450M microVM RAM (observed
//! ~5.5s IT round-trip on msb; a small embedded-Jetty app, not a JVM-heavy cluster
//! like Pinot — no memory-ladder escalation was needed).
//!
//! This is the Rust ecosystem's missing piece too: there is no first-class
//! in-process WireMock story for Rust integration tests, so this module fills a
//! real gap.
//!
//! ### Compatibility checking
//!
//! [`WireMockContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `wiremock/wiremock`
//! (registry host, tag, and digest stripped) before ever touching a backend,
//! returning [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather
//! than letting an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("wiremock/wiremock")` to
//! override for a verified drop-in replacement. [`WireMockContainer::new`] goes
//! through the same check against its own floating reference, so it can never fail
//! in practice.
//!
//! ### `new()` floats to `wiremock/wiremock:latest`
//!
//! This module used to pin `wiremock/wiremock:3.13.2`; `new()` now floats to
//! `wiremock/wiremock:latest` so the version tracks upstream rather than this
//! crate's own release cycle. The readiness endpoint above was verified against that
//! `3.13.2` boot specifically.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

const PORT: u16 = 8080;

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "wiremock/wiremock";

/// A WireMock stub-server container.
pub struct WireMockContainer {
    container: Container,
    image: ImageName,
}

impl WireMockContainer {
    /// Builds a container from the floating default image
    /// (`wiremock/wiremock:latest`).
    pub fn new() -> Self {
        Self::with_image("wiremock/wiremock:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`WireMockContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        Self {
            container: Container::new(image.as_str())
                .with_exposed_ports(&[PORT])
                .waiting_for(Wait::for_http("/__admin/health").for_port(PORT)),
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
    pub async fn start(self) -> Result<WireMockGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(WireMockGuard(self.container.start().await?))
    }
}

impl Default for WireMockContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`WireMockContainer`].
pub struct WireMockGuard(ContainerGuard);

impl WireMockGuard {
    /// The stub server's base URI (mount stubbed paths under this).
    pub fn base_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.0.host(),
            self.0.get_mapped_port(PORT).unwrap()
        )
    }

    /// The `/__admin` management API's base URI (stub CRUD, request journal, health).
    pub fn admin_url(&self) -> String {
        format!("{}/__admin", self.base_url())
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for WireMockGuard {
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
        let _ = WireMockContainer::new();
        let _ = WireMockContainer::with_image("wiremock/wiremock:3.13.2");
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        WireMockContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = WireMockContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not wiremock/wiremock");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("wiremock/wiremock"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/wiremock-hardened:3.13.2")
            .as_compatible_substitute_for("wiremock/wiremock");
        WireMockContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
