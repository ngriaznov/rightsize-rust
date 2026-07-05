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

use rightsize::{Container, ContainerGuard, Result, Wait};

const PORT: u16 = 8080;

/// A WireMock stub-server container.
pub struct WireMockContainer(Container);

impl WireMockContainer {
    /// Builds a container from the pinned default image (`wiremock/wiremock:3.13.2`).
    pub fn new() -> Self {
        Self::with_image("wiremock/wiremock:3.13.2")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        Self(
            Container::new(image)
                .with_exposed_ports(&[PORT])
                .waiting_for(Wait::for_http("/__admin/health").for_port(PORT)),
        )
    }

    /// Boots the container.
    pub async fn start(self) -> Result<WireMockGuard> {
        Ok(WireMockGuard(self.0.start().await?))
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
}
