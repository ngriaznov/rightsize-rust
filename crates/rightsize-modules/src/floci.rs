//! A [floci.io](https://floci.io) cloud emulator — one native Quarkus image per cloud
//! provider, each speaking that provider's REST APIs against an in-memory backing
//! store. One module type covers all three variants; pick one with the
//! [`FlociContainer::aws`]/[`FlociContainer::azure`]/[`FlociContainer::gcp`] factory
//! functions rather than a bare constructor — each factory pins the provider's own
//! image and guest port.
//!
//! ### Readiness — `/health` works uniformly, unlike the AWS-flavored `/_localstack/health`
//!
//! The AWS variant ships a LocalStack-compatible `/_localstack/health` endpoint (spec
//! hypothesis confirmed), but the Azure and GCP variants do not carry that path (Azure:
//! `501`; GCP: `404`). All three, however, answer a plain **`GET /health`** with `200`
//! and a small JSON status body the moment the embedded Quarkus HTTP listener is up —
//! verified directly against real boots of `floci/floci:1.5.30`,
//! `floci/floci-az:0.8.0`, and `floci/floci-gcp:0.4.0`. `/health` is pinned as the one
//! wait path that works across all three variants; no log-wait fallback was needed.
//!
//! ### No signing needed — verified against the AWS variant's S3 surface
//!
//! The AWS variant's S3-shaped REST endpoints accept unsigned requests with no
//! `Authorization` header at all: `PUT /<bucket>`, `PUT /<bucket>/<key>`, and `GET
//! /<bucket>/<key>` all round-trip successfully with a bare HTTP client call — no
//! SigV4, no AWS SDK dependency required. This module's IT exercises that plain-REST
//! path rather than pulling in an AWS SDK.
//!
//! ### Memory — tiny, no ladder needed
//!
//! All three images are native (GraalVM) Quarkus binaries; a real boot settles at
//! roughly 11-27 MiB RSS (`docker stats`), and each variant boots and answers
//! `/health` under msb's default microVM RAM with no `with_memory_limit` override.
//!
//! No control characters were found in any of the three images' baked env (checked
//! via `docker image inspect`).

use rightsize::{Container, ContainerGuard, Result, Wait};

const AWS_PORT: u16 = 4566;
const AZURE_PORT: u16 = 4577;
const GCP_PORT: u16 = 4588;

/// A floci.io cloud emulator container — one AWS/Azure/GCP variant, picked via
/// [`FlociContainer::aws`]/[`FlociContainer::azure`]/[`FlociContainer::gcp`].
pub struct FlociContainer {
    container: Container,
    port: u16,
}

impl FlociContainer {
    fn new(image: &str, port: u16) -> Self {
        Self {
            container: Container::new(image)
                .with_exposed_ports(&[port])
                .waiting_for(Wait::for_http("/health").for_port(port)),
            port,
        }
    }

    /// The AWS emulator (`floci/floci:1.5.30`), guest port 4566 — S3, DynamoDB, SQS,
    /// etc.
    pub fn aws() -> Self {
        Self::aws_with_image("floci/floci:1.5.30")
    }

    /// The AWS emulator, from a caller-chosen image.
    pub fn aws_with_image(image: &str) -> Self {
        Self::new(image, AWS_PORT)
    }

    /// The Azure emulator (`floci/floci-az:0.8.0`), guest port 4577.
    pub fn azure() -> Self {
        Self::azure_with_image("floci/floci-az:0.8.0")
    }

    /// The Azure emulator, from a caller-chosen image.
    pub fn azure_with_image(image: &str) -> Self {
        Self::new(image, AZURE_PORT)
    }

    /// The GCP emulator (`floci/floci-gcp:0.4.0`), guest port 4588.
    pub fn gcp() -> Self {
        Self::gcp_with_image("floci/floci-gcp:0.4.0")
    }

    /// The GCP emulator, from a caller-chosen image.
    pub fn gcp_with_image(image: &str) -> Self {
        Self::new(image, GCP_PORT)
    }

    /// Boots the container.
    pub async fn start(self) -> Result<FlociGuard> {
        crate::register_default_backends();
        Ok(FlociGuard {
            guard: self.container.start().await?,
            port: self.port,
        })
    }
}

/// The running guard for a [`FlociContainer`].
pub struct FlociGuard {
    guard: ContainerGuard,
    port: u16,
}

impl FlociGuard {
    /// This variant's REST endpoint (`http://<host>:<mapped port>`), the base URI for
    /// every emulated API call.
    pub fn endpoint_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.guard.host(),
            self.guard.get_mapped_port(self.port).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.guard.stop().await
    }
}

impl std::ops::Deref for FlociGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aws_picks_port_4566() {
        let c = FlociContainer::aws();
        assert_eq!(c.port, AWS_PORT);
    }

    #[test]
    fn azure_picks_port_4577() {
        let c = FlociContainer::azure();
        assert_eq!(c.port, AZURE_PORT);
    }

    #[test]
    fn gcp_picks_port_4588() {
        let c = FlociContainer::gcp();
        assert_eq!(c.port, GCP_PORT);
    }
}
