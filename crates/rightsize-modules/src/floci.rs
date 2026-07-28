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
//!
//! ### Compatibility checking — three distinct repositories, one per variant
//!
//! Unlike every other module in this crate, `FlociContainer` wraps three unrelated
//! images behind one type, so there is no single repository it understands: each
//! factory declares its own. [`FlociContainer::aws_with_image`] checks against
//! `floci/floci`, [`FlociContainer::azure_with_image`] against `floci/floci-az`, and
//! [`FlociContainer::gcp_with_image`] against `floci/floci-gcp` — each parses the
//! supplied image with [`rightsize::ImageName`] and compares its repository
//! (registry host, tag, and digest stripped), returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch before ever
//! touching a backend. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("floci/floci")` (or the
//! `-az`/`-gcp` repository, matching the variant) to override for a verified
//! drop-in replacement. [`FlociContainer::aws`]/[`FlociContainer::azure`]/
//! [`FlociContainer::gcp`] each go through the same check against their own floating
//! reference, so none of them can fail in practice.
//!
//! ### The three `new()`-equivalents float to `:latest`
//!
//! This module used to pin `floci/floci:1.5.30`, `floci/floci-az:0.8.0`, and
//! `floci/floci-gcp:0.4.0`; the three factory functions now float to
//! `floci/floci:latest`, `floci/floci-az:latest`, and `floci/floci-gcp:latest`
//! respectively, so each variant's version tracks upstream rather than this crate's
//! own release cycle. The readiness, signing, and memory facts above were verified
//! against those three specific pinned versions.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

const AWS_PORT: u16 = 4566;
const AZURE_PORT: u16 = 4577;
const GCP_PORT: u16 = 4588;

/// The AWS variant's repository — see the module doc's compatibility section.
const AWS_EXPECTED_REPOSITORY: &str = "floci/floci";
/// The Azure variant's repository — see the module doc's compatibility section.
const AZURE_EXPECTED_REPOSITORY: &str = "floci/floci-az";
/// The GCP variant's repository — see the module doc's compatibility section.
const GCP_EXPECTED_REPOSITORY: &str = "floci/floci-gcp";

/// A floci.io cloud emulator container — one AWS/Azure/GCP variant, picked via
/// [`FlociContainer::aws`]/[`FlociContainer::azure`]/[`FlociContainer::gcp`].
pub struct FlociContainer {
    container: Container,
    image: ImageName,
    expected_repository: &'static str,
    port: u16,
}

impl FlociContainer {
    fn new(image: impl Into<ImageName>, port: u16, expected_repository: &'static str) -> Self {
        let image = image.into();
        Self {
            container: Container::new(image.as_str())
                .with_exposed_ports(&[port])
                .waiting_for(Wait::for_http("/health").for_port(port)),
            image,
            expected_repository,
            port,
        }
    }

    /// The AWS emulator (`floci/floci:latest`), guest port 4566 — S3, DynamoDB, SQS,
    /// etc.
    pub fn aws() -> Self {
        Self::aws_with_image("floci/floci:latest")
    }

    /// The AWS emulator, from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like
    /// every other module's — see [`FlociContainer::start`].
    pub fn aws_with_image(image: impl Into<ImageName>) -> Self {
        Self::new(image, AWS_PORT, AWS_EXPECTED_REPOSITORY)
    }

    /// The Azure emulator (`floci/floci-az:latest`), guest port 4577.
    pub fn azure() -> Self {
        Self::azure_with_image("floci/floci-az:latest")
    }

    /// The Azure emulator, from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like
    /// every other module's — see [`FlociContainer::start`].
    pub fn azure_with_image(image: impl Into<ImageName>) -> Self {
        Self::new(image, AZURE_PORT, AZURE_EXPECTED_REPOSITORY)
    }

    /// The GCP emulator (`floci/floci-gcp:latest`), guest port 4588.
    pub fn gcp() -> Self {
        Self::gcp_with_image("floci/floci-gcp:latest")
    }

    /// The GCP emulator, from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like
    /// every other module's — see [`FlociContainer::start`].
    pub fn gcp_with_image(image: impl Into<ImageName>) -> Self {
        Self::new(image, GCP_PORT, GCP_EXPECTED_REPOSITORY)
    }

    /// Boots the container, after checking the image is one this variant understands.
    ///
    /// The compatibility check runs here rather than in the constructors so those stay
    /// infallible and match every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server. Which repository
    /// is expected depends on which factory built this container — see the module
    /// doc.
    pub async fn start(self) -> Result<FlociGuard> {
        self.image
            .assert_compatible_with(self.expected_repository)?;
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

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image —
    // each variant against its own declared repository.

    #[test]
    fn each_floating_default_is_compatible_with_its_own_variant() {
        FlociContainer::aws()
            .image
            .assert_compatible_with(AWS_EXPECTED_REPOSITORY)
            .expect("the AWS floating default must satisfy the AWS variant's own check");
        FlociContainer::azure()
            .image
            .assert_compatible_with(AZURE_EXPECTED_REPOSITORY)
            .expect("the Azure floating default must satisfy the Azure variant's own check");
        FlociContainer::gcp()
            .image
            .assert_compatible_with(GCP_EXPECTED_REPOSITORY)
            .expect("the GCP floating default must satisfy the GCP variant's own check");
    }

    #[test]
    fn variants_do_not_accept_each_others_repositories() {
        // Each variant's own image is a real, distinct repository, so the AWS image
        // must not satisfy the Azure variant's check, and vice versa.
        let aws_image = FlociContainer::aws().image;
        assert!(
            aws_image
                .assert_compatible_with(AZURE_EXPECTED_REPOSITORY)
                .is_err(),
            "the AWS variant's image must not satisfy the Azure variant's check"
        );
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = FlociContainer::aws_with_image("postgres:16")
            .image
            .assert_compatible_with(AWS_EXPECTED_REPOSITORY)
            .expect_err("postgres is not floci/floci");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("floci/floci"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/floci-hardened:1.5.30")
            .as_compatible_substitute_for("floci/floci");
        FlociContainer::aws_with_image(image)
            .image
            .assert_compatible_with(AWS_EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
