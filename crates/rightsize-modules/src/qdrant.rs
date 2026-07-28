//! A single-node Qdrant vector database container, queried over its REST interface
//! (port 6333). The gRPC port (6334) is exposed too, but this module's helpers don't
//! wrap it — HTTP-first, matching the house convention for HTTP-first modules (see
//! [`crate::clickhouse::ClickHouseContainer`]).
//!
//! ### Compatibility checking
//!
//! [`QdrantContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `qdrant/qdrant`
//! (registry host, tag, and digest stripped) before ever touching a backend,
//! returning [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather
//! than letting an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("qdrant/qdrant")` to
//! override for a verified drop-in replacement. [`QdrantContainer::new`] goes
//! through the same check against its own floating reference, so it can never fail
//! in practice.
//!
//! ### `new()` floats to `qdrant/qdrant:latest`
//!
//! Unlike [`crate::elasticsearch::ElasticsearchContainer`] (which has no floating
//! tag to default to at all), Qdrant publishes one — this module's `new()` tracks
//! upstream through it rather than a version pinned to this crate's own release
//! cycle. The functional facts documented below were captured against a real
//! `qdrant/qdrant:v1.18.3` (arm64; note the image's tags carry a `v` prefix, unlike
//! `new()`'s own `:latest`) boot.
//!
//! ### Readiness — HTTP 200 on `/readyz`, answered on the first poll
//!
//! Verified directly: `GET /readyz` returned 200 on the very first poll after boot
//! — no restart/double-boot race to account for here, so this module keeps the
//! default 120s startup timeout rather than widening it.
//!
//! ### No memory limit — verified directly
//!
//! This module sets no memory limit. Verified against a real boot with the limit
//! removed entirely: the full create-collection/upsert/search round trip below
//! completed in a guest reporting ~480 MB total.
//!
//! Verified end to end: created a collection (vector size 4, `Dot` distance),
//! upserted a point, and a search against it returned a score.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

const REST_PORT: u16 = 6333;
const GRPC_PORT: u16 = 6334;

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "qdrant/qdrant";

/// A single-node Qdrant container.
pub struct QdrantContainer {
    container: Container,
    image: ImageName,
}

impl QdrantContainer {
    /// Builds a container from the floating default image (`qdrant/qdrant:latest`)
    /// — see the module doc for why, unlike
    /// [`crate::elasticsearch::ElasticsearchContainer`], Qdrant has one to float to.
    pub fn new() -> Self {
        Self::with_image("qdrant/qdrant:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`QdrantContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        Self {
            container: Container::new(image.as_str())
                .with_exposed_ports(&[REST_PORT, GRPC_PORT])
                // No memory-limit override: verified serving a full round trip in a
                // guest reporting ~480 MB total — see the module doc.
                .waiting_for(Wait::for_http("/readyz").for_port(REST_PORT)),
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
    pub async fn start(self) -> Result<QdrantGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(QdrantGuard(self.container.start().await?))
    }
}

impl Default for QdrantContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`QdrantContainer`].
pub struct QdrantGuard(ContainerGuard);

impl QdrantGuard {
    /// The REST interface's base URI (port 6333).
    pub fn rest_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.0.host(),
            self.0.get_mapped_port(REST_PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host ports.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for QdrantGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_builds_from_the_floating_default() {
        let _ = QdrantContainer::new();
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        QdrantContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn a_compatible_repository_passes() {
        QdrantContainer::with_image("qdrant/qdrant:v1.18.3")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("qdrant/qdrant:v1.18.3 is compatible with 'qdrant/qdrant'");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = QdrantContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not qdrant/qdrant");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("qdrant/qdrant"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/qdrant-hardened:v1.18.3")
            .as_compatible_substitute_for("qdrant/qdrant");
        QdrantContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
