//! A single-node Elasticsearch container, queried over its REST interface (port
//! 9200). The transport port (9300) is exposed too, but this module's helpers don't
//! wrap it — nothing outside the cluster talks the transport protocol directly.
//!
//! ### No no-arg constructor — Elastic publishes no floating tag
//!
//! Every other module here that has a `new()` boots some floating reference that
//! tracks upstream rather than a version this crate pins (most modules:
//! `<repository>:latest`; [`crate::rabbitmq::RabbitMqContainer`]:
//! `rabbitmq:management`, verified to exist because the module is built around the
//! management plugin). Elasticsearch has no such reference to pick on a caller's
//! behalf: `elasticsearch:latest`, `elasticsearch:9`, and `elasticsearch:8` are all
//! 404 on Docker Hub — verified directly. Elastic publishes only fully-qualified
//! version tags (402 of them, actively maintained, e.g. `9.4.4`, `8.19.19`), so any
//! version this module picked today would eventually 404 out from under a caller who
//! never asked for a moving target in the first place. [`ElasticsearchContainer::with_image`]
//! is therefore the only constructor — an explicit, caller-chosen version tag is
//! required, and there is no `ElasticsearchContainer::new()`.
//!
//! ### Compatibility checking
//!
//! `with_image` parses the supplied image with [`rightsize::ImageName`] and checks
//! its repository against `elasticsearch` (registry host, tag, and digest stripped)
//! before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than
//! letting an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("elasticsearch")` to
//! override for a verified drop-in replacement.
//!
//! ### Env — verified against a real `elasticsearch:9.4.4` (arm64) boot
//!
//! `discovery.type=single-node` skips the cluster-formation bootstrap a real
//! multi-node deployment needs. `xpack.security.enabled=false` drops TLS/auth setup
//! that a smoke-test container has no use for. `ES_JAVA_OPTS=-Xms512m -Xmx512m`
//! fixes the JVM heap rather than letting it size itself off host memory — this
//! crate's usual lever for a containerized JVM server (see
//! [`crate::cassandra::CassandraContainer`]'s own heap env, `MAX_HEAP_SIZE`/
//! `HEAP_NEWSIZE`). No control characters were found in the image's baked env.
//!
//! ### Cluster health is `yellow` forever on a single node
//!
//! A single-node cluster has nowhere to place a replica shard, so `cluster_health`
//! never reaches `green` — it sits at `yellow` indefinitely. A readiness check
//! waiting for `green` would hang until its startup timeout on every single-node
//! boot, which is the only shape this module ever creates. This module's own
//! readiness wait (below) never checks cluster health at all, precisely to avoid
//! that trap; a caller who wants a health-based check of their own should poll
//! `GET /_cluster/health` for `yellow`, not `green`.
//!
//! ### Memory limit — 2560 MB, verified at that value
//!
//! `with_memory_limit(2560)` is this module's fixed limit, verified against a real
//! boot with the heap settings above: ~1.1 GB used of a 2.48 GB guest.
//!
//! ### Readiness — HTTP 200 on `/`, 27s observed; 300s budget for loaded CI runners
//!
//! `GET /` on the REST port returning 200 is the earliest signal that the HTTP
//! layer itself is serving — it does not imply `green`/`yellow` cluster health (see
//! above), only that the node answers requests at all. Observed at 27s on a real
//! boot; the startup timeout is 300s to give a loaded CI runner headroom beyond
//! that.
//!
//! Verified end to end: `PUT /books/_doc/1?refresh=true` then `GET /books/_search`
//! returned the indexed document.

use std::time::Duration;

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

const REST_PORT: u16 = 9200;
const TRANSPORT_PORT: u16 = 9300;

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "elasticsearch";

/// A single-node Elasticsearch container. See the module doc for why there is no
/// `new()` — [`Self::with_image`] always requires an explicit, caller-chosen
/// version tag.
pub struct ElasticsearchContainer {
    container: Container,
    image: ImageName,
}

impl ElasticsearchContainer {
    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`ElasticsearchContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        Self {
            container: (Container::new(image.as_str())
                .with_exposed_ports(&[REST_PORT, TRANSPORT_PORT])
                .with_env("discovery.type", "single-node")
                .with_env("xpack.security.enabled", "false")
                .with_env("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
                .with_memory_limit(2560)
                // GET / at 200 is the earliest "the HTTP layer is serving" signal —
                // deliberately not a cluster-health check, which would hang forever
                // on this module's single-node yellow state. See the module doc.
                .waiting_for(
                    Wait::for_http("/")
                        .for_port(REST_PORT)
                        .with_startup_timeout(Duration::from_secs(300)),
                )),
            image,
        }
    }

    /// Boots the container, after checking the image is one this module understands.
    ///
    /// The compatibility check runs here rather than in the constructor so it stays
    /// infallible and matches every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server.
    pub async fn start(self) -> Result<ElasticsearchGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(ElasticsearchGuard(self.container.start().await?))
    }
}

/// The running guard for an [`ElasticsearchContainer`].
pub struct ElasticsearchGuard(ContainerGuard);

impl ElasticsearchGuard {
    /// The REST interface's base URI (port 9200).
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

impl std::ops::Deref for ElasticsearchGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn a_compatible_repository_passes() {
        ElasticsearchContainer::with_image("elasticsearch:9.4.4")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("elasticsearch:9.4.4 is compatible with 'elasticsearch'");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = ElasticsearchContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not elasticsearch");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("elasticsearch"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/es-hardened:9.4.4")
            .as_compatible_substitute_for("elasticsearch");
        ElasticsearchContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
