//! A single-node Kafka broker (KRaft mode, no ZooKeeper).
//!
//! ### Compatibility checking
//!
//! [`KafkaContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `apache/kafka`
//! (registry host, tag, and digest stripped) before ever touching a backend,
//! returning [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather
//! than letting an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("apache/kafka")` to override
//! for a verified drop-in replacement. [`KafkaContainer::new`] goes through the same
//! check against its own floating reference, so it can never fail in practice.
//!
//! ### `new()` floats to `apache/kafka:latest`
//!
//! This module used to pin `apache/kafka:4.0.0`; `new()` now floats to
//! `apache/kafka:latest` so the version tracks upstream rather than this crate's own
//! release cycle. The `KAFKA_HEAP_OPTS` fix below was verified against that
//! `apache/kafka:4.0.0` boot specifically.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "apache/kafka";

/// A single-node Kafka broker.
pub struct KafkaContainer {
    container: Container,
    image: ImageName,
}

impl KafkaContainer {
    const PORT: u16 = 9092;

    /// Builds a container from the floating default image (`apache/kafka:latest`).
    pub fn new() -> Self {
        Self::with_image("apache/kafka:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`KafkaContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        let container = Container::new(image.as_str())
            .with_exposed_ports(&[Self::PORT])
            .with_env("KAFKA_NODE_ID", "1")
            .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
            .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9091")
            .with_env(
                "KAFKA_LISTENERS",
                "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9091",
            )
            .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .with_env(
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
            )
            .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            // The apache/kafka image defaults KAFKA_HEAP_OPTS to -Xmx1G, which
            // exceeds microsandbox's default microVM RAM (~450M) and aborts the
            // JVM ("insufficient memory"). A single-node KRaft dev broker runs
            // comfortably in a 256M heap; harmless on the Docker backend, which isn't
            // memory-constrained here.
            .with_env("KAFKA_HEAP_OPTS", "-Xmx256M -Xms256M")
            .waiting_for(Wait::for_log_message(".*Kafka Server started.*", 1))
            // Rewrites the advertised listener to carry the mapped host port; see
            // RedpandaContainer's customizer for why this needs the `mapped`
            // callback.
            .with_spec_customizer(|mut spec, mapped| {
                spec.env.push((
                    "KAFKA_ADVERTISED_LISTENERS".to_string(),
                    format!("PLAINTEXT://127.0.0.1:{}", mapped(Self::PORT)),
                ));
                spec
            });
        Self { container, image }
    }

    /// Boots the container, after checking the image is one this module understands.
    ///
    /// The compatibility check runs here rather than in the constructors so those stay
    /// infallible and match every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server.
    pub async fn start(self) -> Result<KafkaGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(KafkaGuard(self.container.start().await?))
    }
}

impl Default for KafkaContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`KafkaContainer`].
pub struct KafkaGuard(ContainerGuard);

impl KafkaGuard {
    /// The `PLAINTEXT://` bootstrap-servers address for the running broker.
    pub fn bootstrap_servers(&self) -> String {
        format!(
            "PLAINTEXT://{}:{}",
            self.0.host(),
            self.0.get_mapped_port(KafkaContainer::PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for KafkaGuard {
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
        let _ = KafkaContainer::new();
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        KafkaContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = KafkaContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not apache/kafka");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("apache/kafka"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/kafka-hardened:4.0.0")
            .as_compatible_substitute_for("apache/kafka");
        KafkaContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
