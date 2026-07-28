//! A single-node Redpanda broker (Kafka API-compatible) with its schema registry
//! enabled.
//!
//! ### Compatibility checking
//!
//! [`RedpandaContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against
//! `redpandadata/redpanda` (registry host, tag, and digest stripped) before ever
//! touching a backend, returning [`rightsize::RightsizeError::IncompatibleImage`] on
//! a mismatch rather than letting an unrelated image run all the way to a
//! wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("redpandadata/redpanda")` to
//! override for a verified drop-in replacement. [`RedpandaContainer::new`] goes
//! through the same check against its own floating reference, so it can never fail
//! in practice.
//!
//! ### `new()` floats to `redpandadata/redpanda:latest`
//!
//! This module used to pin `redpandadata/redpanda:v24.2.4`; `new()` now floats to
//! `redpandadata/redpanda:latest` so the version tracks upstream rather than this
//! crate's own release cycle.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

/// The alias siblings resolve INTERNAL through — native docker networks, or the msb
/// exec-tunnel alias emulation (best-effort; this port has no sibling Kafka consumer
/// module of its own yet).
const INTERNAL_ALIAS: &str = "redpanda";

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "redpandadata/redpanda";

/// A single-node Redpanda broker.
pub struct RedpandaContainer {
    container: Container,
    image: ImageName,
}

impl RedpandaContainer {
    const KAFKA_PORT: u16 = 9092;
    const INTERNAL_KAFKA_PORT: u16 = 9093;
    const SCHEMA_REGISTRY_PORT: u16 = 8081;

    /// Builds a container from the floating default image
    /// (`redpandadata/redpanda:latest`).
    pub fn new() -> Self {
        Self::with_image("redpandadata/redpanda:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`RedpandaContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        let container = Container::new(image.as_str())
            .with_exposed_ports(&[
                Self::KAFKA_PORT,
                Self::INTERNAL_KAFKA_PORT,
                Self::SCHEMA_REGISTRY_PORT,
            ])
            .waiting_for(Wait::for_log_message(
                ".*Successfully started Redpanda.*",
                1,
            ))
            .with_spec_customizer(|mut spec, mapped| {
                // The advertised listener must carry the *mapped host port*, known
                // only now (ports were allocated before boot). This is why the
                // customizer takes `mapped` — a one-shot rewrite the instant before
                // create, so the broker advertises an address host clients can
                // actually reach. EXTERNAL advertises that mapped host port for host
                // clients; INTERNAL advertises the fixed alias:port siblings resolve
                // on the container network. See KafkaContainer's customizer for the
                // same trick applied to a single advertised listener.
                let kafka_mapped = mapped(Self::KAFKA_PORT);
                spec.command = Some(
                    [
                        "redpanda",
                        "start",
                        "--mode",
                        "dev-container",
                        "--smp",
                        "1",
                        "--kafka-addr",
                        "EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:9093",
                        "--advertise-kafka-addr",
                    ]
                    .into_iter()
                    .map(String::from)
                    .chain(std::iter::once(format!(
                        "EXTERNAL://127.0.0.1:{kafka_mapped},INTERNAL://{INTERNAL_ALIAS}:9093"
                    )))
                    .chain(
                        ["--schema-registry-addr", "0.0.0.0:8081"]
                            .into_iter()
                            .map(String::from),
                    )
                    .collect(),
                );
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
    pub async fn start(self) -> Result<RedpandaGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(RedpandaGuard(self.container.start().await?))
    }
}

impl Default for RedpandaContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`RedpandaContainer`].
pub struct RedpandaGuard(ContainerGuard);

impl RedpandaGuard {
    /// The `PLAINTEXT://` bootstrap-servers address for the running broker (EXTERNAL
    /// listener).
    pub fn bootstrap_servers(&self) -> String {
        format!(
            "PLAINTEXT://{}:{}",
            self.0.host(),
            self.0
                .get_mapped_port(RedpandaContainer::KAFKA_PORT)
                .unwrap()
        )
    }

    /// The schema registry's base URI for the running broker.
    pub fn schema_registry_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.0.host(),
            self.0
                .get_mapped_port(RedpandaContainer::SCHEMA_REGISTRY_PORT)
                .unwrap()
        )
    }

    /// Stops and removes the container, releasing its host ports.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for RedpandaGuard {
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
        let _ = RedpandaContainer::new();
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        RedpandaContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = RedpandaContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not redpandadata/redpanda");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("redpandadata/redpanda"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/redpanda-hardened:v24.2.4")
            .as_compatible_substitute_for("redpandadata/redpanda");
        RedpandaContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
