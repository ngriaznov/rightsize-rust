//! A single-node Redpanda broker (Kafka API-compatible) with its schema registry
//! enabled.

use rightsize::{Container, ContainerGuard, Result, Wait};

/// The alias siblings resolve INTERNAL through — native docker networks, or the msb
/// exec-tunnel alias emulation (best-effort; this port has no sibling Kafka consumer
/// module of its own yet).
const INTERNAL_ALIAS: &str = "redpanda";

/// A single-node Redpanda broker.
pub struct RedpandaContainer(Container);

impl RedpandaContainer {
    const KAFKA_PORT: u16 = 9092;
    const INTERNAL_KAFKA_PORT: u16 = 9093;
    const SCHEMA_REGISTRY_PORT: u16 = 8081;

    /// Builds a container from the pinned default image
    /// (`docker.redpanda.com/redpandadata/redpanda:v24.2.4`).
    pub fn new() -> Self {
        Self::with_image("docker.redpanda.com/redpandadata/redpanda:v24.2.4")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let container = Container::new(image)
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
        Self(container)
    }

    /// Boots the container.
    pub async fn start(self) -> Result<RedpandaGuard> {
        Ok(RedpandaGuard(self.0.start().await?))
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
}
