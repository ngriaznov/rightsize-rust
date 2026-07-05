//! A single-node Kafka broker (KRaft mode, no ZooKeeper).

use rightsize::{Container, ContainerGuard, Result, Wait};

/// A single-node Kafka broker.
pub struct KafkaContainer(Container);

impl KafkaContainer {
    const PORT: u16 = 9092;

    /// Builds a container from the pinned default image (`apache/kafka:4.0.0`).
    pub fn new() -> Self {
        Self::with_image("apache/kafka:4.0.0")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let container = Container::new(image)
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
        Self(container)
    }

    /// Boots the container.
    pub async fn start(self) -> Result<KafkaGuard> {
        Ok(KafkaGuard(self.0.start().await?))
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
}
