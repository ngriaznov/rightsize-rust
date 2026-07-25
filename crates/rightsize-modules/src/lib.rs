#![warn(missing_docs)]
#![forbid(unsafe_code)]

//! `rightsize-modules` ships twenty-one preconfigured containers on top of the
//! `rightsize` core: Redis, Memcached, ArangoDB, MongoDB, Redpanda, Kafka,
//! SpringCloudConfig, PostgreSQL, MySQL, Apache Pinot, RabbitMQ, MariaDB, WireMock,
//! ClickHouse, Keycloak, Neo4j, Floci, Apache Flink, Valkey, MinIO, and Cassandra. Each
//! module is a thin newtype wrapping [`rightsize`]'s `Container` builder — no
//! subclassing, just the spec-customizer and post-start hooks the core exposes — with
//! connection helpers on its guard.
//!
//! Backend wiring is a Cargo feature choice, not a runtime one: `backend-msb` and
//! `backend-docker` (both on by default) pull in `rightsize-msb` and
//! `rightsize-docker` respectively so consumers can trim the dependency they don't
//! need. The feature-enabled backends register themselves the first time any module
//! starts — no `register_provider` boilerplate; see [`register_default_backends`]
//! for the one case where calling it yourself is still useful.

/// Registers the feature-enabled backend providers (`backend-msb`, `backend-docker`)
/// with [`rightsize::backends`]. Runs its body once per process, and the core
/// registry is itself idempotent by provider name, so calling this again — or also
/// registering a provider by hand — is harmless.
///
/// Every module's `start` calls this on its way into the core, so consumers of this
/// crate write no backend wiring at all. Call it yourself only when the **first**
/// container your process starts is a plain [`rightsize::Container`] rather than one
/// of these modules (backend resolution happens at that first start and is cached
/// for the life of the process).
pub fn register_default_backends() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        #[cfg(feature = "backend-msb")]
        rightsize::backends::register_provider(Box::new(rightsize_msb::MsbBackendProvider));
        #[cfg(feature = "backend-docker")]
        rightsize::backends::register_provider(Box::new(rightsize_docker::DockerBackendProvider));
    });
}

pub mod arango;
pub mod cassandra;
pub mod clickhouse;
pub mod flink;
pub mod floci;
pub mod kafka;
pub mod keycloak;
pub mod mariadb;
pub mod memcached;
pub mod minio;
pub mod mongodb;
pub mod mysql;
pub mod neo4j;
pub mod pinot;
pub mod postgres;
pub mod rabbitmq;
pub mod redis;
pub mod redpanda;
pub mod spring_cloud_config;
pub mod valkey;
pub mod wiremock;

pub use arango::{ArangoContainer, ArangoGuard};
pub use cassandra::{CassandraContainer, CassandraGuard};
pub use clickhouse::{ClickHouseContainer, ClickHouseGuard};
pub use flink::{FlinkContainer, FlinkGuard};
pub use floci::{FlociContainer, FlociGuard};
pub use kafka::{KafkaContainer, KafkaGuard};
pub use keycloak::{KeycloakContainer, KeycloakGuard};
pub use mariadb::{MariaDbContainer, MariaDbGuard};
pub use memcached::{MemcachedContainer, MemcachedGuard};
pub use minio::{MinioContainer, MinioGuard};
pub use mongodb::{MongoDbContainer, MongoDbGuard};
pub use mysql::{MySqlContainer, MySqlGuard};
pub use neo4j::{Neo4jContainer, Neo4jGuard};
pub use pinot::{PinotContainer, PinotGuard};
pub use postgres::{PostgresContainer, PostgresGuard};
pub use rabbitmq::{RabbitMqContainer, RabbitMqGuard};
pub use redis::{RedisContainer, RedisGuard};
pub use redpanda::{RedpandaContainer, RedpandaGuard};
pub use spring_cloud_config::{SpringCloudConfigContainer, SpringCloudConfigGuard};
pub use valkey::{ValkeyContainer, ValkeyGuard};
pub use wiremock::{WireMockContainer, WireMockGuard};
