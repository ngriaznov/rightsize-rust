#![warn(missing_docs)]
#![forbid(unsafe_code)]

//! `rightsize-modules` ships eighteen preconfigured containers on top of the
//! `rightsize` core: Redis, Memcached, ArangoDB, MongoDB, Redpanda, Kafka,
//! SpringCloudConfig, PostgreSQL, MySQL, Apache Pinot, RabbitMQ, MariaDB, WireMock,
//! ClickHouse, Keycloak, Neo4j, Floci, and Apache Flink. Each module is a thin
//! newtype wrapping [`rightsize`]'s `Container` builder — no subclassing, just the
//! spec-customizer and post-start hooks the core exposes — with connection helpers on
//! its guard.
//!
//! Backend wiring is a Cargo feature choice, not a runtime one: `backend-msb` and
//! `backend-docker` (both on by default) pull in `rightsize-msb` and
//! `rightsize-docker` respectively so consumers can trim the dependency they don't
//! need.

pub mod arango;
pub mod clickhouse;
pub mod flink;
pub mod floci;
pub mod kafka;
pub mod keycloak;
pub mod mariadb;
pub mod memcached;
pub mod mongodb;
pub mod mysql;
pub mod neo4j;
pub mod pinot;
pub mod postgres;
pub mod rabbitmq;
pub mod redis;
pub mod redpanda;
pub mod spring_cloud_config;
pub mod wiremock;

pub use arango::{ArangoContainer, ArangoGuard};
pub use clickhouse::{ClickHouseContainer, ClickHouseGuard};
pub use flink::{FlinkContainer, FlinkGuard};
pub use floci::{FlociContainer, FlociGuard};
pub use kafka::{KafkaContainer, KafkaGuard};
pub use keycloak::{KeycloakContainer, KeycloakGuard};
pub use mariadb::{MariaDbContainer, MariaDbGuard};
pub use memcached::{MemcachedContainer, MemcachedGuard};
pub use mongodb::{MongoDbContainer, MongoDbGuard};
pub use mysql::{MySqlContainer, MySqlGuard};
pub use neo4j::{Neo4jContainer, Neo4jGuard};
pub use pinot::{PinotContainer, PinotGuard};
pub use postgres::{PostgresContainer, PostgresGuard};
pub use rabbitmq::{RabbitMqContainer, RabbitMqGuard};
pub use redis::{RedisContainer, RedisGuard};
pub use redpanda::{RedpandaContainer, RedpandaGuard};
pub use spring_cloud_config::{SpringCloudConfigContainer, SpringCloudConfigGuard};
pub use wiremock::{WireMockContainer, WireMockGuard};
