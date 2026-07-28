# Modules

`rightsize-modules` ships twenty-three preconfigured containers on top of the
`rightsize` core: Redis, Memcached, ArangoDB, MongoDB, Redpanda, Kafka,
SpringCloudConfig, PostgreSQL, MySQL, Apache Pinot, RabbitMQ, MariaDB, WireMock,
ClickHouse, Keycloak, Neo4j, Floci, Apache Flink, Valkey, MinIO, Cassandra,
Elasticsearch, and Qdrant. Each module is a thin newtype wrapping `Container` — no
subclassing, just the spec-customizer and post-start hooks the core exposes (see
[Containers & Guards](../core-concepts/containers-and-guards.md)) — with connection
helpers on its guard.

| Module | Helper(s) |
|---|---|
| [`RedisContainer`](./redis.md) | `uri()` |
| [`ValkeyContainer`](./valkey.md) | `uri()` (Redis-protocol-compatible; deliberately returns `redis://`, not `valkey://`) |
| [`MemcachedContainer`](./memcached.md) | `address()` (protocol-level `VERSION` probe, not a bare port wait) |
| [`MongoDbContainer`](./mongodb.md) | `connection_string()` (single-node replica set, auto-initiated) |
| [`ArangoContainer`](./arango.md) | `endpoint()`, `with_root_password(…)` |
| [`PostgresContainer`](./postgres.md) | `connection_string()`, `username()`, `password()`, `database_name()`, `with_username`/`with_password`/`with_database(…)` |
| [`MySqlContainer`](./mysql.md) | `connection_string()`, `username()`, `password()`, `database_name()`, `with_username`/`with_password`/`with_database(…)` |
| [`MariaDbContainer`](./mariadb.md) | `connection_string()`, `username()`, `password()`, `database_name()`, `with_username`/`with_password`/`with_database(…)` |
| [`RedpandaContainer`](./redpanda.md) | `bootstrap_servers()`, `schema_registry_url()` |
| [`KafkaContainer`](./kafka.md) | `bootstrap_servers()` (KRaft single node) |
| [`RabbitMqContainer`](./rabbitmq.md) | `amqp_url()`, `management_url()`, `username()`, `password()`, `with_username`/`with_password(…)` |
| [`ClickHouseContainer`](./clickhouse.md) | `http_url()`, `username()`, `password()`, `database_name()`, `with_username`/`with_password`/`with_database(…)` |
| [`SpringCloudConfigContainer`](./spring-cloud-config.md) | `uri()` (defaults `with_memory_limit(1024)`) |
| [`PinotContainer`](./pinot.md) | `controller_url()`, `broker_url()` (defaults `with_memory_limit(4096)`) |
| [`WireMockContainer`](./wiremock.md) | `base_url()`, `admin_url()` |
| [`KeycloakContainer`](./keycloak.md) | `auth_server_url()`, `management_url()`, `admin_username()`, `admin_password()`, `with_admin_username`/`with_admin_password(…)` (defaults `with_memory_limit(1024)`) |
| [`Neo4jContainer`](./neo4j.md) | `http_url()`, `bolt_url()`, `username()`, `password()`, `with_password(…)` (defaults `with_memory_limit(1024)`) |
| [`FlociContainer`](./floci.md) | `FlociContainer::aws()`/`azure()`/`gcp()`, `endpoint_url()` |
| [`FlinkContainer`](./flink.md) | `rest_url()`, `with_task_manager()` (docker only; defaults `with_memory_limit(1024)`) |
| [`MinioContainer`](./minio.md) | `s3_url()`, `console_url()`, `root_user()`, `root_password()`, `with_root_user`/`with_root_password(…)` (no memory limit set) |
| [`CassandraContainer`](./cassandra.md) | `contact_point()`, `cql_port()`, `local_datacenter()` (defaults `with_memory_limit(2560)`) |
| [`ElasticsearchContainer`](./elasticsearch.md) | `rest_url()` (no `new()` — Elastic publishes no floating tag, so `with_image` always requires an explicit version; defaults `with_memory_limit(2560)`) |
| [`QdrantContainer`](./qdrant.md) | `rest_url()` (`new()` floats to `qdrant/qdrant:latest`; no memory limit set) |

Backend wiring is a Cargo feature choice, not a runtime one: `backend-msb` and
`backend-docker` (both on by default) pull in `rightsize-msb` and `rightsize-docker`
respectively, so you can trim the one you don't need.

## Compatibility checking

Every module's `with_image` takes `impl Into<ImageName>` and checks the supplied
image's repository (registry host, tag, and digest stripped) against what that
module declares it understands, before ever touching a backend — a mismatch
fails fast with `RightsizeError::IncompatibleImage` rather than surfacing later
as a bare wait-strategy timeout. The check runs at the top of `start()`, not in
the constructor, so every constructor here stays infallible;
`ImageName::parse(image).as_compatible_substitute_for(repo)` is the escape hatch
for a verified drop-in replacement from another registry. Each module's own
page states the exact repository it expects under "Expected repository" near
the top, and walks the check in its own "Compatibility checking" section.
`FlociContainer` is the one exception to "one repository per module": it wraps
three unrelated images behind a single type, and each of its three factories
(`aws`/`azure`/`gcp`) checks against its own distinct repository — see
[its page](./floci.md).

## Anything not on this list

Twenty-three modules is not an exhaustive catalog — it's the current coverage,
growing as needs arise. Anything else is the plain `Container` API (see
[Containers & Guards](../core-concepts/containers-and-guards.md)); a thin
wrapper following the existing modules' shape (a newtype over `Container`, a
`*Guard` newtype over `ContainerGuard` with connection helpers, a `with_image`/`new`
pair) is a small, welcome contribution — see the repository's `CONTRIBUTING.md`.
