# RedpandaContainer

A single-node Redpanda broker (Kafka API-compatible) with its schema registry
enabled.

**Default image:** `docker.redpanda.com/redpandadata/redpanda:latest`
**Guest ports:** `9092` (external Kafka), `9093` (internal Kafka), `8081` (schema
registry)

| Method | On | Effect |
|---|---|---|
| `RedpandaContainer::new()` | builder | Pinned default image. |
| `RedpandaContainer::with_image(image)` | builder | Caller-chosen image. |
| `.start()` | builder → `Result<RedpandaGuard>` | Boots the container. |
| `.bootstrap_servers()` | guard | `PLAINTEXT://host:port` for the EXTERNAL listener. |
| `.schema_registry_url()` | guard | Schema registry base URI. |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## The advertised-listener rewrite

Redpanda needs to advertise the address a client can actually reach — but the
mapped host port for the Kafka listener is only known *after* ports are allocated,
which happens right before `create()`. This module uses
[`Container::with_spec_customizer`](../core-concepts/containers-and-guards.md) to
rewrite the boot command the instant before create, once the mapped port is known:

- **EXTERNAL** listener advertises `127.0.0.1:<mapped host port>` — what a test
  process on the host actually dials.
- **INTERNAL** listener advertises the fixed `redpanda:9093` alias/port —
  what a sibling container on the same [`Network`](../core-concepts/networking.md)
  resolves to reach this broker without going through the host port mapping at all.

This is the same trick [`KafkaContainer`](./kafka.md) uses for its single advertised
listener; Redpanda's version is the fuller example because it has two listeners to
rewrite (EXTERNAL and INTERNAL) rather than one.

## Complete example

```rust,ignore
use rightsize_modules::RedpandaContainer;

#[tokio::test]
async fn redpanda_boots_and_advertises_a_reachable_port() -> rightsize::Result<()> {
    let guard = RedpandaContainer::new().start().await?;

    // guard.bootstrap_servers() is a PLAINTEXT://127.0.0.1:<port> address usable by
    // any Kafka-protocol client crate (rdkafka, kafka-protocol, etc.) from the host.
    println!("bootstrap: {}", guard.bootstrap_servers());
    println!("schema registry: {}", guard.schema_registry_url());

    guard.stop().await?;
    Ok(())
}
```

This crate does not take a Kafka client-library dev-dependency for its own
integration suite (`crates/rightsize-modules/tests/broker_modules_it.rs` checks the
schema registry over plain HTTP instead) — bring whichever Kafka-protocol client
your project already uses.

## Backend notes

No memory-limit override. `INTERNAL_ALIAS = "redpanda"` is the alias siblings resolve
through on a shared `Network` — this port has no sibling Kafka-consumer module of its
own yet, so cross-container consumption over the microsandbox
[network-link emulation](../core-concepts/networking.md#limits-on-the-microvm-backend)
is best-effort, not covered by this crate's own contract suite.
