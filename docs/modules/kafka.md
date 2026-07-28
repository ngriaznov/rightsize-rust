# KafkaContainer

A single-node Kafka broker running in **KRaft mode** — no ZooKeeper.

**Default image:** floats to `apache/kafka:latest` — this module previously
pinned `apache/kafka:4.0.0`.
**Guest port:** `9092`
**Expected repository:** `apache/kafka`

| Method | On | Effect |
|---|---|---|
| `KafkaContainer::new()` | builder | Floating default image. |
| `KafkaContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.start()` | builder → `Result<KafkaGuard>` | Checks the image's repository, then boots the container. |
| `.bootstrap_servers()` | guard | `PLAINTEXT://host:port` bootstrap address. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `apache/kafka` before any backend is resolved or any sandbox is
created, which keeps the constructors infallible like every other module's. A
mismatch returns `RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("apache/kafka")` is the escape hatch for a
verified drop-in replacement from another registry. `new()` goes through this
same check against its own floating reference, so it can never fail in
practice.

## Defaults baked in

This module sets a full KRaft single-node env block so the broker is usable with
zero configuration: `KAFKA_NODE_ID=1`, `KAFKA_PROCESS_ROLES=broker,controller`, a
self-quorum `KAFKA_CONTROLLER_QUORUM_VOTERS`, and `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`
(a single-node broker can't satisfy a replication factor greater than 1).

**The heap fix:** the `apache/kafka` image defaults `KAFKA_HEAP_OPTS` to `-Xmx1G`,
which exceeds microsandbox's ~450 MB default microVM RAM and aborts the JVM with an
"insufficient memory" error. This module overrides it to `-Xmx256M -Xms256M` — a
single-node KRaft dev broker runs comfortably in a 256 MB heap, and the override is
harmless on the Docker backend, which isn't memory-constrained here. If you're
wrapping a different Kafka-family image yourself and it aborts on boot under
`RIGHTSIZE_BACKEND=microsandbox`, check its default heap flags before reaching for
`with_memory_limit` — see [Files & Resources](../core-concepts/files-and-resources.md#memory-limits-when-and-why).

**The advertised-listener rewrite:** same trick as
[`RedpandaContainer`](./redpanda.md), simpler (one listener instead of two) — a
`with_spec_customizer` hook rewrites `KAFKA_ADVERTISED_LISTENERS` to
`PLAINTEXT://127.0.0.1:<mapped host port>` right before `create()`, once the mapped
port is actually known.

## Complete example

```rust,ignore
use rightsize_modules::KafkaContainer;

#[tokio::test]
async fn kafka_boots_and_advertises_a_reachable_port() -> rightsize::Result<()> {
    let guard = KafkaContainer::new().start().await?;

    // guard.bootstrap_servers() is a PLAINTEXT://127.0.0.1:<port> address usable by
    // any Kafka-protocol client crate.
    println!("bootstrap: {}", guard.bootstrap_servers());

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

The `KAFKA_HEAP_OPTS` override above is required for this module to boot under
microsandbox's default RAM — without it, boot fails outright. No
`with_memory_limit` call is needed on top of the heap override; the reduced heap
already fits comfortably.
