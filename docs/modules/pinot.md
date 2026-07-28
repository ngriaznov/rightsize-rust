# PinotContainer

A single-container Apache Pinot "QuickStart" cluster: controller, broker, server,
minion, and an embedded ZooKeeper, all as one process tree inside one image,
started with `QuickStart -type EMPTY` — a clean cluster with no demo tables. This is
a real-cluster smoke fixture, not a data-loading harness.

**Default image:** floats to `apachepinot/pinot:latest` — this module previously
pinned `apachepinot/pinot:1.5.1`.
**Guest ports:** `9000` (controller REST API), `8000` (broker query API)
**Expected repository:** `apachepinot/pinot`

| Method | On | Effect |
|---|---|---|
| `PinotContainer::new()` | builder | Floating default image. |
| `PinotContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.start()` | builder → `Result<PinotGuard>` | Checks the image's repository, then boots the container (up to 180s timeout). |
| `.controller_url()` | guard | Controller REST base URI (schema/table/segment admin). |
| `.broker_url()` | guard | Broker query base URI. |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `apachepinot/pinot` before any backend is resolved or any sandbox is
created, which keeps the constructors infallible like every other module's. A
mismatch returns `RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("apachepinot/pinot")` is the escape hatch for a
verified drop-in replacement from another registry. `new()` goes through this
same check against its own floating reference, so it can never fail in
practice.

## Ports — empirically verified, not the QuickStart docs' assumption

The controller REST API is on 9000 as documented. The broker's **query** port is
**8000**, not 8099 as some QuickStart documentation implies — confirmed from a real
boot log:

```text
StartControllerCommand ... -controllerPort 9000 ...
INFO: Started listener bound to [0.0.0.0:9000]
StartBrokerCommand ... -brokerPort 8000 -brokerGrpcPort 8010 ...
INFO: Started listener bound to [0.0.0.0:8000]
```

`curl http://<host>:8000/health` returns `503` while the broker is still registering
with the cluster and `200` once it's live; 8099 is not opened by QuickStart at all.
This module exposes 8000 and names the helper `broker_url` accordingly — if you've
seen 8099 referenced elsewhere for Pinot, that's not this deployment mode.

## Memory — measured, not the originally-planned 2048 MB

See [Files & Resources](../core-concepts/files-and-resources.md#the-pinot-story-pinotcontainer)
for the full measured table (2048/2560/3072/4096 MB attempts). Short version: the
image bakes `JAVA_OPTS=-Xms4G -Xmx4G`, and this module ships with
`.with_memory_limit(4096)` — the lowest round number with real headroom, verified
stable (schema POSTs succeeding under repeated load) on both backends. Anything at or
below 3072 MB either gets OOM-killed outright or runs Helix RPCs into intermittent
500s under memory pressure even though `/health` reports 200 — a passing readiness
check that doesn't mean the cluster is actually usable is exactly the trap this
measurement avoids.

## Boot time

A four-JVM cluster booting cold on a laptop is legitimately slow (observed 60-120s).
This module's wait strategy uses a 180-second startup timeout on
`Wait::for_http("/health").for_port(9000)` for exactly this reason — the
controller's REST listener comes up well before the whole cluster has stabilized,
but it's the only readiness signal that's both meaningful and doesn't require
polling the broker separately before every test can proceed.

## Complete example

```rust,ignore
use rightsize_modules::PinotContainer;

#[tokio::test]
async fn pinot_controller_and_broker_are_reachable() -> Result<(), Box<dyn std::error::Error>> {
    let guard = PinotContainer::new().start().await?;

    let body = ureq::get(format!("{}/health", guard.controller_url()))
        .call()?
        .body_mut()
        .read_to_string()?;
    println!("controller health: {body}");
    println!("broker url: {}", guard.broker_url());

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

`.with_memory_limit(4096)` is set unconditionally by the module. This is the largest
memory floor of any shipped module — if you're timing out at the default 180s
startup wait, check available host RAM before assuming it's a readiness-wait bug;
four JVMs at a 4 GiB ceiling is a genuinely heavy boot.
