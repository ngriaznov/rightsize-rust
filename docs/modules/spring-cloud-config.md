# SpringCloudConfigContainer

A Spring Cloud Config Server container, ready-checked via its actuator health
endpoint.

**Default image:** `hyness/spring-cloud-config-server:latest`
**Guest port:** `8888`

| Method | On | Effect |
|---|---|---|
| `SpringCloudConfigContainer::new()` | builder | Pinned default image. |
| `SpringCloudConfigContainer::with_image(image)` | builder | Caller-chosen image. |
| `.with_env(key, value)` | builder | Thin passthrough to `Container::with_env` — see below. |
| `.start()` | builder → `Result<SpringCloudConfigGuard>` | Boots the container. |
| `.uri()` | guard | Config server base URI, e.g. `http://127.0.0.1:<port>`. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## The Paketo memory story

Paketo's memory calculator sizes this JVM image's fixed regions (metaspace, thread
stacks, direct memory) at roughly 688 MB — above microsandbox's ~450 MB default
microVM RAM. This module ships with `.with_memory_limit(1024)` baked into its
constructor for exactly this reason; you don't set this yourself when using the
module. See [Files & Resources](../core-concepts/files-and-resources.md#the-paketo-story-springcloudconfigcontainer)
for the full account, and [`PinotContainer`](./pinot.md) for the module that hit an
even bigger version of the same problem.

## The native-profile decision, and why it's the caller's call

The default, git-backed environment repository needs a configured git URI to boot at
all — booting this module with zero configuration will not serve config from
anywhere useful. Callers wanting the classpath/filesystem-backed environment
repository instead should chain:

```rust,ignore
use rightsize_modules::SpringCloudConfigContainer;

let config = SpringCloudConfigContainer::new()
    .with_env("SPRING_PROFILES_ACTIVE", "native")
    .start()
    .await?;
```

This module deliberately does **not** set this profile itself — that decision
belongs to the caller/test. If you're networking this container to a consumer service via
[`Network`](../core-concepts/networking.md) (the pattern this module's rustdoc and
this crate's own contract tests both use it for), you'll almost always want the
`native` profile plus a mounted or baked-in config repository.

## Complete example (networked)

`SpringCloudConfigContainer` has no `with_network`/`with_network_aliases`
passthrough — unlike `with_env`, networking was never wired through this
newtype. Networking this config server to a consumer means building it with
the plain `Container` API directly (the same image the module wraps), not the
module type:

```rust,ignore
use rightsize::{Container, Network, Wait};
use std::sync::Arc;

#[tokio::test]
async fn app_fetches_config_from_a_sibling() -> rightsize::Result<()> {
    let net = Arc::new(Network::new_network());

    let config = Container::new("hyness/spring-cloud-config-server:latest")
        .with_env("SPRING_PROFILES_ACTIVE", "native")
        .with_exposed_ports(&[8888])
        .waiting_for(Wait::for_http("/actuator/health").for_port(8888))
        .with_memory_limit(1024) // Paketo's JVM regions need more than msb's ~450 MB default
        .with_network(&net)
        .with_network_aliases(&["configuration-stub"])
        .start()
        .await?;

    let app = Container::new("my-service:latest")
        .with_network(&net)
        .with_env(
            "SPRING_CLOUD_CONFIG_URI",
            &format!("http://{}", net.resolve("configuration-stub", 8888)?),
        )
        .start()
        .await?;

    // ... exercise `app`, which fetches its config from the sibling above ...

    app.stop().await?;
    config.stop().await?;
    Ok(())
}
```

This mirrors the cross-container pattern from [Networking](../core-concepts/networking.md)
exactly — `SpringCloudConfigContainer` is the right type for the direct-use case
(no sibling to reach it from), but reaching it from another container over a
`Network` currently means dropping to the plain `Container` API and reproducing
the module's own wait strategy and memory limit by hand, as above.

## Backend notes

`.with_memory_limit(1024)` is set unconditionally by the module — harmless on
Docker, required on microsandbox. No other backend-specific quirks known.
