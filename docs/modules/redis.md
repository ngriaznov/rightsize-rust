# RedisContainer

A single-node Redis container, ready-checked with a plain TCP read-probe
([`Wait::for_listening_port`](../core-concepts/wait-strategies.md)) — Redis speaks
first on connect, so the bare listening-port wait is sufficient here (contrast
[`MemcachedContainer`](./memcached.md), which needs a protocol-level probe).

**Default image:** floats to `redis:latest` — this module previously pinned
`redis:8.6-alpine`; Docker Hub publishes `redis:latest` as a Debian-based image
rather than Alpine, functionally equivalent for this module's own use, just a
larger pull.
**Guest port:** `6379`
**Expected repository:** `redis`

| Method | On | Effect |
|---|---|---|
| `RedisContainer::new()` | builder | Floating default image (`redis:latest`). |
| `RedisContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.start()` | builder → `Result<RedisGuard>` | Checks the image's repository, then boots the container. |
| `.uri()` | guard | `redis://host:port` connection URI. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

`RedisGuard` derefs to `ContainerGuard`, so `exec()`, `logs()`, `get_mapped_port()`,
etc. are all available directly on it too.

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `redis` before any backend is resolved or any sandbox is created, which
keeps the constructors infallible like every other module's:

```rust,ignore
use rightsize::ImageName;
use rightsize_modules::RedisContainer;

// Checked on start, common case.
let guard = RedisContainer::with_image("redis:8").start().await?;

// A verified drop-in replacement from another registry — the escape hatch.
let guard = RedisContainer::with_image(
    ImageName::parse("mycorp/redis-hardened:8").as_compatible_substitute_for("redis"),
)
.start()
.await?;
```

A mismatch returns `RightsizeError::IncompatibleImage` — naming the supplied
repository, the expected one, and the override above — rather than letting an
unrelated image run all the way to a wait-strategy timeout. `new()` goes through
this same check against its own floating reference, so it can never fail in
practice.

## Complete example

```rust,ignore
use rightsize_modules::RedisContainer;

#[tokio::test]
async fn cache_roundtrip() -> rightsize::Result<()> {
    let redis = RedisContainer::new().start().await?;

    let client = redis::Client::open(redis.uri()).unwrap();
    let mut con = client.get_connection().unwrap();
    redis::cmd("SET").arg("k").arg("v").execute(&mut con);
    let v: String = redis::cmd("GET").arg("k").query(&mut con).unwrap();
    assert_eq!(v, "v");

    redis.stop().await?;
    Ok(())
}
```

(This crate's own integration suite proves the same connectivity with a raw TCP
`PING`/`PONG` round-trip rather than pulling in the `redis` crate as a dev-dependency
— see `crates/rightsize-modules/tests/datastore_modules_it.rs`.)

## Backend notes

No memory-limit override and no known quirks on either backend — Redis's default
footprint is well under microsandbox's ~450 MB default microVM RAM. Nothing else to
flag here; see [Backends](../backends.md) for the general backend-difference list if
you hit something unexpected.
