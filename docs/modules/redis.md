# RedisContainer

A single-node Redis container, ready-checked with a plain TCP read-probe
([`Wait::for_listening_port`](../core-concepts/wait-strategies.md)) — Redis speaks
first on connect, so the bare listening-port wait is sufficient here (contrast
[`MemcachedContainer`](./memcached.md), which needs a protocol-level probe).

**Default image:** `redis:8.6-alpine`
**Guest port:** `6379`

| Method | On | Effect |
|---|---|---|
| `RedisContainer::new()` | builder | Pinned default image. |
| `RedisContainer::with_image(image)` | builder | Caller-chosen image. |
| `.start()` | builder → `Result<RedisGuard>` | Boots the container. |
| `.uri()` | guard | `redis://host:port` connection URI. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

`RedisGuard` derefs to `ContainerGuard`, so `exec()`, `logs()`, `get_mapped_port()`,
etc. are all available directly on it too.

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
