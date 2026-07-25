# ValkeyContainer

A single-node Valkey container. Valkey is a protocol-compatible Redis fork; this
module mirrors [`RedisContainer`](./redis.md)'s shape exactly — same wait strategy,
same lack of a memory-limit override, same single-newtype build.

**Default image:** `valkey/valkey:9.1-alpine`
**Guest port:** `6379`

| Method | On | Effect |
|---|---|---|
| `ValkeyContainer::new()` | builder | Pinned default image. |
| `ValkeyContainer::with_image(image)` | builder | Caller-chosen image. |
| `.start()` | builder → `Result<ValkeyGuard>` | Boots the container. |
| `.uri()` | guard | `redis://host:port` connection URI (see below for why the scheme is `redis://`). |
| `.stop()` | guard | Stops and removes the container, releases its port. |

`ValkeyGuard` derefs to `ContainerGuard`, so `exec()`, `logs()`, `get_mapped_port()`,
etc. are all available directly on it too.

## Readiness — same signal, same reasoning as Redis

`Ready to accept connections tcp` is Valkey's own log line, and a bare
listening-port probe is not enough here for the identical reason it isn't for
[`RedisContainer`](./redis.md): on a loaded host, msb's loopback port forwarder can
accept and hold a TCP connection in the window between the guest binding its socket
and the server actually serving it, which a port-only check can't see through.
Verified against a real boot: `valkey/valkey:9.1-alpine` came up, and `valkey-cli
PING` against the mapped port replied `PONG`. No env is required and no
memory-limit override was needed beyond msb's default during that boot.

## `uri()` returns `redis://`, not `valkey://` — deliberate

Every client this module's tests (and its users) actually reach for — lettuce,
node-redis, raw RESP over TCP — parses the `redis://` scheme; none of them recognize
`valkey://`. `ValkeyGuard::uri` returns `redis://<host>:<port>` on purpose, carried
over unchanged from `RedisContainer::uri`'s own scheme rather than an oversight.

## Complete example

```rust,ignore
use rightsize_modules::ValkeyContainer;

#[tokio::test]
async fn cache_roundtrip() -> rightsize::Result<()> {
    let valkey = ValkeyContainer::new().start().await?;

    // Any redis:// client works unmodified — Valkey speaks the same RESP protocol.
    let client = redis::Client::open(valkey.uri()).unwrap();
    let mut con = client.get_connection().unwrap();
    redis::cmd("SET").arg("k").arg("v").execute(&mut con);
    let v: String = redis::cmd("GET").arg("k").query(&mut con).unwrap();
    assert_eq!(v, "v");

    valkey.stop().await?;
    Ok(())
}
```

(This crate's own integration suite proves the same connectivity with a raw TCP
`PING`/`PONG` round-trip rather than pulling in a Redis client crate as a
dev-dependency — see `crates/rightsize-modules/tests/datastore_modules_it.rs`.)

## Backend notes

No memory-limit override and no known quirks on either backend — same footprint
story as [`RedisContainer`](./redis.md). Nothing else to flag here; see
[Backends](../backends.md) for the general backend-difference list if you hit
something unexpected.
