# MemcachedContainer

A single-node Memcached container, ready-checked with a **protocol-level `version`
probe** instead of the bare listening-port wait.

**Default image:** floats to `memcached:latest` — this module previously pinned
`memcached:1.6-alpine`; Docker Hub publishes `memcached:latest` as a
Debian-based image rather than Alpine, functionally equivalent for this
module's own use, just a larger pull.
**Guest port:** `11211`
**Expected repository:** `memcached`

| Method | On | Effect |
|---|---|---|
| `MemcachedContainer::new()` | builder | Floating default image (`memcached:latest`). |
| `MemcachedContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.start()` | builder → `Result<MemcachedGuard>` | Checks the image's repository, then boots the container. |
| `.address()` | guard | `host:port` address of the running container. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `memcached` before any backend is resolved or any sandbox is created,
which keeps the constructors infallible like every other module's. A mismatch
returns `RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("memcached")` is the escape hatch for a verified
drop-in replacement from another registry. `new()` goes through this same check
against its own floating reference, so it can never fail in practice.

## Why not the default wait strategy

Memcached logs nothing useful on startup, and the port-forwarding layer on either
backend can bind the host port before the server inside is actually accepting — a
bare TCP-connect wait (even with the standard read-probe) can pass while the first
real client connection still gets a dead stream. This module ships a custom
`WaitStrategy` that sends `version\r\n` over the wire and requires a reply starting
with `VERSION` before considering the container ready — a genuine protocol
handshake, not just "something answered."

This is the worked example referenced throughout the rest of this book whenever a
readiness signal needs to be protocol-level rather than a bare port check — see
[Wait Strategies](../core-concepts/wait-strategies.md#custom-wait-strategies-via-poll_until_ready).

## Complete example

```rust,ignore
use rightsize_modules::MemcachedContainer;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

#[tokio::test]
async fn memcached_speaks_the_protocol() -> rightsize::Result<()> {
    let guard = MemcachedContainer::new().start().await?;

    let mut stream = TcpStream::connect(guard.address()).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    stream.write_all(b"version\r\n").unwrap();
    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).starts_with("VERSION"));

    guard.stop().await?;
    Ok(())
}
```

By the time `start()` returns `Ok`, the module's own wait strategy has already proven
this exact protocol exchange succeeds — a test using a real memcached client crate is
proving the client library, not the container's readiness.

## Backend notes

No memory-limit override and no known quirks beyond the readiness story above, which
applies identically on both backends.
