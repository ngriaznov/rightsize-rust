# MemcachedContainer

A single-node Memcached container, ready-checked with a **protocol-level `version`
probe** instead of the bare listening-port wait.

**Default image:** `memcached:1.6-alpine`
**Guest port:** `11211`

| Method | On | Effect |
|---|---|---|
| `MemcachedContainer::new()` | builder | Pinned default image. |
| `MemcachedContainer::with_image(image)` | builder | Caller-chosen image. |
| `.start()` | builder → `Result<MemcachedGuard>` | Boots the container. |
| `.address()` | guard | `host:port` address of the running container. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

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
