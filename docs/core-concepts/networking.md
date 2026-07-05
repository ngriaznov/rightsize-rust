# Networking

`Network` gives containers alias-based connectivity on both backends — the same API
whether the containers underneath are Docker containers on a bridge network or
fully-isolated microVMs with no shared network at all.

```rust,ignore
use rightsize::{Container, Network, Wait};
use std::sync::Arc;

let net = Arc::new(Network::new_network());
let config = Container::new("hyness/spring-cloud-config-server:latest")
    .with_network(&net)
    .with_network_aliases(&["configuration-stub"])
    .with_exposed_ports(&[8888])
    .start()
    .await?;

let app = Container::new("my-service:latest")
    .with_network(&net)
    .with_env("CONFIG_URI", &format!("http://{}", net.resolve("configuration-stub", 8888)?))
    .start()
    .await?;
```

`Network::resolve(alias, guest_port)` returns `alias:guest_port` — identical string
shape on both backends — and errors, naming the alias, if no registered member
carries it. A container only contributes links to *later* joiners once it's
registered, which happens after its own network-link installation step, so a
container can never end up linked to itself.

## What each backend actually does under the hood

**On Docker:** this is a native Docker network alias — the daemon's own bridge
networking and embedded DNS resolve `alias:port` for you. No emulation, no tunnel,
full native container-to-container connectivity.

**On microsandbox:** microVMs are fully isolated from each other — there's no shared
bridge network to attach to. rightsize-rust transparently installs:

1. An `/etc/hosts` entry inside the consuming container's guest, mapping the alias to
   `127.0.0.1`.
2. A TCP relay tunneled over the sandbox's `exec --stream` channel — the *only* guest
   data path available on this msb build (no sandbox→host TCP under any net-rule
   tried; SSH forwarding was found broken too). The tunnel pumps raw bytes,
   unbuffered, flush-per-read, in both directions.

This is real emulation, not a shortcut, and it has real limits — see below.

## Limits on the microVM backend

- **Start dependencies before their consumers.** Network links are computed for a
  new member from whichever siblings are *already running* at the moment it joins —
  see [Containers & Guards](./containers-and-guards.md#the-raii-lifecycle), step 4.
  A container started before its dependency is up won't retroactively gain a link to
  it.
- **One connection at a time per tunnel.** The in-guest `nc -l` listener backing a
  tunnel serves one connection, then gets respawned for the next. Fine for
  config-fetch-style traffic; not fine for a long-lived cross-container consumer
  (e.g. a Kafka consumer reading continuously from a broker on a sibling microVM).
- **Client speaks first.** The tunnel protocol assumes the connecting side sends the
  first bytes — matches HTTP requests and most RPC-style protocols; a server that
  waits silently for the client to speak needs the client end to actually be the one
  initiating data, which HTTP/REST calls naturally are.
- **The consumer image needs `nc`/busybox.** The tunnel is implemented as a shelled-out
  `nc` listener inside the guest. An image without it (a scratch-based image, or one
  that stripped busybox) fails `start()` fast, with an error naming the missing
  binary and suggesting `RIGHTSIZE_BACKEND=docker` as the workaround — verified by
  this crate's own integration suite using `mongo:8.0` as the no-`nc` counter-example.
- **A target that never propagates TCP close can't be detected by naive EOF.** The
  msb port-publish proxy doesn't propagate the target socket's close to the tunnel,
  so end-of-exchange is inferred from an idle window *after* the first byte arrives
  — not from the whole connection, which would wrongly truncate a slow-to-respond
  target. This is an internal detail (see [How It Works](../how-it-works.md)), but it
  explains why a connection that never sends any bytes back can hang until the idle
  timeout rather than closing immediately.

Every one of these is a real capability gap versus Docker's native bridge networking
on this backend, not a timing quirk that will resolve itself with retries — pick
`RIGHTSIZE_BACKEND=docker` for a network topology this doesn't fit, or restructure the
test to fit inside these bounds (they cover this project's own contract suite, which
is mostly one-shot config-fetch-shaped traffic).

## Alias resolution is registration-order-independent for readers

`Network::resolve` just checks "is any registered member carrying this alias" — it
doesn't care what order `resolve` calls happen in relative to `start()` calls, only
that the *target* container has already finished starting (and thus registering)
by the time you call `resolve` on its alias. Calling `resolve` for an alias that
hasn't started yet returns an error naming the alias, not a hang.
