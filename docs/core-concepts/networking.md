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
bridge network to attach to. rightsize-rust transparently installs, for every link:

1. An `/etc/hosts` entry inside the consuming container's guest, mapping the alias to
   `127.0.0.1`.
2. A TCP link gets a relay tunneled over the sandbox's `exec --stream` channel —
   sandboxes share no network with each other on this msb build, so exec is what
   carries the bytes between them. The tunnel pumps raw bytes, unbuffered,
   flush-per-read, in both directions. A UDP link instead gets an
   in-guest forwarder script — see
   [UDP network links on the microVM backend](#udp-network-links-on-the-microvm-backend)
   below for how it differs.

This is real emulation, not a shortcut, and it has real limits — see below.

## Limits on the microVM backend

- **Start dependencies before their consumers.** Network links are computed for a
  new member from whichever siblings are *already running* at the moment it joins —
  see [Containers & Guards](./containers-and-guards.md#the-raii-lifecycle), step 4.
  A container started before its dependency is up won't retroactively gain a link to
  it.
- **One TCP connection at a time per tunnel.** The in-guest `nc -l` listener backing
  a TCP link serves one connection, then gets respawned for the next. Fine for
  config-fetch-style traffic; not fine for a long-lived cross-container consumer
  (e.g. a Kafka consumer reading continuously from a broker on a sibling microVM).
  UDP links don't share this limit — see the dedicated section below.
- **A TCP link's client speaks first.** The tunnel protocol assumes the connecting
  side sends the first bytes — matches HTTP requests and most RPC-style protocols; a
  server that waits silently for the client to speak needs the client end to
  actually be the one initiating data, which HTTP/REST calls naturally are.
- **The consumer image needs `nc`/busybox, for either protocol.** Both link
  mechanisms are implemented as shelled-out `nc` inside the guest. An image without
  it (a scratch-based image, or one that stripped busybox) fails `start()` fast,
  with an error naming the missing binary and suggesting `RIGHTSIZE_BACKEND=docker`
  as the workaround — verified by this crate's own integration suite using
  `mongo:8.0` as the no-`nc` counter-example.
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

## UDP network links on the microVM backend

A container reachable only via `.with_exposed_udp_ports(...)` can be joined to a
`Network` as a link target — the consumer's sandbox reaches `alias:guestPort` through
an in-guest forwarder, not the TCP tunnel above.

**Requirements:**

- The target exposes the port with `.with_exposed_udp_ports(...)` and is started
  *before* the consumer — same link-computation-order rule as every other link (see
  [Limits on the microVM backend](#limits-on-the-microvm-backend) above).
- The consumer image needs a busybox-style `nc` (with `-u` and `-e`) and `timeout`.
  Alpine and other busybox-based images have both; Debian/Ubuntu images and
  OpenBSD's own `nc` do not.

**Behavior:** the consumer's sandbox gets one host-UDP egress rule per linked port —
nothing broader than that, so it still can't reach an arbitrary host UDP port it
didn't link to. Each distinct client socket through a link holds its own small relay
for up to 60 seconds.

**Limits:** a datagram must stay at or under 1472 bytes of payload — this applies to
`.with_exposed_udp_ports(...)` just as much as to a link. A larger one permanently
breaks the RECEIVING sandbox's entire inbound networking (every published port, not
just the oversized one) — an msb limitation, not something this crate can guard
against from the host side.

## Blocking public-internet access

`.with_network_disabled()` blocks a container's outbound access to the public
internet — microsandbox-only (`--net private`); **docker ignores this flag
entirely**, since there's no portable way to block egress on that backend while
keeping published ports reachable.

```rust,ignore
use rightsize::Container;

let sandboxed = Container::new("untrusted/plugin-runner:latest")
    .with_network_disabled()
    .with_exposed_ports(&[8080])
    .start()
    .await?;
```

On microsandbox, published ports keep serving inbound connections and outbound
connections to private (RFC 1918) address ranges keep working — only outbound
connections to the public internet fail.

Mutually exclusive with `.with_network(&net)` — a network-disabled container has
nothing to join a network with, so `start()` returns
`RightsizeError::NetworkDisabledConflict` if both are set on the same container.

## Alias resolution is registration-order-independent for readers

`Network::resolve` just checks "is any registered member carrying this alias" — it
doesn't care what order `resolve` calls happen in relative to `start()` calls, only
that the *target* container has already finished starting (and thus registering)
by the time you call `resolve` on its alias. Calling `resolve` for an alias that
hasn't started yet returns an error naming the alias, not a hang.
