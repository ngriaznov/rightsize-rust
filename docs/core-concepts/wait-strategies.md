# Wait Strategies

A `WaitStrategy` is a pluggable readiness check run after a container starts, before
`Container::start()` hands you the guard. Every built-in strategy shares one polling
loop (`poll_until_ready`): it probes readiness first, *then* checks the deadline — so
even a startup timeout shorter than one poll interval still gets at least one real
readiness check, rather than failing having never actually looked.

On timeout, every built-in strategy's error message includes the container's
`describe()` and its last 50 log lines, so a failure is diagnosable from the test
output alone without re-running anything.

## The four built-in strategies

### `Wait::for_listening_port()`

The default when `.waiting_for()` is never called. Ready when every exposed guest
port accepts a TCP connection *and* passes a read-probe (see below). Vacuously ready
immediately if the container exposes no ports.

### `Wait::for_http(path)`

```rust,ignore
Wait::for_http("/_api/version")
    .for_port(8529)          // defaults to the first exposed port if omitted
    .for_status_code(200)    // defaults to 200
```

Polls a GET to `path` until the response status matches. Built on a tiny hand-rolled
HTTP/1.1 client (core takes no HTTP-client dependency) — connect + read timeouts of
1s each, just enough to read a status line.

### `Wait::for_log_message(regex, times)`

```rust,ignore
Wait::for_log_message(".*database system is ready to accept connections.*", 2)
```

Ready once a log line matching `regex` has appeared at least `times` times.
`times = 0` means "ready immediately, no line ever required" — used as a bare
boot-wait when there's genuinely nothing else to check (see the msb network-link
integration tests, which use `Wait::for_log_message(".*", 0)` for a container whose
whole job is to sit idle as a network peer).

`regex` is matched with [`regex-lite`](https://docs.rs/regex-lite) — the same syntax
family as the `regex` crate, minus unicode tables, chosen because log-line matching
needs none of that. The match is a substring search anywhere in the line, not an
implicit whole-line anchor, so wrap a pattern in `.*` on either side unless you
deliberately want to anchor part of it — see
[`MariaDbContainer`](../modules/mariadb.md) for a worked example with both an anchor
and a literal escaped dot.

### `poll_until_ready` — the shared polling primitive

`rightsize::wait::poll_until_ready` is `pub`, not crate-private, precisely so a custom
`WaitStrategy` doesn't have to hand-roll its own deadline/poll-interval loop. Pass it
a `WaitTarget`, a timeout, a short description, and an async closure returning `bool`;
it returns `Ok(())` once the closure reports ready, or a timeout error with the same
log-tail-plus-`describe()` shape every built-in strategy produces.

## The read-probe story

The single highest-leverage correctness detail in this crate's wait strategies:
**a bare TCP connect is not proof of readiness**, on either backend.

Docker's userland proxy (and microsandbox's loopback forwarder) exist to forward
traffic — they accept a connection before the workload inside the container is
necessarily listening yet, because accepting a socket doesn't require anyone to be
home on the other end. A wait strategy that only checks "did `connect()` succeed"
can hand you a guard whose first real client request hits a socket that gets closed
out from under it.

`Wait::for_listening_port` (and internally, `for_http`) accounts for this with a
short read-probe immediately after connecting: it sets a 200ms read timeout and
attempts a zero-byte-or-more read.

- **EOF (the read returns 0 bytes), or a reset** — the peer closed the stream right
  after accepting. The hallmark of a proxy with nothing listening behind it yet.
  **Not ready** — keep polling.
- **Data arrives, or the read simply times out with the connection still open** —
  either a real server that speaks first, or one that's waiting for the client to
  speak first. Both are **ready**. A closed connection is the only failure signal;
  silence with the connection still open is not.

This is why a service that speaks first on connect (Redis) is fine with the plain
listening-port wait, while one that says nothing until spoken to and has a genuinely
protocol-level readiness question (Memcached) ships its own probe instead — see the
next section.

## Custom wait strategies via `poll_until_ready`

Implement `WaitStrategy` directly when the readiness signal genuinely needs to be
protocol-level — the workload accepts a connection immediately but isn't ready to
serve a real request. The shipped [`MemcachedContainer`](../modules/memcached.md) is
the worked example: Memcached logs nothing useful on startup and the port-forwarding
layer on either backend can bind the host port before the server inside is actually
accepting, so a bare TCP-connect wait can pass while the first real client connection
still gets a dead stream. Its custom strategy sends `version\r\n` and requires a
reply starting with `VERSION`:

```rust,ignore
use rightsize::wait::{WaitStrategy, WaitTarget};
use std::time::Duration;

struct MemcachedResponds;

#[async_trait::async_trait]
impl WaitStrategy for MemcachedResponds {
    async fn wait_until_ready(&self, target: &dyn WaitTarget) -> rightsize::Result<()> {
        let port = target.mapped_port(11211);
        let host = target.host().to_string();
        rightsize::wait::poll_until_ready(
            target,
            Duration::from_secs(60),
            "a VERSION reply",
            || {
                let host = host.clone();
                async move { probe_once(&host, port).await }
            },
        )
        .await
    }

    fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
        self // fixed poll budget; no override on this strategy
    }
}
```

`WaitTarget` is the minimal backend-agnostic view a strategy needs — `host()`,
`mapped_port(guest_port)`, `exposed_guest_ports()`, `current_logs()`, and
`describe()` — so a custom strategy never needs to know which backend booted the
container it's probing.

## Readiness caveats, summarized

- The read-probe caveat applies on **both** backends, not just microsandbox — it's a
  property of port-forwarding, not of microVMs specifically.
- `Wait::for_listening_port`'s read-probe is a good default but is not a substitute
  for a protocol-level check when the workload's own readiness is genuinely
  behind an extra step (auth handshake, schema load, replica election). Prefer
  `for_http`/`for_log_message`, or a custom strategy, in that case.
- `for_log_message`'s `times` parameter exists because some entrypoints print their
  "ready" line more than once during a normal boot (see
  [`PostgresContainer`](../modules/postgres.md), which restarts once internally and
  prints the same line both times) — waiting for the first occurrence can race a
  restart that hasn't finished yet.
