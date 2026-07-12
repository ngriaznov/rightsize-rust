# Failure Diagnostics

When a test fails because a container never became ready, the useful debugging
information — what's actually running, on what port, what did it log — normally has
to be reconstructed by hand after the fact. Failure diagnostics surfaces it
automatically instead.

## The report

`rightsize::diagnostics()` returns a human-readable snapshot of every container this
process currently has running, built from a process-local registry updated on every
successful `start()` and `stop()`:

```rust,ignore
use rightsize::diagnostics;

let report = diagnostics().await;
eprintln!("{report}");
```

Sample output, two containers:

```text
== rightsize diagnostics: 2 running container(s) ==
-- rz-ab12cd34-redis (redis:7-alpine) --
state: running   host: 127.0.0.1   ports: 6379->49213
last 50 log lines:
  1:M 01 Jan 2026 00:00:00.000 * Ready to accept connections tcp
-- rz-ab12cd34-postgres (postgres:16-alpine) --
state: running   host: 127.0.0.1   ports: 5432->49214
last 50 log lines:
  database system is ready to accept connections
```

Nothing running:

```text
== rightsize diagnostics: no running containers ==
```

Each container's log tail is its own `logs` call's last 50 lines, indented two
spaces. A `logs` call that fails degrades that one container's section to a single
line instead of failing the whole report:

```text
-- rz-ab12cd34-redis (redis:7-alpine) --
state: running   host: 127.0.0.1   ports: 6379->49213
logs: unavailable (msb exec timed out after 5s)
```

This exact format — the header line, the count, the per-container blocks — is a
cross-language contract: the same report renders identically whether it's produced
by this crate, the Kotlin port, or the Node port, so a screenshot or a pasted report
from any of them reads the same way.

## The registry

A container is registered the moment `start()` produces a `ContainerGuard` — even if
a readiness wait strategy fails right afterward, since "the container booted but
never became ready" is exactly the failure this feature exists to explain — and
deregistered on `stop()` or `Drop`. It's entirely in-process: no file, no
cross-process visibility, no interaction with the [reaping](./reaping.md) ledger.
Its only job is answering "what is *this* process's own run doing right now."

## Automatic hook: `DiagnosticsGuard`

Wiring `diagnostics()` into every test by hand is exactly the kind of thing worth
automating. `DiagnosticsGuard` does it: construct one at the top of a test, and its
`Drop` prints the report to stderr — but only if the thread is unwinding from a
panic. A passing test prints nothing.

```rust,ignore
use rightsize::{Container, DiagnosticsGuard};

#[tokio::test]
async fn redis_round_trips_a_value() {
    let _diagnostics = DiagnosticsGuard::new();

    let guard = Container::new("redis:7-alpine")
        .with_exposed_ports(&[6379])
        .start()
        .await
        .unwrap();

    // ... assertions that might panic ...

    guard.stop().await.unwrap();
}
```

If an assertion after `start()` panics, the report — including this container's
last 50 log lines — lands on stderr right next to the panic message, without an
extra round trip to reproduce the failure locally.

`DiagnosticsGuard`'s `Drop` cannot itself be `async`, and may run from inside a
`#[tokio::test]`'s own runtime while that runtime is mid-unwind — nesting another
`block_on` there would panic. It sidesteps that by fetching the report on a fresh,
throwaway OS thread (the same trick `crate::cleanup`'s own background thread uses)
and joining it synchronously, so it's safe to use from any test regardless of the
Tokio flavor it runs under.
