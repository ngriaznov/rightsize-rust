# Introduction

rightsize-rust runs your integration-test containers as hardware-isolated
[microsandbox](https://github.com/superradcompany/microsandbox) microVMs instead of
Docker containers. It gives you a Testcontainers-shaped workflow — boot a real
database/broker/service image, get back a handle with a mapped port, run your test,
tear it down — but the isolation boundary underneath is a microVM, not a shared-kernel
container, and there's no daemon to install first.

## What it is

- A Tokio-async-native, RAII-guard Rust API: `Container::new("image").start().await`
  returns a guard; the guard's `Drop` (or an explicit `stop().await`) tears the
  container down.
- A self-provisioning runtime: the pinned [`msb`](https://github.com/superradcompany/microsandbox)
  binary downloads itself (SHA-256-verified) into `~/.cache/rightsize/` on first use.
  One Cargo dependency, zero install steps, no root.
- A hand-rolled Docker fallback for platforms microVMs can't reach — Intel Macs,
  Windows, Linux without `/dev/kvm` — talking to the daemon over a plain
  `tokio::net::UnixStream`, never `bollard`/`hyper`.
- Twenty-one preconfigured containers (`rightsize-modules`) for common test
  dependencies — see the [Modules index](./modules/index.md) for the full list.

```rust,ignore
use rightsize_modules::RedisContainer;

#[tokio::test]
async fn orders_flow_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
    let redis = RedisContainer::new().start().await?;   // boots a real microVM
    let client = redis::Client::open(redis.uri())?;     // 127.0.0.1:<mapped port>

    // ... your test ...

    redis.stop().await?;                                // explicit, ordered teardown
    Ok(())
}
```

Drop the guard instead of calling `stop()` and it still tears down — a dedicated
cleanup thread reclaims it even if the test panics. See
[Containers & Guards](./core-concepts/containers-and-guards.md) for the full story.

## Why a microVM instead of a container

| | Docker + Testcontainers | rightsize-rust |
|---|---|---|
| Isolation | shared kernel (containers) | **hardware-level (microVM per container)** |
| Runtime install | Docker Desktop / daemon required | **none — self-provisions on first use** |
| Licensing | Docker Desktop licensing in orgs | Apache-2.0 all the way down |
| Async model | blocking client calls | **`async fn` throughout — no thread-per-container blocking** |
| Docker client (fallback path) | `bollard`/`shiplift` over `hyper` | **hand-rolled, unix-socket-only — can't be misrouted onto TCP** |

Testcontainers-style libraries run each dependency as a Docker container: same
kernel, same namespace machinery, one daemon coordinating everything. That's
fast and well-understood, but
it means every test process trusts the Docker daemon's isolation guarantees and needs
that daemon (or a compatible one) reachable at all. rightsize-rust's default backend
instead boots each container as its own **microVM** — a real, separate kernel per
container, supervised as an attached child process, with no persistent daemon at all.
The trade is a small amount of boot latency and a narrower feature surface (see
[Backend differences](./backends.md#backend-differences)) in exchange for isolation
that doesn't depend on kernel namespace correctness and a runtime that provisions
itself with no `sudo`.

## The honest platform matrix

rightsize-rust picks a backend automatically; override with
`RIGHTSIZE_BACKEND=microsandbox|docker`.

| Platform | Backend used |
|---|---|
| macOS (Apple Silicon) | microsandbox (microVMs) |
| Linux x86_64 / arm64 with `/dev/kvm` | microsandbox (microVMs) |
| Intel Mac | Docker (auto-fallback) |
| Windows | Docker (auto-fallback) |
| Linux without KVM | Docker (auto-fallback) |

Both backends satisfy one behavioral contract (`SandboxBackend`), verified by a
shared contract test suite — tests you write run unchanged on either. A handful of
edges are backend-specific rather than behavioral divergences; see
[Backend differences](./backends.md#backend-differences) before you hit one in the
wild. That same contract is verified identically in the Kotlin and TypeScript
ports of this library — see [Cross-Language Parity](./parity.md).

## Honest limits, up front

- **Not yet published to crates.io.** Depend on it via a git reference until a
  tagged release exists — see [Getting Started](./getting-started.md).
- **Twenty-one modules, not an exhaustive catalog.** Anything else is the plain
  `Container` API — a thin wrapper is a small, welcome contribution; see the
  [Modules index](./modules/index.md) for the full list.
- **No JUnit-style `@Container` test extension.** The RAII guard is the whole API
  surface; a shared-container test fixture is a documented recipe using
  `tokio::sync::OnceCell`, not a proc-macro annotation.
- **Network-alias emulation on microsandbox is deliberately narrow** — one
  connection at a time per tunnel, client-speaks-first only. See
  [Networking](./core-concepts/networking.md).

**Want to run something first?**
[`crates/rightsize-modules/examples/`](https://github.com/ngriaznov/rightsize-rust/tree/main/crates/rightsize-modules/examples)
in the repo has three runnable examples (RAII guard + RESP, RAII guard + a real
client round-trip, multi-container networking) — see the project README's
[Examples](https://github.com/ngriaznov/rightsize-rust#examples) section for the
exact `cargo run` commands.

This book expands on the [README](https://github.com/ngriaznov/rightsize-rust) — if
you've read the README already, skip ahead to whichever chapter covers what you need
next; nothing here duplicates it verbatim.
