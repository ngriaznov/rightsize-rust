# How It Works

**Architecture, one paragraph:** a Cargo workspace with four member crates.
`rightsize` (core) owns the public API — `Container` builder + RAII `ContainerGuard`,
`Network`, `Wait` strategies, `FreePorts`, `RunId`, the `SandboxBackend` trait +
`BackendProvider` registry, and the error enum — and depends on no backend.
`rightsize-msb` drives the pinned `msb` CLI as attached child processes, provisions
the toolchain from GitHub releases, and emulates networking with TCP-over-`exec
--stream` tunnels. `rightsize-docker` is a from-scratch Docker HTTP client over a
unix domain socket (unix) or Docker Desktop's named pipe (Windows), and the
correctness oracle the microVM backend is checked against. `rightsize-modules` ships the preconfigured containers covered in
[Modules](./modules/index.md). Host ports are pre-allocated core-side — **backends
bind, never allocate** — because `msb` only supports static `host:guest` maps; this
one invariant is what lets advertised-listener modules like
[`RedpandaContainer`](./modules/redpanda.md) work in a single boot attempt instead of
a restart dance.

## One trait, two runtimes

`SandboxBackend` is a small `async_trait` — `create`, `start`, `stop`, `remove`,
`exec`, `logs`, `follow_logs`, `ensure_network`, `remove_network`,
`install_network_links`, plus a synchronous `cleanup_sync` for the `Drop`-path
cleanup thread (see [Containers & Guards](./core-concepts/containers-and-guards.md)).
Every method takes a `&dyn SandboxHandle` — an opaque per-container handle a backend
issues from `create()` and looks up its own mutable state by, rather than the core
ever needing to know what shape that state takes.

```rust,ignore
#[async_trait::async_trait]
pub trait SandboxBackend: Send + Sync {
    fn name(&self) -> &str;
    fn supports_native_networks(&self) -> bool;
    async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>>;
    async fn start(&self, handle: &dyn SandboxHandle) -> Result<()>;
    async fn stop(&self, handle: &dyn SandboxHandle) -> Result<()>;
    async fn remove(&self, handle: &dyn SandboxHandle) -> Result<()>;
    async fn exec(&self, handle: &dyn SandboxHandle, cmd: &[String]) -> Result<ExecResult>;
    async fn logs(&self, handle: &dyn SandboxHandle) -> Result<String>;
    async fn follow_logs(&self, handle: &dyn SandboxHandle, consumer: Box<dyn Fn(String) + Send + Sync>) -> Result<FollowHandle>;
    async fn ensure_network(&self, network_id: &str) -> Result<()>;
    async fn remove_network(&self, network_id: &str) -> Result<()>;
    async fn install_network_links(&self, handle: &dyn SandboxHandle, links: &[NetworkLink]) -> Result<()>;
    fn cleanup_sync(&self, container_id: &str);
}
```

A shared contract test suite (`crates/rightsize-modules/tests/contract.rs`) exercises
this trait's observable behavior against *both* real backends — the Docker backend
serves as the correctness oracle the microVM backend is checked against, since Docker's
behavior here is the well-understood baseline. Every test you write against
`Container` runs against whichever backend resolves at runtime, unchanged — that's
the entire point of the trait boundary.

## Ports before boot

`ContainerSpec.ports` arrives at a backend with host ports **already chosen** — see
[`ContainerSpec`](./core-concepts/containers-and-guards.md). This ordering is load
-bearing, not incidental: `msb` only supports a fixed `host:guest` port map declared
at launch, so a backend that tried to allocate its own port and report it back
*after* boot couldn't support it at all. Docker could allocate its own ports (the
daemon supports it), but doing so consistently across both backends would mean two
different behaviors for the same builder call. Instead, `Container::start()`
allocates from a process-wide `FreePorts` pool before `create()` is ever called, and
every backend just binds what it's given.

This is also what makes advertised-listener rewrites — the
[`RedpandaContainer`](./modules/redpanda.md)/[`KafkaContainer`](./modules/kafka.md)
`with_spec_customizer` trick — a single-boot operation rather than a "boot, discover
the port, restart advertising the right thing" dance: the mapped host port is known
before `create()` runs, so the customizer can bake it into the boot command on the
first and only attempt.

The allocate-then-bind gap is a real, if narrow, race (something else grabs the port
between allocation and the backend's own bind call) — `Container::start()` retries up
to 5 times with fresh ports specifically to absorb it; see
[Containers & Guards](./core-concepts/containers-and-guards.md#the-raii-lifecycle).

## Exec-tunnels: the microVM networking workaround

Showcased in full in [Networking](./core-concepts/networking.md), the mechanism in
one paragraph: microsandbox microVMs share no bridge network with each other, and
`exec --stream` is the *only* guest data path available on this msb build — no
sandbox→host TCP under any net-rule tried, SSH forwarding found broken too. So a
network link between two containers is emulated as an `/etc/hosts` entry (the alias
resolves to `127.0.0.1` inside the consuming guest) plus a raw, unbuffered,
flush-per-read byte pump tunneled over `exec --stream`, backed by a respawned
`nc -l` listener in the target's guest. The pump can't rely on the proxied socket's
TCP close propagating (msb's port-publish proxy doesn't forward it), so
end-of-exchange is inferred from an idle window that only starts counting *after*
the first byte arrives — scoping it to the whole connection instead would wrongly
truncate any target slower than the idle window to respond its first byte.

```text
consumer guest  --exec --stream-->  msb host process  --raw TCP-->  target's published port
      ^                                                                      |
      |  nc -l (respawned per connection)                                   |
      +----------------------------------------------------------------------+
```

This is genuinely narrower than Docker's native bridge networking — one connection
at a time, client-speaks-first only, `nc`/busybox required in the consumer image —
not a temporary limitation waiting to be lifted, but a real trade against "no
persistent daemon, hardware-isolated microVMs." See
[Backend differences](./backends.md#backend-differences) for the complete list.

## Binding decisions

- **Tokio async-native, no async `Drop`.** The public surface is `async fn`
  throughout; cleanup is two-tier — explicit `stop()`, or a synchronous `Drop`
  fallback via a dedicated cleanup thread — because Rust has no async `Drop`. Full
  story in [Containers & Guards](./core-concepts/containers-and-guards.md).
- **RAII guards only.** `container.start().await` returns a guard; there is no
  test-framework extension layered on top. The shared-container pattern (JUnit
  `@Container` static-scope equivalent) is a documented recipe using
  `tokio::sync::OnceCell`, not API surface — see
  [Getting Started](./getting-started.md#the-shared-container-recipe).
- **Idiomatic Rust naming**, not a mechanical port of a Testcontainers-style API —
  `snake_case`, builder-style `with_*` methods, `Container::new("image")`.
- **Hand-rolled Docker client** over a unix domain socket (unix) or Docker Desktop's
  named pipe (Windows) — no `bollard`, no `hyper` — so this crate's dependency tree
  can't be the reason a Docker client gets misrouted onto TCP by an unrelated
  dependency bump elsewhere in a consumer's tree.
  See [Backends](./backends.md#hand-rolled-transport-and-why).

## Where to look in the source

If you want to read the implementation directly rather than take this book's word
for it, start with these files:

| Concern | File |
|---|---|
| `Container`/`ContainerGuard`, two-tier cleanup, port retry | `crates/rightsize/src/container.rs` |
| Wait strategies, the read-probe | `crates/rightsize/src/wait.rs` |
| `Network`, alias resolution | `crates/rightsize/src/network.rs` |
| Backend resolution | `crates/rightsize/src/backends.rs` |
| The `Drop`-path cleanup thread | `crates/rightsize/src/cleanup.rs` |
| msb provisioning | `crates/rightsize-msb/src/provisioner.rs` |
| Exec-tunnel networking | `crates/rightsize-msb/src/exec_tunnel.rs` |
| Docker unix-socket client | `crates/rightsize-docker/src/client.rs` |
| Shared contract suite (both backends) | `crates/rightsize-modules/tests/contract.rs` |
