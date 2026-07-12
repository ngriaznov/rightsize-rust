# rightsize-rust

[![CI](https://github.com/ngriaznov/rightsize-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/ngriaznov/rightsize-rust/actions/workflows/ci.yml)

**Testcontainers-style integration testing on microVMs. No Docker required.**

rightsize-rust runs your integration-test containers as hardware-isolated
[microsandbox](https://github.com/superradcompany/microsandbox) microVMs — one
microVM per container — behind a Tokio-async-native, RAII-guard Rust API. The
runtime self-provisions on first use (one Cargo dependency, zero install steps),
and a hand-rolled Docker backend covers the platforms microVMs can't reach.

```rust
use rightsize_modules::RedisContainer;

#[tokio::test]
async fn orders_flow_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
    let redis = RedisContainer::new().start().await?;   // boots a real microVM
    let client = redis::Client::open(redis.uri())?;     // redis://127.0.0.1:<mapped port>

    // ... your test ...

    redis.stop().await?;                                // explicit, ordered teardown
    Ok(())
}
```

The guard *is* the API: `start()` returns an RAII handle. Call `stop().await` for
explicit, ordered teardown, or just drop it — a dedicated cleanup thread reclaims
the container even if the test panics ([how it works](#how-it-works)).

## Why microVMs

|  | Docker + Testcontainers | rightsize-rust |
|---|---|---|
| Isolation | shared kernel (containers) | **hardware-level (microVM per container)** |
| Runtime install | Docker Desktop / daemon required | **none — self-provisions on first use** |
| Licensing | Docker Desktop licensing in orgs | Apache-2.0 all the way down |
| Async model | blocking client calls | **`async fn` throughout — no thread-per-container blocking** |
| Docker client | `bollard`/`shiplift` over `hyper` | **hand-rolled, unix-socket-only — can't be misrouted onto TCP** |

The Docker client's transport is hand-rolled by design: a small layer over
`tokio::net::UnixStream`, so a dependency bump elsewhere in your tree can never be
the reason a Docker call gets misrouted onto TCP — no `bollard`, no `hyper`, ever. The
JSON layer on top of it is ordinary `serde`/`serde_json`, which carries none of that
transport-routing risk.

## Quickstart

```toml
# Cargo.toml
[dev-dependencies]
rightsize = "0.2.0"
rightsize-modules = "0.2.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`rightsize-modules` pulls in both backend crates by default (the `backend-msb`
and `backend-docker` Cargo features); disable `default-features` and re-enable
one to trim the dependency you don't need.

No backend wiring: the feature-enabled backends (`backend-msb` and
`backend-docker`, both on by default) register themselves the first time any
module starts. On first test run, rightsize-rust downloads the pinned
microsandbox runtime (SHA-256-verified, from GitHub releases) into
`~/.cache/rightsize/` and boots your containers as microVMs — resolution picks
the best supported backend, honoring `RIGHTSIZE_BACKEND` when set. No daemon,
no root, no pre-installed anything.

The one wrinkle: resolution happens at the process's **first** `start()` and is
cached. If that first start is a plain `Container` rather than a module, make
one call first:

```rust
rightsize_modules::register_default_backends();
```

(Depending on `rightsize` and the backend crates directly, without
`rightsize-modules`? Register providers by hand via
`rightsize::backends::register_provider` — see the book.)

### Driving a container by hand

Skip the module helpers and use `Container` directly for any image:

```rust
use rightsize::{Container, Wait};

let arango = Container::new("arangodb:3.11")
    .with_env("ARANGO_NO_AUTH", "1")
    .with_exposed_ports(&[8529])
    .waiting_for(Wait::for_http("/_api/version").for_port(8529))
    .start()
    .await?;

let port = arango.get_mapped_port(8529)?;   // published on 127.0.0.1
// ... your test ...
arango.stop().await?;
```

### Sharing one container across tests

rightsize-rust has no JUnit-style static-scope annotation — the RAII guard is the
whole API surface. To share a container across many tests in one binary, boot it
once behind a `tokio::sync::OnceCell` and let process exit reclaim it:

```rust
use std::sync::Arc;
use tokio::sync::OnceCell;
use rightsize_modules::{RedisContainer, RedisGuard};

static REDIS: OnceCell<Arc<RedisGuard>> = OnceCell::const_new();

async fn shared_redis() -> Arc<RedisGuard> {
    REDIS
        .get_or_init(|| async { Arc::new(RedisContainer::new().start().await.unwrap()) })
        .await
        .clone()
}

#[tokio::test]
async fn first_test_using_redis() {
    let redis = shared_redis().await;   // booted once
    // ... use redis.uri() ...
}
```

## Backends

rightsize-rust picks a backend automatically; override with
`RIGHTSIZE_BACKEND=microsandbox|docker`.

| Platform | Backend used |
|---|---|
| macOS (Apple Silicon) | microsandbox (microVMs) |
| Linux x86_64 / arm64 with `/dev/kvm` | microsandbox (microVMs) |
| Windows x86_64 / arm64 with WHP enabled | microsandbox (microVMs)¹ |
| Intel Mac | Docker (auto-fallback) |
| Windows without WHP | Docker (auto-fallback) |
| Linux without KVM | Docker (auto-fallback) |

¹ Windows support runs on the Windows Hypervisor Platform (WHP) and is upstream
beta. CI-verified on `windows-2022`/`windows-2025` hosted runners, where WHP is
already enabled with no reboot required. If WHP isn't enabled on your machine,
`RIGHTSIZE_BACKEND=microsandbox` fails naming the precondition (run
`msb doctor --fix` in an elevated terminal — this may require a reboot); leaving
`RIGHTSIZE_BACKEND` unset falls back to Docker silently.

Both backends satisfy one behavioral contract (`SandboxBackend`), verified by a
shared test suite — the tests you write run unchanged on either. A few edges are
backend-specific rather than behavioral divergences:

- **Network-alias tunnels on microsandbox have real limits** versus Docker's
  native bridge networking — see [Networking](#networking).
- **Read-only file mounts aren't enforced in-guest on microsandbox 0.6.2.**
  `FileMount::read_only` is honored by Docker; on microsandbox the guest currently
  gets a writable mount regardless. Don't rely on guest-side write protection
  under `RIGHTSIZE_BACKEND=microsandbox`.
- **`follow_output` delivers the same ordered, no-duplicate log stream on both
  backends**, but on microsandbox the final tail can arrive shortly after the
  sandbox reports stopped, rather than exactly at stream EOF (`msb logs -f`
  doesn't close on sandbox stop in 0.6.2, so the backend replays the
  not-yet-delivered tail once stop is confirmed).

## Modules

`rightsize-modules` ships eighteen preconfigured containers with sensible waits
and connection helpers. Each is a thin newtype wrapping `Container`, so its guard
exposes typed accessors while the core builders (`with_env`, `waiting_for`, …)
remain available.

| Module | Helpers |
|---|---|
| `RedisContainer` | `uri()` |
| `MemcachedContainer` | `address()` — protocol-level `VERSION` probe, not a bare port wait |
| `ArangoContainer` | `endpoint()`; `with_root_password(…)` to enable auth (default: no-auth) |
| `MongoDbContainer` | `connection_string()` — single-node replica set, auto-initiated |
| `PostgresContainer` | `connection_string()`, `username()`, `password()`, `database_name()`; `with_username`/`with_password`/`with_database(…)` |
| `MySqlContainer` | `connection_string()`, `username()`, `password()`, `database_name()`; `with_username`/`with_password`/`with_database(…)` |
| `MariaDbContainer` | `connection_string()`, `username()`, `password()`, `database_name()`; `with_username`/`with_password`/`with_database(…)` |
| `ClickHouseContainer` | `http_url()`, `username()`, `password()`, `database_name()`; `with_username`/`with_password`/`with_database(…)` |
| `Neo4jContainer` | `http_url()`, `bolt_url()`, `username()`, `password()`; `with_password(…)` — HTTP Cypher endpoint (username fixed at `neo4j`) |
| `RedpandaContainer` | `bootstrap_servers()`, `schema_registry_url()` |
| `KafkaContainer` | `bootstrap_servers()` — KRaft single node |
| `RabbitMqContainer` | `amqp_url()`, `management_url()`, `username()`, `password()`; `with_username`/`with_password(…)` |
| `PinotContainer` | `controller_url()`, `broker_url()` — single-container QuickStart cluster |
| `WireMockContainer` | `base_url()`, `admin_url()` — stub via the `/__admin` API |
| `KeycloakContainer` | `auth_server_url()`, `management_url()`, `admin_username()`, `admin_password()`; `with_admin_username`/`with_admin_password(…)` |
| `SpringCloudConfigContainer` | `uri()` |
| `FlociContainer` | `endpoint_url()`; `FlociContainer::aws()`/`azure()`/`gcp()` factories — [floci.io](https://floci.io) cloud emulators (unsigned REST, no SDK needed) |
| `FlinkContainer` | `rest_url()`; `with_task_manager()` for a full session cluster — **Docker only**¹ |

Heavyweight JVM images raise their own memory floor via `with_memory_limit` —
SpringCloudConfig, Keycloak, Neo4j and Flink (1024 MB), Pinot's four-JVM cluster
(4096 MB). That's baked into the module; you don't set it. Each module's rustdoc
documents its exact image tag, wait strategy, and the reasoning behind those
choices — the [module chapter of the book](docs/modules/index.md) collects
the worked examples.

¹ `with_task_manager()` returns a `Result`: on microsandbox it errs with
`RightsizeError::UnsupportedByBackend` (the Flink image carries no `nc`/busybox
for the network-link emulation — see [Networking](#networking)), naming the
docker backend as the remedy. A bare JobManager (`rest_url()` only) runs on both.

## Networking

`Network` gives containers alias-based connectivity on both backends:

```rust
use rightsize::{Container, Network};
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

`Network::resolve(alias, port)` returns `alias:port` on **both** backends. On
Docker that's a native network alias. On microsandbox — where microVMs are fully
isolated from each other — rightsize-rust transparently installs an `/etc/hosts`
entry plus a TCP relay tunneled over the sandbox's exec channel.

The microVM emulation has limits worth knowing: start dependencies before their
consumers, one connection at a time per tunnel (fine for config fetches; not for
a cross-container Kafka consumer), and the consumer image needs `nc`/busybox.
Violations fail fast with an actionable error.

## How it works

- **Self-provisioning runtime.** A pinned `msb` release (binary + libkrunfw) is
  downloaded once, SHA-256-verified against the release manifest, and installed
  atomically under `~/.cache/rightsize/` — the binary lands last, so a crashed
  install is detected and repaired, never half-trusted. A cross-process lock
  keeps parallel test binaries from racing.
- **Attached-mode supervision.** Each container is a held child process
  supervising its microVM; the image's `ENTRYPOINT` runs exactly as it would
  under Docker.
- **Pre-allocated ports.** Host ports are chosen before boot, so brokers like
  Redpanda/Kafka get their advertised listeners baked in — no restart dance. A
  backend binds the ports it's given; it never allocates its own.
- **One trait, two runtimes.** `SandboxBackend` is a small `async_trait`; the
  shared contract suite is the referee, with the Docker backend doubling as the
  correctness oracle for the microVM backend.
- **Two-tier cleanup, no async `Drop`.** The happy path is an explicit, awaited
  `guard.stop().await`. The fallback — a guard simply dropped, a test panicking
  mid-body — hands a small teardown descriptor to a dedicated background OS thread
  that tears the container down with blocking I/O only, no Tokio required. Orphan
  reaping (below) is the backstop for the backstop, cleaning up whatever a prior
  process's hard `SIGKILL` left behind.
- **Orphan reaping.** A run-record ledger under the rightsize cache dir, an
  init-time sweep, and a per-run watchdog (default on) reap sandboxes a crashed
  process (`SIGKILL`, OOM-kill, a killed CI step) left behind — liveness-aware, so
  it's safe across concurrent runs, unlike a bare name-prefix scan. Controlled by
  `RIGHTSIZE_REAPER=on|sweep|off`. See [Orphan Reaping](docs/reaping.md).
- **Container reuse.** `.reuse(true)` combined with `RIGHTSIZE_REUSE=true`/`1`
  marks a sandbox to survive process exit and be ADOPTED — not rebuilt — by a
  later, spec-identical `start()`. Identity is a `sha256` over a canonical
  serialization of the reuse-relevant spec, pinned as a cross-language test
  vector so the same spec hashes identically here and in the Kotlin/Node ports.
  See [Container Reuse](docs/reuse.md).
- **Failure diagnostics.** `rightsize::diagnostics()` — and the automatic
  `DiagnosticsGuard` hook, which prints on panic and stays silent otherwise —
  renders every container this process has running, its state, ports, and last
  50 log lines, in a report format pinned identically across the Kotlin/Node
  ports. See [Failure Diagnostics](docs/diagnostics.md).
- **Isolation requirement.** `.require_isolation(true)` refuses to start on a
  backend that doesn't provide hardware isolation, instead of silently running
  an untrusted workload wherever `RIGHTSIZE_BACKEND` happened to resolve. See
  [Isolation Requirement](docs/isolation.md).
- **Checkpoint / restore (docker only).** `guard.checkpoint()` commits a running
  container's filesystem to an image; `Container::from_checkpoint(&cp)` restores
  as many fresh containers from it as needed — boot once, seed once, restore per
  test instead of re-seeding. See [Checkpoint / Restore](docs/checkpoints.md).
- **Cross-language parity.** The `SandboxBackend` contract above — lifecycle,
  networking, reaping, reuse, diagnostics, isolation, checkpoints — is verified
  identically in the Kotlin and TypeScript ports of this library by each port's
  own copy of the shared contract suite. See
  [Cross-Language Parity](docs/parity.md).

## Configuration

| Env var | Effect |
|---|---|
| `RIGHTSIZE_BACKEND` | Force `microsandbox` or `docker` |
| `MSB_PATH` | Use a pre-installed `msb` binary; skip downloads |
| `RIGHTSIZE_CACHE_DIR` | Relocate the runtime cache (default `~/.cache/rightsize`, `%LOCALAPPDATA%\rightsize` on Windows) |
| `RIGHTSIZE_MSB_SKIP_DOWNLOAD` | `true` = fail instead of downloading (air-gapped CI) |
| `RIGHTSIZE_REAPER` | `on` (default) / `sweep` / `off` — see [Orphan Reaping](docs/reaping.md) |
| `RIGHTSIZE_REUSE` | `true` or `1` enables the reuse half of `.reuse(true)`'s double opt-in — see [Container Reuse](docs/reuse.md) |

## Examples

Runnable, minimally-commented examples live under
[`crates/rightsize-modules/examples/`](crates/rightsize-modules/examples), covering
the plain API, a real client round-trip, and multi-container networking:

```bash
cargo run -p rightsize-modules --example redis      # RAII guard: start -> RESP PING/PONG -> stop
cargo run -p rightsize-modules --example postgres   # RAII guard: start -> tokio-postgres round-trip -> stop
cargo run -p rightsize-modules --example network    # two containers on a Network, reached by alias
```

Like everything else in this repo, they run on either backend — force one with
`RIGHTSIZE_BACKEND=microsandbox|docker` prefixed on any of the commands above.

## Development

MSRV is **1.85** (2024 edition), pinned in `rust-toolchain.toml`.

```bash
cargo test --workspace                                       # unit tests, offline, no backend
cargo llvm-cov --package rightsize --lib --fail-under-lines 80   # core coverage floor

# Integration tests boot real containers; they live behind the `sandbox-it`
# feature (not #[ignore]), and you force a backend to pick a lane:
RIGHTSIZE_BACKEND=microsandbox cargo test --workspace --features sandbox-it   # needs Apple Silicon or Linux + /dev/kvm
RIGHTSIZE_BACKEND=docker       cargo test --workspace --features sandbox-it   # needs a reachable Docker daemon
```

CI runs the full matrix (`unit`, `msb-linux`, `msb-macos`, `docker-fallback`).
A change to a `SandboxBackend` implementation, or anything the shared contract
suite exercises, should run the `sandbox-it` suite against both backends before a
PR — see [CONTRIBUTING.md](.github/CONTRIBUTING.md).

## Documentation

The book is at
**[ngriaznov.github.io/rightsize-rust](https://ngriaznov.github.io/rightsize-rust/)**
(source under `docs/`, built with mdBook). It covers getting started, core concepts
(containers & guards, wait strategies, networking, files & resources), backends,
every module, and the internals — all its samples are machine-compile-verified.

## License

[Apache-2.0](LICENSE)
