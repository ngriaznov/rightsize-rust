# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project intends to adhere to [Semantic Versioning](https://semver.org/) once it
reaches its first tagged release.

## [Unreleased]

## [0.1.0] - 2026-07-06

Initial public release.

### Added

- **Native Windows support** (`rightsize-msb`): `Platform::WindowsX64`/
  `WindowsArm64` (msb 0.6.3's `msb-windows-<arch>.exe` +
  `libkrunfw-windows-<arch>.dll`, checksum-verified against the pinned
  release the same as every other platform); the provisioner installs the
  binary as `bin\msb.exe` (a platform-derived basename, no longer the
  literal `"msb"` constant) and treats "exists and is a regular file" as the
  Windows equivalent of the POSIX executable-bit check, since Windows has no
  execute-bit concept; the default cache root is `%LOCALAPPDATA%\rightsize`
  on Windows (`RIGHTSIZE_CACHE_DIR` still overrides on every platform).
  `virtualization_available()` on Windows is attempt-and-report — an
  unusable Windows Hypervisor Platform (WHP) surfaces at msb's own first-boot
  failure, with the unsupported-backend error naming `msb doctor --fix` as
  the remedy. CI-verified on `windows-2025` hosted runners
  (`msb-windows` job in `.github/workflows/ci.yml`); WHP is already enabled
  there with no reboot required. No host-side process-supervision code
  changed — `std::process::Child::kill` is already cross-platform
  (`TerminateProcess` on Windows), and the crate had no POSIX-only signal
  handling to begin with.
- **Examples** (`rightsize-modules`): three runnable examples under
  `crates/rightsize-modules/examples/` — a plain-API Redis quickstart speaking RESP
  PING/PONG directly over a `TcpStream` (`cargo run -p rightsize-modules --example
  redis`), a PostgreSQL round-trip over `tokio-postgres` (`--example postgres`), and
  a two-container `Network` example with a consumer reaching a WireMock stub by
  alias (`--example network`). All three run on either backend via
  `RIGHTSIZE_BACKEND`.
- **Core** (`rightsize`): a Tokio-async-native, RAII-guard API — the `Container`
  builder and `ContainerGuard`, `Network` (alias-based connectivity on either
  backend), `Wait` (`for_listening_port` with a proxy-defeating read-probe,
  `for_http`, `for_log_message`), `MountableFile`, and the `SandboxBackend`/
  `BackendProvider` trait pair that lets alternative runtimes plug in without
  this crate depending on either. Two-tier cleanup: an explicit async
  `stop(self)` on the happy path, and a `Drop` fallback that hands teardown to a
  dedicated blocking-I/O cleanup thread — crash-tolerant, no async runtime
  required in the `Drop` path.
- **microsandbox backend** (`rightsize-msb`): a `SandboxBackend` implementation
  driving [microsandbox](https://github.com/superradcompany/microsandbox)
  (`msb`) as attached child processes — no Docker daemon required. Includes
  self-provisioning runtime download/install (SHA-256-verified, cross-process
  file lock, cached under `~/.cache/rightsize/`), a watchdog that works around
  `msb logs -f` never exiting on its own, and exec-tunnel network-alias
  emulation (a raw TCP-over-`exec --stream` byte pump) for cross-container
  connectivity between otherwise fully isolated microVMs.
- **Docker backend** (`rightsize-docker`): a from-scratch Docker daemon client
  over `tokio::net::UnixStream` — no `bollard`, no `hyper` — decoding chunked
  transfer encoding and the daemon's log-frame multiplexing format by hand.
  Serves as the correctness oracle the microVM backend is checked against, and
  the fallback runtime on platforms microVMs can't reach.
- **Modules** (`rightsize-modules`): eighteen preconfigured containers with
  sensible waits and connection helpers — `RedisContainer` (`uri`), `ArangoContainer`
  (`endpoint`, `with_root_password`), `MemcachedContainer` (`address`, a
  protocol-level `VERSION` probe instead of a bare port wait), `MongoDbContainer`
  (`connection_string`; single-node replica set, auto-initiated), `PostgresContainer`
  (`connection_string`; `with_username`/`with_password`/`with_database`),
  `MySqlContainer` (`connection_string`; readiness pinned to the real server's
  `port: 3306` log line, not the temp-boot or X-Plugin lines), `PinotContainer`
  (`controller_url`/`broker_url`; single-container QuickStart cluster;
  `with_memory_limit(4096)` default, measured against the image's own `-Xmx4G`
  heap request), `RedpandaContainer` (`bootstrap_servers`, `schema_registry_url`),
  `KafkaContainer` (`bootstrap_servers`; KRaft single node),
  `SpringCloudConfigContainer` (`uri`), `RabbitMqContainer` (`amqp_url`;
  management plugin enabled; `with_username`/`with_password`), `MariaDbContainer`
  (`connection_string`; readiness pinned to the real server's `port: 3306` log
  line, following `MySqlContainer`'s precedent), `WireMockContainer` (`base_url`,
  `admin_url`; health-checked on `/__admin/health`), `ClickHouseContainer`
  (`http_url`; HTTP-interface query helpers, health-checked on `/ping`),
  `KeycloakContainer` (`auth_server_url`, `management_url`;
  `KC_BOOTSTRAP_ADMIN_USERNAME`/`PASSWORD` — the 26.x env names, not the legacy
  `KEYCLOAK_ADMIN`; health-checked on `/health` on the management port 9000, not
  8080; `with_memory_limit(1024)` default), `Neo4jContainer` (`http_url`,
  `bolt_url`; HTTP Cypher transaction endpoint, no bolt driver dependency needed;
  readiness pinned to the real server's `Started.` log line; `with_memory_limit(1024)`
  default — the image refuses to start under msb's default microVM RAM budget with
  an explicit memory-configuration error, not an OOM kill), `FlociContainer`
  (`aws`/`azure`/`gcp` factory functions presetting image and port; `endpoint_url`;
  health-checked on `/health` — the AWS variant's LocalStack-compatible
  `/_localstack/health` does not carry over to the Azure/GCP variants, but plain
  `/health` answers `200` on all three; unsigned REST, no AWS SDK dependency
  needed for the S3-shaped surface; tiny native-Quarkus images, no memory override
  needed on either backend), and `FlinkContainer` (`rest_url`; `with_task_manager()`
  adds a companion TaskManager on a shared network for a real session cluster with
  task slots — **docker only**: returns `Err(RightsizeError::UnsupportedByBackend)`
  on the microsandbox backend, because msb's network-link emulation requires
  `nc`/busybox inside the consumer image and the official Flink image has neither;
  a bare JobManager, `REST /overview` only, is fully supported on both backends;
  `with_memory_limit(1024)` default on both roles).
- **API ergonomics**: `Container::waiting_for` now takes `impl WaitStrategy +
  'static` instead of `Box<dyn WaitStrategy>` — every built-in `Wait` factory
  still returns a boxed strategy (a blanket `impl WaitStrategy for Box<dyn
  WaitStrategy>` forwards to the inner strategy), so callers of any built-in
  strategy or a custom one now write `.waiting_for(my_strategy)` directly instead
  of `.waiting_for(Box::new(my_strategy))`. Pre-publish signature change with no
  external consumers affected.
- **Workspace scaffolding**: MSRV 1.85 (2024 edition), `cargo-llvm-cov` coverage
  floor on the core crate (80% line), CI matrix across microsandbox (Linux+KVM,
  Apple Silicon macOS) and Docker-fallback lanes, `#![forbid(unsafe_code)]` and
  `#![warn(missing_docs)]` on every crate.

### Changed

- **Documentation moved from `book/src/` to `docs/`.** `book.toml` (moved to the
  repo root) points `src` at `docs` and builds to `book-build/` (gitignored)
  instead of `book/book/`, so the built HTML never lands inside `docs/` and
  `mdbook build` now runs from the repo root rather than `mdbook build book`. No doc content
  or URL paths changed.
- **Pinned microsandbox runtime bumped from 0.6.2 to 0.6.3** (`rightsize-msb`).
  The provisioner downloads and SHA-256-verifies the new release on first use
  (existing `0.6.2` caches are left in place and simply stop being used). The
  full integration matrix passes unchanged on both backends against 0.6.3, and
  the behaviors the msb backend compensates for were re-verified as still
  present: detached `msb run` still never starts the image ENTRYPOINT, and
  `msb logs -f` still never exits after its sandbox stops.
- **Wait-pattern matching** (`rightsize`): `Wait::for_log_message` now runs on
  [`regex-lite`](https://docs.rs/regex-lite) (MSRV 1.65) instead of a hand-rolled
  `.`/`.*`/literal substring matcher. Same syntax family as the `regex` crate, no
  unicode tables — log-line matching needs none of that. `Neo4jContainer`'s wait
  pattern is back to the properly escaped `.*Started\..*`; `MariaDbContainer`'s
  custom `WaitStrategy` (a hand-written port/distribution-marker scan, needed only
  because the old matcher had no escape support) is gone, replaced by the ordinary
  `Wait::for_log_message` path with the standard anchored readiness pattern.
  `MySqlContainer`'s custom `WaitStrategy` is untouched — it was never built on the
  old matcher.
- **JSON** (`rightsize-docker`, `rightsize-msb`): the hand-rolled JSON
  encoder/tolerant field-extractor in `rightsize-docker` and the hand-rolled
  brace-scanning parser in `rightsize-msb` are both replaced by `serde`/
  `serde_json` — derived structs for the Docker Engine API request/response
  shapes and `msb ls --format json`'s output. Public behavior is unchanged; the
  Docker backend's unix-socket-only HTTP transport (see
  [Backends](docs/backends.md#unix-socket-only-and-why)) is untouched — only
  its JSON layer moved off hand-rolled parsing.

### Fixed

- **The backend self-heals msb's image-cache race** (`rightsize-msb`). Concurrent
  pulls of images sharing base layers can corrupt msb's image cache — the losing
  pull reads a layer tarball the winner's cleanup already deleted, and every
  later boot of that image fails with `cache error at .../layers/<sha>.tar.gz:
  No such file or directory`. A boot that fails with that signature now removes
  the affected image from msb's cache (`msb image remove`, scoped to the one
  reference) and retries the boot exactly once; any other failure, or a second
  failure after the heal, propagates unchanged.
- **`follow_logs` delivers workload output on Windows** (`rightsize-msb`). msb's
  `logs -f` on Windows stays alive but never relays lines while the sandbox
  runs, so followed output was silent until the sandbox stopped. On Windows the
  follow channel is now a polling follower over `msb logs` snapshots: one worker
  thread issues sequential msb invocations, treats a failed invocation as
  no-signal (never as content, never as "stopped"), and once the sandbox is
  gone delivers the terminal tail exactly once — including a final line with no
  trailing newline. The contract case covering that final unterminated line
  runs on Windows again instead of being gated out there.
- **`MySqlContainer` readiness gets a 120-second budget** (`rightsize-modules`).
  MySQL's first boot initializes the datafiles and boots mysqld twice (a temp
  server for init scripts, then the real one); the previous 60-second budget
  held on fast hosts but a loaded Windows CI runner overruns it. The strategy
  also honors `with_startup_timeout` now instead of silently ignoring it.
- **Provisioner downloads over 10 MiB no longer fail** (`rightsize-msb`). ureq's
  `read_to_vec` defaults to a 10 MiB body cap, and both real release assets
  (`msb`, `libkrunfw`) are ~25 MiB — so any genuine download aborted with "the
  response body is larger than request limit". The cap is now an explicit
  256 MiB. Latent since the crate's creation: on machines with a pre-existing
  `~/.cache/rightsize` install, the provisioner found the cached binary and
  never exercised a real download, which is why this only surfaced with the
  0.6.3 pin bump.

[Unreleased]: https://github.com/ngriaznov/rightsize-rust/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ngriaznov/rightsize-rust/releases/tag/v0.1.0
