# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project intends to adhere to [Semantic Versioning](https://semver.org/) once it
reaches its first tagged release.

## [Unreleased]

Nothing yet.

## [0.2.0] - 2026-07-12

### Added

- **Orphan reaping** (`rightsize`, `rightsize-msb`, `rightsize-docker`): a run-record
  ledger (`runs/<run-id>.json`/`.sandboxes`/`.networks` under the rightsize cache
  dir) tracks every sandbox/network this process creates, appended before create and
  removed after a successful stop — a crashed process (`SIGKILL`, OOM-kill, a killed
  CI step) leaves the ledger in place instead of silently losing track. An init-time
  sweep, run once per process right after backend resolution, reaps any OTHER run's
  ledger that is dead (a liveness check on the recorded pid + process start time,
  cross-language and cross-process by construction) and whose recorded backend
  matches its own. A per-run watchdog (default on) — a small detached script blocking
  on a non-inherited stdin pipe — reaps within seconds of a crash instead of waiting
  for the next process's sweep. `RIGHTSIZE_REAPER=on|sweep|off` controls both layers;
  see [Orphan Reaping](docs/reaping.md). The docker backend gets orphan-crash coverage
  for the first time — previously only msb had any sweep at all, and even that one was
  liveness-blind (unsafe for concurrent runs); the former `MsbCliBackend::sweep_orphans`
  is gone, replaced by this ledger-based sweep, which is liveness-aware and covers both
  backends uniformly. `ContainerSpec` gains a `keep_alive` field (defaults to `false`),
  excluding a `keep_alive` sandbox from every own-run cleanup path (the ledger, msb's
  `started_names`, docker's run-id label, the `Drop`-path cleanup thread) — see the
  container-reuse entry below for the feature that actually sets it. `SandboxBackend`
  gains `remove_by_name`, `watchdog_kill_command`, `watchdog_network_kill_command`, and
  `backend_binary_path` — the SPI surface this feature needs: a synchronous, name-keyed
  best-effort remove (the ledger persists names, not backend-native ids) and the
  external CLI commands the standalone watchdog script uses; msb promotes its former
  internal `silently_remove` helper. The rightsize cache dir helper moved to core
  (`rightsize::cache_dir`, same behavior — `RIGHTSIZE_CACHE_DIR` override, else the
  per-OS default) since the reaping ledger needs it even in docker-only processes,
  which never link `rightsize-msb` at all; `rightsize-msb`'s provisioner now delegates
  to it.
- **Container reuse** (`rightsize`, `rightsize-msb`, `rightsize-docker`):
  `Container::reuse(true)` marks a container to survive process exit and be ADOPTED
  — not re-created — by a later, spec-identical `start()`, in this process or a
  later one; `stop()` on a reuse-active guard then leaves the sandbox running
  instead of tearing it down. Gated by a double opt-in: the `.reuse(true)` marker
  AND `RIGHTSIZE_REUSE=true`/`1` in the process environment both required, or the
  container behaves exactly like an ordinary ephemeral one. Identity is a `sha256`
  over a canonical JSON serialization of the reuse-relevant spec
  (image/env/command/exposedPorts/memoryLimitMb/copied-file-contents) — a
  cross-language contract pinned by a fixed test vector, so the same logical spec
  hashes identically here and in the Kotlin/Node ports. The sandbox name is
  `rz-reuse-<first 12 hex chars of the hash>`; the registry lives at
  `<cacheDir>/reuse/<hash>.json`, written atomically once the reused container
  first starts and passes its wait strategy. `.reuse(true)` combined with
  `.with_network(...)` fails fast with a typed `RightsizeError::ReuseNetworkConflict`
  — reuse identity has no concept of network topology. `SandboxBackend` gains
  `find_running`, a best-effort "is a sandbox named X already running, and if so
  hand me a handle for it" query — the adopt path's own primitive, distinct from
  `remove_by_name`'s "make it gone" contract; both real backends implement it for
  real. `RightsizeError` gains a typed `NameConflict` variant (msb: an "already
  exists" `msb run` failure; docker: an HTTP 409 on `POST /containers/create`) for
  the reuse start flow's "another process won the create race" retry. See
  [Container Reuse](docs/reuse.md).
- **Failure diagnostics** (`rightsize`, `rightsize-msb`, `rightsize-docker`):
  `rightsize::diagnostics()` returns a human-readable snapshot of every container
  this process currently has running — image, state, host, mapped ports, and each
  one's last 50 log lines — built from a process-local registry updated on every
  successful `start()`/`stop()`. A failing `logs` call degrades that container's
  section to `logs: unavailable (<reason>)` instead of failing the whole report. The
  exact report format is a cross-language contract, pinned by a golden-fixture test.
  `DiagnosticsGuard` is the automatic hook: construct one at the top of a test, and
  its `Drop` prints the report to stderr iff the thread is unwinding from a panic —
  a passing test prints nothing. See [Failure Diagnostics](docs/diagnostics.md).
- **Isolation requirement** (`rightsize`, `rightsize-msb`, `rightsize-docker`):
  `SandboxBackend` gains `capabilities()`, a small `Capabilities { hardware_isolated,
  checkpoint }` struct (msb: `true`/`false`; docker: `false`/`true`) — deliberately
  separate from the existing `supports_native_networks` flag. `.require_isolation(true)`
  on a `Container` is checked in `start()`, before any create/network work: if the
  active backend's `capabilities().hardware_isolated` is `false`, `start()` returns a
  typed `RightsizeError::IsolationRequired` naming the active backend and the remedy
  (`RIGHTSIZE_BACKEND=microsandbox`) — no sandbox is created. See
  [Isolation Requirement](docs/isolation.md).
- **Checkpoint / restore** (`rightsize`, `rightsize-docker`): `SandboxBackend` gains
  `commit_to_image(handle, imageRef)`, defensively unsupported by default; the docker
  backend implements it via the engine's `POST /commit` endpoint. `ContainerGuard::
  checkpoint()` checks `capabilities().checkpoint` before any backend call — a typed
  `RightsizeError::CheckpointUnsupported` on microsandbox (no commit primitive; the
  generic layer never reaches the backend), naming the docker backend as the remedy
  — and requires the guard to currently be running. On success it returns a
  `Checkpoint { image_ref: "rightsize/checkpoint:<12 hex>", spec }` carrying the
  source container's full spec. `Container::from_checkpoint(&cp)` builds a normal
  container from the checkpoint's image plus its env/command/exposed-ports/memory-
  limit (mounts, network, and aliases are deliberately not carried over — the
  checkpoint image already has whatever those mounts wrote baked in), with every
  ordinary builder still available to override before `start()`. A restored
  container is ordinary in every respect: fresh host ports, normal reaping-ledger
  registration, normal `stop()`. Checkpoint images are never auto-reaped (they're
  images, not containers) — see the cleanup one-liner in the docs. See
  [Checkpoint / Restore](docs/checkpoints.md).
- **Cross-language parity page**: a documented artifact for the claim that the same
  container spec produces the same observable behavior in the Kotlin, Rust, and
  TypeScript ports, on both backends — the verified behavior areas as a table, the
  pinned cross-language identity-hash vector, and a pointer at the contract suite
  (`crates/rightsize-modules/tests/contract.rs`) that enforces it. See
  [Cross-Language Parity](docs/parity.md).

## [0.1.2] - 2026-07-09

### Changed

- **Pinned microsandbox runtime bumped from 0.6.3 to 0.6.6** (`rightsize-msb`).
  The provisioner downloads and SHA-256-verifies the new release on first use
  (existing `0.6.3` caches are left in place and simply stop being used). The
  full integration matrix passes unchanged on both backends against 0.6.6,
  and the backend behaviors the msb backend compensates for were re-verified
  as still present: detached `msb run` still never starts the image
  ENTRYPOINT, and `msb logs -f` still never exits after its sandbox stops.

## [0.1.1] - 2026-07-06

### Fixed

- **The default readiness budget is 120 seconds** (was 60; `rightsize`). Three
  modules in a row (MySQL, ClickHouse, Redpanda) were observed overrunning a
  60-second ceiling on loaded CI runners while booting normally. The budget is
  a deadline, not a wait — `start()` still returns the moment the readiness
  signal fires — so the larger default costs nothing on the happy path and
  only delays the failure verdict when a container is genuinely broken.
  `with_startup_timeout` overrides it as before.
- **`ClickHouseContainer` readiness gets a 180-second budget**
  (`rightsize-modules`). The entrypoint runs a second server pass for
  user/database provisioning before the HTTP interface opens, and a loaded
  Windows CI runner was observed still in early config processing at the
  previous default budget. The budget is a deadline, not a wait — readiness
  returns the moment `/ping` answers.
- **The backend retries a boot that hit msb's state-database error**
  (`rightsize-msb`; `error: database error: ...`). Every msb invocation runs
  schema migrations against its shared SQLite state database on startup, and
  two concurrent invocations can race them — the loser exits before doing any
  work, with whatever wording matches the statement it lost on (three shapes
  observed: `index ... already exists`, `duplicate column name: ...`, and
  `UNIQUE constraint failed: seaql_migrations.version`). A boot is never
  inherently alone (the attached `msb run` races the backend's own state
  polling), so this can fire even under fully serialized tests. The race is
  transient by construction — the winner's migration commits and later
  invocations find the schema in place — so a boot failing with msb's
  state-database framing is retried exactly once after a short delay; a second
  failure propagates with both attempts' output.

### Changed

- **Backends register themselves** (`rightsize-modules`): the feature-enabled
  backend providers are registered automatically the first time any module
  starts, so consumers write no `register_provider` boilerplate. A new public
  `rightsize_modules::register_default_backends()` covers the one case that
  still needs a call — a plain `Container` as the process's first start. Core's
  `register_provider` is now idempotent by provider name (first registration
  wins), so automatic and manual registration coexist without double entries.
  Registering providers by hand remains the path for consumers depending on
  the backend crates directly.

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

[Unreleased]: https://github.com/ngriaznov/rightsize-rust/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/ngriaznov/rightsize-rust/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/ngriaznov/rightsize-rust/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ngriaznov/rightsize-rust/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ngriaznov/rightsize-rust/releases/tag/v0.1.0
