# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project intends to adhere to [Semantic Versioning](https://semver.org/) once it
reaches its first tagged release.

## [Unreleased]

Nothing yet.

## [0.7.6] - 2026-08-29

### Changed

- **The pinned microsandbox release is now 0.6.16** (from 0.6.15). Upstream changes
  relevant here: network address slots are recycled instead of exhausting after many
  sandbox creations, single-file mounts are properly isolated, and a failed boot now
  renders a structured boot error in `msb logs`. One behavior-relevant change did
  land, covered under Fixed below: 0.6.16's convergent-lifecycle rework stops
  surfacing `Running` at all for a workload that finishes before this library's own
  readiness poll can observe it.

- **Downgrading `MSB_PATH` off 0.6.16 is unsafe once its `MSB_HOME` has been used.**
  0.6.16 migrates the shared state database on first run; an older msb binary
  pointed at that same, now-migrated `MSB_HOME` refuses outright with "database
  schema is newer than this msb binary". Back up or recreate `MSB_HOME` before
  downgrading.

### Fixed

- **A workload that finishes quickly no longer fails `start()` on msb 0.6.16.** msb
  0.6.16's convergent-lifecycle rework never surfaces `Running` for a sandbox whose
  command (e.g. a short build or test script) completes before the readiness poll's
  next check — earlier releases usually won that race on a fast host and observed
  `Running` briefly before the same exit. The boot supervision now recognizes this
  case: an attached `msb run` child that exits 0 before `Running` was observed is
  checked post-mortem, and classified as a completed (not failed) start when `msb ls`
  reports the sandbox `Stopped` and the system log carries the boot-completion marker
  msb's guest agent writes only once it has actually come up. Any other combination
  (a non-zero exit, a state other than `Stopped`, or the marker missing) still fails
  exactly as before.

## [0.7.5] - 2026-08-26

### Changed

- **The pinned microsandbox release is now 0.6.15** (from 0.6.14). Upstream changes
  relevant here: host DNS on Windows now routes through the system resolver, file
  copies on NTFS only copy allocated ranges, and read-only mounts no longer get
  write-probed. No CLI surface this library drives changed.

### Fixed

- **Host ports that hit a bind conflict are no longer eligible for the immediate retry.**
  The port-retry loop used to return a conflicted port to the allocator before the next
  attempt, so the OS could hand the same proven-contended port straight back. Conflicted
  ports now stay quarantined until the retry loop exits.

## [0.7.4] - 2026-08-22

### Changed

- **The pinned microsandbox release is now 0.6.14 on all platforms.** Upstream fixed
  the Windows bootstrap regression in 0.6.14: the `msb_krun`/`msb_krun_devices` 0.1.32
  bump starts console-port delivery at `PORT_OPEN` instead of `PORT_READY`, matching
  unix (upstream issue #1426, closed against 0.6.14). The per-platform split pin
  introduced in 0.7.2 — unix on 0.6.12, Windows held back on 0.6.9 — is retired; both
  platforms provision the same release again.

  **If you point `MSB_PATH` at your own msb binary on Windows, never use 0.6.10
  through 0.6.13** — those releases carry the bootstrap regression this fix resolves.

## [0.7.3] - 2026-08-21

### Changed

- **The pinned microsandbox release is now 0.6.12 on macOS and Linux; Windows stays
  on 0.6.9.** msb 0.6.10, 0.6.11, and 0.6.12 are all broken on Windows: since 0.6.10,
  guest bootstrap moved off the kernel command line onto a one-shot pre-boot console
  frame, and on Windows that frame never reaches the guest agent, so the agent times
  out after 60 seconds and the guest dies. The sandbox can briefly report as running,
  but the agent relay endpoint is never created, so exec, logs, and ping can never
  connect. There is no client-side workaround, and upstream has no fix merged or
  staged. The three affected releases changed no CLI surface this library drives, so
  the per-platform pin does not change behavior on either platform.

  **If you point `MSB_PATH` at your own msb binary on Windows, keep it at 0.6.9** —
  a 0.6.10, 0.6.11, or 0.6.12 binary there will hit the regression on every container
  start.

## [0.7.2] - 2026-08-19

### Changed

- **The pinned microsandbox release is now 0.6.10 on macOS and Linux; Windows stays
  on 0.6.9.** msb 0.6.10 has a Windows-only regression: its pre-boot guest bootstrap
  message never reaches the guest agent on Windows hosts, so every sandbox exits
  about 70 seconds after spawn without the agent ever coming up. macOS and Linux are
  unaffected. The two releases are identical across every CLI surface this library
  drives, so the per-platform pin does not change behavior — the provisioner simply
  routes Windows around the broken release until upstream fixes it.

  **If you point `MSB_PATH` at your own msb binary on Windows, keep it at 0.6.9** —
  a 0.6.10 binary there will hit the regression on every container start.

## [0.7.1] - 2026-08-16

### Changed

- **The pinned microsandbox release is now 0.6.9** (was 0.6.8). No CLI surface this
  library drives changed, so no action is needed — the provisioner downloads and
  checksum-verifies the new release automatically, and `MSB_PATH` setups validated
  against 0.6.8 keep working. 0.6.9 also fixes two upstream issues this library
  carried defenses for: the Windows snapshot-save flush failure (the salvage path
  stays in place and now simply never fires) and the concurrent-pull image-cache
  race (the heal path likewise remains as a safety net).

## [0.7.0] - 2026-08-04

### Added

- **`Container::with_disk_limit(megabytes)`** caps the writable root disk
  (`--root-disk <mb>M`) — microsandbox-only; docker runs its normal disk-backed
  rootfs with no ceiling and ignores this. The ceiling grows on an msb reboot but
  never shrinks back down. Mutually exclusive with `with_tmpfs_root` —
  `RightsizeError::RootDiskConflict` at `start()` if both are set.
- **`Container::with_tmpfs_root(megabytes)`** runs the writable root disk from
  guest RAM instead of storage (`--root-disk tmpfs:<mb>M`) — faster, ephemeral
  containers with no disk residue. microsandbox-only; docker ignores it. Must fit
  inside guest memory: msb defaults to 512M when no `with_memory_limit` is set,
  and `RightsizeError::TmpfsRootExceedsMemory` fires at `start()` when both are
  set and the tmpfs root exceeds the memory limit. A tmpfs root is ephemeral and
  cannot be checkpointed — `checkpoint()`/`checkpoint_named()` return
  `RightsizeError::TmpfsRootCheckpoint` before touching anything, and a refused
  named re-checkpoint leaves the existing checkpoint intact. msb also rejects any
  root-disk setting on a `Container::from_checkpoint` restore before boot — the
  snapshot pins the root disk.
- **`Container::with_network_disabled()`** blocks public-internet access on
  microsandbox (`--net private`): published ports keep serving and private-range
  network links keep working, but outbound connections to the public internet
  fail. Docker ignores this flag entirely — there's no portable way to block
  egress while keeping published ports on that backend. Mutually exclusive with
  `with_network` — `RightsizeError::NetworkDisabledConflict` at `start()` if both
  are set.

### Changed

- **microsandbox checkpoint artifacts now live under
  `<cache dir>/checkpoints/`** (`~/.cache/rightsize` on macOS/Linux,
  `%LOCALAPPDATA%\rightsize` on Windows), created via msb's `--dest-dir` rather
  than its default snapshot store. `Checkpoint::checkpoint_ref` for msb is now
  the absolute artifact path — the ref remains an opaque string, so no caller
  code needs to change, but a bare-name ref from an earlier release still
  restores. The snapshot still shows up in `msb snapshot list` (msb keeps its
  own global index); removing it through this library's `Checkpoint::remove`
  cleans up both the artifact and the registry entry.
- **`ContainerSpec` gained three public fields**: `disk_limit_mb`,
  `tmpfs_root_mb`, `network_disabled`. Code constructing a `ContainerSpec`
  struct literal directly needs to add them; `ContainerSpec::new` keeps working
  unchanged.
- **`rightsize-msb`'s `commands::snapshot_create` kept its existing two-argument
  form** and gained a separate `commands::snapshot_create_in` that takes a
  destination directory, backing the checkpoint storage change above.

### Fixed

- The boot-retry classifier for msb's install-lock refusal now also recognizes
  its second phrasing — `another microsandbox install operation is in progress
  until <ts>` — instead of failing the boot on it; the retry already handled
  the first phrasing (`install operation in progress until <ts>`).

## [0.6.2] - 2026-08-01

### Fixed

- A failed container create no longer leaks its pre-allocated host ports: the
  create-error path now returns them to the in-process allocator, as the reuse
  path already did. Without this, every failed create attempt permanently
  retired its ports from the pool for the life of the process.

## [0.6.1] - 2026-08-01

### Changed

- **The pinned microsandbox release is now 0.6.8** (was 0.6.6). The provisioner
  downloads and checksum-verifies it automatically, so no action is needed for the
  usual setup.

  **If you point `MSB_PATH` at your own msb binary, it must be 0.6.8 or newer.**
  0.6.8 renamed three CLI surfaces this library drives, and the calls it now emits do
  not exist in 0.6.6:

  | 0.6.6 | 0.6.8 |
  |---|---|
  | `run --snapshot <ref>` | `run --from-snapshot <PATH_OR_NAME>` |
  | `snapshot export <ref> <dest>` | `snapshot save <SNAPSHOT> <OUT>` |
  | `snapshot import <archive>` | `snapshot load <ARCHIVE> [DEST]` |

  Checkpoint restore and checkpoint archives are the affected features; both fail
  outright against an older binary rather than degrading quietly.

- **A loaded snapshot's effective ref is now a bare 64-character digest**, where 0.6.6
  produced a `sha256-<16hex>` directory name. Nothing in the public API changes — the
  ref was always opaque and content-addressed — but code that pattern-matched the old
  shape will need updating.

- **`FileMount::read_only` now defaults to `false`**, with a new
  `FileMount::read_only()` builder as the opt-in, and the flag is genuinely enforced
  on the microsandbox backend — it previously never reached msb at all, so every
  mount there was writable regardless of the flag; the docker backend enforced it all
  along. What a caller observes: a default `with_copy_file_to_container` mount on
  docker was read-only before and is writable now — call `.read_only()` to get the
  old docker behavior, which both backends now honor as a guest-side write block.
  The mount is a view of the host file, not a copy, so a guest write to a default
  mount reaches the host file itself.

### Fixed

- The Cassandra module's `GPG_KEYS` override remains required: 0.6.8 still aborts
  before the VM starts on any image whose baked environment contains a tab, verified
  directly against this release.

- **File mounts work on Windows.** msb 0.6.7 broke every start-time file mount there:
  its mount-spec parsing splits a token-less spec at the drive letter's colon — on
  the CLI spec, and again on an internally rebuilt one. Every mount spec this backend
  emits now carries an explicit `ro`/`rw` token plus `nodev`, keeping both layers
  parseable. `nodev` is meaningless for a single-file mount.

- **Checkpoint export works on Windows again.** msb 0.6.7/0.6.8 fail every
  `snapshot save` there with `Access is denied. (os error 5)`: the finished archive
  is fsynced through a read-only handle one step before the final rename. When
  exactly that failure occurs with exactly one finished staging file beside the
  destination, `Checkpoint::export_to` completes the rename itself. Transparent,
  Windows-only, and self-disabling once msb fixes the fsync.

- **Container boot rides out msb's transient `install operation in progress`
  refusal** by polling for up to 30 seconds instead of failing on the first attempt.

## [0.6.0] - 2026-07-28

### Upgrading from 0.5.0

Two changes affect existing code.

**Modules no longer pin an image version.** `RedisContainer::new()` previously booted
`redis:8.6-alpine`; it now boots `redis:latest`. Your tests will run whatever version
upstream currently publishes, which is the point — the version tracks the image's own
releases rather than this crate's. To keep a specific version, name it:
`RedisContainer::with_image("redis:8.6-alpine")`. Redis, Valkey, Postgres, and
Memcached additionally move from an Alpine variant to the Debian-based `latest`:
functionally equivalent, noticeably larger to pull.

**`ElasticsearchContainer` has no `new()`.** Elastic publishes no floating tag —
`elasticsearch:latest`, `:9`, and `:8` are all `404` on Docker Hub — so an explicit
version is required and there is nothing this module could pick on your behalf:
`ElasticsearchContainer::with_image("elasticsearch:9.4.4")`.

An explicitly supplied image is also now checked against the repository the module
understands, when the container starts, so an unrelated image fails immediately with
`RightsizeError::IncompatibleImage` instead of timing out against the wrong server.
Constructors stay infallible. If the image really is a drop-in replacement, say so:
`ImageName::parse("mycorp/pg-hardened:16").as_compatible_substitute_for("postgres")`.

### Added

- **`ImageName`** (`rightsize`) — a parsed Docker image reference, built via
  `ImageName::parse`. Module constructors take `impl Into<ImageName>` and stay
  infallible; `start()` checks the supplied image's repository against the one the module
  understands before any backend is resolved, returning the new typed
  `RightsizeError::IncompatibleImage` on a mismatch rather than degrading into a bare
  wait-strategy timeout. `ImageName::as_compatible_substitute_for` is the escape hatch for
  a private mirror, a hardened rebuild, or a rename. Registry-host stripping follows the
  Docker convention: the first path segment is a registry only if it contains a `.` or a
  `:`, or is exactly `localhost`.
- **`ElasticsearchContainer`** (`rightsize-modules`) — a single-node Elasticsearch
  container. Elastic publishes no floating tag for this image, so this module has no
  `new()` — an explicit image is required. Readiness checks plain connectivity rather than
  cluster health, since a single node's health stays `yellow` forever (no peer to place
  replica shards on).
- **`QdrantContainer`** (`rightsize-modules`) — a single-node Qdrant vector database
  container, defaulting to `qdrant/qdrant:latest`. Readiness is Qdrant's own `/readyz`
  probe, which answered on the first poll in direct verification; no memory limit is
  needed.

### Changed

- **Every one of the 21 pre-existing modules now defaults to a floating image reference**
  instead of a pinned version, and checks any explicitly supplied image against the
  repository it understands. Most float to `<repository>:latest`; `RabbitMqContainer`
  floats to `rabbitmq:management` instead, since plain `latest` lacks the management
  plugin the module is built around. Redis, Valkey, Postgres, and Memcached move from a
  pinned Alpine variant to the Debian-based `latest`. Each module's measured facts —
  readiness signal, memory floor, timings — remain attributed to the version that produced
  them rather than reattributed to `latest`.

### Fixed

- **An `exec` issued immediately after `start()` could fail to reach the guest.** A
  sandbox reports `Running` before the in-guest agent has created the endpoint `exec`
  connects to; the gap is invisible whenever a wait strategy runs first, which is every
  module, but a caller that starts and execs at once could lose the race — reliably so on
  Windows, where the endpoint is a named pipe. `exec` now retries on that one signature.
  A guest command's own non-zero exit, and any agent error raised after connecting, still
  return on the first attempt.
- **`MongoDbContainer`'s replica-set budget is now 180s**, up from 60s. `rs.initiate`
  was observed failing at exactly the 60s mark on a loaded Windows CI runner against the
  floating default, matching the budget MySQL and ClickHouse already carry.

## [0.5.0] - 2026-07-25

### Added

- **`ValkeyContainer`** (`rightsize-modules`) — a single-node Valkey container, the
  Redis-protocol-compatible fork. Readiness is anchored on Valkey's own
  `Ready to accept connections` log line, and `uri()` returns a `redis://` URI because
  that is the scheme every Redis-protocol client parses.
- **`MinioContainer`** (`rightsize-modules`) — a single-node MinIO server, S3-compatible
  object storage. The image needs an explicit `server /data --console-address :9001`
  command, which this module always sets; readiness is MinIO's own `/minio/health/live`
  probe on the S3 API port. Defaults to a `testuser`/`testpassword` root pair, since MinIO
  rejects a root password shorter than eight characters.
- **`CassandraContainer`** (`rightsize-modules`) — a single-node Apache Cassandra,
  ready-checked on its `Starting listening for CQL clients` log line. The module overrides
  the image's baked `GPG_KEYS` value, which contains a tab: the microsandbox backend aborts
  before the VM starts on any image whose baked environment carries one. `GPG_KEYS` is
  consumed only at image-build time, so the override has no effect on the running server.

## [0.4.0] - 2026-07-18

### Added

- **Checkpoint export/import** (`rightsize`, `rightsize-msb`, `rightsize-docker`):
  `Checkpoint::export_to(path)` writes a checkpoint to a portable archive (a plain
  tar carrying pinned JSON metadata plus the backend's own payload — msb's
  `snapshot export`, docker's `docker save`), and `Checkpoint::import_from(path)`
  materializes an archive on the active backend and returns a restorable
  `Checkpoint`, re-registering a named checkpoint with the same replace semantics
  as `checkpoint_named`. Both fail with a typed error (a backend mismatch, a stale
  or malformed archive) before any backend or filesystem work. `SandboxBackend`
  gains `export_checkpoint`/`import_checkpoint`; microsandbox's import confirms
  the digest-derived directory name an imported snapshot lands under via `msb
  snapshot list --format json` and returns that as the effective ref (msb does
  not resolve the full `sha256:` digest as a snapshot ref), docker's `docker
  load` preserves the original tag unchanged. Archives never bundle the OCI
  image (msb 0.6.6's `--with-image`
  export fails an integrity check on import) — the destination machine pulls it
  fresh on the restored container's first boot. See
  [Checkpoint / Restore](docs/checkpoints.md#moving-checkpoints-between-machines).

## [0.3.0] - 2026-07-16

### Fixed

- **The MySQL module's readiness budget is 180 seconds** (was 120), the same
  treatment the ClickHouse module already has: a loaded Windows CI runner was
  observed still short of ready at 123 seconds, past the previous ceiling. The
  budget is a deadline, not a wait — a faster boot still completes as fast as
  it ever did.

### Added

- **Runtime file copy** (`rightsize`, `rightsize-msb`, `rightsize-docker`):
  `ContainerGuard::copy_file_to_container(host_path, container_path)`,
  `copy_content_to_container(bytes, container_path)`, and
  `copy_file_from_container(container_path, host_path)` — the Testcontainers-style
  runtime counterpart to the existing start-time `with_copy_file_to_container`
  mount, usable any time after `start()` against an already-running container, on
  either backend. Both directions accept a file or a directory (no separate
  "directory" method) with `cp -r`-style destination naming, require the container
  to be running and `container_path` to be absolute (both checked before any
  backend call), and create the destination's parent directory automatically —
  `mkdir -p` in the guest via `exec` on the way in, `std::fs::create_dir_all` on
  the host on the way out. `copy_content_to_container` writes its bytes to a
  private (mode `0600` on unix) temp file and cleans it up regardless of the
  copy's outcome. `SandboxBackend` gains `copy_to_container`/`copy_from_container`,
  defensively unsupported by default; microsandbox implements both via `msb copy
  -q`, docker by shelling out to the `docker` CLI (`docker cp`) rather than
  hand-rolling the daemon's tar-archive endpoints — the reaping watchdog's own
  kill command already makes the `docker` CLI a hard requirement for that backend,
  so this adds no new dependency. Works on a [reuse](docs/reuse.md) container like
  any other runtime operation, but mutates shared reused state and is not part of
  the reuse identity hash. See [Copying Files](docs/copy.md).
- **microsandbox checkpoint/restore** (`rightsize`, `rightsize-msb`,
  `rightsize-docker`): `capabilities().checkpoint` is now `true` on BOTH real
  backends — microsandbox stops the sandbox, snapshots its disk (`msb snapshot
  create --from <name> rz-ckpt-<12hex>`), removes the stopped sandbox, and boots
  it back from that snapshot under the same name and ports (an attached `msb run
  --snapshot <ref>` re-boot, not `msb start`, which relies on upstream
  microsandbox's detached-spawn path and fails deterministically under a Windows
  CI job object); docker unchanged (image commit). `Capabilities` gains
  `checkpoint_restarts_workload` (docker `false`, microsandbox `true`); when it's
  `true`, `ContainerGuard::checkpoint()` re-runs the container's own configured
  wait strategy before returning, since microsandbox's cycle reboots the guest and
  a bare return would hand back a false-ready container. When this container has
  network links installed (see
  [Networks](docs/backends.md#networking-is-emulated-not-native)), they're
  re-installed before that wait-strategy re-run, since the guest reboot drops
  microsandbox's emulated exec-tunnel links along with everything else. If the
  snapshot step itself fails, the sandbox is left stopped (never removed) and the
  error names `msb start <name>` as the by-hand remedy; if the snapshot succeeds
  but the re-boot from it fails, the error names the checkpoint ref and
  `Container::from_checkpoint(...)` as the recovery path. `Checkpoint` is renamed
  and extended: `image_ref` becomes `checkpoint_ref` (its format is now
  backend-specific — a docker image tag or a microsandbox snapshot name), and it
  gains `backend`, the registered name of whichever backend created it.
  `Container::from_checkpoint(&cp)` refuses to start under a different active
  backend than `cp.backend` with a new typed `RightsizeError::
  CheckpointBackendMismatch`, before any backend work; combining `.reuse(true)`
  with `from_checkpoint` similarly fails fast with a new typed
  `RightsizeError::ReuseCheckpointConflict` (reuse identity has no concept of a
  checkpoint ref). `SandboxBackend::commit_to_image` is renamed to
  `create_checkpoint` (takes a random nonce, returns the backend-native ref) and
  gains a sibling `remove_checkpoint` (SPI-only best-effort cleanup — `msb
  snapshot rm` / `docker rmi`, "not found" is success). `ContainerSpec` gains
  `checkpoint_ref`, set by `from_checkpoint`; microsandbox boots via `msb run
  --snapshot <ref>` instead of its normal image argument when it's set, docker
  ignores it. `CheckpointUnsupported`'s message no longer steers toward docker
  specifically, since both real backends support checkpointing now. Checkpoints
  can also be NAMED and made durable across processes:
  `ContainerGuard::checkpoint_named(name)` (name must match
  `^[a-z0-9][a-z0-9-]{0,40}$`, checked before any backend call — a bad name is a
  new typed `RightsizeError::InvalidCheckpointName`) writes a small registry entry,
  `<cacheDir>/checkpoints/<name>.json`, atomically and only after the backend
  checkpoint has succeeded; re-checkpointing an existing name best-effort removes
  the previous ref first, then replaces the entry (latest wins). `Checkpoint`
  gains `find(name)`, `list()`, and `remove(name)`: `find` rediscovers a named
  checkpoint written by any earlier process sharing the same cache directory,
  probing a same-backend entry's artifact via a new SPI method,
  `SandboxBackend::has_checkpoint` (docker: image inspect; microsandbox: `msb
  snapshot inspect`), and treating a confirmed-gone artifact as a stale entry it
  cleans up automatically — a different-backend entry is returned unprobed, since
  the restore-time mismatch gate is already the authority there; `list` reads the
  registry only, with no artifact probing; `remove` best-effort tears down both
  the artifact and the registry entry and is idempotent. See
  [Checkpoint / Restore](docs/checkpoints.md).

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

[Unreleased]: https://github.com/ngriaznov/rightsize-rust/compare/v0.7.6...HEAD
[0.7.6]: https://github.com/ngriaznov/rightsize-rust/compare/v0.7.5...v0.7.6
[0.7.5]: https://github.com/ngriaznov/rightsize-rust/compare/v0.7.4...v0.7.5
[0.7.4]: https://github.com/ngriaznov/rightsize-rust/compare/v0.7.3...v0.7.4
[0.7.3]: https://github.com/ngriaznov/rightsize-rust/compare/v0.7.2...v0.7.3
[0.7.2]: https://github.com/ngriaznov/rightsize-rust/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/ngriaznov/rightsize-rust/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/ngriaznov/rightsize-rust/compare/v0.6.2...v0.7.0
[0.6.2]: https://github.com/ngriaznov/rightsize-rust/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/ngriaznov/rightsize-rust/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/ngriaznov/rightsize-rust/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/ngriaznov/rightsize-rust/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/ngriaznov/rightsize-rust/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/ngriaznov/rightsize-rust/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/ngriaznov/rightsize-rust/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/ngriaznov/rightsize-rust/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/ngriaznov/rightsize-rust/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/ngriaznov/rightsize-rust/releases/tag/v0.1.0
