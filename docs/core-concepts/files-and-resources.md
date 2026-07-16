# Files & Resources

This page covers mounting files in at start time. To copy files into (or out of) an
already-running container, see [Copying Files](../copy.md) instead.

## Mounting host files into a container

`MountableFile` resolves a host path (or a bundled test resource) before you mount it
in with `Container::with_copy_file_to_container`:

```rust,ignore
use rightsize::{Container, MountableFile};

let fixture = MountableFile::for_classpath_resource("tests/fixtures/init.sql")?;
let db = Container::new("postgres:18-alpine")
    .with_copy_file_to_container(fixture, "/docker-entrypoint-initdb.d/init.sql")
    .start()
    .await?;
```

Two constructors:

- **`MountableFile::for_host_path(path)`** — a file already on the host filesystem.
  A relative path is absolutized against the current working directory; an absolute
  path is used as-is.
- **`MountableFile::for_classpath_resource(resource)`** — Rust has no classpath, so
  this is the documented convention that plays the same role: `resource` resolves
  relative to `CARGO_MANIFEST_DIR`, matching how a crate actually bundles test/data
  files (e.g. under `tests/fixtures/`). Errors, naming the resource and the
  manifest directory searched, if the path doesn't exist — a missing fixture fails
  loud and specific, not with a generic "file not found" from deep inside the
  backend.

Mounts default to **read-only** (`FileMount::new` sets `read_only: true`); call
`.read_write()` on the `FileMount` if the guest genuinely needs to write to it. See
[Backend differences](../backends.md#backend-differences) for a real gap here: on
microsandbox 0.6.2, `read_only` is currently advisory only — the guest gets a
writable mount regardless of the flag. Don't rely on guest-side write protection
under `RIGHTSIZE_BACKEND=microsandbox`; Docker enforces it genuinely.

## Memory limits — when and why

`Container::with_memory_limit(megabytes)` caps the container's guest memory. Leaving
it unset lets each backend apply its own default — and on microsandbox, that default
is small: **~450 MB** per microVM. That's plenty for a lean single-process workload
(Redis, Memcached, a KRaft Kafka broker with a trimmed heap) but not for a JVM image
whose fixed memory regions are computed above that.

Two shipped modules hit this in practice, with measured evidence rather than a
guessed round number:

### The Paketo story (`SpringCloudConfigContainer`)

Paketo's memory calculator sizes a Spring Boot JVM image's fixed regions (metaspace,
thread stacks, direct memory) at roughly 688 MB *before* the heap itself — comfortably
above microsandbox's ~450 MB default. `SpringCloudConfigContainer` therefore ships
with `.with_memory_limit(1024)` baked into its constructor; you don't need to set this
yourself when using the module, only when hand-rolling the equivalent via the plain
`Container` API against a similarly memory-hungry JVM image.

### The Pinot story (`PinotContainer`)

Apache Pinot's `QuickStart -type EMPTY` boots four JVMs (controller, broker, server,
minion) plus an embedded ZooKeeper in one container, and the image itself bakes in
`JAVA_OPTS=-Xms4G -Xmx4G` — the QuickStart driver JVM alone requests a 4 GiB heap
before any of the four sub-JVMs it spawns takes anything. The module originally
scaled the SpringCloudConfig number up naively to 2048 MB ("four JVMs instead of
one") — that undershot badly. Measured directly against a real
`apachepinot/pinot:1.5.1 QuickStart -type EMPTY` boot:

```text
--memory=2048m -> OOMKilled=true (timed out at 180s waiting for /health, reaped by the kernel)
--memory=2560m -> OOMKilled=true
--memory=3072m -> OOMKilled=false; /health 200 within ~15s; BUT settles at ~99% of the 3 GiB
                  limit, and under that pressure the controller's Helix-backed schema/table
                  RPCs intermittently time out (a schema POST returns
                  {"code":500,"error":"java.util.concurrent.TimeoutException"}) even though
                  /health reports 200. Reproduced repeatedly at 3072m.
--memory=4096m -> stable: settles at ~73-75% of the limit, comfortable headroom; schema POST
                  succeeded on every attempt across a 60s repeated-POST probe.
```

`PinotContainer` ships with `.with_memory_limit(4096)` — the lowest round number with
real headroom above the image's own 4 GiB heap request, not merely enough to dodge the
OOM killer outright. Verified stable on both Docker and microsandbox.

### The takeaway for your own images

If you're wrapping a JVM (or otherwise memory-hungry) image in the plain `Container`
API and it hangs or gets silently killed partway through boot on the microsandbox
backend specifically, suspect the ~450 MB default before anything else — check what
the image's own baked `JAVA_OPTS`/heap flags request, and set
`.with_memory_limit(...)` above that with real headroom, the same way these two
modules did. See [Troubleshooting](../troubleshooting.md) for the matching
symptom→fix entry.
