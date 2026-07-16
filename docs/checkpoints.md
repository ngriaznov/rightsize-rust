# Checkpoint / Restore

Boot a container, checkpoint it, then restore as many fresh sandboxes from that
checkpoint as you need — instead of re-running whatever expensive setup got the
original into that state.

## What this actually captures

A checkpoint is a **filesystem capture**, not a memory snapshot: restore boots a
container whose filesystem starts exactly where the checkpoint left off, but every
process inside it starts from scratch — nothing about the checkpointed container's
running processes, open connections, or in-memory state survives. If you have just
written files via `exec`, run `sync` in the guest before checkpointing — an unflushed
write is exactly the kind of in-memory state a checkpoint does not capture. State on RAM-backed
mounts is not captured either: the microsandbox guest mounts `/tmp` as tmpfs, so a
file written there never enters a checkpoint — write anything you need restored to
a rootfs path such as `/srv` or `/var`.

That's enough for the common case: boot a database, run migrations and seed data,
checkpoint it, and every later `Container::from_checkpoint(&cp)` restores a
fully-migrated-and-seeded database in the time it takes to boot, with no
re-migration and no re-seeding. It is not enough for anything that depends on live
process state (an in-flight transaction, a warmed in-memory cache, an open
connection) — that needs true memory snapshotting, which stays on the
[roadmap](./roadmap.md) pending upstream microsandbox support.

## Both backends support it — by different mechanisms

| Backend | Mechanism | Effect on the source container |
|---|---|---|
| docker | Image commit (`POST /commit`) | Undisturbed — the running container keeps running exactly as it was. |
| microsandbox | Disk snapshot: stops the sandbox, snapshots its disk, and boots it back from that snapshot under the same name and ports | The sandbox briefly stops and its workload restarts (the guest reboots); `checkpoint()` re-runs the container's own wait strategy before returning, so you never get back a false-ready guard. |

On microsandbox, the guest reboot also drops any emulated network links (see
["Networking is emulated, not native"](./backends.md#networking-is-emulated-not-native)),
so `checkpoint()` re-installs them, with the same links this container started
with, before the wait-strategy re-run above.

`capabilities().checkpoint` is `true` on both:

```rust,ignore
use rightsize::backends;

let caps = backends::active().capabilities();
if caps.checkpoint {
    // both real backends land here today
}
if caps.checkpoint_restarts_workload {
    // microsandbox: the stop/snapshot/start cycle rebooted the guest
} else {
    // docker: the image commit left the container running, undisturbed
}
```

## API

```rust,ignore
use rightsize::Container;

let original = Container::new("postgres:16-alpine")
    .with_env("POSTGRES_PASSWORD", "test")
    .with_exposed_ports(&[5432])
    .start()
    .await?;

// ... run migrations, seed data ...

let checkpoint = original.checkpoint().await?;
original.stop().await?;

// Later, in this process or a later one, under the SAME backend:
let restored = Container::from_checkpoint(&checkpoint).start().await?;
```

`checkpoint()` requires the guard to be currently running — calling it on a stopped
or never-started guard is a state error, the same shape as `exec`/`logs`. On
success it returns a `Checkpoint`:

```rust,ignore
pub struct Checkpoint {
    pub checkpoint_ref: String, // backend-native ref, random per checkpoint — see "Ref formats" below
    pub backend: String,        // which backend created it, e.g. "docker" / "microsandbox"
    pub spec: ContainerSpec,    // see caveat below
}
```

`spec` is the source container's full spec at checkpoint time **only** when the
`Checkpoint` came directly back from `checkpoint()`/`checkpoint_named()`. A
`Checkpoint` rediscovered via `Checkpoint::find`/`Checkpoint::list` instead carries a
reconstructed spec: only `env`, `command`, exposed ports, and the memory limit are
real (the four fields `from_checkpoint` actually reads back); every other field is a
placeholder, since the registry never persists the full spec — see "Reusing
checkpoints across runs" below.

`Container::from_checkpoint(&checkpoint)` builds a normal `Container` whose image is
`checkpoint.checkpoint_ref` and whose env, command, exposed ports, and memory limit default
to the source container's — everything a restored container needs to behave like
the original. Every ordinary builder still works on the result, so a caller can
override anything before `.start()`:

```rust,ignore
let restored = Container::from_checkpoint(&checkpoint)
    .waiting_for(Wait::for_log_message("database system is ready", 1))
    .start()
    .await?;
```

Deliberately **not** carried over from the checkpoint's spec: mounted files, network
membership, and aliases. A checkpoint already has whatever those mounts wrote baked
directly into its filesystem, and network topology has no well-defined meaning to
replay across a restore.

## Ref formats

A checkpoint's `checkpoint_ref` is backend-native, and its shape differs by backend:

- **docker**: an image tag, `rightsize/checkpoint:<12 hex chars>`.
- **microsandbox**: a disk snapshot name, `rz-ckpt-<12 hex chars>`.

Both are random per checkpoint (never reused across calls).

## Restoring under the wrong backend is a typed error

`Checkpoint::backend` records which backend created it, and `from_checkpoint`
refuses to restore under a *different* active backend — a docker-committed image
has no meaning as a microsandbox snapshot ref, and vice versa:

```rust,ignore
let restored = Container::from_checkpoint(&checkpoint); // created under docker
// If the active backend is microsandbox:
let err = restored.start().await.unwrap_err();
// RightsizeError::CheckpointBackendMismatch, naming both backends:
// "... was started under the 'microsandbox' backend, but this checkpoint was
//  created by the 'docker' backend — set RIGHTSIZE_BACKEND=docker to restore it ..."
```

This check runs before any backend work, so a mismatch never reaches the CLI/daemon
at all.

## Restored containers are ordinary containers

A container started from `Container::from_checkpoint(&cp)` is indistinguishable from
one started any other way: a fresh name, fresh host ports (chosen by the core
allocator exactly like any other `start()`), normal registration in the
[orphan-reaping ledger](./reaping.md), and a normal `stop()` that tears it down like
any other container. Nothing about it is special once `start()` returns.

## The seeded-fixture pattern

The pattern this feature exists for: boot once per test *suite*, seed once, then
restore per test *case* instead of re-seeding every time.

```rust,ignore
use rightsize::{Checkpoint, Container};

// Once, at suite setup:
async fn seed_checkpoint() -> rightsize::Result<Checkpoint> {
    let seed = Container::new("postgres:16-alpine")
        .with_env("POSTGRES_PASSWORD", "test")
        .with_exposed_ports(&[5432])
        .start()
        .await?;

    // run migrations, insert fixture rows, whatever the suite needs baked in ...

    let cp = seed.checkpoint().await?;
    seed.stop().await?;
    Ok(cp)
}

// Per test case:
async fn fresh_seeded_db(cp: &Checkpoint) -> rightsize::Result<rightsize::ContainerGuard> {
    Container::from_checkpoint(cp).start().await
}
```

Every test case gets an independent, already-migrated-and-seeded database, at the
cost of one checkpoint up front instead of N re-runs of migrate-and-seed.

## Reusing checkpoints across runs

The seeded-fixture pattern above still needs to run `seed_checkpoint()` at least
once per PROCESS — the `Checkpoint` it returns only ever lives in memory. A NAMED
checkpoint fixes that: `checkpoint_named(name)` persists a small registry entry
alongside the backend artifact, so a LATER process — a later test run, a later CI
job, a different binary entirely — can rediscover and restore it without
re-seeding, as long as both processes agree on the rightsize cache directory (see
[configuration](./backends.md#configuration)) and run under the same backend.

```rust,ignore
use rightsize::{Checkpoint, Container};

let original = Container::new("postgres:16-alpine")
    .with_env("POSTGRES_PASSWORD", "test")
    .with_exposed_ports(&[5432])
    .start()
    .await?;

// ... run migrations, seed data ...

let checkpoint = original.checkpoint_named("seeded-db").await?;
original.stop().await?;
```

`name` must match `^[a-z0-9][a-z0-9-]{0,40}$` — anything else fails with a typed
`RightsizeError::InvalidCheckpointName` before any backend call. The unnamed
`checkpoint()` shown earlier in this page is unaffected: it keeps its exact
existing behavior (a random ref, no registry entry, ephemeral) — only a NAMED
checkpoint persists.

The idiomatic first-run/later-run pattern is `find(...) ?: seed()` — try to
rediscover the checkpoint first, and only pay the seeding cost if nothing was
found:

```rust,ignore
async fn seeded_db() -> rightsize::Result<Checkpoint> {
    if let Some(cp) = Checkpoint::find("seeded-db").await? {
        return Ok(cp);
    }
    let seed = Container::new("postgres:16-alpine")
        .with_env("POSTGRES_PASSWORD", "test")
        .with_exposed_ports(&[5432])
        .start()
        .await?;
    // ... run migrations, insert fixture rows ...
    let cp = seed.checkpoint_named("seeded-db").await?;
    seed.stop().await?;
    Ok(cp)
}

let restored = Container::from_checkpoint(&seeded_db().await?).start().await?;
```

The first process to call this pays the seed cost once; every later process
(concurrent CI shards, a developer's next local run) rediscovers the same
checkpoint via `Checkpoint::find` and skips straight to restoring it.

`Checkpoint` also exposes `list()` and `remove(name)`:

```rust,ignore
// Every named checkpoint currently registered — registry contents only, no
// artifact probing.
let all = Checkpoint::list()?;

// Tear one down explicitly: best-effort removes the backend artifact, then the
// registry entry. Returns whether anything existed — idempotent, "not found" is
// success.
let removed = Checkpoint::remove("seeded-db").await?;
```

**Replace semantics**: re-checkpointing an existing name REPLACES it —
`checkpoint_named` best-effort removes the previous ref before taking the new
one (only when that previous ref belongs to the currently active backend; see
below), then rewrites the registry entry. Latest wins; there is no versioning.

**The registry**: one JSON file per name, `<cacheDir>/checkpoints/<name>.json`
(the same rightsize cache directory every backend and the
[reaping ledger](./reaping.md) share), written atomically only after the backend
checkpoint has already succeeded. `find(name)` probes a same-backend entry's
artifact before returning it — a stale entry (the artifact deleted out from under
the registry) resolves to absent and is cleaned up automatically; a
different-backend entry is returned unprobed, since [restoring under the wrong
backend](#restoring-under-the-wrong-backend-is-a-typed-error) is already a typed
error at `start()` time. `list()` never probes at all. Removing (or replacing) a
checkpoint under a different active backend than its creator drops the registry
record but leaves the artifact behind, and once the record is gone a later `remove`
finds nothing to act on — remove a checkpoint under its creating backend in the first
place, or
use the [manual CLI cleanup](#cleanup) below.

## Reuse is not a supported combination

`.reuse(true)` combined with `Container::from_checkpoint(...)` fails fast with a
typed `RightsizeError::ReuseCheckpointConflict`, before any backend work — reuse's
identity hash has no concept of a checkpoint ref, and `checkpoint_ref` deliberately
never enters it.

## Cleanup

Checkpoint artifacts are never auto-reaped by [orphan reaping](./reaping.md), the
[cleanup thread](./how-it-works.md), or any other own-run cleanup path — the same
explicit decision as [reuse](./reuse.md). Every checkpoint you take stays on disk
until you remove it. For a NAMED checkpoint, `Checkpoint::remove(name)` is the
affordance built for this — it tears down both the backend artifact and the
registry entry in one call, and is idempotent. The manual, by-hand CLI cleanup
below is still valid (an unnamed checkpoint has no registry entry for
`Checkpoint::remove` to look up in the first place, so it's the only option
there), just no longer the only way to do it:

```sh
# docker
docker rmi rightsize/checkpoint:<12hex>

# microsandbox
msb snapshot rm rz-ckpt-<12hex>
```
