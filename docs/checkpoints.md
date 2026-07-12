# Checkpoint / Restore

Boot a container, checkpoint it into an image, then restore as many fresh
containers from that image as you need — instead of re-running whatever
expensive setup got the original into that state.

## What this actually captures

A checkpoint is a **filesystem capture**, not a memory snapshot: restoring boots a
brand-new container whose filesystem starts exactly where the checkpoint left off,
but every process inside it starts from scratch — nothing about the checkpointed
container's running processes, open connections, or in-memory state survives.

That's enough for the common case: boot a database, run migrations and seed data,
checkpoint it, and every later `Container::from_checkpoint(&cp)` restores a
fully-migrated-and-seeded database in the time it takes to boot the image, with no
re-migration and no re-seeding. It is not enough for anything that depends on live
process state (an in-flight transaction, a warmed in-memory cache, an open
connection) — that needs true memory snapshotting, which stays on the
[roadmap](./roadmap.md) pending upstream microsandbox support.

## Capability

`capabilities().checkpoint` — `true` on docker (implemented via the engine's image
commit), `false` on microsandbox (no commit primitive; see
[Unsupported: microsandbox](#unsupported-microsandbox) below).

```rust,ignore
use rightsize::backends;

let caps = backends::active().capabilities();
if caps.checkpoint {
    // running on docker: checkpoint/restore is available
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

// Later, in this process or a later one:
let restored = Container::from_checkpoint(&checkpoint).start().await?;
```

`checkpoint()` requires the guard to be currently running — calling it on a stopped
or never-started guard is a state error, the same shape as `exec`/`logs`. On
success it returns a `Checkpoint`:

```rust,ignore
pub struct Checkpoint {
    pub image_ref: String,   // "rightsize/checkpoint:<12 hex chars>", random per checkpoint
    pub spec: ContainerSpec, // the source container's full spec at checkpoint time
}
```

`Container::from_checkpoint(&checkpoint)` builds a normal `Container` whose image is
`checkpoint.image_ref` and whose env, command, exposed ports, and memory limit
default to the source container's — everything a restored container needs to behave
like the original. Every ordinary builder still works on the result, so a caller can
override anything before `.start()`:

```rust,ignore
let restored = Container::from_checkpoint(&checkpoint)
    .waiting_for(Wait::for_log_message("database system is ready", 1))
    .start()
    .await?;
```

Deliberately **not** carried over from the checkpoint's spec: mounted files, network
membership, and aliases. The checkpoint image already has whatever those mounts
wrote baked directly into its filesystem, and network topology has no well-defined
meaning to replay across a restore.

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
cost of one image-commit up front instead of N re-runs of migrate-and-seed.

## Unsupported: microsandbox

`checkpoint()` on the microsandbox backend returns a typed
`RightsizeError::CheckpointUnsupported` — checked against `capabilities().checkpoint`
before any backend call is made, so calling it never even reaches the msb CLI:

```text
Checkpoint/restore was requested but the active 'microsandbox' backend does not support
it — set RIGHTSIZE_BACKEND=docker to checkpoint (see the checkpoints docs for the
microVM-memory-snapshot roadmap item)
```

microsandbox has no image-commit primitive to build a checkpoint from today. True
microVM memory snapshots — which would make *both* backends checkpoint-capable, and
make restore near-instant since a live process's memory state would survive too —
need upstream microsandbox support; see the [roadmap](./roadmap.md).

## Image cleanup

Checkpoint images are images, not containers — they are never auto-reaped by
[orphan reaping](./reaping.md), the [cleanup thread](./how-it-works.md), or any
other own-run cleanup path. Every checkpoint you take is on disk until you remove
it:

```sh
docker rmi $(docker images -q rightsize/checkpoint)
```
