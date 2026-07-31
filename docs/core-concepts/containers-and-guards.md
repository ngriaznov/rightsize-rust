# Containers & Guards

## The builder

`Container::new(image)` starts a builder with no exposed ports, no env, the default
[`Wait::for_listening_port`](./wait-strategies.md) readiness check, and every other
option at its default. Every `with_*` method takes `self` by value and returns `Self`,
so calls chain:

```rust,ignore
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

The builder surface, as it exists today:

| Method | Effect |
|---|---|
| `Container::new(image)` | Starts building from an image reference. |
| `.with_env(k, v)` | Sets an env var. Calling it again with the same key overwrites the value but keeps the key's *first* position — last-value-wins, first-position-wins, exactly `LinkedHashMap` semantics. |
| `.remove_env(key)` | Removes every entry for `key` set so far. No-op if never set. Needed when a later env var must retract an earlier one's *effect on the entrypoint*, not just be shadowed by value — see [`ArangoContainer::with_root_password`](../modules/arango.md) for the worked example. |
| `.with_exposed_ports(&[..])` | Declares guest ports to publish; each gets a host port assigned before boot. |
| `.with_command(&[..])` | Overrides the image's default entrypoint/command. |
| `.with_network(&net)` | Joins an `Arc<Network>` — see [Networking](./networking.md). |
| `.with_network_aliases(&[..])` | Names this container is reachable as on its network. |
| `.with_copy_file_to_container(file, guest_path)` | Mounts a `MountableFile` read-write into the guest; the mount is a view of the host file, so guest writes reach it — see [Files & Resources](./files-and-resources.md). |
| `.waiting_for(strategy)` | Overrides the readiness check; defaults to `Wait::for_listening_port()`. |
| `.with_memory_limit(megabytes)` | Caps guest memory. Unset means "each backend's own default" — see [Files & Resources](./files-and-resources.md#memory-limits-when-and-why). |
| `.start()` | Consumes the builder, returns `Result<ContainerGuard>`. |

`Container::start()` **consumes** the builder — there's no mutate-in-place `start()`
followed by later getters on the same object, unlike Testcontainers-for-Java. The
guard returned is the thing you hold for the rest of the test.

## The RAII lifecycle

`start()` does the following, in order, and on **any** failure partway through, tears
everything back down before the error ever reaches your code:

1. If the container joins a `Network`, ensure the network exists on the backend.
2. Allocate host ports for every exposed guest port (see the port-bind-conflict retry
   note below).
3. Create the container on the active backend, then start it.
4. If networked: install network links to every already-running sibling, then
   register this container as a network member.
5. Run the wait strategy until ready (or time out).
6. If the module set a post-start hook (e.g. MongoDB's replica-set initiation — see
   [MongoDbContainer](../modules/mongodb.md)), run it.

If step 4, 5, or 6 fails, `start()` awaits this container's own `stop()` to completion
— stop, remove, release ports — **before** returning the error. A container that
fails partway through boot never leaks a running process or a held port; you don't
need a `try`/cleanup dance around a failed `start()` call.

**Port-bind-conflict retries.** Allocating a host port and actually binding it are two
separate steps with an unavoidable race (something else grabs the port between
allocation and bind). `start()` retries the whole create+start step up to 5 times with
freshly allocated ports whenever the failure is classified as a port-bind conflict —
recognized either as the typed `RightsizeError::PortBindConflict` (checked first, by
walking the error's `source` chain) or by matching known message phrasings
(`"address already in use"`, `"already allocated"`) as a fallback for backends that
don't throw the typed variant. Any other failure is not retried — it fails fast.

## `stop().await` vs `Drop` — the cleanup story

Two-tier cleanup, because Rust has no async `Drop`:

**Happy path:** `guard.stop().await` is an explicit, awaited, ordered teardown —
backend `stop`, then `remove`, then release the guard's mapped host ports back to the
process-wide allocator. It's idempotent: calling the internal teardown twice (which
can only happen via `Drop` running after an explicit `stop()`, since `stop()` consumes
the guard by value) never double-calls the backend or double-releases a port.

**Fallback:** if a guard is simply dropped — a test panics mid-body, a scope exits
early without calling `stop()` — `Drop` still tears the container down. `Drop` cannot
be `async`, and it must work correctly even with **no Tokio runtime anywhere in the
process** (a plain `Drop` can run during runtime shutdown, or on a thread that never
had a runtime at all). So the fallback does the least synchronous work that's still
correct:

1. Release the guard's mapped host ports **synchronously** — `FreePorts` is a plain
   `std::sync::Mutex`, no runtime needed — so a dropped-not-stopped guard never leaks
   its ports even if the next step is slow, or never runs at all.
2. Hand a small `{ backend, container_id }` teardown descriptor to a dedicated,
   process-global cleanup **OS thread** (started once via `std::thread`, not Tokio)
   and return immediately.

That cleanup thread drains a channel of these descriptors and tears each one down
using **blocking std I/O only** — `std::process::Command` for msb `stop`/`rm`, a
blocking `std::os::unix::net::UnixStream` for docker's DELETE calls. Never Tokio,
never `block_on`, because this thread has no async runtime and must not assume one
exists anywhere else in the process either. A panic tearing down one job is caught
(`std::panic::catch_unwind`) so it can't take the whole cleanup thread down for the
rest of the process.

This is the direct analogue of a JVM shutdown hook, made crash-tolerant. But `Drop`
itself never runs on a bare `SIGKILL` — nothing running in userland survives that —
which is why both backends *also* run a **run-id-scoped orphan reaper** at startup:
it lists sandboxes/containers, filters out anything belonging to *this* run, and
force-removes the rest — leftovers from a prior process that died hard enough to skip
`Drop` entirely. The graceful path (`Drop` → cleanup thread) is the backstop for a
panicking test; the reaper is the backstop for the backstop.

```text
stop().await          -> ordered async teardown (backend stop, remove, ports)   [happy path]
drop(guard)            -> sync port release + cleanup-thread teardown           [fallback]
process SIGKILL        -> Drop never runs; next process's reaper cleans it up   [backstop]
```

## `is_running()`, `get_mapped_port()`, `exec()`, `logs()`, `follow_output()`

Once you have a `ContainerGuard`:

- `guard.host()` — always `"127.0.0.1"`; published ports are loopback-only.
- `guard.get_mapped_port(guest_port)` — the host port a declared guest port is
  published on. Distinguishes two failure causes in its error message: the guard
  isn't running at all (never started, or already stopped), versus the guard is
  running but that guest port was never declared via `with_exposed_ports`.
- `guard.is_running()` — `true` from a successful `start()` until `stop()`.
- `guard.exec(&["cmd", "arg"]).await` — runs a command inside the running container,
  returns `ExecResult { exit_code, stdout, stderr }`.
- `guard.logs().await` — the full captured logs so far.
- `guard.follow_output(|line| ...).await` — streams log lines to a closure as
  they're produced; dropping (or explicitly closing) the returned `FollowHandle`
  halts delivery — no further lines reach the closure even if the container keeps
  running. See [Backend differences](../backends.md#backend-differences) for a
  microsandbox-specific timing nuance in how the *last* line is delivered.
- `guard.copy_file_to_container(...)`/`copy_content_to_container(...)`/
  `copy_file_from_container(...)` — runtime file copy against an already-running
  container, on either backend. See [Copying Files](../copy.md).
- `guard.checkpoint().await` — captures the running container's filesystem state
  for `Container::from_checkpoint(...)` to restore later; `guard.checkpoint_named(name).await`
  does the same but persists a registry entry a later process can rediscover. See
  [Checkpoint / Restore](../checkpoints.md).

## The `OnceCell` shared-container recipe

Covered in full in [Getting Started](../getting-started.md#the-shared-container-recipe)
— the short version: build the container once behind a
`tokio::sync::OnceCell<Arc<SomeGuard>>`, clone the `Arc` into every test that needs it,
and never call `stop()` on the shared instance. This is the RAII-guard analogue of a
JUnit `@Container` static-scope field — a documented pattern, not extra API surface.
