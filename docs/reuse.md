# Container Reuse

A container marked for reuse survives process exit: the next equivalent `start()` —
in this process or a later one — ADOPTS the same sandbox instead of booting a fresh
one. `stop()` on a reuse-active guard leaves the sandbox running; that's the feature.

## Double opt-in

Reuse only activates when BOTH are true:

- the container is marked via `.reuse(true)`;
- `RIGHTSIZE_REUSE` is set to exactly `"true"` or `"1"` in the process environment.

```rust,ignore
use rightsize::Container;

let guard = Container::new("redis:7-alpine")
    .with_exposed_ports(&[6379])
    .reuse(true)
    .start()
    .await?;
guard.stop().await?;
```

`.reuse(true)` with `RIGHTSIZE_REUSE` unset (or anything other than `"true"`/`"1"`)
behaves exactly like an ordinary ephemeral container — no adoption, no registry file,
`stop()` tears it down normally — with a single stderr note that reuse was requested
but not enabled. Neither knob alone does anything; both are required, on purpose: an
env var that's easy to forget to set locally, and impossible to set by accident on a
CI runner that never exports it.

## Identity: what busts the hash

A reuse container's identity is `sha256` over a canonical JSON serialization of:

```text
{image, env (sorted by key), command, exposedPorts (sorted), memoryLimitMb,
 copies: [{guestPath, sha256(content)}] sorted by guestPath}
```

Two `Container`s hash identically — and therefore adopt the same sandbox — only if
their image, environment (regardless of the order `.with_env` was called in), command,
exposed ports, memory limit, and every copied-in file's content all match exactly.
Change any one of those and the next `start()` computes a different hash, a different
`rz-reuse-<12hex>` name, and boots a brand-new sandbox alongside the old one (which
still needs manual cleanup — see below).

Notably NOT part of the identity: network membership (see
[Unsupported: custom networks](#unsupported-custom-networks-together) below), wait
strategy, aliases, and the mount's `read_only` flag.

The identity hash and its canonical JSON shape are a cross-language contract: the
same logical spec hashes identically whether it was built in this crate, the Kotlin
port, or the Node port, pinned by a fixed test vector in each implementation.

## Sandbox naming

`rz-reuse-<first 12 hex chars of the identity hash>` — deterministic, not
process-unique like an ordinary container's `rz-<run-id>-<seq>`. This is what makes
adoption possible: two processes building an identical spec compute the identical
name and can find each other's sandbox by it.

## The registry

`<cacheDir>/reuse/<full-hash>.json` (same [cache dir](./backends.md#configuration)
the reaping ledger uses), written atomically after the reused container first starts
successfully and passes its wait strategy:

```json
{
  "name": "rz-reuse-<12hex>",
  "image": "redis:7-alpine",
  "ports": { "6379": 32768 },
  "createdIso": "2025-01-01T00:00:00Z",
  "backend": "microsandbox"
}
```

## Start flow

1. **Registry file exists** — verify the sandbox is actually running (a real backend
   query, not just "the file says so"), then re-run the container's own wait strategy
   against the registry's recorded host ports. Success: ADOPT — mapped ports come
   from the registry, no `create()` call on the backend, `is_running()` is `true`
   immediately. Failure (not running, the wait strategy fails, or the file doesn't
   parse) — best-effort remove whatever's actually there and the registry file, then
   fall through to step 2.
2. **No registry file** (or the fallback above) — allocate host ports normally,
   create under the `rz-reuse-<12hex>` name, run the wait strategy, and on success
   write the registry file.
3. **Name collision on create** — another process won the race to create the same
   identity's sandbox first. Re-enter the adopt path (step 1) once, reading whatever
   the winner has by then written; if that still doesn't pan out, the original
   collision error surfaces.

## Crash-mid-boot orphan recovery

Step 2's registry write happens only *after* the fresh sandbox's own wait strategy
has succeeded — so a process that crashes, or fails its own wait, after `create()`
but before that write leaves a RUNNING sandbox under the identity's fixed name with
**no registry entry pointing at it at all**. Because a reuse sandbox is `keep_alive`
(see [Interaction with reaping](#interaction-with-reaping)), no watchdog or sweep
ever touches it — invisible to reaping is `keep_alive`'s whole point, and that holds
here too, for better or worse. Left alone, the next fresh-create of the same identity
would collide with it: docker at least rejects the create with a 409 on the name, but
microsandbox has no such guard at all and happily starts a *second* workload under
the same sandbox name — the two then fight over in-guest ports, and every future
start of that identity times out until someone removes it by hand.

Step 2 closes that gap itself: once the adopt path above has concluded there is no
usable registry entry (missing, corrupt, or failed verification), `start()` asks the
backend directly — the same `find_running` query the adopt path itself uses — whether
a sandbox under the identity's fixed name is already running, and if so, best-effort
removes it *before* attempting the fresh create. This is deliberately narrower than
step 3's name-collision retry: there, a `create()` conflict means another *live*
process just won the race and is (or is about to be) the one writing the registry
entry, so that sandbox is adopted, never removed — removing it out from under a live
concurrent creator would defeat the entire point of reuse.

## Stop semantics

`stop()` on a reuse-active guard clears only in-process bookkeeping (the handle, the
mapped-port cache) — no `backend.stop`/`backend.remove` call, and the sandbox is
never listed in the [reaping](./reaping.md) ledger at any point, so no sweep or
watchdog ever touches it either. The container keeps running, holding its host ports,
until something removes it explicitly.

There is no `remove()`/full-teardown API in this release — clean it up with the
backend's own tooling:

```sh
# docker
docker rm -f <name>

# microsandbox
msb stop <name> && msb rm <name>
```

or delete `<cacheDir>/reuse/<hash>.json` and let the next `start()` for that identity
create a fresh sandbox (the stale one becomes an ordinary orphan at that point — worth
removing by hand rather than leaving it running indefinitely).

## Unsupported: custom networks together

`.reuse(true)` combined with `.with_network(...)` fails fast at `start()` with a typed
error (`RightsizeError::ReuseNetworkConflict` in this crate) rather than silently
ignoring one or the other: the identity hash has no concept of network topology, so
"which network is this adopted sandbox on" has no well-defined answer. Drop either
`.reuse(true)` or `.with_network(...)`.

## Interaction with runtime copy

`copy_file_to_container`/`copy_content_to_container`/`copy_file_from_container`
(see [Copying Files](./copy.md)) work against a reuse sandbox exactly like any
other runtime operation — but they mutate SHARED reused state, and they are **not**
part of the reuse identity hash. Two `Container`s with an identical spec still
adopt the same sandbox even if one process has copied files into it that another
adopting process never touched; if a workload's correctness depends on a copied-in
file's presence or content, tracking that dependency is on the caller, not the
identity hash.

## Interaction with reaping

A reuse sandbox is `keep_alive` under the hood — see
[Orphan Reaping's reuse immunity](./reaping.md#reuse-immunity) for the full mechanism.
In short: it's excluded from every own-run cleanup path (the reaping ledger, the
`Drop`-path cleanup thread, each backend's own run-scoped bookkeeping) from the moment
it's created, by construction, not by a special case bolted on afterward.

## CI guidance

**Do not enable `RIGHTSIZE_REUSE` on ephemeral CI.** A fresh CI runner gets nothing
back from reuse (there's no warm sandbox from a "previous run" to adopt — every run
starts a clean machine) and pays the cost of a sandbox that outlives the test process,
which then needs its own cleanup step or it accumulates across runs on a
long-lived/self-hosted runner. Reuse earns its keep in a local dev loop, where the
same machine runs the same test suite repeatedly and skipping the boot is the entire
point.

## Troubleshooting

See [Troubleshooting](./troubleshooting.md#sandboxes-left-behind-after-a-crashed-test-run)
if a reuse sandbox needs to be found and removed by hand.
