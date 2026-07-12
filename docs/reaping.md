# Orphan Reaping

While your test process is alive, normal lifecycle handles cleanup: `stop(self)` on
the happy path, `Drop`'s synchronous fallback (see
[Containers & Guards](./core-concepts/containers-and-guards.md)) if a guard is simply
dropped. The only event that can leave a sandbox running forever is the *process*
dying without either of those getting a chance to run — a bare `SIGKILL`, an OOM-kill,
a crashed CI step. Reaping exists for exactly that case.

## Two layers

### Run records + init-time sweep (always on)

Under the [rightsize cache dir](./backends.md#configuration) (`~/.cache/rightsize` on
macOS/Linux, `%LOCALAPPDATA%\rightsize` on Windows — the same directory the msb
provisioner uses, and the same one a Kotlin/Node/Rust process on this host all agree
on):

- `runs/<run-id>.json` — written atomically, *before* this process's first sandbox is
  created: the process id, the ISO-8601 instant the *process* started (not the record
  — this defeats pid reuse), the active backend's name, and the provisioned `msb`
  binary path when that's the backend.
- `runs/<run-id>.sandboxes` / `runs/<run-id>.networks` — one name/id per line,
  appended *before* the backend actually creates the resource and removed *after* a
  successful stop/remove. The file is always a superset of what's actually live.

At first backend use, right after resolution succeeds (once per process — no extra
processes, no extra thread), every *other* run's record on disk is checked:
unparseable-and-fresh is left alone (it might be another process mid-write); dead
(no process with the recorded pid, or a pid whose actual start time doesn't match
the recorded one within 2 seconds) means every name in that run's `.sandboxes` is
removed via this process's own backend, then every id in `.networks` too (`in use`
errors ignored — a network the crashed run's own containers are still attached to
until they're removed is expected, not a failure), `not found` errors included
either way — the sweep is idempotent and may race a sibling process's own sweep of
the same leftover. **A process only reaps a dead run whose recorded backend matches
its own** — a docker process cannot remove msb sandboxes, so an msb run's leftovers
wait for the next process that resolves to msb (and vice versa).

On clean shutdown — the last sandbox stopping with no networks left either —
the three files for this run are deleted. A crash leaves them in place for the next
sweep to find.

### Watchdog (default on, per run, not a daemon)

A small detached script (POSIX `sh` on macOS/Linux, PowerShell on Windows) is
written to `<cache dir>/reaper/` and spawned once per process, right before the
first sandbox is created. It blocks reading its stdin, held open by a pipe whose
write end only this process holds (never inherited by any other child this process
spawns — the one detail that makes the whole mechanism work: a leaked write end means
the watchdog's stdin never reaches EOF). On EOF — this process exited, cleanly or via
`SIGKILL` — it removes this run's sandboxes and networks via backend-CLI commands
(`docker rm -f`, or msb's `stop`+`rm` with one retry on msb's own state-database
error) and deletes the run's record files, then exits. An empty/never-populated
`.sandboxes` (the clean-shutdown case) just means it deletes the record files and
exits — no clean/crash signaling needed, since this is idempotent either way.

This is the fast path: seconds after a crash, instead of waiting for the next
process's init-time sweep.

## `RIGHTSIZE_REAPER`

| Value | Effect |
|---|---|
| `on` (default) | Run records + init-time sweep + watchdog. |
| `sweep` | Run records + init-time sweep only — no watchdog process. |
| `off` | Neither. No run record is written at all — nothing would ever read it. |

Unknown values are treated as `on`.

## What clean shutdown does

`ContainerGuard::stop(self)` removes the sandbox's line from `.sandboxes` right after
the backend confirms it's gone. Once both `.sandboxes` and `.networks` are empty, the
three ledger files for this run are deleted — the next container this process starts
(if any) recreates them from scratch.

## The crash story

A `Drop`-only teardown (no explicit `stop()`, process still alive) is already covered
by the cleanup thread described in
[Containers & Guards](./core-concepts/containers-and-guards.md) — reaping is the
backstop for the case that thread itself never runs: the process is gone before it
gets the chance. `SIGKILL`'d mid-test, `Drop` never fires *at all* (nothing survives
that signal) — the watchdog is what notices, within seconds. If the watchdog itself
somehow didn't fire (its script failed to write, the process was frozen rather than
killed and the OS never actually closed the pipe), the next process on this host that
resolves the same backend reaps it at startup instead — bounded by however long until
that next process runs, not indefinitely.

## Reuse immunity

A `keep_alive` sandbox — the flag [Container Reuse](./reuse.md) sets — is never
listed in `.sandboxes` in the first place, so it is structurally immune to both the
sweep and the watchdog: there is no line naming it for either to act on. The msb
backend also excludes it from its own shutdown bookkeeping, and the docker backend
labels it `dev.rightsize.reuse=<hash>` instead of
`dev.rightsize.run_id=<run-id>`, so `close()`'s run-id-scoped listing structurally
cannot match it either.

## The docker-remote caveat

A local watchdog process cannot outlive the machine it runs on — if a CI VM is torn
down mid-test, no local process survives to reap anything, watchdog included. This is
only a real gap for a **remote** docker daemon: containers created against a daemon
running on a different host (or one that outlives the VM issuing commands, e.g. a
shared CI docker-in-docker setup) can outlive the caller entirely. The next process
that runs a test against that *same daemon* still reaps them at its own init-time
sweep — the sweep only needs the ledger's cache dir to be reachable by a *later*
process, not the watchdog's own process to have survived. A fully isolated,
one-VM-per-run docker daemon (the common case) has no such gap: the daemon and every
container on it die with the VM regardless of reaping.

## Troubleshooting

See [Troubleshooting](./troubleshooting.md#sandboxes-left-behind-after-a-crashed-test-run)
for what to check if a sandbox survives anyway.
