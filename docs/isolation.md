# Isolation Requirement

The two backends give meaningfully different isolation guarantees. Most tests never
need to care — but a test that runs *untrusted* code (a plugin, a user-supplied
script, an artifact pulled from somewhere you don't fully control) does, and
`.require_isolation(true)` lets it demand the stronger one instead of silently
running with whatever backend happened to resolve.

## Guarantees table

| Backend | `hardware_isolated` | What that means |
|---|---|---|
| microsandbox (msb) | `true` | Each sandbox is its own microVM with its own kernel — a workload cannot see or affect the host kernel, other sandboxes, or the host process table. |
| docker | `false` | Containers are namespaces/cgroups on one shared host kernel. Strong process/filesystem isolation, but a kernel-level exploit inside the container can reach the host. |

Exposed on the backend SPI as `capabilities().hardware_isolated`:

```rust,ignore
use rightsize::backends;

let caps = backends::active().capabilities();
if caps.hardware_isolated {
    // running on msb: real microVM isolation
}
```

## When to require it

Reach for `.require_isolation(true)` when the container will run code you don't
trust — not merely code you didn't write, but code whose behavior you can't bound in
advance:

- executing a user-supplied plugin, script, or build step;
- running an artifact pulled from an untrusted or unverified source;
- any workload where "it could try to escape the container" is a real threat model,
  not a hypothetical.

Ordinary test fixtures — a database, a message broker, a well-known third-party
image you already trust — don't need it; the docker fallback's isolation is enough,
and requiring hardware isolation there would just mean the test refuses to run
without `RIGHTSIZE_BACKEND=microsandbox`.

## API

```rust,ignore
use rightsize::Container;

let guard = Container::new("untrusted/plugin-runner:latest")
    .require_isolation(true)
    .start()
    .await?;
```

Checked in `start()`, before any create or network work: if the active backend's
`capabilities().hardware_isolated` is `false`, `start()` returns
`RightsizeError::IsolationRequired` and no sandbox is created. The error names the
active backend and the fix:

```text
Hardware isolation was required (.require_isolation(true)) but the active 'docker'
backend does not provide it — set RIGHTSIZE_BACKEND=microsandbox to run on a hardware-isolated
backend, or drop .require_isolation(true) if this workload does not need it
```

## Untrusted-code guidance

Hardware isolation raises the ceiling on what a workload could do if compromised —
it doesn't replace ordinary caution about what you hand it in the first place:

- **Cap memory.** Untrusted code with an unbounded memory limit can still degrade the
  host by exhausting it. Use `.with_memory_limit(megabytes)`.
- **No secrets in env.** Environment variables are visible to `exec`/process
  inspection tools and often end up in logs. Don't pass API keys, credentials, or
  tokens the workload doesn't strictly need to an untrusted container — hardware
  isolation protects the host kernel, not a secret you handed the workload directly.
- **Prefer `require_isolation(true)` over hoping the active backend happens to be
  msb.** Without it, a host or CI runner where msb isn't available (no KVM, no
  supported OS) silently falls back to docker, and the untrusted workload runs with
  weaker guarantees than intended, with nothing failing to say so.
- **Mounts are read-write by default — protect them explicitly.** A file mount is a
  view of the host file, not a copy, so a guest write reaches the host file itself
  (see [Files & Resources](./core-concepts/files-and-resources.md)). For untrusted
  workloads, call `.read_only()` on the mount unless the workload genuinely needs to
  write back — both backends enforce it as a guest-side write block.

## Interaction with other features

`capabilities()` is independent of [reuse](./reuse.md) and [reaping](./reaping.md) —
a reused or reaped sandbox is still subject to the same `.require_isolation(true)`
check on the `start()` call that adopts or creates it, since the check runs before
either of those code paths does any backend work.
