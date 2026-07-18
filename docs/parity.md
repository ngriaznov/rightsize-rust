# Cross-Language Parity

## The claim

The same container spec produces the same observable behavior whether it's built
in Kotlin, Rust, or TypeScript, on either backend. A `Container` built from a given
image, env, command, ports, mounts, and options starts the same way, exposes the
same mapped ports, streams logs the same way, reaps the same way after a crash, and
computes the same reuse identity — regardless of which language wrote the spec or
which of the two backends is running it. This isn't an aspiration: every row in the
table below is verified by each port's own shared contract suite, run against both
backends, on every change to a `SandboxBackend` implementation.

## Verified behavior areas

| Area | What's verified |
|---|---|
| Lifecycle | Start, stop, and idempotence — a second `stop()` on an already-stopped guard is a no-op, not an error. |
| Host port mapping | A container's exposed port is published to `127.0.0.1` on both backends, readable back via the guard. |
| Env / command propagation | Environment variables and a custom command are visible inside the running workload. |
| File copy-in | A bundled resource and a host path both round-trip into the guest at the requested path at start time (`with_copy_file_to_container`); the default mount is read-only. |
| Runtime file copy | `copyFileToContainer` / `copyContentToContainer` / `copyFileFromContainer` round-trip files, in-memory content, and directories against a RUNNING container on both backends; destination parents are created automatically; both operations require a running container and fail with a typed error otherwise. |
| Exec | Real exit codes and stderr come back from an in-guest command. |
| Logs + follow | Captured stdout, `for_log_message` waiting on it, and `follow_output`'s ordered no-duplicate streaming — including each backend's own follow-channel quirk (msb's watchdog-driven tail replay vs. docker's natural stream close). |
| Wait strategies and budgets | Port, HTTP, and log-message waits all honor a configured startup timeout. |
| Networks and aliases | Alias-based resolution reaches a peer container by name on both backends, including the microVM backend's exec-tunneled emulation. |
| Boot-failure retries | State-db migration races and image-cache corruption self-heal with an automatic single retry rather than surfacing to the caller. |
| Reaping ledger + sweep | A run's ledger files are written before create and removed after a clean stop; a fabricated dead run's sandbox is provably reaped, at the backend level, by a fresh process's init-time sweep. |
| Reuse gating + identity hash | The double opt-in (`.reuse(true)` + `RIGHTSIZE_REUSE`), adoption of an already-running spec-identical sandbox, and the identity hash's pinned cross-language test vector — see below. |
| Capabilities | Each backend reports its pinned `hardwareIsolated`/`checkpoint` values — microsandbox: isolated (its own kernel per sandbox) AND checkpoint-capable via disk snapshot; docker: not isolated (shared host kernel) but checkpoint-capable via image commit. |
| `requireIsolation` gating | A container requiring hardware isolation refuses to start on a non-isolated backend and starts normally on a hardware-isolated one. |
| Diagnostics report format | The exact rendered shape of a live-container diagnostics report — see below. |
| Checkpoint gating | `checkpoint()` succeeds on both real backends and throws a typed, backend-naming error on a backend without the capability, before any backend call; restoring a checkpoint under a different active backend than the one that created it fails with a typed mismatch error before any backend work. |
| Named checkpoints | A checkpoint created with a name persists a registry entry (one JSON file per name under the rightsize cache directory, pinned field names) and is rediscoverable in any later process via find/list/remove; re-checkpointing a name replaces its artifact and entry; a stale entry whose artifact is gone resolves to absent and is cleaned up. |
| Checkpoint export/import | `exportTo` writes a self-describing archive (pinned metadata plus the backend artifact) for a checkpoint created by the active backend; `importFrom` materializes it on a machine running the same backend, re-registers a named checkpoint with replace semantics, and returns a restorable checkpoint; a backend mismatch or malformed archive fails with a typed error before any backend work. |

## The pinned identity-hash vector

Container reuse identifies a sandbox by `sha256` over a canonical JSON
serialization of its reuse-relevant spec. That hash has to come out identical no
matter which language computed it, so one exact input is pinned as a
cross-language test vector and checked against a fixed output in every port:

```text
{"image":"redis:7-alpine","env":{"A":"1","B":"2"},"command":[],"exposedPorts":[6379],"memoryLimitMb":null,"copies":[]}

sha256 -> 799aad5a3338ce3d36999c7ff2733d4673c0592d417563f334544693ec1907a5
```

The resulting sandbox name — `rz-reuse-` plus the first 12 hex characters of that
hash — is `rz-reuse-799aad5a3338`, and this crate's contract suite starts a real
container from that exact spec and asserts the backend received exactly that name.
See [Container Reuse](./reuse.md#identity-what-busts-the-hash) for the full
canonical-JSON shape.

## Sibling implementations

- **Kotlin**: [rightsize-kotlin](https://github.com/ngriaznov/rightsize-kotlin) —
  docs at [ngriaznov.github.io/rightsize-kotlin](https://ngriaznov.github.io/rightsize-kotlin/)
- **Rust** (this repo): [rightsize-rust](https://github.com/ngriaznov/rightsize-rust) —
  docs at [ngriaznov.github.io/rightsize-rust](https://ngriaznov.github.io/rightsize-rust/)
- **TypeScript**: [rightsize-node](https://github.com/ngriaznov/rightsize-node) —
  docs at [ngriaznov.github.io/rightsize-node](https://ngriaznov.github.io/rightsize-node/)

## How the contract is enforced

Every behavior in the table above is a real test against a real backend, not a
document anyone has to keep in sync by hand:
[`crates/rightsize-modules/tests/contract.rs`](https://github.com/ngriaznov/rightsize-rust/blob/main/crates/rightsize-modules/tests/contract.rs)
is a single suite of `#[tokio::test]` functions, parameterized at runtime by
`RIGHTSIZE_BACKEND` rather than compiled twice, so the same test bodies run
unchanged against microsandbox and docker:

```sh
RIGHTSIZE_BACKEND=docker       cargo test -p rightsize-modules --features sandbox-it --test contract
RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test contract
```

The Kotlin and Node ports carry the equivalent suite in their own repos
(`BackendContractTest` and `test/it/contract.test.ts` respectively) — same
behaviors, same assertions, adapted to each language's test framework and async
model. Any change to a `SandboxBackend` implementation, in any of the three repos,
is expected to pass its contract suite against both backends before merging; that
discipline is what keeps this page true rather than aspirational.
