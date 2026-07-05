# Backends

rightsize-rust picks a backend automatically; override with
`RIGHTSIZE_BACKEND=microsandbox|docker`. Both satisfy the same `SandboxBackend`
contract (verified by a shared contract test suite in
`crates/rightsize-modules/tests/contract.rs`) — code you write against `Container`
runs unchanged on either.

## Selection

| Platform | Backend used |
|---|---|
| macOS (Apple Silicon) | microsandbox (microVMs) |
| Linux x86_64 / arm64 with `/dev/kvm` | microsandbox (microVMs) |
| Intel Mac | Docker (auto-fallback) |
| Windows | Docker (auto-fallback) |
| Linux without KVM | Docker (auto-fallback) |

Resolution logic (`rightsize::backends::resolve`), precisely:

- If `RIGHTSIZE_BACKEND` names a provider (case-insensitive), that provider is used
  even if a higher-priority one is also registered — and it's an error if the named
  provider isn't supported on this host (the error names the specific reason).
- Otherwise, the highest-priority *supported* provider wins.
- An empty provider list, or a list where nothing is supported, is an error listing
  every provider's `unsupported_reason`.
- An unknown `RIGHTSIZE_BACKEND` name is an error listing every known provider name.

Resolution happens once per process and is cached — the env var is read exactly once,
at the first `Container::start()`/module `start()` call.

## Configuration

| Env var | Effect |
|---|---|
| `RIGHTSIZE_BACKEND` | Force `microsandbox` or `docker`. |
| `MSB_PATH` | Use a pre-installed `msb` binary; skip downloads. |
| `RIGHTSIZE_CACHE_DIR` | Relocate the runtime cache (default `~/.cache/rightsize`). |
| `RIGHTSIZE_MSB_SKIP_DOWNLOAD` | `true` = fail instead of downloading (air-gapped CI). |

## microsandbox deep-dive

### Provisioning

Covered step-by-step in [Getting Started](./getting-started.md#what-happens-on-first-run):
download, SHA-256-verify, atomically install (`krun` before `msb`, so `msb`'s presence
is the completion marker), cache under `~/.cache/rightsize/msb/<version>/`, guarded by
a cross-process file lock. `MSB_PATH` bypasses the whole pin — bring your own binary,
any version, at your own risk of behavioral drift from what this crate was tested
against.

### Attached-mode supervision

Each container is a held child process supervising its own microVM — the image's
`ENTRYPOINT` runs exactly as it would under Docker. This matters because **detached**
`msb run -d` never actually starts the image's `ENTRYPOINT` on this msb build —
attached mode is the only mode that does. Readiness of the sandbox itself (not the
workload inside it) is "name shows `Running` in `msb ls --format json`"; workload logs
come from `msb logs`, which has its own quirk (see
[Backend differences](#backend-differences) below).

`msb exec` blocks until stdin reaches EOF, so every child process rightsize-rust spawns
under the hood gets a closed/null stdin — an exec call that's fed an open, never-EOF'd
stdin (piping in a live process's output, say) hangs forever. This is the single most
common way to accidentally wedge an `exec()` call — see
[Troubleshooting](./troubleshooting.md).

### Cache

Both the provisioned toolchain (`~/.cache/rightsize/msb/<version>/`) and pulled
container images live under the msb cache root. Seed an image once if you expect
rate-limiting on first pull (see [Troubleshooting](./troubleshooting.md) for the
Redpanda case specifically):

```sh
docker save redpanda/redpanda:<tag> | msb load -t redpanda/redpanda:<tag>
```

### Networking is emulated, not native

Covered in full in [Networking](./core-concepts/networking.md#what-each-backend-actually-does-under-the-hood):
`/etc/hosts` + an exec-tunneled TCP relay, because microVMs share no bridge network
with each other on this msb build.

## Docker deep-dive

### Unix-socket-only, and why

`rightsize-docker` is a from-scratch client over `tokio::net::UnixStream` — no
`bollard`, no `hyper`. This is a binding decision, not an oversight: a shared HTTP
stack that some *other* dependency in a consumer's tree can independently upgrade is a
real, previously-observed failure mode (a `httpclient5` bump misrouting a JVM Docker
client onto plaintext TCP port 2375 in the wild). Hand-rolling the client means this
crate's dependency tree structurally cannot be the reason a Docker client gets
misrouted onto TCP by an unrelated bump elsewhere in your tree — the whole surface is
exactly the daemon endpoints this backend uses, talking to the daemon over a plain
unix-socket HTTP connection, always.

Chunked transfer decoding and the daemon's log-stream multiplexing frame format
(`[stream_type: u8, 0, 0, 0, len: u32_be, payload]`) are still hand-parsed — that's
wire transport, the same binding decision as the socket choice above. The JSON layer
itself is ordinary `serde`/`serde_json`: derived structs for the request bodies this
backend sends and the response fields it reads (`Id`, `ExitCode`, network id,
container-list ids).

### Cleanup by label

Containers are labeled `dev.rightsize.run_id=<RunId>`; the orphan reaper lists and
force-removes this run's labeled leftovers on `close()`, and a separate reaper at
startup cleans up labeled containers from a *crashed prior run*. Networks are created
and removed explicitly. `host.docker.internal:host-gateway` is added as an extra host
entry on every container, so containers can reach services on the host uniformly
across platforms.

### Port-bind-conflict detection

The Docker daemon has no distinct exception type for "this port is already bound" —
it returns a plain 500 with a message containing `"address already in use"` or `"port
is already allocated"`. This backend classifies by message content and re-throws the
typed `RightsizeError::PortBindConflict`, which is what lets
`Container::start()`'s retry loop (see
[Containers & Guards](./core-concepts/containers-and-guards.md#the-raii-lifecycle))
recognize it without string-matching at the core layer.

## Backend differences

The two backends are contract-equivalent on everything the shared suite exercises,
but a few edges are real, not just timing quirks:

- **Read-only file mounts aren't enforced in-guest on microsandbox 0.6.2.**
  `FileMount::read_only` is honored by Docker (the bind mount is genuinely read-only
  inside the container); on microsandbox the guest currently gets a writable mount
  regardless of the flag. Don't rely on guest-side write protection under
  `RIGHTSIZE_BACKEND=microsandbox`.
- **`follow_output`'s tail-flush on microsandbox is a watchdog, not a stream close.**
  `msb logs -f` doesn't exit when its sandbox stops (a documented gap in msb 0.6.2),
  so this backend polls in the background and replays only the not-yet-delivered tail
  once the sandbox is confirmed stopped. Consumers see the same ordered,
  no-duplicate output either backend produces — this is an implementation detail, not
  an observable behavior difference — but it means a `follow_output` subscriber on
  microsandbox can see its last line arrive slightly after the sandbox itself reports
  stopped, rather than exactly at stream EOF.
- **Network-alias tunnels on microsandbox serve one connection at a time.** See
  [Networking](./core-concepts/networking.md#limits-on-the-microvm-backend) — a real
  capability gap versus Docker's native bridge networking, not a timing quirk.
- **Readiness-probe caveats apply to both backends**, not just microsandbox — see
  [Wait Strategies](./core-concepts/wait-strategies.md#the-read-probe-story). A
  userland proxy/forwarder on *either* backend can accept a connection before the
  guest process is actually listening.

## Wiring a backend explicitly

Covered in [Getting Started](./getting-started.md#wiring-a-backend-yourself-when-not-using-rightsize-modules-defaults) —
register whichever provider(s) you want via
`rightsize::backends::register_provider` once per process, before the first
`Container::start()`.
