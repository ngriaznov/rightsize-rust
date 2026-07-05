# Troubleshooting

Every entry here traces back to an empirically observed failure mode — not a
guess. Where relevant, the underlying fact is cited so you can find the exact
mechanism in [How It Works](./how-it-works.md) or the source.

## "My `exec()` call hangs forever"

**Cause:** `msb exec` blocks until its stdin reaches EOF. Every child process
rightsize-rust spawns under the hood gets a closed/null stdin for exactly this reason
— but if you're driving `exec()` with something that pipes in a live, never-closing
stream of your own, the underlying `msb exec --stream` call has no reason to ever
see EOF and simply never returns.

**Fix:** make sure whatever's feeding stdin into an `exec` call closes cleanly.
This only bites you if you're doing something unusual with a command's stdin
directly — `guard.exec(&["cmd", "arg"])` as shown throughout this book already
gives every spawned command a closed stdin and is not affected.

## "The first connection through a network alias fails, but retries work"

**Cause:** on the microsandbox backend, a network-alias tunnel's in-guest `nc -l`
listener serves exactly one connection, then gets respawned for the next. A
connection attempt that lands in the gap between "previous connection closed" and
"listener respawned" sees a connection refused/reset — a real, if narrow, timing
window, not a bug in your test.

**Fix:** for a client that only makes one request against an alias (config fetch,
health check), a short retry-on-connect-failure loop around the first call is the
practical fix. For a workload that needs a persistent, always-available connection
to a sibling container, this backend's network emulation isn't the right shape —
switch to `RIGHTSIZE_BACKEND=docker`, whose native bridge networking has no such
gap. See [Networking](./core-concepts/networking.md#limits-on-the-microvm-backend).

## "Container won't boot on microsandbox but works fine on Docker"

Three distinct, independently observed causes share this symptom. Check them in
this order:

### 1. Memory ceiling

**Cause:** microsandbox's default microVM RAM is ~450 MB. A JVM (or otherwise
memory-hungry) image whose fixed memory regions are computed above that either
hangs indefinitely or gets silently OOM-killed by the kernel — the symptom looks
like a readiness-wait timeout, not an out-of-memory error, because the process dies
before it ever gets a chance to log anything useful.

**Fix:** check the image's own baked heap/memory flags (`JAVA_OPTS`, `-Xmx`, etc.)
and set `Container::with_memory_limit(megabytes)` above that with real headroom —
not just enough to dodge the OOM killer, but enough that the workload isn't running
at 99% of its ceiling under load (a passing readiness check that starts failing
requests under memory pressure is a real, previously-measured trap; see
[Files & Resources](./core-concepts/files-and-resources.md#memory-limits-when-and-why) for the
exact measured numbers from two shipped modules that hit this).

### 2. A baked env var with a control character

**Cause:** msb 0.6.2's krun VMM builder panics with `InvalidAscii` if any boot-env
value contains a control character — and at least one official image
(`postgres:*-alpine`) bakes one into its own manifest (`DOCKER_PG_LLVM_DEPS` with a
literal tab). This happens before the guest ever boots, with zero rightsize-set env
vars involved — it's the image, not your test.

**Fix:** override the offending variable to an empty string
(`.with_env("VAR_NAME", "")`) — a no-op for whatever the image's build already
baked, and harmless on Docker. `PostgresContainer` ships this fix already; if a
*different* image hits `InvalidAscii`, inspect its baked env (`docker inspect
<image>` and look at `Config.Env`) for anything with an embedded tab/control
character and apply the same override.

### 3. Read-only mounts you're relying on

**Cause:** `FileMount::read_only` is advisory-only on microsandbox 0.6.2 — the
guest gets a writable mount regardless of the flag. If your test asserts the guest
*cannot* write to a mounted path, it'll pass on Docker (which enforces it) and fail
on microsandbox (which doesn't) — or vice versa if your test logic assumes
writability is blocked.

**Fix:** don't write a test that depends on guest-side write protection while
targeting `RIGHTSIZE_BACKEND=microsandbox`. See
[Backends](./backends.md#backend-differences).

## "A no-`nc` image fails to join a network, citing Docker"

**Cause:** microsandbox's network-alias emulation is implemented as a shelled-out
`nc` listener inside the consuming container's guest. An image without `nc`/busybox
(a scratch-based image, or one that stripped it) can't host that listener at all.

**Fix:** this is a fail-fast, not a flaky timeout — the error message already names
`RIGHTSIZE_BACKEND=docker` as the workaround. Either switch backends for this test,
or use an image that ships `nc`/busybox as the network-joining container.

## "`msb ls` output looks fine but my code can't parse it"

**Cause:** `msb ls --format json` returns a flat array of
`{created_at,image,name,status}` with `status` capitalized (`"Running"`, not
`"running"`) — and there is no plain `--json` flag, only `--format json`. If you're
scripting around `msb` directly (rather than going through rightsize-rust), a
case-sensitive or key-order-dependent parser will misfire on real output.

**Fix:** this is only relevant if you're shelling out to `msb` yourself outside of
rightsize-rust — the crate's own parser (`serde_json`, deserializing into a struct with
no key-order assumption, tolerant of extra/missing fields) doesn't have this problem.
Don't hand-roll your own key-order- or case-sensitive parser against `msb ls`'s output
if you're writing tooling of your own against `msb`.

## "Registry pulls are rate-limited / a broker image won't pull"

**Cause:** some registries (`docker.redpanda.com` specifically) rate-limit
anonymous pulls, and msb's image pulls are single-arch — a cold cache plus a
rate-limited registry can make the first `RedpandaContainer` boot in CI fail well
before any rightsize-rust-specific logic runs.

**Fix:** seed the image into the msb cache once, ahead of time:

```sh
docker save redpanda/redpanda:<tag> | msb load -t redpanda/redpanda:<tag>
```

## "My wait strategy times out even though the service looks fine in the logs"

**Cause:** a bare TCP-connect readiness check (or even the built-in read-probe) is
not the same as "the workload is ready to serve a real request." A userland
proxy/forwarder on either backend can accept a connection before the guest process
is genuinely listening, and some workloads (Memcached, MongoDB before its replica
set has a primary) have a real protocol-level gap between "accepting connections"
and "actually ready."

**Fix:** see [Wait Strategies](./core-concepts/wait-strategies.md) for the full
read-probe mechanism and when to reach past it — `Wait::for_http`,
`Wait::for_log_message`, or (following the
[`MemcachedContainer`](./modules/memcached.md) example) a custom `WaitStrategy` that
speaks the actual protocol.

## "A log-line wait fires on the wrong line" (e.g. MySQL-style multi-boot images)

**Cause:** some entrypoints print their "ready" line more than once during a
completely normal boot (a temp-server pass, a sub-component's own "ready" line that
happens to share a substring with the real one). `Wait::for_log_message`'s built-in
matcher is a plain substring/wildcard searcher with no anchors — a naive pattern can
match a decoy line before the real one appears.

**Fix:** see [`MySqlContainer`](./modules/mysql.md)'s worked example: it ships a
small custom `WaitStrategy` that anchors on the *end* of the port number
(`port: 3306` immediately followed by end-of-line or a non-digit) rather than
stretching the shared substring matcher to do anchored matching it isn't built for.
Capture a real boot log for your own image before writing the pattern — don't guess
at what the "real" ready line looks like.
