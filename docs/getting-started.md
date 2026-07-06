# Getting Started

## Install

```toml
# Cargo.toml
[dev-dependencies]
rightsize = "0.1.0"
rightsize-modules = "0.1.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`rightsize-modules` pulls in both backend crates by default (`backend-msb` and
`backend-docker` Cargo features, both on) so you don't have to wire anything up by
hand for the common case. Once a tagged release exists, the same dependency block
works with a version requirement (`rightsize = "0.1"`) instead of a `git` key —
nothing else about the API changes.

## Your first test

```rust,ignore
use rightsize_modules::RedisContainer;

#[tokio::test]
async fn cache_roundtrip() -> rightsize::Result<()> {
    let redis = RedisContainer::new().start().await?;

    let client = redis::Client::open(redis.uri()).unwrap();
    let mut con = client.get_connection().unwrap();
    redis::cmd("SET").arg("k").arg("v").execute(&mut con);
    let v: String = redis::cmd("GET").arg("k").query(&mut con).unwrap();
    assert_eq!(v, "v");

    redis.stop().await?;
    Ok(())
}
```

Run it with plain `cargo test` — no separate setup step, no daemon to start by hand.

## What happens on first run

The first time any process calls `Container::start()` (directly or via a module like
`RedisContainer`) with the microsandbox backend active, rightsize-rust provisions its
own runtime before doing anything else:

1. **Platform check.** It detects your OS/arch (macOS aarch64, or Linux x86_64/arm64
   with a readable `/dev/kvm`). No match — no `msb` build for this platform — and
   provisioning fails with an actionable error naming `RIGHTSIZE_BACKEND=docker` or
   `MSB_PATH` as the way out.
2. **Download.** The pinned `msb` release (currently `0.6.3`) is fetched from GitHub
   releases: the `msb` binary itself, the matching `libkrunfw` asset, and a
   `checksums.sha256` manifest — three separate downloads, over a small blocking
   `ureq` client (this bootstrap runs before any async work, so a blocking HTTP
   client is the honest tool for the job).
3. **Verify.** Every downloaded asset is SHA-256-checked against the manifest before
   anything is installed.
4. **Atomic install.** Both files land in a temp location first; the `krun` asset is
   moved into `lib/` *before* the `msb` binary is moved into `bin/`. The `msb`
   binary's presence is therefore the commit marker for a *complete* install — if
   the process is killed mid-install, the next run detects the half-finished state
   (missing `bin/msb`) and repairs it, rather than trusting a partial install.
5. **Cache.** Everything lands under `~/.cache/rightsize/msb/0.6.3/` (or
   `RIGHTSIZE_CACHE_DIR` if you've set it). Every later test run in every project on
   this machine reuses it — no re-download.
6. **Cross-process lock.** If two `cargo test` processes race to provision the same
   cache dir at once, the second one blocks on a lock file and then finds the first
   one already did the work.

None of this needs `sudo`, a running daemon, or a pre-installed anything. If you're
on a platform microsandbox doesn't support, rightsize-rust falls back to the Docker
backend automatically (see [Backends](./backends.md)) — the only prerequisite there
is a reachable Docker-compatible daemon.

Useful env vars while you're getting set up (full reference in
[Backends](./backends.md#configuration)):

| Env var | Effect |
|---|---|
| `RIGHTSIZE_BACKEND` | Force `microsandbox` or `docker`, skipping auto-detection. |
| `MSB_PATH` | Point at an `msb` binary you already have; skips the download entirely. |
| `RIGHTSIZE_CACHE_DIR` | Relocate the provisioner's cache root. |
| `RIGHTSIZE_MSB_SKIP_DOWNLOAD` | `true` turns a cache miss into a hard error instead of a network fetch — for air-gapped CI that pre-seeds the cache. |

## Backend wiring — automatic with modules, explicit without

Consumers of `rightsize-modules` write no backend wiring at all: the
feature-enabled backends (`backend-msb`, `backend-docker` — both default
features) register themselves the first time any module starts.

Backend resolution happens at the process's **first** `Container::start()` and
is cached for the life of the process. Two cases still call for one explicit
line:

- **Your first container is a plain `Container`, not a module.** Call the same
  entry point every module start uses, before that first start:

  ```rust,ignore
  rightsize_modules::register_default_backends();
  ```

- **You depend on `rightsize` and the backend crates directly**, without
  `rightsize-modules`. Rust has no `ServiceLoader`-style automatic plugin
  discovery, so register once per process before the first start:

  ```rust,ignore
  rightsize::backends::register_provider(Box::new(rightsize_msb::MsbBackendProvider));
  rightsize::backends::register_provider(Box::new(rightsize_docker::DockerBackendProvider));
  ```

  Registration is idempotent by provider name, so a shared test-fixture module
  calling this from several places is fine. `rightsize::backends::resolve` picks
  among whatever's registered, honoring `RIGHTSIZE_BACKEND` when it's set.

## The shared-container recipe

There's no JUnit-style `@Container` static-scope extension in rightsize-rust — the RAII
guard *is* the API (see [Binding decisions](./how-it-works.md#binding-decisions)). For
a container shared across many tests in one binary, boot it once behind a
`tokio::sync::OnceCell` and never call `stop()` on it; process exit (or the `Drop`
fallback) reclaims it:

```rust,ignore
use std::sync::Arc;
use tokio::sync::OnceCell;
use rightsize_modules::{RedisContainer, RedisGuard};

static REDIS: OnceCell<Arc<RedisGuard>> = OnceCell::const_new();

async fn shared_redis() -> Arc<RedisGuard> {
    REDIS
        .get_or_init(|| async { Arc::new(RedisContainer::new().start().await.unwrap()) })
        .await
        .clone()
}

#[tokio::test]
async fn first_test_using_redis() {
    let redis = shared_redis().await;
    // ... use redis.uri() ...
}

#[tokio::test]
async fn second_test_using_redis() {
    let redis = shared_redis().await;   // same instance, booted once
    // ...
}
```

More on why this shape, and the full RAII lifecycle it relies on, in
[Containers & Guards](./core-concepts/containers-and-guards.md).
