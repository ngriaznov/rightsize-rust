# Contributing to rightsize-rust

Thanks for considering a contribution. rightsize-rust is a Tokio-async-native
RAII API that runs integration-test containers as microsandbox microVMs, with
a hand-rolled Docker fallback. Most contributions touch either `rightsize` (backend-agnostic
core), one of the two backend crates, or `rightsize-modules` (preconfigured
containers). Read on for how to build, test, and submit changes.

## Prerequisites

- The pinned Rust toolchain (`rust-toolchain.toml`; currently 1.85, stable,
  2024 edition). `rustup` provisions it automatically if you don't have it.
- Git.
- To run integration tests against a real backend, at least one of:
  - **microsandbox**: macOS on Apple Silicon, or Linux (x86_64/arm64) with a
    readable `/dev/kvm`. rightsize-rust self-provisions the `msb` binary on
    first use — no manual install required.
  - **Docker**: any Docker-compatible daemon reachable at the default socket,
    or via `DOCKER_HOST`.

## Building

```bash
cargo build --workspace
cargo test --workspace
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
```

This compiles every crate and runs the unit test suite only — integration
tests need a real backend and are gated behind a Cargo feature (see below), so
plain `cargo test --workspace` works offline, on any machine, with no runtime
installed. This must stay green on every commit.

`cargo doc --workspace --no-deps` must build with zero warnings — every public
item is documented (`#![warn(missing_docs)]` on every crate root), and a
broken intra-doc link is a real defect, not a lint to silence.

## Running integration tests

Tests that need a real backend live behind the `sandbox-it` Cargo feature —
Cargo has no test-tag mechanism, so a feature flag gates them rather than
`#[ignore]` + `--ignored`. Run them explicitly, forcing a backend with
`RIGHTSIZE_BACKEND`:

```bash
# microsandbox backend (needs Apple Silicon or Linux + /dev/kvm)
RIGHTSIZE_BACKEND=microsandbox cargo test --workspace --features sandbox-it

# Docker backend (needs a reachable Docker daemon)
RIGHTSIZE_BACKEND=docker cargo test --workspace --features sandbox-it
```

Both backends satisfy the same `SandboxBackend` contract (see the shared
contract suite in `crates/rightsize-modules/tests/contract.rs`), so **a change
that affects observable behavior should be exercised on both before you open a
PR** — CI runs the full matrix (`unit`, `msb-linux`, `msb-macos`,
`docker-fallback`; see `.github/workflows/ci.yml`), but a local run catches
problems faster and doesn't wait on a runner queue.

Before running the msb-backed suite for the first time, redpanda's image needs
seeding into the msb cache once (`docker.redpanda.com` rate-limits anonymous
pulls):

```bash
docker save redpanda/redpanda:<tag> | msb load -t redpanda/redpanda:<tag>
```

Env vars useful while developing (full reference in the
[README](README.md#configuration)):

| Variable | Purpose |
| --- | --- |
| `RIGHTSIZE_BACKEND` | Force `microsandbox` or `docker`. Required to pick a lane for `--features sandbox-it` runs. |
| `MSB_PATH` | Point at a pre-installed `msb` binary; skips the download/verify step entirely. |
| `RIGHTSIZE_CACHE_DIR` | Relocate the provisioner's cache root away from `~/.cache/rightsize`. |
| `RIGHTSIZE_MSB_SKIP_DOWNLOAD` | `true` turns a cache miss into a hard, actionable error instead of a network fetch — useful for air-gapped CI that pre-seeds the cache. |

## Coverage

The core crate (`rightsize`) carries an 80%-line coverage floor, checked with
`cargo-llvm-cov`:

```bash
cargo llvm-cov --package rightsize --lib --fail-under-lines 80
```

## Code style

- `cargo fmt` and `cargo clippy --all-targets -- -D warnings` must be clean.
- `#![forbid(unsafe_code)]` is set on every crate; a contribution that needs
  `unsafe` needs a different approach or an explicit discussion first.
- Every public item needs a doc comment. House voice: precise, wry, fact-first
  — a fact, then (if something is missing) an em-dash, then the remedy. Explain
  the *why* and the one non-obvious constraint, not a narration of the code
  underneath it.
- No AI/Claude attribution anywhere — commits, code comments, or docs. This is
  a hard rule, not a style preference.

## Pull requests

- Keep commits scoped and use conventional-commit-style messages
  (`feat(core): ...`, `fix(modules): ...`, `test: ...`, `docs: ...`).
- If your change touches a `SandboxBackend` implementation or anything the
  shared contract suite exercises, say in the PR description which backends
  you ran the `sandbox-it` suite against.
- New modules (a new preconfigured container in `rightsize-modules`) should
  follow the existing shape: a thin newtype wrapping `Container`, a `*Guard`
  newtype wrapping `ContainerGuard` with connection helpers, and a
  `with_image`/`new` pair matching the existing modules' builder style.
