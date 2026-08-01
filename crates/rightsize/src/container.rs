//! The `Container` builder and the `ContainerGuard` RAII handle it returns.
//!
//! This module owns the hardest moment in the core crate: the two-tier cleanup story.
//! **Happy path:** `ContainerGuard::stop(self)` is an explicit `async fn` that awaits an
//! ordered teardown (backend `stop` then `remove`, then port release). **Fallback:**
//! `Drop` cannot be `async` and must not assume a Tokio runtime is anywhere in the
//! process, so it does the least synchronous work that's still correct — release ports
//! synchronously (`FreePorts` is plain `std::sync::Mutex`-guarded, no runtime needed)
//! and hand the container off to the dedicated cleanup thread in `crate::cleanup`, which
//! tears it down with blocking std I/O only. See `crate::cleanup`'s module docs for why
//! that thread exists and what it promises.
//!
//! `Container::start()`'s ordering (allocate → create → start, with a port-bind-conflict
//! retry loop; then network-link-install → register → wait, as one guarded unit whose
//! failure triggers a full awaited teardown before the error reaches the caller) is
//! deliberate: any partial failure after resources are allocated must still reach a
//! fully-torn-down state before the error propagates to the caller.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::backend::{SandboxBackend, SandboxHandle};
use crate::backends;
use crate::checkpoint::{self, Checkpoint};
use crate::cleanup::{self, CleanupJob};
use crate::error::{Result, RightsizeError};
use crate::free_ports::FreePorts;
use crate::model::{ContainerSpec, ExecResult, FileMount};
use crate::mountable_file::MountableFile;
use crate::network::{Network, NetworkMember};
use crate::run_id::RunId;
use crate::wait::{Wait, WaitStrategy, WaitTarget};

/// How many times `Container::start()` retries the create+start step with fresh host
/// ports before giving up, when every failure is a port-bind conflict.
const PORT_BIND_ATTEMPTS: usize = 5;

/// A process-wide counter for container name suffixes. Retries advance it too (a
/// discarded attempt's name is never reused), so names aren't dense — only unique per
/// process, which is all a name needs to be.
static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A per-process free-port allocator, shared by every `Container` in this process.
static FREE_PORTS: std::sync::OnceLock<FreePorts> = std::sync::OnceLock::new();

fn free_ports() -> &'static FreePorts {
    FREE_PORTS.get_or_init(FreePorts::new)
}

/// A closure that rewrites a `ContainerSpec` with knowledge of this container's mapped
/// host ports, before `create()` (e.g. Redpanda/Kafka's advertised-listener rewrite).
type SpecCustomizer = dyn Fn(ContainerSpec, &dyn Fn(u16) -> u16) -> ContainerSpec + Send + Sync;

/// A closure run once a guard exists and the wait strategy is satisfied (e.g.
/// Mongo's replica-set init).
type PostStartHook = dyn Fn(&ContainerGuard) -> crate::BoxFuture<'_, Result<()>> + Send + Sync;

/// A single sandboxed container, built with a fluent API and run by whichever
/// [`crate::backend::SandboxBackend`] is active. Configure it with the `with_*`
/// builders, call [`Container::start`] to boot it and get back a [`ContainerGuard`].
pub struct Container {
    image: String,
    env: Vec<(String, String)>,
    exposed_ports: Vec<u16>,
    command: Option<Vec<String>>,
    network: Option<Arc<Network>>,
    aliases: Vec<String>,
    mounts: Vec<FileMount>,
    wait_strategy: Arc<dyn WaitStrategy>,
    memory_limit_mb: Option<u64>,
    disk_limit_mb: Option<u64>,
    tmpfs_root_mb: Option<u64>,
    network_disabled: bool,
    backend_override: Option<Arc<dyn SandboxBackend>>,
    spec_customizer: Option<Arc<SpecCustomizer>>,
    post_start: Option<Arc<PostStartHook>>,
    reuse: bool,
    cache_dir_override: Option<std::path::PathBuf>,
    reuse_env_override: Option<bool>,
    require_isolation: bool,
    reaper_cache_dir_override: Option<std::path::PathBuf>,
    /// Set by [`Container::from_checkpoint`] to the source checkpoint's `ref` —
    /// threaded into the started spec's `ContainerSpec::checkpoint_ref`. `None` for
    /// every ordinarily-built `Container`.
    checkpoint_ref: Option<String>,
    /// Set by [`Container::from_checkpoint`] to the source checkpoint's `backend` —
    /// `start()` refuses before any backend work if this doesn't match the active
    /// backend's name (see `RightsizeError::CheckpointBackendMismatch`).
    checkpoint_backend: Option<String>,
    /// Test/module seam: overrides the named-checkpoint registry's cache dir —
    /// see [`Self::with_checkpoint_cache_dir_override`].
    checkpoint_cache_dir_override: Option<std::path::PathBuf>,
}

impl Container {
    /// Starts building a container from `image`, with no exposed ports, no env, the
    /// default [`Wait::for_listening_port`] readiness check, and every other option at
    /// its default.
    pub fn new(image: &str) -> Container {
        Container {
            image: image.to_string(),
            env: Vec::new(),
            exposed_ports: Vec::new(),
            command: None,
            network: None,
            aliases: Vec::new(),
            mounts: Vec::new(),
            wait_strategy: Arc::from(Wait::for_listening_port()),
            memory_limit_mb: None,
            disk_limit_mb: None,
            tmpfs_root_mb: None,
            network_disabled: false,
            backend_override: None,
            spec_customizer: None,
            post_start: None,
            reuse: false,
            cache_dir_override: None,
            reuse_env_override: None,
            require_isolation: false,
            reaper_cache_dir_override: None,
            checkpoint_ref: None,
            checkpoint_backend: None,
            checkpoint_cache_dir_override: None,
        }
    }

    /// Builds a normal `Container` from a [`Checkpoint`]'s image and the source
    /// container's env, command, exposed ports (guest side only — a restored
    /// container gets fresh host ports, chosen by the core allocator exactly like
    /// any other `start()`), and memory limit. Every other field starts at its
    /// ordinary default and can still be overridden with the usual builders before
    /// `start()` (e.g. a different `.waiting_for(...)` wait strategy) — the value
    /// returned here is a plain `Container`, indistinguishable from one built by
    /// [`Container::new`]. A container started from it is ordinary in every other
    /// respect too: a fresh name, fresh host ports, normal reaping, normal `stop()`.
    ///
    /// Deliberately does NOT carry over the checkpoint's `mounts`, `network_id`, or
    /// `aliases` — the checkpoint already has whatever those mounts wrote, baked
    /// directly into its filesystem (see [`Checkpoint`]'s own doc for the
    /// "filesystem capture, not memory" semantics), and network topology has no
    /// well-defined meaning to carry across a restore.
    ///
    /// Sets both `image` and [`crate::model::ContainerSpec::checkpoint_ref`] to
    /// `cp.ref` — docker ignores the latter (the ref already is a normal image tag,
    /// so the ordinary create path just works); microsandbox, when it's set, boots
    /// via `msb run --from-snapshot <ref>` instead of its normal image boot. `start()`
    /// refuses before any backend work — [`RightsizeError::CheckpointBackendMismatch`]
    /// — if the active backend's name doesn't match `cp.backend`, and
    /// [`RightsizeError::ReuseCheckpointConflict`] if `.reuse(true)` is also active,
    /// since reuse identity has no concept of a checkpoint reference.
    pub fn from_checkpoint(cp: &Checkpoint) -> Container {
        let mut c = Container::new(&cp.checkpoint_ref);
        c.env = cp.spec.env.clone();
        c.command = cp.spec.command.clone();
        c.exposed_ports = cp.spec.ports.iter().map(|p| p.guest_port).collect();
        c.memory_limit_mb = cp.spec.memory_limit_mb;
        c.checkpoint_ref = Some(cp.checkpoint_ref.clone());
        c.checkpoint_backend = Some(cp.backend.clone());
        c
    }

    /// Sets a single environment variable for the container process. Call again with
    /// the same key to overwrite it — the *value* from the last call wins, but the
    /// key keeps the position of its *first* `with_env` call in the final spec's
    /// iteration order (see `dedup_env_last_wins`, applied once at `start()` time,
    /// right before the spec reaches a backend — this builder itself still just
    /// appends, so a spec-customizer pushing more env entries is resolved the same way).
    pub fn with_env(mut self, k: &str, v: &str) -> Self {
        self.env.push((k.to_string(), v.to_string()));
        self
    }

    /// Removes every entry for `key` set so far via [`Self::with_env`]. The core-level
    /// primitive a module needs when a later env var must ALSO retract an earlier
    /// one's effect on the running process, not merely be shadowed by last-wins
    /// value resolution (e.g.
    /// `ArangoContainer::with_root_password` clearing `ARANGO_NO_AUTH`, whose
    /// entrypoint checks presence/absence of the variable itself, not just its
    /// value — last-wins on `ARANGO_NO_AUTH`'s own value can't turn it "off").
    /// No-op if `key` was never set.
    pub fn remove_env(mut self, key: &str) -> Self {
        self.env.retain(|(k, _)| k != key);
        self
    }

    /// The env entries set so far, in call order — a plain read-only introspection
    /// point (e.g. for a module's own unit tests asserting what its builder produced,
    /// such as `ArangoContainer::with_root_password`'s NO_AUTH-removed/password-added
    /// contract) rather than a way to mutate the builder. Reflects [`Self::with_env`]
    /// calls made so far exactly, duplicates and all — the last-wins dedup
    /// (`dedup_env_last_wins`) only runs once, at `start()` time.
    pub fn env(&self) -> &[(String, String)] {
        &self.env
    }

    /// Declares guest ports to publish; each gets a host port assigned before boot.
    pub fn with_exposed_ports(mut self, ports: &[u16]) -> Self {
        self.exposed_ports.extend_from_slice(ports);
        self
    }

    /// Overrides the image's default entrypoint/command.
    pub fn with_command(mut self, cmd: &[&str]) -> Self {
        self.command = Some(cmd.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Joins `net`, making this container's exposed ports reachable at its
    /// [`Container::with_network_aliases`].
    pub fn with_network(mut self, net: &Arc<Network>) -> Self {
        self.network = Some(net.clone());
        self
    }

    /// Names this container is reachable as on its network (see [`Network::resolve`]).
    pub fn with_network_aliases(mut self, names: &[&str]) -> Self {
        self.aliases.extend(names.iter().map(|s| s.to_string()));
        self
    }

    /// Mounts `file` read-write into the guest at `guest_path` ([`FileMount::new`]'s
    /// default); takes effect at the next `start()`. The mount is a view of the host
    /// file, not a copy — docker binds the host path directly, msb hard-links it into
    /// its staging directory — so a guest write reaches the host file itself, on both
    /// backends. A mount built with [`FileMount::read_only`] blocks guest writes
    /// (`Read-only file system`), enforced by both backends.
    pub fn with_copy_file_to_container(mut self, file: MountableFile, guest_path: &str) -> Self {
        self.mounts.push(FileMount::new(file.path(), guest_path));
        self
    }

    /// Overrides the readiness check run after boot; defaults to
    /// [`Wait::for_listening_port`]. Takes the strategy by value — a bare
    /// `Wait::for_http("/health").for_port(80)` or a caller's own `impl WaitStrategy`
    /// works directly, no `Box::new(..)` wart at the call site; this boxes internally
    /// (a `Box<dyn WaitStrategy>` from a built-in factory that already returns one
    /// satisfies the bound too, via the blanket impl in `crate::wait`, so it is never
    /// double-boxed).
    pub fn waiting_for(mut self, strategy: impl WaitStrategy + 'static) -> Self {
        self.wait_strategy = Arc::new(strategy);
        self
    }

    /// Caps the container's guest memory at `megabytes`. Leaving this unset lets each
    /// backend apply its own default.
    pub fn with_memory_limit(mut self, megabytes: u64) -> Self {
        self.memory_limit_mb = Some(megabytes);
        self
    }

    /// Caps the writable root disk at `megabytes` — microsandbox-only
    /// (`--root-disk <mb>M`); docker runs its normal disk-backed rootfs with no
    /// ceiling and ignores this. The ceiling grows only on an msb reboot, never
    /// shrinks back down. Mutually exclusive with [`Self::with_tmpfs_root`] —
    /// `start()` returns [`RightsizeError::RootDiskConflict`] if both are set.
    /// msb rejects a root-disk setting on a [`Container::from_checkpoint`]
    /// restore before boot — the snapshot pins the root disk.
    pub fn with_disk_limit(mut self, megabytes: u64) -> Self {
        self.disk_limit_mb = Some(megabytes);
        self
    }

    /// Runs the writable root disk from guest RAM instead of storage, sized at
    /// `megabytes` — microsandbox-only (`--root-disk tmpfs:<mb>M`); docker runs its
    /// normal disk-backed rootfs and ignores this. Must not exceed
    /// [`Self::with_memory_limit`] — `start()` returns
    /// [`RightsizeError::TmpfsRootExceedsMemory`] when it does (msb's own default
    /// guest memory is 512M when no memory limit is set, so nothing is validated
    /// in that case — msb's own error at boot is already precise there). Mutually
    /// exclusive with [`Self::with_disk_limit`] —
    /// [`RightsizeError::RootDiskConflict`] if both are set. A tmpfs root is
    /// ephemeral and cannot be checkpointed, and msb rejects a root-disk
    /// setting on a [`Container::from_checkpoint`] restore before boot.
    pub fn with_tmpfs_root(mut self, megabytes: u64) -> Self {
        self.tmpfs_root_mb = Some(megabytes);
        self
    }

    /// Blocks public-internet access on microsandbox (`--net private` — published
    /// ports and private-range links keep working); docker ignores this and runs
    /// with normal networking. Mutually exclusive with [`Self::with_network`] —
    /// `start()` returns [`RightsizeError::NetworkDisabledConflict`] if both are
    /// set.
    pub fn with_network_disabled(mut self) -> Self {
        self.network_disabled = true;
        self
    }

    /// Marks this container for reuse: a container built from an identical
    /// image/env/command/exposed-ports/memory-limit/mounted-files spec survives
    /// process exit and is ADOPTED — not re-created — by a later `start()`, in this
    /// process or a later one; `stop()` then leaves the sandbox running instead of
    /// tearing it down. Requires a double opt-in: `RIGHTSIZE_REUSE` must ALSO be
    /// set to `"true"` or `"1"` in the real process environment, or `start()`
    /// behaves exactly as an ordinary ephemeral container (with a single stderr
    /// note that reuse was requested but not enabled). Reuse cannot be combined
    /// with [`Self::with_network`] — `start()` returns
    /// [`RightsizeError::ReuseNetworkConflict`] up front — nor with
    /// [`Self::from_checkpoint`] — [`RightsizeError::ReuseCheckpointConflict`],
    /// since reuse identity has no concept of a checkpoint ref. Defaults to
    /// `false`.
    pub fn reuse(mut self, enabled: bool) -> Self {
        self.reuse = enabled;
        self
    }

    /// Requires the active backend to provide hardware isolation — its own kernel per
    /// sandbox, see [`crate::backend::Capabilities::hardware_isolated`] — before this
    /// container is allowed to start. Checked in [`Self::start`], before any
    /// create/network work: if the active backend does not provide it (the docker
    /// backend, which shares the host kernel), `start()` returns
    /// [`RightsizeError::IsolationRequired`] and no sandbox is created. Use this for
    /// workloads that genuinely need microVM-strength isolation (untrusted code),
    /// rather than trusting every backend a caller might resolve to. Defaults to
    /// `false`.
    pub fn require_isolation(mut self, enabled: bool) -> Self {
        self.require_isolation = enabled;
        self
    }

    /// Test/module seam: overrides the backend this container starts on, instead of the
    /// process-wide active backend. Every current caller is a unit test in this crate —
    /// `#[cfg_attr(not(test), allow(dead_code))]` reflects that precisely instead of
    /// blanket-silencing dead-code analysis on this method.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn with_backend(mut self, b: Arc<dyn SandboxBackend>) -> Self {
        self.backend_override = Some(b);
        self
    }

    /// Test/module seam: overrides the reuse registry's cache dir instead of the
    /// real `crate::cache_dir::dir()` (which reads `RIGHTSIZE_CACHE_DIR` from the
    /// real process environment) — every current caller is a unit test exercising
    /// the reuse flow's registry file, which must never touch a real machine's
    /// `~/.cache/rightsize/reuse/` directory.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn with_cache_dir_override(mut self, dir: std::path::PathBuf) -> Self {
        self.cache_dir_override = Some(dir);
        self
    }

    /// Test/module seam: overrides the `RIGHTSIZE_REUSE` double-opt-in check instead
    /// of reading the real process environment (`crate::reuse::env_enabled`) — lets
    /// unit tests exercise both gating outcomes deterministically, without mutating
    /// real process env (racy across the parallel test threads `cargo test` uses
    /// within one binary) or needing `unsafe` (`std::env::set_var` requires it as of
    /// the 2024 edition, and this crate forbids unsafe code entirely).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn with_reuse_env_override(mut self, enabled: bool) -> Self {
        self.reuse_env_override = Some(enabled);
        self
    }

    /// Test/module seam: overrides the REAPING ledger's cache dir instead of the
    /// real `crate::cache_dir::dir()` — distinct from [`Self::with_cache_dir_override`],
    /// which only redirects the REUSE registry. Every current caller is a unit
    /// test in this module that asserts against `.sandboxes`/`.networks`
    /// directly (`crate::reaper::Ledger`); without this, those tests read/write
    /// the real, process-wide ledger under this developer machine's actual
    /// `~/.cache/rightsize/runs/` and are inherently coupled to every other
    /// concurrently-running test in the same binary sharing that same file (see
    /// `crate::reaper::ledger`'s module doc — one `WRITE_LOCK`-guarded ledger per
    /// process, keyed only by run id, not by test). Threaded down to every
    /// `crate::reaper::before_create`/`after_stop`/`before_ensure_network`/
    /// `after_remove_network` call this container's own lifecycle makes (see
    /// [`crate::reaper::ledger_for`]'s doc for exactly what does and doesn't
    /// change under the override) — but NOT to the watchdog-spawn-once gate or
    /// this process's `RIGHTSIZE_REAPER` mode, which stay tied to the real,
    /// process-wide reaper state regardless, matching every other test in this
    /// binary. Production never sets this field, so `None` (the real cache dir)
    /// is always what a real caller gets.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn with_reaper_cache_dir_override(mut self, dir: std::path::PathBuf) -> Self {
        self.reaper_cache_dir_override = Some(dir);
        self
    }

    /// Test/module seam: overrides the named-checkpoint registry's cache dir
    /// instead of the real `crate::cache_dir::dir()` — distinct from
    /// [`Self::with_cache_dir_override`] (reuse) and
    /// [`Self::with_reaper_cache_dir_override`] (the reaping ledger). Every
    /// current caller is a unit test exercising `ContainerGuard::checkpoint_named`,
    /// which must never touch a real machine's `~/.cache/rightsize/checkpoints/`
    /// directory. Threaded onto the returned [`ContainerGuard`] at `start()` time.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn with_checkpoint_cache_dir_override(mut self, dir: std::path::PathBuf) -> Self {
        self.checkpoint_cache_dir_override = Some(dir);
        self
    }

    /// Module hook: rewrites the `ContainerSpec` with knowledge of this container's own
    /// mapped host ports, right before `create()` — e.g. Redpanda/Kafka's
    /// advertised-listener rewrite, which needs to know its own mapped port to advertise
    /// it.
    pub fn with_spec_customizer(
        mut self,
        f: impl Fn(ContainerSpec, &dyn Fn(u16) -> u16) -> ContainerSpec + Send + Sync + 'static,
    ) -> Self {
        self.spec_customizer = Some(Arc::new(f));
        self
    }

    /// Module hook: runs once the guard exists and the wait strategy is satisfied — e.g.
    /// Mongo's replica-set initialization.
    pub fn with_post_start(
        mut self,
        f: impl for<'a> Fn(&'a ContainerGuard) -> crate::BoxFuture<'a, Result<()>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.post_start = Some(Arc::new(f));
        self
    }

    fn active_backend(&self) -> Arc<dyn SandboxBackend> {
        match &self.backend_override {
            Some(b) => b.clone(),
            None => backends::active(),
        }
    }

    fn describe(id: &str, image: &str) -> String {
        format!("container(image={image}, id={id})")
    }

    /// Boots the container and returns an RAII guard. On ANY failure partway (create,
    /// start, install_network_links, register, OR wait), teardown runs and nothing
    /// leaks — `start()` does not return its error to the caller until that teardown
    /// has finished.
    pub async fn start(self) -> Result<ContainerGuard> {
        let backend = self.active_backend();

        if let Some(creator) = &self.checkpoint_backend {
            // Checked before ANY backend work, including require_isolation below —
            // a checkpoint ref from one backend's mechanism has no meaning to the
            // other's, so there is nothing useful to attempt on a mismatch.
            if creator != backend.name() {
                return Err(RightsizeError::CheckpointBackendMismatch {
                    active_backend: backend.name().to_string(),
                    checkpoint_backend: creator.clone(),
                });
            }
        }

        if self.require_isolation && !backend.capabilities().hardware_isolated {
            // Checked before any create/network work — reuse's own registry lookup,
            // ensure_network, and create_started_container all come after this, so a
            // non-isolated backend never gets far enough to create anything.
            return Err(RightsizeError::IsolationRequired {
                backend: backend.name().to_string(),
            });
        }

        // Checked before any backend work — see `validate_spec_conflicts`'s own
        // doc for exactly what this refuses. The identical check runs again,
        // against the FINISHED spec, after a spec-customizer (if any) has run
        // (`create_started_container`/`create_and_start_reuse_sandbox`) — a
        // customizer can set any of these fields directly on the `ContainerSpec`
        // it returns, and this pre-flight pass alone cannot see that.
        validate_spec_conflicts(
            self.disk_limit_mb,
            self.tmpfs_root_mb,
            self.memory_limit_mb,
            self.network_disabled,
            self.network.as_ref().map(|n| n.id()),
        )?;

        if self.reuse {
            let reuse_env_enabled = self
                .reuse_env_override
                .unwrap_or_else(crate::reuse::env_enabled);
            if reuse_env_enabled {
                if self.checkpoint_ref.is_some() {
                    // Reuse identity has no concept of a checkpoint ref (it
                    // deliberately never enters the identity hash) — this
                    // combination has no well-defined adopt/create behavior.
                    return Err(RightsizeError::ReuseCheckpointConflict);
                }
                if let Some(net) = &self.network {
                    return Err(RightsizeError::ReuseNetworkConflict {
                        network_id: net.id().to_string(),
                    });
                }
                return start_reuse(self, backend).await;
            }
            // API-marked but env-disabled: the double opt-in requires both, so this
            // container runs as an ordinary ephemeral one — Testcontainers
            // semantics — falling straight through to the unchanged path below,
            // with a single note so a caller who forgot to set RIGHTSIZE_REUSE
            // notices why nothing was adopted.
            eprintln!(
                "rightsize: .reuse(true) was requested but RIGHTSIZE_REUSE is not enabled (set \
                 it to \"true\" or \"1\") — starting an ordinary, non-reused container instead."
            );
        }

        if let Some(net) = &self.network {
            // Append-before-create, same discipline as a sandbox name (see
            // `crate::reaper`'s module doc) — dedupes across repeat joiners of the
            // same network, since `Ledger::append_network` is itself idempotent.
            crate::reaper::before_ensure_network(
                net.id(),
                self.reaper_cache_dir_override.as_deref(),
            );
            if let Err(e) = backend.ensure_network(net.id()).await {
                // This attempt never produced a usable network — undo the ledger
                // append above so a discarded id doesn't sit in `.networks` forever
                // and block the clean-shutdown deletion trigger for the rest of this
                // process. Mirrors `create_started_container`'s `after_stop` cleanup
                // on its own `create`/`start` failure branches.
                crate::reaper::after_remove_network(
                    net.id(),
                    self.reaper_cache_dir_override.as_deref(),
                );
                return Err(e);
            }
        }

        let (handle, mapped_ports) = create_started_container(
            &backend,
            &self.image,
            &self.env,
            &self.command,
            &self.exposed_ports,
            &self.mounts,
            self.network.as_deref(),
            &self.aliases,
            self.memory_limit_mb,
            self.disk_limit_mb,
            self.tmpfs_root_mb,
            self.network_disabled,
            self.spec_customizer.as_deref(),
            self.reaper_cache_dir_override.as_deref(),
            self.checkpoint_ref.as_deref(),
        )
        .await?;

        let name = handle.id().to_string();
        // The reaping ledger tracks `ContainerSpec::name` (`rz-<run_id>-<seq>`), not
        // `SandboxHandle::id()` — the two coincide for msb but NOT for docker, whose
        // `id()` is the daemon-assigned container id. Captured here, before `handle`
        // moves into the guard, for `stop_inner`/`Drop` to hand to
        // `crate::reaper::after_stop`.
        let ledger_name = handle.spec().name.clone();
        let keep_alive = handle.spec().keep_alive;
        // Captured before `handle` moves into the guard below — the diagnostics
        // registry owns its own copy of the handle id/spec, independent of the
        // guard's lifetime (see `crate::diagnostics`'s module doc).
        let diagnostics_handle_id = handle.id().to_string();
        let diagnostics_spec = handle.spec().clone();
        let diagnostics_ports = mapped_ports.clone();
        let mut guard = ContainerGuard {
            handle: Some(handle),
            backend: backend.clone(),
            mapped_ports: Mutex::new(mapped_ports),
            network: self.network.clone(),
            image: self.image.clone(),
            exposed_ports: self.exposed_ports.clone(),
            name,
            ledger_name,
            keep_alive,
            reaper_cache_dir_override: self.reaper_cache_dir_override.clone(),
            checkpoint_cache_dir_override: self.checkpoint_cache_dir_override.clone(),
            wait_strategy: self.wait_strategy.clone(),
            network_links: Vec::new(),
        };

        // Guarded block: install_network_links -> register -> wait. On ANY error here,
        // await the guard's own stop to completion, then propagate the original error —
        // a half-started container never leaks regardless of where in this block it
        // failed, and start() does not return until teardown has finished.
        match link_register_and_wait(
            &guard,
            &backend,
            self.network.as_deref(),
            &self.aliases,
            guard.wait_strategy.as_ref(),
        )
        .await
        {
            Ok(links) => guard.network_links = links,
            Err(e) => {
                let _ = guard.stop().await;
                return Err(e);
            }
        }

        // Registered only once the readiness wait has fully succeeded — mirrors the
        // Kotlin port's resolution of this same finding (adopt path registers after
        // its own wait re-run). A container that boots but never becomes ready must
        // never appear in the report, so a mid-wait `diagnostics()` call cannot list
        // it, and the wait-failure branch above has nothing to deregister.
        crate::diagnostics::register(
            &guard.ledger_name,
            &guard.image,
            guard.host(),
            diagnostics_ports,
            backend.clone(),
            &diagnostics_handle_id,
            diagnostics_spec,
        );

        if let Some(post_start) = &self.post_start {
            if let Err(e) = post_start(&guard).await {
                let _ = guard.stop().await;
                return Err(e);
            }
        }

        Ok(guard)
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_started_container(
    backend: &Arc<dyn SandboxBackend>,
    image: &str,
    env: &[(String, String)],
    command: &Option<Vec<String>>,
    exposed_ports: &[u16],
    mounts: &[FileMount],
    network: Option<&Network>,
    aliases: &[String],
    memory_limit_mb: Option<u64>,
    disk_limit_mb: Option<u64>,
    tmpfs_root_mb: Option<u64>,
    network_disabled: bool,
    spec_customizer: Option<&SpecCustomizer>,
    reaper_cache_dir_override: Option<&std::path::Path>,
    checkpoint_ref: Option<&str>,
) -> Result<(Box<dyn SandboxHandle>, Vec<(u16, u16)>)> {
    let mut last_conflict: Option<RightsizeError> = None;

    for _ in 0..PORT_BIND_ATTEMPTS {
        let mapped_ports = allocate_ports(exposed_ports)?;
        let seq = NAME_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("rz-{}-{seq}", RunId::value());

        let mut spec = ContainerSpec {
            name: name.clone(),
            image: image.to_string(),
            env: env.to_vec(),
            command: command.clone(),
            ports: mapped_ports
                .iter()
                .map(|&(guest_port, host_port)| crate::model::PortBinding {
                    host_port,
                    guest_port,
                })
                .collect(),
            mounts: mounts.to_vec(),
            network_id: network.map(|n| n.id().to_string()),
            aliases: aliases.to_vec(),
            run_id: RunId::value().to_string(),
            memory_limit_mb,
            keep_alive: false,
            checkpoint_ref: checkpoint_ref.map(ToString::to_string),
            disk_limit_mb,
            tmpfs_root_mb,
            network_disabled,
        };

        if let Some(customizer) = spec_customizer {
            let lookup: std::collections::HashMap<u16, u16> =
                mapped_ports.iter().copied().collect();
            let mapped_fn = move |guest: u16| -> u16 {
                *lookup
                    .get(&guest)
                    .expect("customizer looked up an unexposed port")
            };
            spec = customizer(spec, &mapped_fn);
        }

        // Last-wins, insertion-order-of-first-occurrence dedup — see
        // `dedup_env_last_wins`'s doc for why this runs here (after the
        // spec-customizer, which may itself push more env entries) rather than
        // earlier.
        spec.env = dedup_env_last_wins(spec.env);

        // Re-validated against the FINISHED spec — `Container::start()`'s own
        // pre-flight pass (`validate_spec_conflicts`, above) only ever saw the
        // builder's own fields; a customizer just ran and can have set any of
        // these directly on `spec`, reaching the backend unvalidated otherwise.
        validate_spec_conflicts(
            spec.disk_limit_mb,
            spec.tmpfs_root_mb,
            spec.memory_limit_mb,
            spec.network_disabled,
            spec.network_id.as_deref(),
        )?;

        // The reaping ledger's own append-before-create discipline: the name must be
        // recorded as a superset BEFORE the backend actually creates it, so a crash
        // between this line and `backend.create` still leaves a (harmlessly
        // not-found-on-remove) name in the ledger rather than a live sandbox with no
        // record at all. See `crate::reaper`'s module doc.
        crate::reaper::before_create(
            backend,
            &spec.name,
            spec.keep_alive,
            reaper_cache_dir_override,
        );
        let attempt_name = spec.name.clone();
        let attempt_keep_alive = spec.keep_alive;

        let handle = match backend.create(spec).await {
            Ok(h) => h,
            Err(e) => {
                // This attempt never produced a live sandbox — undo the ledger append
                // above so a discarded name doesn't sit in `.sandboxes` forever.
                crate::reaper::after_stop(
                    &attempt_name,
                    attempt_keep_alive,
                    reaper_cache_dir_override,
                );
                return Err(e);
            }
        };
        match backend.start(handle.as_ref()).await {
            Ok(()) => return Ok((handle, mapped_ports)),
            Err(e) => {
                let _ = backend.stop(handle.as_ref()).await;
                let _ = backend.remove(handle.as_ref()).await;
                release_ports(&mapped_ports);
                // Same rationale as the `create` failure branch above: this attempt's
                // container was just torn down, so its ledger line must go too.
                crate::reaper::after_stop(
                    &attempt_name,
                    attempt_keep_alive,
                    reaper_cache_dir_override,
                );
                if is_port_bind_conflict(&e) {
                    last_conflict = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(RightsizeError::Backend(format!(
        "Could not bind free host ports for {} after {PORT_BIND_ATTEMPTS} attempts — another \
         process kept grabbing the allocated ports first; if this persists, check for a port \
         scanner/leaked process racing the allocator on this host{}",
        Container::describe("<unstarted>", image),
        last_conflict
            .map(|c| format!(" (last conflict: {c})"))
            .unwrap_or_default(),
    )))
}

/// Collapses `env` to last-wins-per-key, keeping each key at the position of its
/// **first** occurrence — an overwrite updates the value in place without moving
/// the key to the end of iteration order, the way an insertion-ordered map's `put`
/// behaves.
///
/// `Container::with_env` (and a spec-customizer pushing straight onto
/// `ContainerSpec::env`) is append-only — a `Vec<(String, String)>`, not a map — so
/// calling it twice with the same key previously left *both* entries in the spec,
/// and which one "won" was whatever the backend's own env-merge order happened to
/// do with a duplicate key (Docker and msb are not guaranteed to agree — see the
/// fix commit this function landed in). This function is the single seam that
/// restores map-like last-wins semantics without changing `with_env`'s append-only
/// builder shape or `ContainerSpec::env`'s `Vec` type (both stay exactly as
/// documented elsewhere — order and duplicate-key handling stay under the caller's
/// control up to this point; this is where they get resolved, once, right before
/// the spec reaches a backend).
fn dedup_env_last_wins(env: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut order: Vec<String> = Vec::new();
    let mut values: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for (k, v) in env {
        if !values.contains_key(&k) {
            order.push(k.clone());
        }
        values.insert(k, v);
    }
    order
        .into_iter()
        .map(|k| {
            let v = values.remove(&k).expect("key was just recorded in order");
            (k, v)
        })
        .collect()
}

/// The conflicting-field checks shared between [`Container::start`]'s pre-flight
/// gate (before any backend work) and the post-spec-customizer re-validation in
/// [`create_started_container`]/[`create_and_start_reuse_sandbox`]: a
/// `.with_spec_customizer(...)` hook returns a brand-new `ContainerSpec` and can
/// set any of these fields directly on it, bypassing the pre-flight checks
/// entirely — see [`RightsizeError::RootDiskConflict`],
/// [`RightsizeError::TmpfsRootExceedsMemory`], and
/// [`RightsizeError::NetworkDisabledConflict`]'s own docs for what each one
/// means; this is the single place all three are checked, so the pre-flight and
/// post-customizer callers can never drift apart on what counts as a conflict.
fn validate_spec_conflicts(
    disk_limit_mb: Option<u64>,
    tmpfs_root_mb: Option<u64>,
    memory_limit_mb: Option<u64>,
    network_disabled: bool,
    network_id: Option<&str>,
) -> Result<()> {
    if disk_limit_mb.is_some() && tmpfs_root_mb.is_some() {
        // The root disk cannot be both a fixed-size ceiling and RAM-backed at
        // once, on either backend.
        return Err(RightsizeError::RootDiskConflict);
    }
    if let Some(tmpfs_mb) = tmpfs_root_mb {
        if let Some(memory_mb) = memory_limit_mb {
            // Only validated when a memory limit is actually set — with none,
            // msb's own default guest memory applies and its own error at boot
            // is already precise, so there is nothing useful to check here.
            if tmpfs_mb > memory_mb {
                return Err(RightsizeError::TmpfsRootExceedsMemory {
                    tmpfs_mb,
                    memory_mb,
                });
            }
        }
    }
    if network_disabled && network_id.is_some() {
        return Err(RightsizeError::NetworkDisabledConflict);
    }
    Ok(())
}

fn allocate_ports(exposed_ports: &[u16]) -> Result<Vec<(u16, u16)>> {
    let mut mapped = Vec::with_capacity(exposed_ports.len());
    for &guest_port in exposed_ports {
        match free_ports().allocate() {
            Ok(host_port) => mapped.push((guest_port, host_port)),
            Err(e) => {
                // Roll back this attempt's partial allocation before propagating.
                release_ports(&mapped);
                return Err(e);
            }
        }
    }
    Ok(mapped)
}

fn release_ports(mapped_ports: &[(u16, u16)]) {
    for &(_, host_port) in mapped_ports {
        free_ports().release(host_port);
    }
}

/// True if `e` represents a host-port bind conflict worth retrying with fresh ports.
/// Prefers the typed [`RightsizeError::PortBindConflict`], walking the `source` chain;
/// falls back to matching known message phrasings (case-insensitive) for backends that
/// don't throw the typed variant. Negatives — any other error — must NOT retry.
pub(crate) fn is_port_bind_conflict(e: &RightsizeError) -> bool {
    let mut current: Option<&RightsizeError> = Some(e);
    while let Some(err) = current {
        if matches!(err, RightsizeError::PortBindConflict { .. }) {
            return true;
        }
        let msg = err.to_string().to_lowercase();
        if msg.contains("address already in use") || msg.contains("already allocated") {
            return true;
        }
        current = match err {
            RightsizeError::PortBindConflict {
                source: Some(s), ..
            } => Some(s.as_ref()),
            _ => None,
        };
    }
    false
}

async fn link_register_and_wait(
    guard: &ContainerGuard,
    backend: &Arc<dyn SandboxBackend>,
    network: Option<&Network>,
    aliases: &[String],
    wait_strategy: &dyn WaitStrategy,
) -> Result<Vec<crate::backend::NetworkLink>> {
    let links = if let Some(net) = network {
        let links = net.links_for_new_member();
        backend
            .install_network_links(guard.handle_ref(), &links)
            .await?;
        links
    } else {
        Vec::new()
    };
    if let Some(net) = network {
        net.register(guard.as_network_member(), aliases.to_vec(), backend.clone());
    }
    let target = GuardWaitTarget { guard };
    wait_strategy.wait_until_ready(&target).await?;
    Ok(links)
}

/// Adapts a [`ContainerGuard`] to the [`WaitTarget`] a [`WaitStrategy`] needs.
struct GuardWaitTarget<'a> {
    guard: &'a ContainerGuard,
}

#[async_trait::async_trait]
impl WaitTarget for GuardWaitTarget<'_> {
    fn host(&self) -> &str {
        self.guard.host()
    }
    fn mapped_port(&self, guest_port: u16) -> u16 {
        self.guard.get_mapped_port(guest_port).unwrap_or(guest_port)
    }
    fn exposed_guest_ports(&self) -> Vec<u16> {
        self.guard.exposed_ports.clone()
    }
    async fn current_logs(&self) -> String {
        self.guard.logs().await.unwrap_or_default()
    }
    fn describe(&self) -> String {
        self.guard.describe()
    }
}

/// The same [`WaitTarget`] adapter [`GuardWaitTarget`] provides, but usable BEFORE a
/// [`ContainerGuard`] exists — the reuse flow's fresh-create and adopt-verify steps
/// both need to run a wait strategy against a raw handle/mapped-ports pair, and a
/// failed wait there must tear the sandbox down for real (not through a keep_alive
/// guard's own `stop()`, which would leave it running by design). See
/// [`create_fresh_reuse`] and [`try_adopt`].
struct RawWaitTarget<'a> {
    host: &'a str,
    mapped_ports: &'a [(u16, u16)],
    exposed_ports: &'a [u16],
    backend: &'a Arc<dyn SandboxBackend>,
    handle: &'a dyn SandboxHandle,
    name: &'a str,
    image: &'a str,
}

#[async_trait::async_trait]
impl WaitTarget for RawWaitTarget<'_> {
    fn host(&self) -> &str {
        self.host
    }
    fn mapped_port(&self, guest_port: u16) -> u16 {
        self.mapped_ports
            .iter()
            .find(|&&(g, _)| g == guest_port)
            .map(|&(_, h)| h)
            .unwrap_or(guest_port)
    }
    fn exposed_guest_ports(&self) -> Vec<u16> {
        self.exposed_ports.to_vec()
    }
    async fn current_logs(&self) -> String {
        self.backend.logs(self.handle).await.unwrap_or_default()
    }
    fn describe(&self) -> String {
        Container::describe(self.name, self.image)
    }
}

/// Orchestrates a reuse-active `start()` once both opt-ins are confirmed and any
/// custom network has already been rejected by the caller (see `Container::start`).
/// Registry lookup miss/corrupt/stale all fall through to [`create_fresh_reuse`];
/// a name-collision on that create (`crate::reuse::is_name_conflict`) gets exactly
/// one retry back into [`try_adopt`], on the theory that the process that won the
/// race is (or is about to be) the one that wrote the registry entry this retry
/// reads.
async fn start_reuse(
    container: Container,
    backend: Arc<dyn SandboxBackend>,
) -> Result<ContainerGuard> {
    let env = dedup_env_last_wins(container.env.clone());
    let identity = crate::reuse::compute_identity(
        &container.image,
        &env,
        &container.command,
        &container.exposed_ports,
        container.memory_limit_mb,
        container.disk_limit_mb,
        container.tmpfs_root_mb,
        container.network_disabled,
        &container.mounts,
    )?;

    let cache_dir = container
        .cache_dir_override
        .clone()
        .unwrap_or_else(crate::cache_dir::dir);
    let registry = crate::reuse::Registry::new(&cache_dir, &identity.hash_hex);

    if registry.exists() {
        match registry.read() {
            Some(entry) => {
                if let Some(guard) = try_adopt(&container, &backend, &identity, &entry).await {
                    return Ok(guard);
                }
                // Not adoptable (not running, wait failed, or a port this call's
                // own exposed_ports needs was missing from the entry): best-effort
                // remove whatever's actually there and the stale registry file,
                // then fall through to a fresh create below.
                backend.remove_by_name(&entry.name);
                registry.delete();
            }
            None => {
                // The file exists but didn't parse — we don't know what name it
                // recorded, but the identity-derived name is deterministic
                // regardless of registry content, so best-effort removal still has
                // a target.
                backend.remove_by_name(&identity.name);
                registry.delete();
            }
        }
    }

    // Crash-mid-boot orphan recovery: by this point the adopt path has concluded
    // there is no usable registry entry at all — missing, corrupt, or stale/failed
    // verification (each branch above already best-effort removed what IT knew
    // about). But a sandbox under this identity's FIXED name can still be RUNNING
    // regardless: a process that crashed (or failed its own wait strategy) after
    // `create_and_start_reuse_sandbox` but before `create_fresh_reuse` ever reached
    // its registry write leaves exactly this state, and `keep_alive` hides it from
    // every reaping/sweep path by design (see `crate::reaper`'s module doc and
    // `docs/reuse.md`'s crash-mid-boot orphan section) — this is the only place left
    // that can ever notice and clean it up. Ask the backend directly rather than
    // trusting the registry's absence, and only remove when it actually reports one
    // running: an unconditional remove_by_name here would be a wasted backend call
    // on the overwhelmingly common genuinely-fresh-identity path, and — more
    // importantly — this check must stay a strict subset of "is a sandbox already
    // there right now," never "assume the identity is ours to clear": the
    // name-collision-retry branch below is what handles a LIVE concurrent creator,
    // and it deliberately never calls remove_by_name.
    if find_running_by_name(&backend, &identity.name, &container.image)
        .await
        .is_some()
    {
        backend.remove_by_name(&identity.name);
    }

    match create_fresh_reuse(&container, &backend, &identity, &env, &registry).await {
        Ok(guard) => Ok(guard),
        Err(e) if crate::reuse::is_name_conflict(&e) => {
            // Another process won the create race. Re-enter the adopt path once,
            // reading whatever the winner has (or hasn't yet) written — if that
            // doesn't pan out either, surface the ORIGINAL collision error rather
            // than inventing a new one, and critically do NOT best-effort-remove
            // anything here: unlike the stale-registry branch above, a name
            // collision means a sandbox some OTHER live process just legitimately
            // created is sitting there, and removing it out from under that
            // process would defeat the entire point of reuse.
            match registry.read() {
                Some(entry) => match try_adopt(&container, &backend, &identity, &entry).await {
                    Some(guard) => Ok(guard),
                    None => Err(e),
                },
                None => Err(e),
            }
        }
        Err(e) => Err(e),
    }
}

/// Best-effort query for whether a sandbox is already running under `name` —
/// [`start_reuse`]'s own crash-mid-boot orphan check, built around the minimal
/// [`ContainerSpec`] [`SandboxBackend::find_running`] actually needs (both real
/// backends' implementations key only on `spec.name`; see `rightsize-msb` and
/// `rightsize-docker`'s own `find_running`). `None` on any failure or "not
/// running" — same fold as [`SandboxBackend::find_running`]'s own contract, since
/// this is a pure best-effort probe, never a fatal error in its own right.
async fn find_running_by_name(
    backend: &Arc<dyn SandboxBackend>,
    name: &str,
    image: &str,
) -> Option<Box<dyn SandboxHandle>> {
    let probe = ContainerSpec::new(name, image, RunId::value());
    backend.find_running(&probe).await.ok().flatten()
}

/// Attempts to adopt an already-running reuse sandbox recorded in `entry`: verifies
/// it's actually running (via [`SandboxBackend::find_running`]), then re-runs the
/// container's own wait strategy against the registry's recorded ports. `None` for
/// any failure along the way (not running, a currently-exposed port missing from the
/// registry, or the wait strategy failing) — every failure mode here is the caller's
/// cue to fall back to a fresh create, never a fatal error in its own right.
async fn try_adopt(
    container: &Container,
    backend: &Arc<dyn SandboxBackend>,
    identity: &crate::reuse::Identity,
    entry: &crate::reuse::RegistryEntry,
) -> Option<ContainerGuard> {
    let mut mapped_ports = Vec::with_capacity(container.exposed_ports.len());
    for &guest_port in &container.exposed_ports {
        let host_port = *entry.ports.get(&guest_port.to_string())?;
        mapped_ports.push((guest_port, host_port));
    }

    let adopted_spec = ContainerSpec {
        name: identity.name.clone(),
        image: container.image.clone(),
        env: dedup_env_last_wins(container.env.clone()),
        command: container.command.clone(),
        ports: mapped_ports
            .iter()
            .map(|&(guest_port, host_port)| crate::model::PortBinding {
                host_port,
                guest_port,
            })
            .collect(),
        mounts: container.mounts.clone(),
        network_id: None,
        aliases: container.aliases.clone(),
        run_id: RunId::value().to_string(),
        memory_limit_mb: container.memory_limit_mb,
        keep_alive: true,
        // Reuse + from_checkpoint is rejected up front in `Container::start()`
        // (`RightsizeError::ReuseCheckpointConflict`) — a reuse sandbox never
        // carries a checkpoint ref.
        checkpoint_ref: None,
        disk_limit_mb: container.disk_limit_mb,
        tmpfs_root_mb: container.tmpfs_root_mb,
        network_disabled: container.network_disabled,
    };

    let Ok(Some(handle)) = backend.find_running(&adopted_spec).await else {
        return None;
    };

    let raw = RawWaitTarget {
        host: "127.0.0.1",
        mapped_ports: &mapped_ports,
        exposed_ports: &container.exposed_ports,
        backend,
        handle: handle.as_ref(),
        name: &identity.name,
        image: &container.image,
    };
    if container
        .wait_strategy
        .wait_until_ready(&raw)
        .await
        .is_err()
    {
        return None;
    }

    let diagnostics_handle_id = handle.id().to_string();
    let diagnostics_spec = handle.spec().clone();
    let diagnostics_ports = mapped_ports.clone();
    let guard = ContainerGuard {
        handle: Some(handle),
        backend: backend.clone(),
        mapped_ports: Mutex::new(mapped_ports),
        network: None,
        image: container.image.clone(),
        exposed_ports: container.exposed_ports.clone(),
        name: identity.name.clone(),
        ledger_name: identity.name.clone(),
        keep_alive: true,
        reaper_cache_dir_override: container.reaper_cache_dir_override.clone(),
        checkpoint_cache_dir_override: container.checkpoint_cache_dir_override.clone(),
        wait_strategy: container.wait_strategy.clone(),
        network_links: Vec::new(),
    };
    crate::diagnostics::register(
        &guard.ledger_name,
        &guard.image,
        guard.host(),
        diagnostics_ports,
        backend.clone(),
        &diagnostics_handle_id,
        diagnostics_spec,
    );
    Some(guard)
}

/// Creates and starts a reuse sandbox under the identity-derived, FIXED
/// `rz-reuse-<12hex>` name, retrying with freshly allocated ports on a host-port
/// bind conflict — the same retry discipline [`create_started_container`] uses for
/// an ordinary container. Ported here as its own helper (rather than inlined
/// straight-line code) because a reuse sandbox's name is deterministic
/// (identity-derived) instead of a fresh name-per-attempt, so only the ports (and
/// the spec built from them) change between attempts; everything else about the
/// retry — release ports, stop+remove the failed attempt, `is_port_bind_conflict`
/// as the sole retry trigger, the same exhausted-attempts error — mirrors
/// [`create_started_container`] exactly.
async fn create_and_start_reuse_sandbox(
    backend: &Arc<dyn SandboxBackend>,
    container: &Container,
    identity: &crate::reuse::Identity,
    env: &[(String, String)],
) -> Result<(Box<dyn SandboxHandle>, Vec<(u16, u16)>)> {
    let mut last_conflict: Option<RightsizeError> = None;

    for _ in 0..PORT_BIND_ATTEMPTS {
        let mapped_ports = allocate_ports(&container.exposed_ports)?;

        let mut spec = ContainerSpec {
            name: identity.name.clone(),
            image: container.image.clone(),
            env: env.to_vec(),
            command: container.command.clone(),
            ports: mapped_ports
                .iter()
                .map(|&(guest_port, host_port)| crate::model::PortBinding {
                    host_port,
                    guest_port,
                })
                .collect(),
            mounts: container.mounts.clone(),
            network_id: None,
            aliases: container.aliases.clone(),
            run_id: RunId::value().to_string(),
            memory_limit_mb: container.memory_limit_mb,
            keep_alive: true,
            checkpoint_ref: None,
            disk_limit_mb: container.disk_limit_mb,
            tmpfs_root_mb: container.tmpfs_root_mb,
            network_disabled: container.network_disabled,
        };
        if let Some(customizer) = &container.spec_customizer {
            let lookup: std::collections::HashMap<u16, u16> =
                mapped_ports.iter().copied().collect();
            let mapped_fn = move |guest: u16| -> u16 {
                *lookup
                    .get(&guest)
                    .expect("customizer looked up an unexposed port")
            };
            spec = customizer(spec, &mapped_fn);
        }
        spec.env = dedup_env_last_wins(spec.env);

        // Re-validated against the FINISHED spec — same rationale as
        // `create_started_container`'s own post-customizer pass: a customizer
        // just ran and can have set any of these directly on `spec`.
        if let Err(e) = validate_spec_conflicts(
            spec.disk_limit_mb,
            spec.tmpfs_root_mb,
            spec.memory_limit_mb,
            spec.network_disabled,
            spec.network_id.as_deref(),
        ) {
            release_ports(&mapped_ports);
            return Err(e);
        }

        let handle = match backend.create(spec).await {
            Ok(h) => h,
            Err(e) => {
                release_ports(&mapped_ports);
                return Err(e);
            }
        };
        match backend.start(handle.as_ref()).await {
            Ok(()) => return Ok((handle, mapped_ports)),
            Err(e) => {
                let _ = backend.stop(handle.as_ref()).await;
                let _ = backend.remove(handle.as_ref()).await;
                release_ports(&mapped_ports);
                if is_port_bind_conflict(&e) {
                    last_conflict = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(RightsizeError::Backend(format!(
        "Could not bind free host ports for {} after {PORT_BIND_ATTEMPTS} attempts — another \
         process kept grabbing the allocated ports first; if this persists, check for a port \
         scanner/leaked process racing the allocator on this host{}",
        Container::describe(&identity.name, &container.image),
        last_conflict
            .map(|c| format!(" (last conflict: {c})"))
            .unwrap_or_default(),
    )))
}

/// Creates a brand-new reuse sandbox: allocates ports, creates+starts it under the
/// identity-derived `rz-reuse-<12hex>` name with `keep_alive: true` (retrying on a
/// host-port bind conflict — see [`create_and_start_reuse_sandbox`]), runs the wait
/// strategy, and — only on success — writes the registry file. Any failure after
/// resources are allocated tears the sandbox down for real (never through a
/// keep_alive guard's own `stop()`, which would leave a possibly-broken sandbox
/// running with no registry entry pointing at it — an actual leak, not a feature).
async fn create_fresh_reuse(
    container: &Container,
    backend: &Arc<dyn SandboxBackend>,
    identity: &crate::reuse::Identity,
    env: &[(String, String)],
    registry: &crate::reuse::Registry,
) -> Result<ContainerGuard> {
    let (handle, mapped_ports) =
        create_and_start_reuse_sandbox(backend, container, identity, env).await?;

    let raw = RawWaitTarget {
        host: "127.0.0.1",
        mapped_ports: &mapped_ports,
        exposed_ports: &container.exposed_ports,
        backend,
        handle: handle.as_ref(),
        name: &identity.name,
        image: &container.image,
    };
    if let Err(e) = container.wait_strategy.wait_until_ready(&raw).await {
        let _ = backend.stop(handle.as_ref()).await;
        let _ = backend.remove(handle.as_ref()).await;
        release_ports(&mapped_ports);
        return Err(e);
    }

    // Success: write the registry BEFORE handing back the guard — best-effort; a
    // write failure here shouldn't fail a boot that already succeeded (the next
    // start() attempt just won't find a registry and will create fresh again,
    // which is safe, just not the reuse win this call almost delivered).
    let entry = crate::reuse::RegistryEntry {
        name: identity.name.clone(),
        image: container.image.clone(),
        ports: mapped_ports
            .iter()
            .map(|&(guest_port, host_port)| (guest_port.to_string(), host_port))
            .collect(),
        created_iso: crate::reuse::now_iso8601(),
        backend: backend.name().to_string(),
    };
    let _ = registry.write_atomic(&entry);

    let diagnostics_handle_id = handle.id().to_string();
    let diagnostics_spec = handle.spec().clone();
    let diagnostics_ports = mapped_ports.clone();
    let guard = ContainerGuard {
        handle: Some(handle),
        backend: backend.clone(),
        mapped_ports: Mutex::new(mapped_ports),
        network: None,
        image: container.image.clone(),
        exposed_ports: container.exposed_ports.clone(),
        name: identity.name.clone(),
        ledger_name: identity.name.clone(),
        keep_alive: true,
        reaper_cache_dir_override: container.reaper_cache_dir_override.clone(),
        checkpoint_cache_dir_override: container.checkpoint_cache_dir_override.clone(),
        wait_strategy: container.wait_strategy.clone(),
        network_links: Vec::new(),
    };
    crate::diagnostics::register(
        &guard.ledger_name,
        &guard.image,
        guard.host(),
        diagnostics_ports,
        backend.clone(),
        &diagnostics_handle_id,
        diagnostics_spec,
    );

    if let Some(post_start) = &container.post_start {
        if let Err(e) = post_start(&guard).await {
            let handle_ref = guard.handle_ref();
            let _ = backend.stop(handle_ref).await;
            let _ = backend.remove(handle_ref).await;
            registry.delete();
            for &(_, host_port) in guard
                .mapped_ports
                .lock()
                .expect("mapped_ports mutex poisoned")
                .iter()
            {
                free_ports().release(host_port);
            }
            return Err(e);
        }
    }

    Ok(guard)
}

/// The RAII guard for a running container. Dropping it without calling
/// [`ContainerGuard::stop`] still tears the container down — see the module docs for
/// the two-tier cleanup story.
pub struct ContainerGuard {
    handle: Option<Box<dyn SandboxHandle>>,
    backend: Arc<dyn SandboxBackend>,
    mapped_ports: Mutex<Vec<(u16, u16)>>,
    network: Option<Arc<Network>>,
    image: String,
    exposed_ports: Vec<u16>,
    /// `SandboxHandle::id()` at creation time — the backend-native id (msb: the
    /// same as `ledger_name`; docker: the daemon-assigned container id). Used only
    /// for internal error text ([`Self::describe`]); the public [`Self::name`]
    /// accessor and the diagnostics report both use `ledger_name` instead, since
    /// that's the name a caller can actually act on (e.g. `docker logs <name>`).
    name: String,
    /// The reaping ledger's own name for this sandbox (`ContainerSpec::name`,
    /// e.g. `rz-<run-id>-<seq>`) — see [`Container::start`]'s doc at the capture
    /// site for why this differs from `name` (the raw `SandboxHandle::id()`) on
    /// the docker backend.
    ledger_name: String,
    /// Mirrors `ContainerSpec::keep_alive` — a reuse sandbox is kept out of every
    /// own-process automatic cleanup path (see [`Drop`]'s impl below).
    keep_alive: bool,
    /// Carries [`Container::with_reaper_cache_dir_override`]'s value across into
    /// `stop_inner`/`Drop`'s own `crate::reaper::after_stop` calls — see that
    /// builder's doc. `None` (the real cache dir) for every real caller.
    reaper_cache_dir_override: Option<std::path::PathBuf>,
    /// Carries [`Container::with_checkpoint_cache_dir_override`]'s value across
    /// into [`Self::checkpoint_named`]'s own registry writes. `None` (the real
    /// cache dir) for every real caller.
    checkpoint_cache_dir_override: Option<std::path::PathBuf>,
    /// The wait strategy this container was started with — re-run by
    /// [`Self::checkpoint`] when the backend's checkpoint mechanism restarts the
    /// workload (see [`crate::backend::Capabilities::checkpoint_restarts_workload`]).
    /// `Arc`, not `Box`, so both the guard and the reuse-adopt path (which only
    /// ever borrows a `&Container`) can share the same strategy instance without
    /// cloning a `dyn WaitStrategy` itself.
    wait_strategy: Arc<dyn WaitStrategy>,
    /// The network links installed at `start()` time (empty if this container has
    /// no `network`) — re-sent to `install_network_links` by [`Self::checkpoint`]
    /// when the backend's checkpoint mechanism restarts the workload, since a
    /// guest reboot kills msb's emulated exec-tunnel links just like it would any
    /// other in-guest state. Always empty for a reuse-adopted guard: reuse and
    /// networks are mutually exclusive (see `RightsizeError::ReuseNetworkConflict`),
    /// so there's never anything to re-install there.
    network_links: Vec<crate::backend::NetworkLink>,
}

impl ContainerGuard {
    fn handle_ref(&self) -> &dyn SandboxHandle {
        self.handle
            .as_deref()
            .expect("ContainerGuard invariant: handle is only None after being consumed by stop()")
    }

    fn as_network_member(&self) -> Arc<dyn NetworkMember> {
        Arc::new(GuardMemberSnapshot {
            mapped_ports: self
                .mapped_ports
                .lock()
                .expect("mapped_ports mutex poisoned")
                .clone(),
        })
    }

    /// The network this container joined, if any — a module may use this to
    /// resolve a sibling's alias from the guard itself rather than needing to keep a
    /// separate `Network` reference around.
    pub fn network(&self) -> Option<&Arc<Network>> {
        self.network.as_ref()
    }

    /// The host address published ports are reachable on — always loopback.
    pub fn host(&self) -> &str {
        "127.0.0.1"
    }

    /// The backend-facing sandbox name (e.g. `rz-<run-id>-<seq>`, or
    /// `rz-reuse-<12hex>` for a reuse container — see [`Container::reuse`]). This
    /// is always the human-readable ledger/reaping name, on every backend — never
    /// the docker daemon's opaque container id — matching what the diagnostics
    /// report and the reaping ledger both name this sandbox.
    // Deliberately returns `ledger_name`, not the field literally called `name`
    // (which holds the raw `SandboxHandle::id()` — see that field's own doc):
    // the ledger/human name is the one a caller can act on, and is what this
    // accessor has always been documented to return.
    #[allow(clippy::misnamed_getters)]
    pub fn name(&self) -> &str {
        &self.ledger_name
    }

    /// The host port `guest_port` is published on.
    ///
    /// Distinguishes two failure causes: if this guard isn't running (stopped, or never
    /// successfully started), the error says so; if it IS running but `guest_port` was
    /// never declared via `with_exposed_ports`, the error says the port isn't exposed.
    pub fn get_mapped_port(&self, guest_port: u16) -> Result<u16> {
        let mapped = self
            .mapped_ports
            .lock()
            .expect("mapped_ports mutex poisoned");
        if let Some(&(_, host_port)) = mapped.iter().find(|&&(g, _)| g == guest_port) {
            return Ok(host_port);
        }
        if !self.is_running() {
            Err(RightsizeError::Backend(format!(
                "Cannot get mapped port {guest_port} on {}: the container is not running — call \
                 start() first, or check that it did not stop/fail after start()",
                self.describe()
            )))
        } else {
            Err(RightsizeError::Backend(format!(
                "Port {guest_port} is not exposed on {} — call with_exposed_ports({guest_port}) \
                 before start(), or check exposed_ports for the port you actually declared",
                self.describe()
            )))
        }
    }

    /// The full logs captured so far. Requires the container to be running.
    pub async fn logs(&self) -> Result<String> {
        self.backend.logs(self.require_handle()?).await
    }

    /// Runs `cmd` inside the running container and returns its exit code and captured
    /// output.
    pub async fn exec(&self, cmd: &[&str]) -> Result<ExecResult> {
        let cmd: Vec<String> = cmd.iter().map(|s| s.to_string()).collect();
        self.backend.exec(self.require_handle()?, &cmd).await
    }

    /// Copies `host_path` (a file or directory) into this RUNNING container at
    /// `container_path` — the RUNTIME counterpart to a start-time
    /// [`Container::with_copy_file_to_container`] mount, usable any time after
    /// `start()` rather than only pre-boot. `container_path` must be absolute (both
    /// backend CLIs require a `NAME:/abs/path` shape); its parent directory is
    /// created in the guest first (`mkdir -p`), so the destination never has to
    /// pre-exist. Directory semantics match `cp -r`/`docker cp`/`msb copy`
    /// themselves: copying a directory to an absent destination produces the
    /// destination as a copy of the source (contents under the destination, not
    /// nested one level deeper).
    ///
    /// Requires this guard to currently be running and `container_path` to be
    /// absolute — both checked BEFORE any backend call, same state-error shape as
    /// [`Self::exec`]/[`Self::logs`]. Works on a reuse container (it's an ordinary
    /// runtime operation), but mutates shared reused state and is NOT part of the
    /// reuse identity hash — see the reuse docs.
    pub async fn copy_file_to_container(
        &self,
        host_path: impl AsRef<std::path::Path>,
        container_path: &str,
    ) -> Result<()> {
        let handle = self.require_handle()?;
        require_absolute_container_path(container_path)?;
        if let Some(parent) = container_parent_dir(container_path) {
            let mkdir = vec!["mkdir".to_string(), "-p".to_string(), parent.to_string()];
            self.backend.exec(handle, &mkdir).await?;
        }
        self.backend
            .copy_to_container(handle, host_path.as_ref(), container_path)
            .await
    }

    /// Convenience for [`Self::copy_file_to_container`] when the content to copy in
    /// only exists in memory: writes `content` to a private (mode `0600` on unix)
    /// temp file, delegates to [`Self::copy_file_to_container`], and removes the
    /// temp file afterward regardless of the outcome. No streaming protocol — this
    /// is exactly the file path, minus the caller having to manage one.
    pub async fn copy_content_to_container(
        &self,
        content: impl AsRef<[u8]>,
        container_path: &str,
    ) -> Result<()> {
        let temp = TempCopyFile::create(content.as_ref())?;
        self.copy_file_to_container(&temp.path, container_path)
            .await
    }

    /// The reverse direction of [`Self::copy_file_to_container`]: copies
    /// `container_path` (a file or directory) out of this RUNNING container to
    /// `host_path`. `host_path`'s parent directory is created on the host first
    /// (via the stdlib), so the destination never has to pre-exist. Same
    /// running/absolute-path checks as the copy-in direction, in the same order,
    /// before any backend call.
    pub async fn copy_file_from_container(
        &self,
        container_path: &str,
        host_path: impl AsRef<std::path::Path>,
    ) -> Result<()> {
        let handle = self.require_handle()?;
        require_absolute_container_path(container_path)?;
        let host_path = host_path.as_ref();
        if let Some(parent) = host_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        self.backend
            .copy_from_container(handle, container_path, host_path)
            .await
    }

    /// Checkpoints this RUNNING container via the active backend's own
    /// `create_checkpoint` mechanism (docker: image commit; microsandbox: disk
    /// snapshot), and returns a [`Checkpoint`] carrying the resulting ref, which
    /// backend created it, and this container's full spec (see [`Checkpoint`]'s own
    /// doc for the "filesystem capture, not memory" semantics and
    /// [`Container::from_checkpoint`] for restoring from the result).
    ///
    /// Gated on [`crate::backend::Capabilities::checkpoint`] BEFORE any backend
    /// call: on a backend that doesn't support it (a test double only — both real
    /// backends do), this returns [`RightsizeError::CheckpointUnsupported`] without
    /// ever reaching `create_checkpoint`. Requires this guard to currently be
    /// running — a state error otherwise, same shape as [`Self::exec`]/[`Self::logs`].
    ///
    /// When the backend's checkpoint mechanism restarts the sandbox's workload
    /// (`capabilities().checkpoint_restarts_workload` — microsandbox's stop/
    /// snapshot/start cycle reboots the guest), this re-installs this container's
    /// network links (if any were installed at `start()` time — a reboot kills
    /// msb's emulated exec-tunnel links along with everything else in the guest)
    /// and then re-runs the container's own configured wait strategy before
    /// returning, so a caller never gets back a false-ready or unreachable-by-alias
    /// container. Docker's image commit leaves the container undisturbed, so
    /// neither re-installation nor re-wait runs there.
    pub async fn checkpoint(&self) -> Result<Checkpoint> {
        self.ensure_checkpoint_capable()?;
        let handle = self.require_handle()?;
        self.ensure_checkpoint_target_survives_a_stop(handle)?;
        let nonce = checkpoint::generate_ref_nonce();
        let (checkpoint_ref, spec) = self.checkpoint_core(handle, &nonce).await?;
        Ok(Checkpoint {
            checkpoint_ref,
            backend: self.backend.name().to_string(),
            spec,
        })
    }

    /// Named counterpart to [`Self::checkpoint`]: everything that method does
    /// (the capability gate, the running-guard check, the backend
    /// `create_checkpoint` call, and — on a backend whose checkpoint mechanism
    /// restarts the workload — the network-relink-then-rewait dance) applies
    /// here identically, PLUS a durable, rediscoverable registry entry under
    /// `name` (see the checkpoints docs' "Reusing checkpoints across runs"
    /// section and [`Checkpoint::find`]/[`Checkpoint::list`]/[`Checkpoint::remove`]).
    ///
    /// `name` must match `^[a-z0-9][a-z0-9-]{0,40}$` —
    /// [`RightsizeError::InvalidCheckpointName`] before any backend call or
    /// registry I/O otherwise, the same up-front gate every named-checkpoint
    /// entry point shares.
    ///
    /// **Re-checkpointing an existing name REPLACES it**: if the registry
    /// already has an entry for `name`, this best-effort removes that entry's
    /// backend-native artifact BEFORE taking the new checkpoint — but only when
    /// that entry's backend matches the currently active one, mirroring
    /// [`Checkpoint::find`]'s own same-backend gate. If the entry belongs to a
    /// different backend, the removal call is skipped outright (the active
    /// backend has no business operating on a ref format it didn't create) and
    /// that artifact is left behind under its original backend. Either way, the
    /// registry entry itself is rewritten once the new checkpoint succeeds.
    /// Latest wins in the registry; a skipped cross-backend artifact has to be
    /// cleaned up manually under the backend that created it (see the
    /// checkpoints docs' cross-run section).
    ///
    /// The registry write happens ONLY after the backend checkpoint itself has
    /// succeeded — a `create_checkpoint` failure leaves any existing registry
    /// entry for `name` exactly as it was (already removed above, in the
    /// replace case — so a failed re-checkpoint does lose the old entry; there
    /// is no atomic "keep the old one if the new one fails" here, matching the
    /// backend-level reality that the old artifact was already best-effort torn
    /// down before the new one was attempted).
    ///
    /// The tmpfs-root refusal below (see
    /// [`Self::ensure_checkpoint_target_survives_a_stop`]) runs BEFORE any of
    /// that replace-removal work — a tmpfs-root re-checkpoint that were instead
    /// refused only once it reached the backend's own `create_checkpoint` would
    /// have already best-effort destroyed the previous same-name entry above,
    /// for nothing.
    pub async fn checkpoint_named(&self, name: &str) -> Result<Checkpoint> {
        checkpoint::validate_name(name)?;
        self.ensure_checkpoint_capable()?;
        let handle = self.require_handle()?;
        self.ensure_checkpoint_target_survives_a_stop(handle)?;

        let cache_dir = self
            .checkpoint_cache_dir_override
            .clone()
            .unwrap_or_else(crate::cache_dir::dir);
        let registry = checkpoint::Registry::new(&cache_dir, name);
        if let Some(previous) = registry.read() {
            if previous.backend == self.backend.name() {
                let _ = self
                    .backend
                    .remove_checkpoint(&previous.checkpoint_ref)
                    .await;
            }
        }

        let (checkpoint_ref, spec) = self.checkpoint_core(handle, name).await?;

        let entry = checkpoint::NamedRegistryEntry {
            name: name.to_string(),
            checkpoint_ref: checkpoint_ref.clone(),
            backend: self.backend.name().to_string(),
            created_iso: crate::reuse::now_iso8601(),
            spec: checkpoint::NamedRegistrySpec::from_container_spec(&spec),
        };
        registry.write_atomic(&entry)?;

        Ok(Checkpoint {
            checkpoint_ref,
            backend: self.backend.name().to_string(),
            spec,
        })
    }

    /// The capability gate [`Self::checkpoint`] and [`Self::checkpoint_named`]
    /// both apply BEFORE any backend call: on a backend that doesn't support
    /// checkpointing (a test double only — both real backends do), this
    /// returns [`RightsizeError::CheckpointUnsupported`] without ever reaching
    /// `create_checkpoint`.
    fn ensure_checkpoint_capable(&self) -> Result<()> {
        if !self.backend.capabilities().checkpoint {
            return Err(RightsizeError::CheckpointUnsupported {
                backend: self.backend.name().to_string(),
            });
        }
        Ok(())
    }

    /// Refuses a tmpfs-root container BEFORE any of [`Self::checkpoint`]/
    /// [`Self::checkpoint_named`]'s own remove/registry/backend work, on the
    /// microsandbox backend specifically — its checkpoint mechanism stops the
    /// guest before snapshotting it, and a tmpfs root is RAM-backed and gone the
    /// moment the guest stops, so there is nothing durable left to capture.
    ///
    /// This is a hoist of a check `rightsize-msb`'s own `create_checkpoint`
    /// backend implementation ALSO makes (kept there as a defense-in-depth
    /// backstop) — the reason it has to be duplicated up here, ahead of
    /// everything else, is [`Self::checkpoint_named`]'s replace semantics: that
    /// method best-effort removes the PREVIOUS same-name checkpoint before
    /// asking the backend to take the new one. If the refusal only ever
    /// happened inside the backend's own `create_checkpoint`, a doomed
    /// tmpfs-root re-checkpoint would still have already destroyed the
    /// previous, perfectly good, checkpoint by the time that refusal fired —
    /// gating here, before ANY of that removal work runs, is what keeps a
    /// refused re-checkpoint from being a data-loss bug.
    ///
    /// Docker's image commit never stops the container at all, so nothing here
    /// is at risk on that backend — this only fires for microsandbox.
    fn ensure_checkpoint_target_survives_a_stop(&self, handle: &dyn SandboxHandle) -> Result<()> {
        if handle.spec().tmpfs_root_mb.is_some() && self.backend.name() == "microsandbox" {
            return Err(RightsizeError::TmpfsRootCheckpoint);
        }
        Ok(())
    }

    /// Mints the ref this guard's ACTIVE backend expects to receive for
    /// `nonce_or_name` in [`Self::checkpoint_core`]'s `create_checkpoint` call.
    ///
    /// Every backend but microsandbox gets `nonce_or_name` back UNCHANGED — a
    /// bare random nonce for [`Self::checkpoint`], the caller-chosen name for
    /// [`Self::checkpoint_named`] — and formats its own ref shape from it
    /// (docker: `rightsize/checkpoint:<nonce_or_name>`).
    ///
    /// On microsandbox, this mints the FULL ref up front instead:
    /// `<cache dir>/checkpoints/rz-ckpt-<nonce_or_name>`, an absolute path under
    /// the SAME effective cache dir [`Self::checkpoint_named`]'s own registry
    /// uses (`checkpoint_cache_dir_override`, falling back to
    /// [`crate::cache_dir::dir`]) — this is the override seam's whole point:
    /// before this, `rightsize-msb`'s backend minted this same path itself,
    /// unconditionally from the real [`crate::cache_dir::dir`], ignoring
    /// whatever override a test (or an embedding caller) had set, so a
    /// test-isolated checkpoint could still leak its backend-native artifact
    /// into the real cache dir even though its registry entry stayed
    /// correctly isolated. Minting it here instead means the artifact
    /// destination and the registry agree on the SAME cache dir, always.
    ///
    /// Absolutized via `std::path::absolute` when the resolved cache dir is
    /// itself relative (a relative `RIGHTSIZE_CACHE_DIR`) — msb's own
    /// `--dest-dir` flag needs an absolute path.
    fn microsandbox_checkpoint_ref(&self, nonce_or_name: &str) -> Result<String> {
        if self.backend.name() != "microsandbox" {
            return Ok(nonce_or_name.to_string());
        }
        mint_microsandbox_checkpoint_ref(
            self.checkpoint_cache_dir_override.as_deref(),
            nonce_or_name,
        )
    }

    /// The machinery [`Self::checkpoint`] and [`Self::checkpoint_named`] share:
    /// the backend `create_checkpoint` call under `nonce_or_name` (a random
    /// nonce for the former, the caller-chosen name for the latter), after
    /// resolving it through [`Self::microsandbox_checkpoint_ref`] (a no-op on
    /// every backend but microsandbox), and, on a backend whose checkpoint
    /// mechanism restarts the workload
    /// (`capabilities().checkpoint_restarts_workload` — microsandbox's stop/
    /// snapshot/start cycle reboots the guest), re-installing this container's
    /// network links (if any were installed at `start()` time — a reboot kills
    /// msb's emulated exec-tunnel links along with everything else in the
    /// guest) and re-running the container's own configured wait strategy
    /// before returning, so a caller never gets back a false-ready or
    /// unreachable-by-alias container. Docker's image commit leaves the
    /// container undisturbed, so neither re-installation nor re-wait runs
    /// there. Returns the resulting ref and this container's full spec at
    /// checkpoint time.
    async fn checkpoint_core(
        &self,
        handle: &dyn SandboxHandle,
        nonce_or_name: &str,
    ) -> Result<(String, ContainerSpec)> {
        let backend_ref = self.microsandbox_checkpoint_ref(nonce_or_name)?;
        let checkpoint_ref = self.backend.create_checkpoint(handle, &backend_ref).await?;
        if self.backend.capabilities().checkpoint_restarts_workload {
            if !self.network_links.is_empty() {
                self.backend
                    .install_network_links(handle, &self.network_links)
                    .await?;
            }
            let target = GuardWaitTarget { guard: self };
            self.wait_strategy.wait_until_ready(&target).await?;
        }
        Ok((checkpoint_ref, handle.spec().clone()))
    }

    /// Streams log lines to `consumer` as they're produced. Closing (or dropping) the
    /// returned [`crate::backend::FollowHandle`] halts delivery — no further lines
    /// reach `consumer` afterward, even if the container keeps running.
    pub async fn follow_output(
        &self,
        consumer: impl Fn(String) + Send + Sync + 'static,
    ) -> Result<crate::backend::FollowHandle> {
        self.backend
            .follow_logs(self.require_handle()?, Box::new(consumer))
            .await
    }

    /// True from a successful `start()` until `stop()`; false before the first `start()`
    /// and after.
    pub fn is_running(&self) -> bool {
        self.handle.is_some()
    }

    fn require_handle(&self) -> Result<&dyn SandboxHandle> {
        self.handle.as_deref().ok_or_else(|| {
            RightsizeError::Backend(format!(
                "{} is not running — call start() first",
                self.describe()
            ))
        })
    }

    fn describe(&self) -> String {
        Container::describe(&self.name, &self.image)
    }

    /// Happy-path explicit teardown: stops and removes the container on the backend
    /// (both best-effort — errors are swallowed, since a failed cleanup step
    /// shouldn't block release of the ones after it), then releases its mapped host
    /// ports. Idempotent — a no-op if already stopped
    /// (never re-calls the backend, never double-releases ports), and consumes the
    /// guard so it cannot be used again after this returns.
    pub async fn stop(mut self) -> Result<()> {
        self.stop_inner().await;
        Ok(())
    }

    /// The actual teardown logic, factored out so both `stop(self)` and `Drop` (via the
    /// synchronous fallback) converge on the same "idempotent, ports-then-clear"
    /// contract — `Drop` cannot call this directly (it's async), but it follows the
    /// same shape with blocking primitives instead.
    async fn stop_inner(&mut self) {
        let Some(handle) = self.handle.take() else {
            return; // already stopped (or never started): no-op, no backend call.
        };
        // The diagnostics registry's own "no longer live" moment — mirrors
        // `crate::reaper::after_stop` below, but applies to BOTH branches (keep_alive
        // or not): a reuse sandbox stays alive on the backend, but this guard no
        // longer holds a live handle for it, so it drops out of "what THIS process
        // can currently report on" either way.
        crate::diagnostics::deregister(&self.ledger_name);
        if self.keep_alive {
            // Reuse containers: stop() is the feature's own contract — the sandbox
            // is LEFT RUNNING, and only in-process bookkeeping is cleared. No
            // backend.stop/remove call (that's the whole point), no ledger touch
            // (never listed there in the first place), and no port release: the
            // sandbox is still bound to those host ports for real, and releasing
            // them here would let an unrelated container in this same process grab
            // one out from under it. Mirrors `Drop`'s own keep_alive short-circuit
            // below. `mapped_ports` itself IS in-process bookkeeping, though, so it
            // still gets cleared — `get_mapped_port` must agree with `is_running()`
            // that this guard is no longer live, not keep resolving a port for a
            // handle it no longer holds.
            self.mapped_ports
                .lock()
                .expect("mapped_ports mutex poisoned")
                .clear();
            return;
        }
        let _ = self.backend.stop(handle.as_ref()).await;
        let _ = self.backend.remove(handle.as_ref()).await;
        crate::reaper::after_stop(
            &self.ledger_name,
            self.keep_alive,
            self.reaper_cache_dir_override.as_deref(),
        );
        let mut mapped = self
            .mapped_ports
            .lock()
            .expect("mapped_ports mutex poisoned");
        for &(_, host_port) in mapped.iter() {
            free_ports().release(host_port);
        }
        mapped.clear();
    }
}

struct GuardMemberSnapshot {
    mapped_ports: Vec<(u16, u16)>,
}
impl NetworkMember for GuardMemberSnapshot {
    fn is_running(&self) -> bool {
        true // only constructed for a guard that just successfully started.
    }
    fn mapped_ports(&self) -> Vec<(u16, u16)> {
        self.mapped_ports.clone()
    }
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        // Best-effort SYNCHRONOUS fallback (decision 1). MUST NOT panic; MUST work with
        // no Tokio runtime in context.
        let Some(handle) = self.handle.take() else {
            return; // already stopped via stop(self): nothing to do.
        };
        // Synchronous, unlike the ledger update below — the diagnostics registry is
        // an in-memory `Mutex<Vec<_>>`, not a file, so there's no reason to defer
        // this to the cleanup thread the way `crate::reaper::after_stop` is: this
        // guard is done being "live" the moment Drop starts, regardless of whether
        // the async backend teardown below has run yet.
        crate::diagnostics::deregister(&self.ledger_name);
        if self.keep_alive {
            // A reuse sandbox must survive this guard's own automatic teardown
            // entirely — no port release (the container keeps running bound to
            // them; releasing here would let an unrelated container grab the same
            // host port out from under it) and no cleanup-thread enqueue (which
            // would stop+remove a container meant to outlive this process). See
            // `ContainerSpec::keep_alive`'s doc — every own-run cleanup path leaves
            // a keep_alive sandbox alone, and this is core's own piece of that.
            return;
        }
        // Release ports synchronously here — FreePorts is a plain std Mutex, no runtime
        // needed — so a dropped-not-stopped guard doesn't leak its ports even if the
        // cleanup thread is slow or (in a crash) never runs at all.
        if let Ok(mut mapped) = self.mapped_ports.lock() {
            for &(_, host_port) in mapped.iter() {
                free_ports().release(host_port);
            }
            mapped.clear();
        }
        // The Drop-path's own update to the reaping ledger — mirrors `stop_inner`'s
        // `crate::reaper::after_stop` call, but deferred to run on the cleanup thread,
        // AFTER `cleanup_sync` has actually been attempted (see `crate::cleanup`'s
        // `after_teardown` doc), not here in `Drop` itself. Without this, a sandbox
        // torn down only via this fallback path (never an explicit `.stop()`) stays
        // listed in `.sandboxes` for the rest of THIS process's life — reachable only
        // by a future sweep/watchdog, never by this run's own clean-shutdown deletion
        // trigger — even though it's already gone.
        let ledger_name = self.ledger_name.clone();
        let reaper_cache_dir_override = self.reaper_cache_dir_override.clone();
        cleanup::enqueue(CleanupJob {
            backend: self.backend.clone(),
            container_id: handle.id().to_string(),
            after_teardown: Some(Box::new(move || {
                crate::reaper::after_stop(
                    &ledger_name,
                    false,
                    reaper_cache_dir_override.as_deref(),
                );
            })),
        });
    }
}

/// Mints microsandbox's absolute checkpoint-artifact ref for `nonce_or_name`:
/// `<cache dir>/checkpoints/rz-ckpt-<nonce_or_name>`, resolving the effective
/// cache dir the SAME way [`ContainerGuard::checkpoint_named`]'s own registry
/// does (`cache_dir_override`, falling back to [`crate::cache_dir::dir`]) — the
/// pure seam [`ContainerGuard::microsandbox_checkpoint_ref`] delegates to,
/// factored out as a plain function so this minting logic is unit-testable
/// without constructing a full guard.
///
/// Absolutized via `std::path::absolute` when the resolved cache dir is itself
/// relative (a relative `RIGHTSIZE_CACHE_DIR`, or an override a caller set to a
/// relative path) — msb's own `--dest-dir` flag needs an absolute path.
fn mint_microsandbox_checkpoint_ref(
    cache_dir_override: Option<&std::path::Path>,
    nonce_or_name: &str,
) -> Result<String> {
    let cache_dir = cache_dir_override
        .map(std::path::Path::to_path_buf)
        .unwrap_or_else(crate::cache_dir::dir);
    let cache_dir = if cache_dir.is_absolute() {
        cache_dir
    } else {
        std::path::absolute(&cache_dir)?
    };
    Ok(cache_dir
        .join("checkpoints")
        .join(format!("rz-ckpt-{nonce_or_name}"))
        .display()
        .to_string())
}

impl Checkpoint {
    /// Rediscovers a NAMED checkpoint written by an earlier call to
    /// [`ContainerGuard::checkpoint_named`] — in this process or an earlier
    /// one, as long as both agree on the rightsize cache directory (see
    /// `crate::cache_dir`). `Ok(None)` when no registry entry exists for
    /// `name` at all — including a corrupt/unreadable entry, which is
    /// best-effort cleaned up and treated exactly like "never existed."
    ///
    /// An entry whose `backend` matches the CURRENTLY ACTIVE backend is
    /// PROBED (via `SandboxBackend::has_checkpoint`) before being returned: if
    /// the backend-native artifact is gone (the disk snapshot/image was
    /// deleted out from under the registry), the entry is stale — this
    /// best-effort deletes the registry file and returns `Ok(None)`, same as
    /// "never existed." An entry for a DIFFERENT backend is returned WITHOUT
    /// probing: this crate never forces a backend the host may not even have
    /// installed to answer a query it can't, and
    /// [`Container::from_checkpoint`]'s own restore-time mismatch gate
    /// (`RightsizeError::CheckpointBackendMismatch`) stays the sole authority
    /// for that case.
    ///
    /// A probe failure (the backend's own `has_checkpoint` erroring — a
    /// daemon unreachable, a malformed ref) propagates rather than resolving
    /// to `Ok(None)`: only a definite "not there" is treated as stale.
    ///
    /// `name` is validated (`^[a-z0-9][a-z0-9-]{0,40}$`) before any registry
    /// I/O or backend call.
    pub async fn find(name: &str) -> Result<Option<Checkpoint>> {
        checkpoint::validate_name(name)?;
        find_named(name, &backends::active(), &crate::cache_dir::dir()).await
    }

    /// Every named checkpoint currently registered under the rightsize cache
    /// directory — registry contents only, NO artifact probing (a checkpoint
    /// whose backend-native artifact has since been deleted out from under
    /// the registry still appears here; only [`Self::find`] resolves that
    /// discrepancy, and only for the one name it's asked about).
    /// Corrupt/unreadable entries are silently skipped.
    pub fn list() -> Result<Vec<Checkpoint>> {
        list_named(&crate::cache_dir::dir())
    }

    /// Removes a named checkpoint: best-effort removes the backend-native
    /// artifact via the currently active backend's `remove_checkpoint`, then
    /// deletes the registry file. Returns whether anything existed to remove
    /// — `Ok(false)`, not an error, when `name` has no registry entry at all.
    /// Idempotent: safe to call more than once, or on a name that was never
    /// checkpointed — "not found anywhere" is success.
    ///
    /// The artifact-removal call is skipped when the entry's backend doesn't
    /// match the currently active one — the same same-backend gate
    /// [`Checkpoint::find`] applies — since the active backend has no business
    /// operating on a ref format it didn't create. The registry entry is still
    /// deleted (and this still returns `Ok(true)`) either way; a skipped
    /// cross-backend artifact is left behind and must be cleaned up manually
    /// under the backend that created it (see the checkpoints docs' cross-run
    /// section).
    ///
    /// `name` is validated before any registry I/O or backend call.
    pub async fn remove(name: &str) -> Result<bool> {
        checkpoint::validate_name(name)?;
        remove_named(name, &backends::active(), &crate::cache_dir::dir()).await
    }

    /// Exports this checkpoint to a portable archive at `path`: a plain tar
    /// containing pinned JSON metadata (`checkpoint.json`) plus the backend's own
    /// checkpoint payload (`artifact`), written byte-for-byte exactly as the
    /// backend CLI produced it (msb's `snapshot save`; docker's `docker save`).
    /// See the checkpoints docs' "Moving checkpoints between machines" section for
    /// the full story — the destination machine pulls the image fresh on first
    /// boot rather than the archive bundling it, size expectations, and the msb
    /// digest-shaped ref an import produces.
    ///
    /// Requires the ACTIVE backend to equal `self.backend` — before any backend or
    /// filesystem work, this fails with [`RightsizeError::CheckpointBackendMismatch`]
    /// otherwise. The backend-native artifact is then probed
    /// (`SandboxBackend::has_checkpoint`): exporting a stale checkpoint fails with
    /// [`RightsizeError::CheckpointArtifactMissing`] rather than producing a broken
    /// archive. `path`'s parent directories are created if missing; a pre-existing
    /// file at `path` is overwritten. Works on an unnamed checkpoint too — the
    /// archive's `name` field is simply `null`.
    pub async fn export_to(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        export_to_impl(
            self,
            path.as_ref(),
            &backends::active(),
            &crate::cache_dir::dir(),
            &std::env::temp_dir(),
        )
        .await
    }

    /// Imports a checkpoint archive written by [`Self::export_to`], materializing
    /// its payload on the ACTIVE backend and returning a `Checkpoint` restorable
    /// via [`crate::Container::from_checkpoint`] exactly like any other.
    ///
    /// The archive is extracted to a fresh temp dir and fully validated BEFORE any
    /// backend call or registry write: the file must exist and extract as a tar
    /// containing `checkpoint.json`, which must parse, carry
    /// `rightsizeArchive: 1`, a `name` matching the checkpoint-name pattern when
    /// non-null, and a `backend` equal to the currently active one. Each violation
    /// is a typed error — [`RightsizeError::MalformedArchive`],
    /// [`RightsizeError::InvalidCheckpointName`], or
    /// [`RightsizeError::CheckpointBackendMismatch`] respectively.
    ///
    /// The backend then materializes the artifact
    /// (`SandboxBackend::import_checkpoint`) and returns the EFFECTIVE ref: docker's
    /// is the original ref unchanged; microsandbox's is the resolved digest
    /// (content-addressed, so re-importing the identical archive twice is a
    /// harmless no-op that resolves to the same ref both times). A NAMED archive
    /// then replaces the matching registry entry — best-effort removing the
    /// PREVIOUS same-backend artifact first, but only when its ref actually
    /// differs from the new effective ref, so re-importing the identical archive
    /// never deletes the artifact it just materialized — and writes the new entry
    /// under the effective ref. An UNNAMED archive writes no registry entry at all
    /// and returns an ephemeral `Checkpoint`.
    pub async fn import_from(path: impl AsRef<std::path::Path>) -> Result<Checkpoint> {
        import_from_impl(
            path.as_ref(),
            &backends::active(),
            &crate::cache_dir::dir(),
            &std::env::temp_dir(),
        )
        .await
    }
}

/// [`Checkpoint::find`]'s actual logic, parameterized over the backend and
/// cache dir so this crate's own unit tests can exercise it against a fake
/// backend and a temp directory instead of the real process-wide active
/// backend and `~/.cache/rightsize`.
async fn find_named(
    name: &str,
    backend: &Arc<dyn SandboxBackend>,
    cache_dir: &std::path::Path,
) -> Result<Option<Checkpoint>> {
    let registry = checkpoint::Registry::new(cache_dir, name);
    let Some(entry) = registry.read() else {
        // Missing OR corrupt: best-effort clean up a corrupt file, harmless
        // no-op if genuinely absent.
        registry.delete();
        return Ok(None);
    };

    if entry.backend != backend.name() {
        // A different backend's entry is returned unprobed — see this
        // method's own doc for why.
        return Ok(Some(checkpoint_from_entry(entry)));
    }

    match backend.has_checkpoint(&entry.checkpoint_ref).await {
        Ok(true) => Ok(Some(checkpoint_from_entry(entry))),
        Ok(false) => {
            // Definitely gone: the registry entry is stale.
            registry.delete();
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// [`Checkpoint::list`]'s actual logic, parameterized over the cache dir for
/// the same reason [`find_named`] is.
fn list_named(cache_dir: &std::path::Path) -> Result<Vec<Checkpoint>> {
    let entries = checkpoint::list_registry_entries(cache_dir)?;
    Ok(entries.into_iter().map(checkpoint_from_entry).collect())
}

/// [`Checkpoint::remove`]'s actual logic, parameterized over the backend and
/// cache dir for the same reason [`find_named`] is.
async fn remove_named(
    name: &str,
    backend: &Arc<dyn SandboxBackend>,
    cache_dir: &std::path::Path,
) -> Result<bool> {
    let registry = checkpoint::Registry::new(cache_dir, name);
    let existed = registry.exists();
    if let Some(entry) = registry.read() {
        if entry.backend == backend.name() {
            let _ = backend.remove_checkpoint(&entry.checkpoint_ref).await;
        }
    }
    registry.delete();
    Ok(existed)
}

/// [`Checkpoint::export_to`]'s actual logic, parameterized over the backend and
/// cache dir for the same reason [`find_named`] is, plus `staging_parent` — the
/// directory the export's [`crate::archive::TempStagingDir`] is created under
/// (`std::env::temp_dir()` in production; a private per-test directory in a
/// test that wants a deterministic staging-cleanup scan).
async fn export_to_impl(
    cp: &Checkpoint,
    dest: &std::path::Path,
    backend: &Arc<dyn SandboxBackend>,
    cache_dir: &std::path::Path,
    staging_parent: &std::path::Path,
) -> Result<()> {
    if cp.backend != backend.name() {
        return Err(RightsizeError::CheckpointBackendMismatch {
            active_backend: backend.name().to_string(),
            checkpoint_backend: cp.backend.clone(),
        });
    }
    if !backend.has_checkpoint(&cp.checkpoint_ref).await? {
        return Err(RightsizeError::CheckpointArtifactMissing {
            checkpoint_ref: cp.checkpoint_ref.clone(),
            backend: cp.backend.clone(),
        });
    }

    // Guard: cleaned up on drop, success or failure alike (see the guard's own
    // doc for why this is the finally/defer equivalent here).
    let staging = crate::archive::TempStagingDir::create_in(staging_parent, "export")?;
    backend
        .export_checkpoint(
            &cp.checkpoint_ref,
            &crate::archive::artifact_path(staging.path()),
        )
        .await?;

    // A `Checkpoint` doesn't carry its own name — only the registry does — so
    // recover it (if any) by matching this ref/backend against every registered
    // entry. `None` (an unnamed archive) either for a genuinely unnamed
    // checkpoint or one whose registry entry has since been removed/replaced.
    let name = find_registered_name(cache_dir, &cp.backend, &cp.checkpoint_ref);
    let manifest = crate::archive::ArchiveManifest {
        rightsize_archive: crate::archive::FORMAT_VERSION,
        name,
        checkpoint_ref: cp.checkpoint_ref.clone(),
        backend: cp.backend.clone(),
        created_iso: crate::reuse::now_iso8601(),
        spec: checkpoint::NamedRegistrySpec::from_container_spec(&cp.spec),
    };
    crate::archive::write_manifest(&crate::archive::manifest_path(staging.path()), &manifest)?;

    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    crate::archive::tar_create(dest, staging.path()).await
}

/// The reverse-lookup [`export_to_impl`] needs: the name of the named-checkpoint
/// registry entry (if any) whose `checkpoint_ref`/`backend` match exactly. A
/// missing/unreadable registry directory resolves to `None`, same as "nothing
/// registered" — this is a best-effort archive-metadata lookup, never a fatal
/// error in its own right.
fn find_registered_name(
    cache_dir: &std::path::Path,
    backend: &str,
    checkpoint_ref: &str,
) -> Option<String> {
    checkpoint::list_registry_entries(cache_dir)
        .ok()?
        .into_iter()
        .find(|entry| entry.backend == backend && entry.checkpoint_ref == checkpoint_ref)
        .map(|entry| entry.name)
}

/// [`Checkpoint::import_from`]'s actual logic, parameterized over the backend and
/// cache dir for the same reason [`find_named`] is, plus `staging_parent` — see
/// [`export_to_impl`]'s doc for why this is a parameter rather than always
/// `std::env::temp_dir()`.
async fn import_from_impl(
    src: &std::path::Path,
    backend: &Arc<dyn SandboxBackend>,
    cache_dir: &std::path::Path,
    staging_parent: &std::path::Path,
) -> Result<Checkpoint> {
    let staging = crate::archive::TempStagingDir::create_in(staging_parent, "import")?;
    crate::archive::tar_extract(src, staging.path()).await?;

    let manifest =
        crate::archive::read_manifest(src, &crate::archive::manifest_path(staging.path()))?;
    if manifest.rightsize_archive != crate::archive::FORMAT_VERSION {
        return Err(RightsizeError::MalformedArchive {
            path: src.to_path_buf(),
            reason: format!(
                "unsupported rightsizeArchive version {} (this port understands only {})",
                manifest.rightsize_archive,
                crate::archive::FORMAT_VERSION
            ),
        });
    }
    if let Some(name) = &manifest.name {
        checkpoint::validate_name(name)?;
    }
    if manifest.backend != backend.name() {
        return Err(RightsizeError::CheckpointBackendMismatch {
            active_backend: backend.name().to_string(),
            checkpoint_backend: manifest.backend.clone(),
        });
    }

    // Every check above is pure (filesystem-only) — this is the first backend
    // call import_from ever makes.
    let effective_ref = backend
        .import_checkpoint(
            &crate::archive::artifact_path(staging.path()),
            &manifest.checkpoint_ref,
        )
        .await?;

    if let Some(name) = &manifest.name {
        let registry = checkpoint::Registry::new(cache_dir, name);
        if let Some(previous) = registry.read() {
            // Only remove the OLD artifact when it's actually a different ref: msb
            // imports are content-addressed, so re-importing the identical
            // archive under the same name resolves to the SAME effective ref the
            // previous import already registered — best-effort-removing it here
            // would delete the artifact this call just materialized, before the
            // registry write below even lands.
            if previous.backend == backend.name() && previous.checkpoint_ref != effective_ref {
                let _ = backend.remove_checkpoint(&previous.checkpoint_ref).await;
            }
        }
        let entry = checkpoint::NamedRegistryEntry {
            name: name.clone(),
            checkpoint_ref: effective_ref.clone(),
            backend: backend.name().to_string(),
            created_iso: crate::reuse::now_iso8601(),
            spec: manifest.spec.clone(),
        };
        registry.write_atomic(&entry)?;
    }

    Ok(checkpoint_from_reduced(
        manifest.name.as_deref().unwrap_or("imported"),
        effective_ref,
        backend.name().to_string(),
        manifest.spec,
    ))
}

/// Builds a [`Checkpoint`] from a reduced spec plus the identity a full registry
/// entry would otherwise carry — the shared construction [`checkpoint_from_entry`]
/// (a rediscovered NAMED checkpoint) and [`import_from_impl`] (an imported archive,
/// named or not) both need. `display_name` only ever feeds the placeholder
/// `ContainerSpec::name` (`rz-checkpoint-<display_name>`) — never read by
/// `Container::from_checkpoint`, which only reads `Checkpoint::checkpoint_ref` for
/// that. Every other placeholder field mirrors [`checkpoint_from_entry`]'s
/// original doc: `from_checkpoint` never reads a checkpoint's `spec.mounts`,
/// `network_id`, `aliases`, `run_id`, `keep_alive`, or `spec.checkpoint_ref`. Host
/// ports are unknowable from a persisted spec (only the guest side was ever saved)
/// and unused by `from_checkpoint` either way (it re-derives fresh host ports at
/// `start()` time), so they're placeholder `0`.
fn checkpoint_from_reduced(
    display_name: &str,
    checkpoint_ref: String,
    backend: String,
    spec: checkpoint::NamedRegistrySpec,
) -> Checkpoint {
    let ports = spec
        .exposed_ports
        .iter()
        .map(|&guest_port| crate::model::PortBinding {
            host_port: 0,
            guest_port,
        })
        .collect();
    Checkpoint {
        checkpoint_ref: checkpoint_ref.clone(),
        backend,
        spec: ContainerSpec {
            name: format!("rz-checkpoint-{display_name}"),
            image: checkpoint_ref,
            env: spec.env.into_iter().collect(),
            command: spec.command,
            ports,
            mounts: Vec::new(),
            network_id: None,
            aliases: Vec::new(),
            run_id: String::new(),
            memory_limit_mb: spec.memory_limit_mb,
            keep_alive: false,
            checkpoint_ref: None,
            // `NamedRegistrySpec` deliberately isn't extended with these — see
            // the reduced-spec doc above; placeholder defaults, same as `mounts`
            // and `network_id`.
            disk_limit_mb: None,
            tmpfs_root_mb: None,
            network_disabled: false,
        },
    }
}

/// Reconstructs a [`Checkpoint`] from a registry entry read back off disk — see
/// [`checkpoint_from_reduced`] for the shared construction and its own doc for
/// which fields are real versus placeholder.
fn checkpoint_from_entry(entry: checkpoint::NamedRegistryEntry) -> Checkpoint {
    checkpoint_from_reduced(&entry.name, entry.checkpoint_ref, entry.backend, entry.spec)
}

/// Fails fast with an actionable message unless `path` is absolute — both backend
/// CLIs (`msb copy`, `docker cp`) require a `NAME:/abs/path` shape, and a relative
/// path would resolve against whatever directory happens to be the guest's default
/// working directory, which no caller of this API should have to know or guess.
fn require_absolute_container_path(path: &str) -> Result<()> {
    if path.starts_with('/') {
        Ok(())
    } else {
        Err(RightsizeError::Backend(format!(
            "container path '{path}' must be absolute — both msb copy and docker cp require a \
             NAME:/abs/path shape"
        )))
    }
}

/// The parent directory of an absolute guest path, as a pure string operation (guest
/// paths are always POSIX-shaped, regardless of the host OS this library runs on).
/// `None` only for the root itself (`"/"`, after trimming a trailing slash) — nothing
/// to `mkdir -p` there, since it always exists.
fn container_parent_dir(path: &str) -> Option<&str> {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return None; // path was "/" (or "///...") — no parent to create.
    }
    match trimmed.rfind('/') {
        Some(0) => Some("/"),
        Some(idx) => Some(&trimmed[..idx]),
        None => None, // unreachable once `require_absolute_container_path` passed.
    }
}

/// A host temp file created for [`ContainerGuard::copy_content_to_container`]:
/// private permissions (mode `0600` on unix — best-effort elsewhere), removed on
/// drop regardless of whether the copy that follows succeeds. RAII rather than a
/// bare trailing cleanup call, so a copy failure still cleans up — the same
/// `Drop`-guard discipline this crate's own integration tests use for a reuse
/// sandbox's cleanup (see `rightsize-msb/tests/reuse_it.rs`).
struct TempCopyFile {
    path: std::path::PathBuf,
}

impl TempCopyFile {
    /// Writes `content` to a fresh, uniquely-named file under the host temp
    /// directory and returns a guard that removes it on drop.
    fn create(content: &[u8]) -> Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "rightsize-copy-content-{}-{seq}-{nanos}",
            std::process::id()
        ));
        write_private_temp_file(&path, content)?;
        Ok(Self { path })
    }
}

impl Drop for TempCopyFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Writes `content` to a brand-new file at `path` with permissions restricted to
/// the owner (mode `0600`) on unix — the file may carry arbitrary caller content,
/// so it gets the same private-by-default treatment as any other host temp file
/// this crate writes.
#[cfg(unix)]
fn write_private_temp_file(path: &std::path::Path, content: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(content)
}

/// Non-unix fallback: no portable "create with restricted permissions" primitive in
/// `std` outside unix, so this is a plain create-new write.
#[cfg(not(unix))]
fn write_private_temp_file(path: &std::path::Path, content: &[u8]) -> std::io::Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    use std::io::Write;
    f.write_all(content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait::{WaitStrategy, WaitTarget};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    /// `ContainerGuard` deliberately isn't `Debug` (it holds a `Box<dyn SandboxHandle>`
    /// and friends), so `Result::expect_err`/`unwrap_err` don't work directly on
    /// `Result<ContainerGuard, _>` — this pulls the error out by hand.
    fn expect_start_err(result: Result<ContainerGuard>, msg: &str) -> RightsizeError {
        match result {
            Ok(_) => panic!("{msg}: expected an error, got Ok"),
            Err(e) => e,
        }
    }

    struct FakeHandle {
        id: String,
        spec: ContainerSpec,
    }
    impl SandboxHandle for FakeHandle {
        fn id(&self) -> &str {
            &self.id
        }
        fn spec(&self) -> &ContainerSpec {
            &self.spec
        }
    }

    /// A wait strategy that's immediately ready — the fake backend runs nothing to
    /// actually connect to.
    struct ReadyImmediately;
    #[async_trait::async_trait]
    impl WaitStrategy for ReadyImmediately {
        async fn wait_until_ready(&self, _target: &dyn WaitTarget) -> Result<()> {
            Ok(())
        }
        fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
            self
        }
    }

    /// A wait strategy that always fails — forces the start()-then-teardown cleanup
    /// path. Captures the mapped port it saw (via `probe`) before failing, so a test
    /// can assert on the *exact* port that was released, not just "some" port.
    struct NeverReady {
        precaptured_port: Arc<StdMutex<Option<u16>>>,
    }
    #[async_trait::async_trait]
    impl WaitStrategy for NeverReady {
        async fn wait_until_ready(&self, target: &dyn WaitTarget) -> Result<()> {
            *self.precaptured_port.lock().unwrap() = Some(target.mapped_port(6379));
            Err(RightsizeError::ContainerLaunch("never ready".to_string()))
        }
        fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
            self
        }
    }

    #[derive(Default)]
    struct FakeBackendState {
        created: Vec<ContainerSpec>,
        started: Vec<String>,
        stopped: Vec<String>,
        installed_links: Vec<(String, Vec<crate::backend::NetworkLink>)>,
        /// `(handle id, ref)` for every `create_checkpoint` call this backend
        /// actually received — the checkpoint gating tests' proof that a capability
        /// refusal never reaches the backend at all.
        committed: Vec<(String, String)>,
        /// `(handle id, host path, container path, did host_path exist at call
        /// time)` for every `copy_to_container` call this backend actually
        /// received — the `existed` flag is the temp-file-cleanup test's proof that
        /// the file was really there during the call, not merely gone afterward.
        copied_in: Vec<(String, std::path::PathBuf, String, bool)>,
        /// `(handle id, container path, host path)` for every `copy_from_container`
        /// call this backend actually received.
        copied_out: Vec<(String, String, std::path::PathBuf)>,
        /// Every ref `remove_checkpoint` actually received — the named-checkpoint
        /// replace/remove tests' proof of exactly which ref was torn down.
        removed_checkpoints: Vec<String>,
        /// Every ref `has_checkpoint` actually received — `Checkpoint::find`'s
        /// probe tests' proof that a different-backend entry is never probed.
        probed_checkpoints: Vec<String>,
        /// `(ref, dest path)` for every `export_checkpoint` call this backend
        /// actually received — the archive export tests' proof of exactly what
        /// was asked to be exported, and that nothing is asked for once a
        /// pre-backend-call gate refuses.
        exported_checkpoints: Vec<(String, std::path::PathBuf)>,
        /// `(src file path, ref hint, the bytes actually sitting at that path at
        /// call time)` for every `import_checkpoint` call this backend actually
        /// received — the archive import tests' proof that every validation step
        /// genuinely runs before any backend call, and that the round-tripped
        /// payload bytes really reached the backend. The bytes are captured
        /// eagerly (at call time), since the source path lives in a temp staging
        /// dir that's gone by the time `import_from` returns.
        imported_checkpoints: Vec<(std::path::PathBuf, String, Vec<u8>)>,
    }

    struct FakeBackend {
        state: StdMutex<FakeBackendState>,
        fail_install_network_links: bool,
        fail_ensure_network: bool,
        hardware_isolated: bool,
        checkpoint_capable: bool,
        checkpoint_restarts_workload: bool,
        /// When true, `create_checkpoint` fails outright — the
        /// "checkpoint_named writes no registry entry on backend failure" test's
        /// fixture.
        fail_create_checkpoint: bool,
        /// When true, `export_checkpoint` fails outright, AFTER the staging dir
        /// already exists — the "temp dir cleaned up on failure too" test's
        /// fixture.
        fail_export_checkpoint: bool,
        /// `has_checkpoint`'s canned answer: `Some(true)`/`Some(false)` for a
        /// definite exists/absent, `None` to simulate a probe FAILURE (an `Err`,
        /// never resolved to `Ok(false)` — see `SandboxBackend::has_checkpoint`'s
        /// own "only a definite not-there may return false" contract). Defaults to
        /// `Some(true)`, overridable after construction via
        /// [`FakeBackend::set_probe_result`].
        probe_result: StdMutex<Option<bool>>,
        /// Overrides `import_checkpoint`'s returned effective ref; `None` (the
        /// default) returns `format!("effective:{ref_hint}")` — distinct from
        /// `ref_hint` on purpose, so a test can tell "the effective ref" and "the
        /// ref the archive's manifest recorded" apart even when nothing
        /// deliberately overrides it.
        import_effective_ref: StdMutex<Option<String>>,
        name: &'static str,
    }
    impl FakeBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
                fail_ensure_network: false,
                hardware_isolated: false,
                checkpoint_capable: false,
                checkpoint_restarts_workload: false,
                fail_create_checkpoint: false,
                fail_export_checkpoint: false,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name: "fake",
            })
        }
        fn failing_install_network_links() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: true,
                fail_ensure_network: false,
                hardware_isolated: false,
                checkpoint_capable: false,
                checkpoint_restarts_workload: false,
                fail_create_checkpoint: false,
                fail_export_checkpoint: false,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name: "fake",
            })
        }
        fn failing_ensure_network() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
                fail_ensure_network: true,
                hardware_isolated: false,
                checkpoint_capable: false,
                checkpoint_restarts_workload: false,
                fail_create_checkpoint: false,
                fail_export_checkpoint: false,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name: "fake",
            })
        }
        /// A fake backend that reports `capabilities().hardware_isolated == true` —
        /// the require_isolation happy path's fixture.
        fn hardware_isolated() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
                fail_ensure_network: false,
                hardware_isolated: true,
                checkpoint_capable: false,
                checkpoint_restarts_workload: false,
                fail_create_checkpoint: false,
                fail_export_checkpoint: false,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name: "fake",
            })
        }
        /// A fake backend that reports `capabilities().checkpoint == true` and
        /// `checkpoint_restarts_workload == false` — the docker-shaped checkpoint
        /// happy-path fixture (the container is left undisturbed).
        fn checkpoint_capable() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
                fail_ensure_network: false,
                hardware_isolated: false,
                checkpoint_capable: true,
                checkpoint_restarts_workload: false,
                fail_create_checkpoint: false,
                fail_export_checkpoint: false,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name: "fake",
            })
        }
        /// A fake backend that reports both `capabilities().checkpoint == true` AND
        /// `checkpoint_restarts_workload == true` — the microsandbox-shaped
        /// checkpoint fixture (the stop/snapshot/start cycle restarts the workload,
        /// so `ContainerGuard::checkpoint` must re-run the wait strategy).
        fn checkpoint_capable_restarts_workload() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
                fail_ensure_network: false,
                hardware_isolated: false,
                checkpoint_capable: true,
                checkpoint_restarts_workload: true,
                fail_create_checkpoint: false,
                fail_export_checkpoint: false,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name: "fake",
            })
        }
        /// A fake backend with a caller-chosen name and `checkpoint` capability —
        /// the backend-mismatch test's fixture, which needs two fakes with
        /// DIFFERENT names (the default "fake" is shared by every other
        /// constructor here, so a mismatch test needs its own).
        fn named(name: &'static str) -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
                fail_ensure_network: false,
                hardware_isolated: false,
                checkpoint_capable: true,
                checkpoint_restarts_workload: false,
                fail_create_checkpoint: false,
                fail_export_checkpoint: false,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name,
            })
        }
        /// A `checkpoint`-capable fake whose `create_checkpoint` always fails —
        /// the "a failed checkpoint_named leaves no registry entry" test's fixture.
        fn checkpoint_capable_that_fails_create() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
                fail_ensure_network: false,
                hardware_isolated: false,
                checkpoint_capable: true,
                checkpoint_restarts_workload: false,
                fail_create_checkpoint: true,
                fail_export_checkpoint: false,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name: "fake",
            })
        }
        /// A `checkpoint`-capable fake whose `export_checkpoint` always fails,
        /// AFTER the caller's staging dir already exists — the "export_to cleans
        /// up its staging dir on failure too" test's fixture.
        fn checkpoint_capable_that_fails_export() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
                fail_ensure_network: false,
                hardware_isolated: false,
                checkpoint_capable: true,
                checkpoint_restarts_workload: false,
                fail_create_checkpoint: false,
                fail_export_checkpoint: true,
                probe_result: StdMutex::new(Some(true)),
                import_effective_ref: StdMutex::new(None),
                name: "fake",
            })
        }

        /// Overrides this fake's `has_checkpoint` answer after construction — see
        /// the `probe_result` field's own doc.
        fn set_probe_result(&self, result: Option<bool>) {
            *self.probe_result.lock().unwrap() = result;
        }

        /// Overrides this fake's `import_checkpoint` effective-ref answer after
        /// construction — see the `import_effective_ref` field's own doc.
        fn set_import_effective_ref(&self, effective_ref: impl Into<String>) {
            *self.import_effective_ref.lock().unwrap() = Some(effective_ref.into());
        }
    }
    #[async_trait::async_trait]
    impl SandboxBackend for FakeBackend {
        fn name(&self) -> &str {
            self.name
        }
        fn supports_native_networks(&self) -> bool {
            false
        }
        fn capabilities(&self) -> crate::backend::Capabilities {
            crate::backend::Capabilities {
                hardware_isolated: self.hardware_isolated,
                checkpoint: self.checkpoint_capable,
                checkpoint_restarts_workload: self.checkpoint_restarts_workload,
            }
        }
        async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
            self.state.lock().unwrap().created.push(spec.clone());
            Ok(Box::new(FakeHandle {
                id: spec.name.clone(),
                spec,
            }))
        }
        async fn start(&self, handle: &dyn SandboxHandle) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .started
                .push(handle.id().to_string());
            Ok(())
        }
        async fn stop(&self, handle: &dyn SandboxHandle) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .stopped
                .push(handle.id().to_string());
            Ok(())
        }
        async fn remove(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn exec(&self, _handle: &dyn SandboxHandle, cmd: &[String]) -> Result<ExecResult> {
            Ok(ExecResult {
                exit_code: 0,
                stdout: cmd.join(" "),
                stderr: String::new(),
            })
        }
        async fn logs(&self, _handle: &dyn SandboxHandle) -> Result<String> {
            Ok(String::new())
        }
        async fn follow_logs(
            &self,
            _handle: &dyn SandboxHandle,
            _consumer: Box<dyn Fn(String) + Send + Sync>,
        ) -> Result<crate::backend::FollowHandle> {
            unimplemented!("not exercised by this test suite")
        }
        async fn ensure_network(&self, _network_id: &str) -> Result<()> {
            if self.fail_ensure_network {
                return Err(RightsizeError::Backend("ensure_network failed".to_string()));
            }
            Ok(())
        }
        async fn remove_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        async fn install_network_links(
            &self,
            handle: &dyn SandboxHandle,
            links: &[crate::backend::NetworkLink],
        ) -> Result<()> {
            if self.fail_install_network_links {
                return Err(RightsizeError::unsupported_with_remedy(
                    "network links (no nc in image; try docker)",
                    "fake",
                    "run with a different backend",
                ));
            }
            if !links.is_empty() {
                self.state
                    .lock()
                    .unwrap()
                    .installed_links
                    .push((handle.id().to_string(), links.to_vec()));
            }
            Ok(())
        }
        fn cleanup_sync(&self, _container_id: &str) {}
        fn remove_by_name(&self, _name: &str) {}
        fn watchdog_kill_command(&self) -> Vec<String> {
            vec!["true".to_string()]
        }
        async fn create_checkpoint(
            &self,
            handle: &dyn SandboxHandle,
            nonce: &str,
        ) -> Result<String> {
            if self.fail_create_checkpoint {
                return Err(RightsizeError::Backend(
                    "create_checkpoint failed".to_string(),
                ));
            }
            let checkpoint_ref = format!("fake-checkpoint:{nonce}");
            self.state
                .lock()
                .unwrap()
                .committed
                .push((handle.id().to_string(), checkpoint_ref.clone()));
            Ok(checkpoint_ref)
        }
        async fn remove_checkpoint(&self, checkpoint_ref: &str) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .removed_checkpoints
                .push(checkpoint_ref.to_string());
            Ok(())
        }
        async fn has_checkpoint(&self, checkpoint_ref: &str) -> Result<bool> {
            self.state
                .lock()
                .unwrap()
                .probed_checkpoints
                .push(checkpoint_ref.to_string());
            match *self.probe_result.lock().unwrap() {
                Some(exists) => Ok(exists),
                None => Err(RightsizeError::Backend(
                    "has_checkpoint probe failed".to_string(),
                )),
            }
        }
        /// Writes a recognizable payload (the ref itself, so a test can assert
        /// the round-tripped archive really carried this call's own bytes) to
        /// `dest`, and records the call.
        async fn export_checkpoint(
            &self,
            checkpoint_ref: &str,
            dest: &std::path::Path,
        ) -> Result<()> {
            if self.fail_export_checkpoint {
                return Err(RightsizeError::Backend(
                    "export_checkpoint failed".to_string(),
                ));
            }
            std::fs::write(dest, format!("fake-artifact-payload:{checkpoint_ref}"))?;
            self.state
                .lock()
                .unwrap()
                .exported_checkpoints
                .push((checkpoint_ref.to_string(), dest.to_path_buf()));
            Ok(())
        }
        /// Records the call and returns the canned effective ref — see the
        /// `import_effective_ref` field's own doc.
        async fn import_checkpoint(
            &self,
            src_file: &std::path::Path,
            ref_hint: &str,
        ) -> Result<String> {
            let bytes = std::fs::read(src_file).unwrap_or_default();
            self.state.lock().unwrap().imported_checkpoints.push((
                src_file.to_path_buf(),
                ref_hint.to_string(),
                bytes,
            ));
            Ok(self
                .import_effective_ref
                .lock()
                .unwrap()
                .clone()
                .unwrap_or_else(|| format!("effective:{ref_hint}")))
        }
        async fn copy_to_container(
            &self,
            handle: &dyn SandboxHandle,
            host_path: &std::path::Path,
            container_path: &str,
        ) -> Result<()> {
            let existed = host_path.exists();
            self.state.lock().unwrap().copied_in.push((
                handle.id().to_string(),
                host_path.to_path_buf(),
                container_path.to_string(),
                existed,
            ));
            Ok(())
        }
        async fn copy_from_container(
            &self,
            handle: &dyn SandboxHandle,
            container_path: &str,
            host_path: &std::path::Path,
        ) -> Result<()> {
            self.state.lock().unwrap().copied_out.push((
                handle.id().to_string(),
                container_path.to_string(),
                host_path.to_path_buf(),
            ));
            Ok(())
        }
    }

    fn container_on(backend: &Arc<FakeBackend>) -> Container {
        Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(ReadyImmediately)
    }

    // U1: port allocate/create/wait/map.
    #[tokio::test]
    async fn u1_start_allocates_ports_creates_spec_waits_and_maps_ports() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_env("A", "1");
        let guard = c.start().await.expect("start must succeed");

        let spec = {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.created.len(), 1);
            state.created[0].clone()
        };
        assert_eq!(spec.image, "redis:8.6-alpine");
        assert_eq!(spec.env, vec![("A".to_string(), "1".to_string())]);
        assert_eq!(spec.ports.len(), 1);
        assert_eq!(spec.ports[0].guest_port, 6379);
        assert!(spec.ports[0].host_port > 0);
        assert_eq!(
            guard.get_mapped_port(6379).unwrap(),
            spec.ports[0].host_port
        );
        assert!(guard.is_running());

        guard.stop().await.unwrap();
    }

    // require_isolation: a non-isolated backend refuses to start, before any
    // create/network work.
    #[tokio::test]
    async fn require_isolation_on_a_non_isolated_backend_errors_before_any_create() {
        let backend = FakeBackend::new(); // capabilities().hardware_isolated == false
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .require_isolation(true);

        let err = expect_start_err(c.start().await, "require_isolation must refuse to start");
        assert!(
            matches!(err, RightsizeError::IsolationRequired { .. }),
            "{err}"
        );
        let msg = err.to_string();
        assert!(msg.contains("fake"), "{msg}");
        assert!(msg.contains("RIGHTSIZE_BACKEND=microsandbox"), "{msg}");
        assert!(
            backend.state.lock().unwrap().created.is_empty(),
            "no sandbox may be created when isolation is required and unavailable"
        );
    }

    // require_isolation: a hardware-isolated backend starts normally.
    #[tokio::test]
    async fn require_isolation_on_a_hardware_isolated_backend_starts_normally() {
        let backend = FakeBackend::hardware_isolated();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .require_isolation(true);

        let guard = c.start().await.expect("start must succeed");
        assert!(guard.is_running());
        assert_eq!(backend.state.lock().unwrap().created.len(), 1);

        guard.stop().await.unwrap();
    }

    // require_isolation(false) (the default) never consults capabilities — a
    // non-isolated backend is fine.
    #[tokio::test]
    async fn require_isolation_defaults_to_false_and_does_not_gate_a_normal_start() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.expect("start must succeed");
        guard.stop().await.unwrap();
    }

    // =========================== checkpoint / restore ==============================

    // Capability gating: a backend with checkpoint == false refuses before any
    // backend call — the typed error, and `create_checkpoint` is never invoked.
    #[tokio::test]
    async fn checkpoint_refuses_before_any_backend_call_when_capability_is_false() {
        let backend = FakeBackend::new(); // capabilities().checkpoint == false
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.expect("start must succeed");

        let err = guard
            .checkpoint()
            .await
            .expect_err("checkpoint must refuse on a non-checkpoint-capable backend");
        assert!(
            matches!(err, RightsizeError::CheckpointUnsupported { .. }),
            "{err}"
        );
        let msg = err.to_string();
        assert!(msg.contains("fake"), "{msg}");
        assert!(msg.contains("capabilities().checkpoint"), "{msg}");
        assert!(
            backend.state.lock().unwrap().committed.is_empty(),
            "create_checkpoint must never be called once capability gating refuses"
        );

        guard.stop().await.unwrap();
    }

    // A non-running container: state error, same shape as exec/logs.
    #[tokio::test]
    async fn checkpoint_on_a_non_running_container_is_a_state_error() {
        let backend = FakeBackend::checkpoint_capable();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let mut guard = c.start().await.unwrap();
        guard.stop_inner().await; // stops it in place, without consuming the guard.

        let err = guard
            .checkpoint()
            .await
            .expect_err("checkpoint must refuse on a stopped guard");
        assert!(err.to_string().contains("not running"), "{err}");
        assert!(
            backend.state.lock().unwrap().committed.is_empty(),
            "create_checkpoint must never be called on a non-running guard"
        );
    }

    // The returned Checkpoint carries a well-formed backend-specific ref, the
    // creating backend's name, and the full source spec — env, command, exposed
    // ports, memory limit and all.
    #[tokio::test]
    async fn checkpoint_returns_the_ref_backend_and_the_full_source_spec() {
        let backend = FakeBackend::checkpoint_capable();
        let c = container_on(&backend)
            .with_env("A", "1")
            .with_exposed_ports(&[6379])
            .with_command(&["redis-server"])
            .with_memory_limit(256);
        let guard = c.start().await.unwrap();

        let cp = guard.checkpoint().await.expect("checkpoint must succeed");

        let tag = cp
            .checkpoint_ref
            .strip_prefix("fake-checkpoint:")
            .unwrap_or_else(|| {
                panic!(
                    "expected the fake-checkpoint: prefix, got {}",
                    cp.checkpoint_ref
                )
            });
        assert_eq!(tag.len(), 12, "{}", cp.checkpoint_ref);
        assert!(
            tag.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "{}",
            cp.checkpoint_ref
        );
        assert_eq!(cp.backend, "fake");

        assert_eq!(cp.spec.env, vec![("A".to_string(), "1".to_string())]);
        assert_eq!(cp.spec.command, Some(vec!["redis-server".to_string()]));
        assert_eq!(cp.spec.ports.len(), 1);
        assert_eq!(cp.spec.ports[0].guest_port, 6379);
        assert_eq!(cp.spec.memory_limit_mb, Some(256));

        let committed = backend.state.lock().unwrap().committed.clone();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0, guard.name());
        assert_eq!(committed[0].1, cp.checkpoint_ref);

        guard.stop().await.unwrap();
    }

    // Backend-mismatch: restoring a checkpoint under a different active backend
    // than the one that created it refuses before any backend call.
    #[tokio::test]
    async fn from_checkpoint_under_a_different_backend_refuses_before_any_backend_call() {
        let source_backend = FakeBackend::named("fake-a");
        let source = container_on(&source_backend).with_exposed_ports(&[6379]);
        let source_guard = source.start().await.unwrap();
        let cp = source_guard.checkpoint().await.unwrap();
        source_guard.stop().await.unwrap();
        assert_eq!(cp.backend, "fake-a");

        let restore_backend = FakeBackend::named("fake-b");
        let restored = Container::from_checkpoint(&cp).with_backend(restore_backend.clone());

        let err = expect_start_err(
            restored.start().await,
            "restoring under a different backend than the creator must refuse",
        );
        assert!(
            matches!(err, RightsizeError::CheckpointBackendMismatch { .. }),
            "{err}"
        );
        let msg = err.to_string();
        assert!(msg.contains("fake-a"), "{msg}");
        assert!(msg.contains("fake-b"), "{msg}");
        assert!(msg.contains("RIGHTSIZE_BACKEND=fake-a"), "{msg}");
        assert!(
            restore_backend.state.lock().unwrap().created.is_empty(),
            "no backend call may happen once the mismatch is detected"
        );
    }

    // reuse + from_checkpoint: a config error, before any backend work.
    #[tokio::test]
    async fn reuse_combined_with_from_checkpoint_is_a_config_error() {
        let source_backend = FakeBackend::checkpoint_capable();
        let source = container_on(&source_backend).with_exposed_ports(&[6379]);
        let source_guard = source.start().await.unwrap();
        let cp = source_guard.checkpoint().await.unwrap();
        source_guard.stop().await.unwrap();

        let restore_backend = FakeBackend::checkpoint_capable();
        let restored = Container::from_checkpoint(&cp)
            .with_backend(restore_backend.clone())
            .with_reuse_env_override(true)
            .reuse(true);

        let err = expect_start_err(
            restored.start().await,
            "reuse combined with from_checkpoint must refuse before any backend work",
        );
        assert!(
            matches!(err, RightsizeError::ReuseCheckpointConflict),
            "{err}"
        );
        assert!(
            restore_backend.state.lock().unwrap().created.is_empty(),
            "no backend call may happen once the conflict is detected"
        );
    }

    /// A wait strategy that counts every call — the checkpoint-rewait tests' proof
    /// of whether (and how many times) the wait strategy actually ran.
    struct CountingWait {
        calls: Arc<StdMutex<usize>>,
    }
    #[async_trait::async_trait]
    impl WaitStrategy for CountingWait {
        async fn wait_until_ready(&self, _target: &dyn WaitTarget) -> Result<()> {
            *self.calls.lock().unwrap() += 1;
            Ok(())
        }
        fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
            self
        }
    }

    // Post-checkpoint rewait: a backend whose checkpoint mechanism restarts the
    // workload gets its wait strategy re-run before checkpoint() returns.
    #[tokio::test]
    async fn checkpoint_reruns_the_wait_strategy_when_the_backend_restarts_the_workload() {
        let calls = Arc::new(StdMutex::new(0usize));
        let backend = FakeBackend::checkpoint_capable_restarts_workload();
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .with_exposed_ports(&[6379])
            .waiting_for(CountingWait {
                calls: calls.clone(),
            });
        let guard = c.start().await.unwrap();
        assert_eq!(*calls.lock().unwrap(), 1, "start() runs the wait once");

        guard.checkpoint().await.expect("checkpoint must succeed");
        assert_eq!(
            *calls.lock().unwrap(),
            2,
            "a backend whose checkpoint restarts the workload must re-run the wait strategy \
             before checkpoint() returns"
        );

        guard.stop().await.unwrap();
    }

    // Post-checkpoint rewait, negative case: a backend whose checkpoint leaves the
    // container undisturbed (docker's image commit) must NOT re-run the wait.
    #[tokio::test]
    async fn checkpoint_does_not_rerun_the_wait_strategy_when_the_container_is_left_undisturbed() {
        let calls = Arc::new(StdMutex::new(0usize));
        let backend = FakeBackend::checkpoint_capable(); // checkpoint_restarts_workload == false
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .with_exposed_ports(&[6379])
            .waiting_for(CountingWait {
                calls: calls.clone(),
            });
        let guard = c.start().await.unwrap();
        assert_eq!(*calls.lock().unwrap(), 1);

        guard.checkpoint().await.expect("checkpoint must succeed");
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "a backend whose checkpoint leaves the container running must not re-run the wait"
        );

        guard.stop().await.unwrap();
    }

    // Post-checkpoint network re-link: a backend whose checkpoint mechanism
    // restarts the workload (killing msb's emulated exec-tunnel links along with
    // everything else in the guest) gets its network links re-installed, a second
    // time with the same links, before checkpoint() returns.
    #[tokio::test]
    async fn checkpoint_reinstalls_network_links_when_the_backend_restarts_the_workload() {
        let backend = FakeBackend::checkpoint_capable_restarts_workload();
        let net = Arc::new(Network::new_network());
        let stub = container_on(&backend)
            .with_exposed_ports(&[8888])
            .with_network(&net)
            .with_network_aliases(&["configuration-stub"]);
        let stub_guard = stub.start().await.unwrap();

        let app = container_on(&backend)
            .with_exposed_ports(&[8080])
            .with_network(&net)
            .waiting_for(ReadyImmediately);
        let app_guard = app.start().await.unwrap();

        let links_at_start = {
            let state = backend.state.lock().unwrap();
            assert_eq!(
                state.installed_links.len(),
                1,
                "start() installs the app's link to the already-running stub once"
            );
            state.installed_links[0].1.clone()
        };

        app_guard
            .checkpoint()
            .await
            .expect("checkpoint must succeed");

        {
            let state = backend.state.lock().unwrap();
            assert_eq!(
                state.installed_links.len(),
                2,
                "a backend whose checkpoint restarts the workload must re-install the network \
                 links a second time before checkpoint() returns: {:?}",
                state.installed_links
            );
            assert_eq!(
                state.installed_links[1].1, links_at_start,
                "the re-installed links must be the exact same links installed at start()"
            );
        }

        app_guard.stop().await.unwrap();
        stub_guard.stop().await.unwrap();
    }

    // Post-checkpoint network re-link, negative case: a backend whose checkpoint
    // leaves the container undisturbed (docker's image commit) must NOT re-install
    // network links — the tunnel/alias state was never disturbed.
    #[tokio::test]
    async fn checkpoint_does_not_reinstall_network_links_when_the_container_is_left_undisturbed() {
        let backend = FakeBackend::checkpoint_capable(); // checkpoint_restarts_workload == false
        let net = Arc::new(Network::new_network());
        let stub = container_on(&backend)
            .with_exposed_ports(&[8888])
            .with_network(&net)
            .with_network_aliases(&["configuration-stub"]);
        let stub_guard = stub.start().await.unwrap();

        let app = container_on(&backend)
            .with_exposed_ports(&[8080])
            .with_network(&net)
            .waiting_for(ReadyImmediately);
        let app_guard = app.start().await.unwrap();
        assert_eq!(backend.state.lock().unwrap().installed_links.len(), 1);

        app_guard
            .checkpoint()
            .await
            .expect("checkpoint must succeed");
        assert_eq!(
            backend.state.lock().unwrap().installed_links.len(),
            1,
            "a backend whose checkpoint leaves the container running must not re-install \
             network links"
        );

        app_guard.stop().await.unwrap();
        stub_guard.stop().await.unwrap();
    }

    // `Container::from_checkpoint` applies the checkpoint's image/env/command/
    // exposed-ports/memory-limit as defaults, and an ordinary builder call after it
    // still overrides (command, here — with_command *replaces* rather than appends).
    #[tokio::test]
    async fn from_checkpoint_applies_the_spec_defaults_and_allows_overrides() {
        let source_backend = FakeBackend::checkpoint_capable();
        let source = container_on(&source_backend)
            .with_env("A", "1")
            .with_exposed_ports(&[6379])
            .with_command(&["redis-server"])
            .with_memory_limit(256);
        let source_guard = source.start().await.unwrap();
        let cp = source_guard.checkpoint().await.unwrap();
        source_guard.stop().await.unwrap();

        // Defaults applied, no override.
        let restore_backend = FakeBackend::new();
        let restored = Container::from_checkpoint(&cp)
            .with_backend(restore_backend.clone())
            .waiting_for(ReadyImmediately);
        let restored_guard = restored.start().await.expect("restore must start");
        let created = restore_backend.state.lock().unwrap().created[0].clone();
        assert_eq!(created.image, cp.checkpoint_ref);
        assert_eq!(created.checkpoint_ref, Some(cp.checkpoint_ref.clone()));
        assert_eq!(created.env, vec![("A".to_string(), "1".to_string())]);
        assert_eq!(created.command, Some(vec!["redis-server".to_string()]));
        assert_eq!(created.ports.len(), 1);
        assert_eq!(created.ports[0].guest_port, 6379);
        assert_eq!(created.memory_limit_mb, Some(256));
        restored_guard.stop().await.unwrap();

        // Override: a caller's own `.with_command(...)` after `from_checkpoint`
        // replaces the checkpoint spec's command rather than being ignored.
        let override_backend = FakeBackend::new();
        let overridden = Container::from_checkpoint(&cp)
            .with_backend(override_backend.clone())
            .waiting_for(ReadyImmediately)
            .with_command(&["redis-server", "--appendonly", "yes"]);
        let overridden_guard = overridden.start().await.expect("restore must start");
        let created = override_backend.state.lock().unwrap().created[0].clone();
        assert_eq!(
            created.command,
            Some(vec![
                "redis-server".to_string(),
                "--appendonly".to_string(),
                "yes".to_string()
            ])
        );
        overridden_guard.stop().await.unwrap();
    }

    // =========================== named checkpoints ==============================

    fn sample_named_entry(
        name: &str,
        checkpoint_ref: &str,
        backend: &str,
    ) -> checkpoint::NamedRegistryEntry {
        checkpoint::NamedRegistryEntry {
            name: name.to_string(),
            checkpoint_ref: checkpoint_ref.to_string(),
            backend: backend.to_string(),
            created_iso: "2025-01-01T00:00:00Z".to_string(),
            spec: checkpoint::NamedRegistrySpec {
                env: std::collections::BTreeMap::from([("A".to_string(), "1".to_string())]),
                command: Some(vec!["redis-server".to_string()]),
                exposed_ports: vec![6379],
                memory_limit_mb: Some(256),
            },
        }
    }

    // Name validation refuses before any backend call.
    #[tokio::test]
    async fn checkpoint_named_rejects_an_invalid_name_before_any_backend_call() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("checkpoint-named-invalid");
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_checkpoint_cache_dir_override(cache_dir);
        let guard = c.start().await.unwrap();

        let err = guard
            .checkpoint_named("Bad Name!")
            .await
            .expect_err("an invalid name must be rejected");
        assert!(
            matches!(err, RightsizeError::InvalidCheckpointName { .. }),
            "{err}"
        );
        assert!(
            backend.state.lock().unwrap().committed.is_empty(),
            "create_checkpoint must never be called once name validation refuses"
        );

        guard.stop().await.unwrap();
    }

    // Writes the pinned registry JSON (exact field names) only after the backend
    // checkpoint has succeeded.
    #[tokio::test]
    async fn checkpoint_named_writes_the_pinned_registry_json_after_backend_success() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("checkpoint-named-write");
        let c = container_on(&backend)
            .with_env("A", "1")
            .with_exposed_ports(&[6379])
            .with_command(&["redis-server"])
            .with_memory_limit(256)
            .with_checkpoint_cache_dir_override(cache_dir.clone());
        let guard = c.start().await.unwrap();

        let cp = guard
            .checkpoint_named("seeded-db")
            .await
            .expect("checkpoint_named must succeed");
        assert_eq!(cp.checkpoint_ref, "fake-checkpoint:seeded-db");
        assert_eq!(cp.backend, "fake");

        let raw = std::fs::read_to_string(cache_dir.join("checkpoints").join("seeded-db.json"))
            .expect("the registry file must exist");
        for pinned in [
            "\"name\": \"seeded-db\"",
            "\"ref\": \"fake-checkpoint:seeded-db\"",
            "\"backend\": \"fake\"",
            "\"createdIso\"",
            "\"spec\"",
            "\"env\"",
            "\"command\"",
            "\"exposedPorts\"",
            "\"memoryLimitMb\": 256",
        ] {
            assert!(raw.contains(pinned), "{pinned} missing from {raw}");
        }

        guard.stop().await.unwrap();
    }

    // A failed backend checkpoint leaves no registry entry behind.
    #[tokio::test]
    async fn checkpoint_named_writes_no_registry_entry_when_the_backend_call_fails() {
        let backend = FakeBackend::checkpoint_capable_that_fails_create();
        let cache_dir = temp_cache_dir("checkpoint-named-fail");
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_checkpoint_cache_dir_override(cache_dir.clone());
        let guard = c.start().await.unwrap();

        guard
            .checkpoint_named("seeded-db")
            .await
            .expect_err("a failing backend checkpoint must propagate");

        assert!(
            !cache_dir
                .join("checkpoints")
                .join("seeded-db.json")
                .exists(),
            "no registry entry may be written when the backend call fails"
        );

        guard.stop().await.unwrap();
    }

    // Replace semantics: a second checkpoint_named under the same name removes the
    // old ref first and leaves the registry pointing at the new one.
    #[tokio::test]
    async fn checkpoint_named_replaces_an_existing_name_removing_the_old_ref_first() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("checkpoint-named-replace");
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_checkpoint_cache_dir_override(cache_dir.clone());
        let guard = c.start().await.unwrap();

        let first = guard.checkpoint_named("seeded-db").await.unwrap();
        assert_eq!(first.checkpoint_ref, "fake-checkpoint:seeded-db");

        let second = guard.checkpoint_named("seeded-db").await.unwrap();
        assert_eq!(second.checkpoint_ref, "fake-checkpoint:seeded-db");

        assert_eq!(
            backend.state.lock().unwrap().removed_checkpoints,
            vec![first.checkpoint_ref.clone()],
            "re-checkpointing an existing name must best-effort remove the OLD ref first"
        );

        let entry = checkpoint::Registry::new(&cache_dir, "seeded-db")
            .read()
            .expect("the registry entry must still exist");
        assert_eq!(entry.checkpoint_ref, second.checkpoint_ref);

        guard.stop().await.unwrap();
    }

    // Replace semantics, cross-backend entry: when the existing registry entry
    // belongs to a DIFFERENT backend than the one now active, checkpoint_named must
    // never call remove_checkpoint on it (the active backend has no business
    // operating on a ref format it didn't create) — it just proceeds with the new
    // capture and overwrites the registry entry.
    #[tokio::test]
    async fn checkpoint_named_replace_never_removes_a_different_backend_entry() {
        let backend = FakeBackend::checkpoint_capable(); // name == "fake"
        let cache_dir = temp_cache_dir("checkpoint-named-replace-cross-backend");
        checkpoint::Registry::new(&cache_dir, "seeded-db")
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "other-backend-ref",
                "fake-other",
            ))
            .unwrap();

        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_checkpoint_cache_dir_override(cache_dir.clone());
        let guard = c.start().await.unwrap();

        let replaced = guard.checkpoint_named("seeded-db").await.unwrap();
        assert_eq!(replaced.checkpoint_ref, "fake-checkpoint:seeded-db");

        assert!(
            backend.state.lock().unwrap().removed_checkpoints.is_empty(),
            "a different-backend entry's ref must never reach remove_checkpoint"
        );

        let entry = checkpoint::Registry::new(&cache_dir, "seeded-db")
            .read()
            .expect("the registry entry must be overwritten with the new one");
        assert_eq!(entry.checkpoint_ref, replaced.checkpoint_ref);
        assert_eq!(entry.backend, "fake");

        guard.stop().await.unwrap();
    }

    // Data-loss guard: a tmpfs-root container on the microsandbox backend must be
    // refused BEFORE checkpoint_named's own replace-removal step ever runs — a
    // refusal that only fired once the backend's own create_checkpoint was
    // reached would already have best-effort destroyed the previous same-name
    // checkpoint for nothing (see `ContainerGuard::ensure_checkpoint_target_survives_a_stop`).
    #[tokio::test]
    async fn checkpoint_named_refuses_a_tmpfs_root_microsandbox_container_before_removing_the_previous_entry()
     {
        let backend = FakeBackend::named("microsandbox");
        let cache_dir = temp_cache_dir("checkpoint-named-tmpfs-root-refuses");
        checkpoint::Registry::new(&cache_dir, "seeded-db")
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "previous-ref",
                "microsandbox",
            ))
            .unwrap();

        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_tmpfs_root(256)
            .with_checkpoint_cache_dir_override(cache_dir.clone());
        let guard = c.start().await.unwrap();

        let err = guard
            .checkpoint_named("seeded-db")
            .await
            .expect_err("a tmpfs-root container on microsandbox must be refused");
        assert!(matches!(err, RightsizeError::TmpfsRootCheckpoint), "{err}");

        assert!(
            backend.state.lock().unwrap().removed_checkpoints.is_empty(),
            "the previous same-name checkpoint must never be removed once this gate refuses"
        );
        assert!(
            backend.state.lock().unwrap().committed.is_empty(),
            "create_checkpoint must never be reached either"
        );

        let entry = checkpoint::Registry::new(&cache_dir, "seeded-db")
            .read()
            .expect("the previous registry entry must survive the refused re-checkpoint");
        assert_eq!(entry.checkpoint_ref, "previous-ref");

        guard.stop().await.unwrap();
    }

    // The same gate applies to the unnamed checkpoint() entry point, before ANY
    // backend call — mirrors the capability-gate tests' own "never reaches the
    // backend" proof.
    #[tokio::test]
    async fn checkpoint_refuses_a_tmpfs_root_microsandbox_container_before_any_backend_call() {
        let backend = FakeBackend::named("microsandbox");
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_tmpfs_root(256);
        let guard = c.start().await.unwrap();

        let err = guard
            .checkpoint()
            .await
            .expect_err("a tmpfs-root container on microsandbox must be refused");
        assert!(matches!(err, RightsizeError::TmpfsRootCheckpoint), "{err}");
        assert!(
            backend.state.lock().unwrap().committed.is_empty(),
            "create_checkpoint must never be reached once this gate refuses"
        );

        guard.stop().await.unwrap();
    }

    // ---- override seam: microsandbox_checkpoint_ref/mint_microsandbox_checkpoint_ref ----

    #[test]
    fn mint_microsandbox_checkpoint_ref_uses_the_override_cache_dir_when_given_one() {
        let cache_dir = temp_cache_dir("mint-microsandbox-ref-override");
        let ref_str = mint_microsandbox_checkpoint_ref(Some(&cache_dir), "seeded-db")
            .expect("an absolute override must mint without error");
        assert_eq!(
            ref_str,
            cache_dir
                .join("checkpoints")
                .join("rz-ckpt-seeded-db")
                .display()
                .to_string()
        );
    }

    #[test]
    fn mint_microsandbox_checkpoint_ref_absolutizes_a_relative_cache_dir_override() {
        // A relative RIGHTSIZE_CACHE_DIR (or an override a caller set to a
        // relative path) must still yield an ABSOLUTE ref — msb's own
        // `--dest-dir` flag requires one.
        let relative = std::path::PathBuf::from("rz-relative-cache-dir-for-this-test-only");
        let ref_str = mint_microsandbox_checkpoint_ref(Some(&relative), "seeded-db")
            .expect("a relative override must still be absolutized, not rejected");
        let ref_path = std::path::Path::new(&ref_str);
        assert!(ref_path.is_absolute(), "{ref_str}");
        assert!(
            ref_str.ends_with(
                &relative
                    .join("checkpoints")
                    .join("rz-ckpt-seeded-db")
                    .display()
                    .to_string()
            ),
            "{ref_str}"
        );
    }

    // The override seam's whole point: checkpoint_named's own registry write and
    // the microsandbox backend ref this mints both resolve the SAME effective
    // cache dir, so a test-isolated (or otherwise overridden) cache dir never
    // leaks the backend-native artifact into the real one while the registry
    // entry stays correctly isolated — the bug this closes had
    // `MsbCliBackend::create_checkpoint` minting its own ref straight from
    // `rightsize::cache_dir::dir()`, unconditionally, ignoring this override
    // entirely.
    #[tokio::test]
    async fn checkpoint_named_on_microsandbox_mints_the_backend_ref_under_the_same_cache_dir_the_registry_uses()
     {
        let backend = FakeBackend::named("microsandbox");
        let cache_dir = temp_cache_dir("checkpoint-named-microsandbox-override-seam");
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_checkpoint_cache_dir_override(cache_dir.clone());
        let guard = c.start().await.unwrap();

        let cp = guard.checkpoint_named("seeded-db").await.unwrap();

        let expected_artifact_ref = cache_dir
            .join("checkpoints")
            .join("rz-ckpt-seeded-db")
            .display()
            .to_string();
        // FakeBackend::create_checkpoint formats `fake-checkpoint:<whatever it
        // was given>` — the prefix proves it went through the backend at all,
        // and the suffix proves core minted the full absolute ref under the
        // override before ever calling it.
        assert_eq!(
            cp.checkpoint_ref,
            format!("fake-checkpoint:{expected_artifact_ref}")
        );

        // The artifact ref's own directory and the registry file's directory
        // are the SAME directory under the override.
        let artifact_dir = std::path::Path::new(&expected_artifact_ref)
            .parent()
            .unwrap()
            .to_path_buf();
        let registry_dir = cache_dir.join("checkpoints");
        assert_eq!(artifact_dir, registry_dir);
        assert!(registry_dir.join("seeded-db.json").exists());

        guard.stop().await.unwrap();
    }

    // Checkpoint::find (via its parameterized inner function): no entry at all.
    #[tokio::test]
    async fn find_named_returns_none_when_no_entry_exists() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("find-named-absent");
        let found = find_named(
            "seeded-db",
            &(backend as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .unwrap();
        assert!(found.is_none());
    }

    // find: an entry whose artifact still exists (per the probe) is returned, and
    // the probe actually ran against the recorded ref.
    #[tokio::test]
    async fn find_named_returns_the_checkpoint_when_the_artifact_still_exists() {
        let backend = FakeBackend::checkpoint_capable(); // probe_result defaults to Some(true)
        let cache_dir = temp_cache_dir("find-named-present");
        checkpoint::Registry::new(&cache_dir, "seeded-db")
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "fake-checkpoint:abc",
                "fake",
            ))
            .unwrap();

        let found = find_named(
            "seeded-db",
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .unwrap()
        .expect("an entry whose artifact exists must be returned");
        assert_eq!(found.checkpoint_ref, "fake-checkpoint:abc");
        assert_eq!(found.backend, "fake");
        assert_eq!(found.spec.command, Some(vec!["redis-server".to_string()]));
        assert_eq!(found.spec.ports[0].guest_port, 6379);
        assert_eq!(found.spec.memory_limit_mb, Some(256));
        assert_eq!(
            backend.state.lock().unwrap().probed_checkpoints,
            vec!["fake-checkpoint:abc".to_string()]
        );
    }

    // find: a gone artifact is a stale entry — deleted, and treated as absent.
    #[tokio::test]
    async fn find_named_treats_a_gone_artifact_as_stale_and_deletes_the_entry() {
        let backend = FakeBackend::checkpoint_capable();
        backend.set_probe_result(Some(false)); // "definitely gone"
        let cache_dir = temp_cache_dir("find-named-stale");
        let registry = checkpoint::Registry::new(&cache_dir, "seeded-db");
        registry
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "fake-checkpoint:gone",
                "fake",
            ))
            .unwrap();

        let found = find_named(
            "seeded-db",
            &(backend as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .unwrap();
        assert!(found.is_none());
        assert!(
            !registry.exists(),
            "a stale entry (artifact confirmed gone) must be deleted"
        );
    }

    // find: corrupt JSON is treated exactly like "no entry" — never probed.
    #[tokio::test]
    async fn find_named_treats_corrupt_json_as_absent_without_probing() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("find-named-corrupt");
        std::fs::create_dir_all(cache_dir.join("checkpoints")).unwrap();
        std::fs::write(
            cache_dir.join("checkpoints").join("seeded-db.json"),
            b"not json",
        )
        .unwrap();

        let found = find_named(
            "seeded-db",
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .unwrap();
        assert!(found.is_none());
        assert!(
            backend.state.lock().unwrap().probed_checkpoints.is_empty(),
            "a corrupt entry must never reach the probe"
        );
    }

    // find: an entry for a DIFFERENT backend is returned WITHOUT probing.
    #[tokio::test]
    async fn find_named_returns_a_different_backend_entry_unprobed() {
        let backend = FakeBackend::named("fake-active");
        let cache_dir = temp_cache_dir("find-named-other-backend");
        checkpoint::Registry::new(&cache_dir, "seeded-db")
            .write_atomic(&sample_named_entry("seeded-db", "other-ref", "fake-other"))
            .unwrap();

        let found = find_named(
            "seeded-db",
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .unwrap()
        .expect("a different-backend entry must still be returned");
        assert_eq!(found.backend, "fake-other");
        assert!(
            backend.state.lock().unwrap().probed_checkpoints.is_empty(),
            "a different-backend entry must never be probed via the active backend"
        );
    }

    // find: a probe failure propagates — never resolves to "absent".
    #[tokio::test]
    async fn find_named_propagates_a_probe_failure_as_an_error() {
        let backend = FakeBackend::checkpoint_capable();
        backend.set_probe_result(None); // simulate a probe error
        let cache_dir = temp_cache_dir("find-named-probe-error");
        checkpoint::Registry::new(&cache_dir, "seeded-db")
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "fake-checkpoint:x",
                "fake",
            ))
            .unwrap();

        let err = find_named(
            "seeded-db",
            &(backend as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .expect_err("a probe failure must surface, never resolve to Ok(None)");
        assert!(matches!(err, RightsizeError::Backend(_)), "{err}");
    }

    // list: corrupt entries are skipped; a missing directory is an empty list.
    #[tokio::test]
    async fn list_named_skips_corrupt_entries() {
        let cache_dir = temp_cache_dir("list-named-mixed");
        assert!(list_named(&cache_dir).unwrap().is_empty());

        checkpoint::Registry::new(&cache_dir, "good")
            .write_atomic(&sample_named_entry("good", "fake-checkpoint:good", "fake"))
            .unwrap();
        std::fs::write(
            cache_dir.join("checkpoints").join("corrupt.json"),
            b"not json",
        )
        .unwrap();

        let listed = list_named(&cache_dir).unwrap();
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert_eq!(listed[0].checkpoint_ref, "fake-checkpoint:good");
    }

    // remove: idempotent, and reports whether anything actually existed.
    #[tokio::test]
    async fn remove_named_is_idempotent_and_reports_correctly() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("remove-named");
        let registry = checkpoint::Registry::new(&cache_dir, "seeded-db");
        registry
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "fake-checkpoint:x",
                "fake",
            ))
            .unwrap();

        let removed = remove_named(
            "seeded-db",
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .unwrap();
        assert!(removed, "a name with a registry entry must report true");
        assert!(!registry.exists());
        assert_eq!(
            backend.state.lock().unwrap().removed_checkpoints,
            vec!["fake-checkpoint:x".to_string()]
        );

        let removed_again = remove_named(
            "seeded-db",
            &(backend as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .unwrap();
        assert!(
            !removed_again,
            "removing a name with no registry entry must report false, not error"
        );
    }

    // remove: an entry belonging to a DIFFERENT backend than the one now active must
    // still delete the registry entry and report true, but must NEVER call
    // remove_checkpoint on the active backend with a ref format it didn't create —
    // the same same-backend gate find_named already applies.
    #[tokio::test]
    async fn remove_named_deletes_a_different_backend_entry_without_calling_remove_checkpoint() {
        let backend = FakeBackend::named("fake-active");
        let cache_dir = temp_cache_dir("remove-named-cross-backend");
        let registry = checkpoint::Registry::new(&cache_dir, "seeded-db");
        registry
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "other-backend-ref",
                "fake-other",
            ))
            .unwrap();

        let removed = remove_named(
            "seeded-db",
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
        )
        .await
        .unwrap();

        assert!(
            removed,
            "a different-backend entry still counts as something existed to remove"
        );
        assert!(
            !registry.exists(),
            "the registry entry must be deleted regardless of the entry's backend"
        );
        assert!(
            backend.state.lock().unwrap().removed_checkpoints.is_empty(),
            "a different-backend entry's ref must never reach remove_checkpoint"
        );
    }

    // ============================ checkpoint archives ============================

    fn temp_archive_path(label: &str) -> std::path::PathBuf {
        temp_cache_dir(label).join("cp.archive")
    }

    fn archive_manifest(
        name: Option<&str>,
        backend: &str,
        checkpoint_ref: &str,
    ) -> crate::archive::ArchiveManifest {
        crate::archive::ArchiveManifest {
            rightsize_archive: crate::archive::FORMAT_VERSION,
            name: name.map(str::to_string),
            checkpoint_ref: checkpoint_ref.to_string(),
            backend: backend.to_string(),
            created_iso: "2025-01-01T00:00:00Z".to_string(),
            spec: checkpoint::NamedRegistrySpec {
                env: std::collections::BTreeMap::from([("A".to_string(), "1".to_string())]),
                command: Some(vec!["redis-server".to_string()]),
                exposed_ports: vec![6379],
                memory_limit_mb: Some(256),
            },
        }
    }

    /// Builds a well-formed archive directly at the tar layer — independent of
    /// `export_to_impl`, so a test can also build a deliberately malformed one
    /// (see [`write_archive_missing_manifest`]/[`write_archive_malformed_json`]).
    async fn write_archive(
        dest: &std::path::Path,
        manifest: &crate::archive::ArchiveManifest,
        artifact_bytes: &[u8],
    ) {
        let staging = crate::archive::TempStagingDir::create("test-fixture").unwrap();
        crate::archive::write_manifest(&crate::archive::manifest_path(staging.path()), manifest)
            .unwrap();
        std::fs::write(
            crate::archive::artifact_path(staging.path()),
            artifact_bytes,
        )
        .unwrap();
        crate::archive::tar_create(dest, staging.path())
            .await
            .unwrap();
    }

    /// A tar containing only `artifact`, no `checkpoint.json` — the "archive
    /// missing checkpoint.json" fixture.
    async fn write_archive_missing_manifest(dest: &std::path::Path) {
        let staging = crate::archive::TempStagingDir::create("test-fixture-no-manifest").unwrap();
        std::fs::write(crate::archive::artifact_path(staging.path()), b"artifact").unwrap();
        let output = tokio::process::Command::new("tar")
            .arg("-cf")
            .arg(dest)
            .arg("-C")
            .arg(staging.path())
            .arg("artifact")
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "{output:?}");
    }

    /// A tar whose `checkpoint.json` member exists but isn't valid JSON — the
    /// "malformed json" fixture.
    async fn write_archive_malformed_json(dest: &std::path::Path) {
        let staging = crate::archive::TempStagingDir::create("test-fixture-bad-json").unwrap();
        std::fs::write(crate::archive::manifest_path(staging.path()), b"not json").unwrap();
        std::fs::write(crate::archive::artifact_path(staging.path()), b"artifact").unwrap();
        crate::archive::tar_create(dest, staging.path())
            .await
            .unwrap();
    }

    // export: backend mismatch refuses before any backend or filesystem work.
    #[tokio::test]
    async fn export_to_refuses_on_a_backend_mismatch_before_any_backend_call() {
        let cp = Checkpoint {
            checkpoint_ref: "fake-checkpoint:abc".to_string(),
            backend: "fake-a".to_string(),
            spec: ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef"),
        };

        let active_backend = FakeBackend::named("fake-b");
        let dest = temp_archive_path("export-mismatch");
        let err = export_to_impl(
            &cp,
            &dest,
            &(active_backend.clone() as Arc<dyn SandboxBackend>),
            &temp_cache_dir("export-mismatch-cache"),
            &std::env::temp_dir(),
        )
        .await
        .expect_err("a backend mismatch must refuse before any work");
        assert!(
            matches!(err, RightsizeError::CheckpointBackendMismatch { .. }),
            "{err}"
        );
        assert!(!dest.exists(), "no archive may be written on a mismatch");
        assert!(
            active_backend
                .state
                .lock()
                .unwrap()
                .exported_checkpoints
                .is_empty(),
            "no backend call may happen once the mismatch is detected"
        );
    }

    // export: a stale artifact (has_checkpoint == false) is a typed error, and
    // export_checkpoint is never called.
    #[tokio::test]
    async fn export_to_refuses_when_the_artifact_is_stale() {
        let backend = FakeBackend::checkpoint_capable();
        backend.set_probe_result(Some(false)); // "definitely gone"
        let cp = Checkpoint {
            checkpoint_ref: "fake-checkpoint:gone".to_string(),
            backend: "fake".to_string(),
            spec: ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef"),
        };

        let dest = temp_archive_path("export-stale");
        let err = export_to_impl(
            &cp,
            &dest,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &temp_cache_dir("export-stale-cache"),
            &std::env::temp_dir(),
        )
        .await
        .expect_err("a stale artifact must refuse");
        assert!(
            matches!(err, RightsizeError::CheckpointArtifactMissing { .. }),
            "{err}"
        );
        assert!(!dest.exists());
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .exported_checkpoints
                .is_empty(),
            "export_checkpoint must never be called once the staleness probe refuses"
        );
    }

    // export: the staging dir is removed whether the export succeeds or fails
    // partway through.
    #[tokio::test]
    async fn export_to_cleans_up_its_staging_dir_on_success_and_on_failure() {
        // A private staging parent, not the shared `std::env::temp_dir()` —
        // sibling tests race identically-prefixed staging dirs into that
        // shared directory, so scanning it for "no `rightsize-archive-
        // export-*` entries" is flaky under parallel test execution. Scanning
        // this test's own private parent instead is fully deterministic.
        let staging_parent = temp_cache_dir("export-cleanup-staging-parent");
        let matching_staging_dirs = |label: &str| -> Vec<std::path::PathBuf> {
            std::fs::read_dir(&staging_parent)
                .map(|rd| {
                    rd.filter_map(std::result::Result::ok)
                        .map(|e| e.path())
                        .filter(|p| {
                            p.file_name()
                                .and_then(|n| n.to_str())
                                .is_some_and(|n| n.contains(label))
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

        // Success path.
        let backend = FakeBackend::checkpoint_capable();
        let cp = Checkpoint {
            checkpoint_ref: "fake-checkpoint:ok".to_string(),
            backend: "fake".to_string(),
            spec: ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef"),
        };
        export_to_impl(
            &cp,
            &temp_archive_path("export-cleanup-ok"),
            &(backend as Arc<dyn SandboxBackend>),
            &temp_cache_dir("export-cleanup-ok-cache"),
            &staging_parent,
        )
        .await
        .expect("export must succeed");
        assert!(
            matching_staging_dirs("rightsize-archive-export-").is_empty(),
            "a successful export must leave no staging dir behind"
        );

        // Failure path: export_checkpoint itself fails after the staging dir
        // already exists.
        let failing_backend = FakeBackend::checkpoint_capable_that_fails_export();
        let cp = Checkpoint {
            checkpoint_ref: "fake-checkpoint:fail".to_string(),
            backend: "fake".to_string(),
            spec: ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef"),
        };
        export_to_impl(
            &cp,
            &temp_archive_path("export-cleanup-fail"),
            &(failing_backend as Arc<dyn SandboxBackend>),
            &temp_cache_dir("export-cleanup-fail-cache"),
            &staging_parent,
        )
        .await
        .expect_err("export_checkpoint must fail");
        assert!(
            matching_staging_dirs("rightsize-archive-export-").is_empty(),
            "a failed export must ALSO leave no staging dir behind"
        );
    }

    // export -> import round trip through a real tar file: payload bytes
    // identical, metadata fields identical, effective ref propagated into the
    // returned checkpoint and (for a named source) the registry entry.
    #[tokio::test]
    async fn export_then_import_round_trips_payload_and_metadata_and_propagates_the_effective_ref()
    {
        let export_backend = FakeBackend::checkpoint_capable();
        let export_cache = temp_cache_dir("archive-roundtrip-export-cache");
        checkpoint::Registry::new(&export_cache, "seeded-db")
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "fake-checkpoint:seeded-db",
                "fake",
            ))
            .unwrap();
        let mut spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        spec.env = vec![("A".to_string(), "1".to_string())];
        spec.command = Some(vec!["redis-server".to_string()]);
        spec.ports = vec![crate::model::PortBinding {
            host_port: 0,
            guest_port: 6379,
        }];
        spec.memory_limit_mb = Some(256);
        let cp = Checkpoint {
            checkpoint_ref: "fake-checkpoint:seeded-db".to_string(),
            backend: "fake".to_string(),
            spec,
        };

        let dest = temp_archive_path("archive-roundtrip");
        export_to_impl(
            &cp,
            &dest,
            &(export_backend.clone() as Arc<dyn SandboxBackend>),
            &export_cache,
            &std::env::temp_dir(),
        )
        .await
        .expect("export must succeed");
        assert_eq!(
            export_backend.state.lock().unwrap().exported_checkpoints[0].0,
            "fake-checkpoint:seeded-db"
        );

        // A second, independent backend instance and cache dir — the "different
        // machine" this feature exists for.
        let import_backend = FakeBackend::checkpoint_capable();
        let import_cache = temp_cache_dir("archive-roundtrip-import-cache");
        let imported = import_from_impl(
            &dest,
            &(import_backend.clone() as Arc<dyn SandboxBackend>),
            &import_cache,
            &std::env::temp_dir(),
        )
        .await
        .expect("import must succeed");

        assert_eq!(
            imported.checkpoint_ref,
            "effective:fake-checkpoint:seeded-db"
        );
        assert_eq!(imported.backend, "fake");
        assert_eq!(
            imported.spec.command,
            Some(vec!["redis-server".to_string()])
        );
        assert_eq!(imported.spec.ports[0].guest_port, 6379);
        assert_eq!(imported.spec.memory_limit_mb, Some(256));

        let state = import_backend.state.lock().unwrap();
        assert_eq!(state.imported_checkpoints.len(), 1);
        assert_eq!(state.imported_checkpoints[0].1, "fake-checkpoint:seeded-db");
        assert_eq!(
            state.imported_checkpoints[0].2,
            b"fake-artifact-payload:fake-checkpoint:seeded-db".to_vec(),
            "the imported artifact bytes must match exactly what export_checkpoint wrote"
        );
        drop(state);

        let entry = checkpoint::Registry::new(&import_cache, "seeded-db")
            .read()
            .expect("a named archive must write a registry entry on import");
        assert_eq!(entry.checkpoint_ref, imported.checkpoint_ref);
        assert_eq!(entry.backend, "fake");
    }

    // export: unnamed checkpoints export fine — the archive's `name` is null, and
    // import writes no registry entry.
    #[tokio::test]
    async fn an_unnamed_checkpoint_exports_and_imports_with_a_null_name_and_no_registry_write() {
        let export_backend = FakeBackend::checkpoint_capable();
        let cp = Checkpoint {
            checkpoint_ref: "fake-checkpoint:anon".to_string(),
            backend: "fake".to_string(),
            spec: ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef"),
        };
        let dest = temp_archive_path("archive-unnamed");
        export_to_impl(
            &cp,
            &dest,
            &(export_backend as Arc<dyn SandboxBackend>),
            &temp_cache_dir("archive-unnamed-export-cache"), // no registry entry here.
            &std::env::temp_dir(),
        )
        .await
        .unwrap();

        let staging = crate::archive::TempStagingDir::create("archive-unnamed-inspect").unwrap();
        crate::archive::tar_extract(&dest, staging.path())
            .await
            .unwrap();
        let raw = std::fs::read_to_string(crate::archive::manifest_path(staging.path())).unwrap();
        assert!(raw.contains("\"name\": null"), "{raw}");

        let import_backend = FakeBackend::checkpoint_capable();
        let import_cache = temp_cache_dir("archive-unnamed-import-cache");
        let imported = import_from_impl(
            &dest,
            &(import_backend as Arc<dyn SandboxBackend>),
            &import_cache,
            &std::env::temp_dir(),
        )
        .await
        .unwrap();
        assert_eq!(imported.checkpoint_ref, "effective:fake-checkpoint:anon");
        assert!(
            checkpoint::list_registry_entries(&import_cache)
                .unwrap()
                .is_empty(),
            "an unnamed archive must write no registry entry at all"
        );
    }

    // import: a missing file is a typed error, no backend call, no registry write.
    #[tokio::test]
    async fn import_from_a_missing_file_is_a_typed_error_before_any_backend_call() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("import-missing-file");
        let err = import_from_impl(
            std::path::Path::new("/definitely/not/a/real/archive"),
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect_err("a missing archive file must be a typed error");
        assert!(
            matches!(err, RightsizeError::MalformedArchive { .. }),
            "{err}"
        );
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .imported_checkpoints
                .is_empty(),
            "no backend call may happen once extraction fails"
        );
        assert!(list_named(&cache_dir).unwrap().is_empty());
    }

    // import: an archive missing checkpoint.json is a typed error, no backend
    // call, no registry write.
    #[tokio::test]
    async fn import_from_an_archive_missing_checkpoint_json_is_a_typed_error() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("import-missing-manifest");
        let archive = temp_archive_path("import-missing-manifest-archive");
        write_archive_missing_manifest(&archive).await;

        let err = import_from_impl(
            &archive,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect_err("a missing checkpoint.json must be a typed error");
        assert!(
            matches!(err, RightsizeError::MalformedArchive { .. }),
            "{err}"
        );
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .imported_checkpoints
                .is_empty()
        );
        assert!(list_named(&cache_dir).unwrap().is_empty());
    }

    // import: malformed JSON is a typed error, no backend call, no registry write.
    #[tokio::test]
    async fn import_from_malformed_json_is_a_typed_error() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("import-malformed-json");
        let archive = temp_archive_path("import-malformed-json-archive");
        write_archive_malformed_json(&archive).await;

        let err = import_from_impl(
            &archive,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect_err("malformed JSON must be a typed error");
        assert!(
            matches!(err, RightsizeError::MalformedArchive { .. }),
            "{err}"
        );
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .imported_checkpoints
                .is_empty()
        );
        assert!(list_named(&cache_dir).unwrap().is_empty());
    }

    // import: an unsupported rightsizeArchive version is a typed error naming the
    // value, no backend call, no registry write.
    #[tokio::test]
    async fn import_from_an_unsupported_archive_version_is_a_typed_error_naming_the_value() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("import-bad-version");
        let archive = temp_archive_path("import-bad-version-archive");
        let mut manifest = archive_manifest(Some("seeded-db"), "fake", "fake-checkpoint:x");
        manifest.rightsize_archive = 2;
        write_archive(&archive, &manifest, b"artifact").await;

        let err = import_from_impl(
            &archive,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect_err("an unsupported version must be a typed error");
        match err {
            RightsizeError::MalformedArchive { reason, .. } => {
                assert!(reason.contains('2'), "{reason}");
            }
            other => panic!("expected MalformedArchive, got {other:?}"),
        }
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .imported_checkpoints
                .is_empty()
        );
        assert!(list_named(&cache_dir).unwrap().is_empty());
    }

    // import: an invalid name is a typed error, no backend call, no registry
    // write.
    #[tokio::test]
    async fn import_from_an_invalid_name_is_a_typed_error() {
        let backend = FakeBackend::checkpoint_capable();
        let cache_dir = temp_cache_dir("import-invalid-name");
        let archive = temp_archive_path("import-invalid-name-archive");
        let manifest = archive_manifest(Some("Bad Name!"), "fake", "fake-checkpoint:x");
        write_archive(&archive, &manifest, b"artifact").await;

        let err = import_from_impl(
            &archive,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect_err("an invalid name must be a typed error");
        assert!(
            matches!(err, RightsizeError::InvalidCheckpointName { .. }),
            "{err}"
        );
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .imported_checkpoints
                .is_empty()
        );
        assert!(list_named(&cache_dir).unwrap().is_empty());
    }

    // import: a backend mismatch is a typed error, no backend call, no registry
    // write.
    #[tokio::test]
    async fn import_from_a_backend_mismatch_is_a_typed_error() {
        let backend = FakeBackend::named("fake-active"); // checkpoint-capable
        let cache_dir = temp_cache_dir("import-backend-mismatch");
        let archive = temp_archive_path("import-backend-mismatch-archive");
        let manifest = archive_manifest(Some("seeded-db"), "fake-other", "other-ref");
        write_archive(&archive, &manifest, b"artifact").await;

        let err = import_from_impl(
            &archive,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect_err("a backend mismatch must be a typed error");
        assert!(
            matches!(err, RightsizeError::CheckpointBackendMismatch { .. }),
            "{err}"
        );
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .imported_checkpoints
                .is_empty()
        );
        assert!(list_named(&cache_dir).unwrap().is_empty());
    }

    // import replace semantics: an existing same-backend entry with a DIFFERENT
    // ref has its old artifact best-effort removed, and the registry entry is
    // rewritten with the new effective ref.
    #[tokio::test]
    async fn import_replaces_an_existing_same_backend_entry_removing_the_old_ref_first() {
        let backend = FakeBackend::checkpoint_capable();
        backend.set_import_effective_ref("fake-checkpoint:new-digest");
        let cache_dir = temp_cache_dir("import-replace-same-backend");
        checkpoint::Registry::new(&cache_dir, "seeded-db")
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "fake-checkpoint:old-digest",
                "fake",
            ))
            .unwrap();

        let archive = temp_archive_path("import-replace-same-backend-archive");
        let manifest = archive_manifest(Some("seeded-db"), "fake", "fake-checkpoint:whatever");
        write_archive(&archive, &manifest, b"artifact").await;

        let imported = import_from_impl(
            &archive,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect("import must succeed");
        assert_eq!(imported.checkpoint_ref, "fake-checkpoint:new-digest");

        assert_eq!(
            backend.state.lock().unwrap().removed_checkpoints,
            vec!["fake-checkpoint:old-digest".to_string()],
            "the OLD ref must be best-effort removed before the registry is rewritten"
        );

        let entry = checkpoint::Registry::new(&cache_dir, "seeded-db")
            .read()
            .expect("the registry entry must still exist");
        assert_eq!(entry.checkpoint_ref, "fake-checkpoint:new-digest");
    }

    // import replace semantics, content-addressed re-import: when the new
    // effective ref is the SAME as the existing entry's ref (msb's content-
    // addressed import resolving to the same digest twice), the old artifact must
    // NOT be removed — that artifact IS the one this import just materialized.
    #[tokio::test]
    async fn import_never_removes_the_old_artifact_when_the_effective_ref_is_unchanged() {
        let backend = FakeBackend::checkpoint_capable();
        backend.set_import_effective_ref("fake-checkpoint:same-digest");
        let cache_dir = temp_cache_dir("import-replace-same-ref");
        checkpoint::Registry::new(&cache_dir, "seeded-db")
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "fake-checkpoint:same-digest",
                "fake",
            ))
            .unwrap();

        let archive = temp_archive_path("import-replace-same-ref-archive");
        let manifest = archive_manifest(Some("seeded-db"), "fake", "fake-checkpoint:whatever");
        write_archive(&archive, &manifest, b"artifact").await;

        let imported = import_from_impl(
            &archive,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect("import must succeed");
        assert_eq!(imported.checkpoint_ref, "fake-checkpoint:same-digest");

        assert!(
            backend.state.lock().unwrap().removed_checkpoints.is_empty(),
            "re-importing an archive that resolves to the SAME ref must never remove it"
        );
    }

    // import replace semantics, cross-backend: an existing entry for a DIFFERENT
    // backend than the one now active is left untouched except for the rewrite —
    // no foreign remove_checkpoint call, the same gate `checkpoint_named`'s own
    // replace path applies.
    #[tokio::test]
    async fn import_never_calls_remove_checkpoint_on_a_different_backend_entry() {
        let backend = FakeBackend::named("fake-active");
        let cache_dir = temp_cache_dir("import-replace-cross-backend");
        checkpoint::Registry::new(&cache_dir, "seeded-db")
            .write_atomic(&sample_named_entry(
                "seeded-db",
                "other-backend-ref",
                "fake-other",
            ))
            .unwrap();

        let archive = temp_archive_path("import-replace-cross-backend-archive");
        let manifest = archive_manifest(Some("seeded-db"), "fake-active", "fake-checkpoint:x");
        write_archive(&archive, &manifest, b"artifact").await;

        let imported = import_from_impl(
            &archive,
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache_dir,
            &std::env::temp_dir(),
        )
        .await
        .expect("import must succeed");

        assert!(
            backend.state.lock().unwrap().removed_checkpoints.is_empty(),
            "a different-backend entry's ref must never reach remove_checkpoint"
        );
        let entry = checkpoint::Registry::new(&cache_dir, "seeded-db")
            .read()
            .expect("the registry entry must be overwritten with the new one");
        assert_eq!(entry.checkpoint_ref, imported.checkpoint_ref);
        assert_eq!(entry.backend, "fake-active");
    }

    // =============================== runtime copy ===================================

    // Not-running: a typed error, before any backend call.
    #[tokio::test]
    async fn copy_file_to_container_on_a_non_running_container_is_a_state_error() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let mut guard = c.start().await.unwrap();
        guard.stop_inner().await; // stops it in place, without consuming the guard.

        let err = guard
            .copy_file_to_container("/tmp/does-not-matter", "/dst")
            .await
            .expect_err("copy must refuse on a stopped guard");
        assert!(err.to_string().contains("not running"), "{err}");
        assert!(
            backend.state.lock().unwrap().copied_in.is_empty(),
            "the backend must never be called once the running check refuses"
        );
    }

    #[tokio::test]
    async fn copy_file_from_container_on_a_non_running_container_is_a_state_error() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let mut guard = c.start().await.unwrap();
        guard.stop_inner().await;

        let err = guard
            .copy_file_from_container("/src", "/tmp/does-not-matter")
            .await
            .expect_err("copy must refuse on a stopped guard");
        assert!(err.to_string().contains("not running"), "{err}");
        assert!(backend.state.lock().unwrap().copied_out.is_empty());
    }

    // Relative container path: a typed error, before any backend call, on both
    // directions.
    #[tokio::test]
    async fn copy_file_to_container_with_a_relative_path_is_a_typed_error() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();

        let err = guard
            .copy_file_to_container("/tmp/does-not-matter", "relative/path")
            .await
            .expect_err("a relative container path must be refused");
        assert!(err.to_string().contains("must be absolute"), "{err}");
        assert!(backend.state.lock().unwrap().copied_in.is_empty());

        guard.stop().await.unwrap();
    }

    #[tokio::test]
    async fn copy_file_from_container_with_a_relative_path_is_a_typed_error() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();

        let err = guard
            .copy_file_from_container("relative/path", "/tmp/does-not-matter")
            .await
            .expect_err("a relative container path must be refused");
        assert!(err.to_string().contains("must be absolute"), "{err}");
        assert!(backend.state.lock().unwrap().copied_out.is_empty());

        guard.stop().await.unwrap();
    }

    // The generic layer's own mkdir-p pre-step: an exec call is issued for the
    // destination's parent directory before the backend's copy_to_container.
    #[tokio::test]
    async fn copy_file_to_container_issues_a_mkdir_p_for_the_parent_before_the_backend_copy() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();

        guard
            .copy_file_to_container("/tmp/host-src.txt", "/mnt/nested/dst.txt")
            .await
            .expect("copy must succeed against the fake backend");

        {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.copied_in.len(), 1);
            assert_eq!(state.copied_in[0].2, "/mnt/nested/dst.txt");
            assert_eq!(
                state.copied_in[0].1,
                std::path::PathBuf::from("/tmp/host-src.txt")
            );
        }

        guard.stop().await.unwrap();
    }

    // copy_content_to_container: writes to a temp file, delegates to the file path,
    // and removes the temp file afterward — the caller never has to manage it.
    #[tokio::test]
    async fn copy_content_to_container_creates_and_removes_its_temp_file() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();

        guard
            .copy_content_to_container(b"hello from memory".as_slice(), "/dst/from-memory.txt")
            .await
            .expect("copy must succeed against the fake backend");

        let (temp_path, existed_during_the_call) = {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.copied_in.len(), 1);
            assert_eq!(state.copied_in[0].2, "/dst/from-memory.txt");
            (state.copied_in[0].1.clone(), state.copied_in[0].3)
        };
        assert!(
            existed_during_the_call,
            "the temp file must exist at the moment the backend is called: {temp_path:?}"
        );
        assert!(
            !temp_path.exists(),
            "the temp file must be removed once the copy call returns: {temp_path:?}"
        );

        guard.stop().await.unwrap();
    }

    // Interlock with the reaping ledger: a restored container is registered and
    // reaped exactly like any other — its name appears in `.sandboxes` while
    // running, same as `dropping_a_guard_without_stop_removes_it_from_the_reaping_ledger`
    // proves for an ordinary container.
    #[tokio::test]
    async fn restored_container_is_registered_in_the_reaping_ledger_like_any_other() {
        // Test isolation seam: a per-test scratch cache dir for the reaping
        // ledger (see `Container::with_reaper_cache_dir_override`'s doc) — without
        // it, this test's assertion reads the real, process-wide ledger every
        // OTHER concurrently-running test in this binary also writes to.
        let cache_dir = temp_cache_dir("restored-ledger");

        let source_backend = FakeBackend::checkpoint_capable();
        let source = container_on(&source_backend)
            .with_exposed_ports(&[6379])
            .with_reaper_cache_dir_override(cache_dir.clone());
        let source_guard = source.start().await.unwrap();
        let cp = source_guard.checkpoint().await.unwrap();
        source_guard.stop().await.unwrap();

        let restore_backend = FakeBackend::new();
        let restored = Container::from_checkpoint(&cp)
            .with_backend(restore_backend.clone())
            .with_reaper_cache_dir_override(cache_dir.clone())
            .waiting_for(ReadyImmediately);
        let restored_guard = restored.start().await.expect("restore must start");
        let ledger_name = restore_backend.state.lock().unwrap().created[0]
            .name
            .clone();

        let ledger = crate::reaper::Ledger::new(&cache_dir, crate::RunId::value());
        assert!(
            ledger.sandbox_names().contains(&ledger_name),
            "a restored container must be listed in the reaping ledger like any other"
        );

        restored_guard.stop().await.unwrap();
    }

    // Diagnostics registration: a running container is reachable through the report
    // by its unique name (`rz-<run-id>-<seq>`, unique regardless of concurrently
    // running tests sharing the process-wide registry); stop() removes it again.
    #[tokio::test]
    async fn a_started_container_is_registered_for_diagnostics_and_deregistered_on_stop() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        let name = guard.name().to_string();
        // The report's own header delimiter — plain `report.contains(&name)` would be
        // a substring false-positive against another concurrently-running test's
        // longer name sharing this one as a numeric prefix (e.g. name `..-42` is a
        // substring of a sibling test's `..-420`); the header's trailing `" ("` rules
        // that out, since a longer name's next character there is never `(`.
        let header = format!("-- {name} (");

        let report = crate::diagnostics().await;
        assert!(report.contains(&header), "{report}");

        guard.stop().await.unwrap();
        let report_after_stop = crate::diagnostics().await;
        assert!(
            !report_after_stop.contains(&header),
            "stop() must deregister this container from the diagnostics report"
        );
    }

    /// A wait strategy that captures a `diagnostics()` snapshot from mid-wait (before
    /// deciding readiness), then always succeeds — proves registration happens only
    /// AFTER the wait passes, not as soon as the guard exists.
    struct CaptureDiagnosticsThenReady {
        mid_wait_report: Arc<StdMutex<Option<String>>>,
    }
    #[async_trait::async_trait]
    impl WaitStrategy for CaptureDiagnosticsThenReady {
        async fn wait_until_ready(&self, _target: &dyn WaitTarget) -> Result<()> {
            *self.mid_wait_report.lock().unwrap() = Some(crate::diagnostics().await);
            Ok(())
        }
        fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
            self
        }
    }

    // Registration timing: a container must never be diagnosable while its readiness
    // wait is still running — only a FULLY-successful start (wait passed) makes it
    // reachable through the report. Mirrors the Kotlin port's resolution of the same
    // finding (register only after the wait succeeds).
    #[tokio::test]
    async fn a_container_is_not_diagnosable_until_its_readiness_wait_succeeds() {
        let backend = FakeBackend::new();
        let mid_wait_report = Arc::new(StdMutex::new(None));
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(CaptureDiagnosticsThenReady {
                mid_wait_report: mid_wait_report.clone(),
            })
            .with_exposed_ports(&[6379]);

        let guard = c.start().await.expect("this wait strategy always succeeds");
        let name = guard.name().to_string();
        let header = format!("-- {name} (");

        let captured = mid_wait_report
            .lock()
            .unwrap()
            .clone()
            .expect("wait strategy must have run");
        assert!(
            !captured.contains(&header),
            "a container must not be diagnosable while its readiness wait is still \
             running: {captured}"
        );

        let report_after_start = crate::diagnostics().await;
        assert!(report_after_start.contains(&header), "{report_after_start}");

        guard.stop().await.unwrap();
    }

    // Registration timing, failure branch: when the wait never succeeds, the
    // container must never become diagnosable at all — there is nothing for the
    // failure path's teardown to deregister.
    #[tokio::test]
    async fn a_container_that_never_becomes_ready_is_never_diagnosable() {
        let backend = FakeBackend::new();
        let precaptured_port = Arc::new(StdMutex::new(None));
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(NeverReady {
                precaptured_port: precaptured_port.clone(),
            })
            .with_exposed_ports(&[6379]);

        let err = expect_start_err(c.start().await, "wait strategy must fail start()");
        assert!(err.to_string().contains("never ready"), "{err}");

        let name = backend.state.lock().unwrap().created[0].name.clone();
        let header = format!("-- {name} (");
        let report = crate::diagnostics().await;
        assert!(
            !report.contains(&header),
            "a container that never became ready must never appear in the \
             diagnostics report: {report}"
        );
    }

    // Not-running vs not-exposed disambiguation.
    #[tokio::test]
    async fn get_mapped_port_reports_not_running_after_stop_clears_the_mappings() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        assert!(guard.get_mapped_port(6379).unwrap() > 0);
        guard.stop().await.unwrap();

        // The guard was consumed by stop(); re-derive a fresh one to exercise the
        // stopped-state error message via a boot that we then let the wait strategy
        // fail after capturing the port — simpler: build a second guard, call stop via
        // an explicit helper path. Since `stop` consumes `self`, we instead assert the
        // not-running message shape directly against a guard we stop through the
        // internal helper without consuming it, mirroring what Drop/stop share.
        let backend2 = FakeBackend::new();
        let c2 = container_on(&backend2).with_exposed_ports(&[6379]);
        let mut guard2 = c2.start().await.unwrap();
        guard2.stop_inner().await;
        let err = guard2.get_mapped_port(6379).unwrap_err().to_string();
        assert!(err.contains("not running"), "{err}");
        assert!(!err.contains("not exposed"), "{err}");
    }

    // U8 (part 2): not-exposed for a port never declared.
    #[tokio::test]
    async fn get_mapped_port_reports_not_exposed_for_an_undeclared_port() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        let err = guard.get_mapped_port(9999).unwrap_err().to_string();
        assert!(err.contains("not exposed"), "{err}");
        guard.stop().await.unwrap();
    }

    // U2: network links installed on the new member for the running sibling, before
    // wait, after registering siblings only (never self).
    #[tokio::test]
    async fn u2_starting_on_a_network_installs_links_to_running_siblings() {
        let backend = FakeBackend::new();
        let net = Arc::new(Network::new_network());
        let stub = container_on(&backend)
            .with_exposed_ports(&[8888])
            .with_network(&net)
            .with_network_aliases(&["configuration-stub"]);
        let stub_guard = stub.start().await.unwrap();

        let app = container_on(&backend)
            .with_exposed_ports(&[8080])
            .with_network(&net);
        let app_guard = app.start().await.unwrap();

        let (consumer_id, links) = {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.installed_links.len(), 1);
            state.installed_links[0].clone()
        };
        assert_eq!(
            consumer_id,
            backend.state.lock().unwrap().created.last().unwrap().name
        );
        assert_eq!(
            links,
            vec![crate::backend::NetworkLink {
                alias: "configuration-stub".to_string(),
                guest_port: 8888,
                target_host_port: stub_guard.get_mapped_port(8888).unwrap(),
            }]
        );
        assert_eq!(
            net.resolve("configuration-stub", 8888).unwrap(),
            "configuration-stub:8888"
        );
        assert!(net.resolve("nope", 1).is_err());

        app_guard.stop().await.unwrap();
        stub_guard.stop().await.unwrap();
    }

    // U8 (part 3): a single container on a network installs no links but is still
    // registered, so a later joiner links back to it.
    #[tokio::test]
    async fn single_container_on_network_installs_no_links_but_is_registered() {
        let backend = FakeBackend::new();
        let net = Arc::new(Network::new_network());
        let solo = container_on(&backend)
            .with_exposed_ports(&[9999])
            .with_network(&net)
            .with_network_aliases(&["solo"]);
        let solo_guard = solo.start().await.unwrap();
        assert!(
            backend.state.lock().unwrap().installed_links.is_empty(),
            "a lone container must not link to itself"
        );

        let joiner = container_on(&backend)
            .with_exposed_ports(&[8080])
            .with_network(&net);
        let joiner_guard = joiner.start().await.unwrap();
        let (_, links) = {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.installed_links.len(), 1);
            state.installed_links[0].clone()
        };
        assert_eq!(
            links,
            vec![crate::backend::NetworkLink {
                alias: "solo".to_string(),
                guest_port: 9999,
                target_host_port: solo_guard.get_mapped_port(9999).unwrap(),
            }]
        );

        joiner_guard.stop().await.unwrap();
        solo_guard.stop().await.unwrap();
    }

    // U5 (injection point 1/2): wait-strategy failure stops the container and releases
    // its host ports — proven against FreePorts' own issued_view(), not just "the port
    // is bindable" (which would pass even if release were never called, since the fake
    // backend never binds OS ports either).
    #[tokio::test]
    async fn u5_wait_strategy_failure_stops_the_container_and_releases_ports() {
        let backend = FakeBackend::new();
        let precaptured_port = Arc::new(StdMutex::new(None));
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(NeverReady {
                precaptured_port: precaptured_port.clone(),
            })
            .with_exposed_ports(&[6379]);

        let err = expect_start_err(c.start().await, "wait strategy must fail start()");
        assert!(err.to_string().contains("never ready"), "{err}");

        let name = backend.state.lock().unwrap().created[0].name.clone();
        assert!(
            backend.state.lock().unwrap().stopped.contains(&name),
            "started container must be stopped when the wait strategy fails"
        );

        let port = precaptured_port
            .lock()
            .unwrap()
            .expect("wait strategy must have observed a real mapped port");
        assert!(port > 0);
        assert!(
            !free_ports().issued_view().contains(&port),
            "port {port} must be released by the wait-strategy-failure cleanup path"
        );
    }

    // U5 (injection point 2/2): install_network_links failure stops the container too,
    // and — like the wait-strategy injection point above — releases its host ports back
    // to FreePorts. Proven against issued_view() directly, not just "stop was called",
    // so this fails the same way U5's first injection point would if `start()` ever
    // stopped releasing ports on this seam specifically.
    #[tokio::test]
    async fn u5_install_network_links_failure_stops_the_container() {
        let backend = FakeBackend::failing_install_network_links();
        let net = Arc::new(Network::new_network());
        let c = container_on(&backend)
            .with_exposed_ports(&[8080])
            .with_network(&net);

        let err = expect_start_err(
            c.start().await,
            "install_network_links failure must propagate",
        );
        assert!(err.to_string().contains("nc"), "{err}");

        let created = backend.state.lock().unwrap().created[0].clone();
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .stopped
                .contains(&created.name),
            "started container must be stopped on link-install failure"
        );

        let port = created
            .ports
            .first()
            .expect("spec must carry the allocated host port")
            .host_port;
        assert!(
            !free_ports().issued_view().contains(&port),
            "port {port} must be released when install_network_links fails, \
             not just when the wait strategy fails"
        );
    }

    // `before_ensure_network` appends to the reaping ledger's `.networks` file BEFORE
    // `backend.ensure_network` is even called, mirroring the sandbox-name discipline in
    // `create_started_container`. Unlike that path, a failed `ensure_network` used to
    // have no matching cleanup — this proves the fix: a failed ensure_network must not
    // leave a phantom `.networks` entry behind (which would otherwise block this run's
    // own clean-shutdown deletion trigger for the rest of the process).
    #[tokio::test]
    async fn ensure_network_failure_does_not_leave_a_phantom_networks_ledger_entry() {
        // Test isolation seam: see `restored_container_is_registered_in_the_
        // reaping_ledger_like_any_other`'s own comment.
        let cache_dir = temp_cache_dir("ensure-network-failure-ledger");

        let backend = FakeBackend::failing_ensure_network();
        let net = Arc::new(Network::new_network());
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_network(&net)
            .with_reaper_cache_dir_override(cache_dir.clone());

        let err = expect_start_err(c.start().await, "ensure_network failure must propagate");
        assert!(err.to_string().contains("ensure_network failed"), "{err}");

        let ledger = crate::reaper::Ledger::new(&cache_dir, crate::RunId::value());
        assert!(
            !ledger.network_ids().contains(&net.id().to_string()),
            "a failed ensure_network must not leave a phantom .networks entry behind"
        );
    }

    // U7: stop is a no-op before start, and Drop after an explicit stop() doesn't
    // double-release ports or double-call the backend (stop() itself can't be called
    // twice in Rust: it consumes the guard by value, so "calling stop() twice" is a
    // compile-time guarantee here instead of a runtime assertion — what remains to
    // prove is that Drop, which still runs after stop() returns, doesn't redo any of
    // stop()'s work).
    #[tokio::test]
    async fn u7_stop_is_idempotent_across_the_stop_then_drop_sequence() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        let name = backend.state.lock().unwrap().created[0].name.clone();
        let mapped_port = guard.get_mapped_port(6379).unwrap();

        guard.stop().await.unwrap(); // guard is consumed here; Drop still runs after.

        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .stopped
                .iter()
                .filter(|n| **n == name)
                .count(),
            1,
            "backend.stop must be called exactly once"
        );
        assert!(
            !free_ports().issued_view().contains(&mapped_port),
            "port must be released by stop()"
        );
    }

    #[tokio::test]
    async fn stop_before_start_is_a_no_op() {
        // There is no "unstarted guard" in this API shape (a guard only exists after a
        // successful start()) — the closest analogue is starting then immediately
        // stopping via the internal helper twice, proving the second call is a no-op.
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let mut guard = c.start().await.unwrap();
        let name = backend.state.lock().unwrap().created[0].name.clone();

        guard.stop_inner().await;
        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .stopped
                .iter()
                .filter(|n| **n == name)
                .count(),
            1
        );
        guard.stop_inner().await; // second call: must not re-call backend.stop or double-release.
        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .stopped
                .iter()
                .filter(|n| **n == name)
                .count(),
            1,
            "a second stop must not re-call backend.stop"
        );
        assert!(!guard.is_running());
    }

    // U6: port-bind-conflict retry — a fake backend that fails start with a typed
    // PortBindConflict on attempts 1-2 then succeeds; ports are reallocated per attempt.
    struct PortConflictBackend {
        fail_first: usize,
        conflict: Box<dyn Fn(u16) -> RightsizeError + Send + Sync>,
        created: StdMutex<Vec<ContainerSpec>>,
        started_ports: StdMutex<Vec<u16>>,
        start_attempts: std::sync::atomic::AtomicUsize,
    }
    impl PortConflictBackend {
        fn new(fail_first: usize) -> Arc<Self> {
            Self::with_conflict(fail_first, |port| {
                RightsizeError::Backend(format!(
                    "driver failed programming external connectivity: failed to bind host port 127.0.0.1:{port}/tcp: address already in use"
                ))
            })
        }
        fn with_conflict(
            fail_first: usize,
            conflict: impl Fn(u16) -> RightsizeError + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                fail_first,
                conflict: Box::new(conflict),
                created: StdMutex::new(Vec::new()),
                started_ports: StdMutex::new(Vec::new()),
                start_attempts: std::sync::atomic::AtomicUsize::new(0),
            })
        }
        fn create_count(&self) -> usize {
            self.created.lock().unwrap().len()
        }
    }
    #[async_trait::async_trait]
    impl SandboxBackend for PortConflictBackend {
        fn name(&self) -> &str {
            "port-conflict"
        }
        fn supports_native_networks(&self) -> bool {
            false
        }
        async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
            self.created.lock().unwrap().push(spec.clone());
            Ok(Box::new(FakeHandle {
                id: spec.name.clone(),
                spec,
            }))
        }
        async fn start(&self, handle: &dyn SandboxHandle) -> Result<()> {
            let attempt = self.start_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let port = handle.spec().ports[0].host_port;
            self.started_ports.lock().unwrap().push(port);
            if attempt <= self.fail_first {
                return Err((self.conflict)(port));
            }
            Ok(())
        }
        async fn stop(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn remove(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn exec(&self, _handle: &dyn SandboxHandle, _cmd: &[String]) -> Result<ExecResult> {
            Ok(ExecResult {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
        async fn logs(&self, _handle: &dyn SandboxHandle) -> Result<String> {
            Ok(String::new())
        }
        async fn follow_logs(
            &self,
            _handle: &dyn SandboxHandle,
            _consumer: Box<dyn Fn(String) + Send + Sync>,
        ) -> Result<crate::backend::FollowHandle> {
            unimplemented!()
        }
        async fn ensure_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        async fn remove_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        fn cleanup_sync(&self, _container_id: &str) {}
        fn remove_by_name(&self, _name: &str) {}
        fn watchdog_kill_command(&self) -> Vec<String> {
            vec!["true".to_string()]
        }
    }

    #[tokio::test]
    async fn u6_start_retries_with_fresh_host_ports_on_a_bind_conflict() {
        let backend = PortConflictBackend::new(2);
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let guard = c.start().await.expect("must eventually succeed");
        assert!(guard.is_running());
        assert_eq!(
            backend.create_count(),
            3,
            "each attempt recreates the container"
        );
        let started_ports = backend.started_ports.lock().unwrap().clone();
        assert_eq!(started_ports.len(), 3, "start attempted three times");
        let distinct: std::collections::HashSet<u16> = started_ports.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            started_ports.len(),
            "ports are reallocated per attempt, not reused after a conflict"
        );
        guard.stop().await.unwrap();
    }

    /// A reuse fresh-create (no registry entry yet) must retry with fresh host
    /// ports on a bind conflict exactly like an ordinary container's `start()`
    /// does (see [`u6_start_retries_with_fresh_host_ports_on_a_bind_conflict`]) —
    /// the sibling ports use the same retry discipline for their own reuse
    /// fresh-create path, and this crate must not silently fail-fast instead on
    /// the very first transient port collision a reuse boot happens to hit.
    #[tokio::test]
    async fn u6_reuse_fresh_create_retries_with_fresh_host_ports_on_a_bind_conflict() {
        let backend = PortConflictBackend::new(2);
        let cache_dir = temp_cache_dir("fresh-create-port-conflict");
        let c = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir)
            .with_reuse_env_override(true)
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379])
            .reuse(true);
        let guard = c
            .start()
            .await
            .expect("reuse fresh-create must retry and eventually succeed");
        assert!(guard.is_running());
        assert_eq!(
            backend.create_count(),
            3,
            "each attempt recreates the container"
        );
        let started_ports = backend.started_ports.lock().unwrap().clone();
        assert_eq!(started_ports.len(), 3, "start attempted three times");
        let distinct: std::collections::HashSet<u16> = started_ports.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            started_ports.len(),
            "ports are reallocated per attempt, not reused after a conflict"
        );
        guard.stop().await.unwrap();
    }

    #[tokio::test]
    async fn u6_start_retries_on_the_typed_port_bind_conflict_bare_or_nested() {
        let bare = PortConflictBackend::with_conflict(1, |port| RightsizeError::PortBindConflict {
            message: format!("could not bind host port {port}"),
            source: None,
        });
        let c = Container::new("redis:8.6-alpine")
            .with_backend(bare.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let guard = c
            .start()
            .await
            .expect("must retry exactly once for the typed exception");
        assert!(guard.is_running());
        assert_eq!(bare.create_count(), 2);
        guard.stop().await.unwrap();

        // Wrapped: a typed PortBindConflict nested two levels deep under other
        // PortBindConflicts whose own messages say nothing about ports — only walking
        // the `source` chain (not the string fallback) finds it.
        let nested =
            PortConflictBackend::with_conflict(1, |port| RightsizeError::PortBindConflict {
                message: "start failed".to_string(),
                source: Some(Box::new(RightsizeError::PortBindConflict {
                    message: "io error".to_string(),
                    source: Some(Box::new(RightsizeError::PortBindConflict {
                        message: format!("could not bind host port {port}"),
                        source: None,
                    })),
                })),
            });
        let c = Container::new("redis:8.6-alpine")
            .with_backend(nested.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let guard = c
            .start()
            .await
            .expect("must unwrap to find a nested typed exception");
        assert!(guard.is_running());
        assert_eq!(nested.create_count(), 2);
        guard.stop().await.unwrap();
    }

    #[tokio::test]
    async fn u6_truth_table_known_phrasings_retry_negative_does_not() {
        let phrasings = [
            "address already in use",
            "port is already allocated",
            "bind: address already in use",
            "Bind for 0.0.0.0:32770 failed: PORT IS ALREADY ALLOCATED",
        ];
        for phrasing in phrasings {
            let backend = PortConflictBackend::with_conflict(1, move |_port| {
                RightsizeError::Backend(phrasing.to_string())
            });
            let c = Container::new("redis:8.6-alpine")
                .with_backend(backend.clone())
                .waiting_for(ReadyImmediately)
                .with_exposed_ports(&[6379]);
            let guard = c.start().await.unwrap_or_else(|e| {
                panic!("must retry and succeed for phrasing '{phrasing}': {e}")
            });
            assert!(guard.is_running());
            assert_eq!(
                backend.create_count(),
                2,
                "must retry exactly once for phrasing: {phrasing}"
            );
            guard.stop().await.unwrap();
        }

        // Negative: an unrelated failure must NOT be treated as a conflict.
        let boom = PortConflictBackend::with_conflict(99, |_port| {
            RightsizeError::Backend("boom".to_string())
        });
        let c = Container::new("redis:8.6-alpine")
            .with_backend(boom.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let err = expect_start_err(
            c.start().await,
            "a non-conflict exception must fail fast, no retry",
        );
        assert_eq!(err.to_string(), "boom");
        assert_eq!(
            boom.create_count(),
            1,
            "a non-conflict exception must fail fast, no retry"
        );
    }

    #[tokio::test]
    async fn u4_and_u6_start_gives_up_after_the_retry_budget_is_exhausted_and_releases_every_attempts_ports()
     {
        let backend = PortConflictBackend::new(99);
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let err = expect_start_err(c.start().await, "must give up after the retry budget");
        assert!(err.to_string().contains("free host ports"), "{err}");

        let started_ports = backend.started_ports.lock().unwrap().clone();
        assert_eq!(
            started_ports.len(),
            PORT_BIND_ATTEMPTS,
            "all attempts must have been tried"
        );
        let distinct: std::collections::HashSet<u16> = started_ports.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            PORT_BIND_ATTEMPTS,
            "each retry must allocate a fresh port"
        );

        let issued = free_ports().issued_view();
        for port in started_ports {
            assert!(
                !issued.contains(&port),
                "port {port} from a discarded attempt must be released — mutation-verified: this is the U4 port-release gate"
            );
        }
    }

    // Memory-limit knob: with_memory_limit reaches the ContainerSpec; None when unset.
    #[tokio::test]
    async fn with_memory_limit_carries_through_to_the_container_spec() {
        let backend = FakeBackend::new();
        let limited = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_memory_limit(1024);
        let guard = limited.start().await.unwrap();
        assert_eq!(
            backend.state.lock().unwrap().created[0].memory_limit_mb,
            Some(1024)
        );
        guard.stop().await.unwrap();

        let unset = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = unset.start().await.unwrap();
        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .created
                .last()
                .unwrap()
                .memory_limit_mb,
            None
        );
        guard.stop().await.unwrap();
    }

    // Disk-limit knob: with_disk_limit reaches the ContainerSpec; None when unset.
    #[tokio::test]
    async fn with_disk_limit_carries_through_to_the_container_spec() {
        let backend = FakeBackend::new();
        let limited = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_disk_limit(2048);
        let guard = limited.start().await.unwrap();
        assert_eq!(
            backend.state.lock().unwrap().created[0].disk_limit_mb,
            Some(2048)
        );
        guard.stop().await.unwrap();

        let unset = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = unset.start().await.unwrap();
        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .created
                .last()
                .unwrap()
                .disk_limit_mb,
            None
        );
        guard.stop().await.unwrap();
    }

    // Tmpfs-root knob: with_tmpfs_root reaches the ContainerSpec; None when unset.
    #[tokio::test]
    async fn with_tmpfs_root_carries_through_to_the_container_spec() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_tmpfs_root(256);
        let guard = c.start().await.unwrap();
        assert_eq!(
            backend.state.lock().unwrap().created[0].tmpfs_root_mb,
            Some(256)
        );
        guard.stop().await.unwrap();
    }

    // Network-disabled knob: with_network_disabled reaches the ContainerSpec; false
    // when unset.
    #[tokio::test]
    async fn with_network_disabled_carries_through_to_the_container_spec() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_network_disabled();
        let guard = c.start().await.unwrap();
        assert!(backend.state.lock().unwrap().created[0].network_disabled);
        guard.stop().await.unwrap();

        let unset = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = unset.start().await.unwrap();
        assert!(
            !backend
                .state
                .lock()
                .unwrap()
                .created
                .last()
                .unwrap()
                .network_disabled
        );
        guard.stop().await.unwrap();
    }

    // with_disk_limit + with_tmpfs_root together is a typed, fail-fast error —
    // never reaches create().
    #[tokio::test]
    async fn disk_limit_plus_tmpfs_root_is_a_typed_error_before_any_create() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_disk_limit(1024)
            .with_tmpfs_root(512);
        let err = expect_start_err(c.start().await, "disk limit + tmpfs root must fail fast");
        assert!(matches!(err, RightsizeError::RootDiskConflict), "{err}");
        assert!(backend.state.lock().unwrap().created.is_empty());
    }

    // with_tmpfs_root(t) > with_memory_limit(m) is a typed, fail-fast error.
    #[tokio::test]
    async fn tmpfs_root_exceeding_the_memory_limit_is_a_typed_error() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_memory_limit(512)
            .with_tmpfs_root(1024);
        let err = expect_start_err(
            c.start().await,
            "tmpfs root exceeding the memory limit must fail fast",
        );
        assert!(
            matches!(
                err,
                RightsizeError::TmpfsRootExceedsMemory {
                    tmpfs_mb: 1024,
                    memory_mb: 512,
                }
            ),
            "{err}"
        );
        assert!(backend.state.lock().unwrap().created.is_empty());
    }

    // No memory limit at all: a tmpfs root of any size, however large, is never
    // validated here — msb's own error at boot time is already precise.
    #[tokio::test]
    async fn tmpfs_root_without_a_memory_limit_is_not_validated() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_tmpfs_root(u64::MAX);
        let guard = c
            .start()
            .await
            .expect("no memory limit set means no tmpfs-vs-memory validation");
        guard.stop().await.unwrap();
    }

    // with_network_disabled + with_network(...) is a typed, fail-fast error —
    // never reaches ensure_network or create().
    #[tokio::test]
    async fn network_disabled_plus_a_network_is_a_typed_error() {
        let backend = FakeBackend::new();
        let net = Arc::new(Network::new_network());
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_network(&net)
            .with_network_disabled();
        let err = expect_start_err(
            c.start().await,
            "network_disabled + a joined network must fail fast",
        );
        assert!(
            matches!(err, RightsizeError::NetworkDisabledConflict),
            "{err}"
        );
        assert!(backend.state.lock().unwrap().created.is_empty());
    }

    // dedup_env_last_wins in isolation — last value wins, key keeps the position
    // of its FIRST occurrence: an insertion-ordered map's put never moves an
    // existing key.
    #[test]
    fn dedup_env_last_wins_keeps_first_position_but_last_value() {
        let env = vec![
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "2".to_string()),
            ("A".to_string(), "override".to_string()),
            ("C".to_string(), "3".to_string()),
            ("B".to_string(), "final".to_string()),
        ];
        let deduped = dedup_env_last_wins(env);
        assert_eq!(
            deduped,
            vec![
                ("A".to_string(), "override".to_string()),
                ("B".to_string(), "final".to_string()),
                ("C".to_string(), "3".to_string()),
            ],
            "A and B must keep their FIRST-occurrence position; each must carry its LAST value"
        );
    }

    #[test]
    fn dedup_env_last_wins_is_a_no_op_on_already_unique_keys() {
        let env = vec![
            ("X".to_string(), "1".to_string()),
            ("Y".to_string(), "2".to_string()),
        ];
        assert_eq!(dedup_env_last_wins(env.clone()), env);
    }

    // Fix 3 (end-to-end through start()): a duplicate key set via with_env twice
    // reaches the backend's ContainerSpec exactly once, with the LAST value, in the
    // position of the FIRST with_env call — proving the dedup runs on the real
    // start() path, not just as a unit in isolation.
    #[tokio::test]
    async fn duplicate_with_env_calls_resolve_last_wins_in_the_spec_reaching_the_backend() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_env("MODE", "first")
            .with_env("OTHER", "x")
            .with_env("MODE", "second");
        let guard = c.start().await.unwrap();

        let spec = backend.state.lock().unwrap().created[0].clone();
        assert_eq!(
            spec.env,
            vec![
                ("MODE".to_string(), "second".to_string()),
                ("OTHER".to_string(), "x".to_string()),
            ],
            "MODE must appear exactly once, in its first-occurrence position, with its last value"
        );
        guard.stop().await.unwrap();
    }

    // Fix 3 (spec-customizer interaction): a customizer pushing a key that
    // duplicates one already set via with_env must ALSO resolve last-wins — the
    // dedup runs after the customizer, not just on the pre-customizer env.
    #[tokio::test]
    async fn a_spec_customizer_overriding_an_existing_key_also_resolves_last_wins() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_env("KAFKA_ADVERTISED_LISTENERS", "PLACEHOLDER")
            .with_spec_customizer(|mut spec, _mapped| {
                spec.env.push((
                    "KAFKA_ADVERTISED_LISTENERS".to_string(),
                    "PLAINTEXT://127.0.0.1:9999".to_string(),
                ));
                spec
            });
        let guard = c.start().await.unwrap();

        let spec = backend.state.lock().unwrap().created[0].clone();
        assert_eq!(
            spec.env,
            vec![(
                "KAFKA_ADVERTISED_LISTENERS".to_string(),
                "PLAINTEXT://127.0.0.1:9999".to_string()
            )],
            "the customizer's later push must win, deduped to a single entry"
        );
        guard.stop().await.unwrap();
    }

    // Validation-bypass fix: a spec-customizer can set conflicting fields
    // directly on the `ContainerSpec` it returns, which `Container::start()`'s
    // own pre-flight checks (running BEFORE the customizer) never see — the
    // post-customizer re-validation in `create_started_container` is what
    // catches these instead of letting them reach `backend.create` unvalidated.
    #[tokio::test]
    async fn a_spec_customizer_setting_both_root_disk_fields_is_refused_before_backend_create() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_spec_customizer(|mut spec, _mapped| {
                spec.disk_limit_mb = Some(512);
                spec.tmpfs_root_mb = Some(256);
                spec
            });

        let err = expect_start_err(
            c.start().await,
            "a customizer setting both root-disk fields must be refused",
        );
        assert!(matches!(err, RightsizeError::RootDiskConflict), "{err}");
        assert!(
            backend.state.lock().unwrap().created.is_empty(),
            "backend.create must never be reached once the post-customizer re-validation refuses"
        );
    }

    #[tokio::test]
    async fn a_spec_customizer_disabling_the_network_while_setting_a_network_id_is_refused_before_backend_create()
     {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_spec_customizer(|mut spec, _mapped| {
                spec.network_disabled = true;
                spec.network_id = Some("some-network".to_string());
                spec
            });

        let err = expect_start_err(
            c.start().await,
            "a customizer combining network_disabled with a network_id must be refused",
        );
        assert!(
            matches!(err, RightsizeError::NetworkDisabledConflict),
            "{err}"
        );
        assert!(backend.state.lock().unwrap().created.is_empty());
    }

    #[tokio::test]
    async fn a_spec_customizer_setting_a_tmpfs_root_over_the_memory_limit_is_refused_before_backend_create()
     {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_memory_limit(128)
            .with_spec_customizer(|mut spec, _mapped| {
                spec.tmpfs_root_mb = Some(256);
                spec
            });

        let err = expect_start_err(
            c.start().await,
            "a customizer setting a tmpfs root over the memory limit must be refused",
        );
        assert!(
            matches!(
                err,
                RightsizeError::TmpfsRootExceedsMemory {
                    tmpfs_mb: 256,
                    memory_mb: 128
                }
            ),
            "{err}"
        );
        assert!(backend.state.lock().unwrap().created.is_empty());
    }

    // U3: exec/get_mapped_port on a not-running guard errors.
    #[tokio::test]
    async fn u3_exec_and_mapped_port_require_a_running_container() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let mut guard = c.start().await.unwrap();
        guard.stop_inner().await;

        assert!(guard.exec(&["ls"]).await.is_err());
        assert!(guard.get_mapped_port(6379).is_err());
    }

    #[tokio::test]
    async fn exec_returns_the_backends_result() {
        let backend = FakeBackend::new();
        let c = container_on(&backend);
        let guard = c.start().await.unwrap();
        let result = guard.exec(&["ls", "-la"]).await.unwrap();
        assert_eq!(result.stdout, "ls -la");
        guard.stop().await.unwrap();
    }

    #[tokio::test]
    async fn dropping_a_guard_without_stop_releases_its_ports_synchronously() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        let port = guard.get_mapped_port(6379).unwrap();
        assert!(free_ports().issued_view().contains(&port));

        drop(guard); // no explicit stop(): Drop's synchronous fallback must still release the port.

        assert!(
            !free_ports().issued_view().contains(&port),
            "Drop must release mapped ports synchronously even without an explicit stop()"
        );
    }

    // The Drop-path's own opportunity to update the reaping ledger (see
    // `crate::cleanup`'s `after_teardown` doc) — without this, a sandbox torn down
    // only via `Drop` (never an explicit `.stop()`) stays listed in `.sandboxes` for
    // the rest of this process's life even though it's already gone, and this run's
    // own clean-shutdown deletion trigger never fires for it.
    #[tokio::test]
    async fn dropping_a_guard_without_stop_removes_it_from_the_reaping_ledger() {
        // Test isolation seam: see `restored_container_is_registered_in_the_
        // reaping_ledger_like_any_other`'s own comment — this test's ledger file
        // is now exclusive to it, not the real, process-wide one every other test
        // in this binary also writes to.
        let cache_dir = temp_cache_dir("dropping-guard-ledger");

        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_reaper_cache_dir_override(cache_dir.clone());
        let guard = c.start().await.unwrap();
        let ledger_name = backend.state.lock().unwrap().created[0].name.clone();

        let ledger = crate::reaper::Ledger::new(&cache_dir, crate::RunId::value());
        assert!(
            ledger.sandbox_names().contains(&ledger_name),
            "before_create must have listed the sandbox in the reaping ledger"
        );

        drop(guard); // no explicit stop(): the cleanup thread's fallback path runs instead.

        // The cleanup thread updates the ledger on a background thread, genuinely
        // asynchronously relative to this test (not a cross-test race — this
        // ledger file is exclusive to this test now) — poll until this entry is
        // gone.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while ledger.sandbox_names().contains(&ledger_name) && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(
            !ledger.sandbox_names().contains(&ledger_name),
            "Drop's cleanup-thread fallback must remove the sandbox from the reaping ledger \
             too, not just tear it down on the backend"
        );
    }

    // ======================================================================
    // Container reuse
    // ======================================================================

    fn temp_cache_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rz-reuse-container-test-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[derive(Default)]
    struct ReuseFakeState {
        created: Vec<ContainerSpec>,
        started: Vec<String>,
        stopped: Vec<String>,
        removed: Vec<String>,
        removed_by_name: Vec<String>,
        running: std::collections::HashSet<String>,
        find_running_calls: usize,
    }

    /// A fake backend for the reuse flow's own tests: unlike [`FakeBackend`] (which
    /// has no notion of "currently running by name" at all), this tracks a
    /// `running` name set directly, so [`SandboxBackend::find_running`] and
    /// [`SandboxBackend::remove_by_name`] behave like a real backend's would —
    /// exactly what the adopt/stale/collision scenarios below need to drive.
    struct ReuseFakeBackend {
        state: StdMutex<ReuseFakeState>,
        conflict_once_for_name: StdMutex<Option<String>>,
        on_conflict: StdMutex<Option<Box<dyn FnMut() + Send>>>,
    }

    impl ReuseFakeBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(ReuseFakeState::default()),
                conflict_once_for_name: StdMutex::new(None),
                on_conflict: StdMutex::new(None),
            })
        }

        /// Marks `name` as already running, as if some other (or earlier, same-
        /// process) call had already created and started it.
        fn mark_running(&self, name: &str) {
            self.state.lock().unwrap().running.insert(name.to_string());
        }

        /// The NEXT `create()` call for a spec named `name` fails with a typed
        /// [`RightsizeError::NameConflict`] instead of succeeding (armed once,
        /// consumed on that call), running `on_conflict` right before returning the
        /// error — the test's chance to simulate the concurrent winner's own
        /// side effects (marking itself running, writing the registry) landing
        /// right as this call loses the race.
        fn fail_next_create_with_conflict(
            &self,
            name: &str,
            on_conflict: impl FnMut() + Send + 'static,
        ) {
            *self.conflict_once_for_name.lock().unwrap() = Some(name.to_string());
            *self.on_conflict.lock().unwrap() = Some(Box::new(on_conflict));
        }
    }

    #[async_trait::async_trait]
    impl SandboxBackend for ReuseFakeBackend {
        fn name(&self) -> &str {
            "reuse-fake"
        }
        fn supports_native_networks(&self) -> bool {
            false
        }
        async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
            {
                let mut conflict = self.conflict_once_for_name.lock().unwrap();
                if conflict.as_deref() == Some(spec.name.as_str()) {
                    *conflict = None;
                    drop(conflict);
                    if let Some(cb) = self.on_conflict.lock().unwrap().as_mut() {
                        cb();
                    }
                    return Err(RightsizeError::NameConflict {
                        message: format!("sandbox '{}' already exists", spec.name),
                        source: None,
                    });
                }
            }
            self.state.lock().unwrap().created.push(spec.clone());
            Ok(Box::new(FakeHandle {
                id: spec.name.clone(),
                spec,
            }))
        }
        async fn start(&self, handle: &dyn SandboxHandle) -> Result<()> {
            let id = handle.id().to_string();
            let mut state = self.state.lock().unwrap();
            state.started.push(id.clone());
            state.running.insert(id);
            Ok(())
        }
        async fn stop(&self, handle: &dyn SandboxHandle) -> Result<()> {
            let id = handle.id().to_string();
            let mut state = self.state.lock().unwrap();
            state.stopped.push(id.clone());
            state.running.remove(&id);
            Ok(())
        }
        async fn remove(&self, handle: &dyn SandboxHandle) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .removed
                .push(handle.id().to_string());
            Ok(())
        }
        async fn exec(&self, _handle: &dyn SandboxHandle, cmd: &[String]) -> Result<ExecResult> {
            Ok(ExecResult {
                exit_code: 0,
                stdout: cmd.join(" "),
                stderr: String::new(),
            })
        }
        async fn logs(&self, _handle: &dyn SandboxHandle) -> Result<String> {
            Ok(String::new())
        }
        async fn follow_logs(
            &self,
            _handle: &dyn SandboxHandle,
            _consumer: Box<dyn Fn(String) + Send + Sync>,
        ) -> Result<crate::backend::FollowHandle> {
            unimplemented!("not exercised by the reuse test suite")
        }
        async fn ensure_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        async fn remove_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        fn cleanup_sync(&self, _container_id: &str) {}
        fn remove_by_name(&self, name: &str) {
            let mut state = self.state.lock().unwrap();
            state.removed_by_name.push(name.to_string());
            state.running.remove(name);
        }
        fn watchdog_kill_command(&self) -> Vec<String> {
            vec!["true".to_string()]
        }
        async fn find_running(
            &self,
            spec: &ContainerSpec,
        ) -> Result<Option<Box<dyn SandboxHandle>>> {
            let mut state = self.state.lock().unwrap();
            state.find_running_calls += 1;
            if state.running.contains(&spec.name) {
                Ok(Some(Box::new(FakeHandle {
                    id: spec.name.clone(),
                    spec: spec.clone(),
                })))
            } else {
                Ok(None)
            }
        }
    }

    fn sample_registry_entry(
        identity: &crate::reuse::Identity,
        host_port: u16,
    ) -> crate::reuse::RegistryEntry {
        crate::reuse::RegistryEntry {
            name: identity.name.clone(),
            image: "redis:7-alpine".to_string(),
            ports: std::collections::BTreeMap::from([("6379".to_string(), host_port)]),
            created_iso: "2025-01-01T00:00:00Z".to_string(),
            backend: "reuse-fake".to_string(),
        }
    }

    // Double opt-in: only the marker-AND-env-both-on combination produces a reuse
    // (`rz-reuse-<hash>`-named, `keep_alive`) sandbox; every other combination
    // behaves exactly like an ordinary ephemeral container.
    #[tokio::test]
    async fn reuse_double_opt_in_gating_all_four_combinations() {
        async fn is_reuse_name(marker: bool, env_enabled: bool) -> bool {
            let backend = ReuseFakeBackend::new();
            let cache_dir = temp_cache_dir("gating");
            let guard = Container::new("redis:7-alpine")
                .with_backend(backend)
                .with_cache_dir_override(cache_dir)
                .with_reuse_env_override(env_enabled)
                .with_exposed_ports(&[6379])
                .waiting_for(ReadyImmediately)
                .reuse(marker)
                .start()
                .await
                .expect("start must succeed regardless of the gating outcome");
            let reused = guard.name().starts_with("rz-reuse-");
            guard.stop().await.unwrap();
            reused
        }

        assert!(
            is_reuse_name(true, true).await,
            "marker on + env on must produce a reuse name"
        );
        assert!(
            !is_reuse_name(true, false).await,
            "marker on, env off: ordinary container"
        );
        assert!(
            !is_reuse_name(false, true).await,
            "env on, marker off: reuse never considered"
        );
        assert!(
            !is_reuse_name(false, false).await,
            "both off: ordinary container"
        );
    }

    // Adopt path: a registry hit whose sandbox the backend reports running, and
    // whose re-run wait strategy succeeds, adopts — no create() call, and the
    // guard's mapped port comes straight from the registry, not a fresh allocation.
    #[tokio::test]
    async fn adopt_path_registry_hit_running_and_wait_ok_skips_create_and_uses_registry_ports() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("adopt-hit");
        let identity = crate::reuse::compute_identity(
            "redis:7-alpine",
            &[],
            &None,
            &[6379],
            None,
            None,
            None,
            false,
            &[],
        )
        .unwrap();

        // A previous process already created, started, and registered this
        // sandbox, then exited cleanly (reuse containers are never torn down by
        // clean exit) — this process's first start() should adopt it.
        backend.mark_running(&identity.name);
        crate::reuse::Registry::new(&cache_dir, &identity.hash_hex)
            .write_atomic(&sample_registry_entry(&identity, 40321))
            .unwrap();

        let guard = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir)
            .with_reuse_env_override(true)
            .with_exposed_ports(&[6379])
            .waiting_for(ReadyImmediately)
            .reuse(true)
            .start()
            .await
            .expect("adopt must succeed");

        assert_eq!(guard.name(), identity.name);
        assert_eq!(
            guard.get_mapped_port(6379).unwrap(),
            40321,
            "must use the REGISTRY's port, not a freshly allocated one"
        );
        {
            let state = backend.state.lock().unwrap();
            assert!(
                state.created.is_empty(),
                "adopt must not call backend.create"
            );
            assert!(state.find_running_calls >= 1);
        }
        guard.stop().await.unwrap();
    }

    // Stale registry: the backend reports the recorded sandbox is NOT running ->
    // best-effort remove-by-name + delete the registry file, then fall through to a
    // fresh create that rewrites the registry.
    #[tokio::test]
    async fn stale_registry_not_running_removes_and_creates_fresh_and_rewrites_registry() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("adopt-stale");
        let identity = crate::reuse::compute_identity(
            "redis:7-alpine",
            &[],
            &None,
            &[6379],
            None,
            None,
            None,
            false,
            &[],
        )
        .unwrap();

        crate::reuse::Registry::new(&cache_dir, &identity.hash_hex)
            .write_atomic(&sample_registry_entry(&identity, 40321))
            .unwrap();
        // Deliberately NOT marked running: find_running must report `None`.

        let guard = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir.clone())
            .with_reuse_env_override(true)
            .with_exposed_ports(&[6379])
            .waiting_for(ReadyImmediately)
            .reuse(true)
            .start()
            .await
            .expect("a stale registry must fall back to a fresh create");

        assert_eq!(guard.name(), identity.name);
        {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.created.len(), 1, "must create exactly once");
            assert_eq!(
                state.removed_by_name,
                vec![identity.name.clone()],
                "the stale sandbox must be best-effort removed by name"
            );
        }

        let rewritten = crate::reuse::Registry::new(&cache_dir, &identity.hash_hex)
            .read()
            .expect("the registry must be rewritten after the fresh create");
        let new_port = *rewritten.ports.get("6379").unwrap();
        assert_eq!(guard.get_mapped_port(6379).unwrap(), new_port);

        guard.stop().await.unwrap();
    }

    // Corrupted registry JSON: unparseable, but still on disk — best-effort remove
    // the identity-derived name (the only name we can know without parsing the
    // file) and the file itself, then fall through to a fresh create.
    #[tokio::test]
    async fn corrupted_registry_json_falls_back_to_fresh_create() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("adopt-corrupt");
        let identity = crate::reuse::compute_identity(
            "redis:7-alpine",
            &[],
            &None,
            &[6379],
            None,
            None,
            None,
            false,
            &[],
        )
        .unwrap();

        let reuse_dir = cache_dir.join("reuse");
        std::fs::create_dir_all(&reuse_dir).unwrap();
        std::fs::write(
            reuse_dir.join(format!("{}.json", identity.hash_hex)),
            b"not json",
        )
        .unwrap();

        let guard = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir.clone())
            .with_reuse_env_override(true)
            .with_exposed_ports(&[6379])
            .waiting_for(ReadyImmediately)
            .reuse(true)
            .start()
            .await
            .expect("corrupt registry JSON must fall back to a fresh create, not fail start()");

        {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.created.len(), 1);
            assert_eq!(state.removed_by_name, vec![identity.name.clone()]);
        }
        assert!(
            crate::reuse::Registry::new(&cache_dir, &identity.hash_hex)
                .read()
                .is_some(),
            "a valid registry must exist after the fresh create"
        );

        guard.stop().await.unwrap();
    }

    // Stop semantics: stop() on a reuse-active guard clears only in-process
    // bookkeeping — no backend.stop/remove call, and the sandbox never appears in
    // the reaping ledger (never listed there in the first place).
    #[tokio::test]
    async fn stop_on_a_reuse_container_leaves_the_sandbox_running_and_never_touches_the_ledger() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("stop-semantics");

        let guard = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir.clone())
            .with_reuse_env_override(true)
            // Test isolation seam: see `restored_container_is_registered_in_the_
            // reaping_ledger_like_any_other`'s own comment. Shares the same scratch
            // dir as the reuse registry override above — the reuse registry lives
            // under `<dir>/reuse/`, the reaper ledger under `<dir>/runs/`, so the
            // two don't collide.
            .with_reaper_cache_dir_override(cache_dir.clone())
            .with_exposed_ports(&[6379])
            .waiting_for(ReadyImmediately)
            .reuse(true)
            .start()
            .await
            .unwrap();
        let name = guard.name().to_string();
        assert!(name.starts_with("rz-reuse-"));

        guard.stop().await.unwrap();

        {
            let state = backend.state.lock().unwrap();
            assert!(
                state.stopped.is_empty(),
                "stop() must not call backend.stop for a reuse container"
            );
            assert!(
                state.removed.is_empty(),
                "stop() must not call backend.remove for a reuse container"
            );
        }

        let ledger = crate::reaper::Ledger::new(&cache_dir, crate::RunId::value());
        assert!(
            !ledger.sandbox_names().contains(&name),
            "a reuse container must never appear in the reaping ledger, before or after stop()"
        );
    }

    // Reuse + a custom network is a typed, fail-fast error — never reaches create().
    #[tokio::test]
    async fn reuse_plus_custom_network_is_a_typed_error() {
        let backend = ReuseFakeBackend::new();
        let net = Arc::new(Network::new_network());
        let start_result = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_reuse_env_override(true)
            .with_network(&net)
            .reuse(true)
            .start()
            .await;
        let err = expect_start_err(start_result, "reuse + a custom network must fail fast");
        assert!(
            matches!(err, RightsizeError::ReuseNetworkConflict { .. }),
            "{err}"
        );
        assert!(
            backend.state.lock().unwrap().created.is_empty(),
            "must fail before any create() call"
        );
    }

    // Name collision on create (another process won the race): exactly one retry
    // back into the adopt path, using whatever the winner has by then written.
    #[tokio::test]
    async fn name_collision_on_create_retries_the_adopt_path_once() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("collision");
        let identity = crate::reuse::compute_identity(
            "redis:7-alpine",
            &[],
            &None,
            &[6379],
            None,
            None,
            None,
            false,
            &[],
        )
        .unwrap();

        // Deliberately NOT marked running yet: this call's own crash-mid-boot
        // orphan check (find_running, right before create) must see nothing and
        // must not remove anything — the concurrent winner only actually starts
        // (and registers) its sandbox at the exact moment THIS call's create()
        // loses the race, simulated below inside `on_conflict`, not before it.
        let entry = sample_registry_entry(&identity, 41111);
        let winner_cache_dir = cache_dir.clone();
        let winner_hash = identity.hash_hex.clone();
        let winner_backend = backend.clone();
        let winner_name = identity.name.clone();
        backend.fail_next_create_with_conflict(&identity.name, move || {
            // Simulate the concurrent winner's own create+start landing (marking
            // itself running) and its registry write, right as this call loses
            // the create race.
            winner_backend.mark_running(&winner_name);
            let _ =
                crate::reuse::Registry::new(&winner_cache_dir, &winner_hash).write_atomic(&entry);
        });

        let guard = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir)
            .with_reuse_env_override(true)
            .with_exposed_ports(&[6379])
            .waiting_for(ReadyImmediately)
            .reuse(true)
            .start()
            .await
            .expect("a name collision must retry the adopt path and succeed");

        assert_eq!(guard.name(), identity.name);
        assert_eq!(guard.get_mapped_port(6379).unwrap(), 41111);
        assert_eq!(
            backend.state.lock().unwrap().created.len(),
            0,
            "the losing create() attempt must not count as a successful create"
        );

        guard.stop().await.unwrap();
    }

    // ======================================================================
    // Crash-mid-boot orphan recovery (fresh-create's own find_running/remove
    // check, run once the adopt path has already concluded there is no usable
    // registry entry) — see docs/reuse.md's own section on this.
    // ======================================================================

    // (a) A sandbox is running under the identity's fixed name, but NO registry
    // entry points at it at all — exactly what a process that crashed (or failed
    // its own wait strategy) between `create` and the registry write leaves
    // behind. The next start() for the same identity must find it via
    // find_running and best-effort remove it BEFORE attempting a fresh create.
    #[tokio::test]
    async fn fresh_create_removes_a_running_but_unregistered_orphan_before_creating() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("orphan-recovery");
        let identity = crate::reuse::compute_identity(
            "redis:7-alpine",
            &[],
            &None,
            &[6379],
            None,
            None,
            None,
            false,
            &[],
        )
        .unwrap();

        // No registry file at all — but a sandbox under the identity's name is
        // already running, exactly as a crash-mid-boot orphan would leave it.
        backend.mark_running(&identity.name);

        let guard = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir)
            .with_reuse_env_override(true)
            .with_exposed_ports(&[6379])
            .waiting_for(ReadyImmediately)
            .reuse(true)
            .start()
            .await
            .expect("an orphaned running sandbox must not fail start(), just be replaced");

        assert_eq!(guard.name(), identity.name);
        {
            let state = backend.state.lock().unwrap();
            assert_eq!(
                state.removed_by_name,
                vec![identity.name.clone()],
                "the orphaned sandbox must be best-effort removed before the fresh create"
            );
            assert_eq!(state.created.len(), 1, "must still create exactly once");
            assert!(state.find_running_calls >= 1);
        }

        guard.stop().await.unwrap();
    }

    // (b) A registry entry IS present and verifies (adopt succeeds) — the
    // orphan-recovery find_running/remove step must never even run: adopt
    // short-circuits before `start_reuse` ever reaches it, so no remove_by_name
    // call happens.
    #[tokio::test]
    async fn adopt_with_a_verified_registry_never_calls_remove_by_name() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("orphan-recovery-adopt");
        let identity = crate::reuse::compute_identity(
            "redis:7-alpine",
            &[],
            &None,
            &[6379],
            None,
            None,
            None,
            false,
            &[],
        )
        .unwrap();

        backend.mark_running(&identity.name);
        crate::reuse::Registry::new(&cache_dir, &identity.hash_hex)
            .write_atomic(&sample_registry_entry(&identity, 40321))
            .unwrap();

        let guard = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir)
            .with_reuse_env_override(true)
            .with_exposed_ports(&[6379])
            .waiting_for(ReadyImmediately)
            .reuse(true)
            .start()
            .await
            .expect("a verified registry entry must adopt");

        assert_eq!(guard.name(), identity.name);
        assert!(
            backend.state.lock().unwrap().removed_by_name.is_empty(),
            "adopting a verified registry entry must never call remove_by_name"
        );

        guard.stop().await.unwrap();
    }

    // (c) No registry AND nothing running under the identity's name —
    // find_running reports None, so remove_by_name must never be called; the
    // fresh create proceeds exactly as it always has for a genuinely first-time
    // identity.
    #[tokio::test]
    async fn fresh_create_with_nothing_running_never_calls_remove_by_name() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("orphan-recovery-clean");

        let guard = Container::new("redis:7-alpine")
            .with_backend(backend.clone())
            .with_cache_dir_override(cache_dir)
            .with_reuse_env_override(true)
            .with_exposed_ports(&[6379])
            .waiting_for(ReadyImmediately)
            .reuse(true)
            .start()
            .await
            .expect("a genuinely fresh identity must create normally");

        {
            let state = backend.state.lock().unwrap();
            assert!(
                state.removed_by_name.is_empty(),
                "nothing was running, so remove_by_name must never be called"
            );
            assert!(
                state.find_running_calls >= 1,
                "the orphan-recovery check must still run"
            );
            assert_eq!(state.created.len(), 1);
        }

        guard.stop().await.unwrap();
    }

    // Validation-bypass fix (reuse path): the reuse fresh-create path builds and
    // applies its own spec-customizer independently of `create_started_container`
    // (`create_and_start_reuse_sandbox`), so it needs the SAME post-customizer
    // re-validation — a customizer setting both root-disk fields must be refused
    // before `backend.create` is ever reached, exactly like the ordinary
    // (non-reuse) start path.
    #[tokio::test]
    async fn reuse_fresh_create_re_validates_a_spec_customizer_setting_both_root_disk_fields() {
        let backend = ReuseFakeBackend::new();
        let cache_dir = temp_cache_dir("reuse-fresh-create-validation-bypass");

        let err = expect_start_err(
            Container::new("redis:7-alpine")
                .with_backend(backend.clone())
                .with_cache_dir_override(cache_dir)
                .with_reuse_env_override(true)
                .with_exposed_ports(&[6379])
                .waiting_for(ReadyImmediately)
                .reuse(true)
                .with_spec_customizer(|mut spec, _mapped| {
                    spec.disk_limit_mb = Some(512);
                    spec.tmpfs_root_mb = Some(256);
                    spec
                })
                .start()
                .await,
            "a customizer setting both root-disk fields must be refused",
        );

        assert!(matches!(err, RightsizeError::RootDiskConflict), "{err}");
        assert!(
            backend.state.lock().unwrap().created.is_empty(),
            "backend.create must never be reached once the post-customizer re-validation refuses"
        );
    }
}
