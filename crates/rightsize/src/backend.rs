//! The entire contract a runtime must satisfy to back rightsize containers — deliberately
//! tiny, because two runtimes as different as Docker and a microVM-over-CLI both live
//! behind it. Docker maps most calls straight to the daemon API; the microsandbox backend
//! drives the `msb` CLI and *emulates* the parts a microVM lacks — most notably
//! networking, which it fakes with per-link exec-stream tunnels (see
//! [`SandboxBackend::install_network_links`], a no-op default for backends with real
//! networks).
//!
//! **The core invariant:** host ports arrive already chosen in
//! [`crate::model::ContainerSpec::ports`] — a backend binds them, it never allocates.
//! That one rule is what lets a module advertise its own mapped port at boot, and no
//! backend implementation in this workspace may call the free-port allocator.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::error::{Result, RightsizeError};
use crate::model::{ContainerSpec, ExecResult};

/// Capability flags a backend exposes about its own runtime — a small, growable
/// struct rather than a single boolean (mirroring the existing
/// [`SandboxBackend::supports_native_networks`] precedent, but deliberately NOT
/// folded into it — that flag stays exactly as it is) so a later wave can add a
/// field without another SPI break. Fields so far:
///
/// - `hardware_isolated`: `true` when each sandbox this backend creates gets its own
///   kernel (a microVM, e.g. microsandbox) rather than sharing the host's (e.g.
///   Docker, whose containers are namespaces/cgroups on one shared kernel). Consulted
///   by [`SandboxBackend::capabilities`]'s caller in `Container::start()` for
///   `.require_isolation(true)`.
/// - `checkpoint`: whether this backend supports checkpoint/restore of a running
///   sandbox. Both real backends have it: docker via image commit, microsandbox via
///   disk snapshots.
/// - `checkpoint_restarts_workload`: `true` when taking a checkpoint on this backend
///   necessarily restarts the sandbox's workload (microsandbox: the stop/snapshot/
///   start cycle reboots the guest) rather than leaving it undisturbed (docker: an
///   image commit touches nothing running). `ContainerGuard::checkpoint` consults
///   this to decide whether to re-run the configured wait strategy before returning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Capabilities {
    /// True for a backend whose sandboxes each run under their own kernel.
    pub hardware_isolated: bool,
    /// True for a backend that supports checkpoint/restore.
    pub checkpoint: bool,
    /// True for a backend whose checkpoint mechanism restarts the sandbox's
    /// workload (docker: `false`; microsandbox: `true`).
    pub checkpoint_restarts_workload: bool,
}

/// A tunnel/alias route: inside the consumer container, `alias:guest_port` must reach
/// `127.0.0.1:target_host_port` on the host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkLink {
    /// The DNS-style name the consumer looks up.
    pub alias: String,
    /// The port the consumer connects to on `alias`.
    pub guest_port: u16,
    /// The host-side port that traffic actually lands on.
    pub target_host_port: u16,
}

/// A backend-native opaque container reference. `id` is whatever the backend's own
/// runtime calls it (a microsandbox sandbox name, a Docker container id); `spec` is what
/// created it, kept around so later calls (e.g. building an advertised-listener rewrite)
/// don't need to be threaded through separately.
pub trait SandboxHandle: Send + Sync {
    /// The backend-native container id/name.
    fn id(&self) -> &str;
    /// The spec this handle was created from.
    fn spec(&self) -> &ContainerSpec;
}

/// The entire contract a runtime must satisfy to back rightsize containers. See the
/// module-level docs for the shape of the two backends that implement it.
#[async_trait::async_trait]
pub trait SandboxBackend: Send + Sync {
    /// The backend's human id, e.g. `"docker"` / `"microsandbox"` — shown in errors and
    /// matched against `RIGHTSIZE_BACKEND`.
    fn name(&self) -> &str;
    /// True when the runtime has real container networks; false means network links are
    /// emulated (see [`SandboxBackend::install_network_links`]).
    fn supports_native_networks(&self) -> bool;
    /// This backend's capability flags (see [`Capabilities`]). Default: neither
    /// hardware-isolated nor checkpoint-capable — the conservative answer for a test
    /// double or any backend that hasn't opted into either; both real backends (msb,
    /// docker) override this with their real values.
    fn capabilities(&self) -> Capabilities {
        Capabilities::default()
    }
    /// Creates (but does not start) a container from `spec`. `spec.ports` are already
    /// chosen — this call binds them, it never allocates.
    async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>>;
    /// Starts a container previously returned by [`SandboxBackend::create`].
    async fn start(&self, handle: &dyn SandboxHandle) -> Result<()>;
    /// Stops a running container. Safe to call on an already-stopped one.
    async fn stop(&self, handle: &dyn SandboxHandle) -> Result<()>;
    /// Removes a stopped container's resources.
    async fn remove(&self, handle: &dyn SandboxHandle) -> Result<()>;
    /// Runs `cmd` inside the running container and returns its exit code plus captured
    /// output.
    async fn exec(&self, handle: &dyn SandboxHandle, cmd: &[String]) -> Result<ExecResult>;
    /// The container's full captured logs so far.
    async fn logs(&self, handle: &dyn SandboxHandle) -> Result<String>;
    /// Streams log lines to `consumer` as they arrive.
    ///
    /// Returns a [`FollowHandle`]; dropping or closing it halts delivery per its
    /// "stop delivery, never flush" contract (see the exactly-once contract, Task
    /// 2.3/3.3).
    async fn follow_logs(
        &self,
        handle: &dyn SandboxHandle,
        consumer: Box<dyn Fn(String) + Send + Sync>,
    ) -> Result<FollowHandle>;
    /// Creates the named network if the runtime needs an explicit create step; no-op
    /// otherwise.
    async fn ensure_network(&self, network_id: &str) -> Result<()>;
    /// Removes the named network; no-op for runtimes with nothing to remove.
    async fn remove_network(&self, network_id: &str) -> Result<()>;
    /// Called after `start`, before the wait strategy runs, to make `links` reachable
    /// from `handle` by alias.
    ///
    /// Default no-op: Docker relies on native networks, which already resolve aliases.
    /// Only an emulating backend (microsandbox) overrides this — and should fail fast
    /// with an actionable [`crate::error::RightsizeError::UnsupportedByBackend`] for anything it
    /// cannot support (e.g. a consumer image missing a required tool) rather than
    /// silently no-op.
    async fn install_network_links(
        &self,
        _handle: &dyn SandboxHandle,
        _links: &[NetworkLink],
    ) -> Result<()> {
        Ok(())
    }
    /// Best-effort teardown of backend-owned resources (client sockets, etc.), called
    /// once when the backend itself is being retired — not per-container.
    async fn close(&self) -> Result<()> {
        Ok(())
    }
    /// SYNCHRONOUS SIGKILL-safe teardown for the `Drop`-path cleanup thread (see
    /// `crate::cleanup`). Given a container id/name, tears it down with blocking std I/O
    /// only — never Tokio, never `block_on` — because this runs on a dedicated OS thread
    /// with no async runtime in context. Called only from that thread, never from async
    /// code.
    fn cleanup_sync(&self, container_id: &str);

    /// Best-effort removal of a sandbox identified by NAME rather than the
    /// backend-native id a [`SandboxHandle`] carries — the shape the reaping ledger
    /// needs, since it persists names (`ContainerSpec::name`), never ids (see
    /// `crate::reaper`). "Not found" is expected and must be silently ignored —
    /// sweeps are idempotent and may race another process's sweep of the same
    /// leftover.
    ///
    /// SYNCHRONOUS and blocking-std-I/O-only, exactly like [`Self::cleanup_sync`]:
    /// the init-time sweep runs from `crate::backends::active()`'s resolution path,
    /// which must work with no Tokio runtime guaranteed to be in context (see that
    /// function's own doc) — an `async fn` here would have nowhere safe to `.await`
    /// from that call site.
    fn remove_by_name(&self, name: &str);

    /// The external, backend-CLI-only command (program + fixed args, in argv order)
    /// the reaping watchdog uses to best-effort remove a sandbox by name AFTER this
    /// library process has already exited — invoked by a detached script as
    /// `<word>... <sandbox-name>` (the name is appended as the final argument at call
    /// time). Must be self-sufficient: the watchdog is a standalone process with no
    /// access to anything living only inside this process (no in-memory client, no
    /// open socket this process owned) — see `crate::reaper::watchdog`.
    fn watchdog_kill_command(&self) -> Vec<String>;

    /// Same shape as [`Self::watchdog_kill_command`], for removing a network by id.
    /// Default empty — "nothing to do" — for backends with no real network resource
    /// to remove externally (e.g. microsandbox, whose networking is emulated and
    /// creates nothing at the backend level).
    fn watchdog_network_kill_command(&self) -> Vec<String> {
        Vec::new()
    }

    /// The absolute path to this backend's own provisioned/installed binary, when it
    /// has one — recorded in the reaping ledger's run record (`msbPath` in the
    /// cross-language JSON schema) so a same-language sweep or diagnostic can
    /// identify exactly which toolchain a dead run used. Default `None` for backends
    /// with no such on-disk binary (docker, which talks to a daemon socket).
    fn backend_binary_path(&self) -> Option<std::path::PathBuf> {
        None
    }

    /// Best-effort check for whether a sandbox named `spec.name` is already
    /// running — the reuse adopt path's own query (`crate::reuse`), distinct from
    /// [`Self::remove_by_name`]'s "make it gone" contract: this one answers "is it
    /// there right now, and if so, hand me a fresh [`SandboxHandle`] for it."
    /// Constructs the handle around `spec` (cloned), never around whatever this
    /// backend instance may or may not already have cached for that name — the
    /// whole point of reuse is adopting a sandbox a DIFFERENT process (or an
    /// earlier `Container` in this one) created, so the handle must not depend on
    /// in-process bookkeeping this call may know nothing about.
    ///
    /// `Ok(None)` for "not running" (including "no such sandbox at all") and for
    /// any backend query failure that isn't worth failing the whole adopt attempt
    /// over — the reuse start flow treats both identically: fall back to a fresh
    /// create. Default `Ok(None)` — "nothing to adopt" — is a safe fallback for any
    /// backend (real or test double) that hasn't implemented real support; both
    /// real backends (msb, docker) override this with a real query.
    async fn find_running(&self, _spec: &ContainerSpec) -> Result<Option<Box<dyn SandboxHandle>>> {
        Ok(None)
    }

    /// Captures `handle`'s current filesystem state as a checkpoint, formatting
    /// `nonce` (a random 12-lowercase-hex string freshly generated per call by
    /// `crate::checkpoint::generate_ref_nonce`) into this backend's own ref shape
    /// and returning the resulting ref — the checkpoint feature's own backend
    /// primitive (`ContainerGuard::checkpoint`, `crate::checkpoint`). `handle` must
    /// currently be running.
    ///
    /// Each backend picks its own ref shape: docker tags an image
    /// `rightsize/checkpoint:<nonce>` (the container is left undisturbed);
    /// microsandbox's checkpoint artifact is an ABSOLUTE PATH,
    /// `<cache dir>/checkpoints/rz-ckpt-<nonce>`, not a bare snapshot name — the
    /// caller (`ContainerGuard::checkpoint`/`checkpoint_named`) mints that full
    /// path up front, against the same cache-dir override its checkpoint registry
    /// honors, and hands it down as `nonce` here — and the sandbox is stopped,
    /// snapshotted at that path, and started back up — see
    /// [`Capabilities::checkpoint_restarts_workload`].
    ///
    /// Gated by [`Capabilities::checkpoint`] at the CALLER (`ContainerGuard::checkpoint`
    /// checks `capabilities().checkpoint` before ever reaching this method) — this
    /// default implementation is the defensive fallback for a backend that hasn't
    /// opted in (or a test double): it always errors with
    /// [`crate::error::RightsizeError::CheckpointUnsupported`], never called in
    /// practice because of the caller-side gate, but correct on its own if it ever
    /// were.
    async fn create_checkpoint(&self, _handle: &dyn SandboxHandle, _nonce: &str) -> Result<String> {
        Err(crate::error::RightsizeError::CheckpointUnsupported {
            backend: self.name().to_string(),
        })
    }

    /// Best-effort removal of a checkpoint this backend created (see
    /// [`Self::create_checkpoint`]): docker `DELETE`s the tagged image; microsandbox
    /// runs `msb snapshot rm`. "Not found" is success, matching
    /// [`Self::remove_by_name`]'s own contract.
    ///
    /// SPI-only — no [`crate::ContainerGuard`] method exposes this; the public API
    /// documents a `docker rmi`/`msb snapshot rm` one-liner instead (checkpoint
    /// artifacts are never auto-reaped, same explicit decision as reuse). This
    /// exists so this crate's own tests can clean up a checkpoint they made without
    /// shelling out to either CLI directly. Default: no-op — a test double that
    /// never creates a checkpoint has nothing to remove.
    async fn remove_checkpoint(&self, _checkpoint_ref: &str) -> Result<()> {
        Ok(())
    }

    /// Probes whether the backend-native artifact behind `checkpoint_ref` still
    /// exists — the named-checkpoint feature's own primitive
    /// (`crate::Checkpoint::find`'s staleness check): docker inspects the tagged
    /// image (`GET /images/{ref}/json`), microsandbox inspects the disk snapshot
    /// (`msb snapshot inspect <ref>`'s exit code).
    ///
    /// **Only a definite "not there" may return `Ok(false)`.** Any other failure
    /// (a daemon unreachable, a malformed ref, a transient probe error) must
    /// propagate as `Err` — a probe failure masquerading as "absent" would let
    /// `find` silently delete a registry entry for a checkpoint that in fact
    /// still exists, just because this call couldn't currently reach it.
    ///
    /// Default: like [`Self::copy_to_container`]'s default, this is
    /// unsupported for a backend that hasn't opted in (test doubles only — both
    /// real backends override it).
    async fn has_checkpoint(&self, _checkpoint_ref: &str) -> Result<bool> {
        Err(RightsizeError::unsupported(
            "checkpoint existence probe",
            self.name(),
        ))
    }

    /// Writes this backend's own checkpoint payload for `checkpoint_ref` to
    /// `dest_file` — the checkpoint-archive feature's export primitive
    /// (`crate::Checkpoint::export_to`): msb runs `snapshot export <ref> <dest>`
    /// (never `--with-image` — its import fails an integrity check in 0.6.6, so
    /// the destination machine pulls the image fresh on first boot instead);
    /// docker runs `docker save -o <dest> <ref>`. `dest_file`'s parent directory
    /// already exists by the time this is called (the generic layer creates a
    /// fresh temp staging directory before calling this); this method writes only
    /// the one file.
    ///
    /// Default: unsupported (test doubles/fakes don't need this); both real
    /// backends override it.
    async fn export_checkpoint(&self, _checkpoint_ref: &str, _dest_file: &Path) -> Result<()> {
        Err(RightsizeError::unsupported(
            "checkpoint export",
            self.name(),
        ))
    }

    /// Materializes a checkpoint archive's payload (`src_file`, extracted from the
    /// archive by the generic layer) onto this backend, returning the EFFECTIVE
    /// ref the imported artifact is now reachable under — the checkpoint-archive
    /// feature's import primitive (`crate::Checkpoint::import_from`). `ref_hint`
    /// is the ref the archive's manifest recorded at export time.
    ///
    /// The two backends resolve the effective ref very differently: docker's
    /// `docker load -i <src_file>` preserves the tag baked into the save file, so
    /// its effective ref is simply `ref_hint` unchanged. Microsandbox's `msb
    /// snapshot import <src_file>` is content-addressed — it unpacks under a
    /// digest-derived directory name that has nothing to do with `ref_hint`, and
    /// re-importing an archive whose digest already exists on this machine fails
    /// with "snapshot already exists", which this method treats as success (the
    /// artifact is already there) rather than an error — so its effective ref is
    /// that digest-dir name (confirmed present via `msb snapshot list --format
    /// json`), never `ref_hint` and never the full `sha256:<64hex>` digest, which
    /// msb does not accept as a snapshot ref.
    ///
    /// Default: unsupported (test doubles/fakes don't need this); both real
    /// backends override it.
    async fn import_checkpoint(&self, _src_file: &Path, _ref_hint: &str) -> Result<String> {
        Err(RightsizeError::unsupported(
            "checkpoint import",
            self.name(),
        ))
    }

    /// Copies `host_path` (a file or directory) into the running container at
    /// `container_path` — the RUNTIME counterpart to a start-time
    /// [`crate::model::FileMount`] (`Container::with_copy_file_to_container`, a
    /// pre-boot mount). Called only after the generic layer
    /// (`ContainerGuard::copy_file_to_container`) has already verified the
    /// container is running, `container_path` is absolute, and the destination's
    /// parent directory exists in the guest (via [`Self::exec`]) — this method does
    /// ONLY the transfer, exactly like `cp -r`/`docker cp` themselves: copying a
    /// directory to an absent destination produces the destination as a copy of
    /// the source (contents under the destination, not nested one level deeper).
    ///
    /// Default: unsupported (test doubles/fakes don't need this); both real
    /// backends override it.
    async fn copy_to_container(
        &self,
        _handle: &dyn SandboxHandle,
        _host_path: &Path,
        _container_path: &str,
    ) -> Result<()> {
        Err(RightsizeError::unsupported(
            "runtime file copy",
            self.name(),
        ))
    }

    /// The reverse direction of [`Self::copy_to_container`]: copies `container_path`
    /// (a file or directory) out of the running container to `host_path`. Same
    /// division of labor — the generic layer has already verified running/absolute/
    /// host-parent-directory-exists before this is called; this method does ONLY
    /// the transfer.
    ///
    /// Default: unsupported (test doubles/fakes don't need this); both real
    /// backends override it.
    async fn copy_from_container(
        &self,
        _handle: &dyn SandboxHandle,
        _container_path: &str,
        _host_path: &Path,
    ) -> Result<()> {
        Err(RightsizeError::unsupported(
            "runtime file copy",
            self.name(),
        ))
    }
}

/// A discoverable factory for a [`SandboxBackend`]. Each backend crate ships one (e.g.
/// `MsbBackendProvider`, `DockerBackendProvider`); [`crate::backends::resolve`] picks
/// among the providers it's given by `priority`, honoring `RIGHTSIZE_BACKEND` when set.
pub trait BackendProvider: Send + Sync {
    /// The backend's human id, matched case-insensitively against `RIGHTSIZE_BACKEND`.
    fn name(&self) -> &str;
    /// Higher wins when auto-selecting; msb's 20 outranks docker's 10 so a microVM-
    /// capable host prefers the microVM.
    fn priority(&self) -> u32;
    /// True if this backend's runtime preconditions are met on the current host (e.g.
    /// KVM available, a Docker daemon reachable).
    fn is_supported(&self) -> bool;
    /// A human-readable reason [`BackendProvider::is_supported`] is false, shown when
    /// this backend was explicitly requested and unavailable, or when no provider on the
    /// list is supported.
    fn unsupported_reason(&self) -> String;
    /// Instantiates the backend. Only called after `is_supported` is confirmed true.
    fn create(&self) -> Result<Box<dyn SandboxBackend>>;
}

/// A backend-appropriate join handle a [`FollowHandle`] must wait on when it closes —
/// kept as a small enum (rather than a trait object) so the type stays uniform across
/// backends without needing `dyn Any` downcasting.
enum JoinTarget {
    /// A blocking OS thread (microsandbox's reader/watchdog threads).
    Thread(Option<std::thread::JoinHandle<()>>),
    /// A Tokio task (Docker's streaming task).
    Task(Option<tokio::task::JoinHandle<()>>),
}

impl JoinTarget {
    /// **Why `Task` gets `abort()` and not a plain `.await`, and why that alone isn't
    /// the "no post-close callback" guarantee:** `abort()` is
    /// cooperative-cancellation-at-the-next-`.await`, not
    /// immediate — a task already past its last await point (e.g. mid-way through
    /// building this delivery's line before calling the consumer) keeps running until
    /// it reaches one. Relying on `abort()` alone would leave a window where `close()`
    /// has returned but the aborted task could still invoke the consumer one more time
    /// with a line it had already queued up.
    ///
    /// The actual guarantee is cooperative on the task's own side: every backend that
    /// builds a `Task`-backed `FollowHandle` (docker's `follow_logs`) must
    /// check the same `close_requested` flag between deliveries and stop delivering the
    /// instant it's set — *before* invoking the consumer for the next line, not after.
    /// `close()`/`Drop` set that flag first, then call this; by the time this reaches a
    /// task still spinning through buffered lines, it observes the flag and returns
    /// without another callback. `abort()` here is the backstop for the other failure
    /// mode — a task genuinely stuck blocked in I/O (a stalled socket read) that would
    /// otherwise never reach its next flag check — not the primary mechanism.
    fn join_best_effort(&mut self) {
        match self {
            JoinTarget::Thread(h) => {
                if let Some(h) = h.take() {
                    let _ = h.join();
                }
            }
            JoinTarget::Task(h) => {
                if let Some(h) = h.take() {
                    h.abort();
                }
            }
        }
    }
}

/// Owns the close mechanism for a live `follow_logs`/`follow_output` stream.
///
/// Semantics: **stop delivery, never flush** — closing a follow stream must never
/// trigger a replay of buffered-but-undelivered lines. (A backend's own watchdog may
/// still do an at-most-once flush on the *workload exiting*, per its own contract — but
/// an explicit `close()`/`Drop` here never triggers one.)
///
/// **Task-backed joiners (docker's shape, [`FollowHandle::from_task`]) carry an extra
/// obligation `Thread`-backed ones don't:** `Task`'s join is `abort()`, which is
/// cooperative-cancellation-at-the-next-`.await`, not immediate (see
/// `JoinTarget::join_best_effort`'s doc for the full reasoning). So "close halts
/// delivery" for a task-backed stream is only guaranteed if the task *itself* also
/// checks `close_requested` between deliveries and stops before the next consumer
/// call — the flag and the abort jointly rule out a callback firing after `close()`
/// returns; neither alone does.
pub struct FollowHandle {
    close_requested: Arc<AtomicBool>,
    joiners: Vec<JoinTarget>,
}

impl FollowHandle {
    /// Builds a `FollowHandle` from the shared close flag and the backend-owned join
    /// handles that must be waited on when this handle closes. Backend-internal — the
    /// two constructors below are the public surface backends use.
    fn new(close_requested: Arc<AtomicBool>, joiners: Vec<JoinTarget>) -> Self {
        Self {
            close_requested,
            joiners,
        }
    }

    /// Constructs a `FollowHandle` backed by `std::thread` join handles (microsandbox's
    /// shape: reader + watchdog threads).
    pub fn from_threads(
        close_requested: Arc<AtomicBool>,
        threads: Vec<std::thread::JoinHandle<()>>,
    ) -> Self {
        Self::new(
            close_requested,
            threads
                .into_iter()
                .map(|h| JoinTarget::Thread(Some(h)))
                .collect(),
        )
    }

    /// Constructs a `FollowHandle` backed by a Tokio task handle (Docker's shape: one
    /// streaming task, no watchdog needed since its stream ends cleanly).
    ///
    /// **Contract on `task`:** it must poll the same `close_requested` given here
    /// between deliveries and return without calling the consumer again once it's set
    /// — see this struct's doc and `JoinTarget::join_best_effort`'s doc for why `abort()`
    /// alone can't make that guarantee on its own.
    pub fn from_task(close_requested: Arc<AtomicBool>, task: tokio::task::JoinHandle<()>) -> Self {
        Self::new(close_requested, vec![JoinTarget::Task(Some(task))])
    }

    /// Signals `close_requested`, joins every backend join handle, and returns. Never
    /// triggers a tail replay/flush — "stop delivery, never flush".
    pub fn close(mut self) {
        self.close_requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        for j in &mut self.joiners {
            j.join_best_effort();
        }
    }
}

impl Drop for FollowHandle {
    fn drop(&mut self) {
        // Best-effort equivalent of close(): set close_requested and join what we can
        // synchronously. Must not panic; must not flush.
        self.close_requested
            .store(true, std::sync::atomic::Ordering::SeqCst);
        for j in &mut self.joiners {
            j.join_best_effort();
        }
    }
}

// Cross-task alias, defined here since it's shared by wait strategies and the
// container builder's post-start hook.
pub use crate::futures::BoxFuture;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn follow_handle_close_sets_close_requested_and_joins_threads() {
        let flag = Arc::new(AtomicBool::new(false));
        let joined = Arc::new(AtomicBool::new(false));
        let joined_clone = joined.clone();
        let handle = std::thread::spawn(move || {
            while !joined_clone.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });
        let fh = FollowHandle::from_threads(flag.clone(), vec![handle]);
        joined.store(true, Ordering::SeqCst);
        fh.close();
        assert!(flag.load(Ordering::SeqCst));
    }

    #[test]
    fn follow_handle_drop_also_sets_close_requested() {
        let flag = Arc::new(AtomicBool::new(false));
        {
            let handle = std::thread::spawn(|| {});
            let _fh = FollowHandle::from_threads(flag.clone(), vec![handle]);
        }
        assert!(flag.load(Ordering::SeqCst));
    }
}
