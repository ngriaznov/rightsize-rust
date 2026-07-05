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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::error::Result;
use crate::model::{ContainerSpec, ExecResult};

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
