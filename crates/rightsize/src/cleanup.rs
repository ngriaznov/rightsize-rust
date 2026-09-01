//! The `Drop`-path fallback half of the two-tier cleanup story (see `crate::container`'s
//! module docs for the full rationale).
//!
//! `ContainerGuard::stop()` is the happy path: an explicit, awaited, ordered async
//! teardown. But a guard can also simply be dropped — a test panics, a scope exits
//! early, the process is mid-`SIGKILL`. `Drop` cannot be `async`, must never panic, and
//! must work with **no Tokio runtime in context** (a plain `Drop` can run during a
//! runtime shutdown, or in a thread that never had one). So the fallback path does the
//! least amount of synchronous work that's still correct: send a small, `'static`
//! teardown descriptor to a dedicated background OS thread and return immediately. That
//! thread — started once per process — drains the descriptors and tears each one down
//! with **blocking std I/O only**: `std::process::Command` for msb, a blocking unix
//! domain socket (unix) or named pipe (Windows) for docker. Never Tokio, never `block_on`, because
//! this thread has no async runtime and must not assume one exists anywhere in the
//! process either.
//!
//! This is a crash-tolerant design by necessity: a *bare* `SIGKILL` skips `Drop`
//! entirely (nothing running in userland survives that), which is why the
//! msb/docker backends *also* run a run-id-scoped reaper at startup — this thread is
//! the graceful-shutdown backstop, the reaper is the backstop for the backstop.

use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, OnceLock};

use crate::backend::SandboxBackend;

/// One unit of work for the cleanup thread: tear down `container_id` on `backend`,
/// using only blocking std I/O.
pub(crate) struct CleanupJob {
    pub(crate) backend: Arc<dyn SandboxBackend>,
    pub(crate) container_id: String,
    /// Runs on the cleanup thread immediately after `cleanup_sync` returns —
    /// `Drop`'s own opportunity to update the reaping ledger (see
    /// `crate::reaper::after_stop`) once the teardown this job represents has
    /// actually been attempted. Kept as a generic callback rather than importing
    /// `crate::reaper` directly into this module, so this thread's own contract
    /// ("blocking std I/O only, must never panic") stays independent of whatever the
    /// callback happens to do; `None` for callers with nothing to run afterward.
    pub(crate) after_teardown: Option<Box<dyn FnOnce() + Send>>,
}

static SENDER: OnceLock<Sender<CleanupJob>> = OnceLock::new();

/// Starts the dedicated cleanup thread on first use and returns a sender to it. Safe to
/// call repeatedly — subsequent calls just return the cached sender.
fn sender() -> &'static Sender<CleanupJob> {
    SENDER.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<CleanupJob>();
        std::thread::Builder::new()
            .name("rightsize-cleanup".to_string())
            .spawn(move || {
                for job in rx {
                    // Blocking std I/O only — never Tokio, never block_on. A panic here
                    // would take down the whole cleanup thread for the rest of the
                    // process, so this join is deliberately isolated per job.
                    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        job.backend.cleanup_sync(&job.container_id);
                    }));
                    // Runs AFTER cleanup_sync has been attempted, never before — a
                    // caller wiring the reaping ledger's `after_stop` into this must
                    // see the teardown attempt happen first. Isolated the same way,
                    // for the same reason.
                    if let Some(after_teardown) = job.after_teardown {
                        let _ =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(after_teardown));
                    }
                }
            })
            .expect("failed to start the rightsize cleanup thread");
        tx
    })
}

/// Enqueues a teardown job from a `Drop` impl. Non-blocking: if the cleanup thread's
/// receiver is somehow gone (e.g. torn down during process exit), this just drops the
/// job silently rather than panicking — `Drop` must never panic.
pub(crate) fn enqueue(job: CleanupJob) {
    let _ = sender().send(job);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContainerSpec, ExecResult};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct RecordingBackend {
        cleaned: Arc<Mutex<Vec<String>>>,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SandboxBackend for RecordingBackend {
        fn name(&self) -> &str {
            "recording"
        }
        fn supports_native_networks(&self) -> bool {
            false
        }
        async fn create(
            &self,
            _spec: ContainerSpec,
        ) -> crate::error::Result<Box<dyn crate::backend::SandboxHandle>> {
            unimplemented!()
        }
        async fn start(
            &self,
            _handle: &dyn crate::backend::SandboxHandle,
        ) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn stop(
            &self,
            _handle: &dyn crate::backend::SandboxHandle,
        ) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn remove(
            &self,
            _handle: &dyn crate::backend::SandboxHandle,
        ) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn exec(
            &self,
            _handle: &dyn crate::backend::SandboxHandle,
            _cmd: &[String],
        ) -> crate::error::Result<ExecResult> {
            unimplemented!()
        }
        async fn logs(
            &self,
            _handle: &dyn crate::backend::SandboxHandle,
        ) -> crate::error::Result<String> {
            unimplemented!()
        }
        async fn follow_logs(
            &self,
            _handle: &dyn crate::backend::SandboxHandle,
            _consumer: Box<dyn Fn(String) + Send + Sync>,
        ) -> crate::error::Result<crate::backend::FollowHandle> {
            unimplemented!()
        }
        async fn ensure_network(&self, _network_id: &str) -> crate::error::Result<()> {
            Ok(())
        }
        async fn remove_network(&self, _network_id: &str) -> crate::error::Result<()> {
            Ok(())
        }
        fn cleanup_sync(&self, container_id: &str) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.cleaned.lock().unwrap().push(container_id.to_string());
        }
        fn remove_by_name(&self, _name: &str) {}
        fn watchdog_kill_command(&self) -> Vec<String> {
            vec!["true".to_string()]
        }
    }

    #[test]
    fn enqueue_runs_cleanup_sync_on_the_background_thread() {
        let cleaned = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let backend: Arc<dyn SandboxBackend> = Arc::new(RecordingBackend {
            cleaned: cleaned.clone(),
            calls: calls.clone(),
        });

        enqueue(CleanupJob {
            backend: backend.clone(),
            container_id: "container-under-test".to_string(),
            after_teardown: None,
        });

        // The cleanup thread is asynchronous relative to this test; poll briefly rather
        // than assume a fixed sleep is enough (and rather than blocking forever if
        // something regresses).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while calls.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(cleaned.lock().unwrap().as_slice(), ["container-under-test"]);
    }

    #[test]
    fn after_teardown_runs_on_the_background_thread_only_once_cleanup_sync_has_returned() {
        let cleaned = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let backend: Arc<dyn SandboxBackend> = Arc::new(RecordingBackend {
            cleaned: cleaned.clone(),
            calls: calls.clone(),
        });

        // Records, at the moment `after_teardown` runs, how many `cleanup_sync` calls
        // had already completed — proving ordering, not just eventual invocation.
        let cleanup_sync_calls_seen_by_after_teardown = Arc::new(AtomicUsize::new(usize::MAX));
        let seen = cleanup_sync_calls_seen_by_after_teardown.clone();
        let calls_for_closure = calls.clone();
        let after_teardown_ran = Arc::new(AtomicUsize::new(0));
        let after_teardown_ran_writer = after_teardown_ran.clone();

        enqueue(CleanupJob {
            backend: backend.clone(),
            container_id: "container-under-test".to_string(),
            after_teardown: Some(Box::new(move || {
                seen.store(calls_for_closure.load(Ordering::SeqCst), Ordering::SeqCst);
                after_teardown_ran_writer.fetch_add(1, Ordering::SeqCst);
            })),
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while after_teardown_ran.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert_eq!(
            after_teardown_ran.load(Ordering::SeqCst),
            1,
            "after_teardown must run exactly once"
        );
        assert_eq!(
            cleanup_sync_calls_seen_by_after_teardown.load(Ordering::SeqCst),
            1,
            "after_teardown must observe cleanup_sync as already having run"
        );
    }

    #[test]
    fn a_missing_after_teardown_is_a_harmless_no_op() {
        // `enqueue_runs_cleanup_sync_on_the_background_thread` above already proves
        // `after_teardown: None` doesn't stop `cleanup_sync` from running; this test
        // is the flip side, proving a job with a `None` callback doesn't panic the
        // cleanup thread (which would silently break every job queued after it).
        let calls = Arc::new(AtomicUsize::new(0));
        let backend: Arc<dyn SandboxBackend> = Arc::new(RecordingBackend {
            cleaned: Arc::new(Mutex::new(Vec::new())),
            calls: calls.clone(),
        });
        enqueue(CleanupJob {
            backend: backend.clone(),
            container_id: "no-after-teardown".to_string(),
            after_teardown: None,
        });

        // A second job on the same thread proves the thread survived the first.
        let calls2 = Arc::new(AtomicUsize::new(0));
        let backend2: Arc<dyn SandboxBackend> = Arc::new(RecordingBackend {
            cleaned: Arc::new(Mutex::new(Vec::new())),
            calls: calls2.clone(),
        });
        enqueue(CleanupJob {
            backend: backend2.clone(),
            container_id: "after-the-none-job".to_string(),
            after_teardown: None,
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while calls2.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(calls2.load(Ordering::SeqCst), 1);
    }
}
