//! The init-time sweep: for every OTHER run's ledger under the cache dir, reap it if
//! (and only if) it's dead. See `crate::reaper`'s module doc for the call-site
//! guarantee (runs exactly once per process, from `crate::backends::active()`).

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::SandboxBackend;

use super::ledger::{self, Ledger, RunRecord};
use super::liveness;

/// A record older than this with unparseable JSON is treated as dead (its owning
/// process almost certainly crashed mid-write, or the file is simply corrupt) —
/// fresh unparseable JSON is left alone, since it may just be another process's
/// write-in-progress (the atomic tmp+rename means a reader should never actually see
/// a torn write, but this is a defense-in-depth margin, not the primary guarantee).
const UNPARSEABLE_STALE_AGE: Duration = Duration::from_secs(3600);

/// Sweeps every run record under `cache_dir` other than `own_run_id`, reaping (via
/// `backend`) any that are dead. Best-effort throughout: a single run's I/O failure
/// never aborts the sweep of the others.
pub(crate) fn run(backend: &Arc<dyn SandboxBackend>, cache_dir: &Path, own_run_id: &str) {
    for run_id in ledger::candidate_run_ids(cache_dir) {
        if run_id == own_run_id {
            continue; // never touch this process's own run.
        }
        let candidate = Ledger::new(cache_dir, &run_id);
        reap_if_dead(backend, &candidate);
    }
}

fn reap_if_dead(backend: &Arc<dyn SandboxBackend>, ledger: &Ledger) {
    let raw = match std::fs::read(ledger.record_path()) {
        Ok(b) => b,
        Err(_) => return, // no record (already cleaned up, or never written): nothing to do.
    };
    match serde_json::from_slice::<RunRecord>(&raw) {
        Ok(record) => {
            if liveness::is_alive(record.pid, &record.started_iso) {
                return; // alive: never touched.
            }
            // A docker process cannot remove msb sandboxes and vice versa — a
            // cross-backend leftover waits for a process running that backend.
            if record.backend != backend.name() {
                return;
            }
            reap(backend, ledger);
        }
        Err(_) => {
            let age = ledger::file_age(&ledger.record_path());
            if age.map(|a| a > UNPARSEABLE_STALE_AGE).unwrap_or(false) {
                reap(backend, ledger);
            }
            // else: fresh + unparseable — skip, don't guess.
        }
    }
}

fn reap(backend: &Arc<dyn SandboxBackend>, ledger: &Ledger) {
    for name in ledger.sandbox_names() {
        // Best-effort, "not found" silently ignored — see
        // `SandboxBackend::remove_by_name`'s doc.
        backend.remove_by_name(&name);
    }
    // Spec step 3: "Then remove each network in `.networks` (in-use errors
    // ignored)." — see `remove_networks_blocking`'s doc for how this reconciles
    // `SandboxBackend::remove_network` being async with the sweep's own
    // no-runtime-guaranteed call site.
    remove_networks_blocking(backend, ledger.network_ids());
    ledger.delete_files();
}

/// Removes every network in `network_ids` via the async `SandboxBackend::remove_network`.
/// This sweep runs from `crate::backends::active()`'s resolution path, which must work
/// with **no Tokio runtime guaranteed** on the calling thread (see that method's doc)
/// — but it's just as often called from *inside* one already (the common case, since
/// `Container::start()` is an `async fn` running on a Tokio worker thread), where
/// building a nested runtime and blocking on it directly would panic ("Cannot start a
/// runtime from within a runtime").
///
/// A dedicated OS thread sidesteps both: it never inherits a Tokio context of its own,
/// so a fresh current-thread runtime built there is always safe to `block_on`,
/// regardless of what the calling thread's own context looks like. Best-effort
/// throughout, matching the sweep's own contract: "in use"/not-found removal errors
/// are ignored, and a failed-to-build-runtime or panicking thread must never abort the
/// sweep of the rest of this (or any other) run.
fn remove_networks_blocking(backend: &Arc<dyn SandboxBackend>, network_ids: Vec<String>) {
    if network_ids.is_empty() {
        return;
    }
    let backend = backend.clone();
    let joined = std::thread::spawn(move || {
        let Ok(rt) = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        else {
            return;
        };
        rt.block_on(async {
            for id in &network_ids {
                let _ = backend.remove_network(id).await;
            }
        });
    })
    .join();
    let _ = joined; // best-effort: a panicked thread must not abort the sweep.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{FollowHandle, SandboxHandle};
    use crate::error::Result;
    use crate::model::{ContainerSpec, ExecResult};
    use std::path::PathBuf;
    use std::sync::Mutex;

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

    #[derive(Default)]
    struct FakeBackend {
        name: String,
        removed: Mutex<Vec<String>>,
        removed_networks: Mutex<Vec<String>>,
    }

    impl FakeBackend {
        fn named(name: &str) -> Arc<Self> {
            Arc::new(FakeBackend {
                name: name.to_string(),
                removed: Mutex::new(Vec::new()),
                removed_networks: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl SandboxBackend for FakeBackend {
        fn name(&self) -> &str {
            &self.name
        }
        fn supports_native_networks(&self) -> bool {
            false
        }
        async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
            Ok(Box::new(FakeHandle {
                id: spec.name.clone(),
                spec,
            }))
        }
        async fn start(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn stop(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn remove(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn exec(&self, _handle: &dyn SandboxHandle, _cmd: &[String]) -> Result<ExecResult> {
            unimplemented!()
        }
        async fn logs(&self, _handle: &dyn SandboxHandle) -> Result<String> {
            unimplemented!()
        }
        async fn follow_logs(
            &self,
            _handle: &dyn SandboxHandle,
            _consumer: Box<dyn Fn(String) + Send + Sync>,
        ) -> Result<FollowHandle> {
            unimplemented!()
        }
        async fn ensure_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        async fn remove_network(&self, network_id: &str) -> Result<()> {
            self.removed_networks
                .lock()
                .unwrap()
                .push(network_id.to_string());
            Ok(())
        }
        fn cleanup_sync(&self, _container_id: &str) {}
        fn remove_by_name(&self, name: &str) {
            self.removed.lock().unwrap().push(name.to_string());
        }
        fn watchdog_kill_command(&self) -> Vec<String> {
            vec!["true".to_string()]
        }
    }

    fn temp_cache_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rz-sweep-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn dead_record(backend: &str) -> RunRecord {
        RunRecord {
            // A pid this test process almost certainly does not own, with a start
            // time that (even if the pid happened to be reused) is extremely
            // unlikely to fall within the 2-second liveness window.
            pid: u32::MAX - 1,
            started_iso: "1999-01-01T00:00:00Z".to_string(),
            backend: backend.to_string(),
            msb_path: None,
        }
    }

    fn alive_record(backend: &str) -> RunRecord {
        let pid = std::process::id();
        let started_iso = liveness::process_started_iso(pid).expect("own start time");
        RunRecord {
            pid,
            started_iso,
            backend: backend.to_string(),
            msb_path: None,
        }
    }

    #[test]
    fn dead_run_matching_the_active_backend_is_reaped() {
        let cache = temp_cache_dir("dead-reaped");
        let backend = FakeBackend::named("docker");
        let dead = Ledger::new(&cache, "dead-run");
        dead.write_record(&dead_record("docker")).unwrap();
        dead.append_sandbox("rz-dead-0").unwrap();
        dead.append_sandbox("rz-dead-1").unwrap();
        dead.append_network("rz-net-dead").unwrap();

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        assert_eq!(
            backend.removed.lock().unwrap().clone(),
            vec!["rz-dead-0".to_string(), "rz-dead-1".to_string()]
        );
        assert_eq!(
            backend.removed_networks.lock().unwrap().clone(),
            vec!["rz-net-dead".to_string()],
            "spec step 3: the sweep must also remove each network in `.networks`"
        );
        assert!(!dead.record_path().exists(), "ledger files must be deleted");
        assert!(!dead.sandboxes_path().exists());
        assert!(!dead.networks_path().exists());
    }

    #[test]
    fn dead_run_with_only_networks_and_no_sandboxes_still_gets_them_removed() {
        let cache = temp_cache_dir("dead-networks-only");
        let backend = FakeBackend::named("docker");
        let dead = Ledger::new(&cache, "dead-run");
        dead.write_record(&dead_record("docker")).unwrap();
        dead.append_network("rz-net-a").unwrap();
        dead.append_network("rz-net-b").unwrap();

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        let mut removed = backend.removed_networks.lock().unwrap().clone();
        removed.sort();
        assert_eq!(
            removed,
            vec!["rz-net-a".to_string(), "rz-net-b".to_string()]
        );
        assert!(!dead.networks_path().exists());
    }

    // The sweep runs from `crate::backends::active()`'s resolution path, which is
    // just as often called from INSIDE an existing Tokio runtime (the common case:
    // `Container::start()` is an `async fn`) as from a plain sync context. Building a
    // nested runtime and blocking on it directly on the calling thread would panic
    // ("Cannot start a runtime from within a runtime") — this proves
    // `remove_networks_blocking`'s dedicated-OS-thread indirection actually avoids
    // that, not just in the plain-`#[test]` cases above.
    #[tokio::test]
    async fn network_removal_does_not_panic_when_called_from_inside_a_tokio_runtime() {
        let cache = temp_cache_dir("dead-networks-inside-runtime");
        let backend = FakeBackend::named("docker");
        let dead = Ledger::new(&cache, "dead-run");
        dead.write_record(&dead_record("docker")).unwrap();
        dead.append_network("rz-net-inside").unwrap();

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        assert_eq!(
            backend.removed_networks.lock().unwrap().clone(),
            vec!["rz-net-inside".to_string()]
        );
    }

    #[test]
    fn alive_run_is_never_touched() {
        let cache = temp_cache_dir("alive-untouched");
        let backend = FakeBackend::named("docker");
        let alive = Ledger::new(&cache, "alive-run");
        alive.write_record(&alive_record("docker")).unwrap();
        alive.append_sandbox("rz-alive-0").unwrap();

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        assert!(backend.removed.lock().unwrap().is_empty());
        assert!(
            alive.record_path().exists(),
            "alive run's files must survive"
        );
    }

    #[test]
    fn own_run_is_never_touched_even_if_it_looks_dead() {
        let cache = temp_cache_dir("own-run-untouched");
        let backend = FakeBackend::named("docker");
        let own = Ledger::new(&cache, "own-run");
        own.write_record(&dead_record("docker")).unwrap();
        own.append_sandbox("rz-own-0").unwrap();

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        assert!(backend.removed.lock().unwrap().is_empty());
        assert!(own.record_path().exists());
    }

    #[test]
    fn cross_backend_dead_run_waits_for_a_process_on_that_backend() {
        let cache = temp_cache_dir("cross-backend");
        let backend = FakeBackend::named("docker");
        let msb_run = Ledger::new(&cache, "msb-run");
        msb_run.write_record(&dead_record("microsandbox")).unwrap();
        msb_run.append_sandbox("rz-msb-0").unwrap();

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        assert!(
            backend.removed.lock().unwrap().is_empty(),
            "a docker process must not remove an msb-backend run's sandboxes"
        );
        assert!(
            msb_run.record_path().exists(),
            "the cross-backend record must be left for an msb process to reap"
        );
    }

    #[test]
    fn not_found_removal_errors_are_silently_ignored() {
        // FakeBackend::remove_by_name never errors (it's `fn`, not `Result`-returning)
        // — this proves the sweep completes and cleans up even when the "removal"
        // itself is a no-op from the backend's point of view (the "not found" case in
        // spirit), matching msb/docker's own best-effort remove_by_name contract.
        let cache = temp_cache_dir("not-found-ignored");
        let backend = FakeBackend::named("docker");
        let dead = Ledger::new(&cache, "dead-run");
        dead.write_record(&dead_record("docker")).unwrap();
        dead.append_sandbox("rz-vanished-already").unwrap();

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        assert!(!dead.record_path().exists());
    }

    #[test]
    fn fresh_unparseable_json_is_skipped() {
        let cache = temp_cache_dir("fresh-unparseable");
        let backend = FakeBackend::named("docker");
        let ledger = Ledger::new(&cache, "garbled-run");
        std::fs::create_dir_all(cache.join("runs")).unwrap();
        std::fs::write(ledger.record_path(), b"{not json").unwrap();
        ledger.append_sandbox("rz-garbled-0").unwrap();

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        assert!(backend.removed.lock().unwrap().is_empty());
        assert!(
            ledger.record_path().exists(),
            "a fresh unparseable record must be left alone, not guessed at"
        );
    }

    #[test]
    fn stale_unparseable_json_is_cleaned_up() {
        let cache = temp_cache_dir("stale-unparseable");
        let backend = FakeBackend::named("docker");
        let ledger = Ledger::new(&cache, "garbled-run");
        std::fs::create_dir_all(cache.join("runs")).unwrap();
        std::fs::write(ledger.record_path(), b"{not json").unwrap();
        ledger.append_sandbox("rz-garbled-0").unwrap();

        // Backdate the record file's mtime well past the staleness threshold.
        let ancient =
            std::time::SystemTime::now() - (UNPARSEABLE_STALE_AGE + Duration::from_secs(60));
        let f = std::fs::File::open(ledger.record_path()).unwrap();
        f.set_modified(ancient).unwrap();
        drop(f);

        run(
            &(backend.clone() as Arc<dyn SandboxBackend>),
            &cache,
            "own-run",
        );

        assert_eq!(
            backend.removed.lock().unwrap().clone(),
            vec!["rz-garbled-0".to_string()]
        );
        assert!(!ledger.record_path().exists());
    }
}
