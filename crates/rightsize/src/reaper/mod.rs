//! Orphan reaping: the backstop for the `Drop`-path cleanup thread (see
//! `crate::cleanup`'s module doc — "the backstop for the backstop"). If the process
//! dies without running any cleanup at all (a bare SIGKILL, an OOM-kill, a crashed CI
//! step skips `Drop` entirely), its sandboxes would otherwise run forever. Two layers:
//!
//! - **Run-record ledger** ([`ledger`]): `runs/<run-id>.json`/`.sandboxes`/`.networks`
//!   under the shared rightsize cache dir (see `crate::cache_dir`) — an ownership
//!   ledger any of Kotlin/Node/Rust can read, since they all agree on the same cache
//!   dir. Written before the first sandbox exists, updated on every create/stop,
//!   deleted on clean shutdown.
//! - **Init-time sweep** ([`sweep`]): runs exactly once per process, from
//!   `crate::backends::active()` right after resolution succeeds (mirroring the
//!   precedent of msb's own former per-process orphan sweep, which this ledger-based
//!   sweep replaces — see that module's history). Reaps every OTHER run's ledger that
//!   is DEAD (liveness protocol: [`liveness`]) and whose recorded backend matches this
//!   process's own — a docker process cannot remove msb sandboxes, so a cross-backend
//!   leftover waits for a process running that backend.
//! - **Watchdog** ([`watchdog`], optional): a detached per-run script that reaps
//!   within seconds of a crash instead of waiting for the next process's sweep.
//!
//! `RIGHTSIZE_REAPER` ([`mode`]) gates both: `on` (default) runs sweep + watchdog,
//! `sweep` runs the sweep only, `off` disables both (and skips writing a ledger at
//! all — nothing would ever read it).

mod ledger;
mod liveness;
mod mode;
mod sweep;
mod watchdog;

pub(crate) use ledger::{Ledger, RunRecord};
pub(crate) use mode::ReaperMode;

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use crate::backend::SandboxBackend;
use crate::run_id::RunId;

/// Ensures `ledger`'s run record exists (written exactly once in the *uncontended*
/// case — a no-op if a record is already on disk) and appends `name` to its
/// `.sandboxes` file unless `keep_alive` (reuse sandboxes are never listed — see
/// `ContainerSpec::keep_alive`). Returns `true` when THIS call is the one that saw no
/// existing record and wrote one — but that check-then-write is racy across
/// concurrent callers in the same process (no lock spans the read and the write), so
/// under concurrent calls more than one caller can observe `true`. Harmless for the
/// record file itself (every caller writes equivalent content), which is why this
/// return value is safe for a test to assert on directly but is NOT used by
/// `before_create` to decide whether to spawn the watchdog — see that function's doc.
/// Pure enough (all inputs explicit, no process-wide state) to unit test directly
/// against a temp-dir `Ledger`.
pub(crate) fn record_create(
    ledger: &Ledger,
    backend_name: &str,
    msb_path: Option<String>,
    pid: u32,
    started_iso: String,
    name: &str,
    keep_alive: bool,
) -> bool {
    let wrote_record = if ledger.read_record().is_none() {
        let record = RunRecord {
            pid,
            started_iso,
            backend: backend_name.to_string(),
            msb_path,
        };
        let _ = ledger.write_record(&record);
        true
    } else {
        false
    };
    if !keep_alive {
        let _ = ledger.append_sandbox(name);
    }
    wrote_record
}

/// Removes `name` from `ledger`'s `.sandboxes` file (unless `keep_alive`, which was
/// never listed in the first place), and — the clean-shutdown rule — deletes all
/// three ledger files once both `.sandboxes` and `.networks` are empty.
pub(crate) fn record_stop(ledger: &Ledger, name: &str, keep_alive: bool) {
    if keep_alive {
        return;
    }
    ledger.remove_sandbox_and_prune_if_empty(name);
}

/// Process-wide reaper state: this process's own mode, cache dir, and ledger — built
/// once, lazily, on first use.
struct State {
    mode: ReaperMode,
    cache_dir: PathBuf,
    ledger: Ledger,
    /// Gates [`watchdog::spawn`] to exactly one call per process — see
    /// [`before_create`]'s doc for why this must be an in-process primitive rather
    /// than `record_create`'s disk-based "did I write the run record" signal.
    watchdog_spawned: OnceLock<()>,
}

static STATE: OnceLock<State> = OnceLock::new();

fn state() -> &'static State {
    STATE.get_or_init(|| {
        let mode = ReaperMode::from_env();
        let cache_dir = crate::cache_dir::dir();
        let ledger = Ledger::new(&cache_dir, RunId::value());
        State {
            mode,
            cache_dir,
            ledger,
            watchdog_spawned: OnceLock::new(),
        }
    })
}

/// This process's own ledger — the real one (`State::ledger`, under the real
/// `crate::cache_dir::dir()`) unless `cache_dir_override` is given, in which case
/// a fresh [`Ledger`] is built against that dir instead (same run id — only the
/// dir changes). Cheap either way: [`Ledger`] does no I/O until a method is
/// called, and `State::ledger` is already just a stored value, not recomputed.
///
/// Test/module seam: production never passes `Some` here — every real call site
/// below passes `None`, which is the plain "use this process's real ledger"
/// behavior this module has always had. `Some` is threaded down from
/// `Container::with_reaper_cache_dir_override` (see that method's doc), so a
/// unit test can point specifically the ledger-asserting container tests at a
/// scratch dir instead of this developer machine's real
/// `~/.cache/rightsize/runs/`, without affecting any other test in the same
/// process (which keeps sharing the real, process-wide ledger exactly as
/// before) or any production caller (which never supplies an override at all).
fn ledger_for(cache_dir_override: Option<&Path>) -> Ledger {
    match cache_dir_override {
        Some(dir) => Ledger::new(dir, RunId::value()),
        None => state().ledger.clone(),
    }
}

/// Runs the init-time sweep. Called from `crate::backends::active()` right after
/// resolution succeeds — that call site's own `OnceLock::get_or_init` already
/// guarantees this runs exactly once per process, so no extra guard is needed here.
/// No-op when `RIGHTSIZE_REAPER=off`.
pub(crate) fn sweep(backend: &Arc<dyn SandboxBackend>) {
    let st = state();
    if !st.mode.sweep_enabled() {
        return;
    }
    sweep::run(backend, &st.cache_dir, RunId::value());
}

/// Called from `crate::container::create_started_container`, right before
/// `backend.create(spec)`. Lazily writes this process's own run record and spawns the
/// watchdog on the very first call; appends `name` to the sandboxes ledger on every
/// call unless `keep_alive`. No-op entirely when `RIGHTSIZE_REAPER=off`; the watchdog
/// specifically is also skipped when `=sweep`.
///
/// The watchdog-spawn-once decision is gated by [`State::watchdog_spawned`], an
/// in-process `OnceLock` — deliberately NOT by [`record_create`]'s `wrote_record`
/// return value, even though that also means "the first call in this process" in
/// intent. `record_create`'s signal is a check-then-write over a file on disk (`Ledger
/// ::read_record` then `Ledger::write_record`, with no lock between them), so two
/// threads calling `before_create` concurrently — the ordinary shape of two
/// `#[tokio::test]`s both starting their first container at once — can both observe
/// "no record yet" and both get `wrote_record = true`. That used to double-call
/// `watchdog::spawn`; the loser's freshly spawned watchdog process then had its
/// `Child` (and stdin) immediately dropped by `watchdog::spawn`'s own
/// `WATCHDOG_CHILD.set` race handling, which closes the pipe the watchdog script
/// blocks reading — an instant, spurious EOF that made it force-remove every sandbox
/// already appended to this run's ledger, including ones a sibling thread still had
/// running. `OnceLock::get_or_init` below has no such window: concurrent callers
/// block on the same in-process cell rather than each independently racing a
/// filesystem check, so `watchdog::spawn` runs exactly once per process regardless of
/// how many threads call `before_create` at once.
///
/// `cache_dir_override` is the test/module seam described on [`ledger_for`] — it
/// governs only which ledger this call's append lands in; the watchdog-spawn-once
/// gate and the process's mode/cache-dir stay tied to the real, process-wide
/// `State` regardless (the watchdog is a one-shot, per-process side effect, not
/// something a single container's test setup should redirect). Every production
/// caller passes `None`.
pub(crate) fn before_create(
    backend: &Arc<dyn SandboxBackend>,
    name: &str,
    keep_alive: bool,
    cache_dir_override: Option<&Path>,
) {
    let st = state();
    if !st.mode.ledger_enabled() {
        return;
    }
    let ledger = ledger_for(cache_dir_override);
    let pid = std::process::id();
    let started_iso = liveness::process_started_iso(pid).unwrap_or_default();
    let _ = record_create(
        &ledger,
        backend.name(),
        backend
            .backend_binary_path()
            .map(|p| p.display().to_string()),
        pid,
        started_iso,
        name,
        keep_alive,
    );
    if st.mode.watchdog_enabled() {
        st.watchdog_spawned
            .get_or_init(|| watchdog::spawn(&st.cache_dir, backend, &st.ledger));
    }
}

/// Called from `ContainerGuard::stop_inner`, after a successful stop+remove.
/// `cache_dir_override`: see [`ledger_for`] — every production caller passes
/// `None`.
pub(crate) fn after_stop(name: &str, keep_alive: bool, cache_dir_override: Option<&Path>) {
    let st = state();
    if !st.mode.ledger_enabled() {
        return;
    }
    record_stop(&ledger_for(cache_dir_override), name, keep_alive);
}

/// Called from `Container::start()`, right before `backend.ensure_network`, mirroring
/// [`before_create`]'s append-before-create discipline for networks.
/// `cache_dir_override`: see [`ledger_for`] — every production caller passes
/// `None`.
pub(crate) fn before_ensure_network(id: &str, cache_dir_override: Option<&Path>) {
    let st = state();
    if !st.mode.ledger_enabled() {
        return;
    }
    let _ = ledger_for(cache_dir_override).append_network(id);
}

/// Called from `Network::close()`, after a successful `backend.remove_network`.
/// `cache_dir_override`: see [`ledger_for`] — every production caller passes
/// `None`.
pub(crate) fn after_remove_network(id: &str, cache_dir_override: Option<&Path>) {
    let st = state();
    if !st.mode.ledger_enabled() {
        return;
    }
    ledger_for(cache_dir_override).remove_network_and_prune_if_empty(id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_cache_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rz-reaper-facade-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn record_create_writes_the_record_exactly_once_and_appends_every_call() {
        let cache = temp_cache_dir("record-once");
        let ledger = Ledger::new(&cache, "run1");

        let wrote_first = record_create(
            &ledger,
            "docker",
            None,
            4242,
            "2025-01-01T00:00:00Z".to_string(),
            "rz-run1-0",
            false,
        );
        assert!(wrote_first, "the first call must write the record");
        let record = ledger.read_record().expect("record must exist");
        assert_eq!(record.pid, 4242);
        assert_eq!(record.backend, "docker");

        let wrote_second = record_create(
            &ledger,
            "docker",
            None,
            4242,
            "2025-01-01T00:00:00Z".to_string(),
            "rz-run1-1",
            false,
        );
        assert!(!wrote_second, "a later call must not rewrite the record");
        assert_eq!(
            ledger.sandbox_names(),
            vec!["rz-run1-0".to_string(), "rz-run1-1".to_string()],
            "every call appends its own sandbox name, record-writing or not"
        );
    }

    #[test]
    fn record_create_skips_the_ledger_append_for_a_keep_alive_spec() {
        let cache = temp_cache_dir("keep-alive-skip");
        let ledger = Ledger::new(&cache, "run1");
        record_create(
            &ledger,
            "docker",
            None,
            1,
            "2025-01-01T00:00:00Z".to_string(),
            "rz-reuse-0",
            true,
        );
        assert!(
            ledger.sandbox_names().is_empty(),
            "a keep_alive sandbox must never be listed in the reaping ledger"
        );
    }

    #[test]
    fn record_stop_removes_the_name_and_clean_shutdown_deletes_files_once_empty() {
        let cache = temp_cache_dir("stop-clean");
        let ledger = Ledger::new(&cache, "run1");
        record_create(
            &ledger,
            "docker",
            None,
            1,
            "2025-01-01T00:00:00Z".to_string(),
            "rz-run1-0",
            false,
        );
        assert!(ledger.record_path().exists());

        record_stop(&ledger, "rz-run1-0", false);

        assert!(
            !ledger.record_path().exists(),
            "the last sandbox stopping with no networks must trigger clean-shutdown deletion"
        );
        assert!(!ledger.sandboxes_path().exists());
    }

    #[test]
    fn record_stop_on_a_keep_alive_name_never_touches_the_ledger() {
        let cache = temp_cache_dir("stop-keep-alive");
        let ledger = Ledger::new(&cache, "run1");
        record_create(
            &ledger,
            "docker",
            None,
            1,
            "2025-01-01T00:00:00Z".to_string(),
            "rz-run1-0",
            false,
        );
        record_stop(&ledger, "rz-run1-0", true); // keep_alive: must be a no-op.
        assert_eq!(
            ledger.sandbox_names(),
            vec!["rz-run1-0".to_string()],
            "a keep_alive stop must not remove a name it never listed"
        );
    }

    #[test]
    fn record_stop_leaves_files_in_place_while_a_network_is_still_listed() {
        let cache = temp_cache_dir("stop-network-remains");
        let ledger = Ledger::new(&cache, "run1");
        record_create(
            &ledger,
            "docker",
            None,
            1,
            "2025-01-01T00:00:00Z".to_string(),
            "rz-run1-0",
            false,
        );
        ledger.append_network("net-1").unwrap();

        record_stop(&ledger, "rz-run1-0", false);

        assert!(
            ledger.record_path().exists(),
            "files survive while a network is still listed, even with no sandboxes left"
        );
    }

    /// A `SandboxBackend` that implements only what `before_create`/`watchdog::spawn`
    /// actually touch (`name`, `watchdog_kill_command`) — every other method is
    /// `unimplemented!()`, matching `crate::cleanup`'s own `RecordingBackend` test
    /// double shape. `watchdog_kill_command` is `["true"]` (a real no-op CLI command)
    /// so the one legitimate watchdog this test's run spawns — which stays blocked on
    /// its stdin pipe for the rest of THIS TEST BINARY's process lifetime, exactly
    /// like every other test in this crate that exercises `Container::start()`
    /// already does — never attempts to touch anything real if it ever did fire.
    struct NoopBackend;

    #[async_trait::async_trait]
    impl crate::backend::SandboxBackend for NoopBackend {
        fn name(&self) -> &str {
            "noop"
        }
        fn supports_native_networks(&self) -> bool {
            false
        }
        async fn create(
            &self,
            _spec: crate::model::ContainerSpec,
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
        ) -> crate::error::Result<crate::model::ExecResult> {
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
        fn cleanup_sync(&self, _container_id: &str) {}
        fn remove_by_name(&self, _name: &str) {}
        fn watchdog_kill_command(&self) -> Vec<String> {
            vec!["true".to_string()]
        }
    }

    /// Regression test for the double-watchdog-spawn race: `record_create`'s own
    /// "did I write the run record" signal is a check-then-write over a file on disk
    /// (`Ledger::read_record` then `Ledger::write_record`, no lock spanning the two —
    /// see `record_create`'s doc) and is racy under concurrent callers. `before_create`
    /// used to gate `watchdog::spawn` on exactly that racy signal, so two threads
    /// calling it for the very first time in this process — the ordinary shape of two
    /// `#[tokio::test]`s both starting their first container at once — could both
    /// observe "no record yet" and both spawn a real watchdog `sh` process. The
    /// loser's `Child` (and stdin) then got dropped immediately by `watchdog::spawn`'s
    /// own race handling, an instant spurious EOF that made ITS watchdog script
    /// force-remove every sandbox already in this run's ledger — including a sibling
    /// thread's still-running container. `before_create` now gates the spawn on
    /// `State::watchdog_spawned`, an in-process `OnceLock`, which has no such window:
    /// this test races many threads through the REAL, global `before_create` (the
    /// exact function `create_started_container` calls) and asserts the watchdog
    /// process was actually forked (`watchdog::SPAWN_CALL_COUNT`) at most once, ever,
    /// for this whole test binary's process lifetime.
    ///
    /// Reverting `before_create`'s gate to `if wrote_record { watchdog::spawn(..) }`
    /// makes this test fail (`SPAWN_CALL_COUNT` observed > 1) with high probability —
    /// the sixteen threads below all race `record_create` against the very first
    /// creation of this process's own run record, which is exactly the narrow TOCTOU
    /// window the old code depended on staying uncontended.
    #[test]
    fn concurrent_before_create_callers_spawn_the_watchdog_at_most_once_ever() {
        if !state().mode.watchdog_enabled() {
            eprintln!(
                "skipping: RIGHTSIZE_REAPER does not have the watchdog enabled in this \
                 environment"
            );
            return;
        }

        let backend: Arc<dyn crate::backend::SandboxBackend> = Arc::new(NoopBackend);
        const THREADS: usize = 16;
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|i| {
                let backend = backend.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    before_create(&backend, &format!("rz-watchdog-race-test-{i}"), false, None);
                })
            })
            .collect();
        for h in handles {
            h.join().expect("before_create must not panic");
        }

        let spawned = watchdog::SPAWN_CALL_COUNT.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            spawned <= 1,
            "the watchdog process must be forked AT MOST ONCE across this process's \
             entire lifetime, even with {THREADS} threads racing before_create \
             concurrently; observed {spawned} real spawns — a second spawn means its \
             losing Child's stdin was closed immediately, which would have fired that \
             watchdog's reap logic against every sandbox already in the shared ledger"
        );
    }

    /// Deterministic companion to the test above: rather than hoping enough thread
    /// scheduling luck lands inside `record_create`'s own unsynchronized
    /// read-then-write window (real disk I/O is fast enough that natural scheduling
    /// rarely lands there — see the module doc's fix commit for how this test was
    /// validated red/green), this FORCES every thread through
    /// `Ledger::read_record` — the exact primitive `record_create` calls first —
    /// before any of them is allowed to call `Ledger::write_record`. That is exactly
    /// the interleaving two `#[tokio::test]`s calling `before_create` for the very
    /// first time can hit under real scheduling; forcing it removes the luck and
    /// makes the demonstration 100% reproducible.
    ///
    /// With every thread forced through the read first, ALL of them must observe "no
    /// record yet" — proving that gating a must-run-once side effect (the old code's
    /// `if wrote_record { watchdog::spawn(..) }`) on this signal would fire it
    /// `THREADS` times over, not once. The fixed gate
    /// (`OnceLock::get_or_init`, exactly `before_create`'s own `watchdog_spawned`
    /// pattern) is exercised immediately after over the SAME forced race and must
    /// run its side effect exactly once regardless.
    #[test]
    fn forced_concurrent_interleaving_shows_the_old_gate_red_and_the_new_gate_green() {
        let cache = temp_cache_dir("toctou-forced-interleaving");
        let ledger = Arc::new(Ledger::new(&cache, "toctou-run"));
        const THREADS: usize = 16;
        let read_barrier = Arc::new(std::sync::Barrier::new(THREADS));

        // Every thread races record_create's own two primitives, in order, with a
        // barrier forcing all THREADS reads to complete before any write is allowed.
        let saw_no_record = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handles: Vec<_> = (0..THREADS)
            .map(|i| {
                let ledger = ledger.clone();
                let read_barrier = read_barrier.clone();
                let saw_no_record = saw_no_record.clone();
                std::thread::spawn(move || {
                    let existing = ledger.read_record();
                    read_barrier.wait();
                    if existing.is_none() {
                        saw_no_record.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let record = RunRecord {
                            pid: i as u32,
                            started_iso: String::new(),
                            backend: "docker".to_string(),
                            msb_path: None,
                        };
                        let _ = ledger.write_record(&record);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let wrote_record_true_count = saw_no_record.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            wrote_record_true_count, THREADS,
            "forcing every thread through Ledger::read_record before any of them calls \
             write_record must make every single one observe \"no record yet\" — this \
             is the exact TOCTOU window before_create's watchdog gate used to depend \
             on staying uncontended"
        );

        // RED: the OLD gate — spawn once per "I wrote the record" signal — would have
        // run its side effect `wrote_record_true_count` times under this exact race.
        assert!(
            wrote_record_true_count > 1,
            "sanity: the old gate's failure mode requires more than one thread to see \
             wrote_record = true"
        );

        // GREEN: the FIXED gate — an in-process OnceLock, exactly
        // `State::watchdog_spawned`'s own pattern — collapses the same
        // `THREADS`-way race down to exactly one call, regardless of how many
        // threads raced `record_create` first.
        let gate: OnceLock<()> = OnceLock::new();
        let spawn_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for _ in 0..wrote_record_true_count {
            let spawn_count = spawn_count.clone();
            gate.get_or_init(|| {
                spawn_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
        }
        assert_eq!(
            spawn_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the OnceLock-gated spawn must run exactly once even when \
             {wrote_record_true_count} callers all believed they were first"
        );
    }
}
