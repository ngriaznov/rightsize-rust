//! Failure diagnostics: a process-local registry of currently running containers,
//! plus [`diagnostics`], a human-readable snapshot of all of them — the same FORMAT
//! in every rightsize language port (see this module's pinned golden-fixture test).
//!
//! Registered only once [`crate::Container::start`] has fully succeeded — the
//! [`crate::ContainerGuard`] exists AND its readiness wait has passed — so a
//! container that boots but never becomes ready never shows up in the report and a
//! mid-wait [`diagnostics`] call can't list it either; deregistered on
//! [`crate::ContainerGuard::stop`] or `Drop`.
//! Entirely in-process: no disk, no cross-process visibility — its only job is "what
//! is THIS process's own run doing right now."
//!
//! [`DiagnosticsGuard`] is the automatic hook: construct one at the start of a test,
//! and its `Drop` prints [`diagnostics`]'s report to stderr iff the thread is
//! unwinding from a panic.
//!
//! The registry logic itself lives on [`Registry`], an injectable type (mirroring
//! `crate::reaper::Ledger`'s own shape) so unit tests exercise their own isolated
//! instance instead of racing every other test in this crate against the one
//! process-wide singleton [`global`] wraps.

use std::sync::{Arc, Mutex, OnceLock};

use crate::backend::SandboxBackend;
use crate::model::ContainerSpec;

/// The number of trailing log lines a report shows per container.
const LOG_TAIL_LINES: usize = 50;

/// A [`crate::backend::SandboxHandle`] good enough for a backend's `logs()` call,
/// built from data captured at registration time rather than borrowed from a live
/// [`crate::ContainerGuard`] (the registry must own what it needs independently of
/// any guard's lifetime). Every backend's `logs()` implementation in this workspace
/// reads only `handle.id()` (msb's subprocess invocation, docker's HTTP path) — never
/// `handle.spec()` — so a cloned id plus the spec it was created from is sufficient.
struct RegisteredHandle {
    id: String,
    spec: ContainerSpec,
}

impl crate::backend::SandboxHandle for RegisteredHandle {
    fn id(&self) -> &str {
        &self.id
    }
    fn spec(&self) -> &ContainerSpec {
        &self.spec
    }
}

/// One registered running container.
struct Entry {
    /// The backend-facing sandbox name shown in the report — `ContainerSpec::name`
    /// (e.g. `rz-<run-id>-<seq>`), NOT `SandboxHandle::id()`, which for the docker
    /// backend is an opaque daemon-assigned id rather than a human-readable name.
    name: String,
    image: String,
    host: String,
    /// `(guest_port, host_port)` pairs, in exposure order.
    ports: Vec<(u16, u16)>,
    backend: Arc<dyn SandboxBackend>,
    handle: RegisteredHandle,
}

/// One container's already-gathered diagnostics data, decoupled from any I/O so
/// [`render`] (and its golden-format tests) never need a backend or an async runtime.
struct ReportEntry {
    name: String,
    image: String,
    host: String,
    ports: Vec<(u16, u16)>,
    /// `Ok` of the last (up to) [`LOG_TAIL_LINES`] log lines, or `Err` of a
    /// human-readable reason `logs()` failed.
    logs: std::result::Result<Vec<String>, String>,
}

/// A process-local registry of currently running containers, and the async
/// `report()` built from it. Real callers use the one process-wide instance behind
/// [`global`]; unit tests build their own via [`Registry::new`] for full isolation
/// from every other test in this crate touching the global one concurrently.
pub(crate) struct Registry {
    entries: Mutex<Vec<Entry>>,
}

impl Registry {
    fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    /// Registers a newly-started container.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register(
        &self,
        name: &str,
        image: &str,
        host: &str,
        ports: Vec<(u16, u16)>,
        backend: Arc<dyn SandboxBackend>,
        handle_id: &str,
        spec: ContainerSpec,
    ) {
        self.entries
            .lock()
            .expect("diagnostics registry mutex poisoned")
            .push(Entry {
                name: name.to_string(),
                image: image.to_string(),
                host: host.to_string(),
                ports,
                backend,
                handle: RegisteredHandle {
                    id: handle_id.to_string(),
                    spec,
                },
            });
    }

    /// Deregisters a container by its `ContainerSpec::name` — a no-op if it isn't
    /// registered (idempotent, mirroring every other own-run cleanup path in this
    /// crate).
    pub(crate) fn deregister(&self, name: &str) {
        self.entries
            .lock()
            .expect("diagnostics registry mutex poisoned")
            .retain(|e| e.name != name);
    }

    /// A human-readable snapshot of every container currently registered, in the
    /// order each was registered. Fetches each one's last 50 log lines; a container
    /// whose `logs()` call fails gets a `logs: unavailable (<reason>)` line instead
    /// of aborting the whole report.
    async fn report(&self) -> String {
        /// A registered entry's data, cloned out from behind the registry lock — a
        /// std `Mutex` guard must not be held across an `.await` point, so every
        /// field `render`/`logs()` needs is copied out up front, in one lock
        /// acquisition.
        struct Snapshot {
            name: String,
            image: String,
            host: String,
            ports: Vec<(u16, u16)>,
            backend: Arc<dyn SandboxBackend>,
            handle: RegisteredHandle,
        }

        let snapshot: Vec<Snapshot> = {
            let reg = self
                .entries
                .lock()
                .expect("diagnostics registry mutex poisoned");
            reg.iter()
                .map(|e| Snapshot {
                    name: e.name.clone(),
                    image: e.image.clone(),
                    host: e.host.clone(),
                    ports: e.ports.clone(),
                    backend: e.backend.clone(),
                    handle: RegisteredHandle {
                        id: e.handle.id.clone(),
                        spec: e.handle.spec.clone(),
                    },
                })
                .collect()
        };

        let mut entries = Vec::with_capacity(snapshot.len());
        for s in snapshot {
            let logs = match s.backend.logs(&s.handle).await {
                Ok(text) => Ok(tail_lines(&text, LOG_TAIL_LINES)),
                Err(e) => Err(e.to_string()),
            };
            entries.push(ReportEntry {
                name: s.name,
                image: s.image,
                host: s.host,
                ports: s.ports,
                logs,
            });
        }

        render(&entries)
    }
}

static GLOBAL: OnceLock<Registry> = OnceLock::new();

/// The one process-wide registry real callers (`crate::container`) use.
fn global() -> &'static Registry {
    GLOBAL.get_or_init(Registry::new)
}

/// Registers a newly-started container in the process-wide registry. Called from
/// `crate::container` only after a [`crate::ContainerGuard`]'s readiness wait has
/// succeeded.
#[allow(clippy::too_many_arguments)]
pub(crate) fn register(
    name: &str,
    image: &str,
    host: &str,
    ports: Vec<(u16, u16)>,
    backend: Arc<dyn SandboxBackend>,
    handle_id: &str,
    spec: ContainerSpec,
) {
    global().register(name, image, host, ports, backend, handle_id, spec);
}

/// Deregisters a container from the process-wide registry by its
/// `ContainerSpec::name`.
pub(crate) fn deregister(name: &str) {
    global().deregister(name);
}

/// Splits `text` into lines and keeps the last `n`.
fn tail_lines(text: &str, n: usize) -> Vec<String> {
    let all: Vec<&str> = text.lines().collect();
    let start = all.len().saturating_sub(n);
    all[start..].iter().map(|s| s.to_string()).collect()
}

/// Renders the exact cross-language report format from a list of already-gathered
/// entries, in registration order:
///
/// ```text
/// == rightsize diagnostics: 2 running container(s) ==
/// -- rz-ab12cd34-redis (redis:7-alpine) --
/// state: running   host: 127.0.0.1   ports: 6379->49213
/// last 50 log lines:
///   <log line>
///   ...
/// -- <next container> --
/// ...
/// ```
///
/// Zero containers: `== rightsize diagnostics: no running containers ==`. A failed
/// `logs()` call degrades that container's log section to a single
/// `logs: unavailable (<reason>)` line instead of throwing.
fn render(entries: &[ReportEntry]) -> String {
    if entries.is_empty() {
        return "== rightsize diagnostics: no running containers ==".to_string();
    }

    let mut out = format!(
        "== rightsize diagnostics: {} running container(s) ==",
        entries.len()
    );
    for e in entries {
        out.push('\n');
        out.push_str(&format!("-- {} ({}) --\n", e.name, e.image));
        let ports = if e.ports.is_empty() {
            "none".to_string()
        } else {
            e.ports
                .iter()
                .map(|(guest, host)| format!("{guest}->{host}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        out.push_str(&format!(
            "state: running   host: {}   ports: {ports}",
            e.host
        ));
        match &e.logs {
            Ok(lines) => {
                out.push('\n');
                out.push_str("last 50 log lines:");
                for line in lines {
                    out.push('\n');
                    out.push_str("  ");
                    out.push_str(line);
                }
            }
            Err(reason) => {
                out.push('\n');
                out.push_str(&format!("logs: unavailable ({reason})"));
            }
        }
    }
    out
}

/// The public entry point — `rightsize::diagnostics()`: a human-readable snapshot of
/// every container this process currently has registered as running, in the order
/// each was started.
pub async fn diagnostics() -> String {
    global().report().await
}

/// The language-idiomatic automatic hook: construct at the start of a test. Its
/// `Drop` prints [`diagnostics`]'s report to stderr exactly once, iff the current
/// thread is unwinding from a panic (`std::thread::panicking()`) — a passing test
/// prints nothing.
#[derive(Default)]
pub struct DiagnosticsGuard {
    _private: (),
}

impl DiagnosticsGuard {
    /// Starts watching for a panic on the current thread.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `Drop` should print the report, given whether the current thread is
    /// unwinding from a panic. Factored out of `Drop::drop` so this gating decision
    /// is directly unit-testable without ever needing to actually panic — a real
    /// panic (`std::thread::panicking()`) can only be observed from inside an
    /// unwind, which would otherwise force any test of this logic to be
    /// `#[should_panic]`.
    fn should_report(panicking: bool) -> bool {
        panicking
    }
}

impl Drop for DiagnosticsGuard {
    fn drop(&mut self) {
        if !Self::should_report(std::thread::panicking()) {
            return;
        }
        Self::report_on_panic();
    }
}

impl DiagnosticsGuard {
    /// The actual panic-drop body, factored out of `Drop::drop` so a test can invoke
    /// it (indirectly, via a real panicking drop — see the `tests` module) and
    /// observe that it ran, rather than only unit-testing the `should_report` gating
    /// boolean.
    ///
    /// `Drop` cannot be `async`, and this runs while unwinding, quite possibly still
    /// inside a Tokio runtime's own worker thread (a panicking `#[tokio::test]`,
    /// which defaults to the current-thread flavor) — nesting another `block_on`
    /// there would itself panic (tokio forbids driving a runtime recursively from
    /// within one), which during an unwind would abort the whole process instead of
    /// just failing this test. Sidestepping that: spawn a fresh OS thread with its
    /// own throwaway runtime, exactly the way `crate::cleanup`'s dedicated thread
    /// avoids the same problem, and block on *that* thread joining, which is plain
    /// synchronous `std::thread` machinery with no runtime-nesting concern at all.
    fn report_on_panic() {
        let report = std::thread::spawn(|| {
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt.block_on(diagnostics()),
                Err(e) => {
                    format!(
                        "rightsize diagnostics: could not build a runtime to fetch the report ({e})"
                    )
                }
            }
        })
        .join()
        .unwrap_or_else(|_| {
            "rightsize diagnostics: the report-fetching thread panicked".to_string()
        });

        // Test seam: forwards the rendered report to a channel a test set up, in
        // addition to the real stderr print below — see `tests::notify_report_rendered`.
        #[cfg(test)]
        tests::notify_report_rendered(&report);

        eprintln!("{report}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{FollowHandle, SandboxHandle};
    use crate::error::{Result, RightsizeError};
    use crate::model::ExecResult;
    use std::sync::mpsc;
    use std::time::Duration;

    fn spec(name: &str, image: &str) -> ContainerSpec {
        ContainerSpec::new(name, image, "deadbeef")
    }

    // -- render(): pure golden-format tests ----------------------------------

    #[test]
    fn render_reports_no_running_containers_when_empty() {
        assert_eq!(
            render(&[]),
            "== rightsize diagnostics: no running containers =="
        );
    }

    /// THE cross-language contract vector: two containers, one log line each, no
    /// failures — the exact spec from the diagnostics feature spec's "The report"
    /// section, reproduced identically in the Kotlin/Node ports.
    #[test]
    fn pinned_cross_language_report_vector() {
        let entries = vec![
            ReportEntry {
                name: "rz-ab12cd34-redis".to_string(),
                image: "redis:7-alpine".to_string(),
                host: "127.0.0.1".to_string(),
                ports: vec![(6379, 49213)],
                logs: Ok(vec![
                    "1:M 01 Jan 2026 00:00:00.000 * Ready to accept connections tcp".to_string(),
                ]),
            },
            ReportEntry {
                name: "rz-ab12cd34-postgres".to_string(),
                image: "postgres:16-alpine".to_string(),
                host: "127.0.0.1".to_string(),
                ports: vec![(5432, 49214)],
                logs: Ok(vec![
                    "database system is ready to accept connections".to_string(),
                ]),
            },
        ];

        assert_eq!(render(&entries), PINNED_REPORT_VECTOR);
    }

    const PINNED_REPORT_VECTOR: &str = "\
== rightsize diagnostics: 2 running container(s) ==
-- rz-ab12cd34-redis (redis:7-alpine) --
state: running   host: 127.0.0.1   ports: 6379->49213
last 50 log lines:
  1:M 01 Jan 2026 00:00:00.000 * Ready to accept connections tcp
-- rz-ab12cd34-postgres (postgres:16-alpine) --
state: running   host: 127.0.0.1   ports: 5432->49214
last 50 log lines:
  database system is ready to accept connections";

    #[test]
    fn render_degrades_a_failing_logs_call_instead_of_a_log_tail() {
        let entries = vec![ReportEntry {
            name: "rz-ab12cd34-redis".to_string(),
            image: "redis:7-alpine".to_string(),
            host: "127.0.0.1".to_string(),
            ports: vec![(6379, 49213)],
            logs: Err("msb exec timed out after 5s".to_string()),
        }];

        let expected = "\
== rightsize diagnostics: 1 running container(s) ==
-- rz-ab12cd34-redis (redis:7-alpine) --
state: running   host: 127.0.0.1   ports: 6379->49213
logs: unavailable (msb exec timed out after 5s)";

        assert_eq!(render(&entries), expected);
    }

    #[test]
    fn render_shows_none_for_a_container_with_no_exposed_ports() {
        let entries = vec![ReportEntry {
            name: "rz-ab12cd34-worker".to_string(),
            image: "worker:latest".to_string(),
            host: "127.0.0.1".to_string(),
            ports: vec![],
            logs: Ok(vec![]),
        }];
        let rendered = render(&entries);
        assert!(rendered.contains("ports: none"), "{rendered}");
    }

    #[test]
    fn tail_lines_keeps_only_the_last_n() {
        let text = (0..60)
            .map(|i| format!("line{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let tail = tail_lines(&text, 50);
        assert_eq!(tail.len(), 50);
        assert_eq!(tail.first().unwrap(), "line10");
        assert_eq!(tail.last().unwrap(), "line59");
    }

    // -- Registry::report(): the full async path, isolated per-test ----------

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

    /// `Ok(text)` or `Err(reason)` per container id — plain strings rather than
    /// `Result<String, RightsizeError>` since `RightsizeError` isn't `Clone` (it wraps
    /// `std::io::Error`, which isn't either), and this map needs to be read
    /// (`.get(...).cloned()`) once per `logs()` call.
    struct FakeBackend {
        logs_by_id: std::collections::HashMap<String, std::result::Result<String, String>>,
    }

    #[async_trait::async_trait]
    impl SandboxBackend for FakeBackend {
        fn name(&self) -> &str {
            "fake"
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
            unimplemented!("not exercised by this test suite")
        }
        async fn logs(&self, handle: &dyn SandboxHandle) -> Result<String> {
            match self.logs_by_id.get(handle.id()) {
                Some(Ok(text)) => Ok(text.clone()),
                Some(Err(reason)) => Err(RightsizeError::Backend(reason.clone())),
                None => Ok(String::new()),
            }
        }
        async fn follow_logs(
            &self,
            _handle: &dyn SandboxHandle,
            _consumer: Box<dyn Fn(String) + Send + Sync>,
        ) -> Result<FollowHandle> {
            unimplemented!("not exercised by this test suite")
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
    async fn register_then_report_matches_the_pinned_golden_vector() {
        let registry = Registry::new();
        let backend: Arc<dyn SandboxBackend> = Arc::new(FakeBackend {
            logs_by_id: [
                (
                    "rz-ab12cd34-redis".to_string(),
                    Ok(
                        "1:M 01 Jan 2026 00:00:00.000 * Ready to accept connections tcp"
                            .to_string(),
                    ),
                ),
                (
                    "rz-ab12cd34-postgres".to_string(),
                    Ok("database system is ready to accept connections".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        });

        registry.register(
            "rz-ab12cd34-redis",
            "redis:7-alpine",
            "127.0.0.1",
            vec![(6379, 49213)],
            backend.clone(),
            "rz-ab12cd34-redis",
            spec("rz-ab12cd34-redis", "redis:7-alpine"),
        );
        registry.register(
            "rz-ab12cd34-postgres",
            "postgres:16-alpine",
            "127.0.0.1",
            vec![(5432, 49214)],
            backend.clone(),
            "rz-ab12cd34-postgres",
            spec("rz-ab12cd34-postgres", "postgres:16-alpine"),
        );

        assert_eq!(registry.report().await, PINNED_REPORT_VECTOR);
    }

    #[tokio::test]
    async fn deregister_removes_the_container_from_the_report() {
        let registry = Registry::new();
        let backend: Arc<dyn SandboxBackend> = Arc::new(FakeBackend {
            logs_by_id: std::collections::HashMap::new(),
        });

        registry.register(
            "rz-dereg-test",
            "redis:7-alpine",
            "127.0.0.1",
            vec![],
            backend,
            "rz-dereg-test",
            spec("rz-dereg-test", "redis:7-alpine"),
        );
        let report = registry.report().await;
        assert!(report.contains("rz-dereg-test"), "{report}");

        registry.deregister("rz-dereg-test");
        assert_eq!(
            registry.report().await,
            "== rightsize diagnostics: no running containers =="
        );
    }

    #[tokio::test]
    async fn a_failing_logs_call_degrades_instead_of_failing_the_whole_report() {
        let registry = Registry::new();
        let backend: Arc<dyn SandboxBackend> = Arc::new(FakeBackend {
            logs_by_id: [(
                "rz-degrade-test".to_string(),
                Err("msb exec timed out after 5s".to_string()),
            )]
            .into_iter()
            .collect(),
        });

        registry.register(
            "rz-degrade-test",
            "redis:7-alpine",
            "127.0.0.1",
            vec![(6379, 49213)],
            backend,
            "rz-degrade-test",
            spec("rz-degrade-test", "redis:7-alpine"),
        );

        let report = registry.report().await;
        assert!(
            report.contains("logs: unavailable (msb exec timed out after 5s)"),
            "{report}"
        );
    }

    // -- DiagnosticsGuard: the automatic panic hook -------------------------

    /// The gating logic itself: prints iff the thread is panicking, never
    /// otherwise. `#[should_panic]`-free per the spec's testing requirement — this
    /// exercises `should_report` directly with both boolean inputs rather than
    /// forcing a real unwind.
    #[test]
    fn diagnostics_guard_should_report_gates_on_panicking_only() {
        assert!(
            !DiagnosticsGuard::should_report(false),
            "a non-panicking drop must not report"
        );
        assert!(
            DiagnosticsGuard::should_report(true),
            "a panicking drop must report"
        );
    }

    /// Construction plus the passing (non-panicking) `Drop` path: a guard built and
    /// dropped on an ordinary, non-unwinding thread must not spawn the
    /// report-fetching thread at all (gated by `should_report` before any of that
    /// machinery runs) and must not panic itself.
    #[test]
    fn diagnostics_guard_constructs_and_drops_cleanly_when_not_panicking() {
        let guard = DiagnosticsGuard::new();
        drop(guard);
    }

    /// `DiagnosticsGuard::default()` is the same construction path as `new()` — both
    /// must be droppable cleanly on the non-panicking path.
    #[test]
    fn diagnostics_guard_default_constructs_and_drops_cleanly_when_not_panicking() {
        let guard = DiagnosticsGuard::default();
        drop(guard);
    }

    #[tokio::test]
    async fn global_registry_is_reachable_through_the_public_diagnostics_entry_point() {
        // Exercises the real process-wide singleton end to end, without asserting
        // exact content (other tests in this crate register/deregister against the
        // same global registry concurrently) — just that `diagnostics()` runs and
        // returns a well-formed report shape either way.
        let report = diagnostics().await;
        assert!(report.starts_with("== rightsize diagnostics:"), "{report}");
    }

    /// Test seam for the panicking-Drop body: `report_on_panic` sends the report it
    /// rendered here (in addition to `eprintln!`ing it), so a test can observe that
    /// the actual `thread::spawn` + throwaway-runtime + `block_on(diagnostics())`
    /// machinery ran end to end, not just the `should_report` gating boolean.
    /// `pub(super)` — called from the parent module's `Drop::drop` via
    /// `report_on_panic`, which is only reachable from a descendant module's
    /// (private) item under Rust's usual visibility rules when marked this way.
    static REPORT_SINK: Mutex<Option<mpsc::Sender<String>>> = Mutex::new(None);

    pub(super) fn notify_report_rendered(report: &str) {
        if let Some(tx) = REPORT_SINK
            .lock()
            .expect("diagnostics test report sink mutex poisoned")
            .as_ref()
        {
            let _ = tx.send(report.to_string());
        }
    }

    /// Exercises the actual panic-drop body — `thread::spawn` + a throwaway Tokio
    /// runtime + `block_on(diagnostics())` + the stderr print — rather than only the
    /// `should_report` gating logic. A real thread panics while holding a
    /// `DiagnosticsGuard`; its `Drop` fires mid-unwind, renders the report on its own
    /// dedicated OS thread, and this test observes that render happened (and saw a
    /// container this test itself registered) via the sink above.
    #[test]
    fn diagnostics_guard_reports_on_a_genuinely_panicking_drop() {
        let (tx, rx) = mpsc::channel();
        *REPORT_SINK
            .lock()
            .expect("diagnostics test report sink mutex poisoned") = Some(tx);

        // A uniquely-named fake container so the rendered report is unambiguously
        // attributable to THIS test's panic, not some other test's container sharing
        // the process-wide registry.
        let backend: Arc<dyn SandboxBackend> = Arc::new(FakeBackend {
            logs_by_id: std::collections::HashMap::new(),
        });
        register(
            "rz-panicking-drop-test",
            "redis:7-alpine",
            "127.0.0.1",
            vec![],
            backend,
            "rz-panicking-drop-test",
            spec("rz-panicking-drop-test", "redis:7-alpine"),
        );

        let join_result = std::thread::spawn(|| {
            let _guard = DiagnosticsGuard::new();
            panic!("intentional panic to exercise DiagnosticsGuard's report-on-panic Drop path");
        })
        .join();
        assert!(
            join_result.is_err(),
            "the spawned thread must have actually panicked"
        );

        let report = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("DiagnosticsGuard's panicking Drop must render and emit a report within 5s");
        assert!(report.contains("rz-panicking-drop-test"), "{report}");

        deregister("rz-panicking-drop-test");
        *REPORT_SINK
            .lock()
            .expect("diagnostics test report sink mutex poisoned") = None;
    }
}
