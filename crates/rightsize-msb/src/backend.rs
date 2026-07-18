//! `MsbCliBackend`: drives `msb` as an ATTACHED child process per container (detached
//! mode never starts the image `ENTRYPOINT` on 0.6.2, only attached mode does),
//! classifies port-bind failures from the child's combined output, and works around
//! `msb logs -f` never exiting on its own once a sandbox stops with a watchdog that
//! does one authoritative, at-most-once tail replay.
//!
//! **Handle-side mutable state:** `Handle` itself is immutable — `spec` plus
//! the backend-assigned `id`. Everything that changes over a container's lifetime (the
//! attached child, its log tail, the exec-tunnels installed for network links) lives in
//! `MsbCliBackend`'s own `handles: Mutex<HashMap<String, HandleState>>`, keyed by
//! `handle.id()`. No method here downcasts `&dyn SandboxHandle` — every one that needs
//! mutable state looks it up by id under that mutex instead.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rightsize::backend::{Capabilities, FollowHandle, NetworkLink, SandboxBackend, SandboxHandle};
use rightsize::error::{Result, RightsizeError};
use rightsize::model::{ContainerSpec, ExecResult};

use crate::commands;
use crate::exec_tunnel::ExecTunnel;
use crate::ls_json;

/// How many trailing lines of a container's combined `msb run` output are kept for
/// diagnostics (port-conflict classification, boot-failure messages).
const TAIL_LINES: usize = 50;

/// `msb run` on a first pull may need to fetch the image; give it plenty of headroom
/// before concluding the sandbox will never reach `Running`.
const FIRST_RUN_TIMEOUT: Duration = Duration::from_secs(600);

/// How often the readiness/watchdog loops poll `msb ls --format json`.
const READINESS_POLL: Duration = Duration::from_millis(300);

const STOP_TIMEOUT: Duration = Duration::from_secs(60);
const EXEC_TIMEOUT: Duration = Duration::from_secs(120);
const LOGS_TIMEOUT: Duration = Duration::from_secs(30);
const ATTACHED_STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// `msb copy` can move an arbitrarily large directory tree — generous headroom
/// over [`EXEC_TIMEOUT`], which only ever runs a single guest command.
const COPY_TIMEOUT: Duration = Duration::from_secs(300);
/// Each step of the checkpoint stop/snapshot/start cycle — a snapshot write is disk
/// I/O over a (typically sparse, small) rootfs, but generous headroom matters more
/// than tightness here since a slow step must not spuriously fail a checkpoint.
const CHECKPOINT_STEP_TIMEOUT: Duration = Duration::from_secs(120);
/// `msb snapshot export`/`snapshot import`/`snapshot list` — moving a
/// (zstd-compressed, typically small) disk snapshot artifact to/from an archive
/// file. Same generous headroom as [`COPY_TIMEOUT`]: a slow disk must not
/// spuriously fail an export/import.
const ARCHIVE_TIMEOUT: Duration = Duration::from_secs(300);

/// An immutable `msb` sandbox reference: its `ContainerSpec` and the name `msb` knows
/// it by (always `spec.name` for this backend). All mutable per-container state lives
/// in the backend's own `handles` map instead — see the module docs.
struct Handle {
    spec: ContainerSpec,
}

impl SandboxHandle for Handle {
    fn id(&self) -> &str {
        &self.spec.name
    }
    fn spec(&self) -> &ContainerSpec {
        &self.spec
    }
}

/// Mutable per-container runtime state, keyed by container id in
/// `MsbCliBackend::handles`. See the module docs for why this isn't stored on the
/// handle itself.
#[derive(Default)]
struct HandleState {
    /// The attached `msb run` child, once `start()` has spawned it.
    attached: Option<Child>,
    /// Exec-tunnels installed by `install_network_links`, torn down on `stop`.
    resources: Vec<ExecTunnel>,
}

/// Drives `msb` as attached child processes. See the module docs for the shape.
pub struct MsbCliBackend {
    msb: PathBuf,
    started_names: Mutex<HashSet<String>>,
    handles: Mutex<HashMap<String, HandleState>>,
}

// Aliases are interpolated into a `sh -c` `/etc/hosts` echo (see `install_network_links`
// in `exec_tunnel`'s sibling install step) — this permissive DNS-label charset exists to
// reject shell-breaking characters, not to enforce a strict hostname grammar.
const ALIAS_CHARSET_OK: fn(&str) -> bool = |s: &str| {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
};

impl MsbCliBackend {
    /// Builds a backend driving the `msb` binary at `msb_path`. Does not itself sweep
    /// orphans or otherwise talk to `msb` — see [`crate::provider::MsbBackendProvider`]
    /// for the constructor real callers use, which does.
    pub fn new(msb_path: PathBuf) -> Self {
        MsbCliBackend {
            msb: msb_path,
            started_names: Mutex::new(HashSet::new()),
            handles: Mutex::new(HashMap::new()),
        }
    }

    fn silently_remove(&self, name: &str) {
        self.invoke_retrying_on_state_db_error(&commands::stop(name), STOP_TIMEOUT);
        self.invoke_retrying_on_state_db_error(&commands::rm(name), STOP_TIMEOUT);
    }

    /// Runs `args` via [`Self::invoke`], retrying once after [`STATE_DB_RETRY_DELAY`]
    /// if the combined stdout/stderr matches msb's state-database error signature
    /// (see [`is_msb_state_db_error`]) — the same one-shot policy the boot path
    /// (`spawn_and_await_running`) and the external watchdog script
    /// ([`watchdog_kill_script`]) both apply to this exact stop/rm pair. This is the
    /// third caller of that policy: [`Self::remove_by_name`] (the init-time sweep's
    /// removal path, via [`Self::silently_remove`]) must retry a state-database hit
    /// exactly like the other two, per spec's "msb specifics" requirement.
    ///
    /// Best-effort throughout, matching [`Self::silently_remove`]'s callers: neither
    /// the first attempt's error nor the retry's is surfaced.
    fn invoke_retrying_on_state_db_error(&self, args: &[String], timeout: Duration) {
        if let Ok(result) = self.invoke(args, timeout) {
            if is_msb_state_db_error(&result.stdout) || is_msb_state_db_error(&result.stderr) {
                std::thread::sleep(STATE_DB_RETRY_DELAY);
                let _ = self.invoke(args, timeout);
            }
        }
    }

    /// Spawns `msb <args>`, feeding it a closed/null stdin (`msb exec` blocks on
    /// stdin EOF, and every msb child needs the same treatment to avoid hanging),
    /// drains stdout/stderr on threads, and waits up to `timeout`. The drain threads are
    /// joined **without a bound** after the process exits (not a fixed cap) so a
    /// large-output command's tail is never truncated by a join deadline.
    fn invoke(&self, args: &[String], timeout: Duration) -> Result<ExecResult> {
        let mut child = spawn_msb_command(|| {
            let mut cmd = Command::new(&self.msb);
            cmd.args(args)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            cmd
        })
        .map_err(|e| {
            RightsizeError::Backend(format!("failed to spawn msb {}: {e}", args.join(" ")))
        })?;

        let stdout_pipe = child.stdout.take().expect("piped stdout");
        let stderr_pipe = child.stderr.take().expect("piped stderr");
        let stdout_buf = Arc::new(Mutex::new(String::new()));
        let stderr_buf = Arc::new(Mutex::new(String::new()));
        let t_out = spawn_line_drain(stdout_pipe, stdout_buf.clone(), |buf, line| {
            buf.push_str(&line);
            buf.push('\n');
        });
        let t_err = spawn_line_drain(stderr_pipe, stderr_buf.clone(), |buf, line| {
            buf.push_str(&line);
            buf.push('\n');
        });

        let deadline = Instant::now() + timeout;
        let status = loop {
            if let Some(status) = child.try_wait().map_err(RightsizeError::from)? {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                let _ = t_out.join();
                let _ = t_err.join();
                return Err(RightsizeError::Backend(format!(
                    "msb {} timed out after {}s and was force-killed — the msb daemon may be \
                     overloaded or unresponsive; retry, or check `msb` directly",
                    args.join(" "),
                    timeout.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_millis(20));
        };

        // The process has already exited, so its pipes will EOF and these drain
        // threads finish promptly — join without a bound rather than a fixed cap,
        // which could truncate the tail of a large-output command that hadn't
        // finished draining yet.
        let _ = t_out.join();
        let _ = t_err.join();

        Ok(ExecResult {
            exit_code: status.code().unwrap_or(-1),
            stdout: stdout_buf.lock().expect("stdout mutex poisoned").clone(),
            stderr: stderr_buf.lock().expect("stderr mutex poisoned").clone(),
        })
    }
}

/// Drains `stream` line-by-line on a dedicated thread, calling `on_line(buf, line)` for
/// each — used both for `invoke`'s stdout/stderr capture and `start`'s combined-output
/// tail. Returns the join handle so callers can wait for it (unbounded, never a fixed
/// cap — see [`MsbCliBackend::invoke`]'s doc for why).
fn spawn_line_drain<T: Send + 'static>(
    mut stream: impl Read + Send + 'static,
    state: Arc<Mutex<T>>,
    on_line: impl Fn(&mut T, String) + Send + 'static,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
            // Split complete lines out of `buf` as they arrive, keeping any trailing
            // partial line for the next read.
            while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
                let line = String::from_utf8_lossy(&line_bytes[..line_bytes.len() - 1]).to_string();
                let mut guard = state.lock().expect("drain state mutex poisoned");
                on_line(&mut guard, line);
            }
        }
        if !buf.is_empty() {
            let line = String::from_utf8_lossy(&buf).to_string();
            let mut guard = state.lock().expect("drain state mutex poisoned");
            on_line(&mut guard, line);
        }
    })
}

/// True if `output` (a `msb run` child's combined stdout/stderr) names a host-port
/// bind conflict. msb has no structured error for this — only free-text diagnostic
/// output — so this is a best-effort message match, same idea as the core container
/// builder's own fallback classifier, kept local to this backend since the wording is
/// msb-specific.
fn is_port_bind_conflict(output: &str) -> bool {
    let m = output.to_lowercase();
    m.contains("address already in use")
        || m.contains("port is already allocated")
        || (m.contains("already in use") && m.contains("port"))
}

/// True if `output` (a `msb run` child's combined stdout/stderr) names msb's image
/// cache error: a manifest/layer index entry pointing at a cache file that isn't on
/// disk. Observed verbatim against a real msb 0.6.3 binary:
///
/// ```text
/// error: image error: cache error at /path/to/.microsandbox/cache/layers/sha256_<64hex>.tar.gz: No such file or directory (os error 2)
/// ```
///
/// Root cause, reproduced locally by racing concurrent `msb run`/`msb pull` of images
/// that share a base layer against one fresh cache: two pulls converting the same
/// shared blob race, and the loser's read of the shared `.tar.gz` finds it already
/// deleted by the winner's post-conversion cleanup. On a fresh CI cache the three
/// floci images (`floci/floci:1.5.30`, `floci/floci-az:0.8.0`, `floci/floci-gcp:0.4.0`)
/// share a base layer, and rightsize's own `sandbox-it` suite boots all three
/// concurrently (separate `#[tokio::test]` functions), so this is a real race this
/// backend's own usage pattern triggers, not just an artificial stress case. Confirmed
/// order-independent: across ten local trials, seven reproduced the error, naming each
/// of the three images as the victim at least once.
///
/// This is deliberately a substring match on the stable parts of msb's wording
/// ("cache error at", "No such file") rather than the full sentence — the path and
/// digest vary per host/image, and msb has no structured/typed error for this.
fn is_image_cache_corruption(output: &str) -> bool {
    output.contains("cache error at") && output.contains("No such file")
}

/// True if `output` (a `msb run` child's combined stdout/stderr) names a failure of
/// msb's own shared SQLite state database. Every msb invocation runs schema
/// migrations against it on startup, and two concurrent invocations can race them —
/// the loser dies before doing any work, with whatever wording matches the migration
/// statement it lost on. Observed verbatim against the real msb 0.6.3 Windows
/// binary, one race, three shapes:
///
/// ```text
/// error: database error: Execution Error: error returned from database: (code: 1) index idx_manifest_layers_unique already exists
/// error: database error: Execution Error: error returned from database: (code: 1) duplicate column name: kind
/// ```
///
/// plus `UNIQUE constraint failed: seaql_migrations.version`. Chasing individual
/// wordings is a losing game — the stable part is msb's own `error: database error:`
/// framing, which is always msb's state database and never the workload's output.
///
/// A boot is never inherently alone even under fully serialized tests: the attached
/// `msb run` child races this backend's own `msb ls` readiness polling (and, on
/// Windows, an active log poller). The migration race is transient by construction;
/// for a state-database failure that is NOT the race, the one-shot retry costs a
/// moment and then propagates the failure with both attempts' output.
fn is_msb_state_db_error(output: &str) -> bool {
    output.contains("error: database error:")
}

/// True if `output` (a `msb run` child's combined stdout/stderr) names a sandbox-
/// name collision — msb refuses to create a second sandbox under a name it already
/// has one for. This backend's own cue, mirrored by
/// `rightsize::reuse::is_name_conflict`'s string fallback (`"already exists"`), for
/// the reuse start flow's "another process won the create race" retry path — a
/// reuse container's name is deterministic (`rz-reuse-<hash>`, not the usual
/// process-unique `rz-<run-id>-<seq>`), so two processes racing to adopt the same
/// identity can genuinely both attempt `msb run` under the same name.
fn is_name_conflict(output: &str) -> bool {
    output.to_lowercase().contains("already exists")
}

/// True if `output` (an `msb snapshot inspect` child's combined stdout/stderr) names
/// a missing snapshot — msb's own "not found" wording, following the same `error:
/// <noun> not found: <ref>` convention already confirmed for images (see the
/// captured `"error: image not found: <ref>"` wording this backend's `image remove`
/// heal path negatively tests against). This is the ONLY signal
/// [`MsbCliBackend::has_checkpoint`] treats as "definitely absent" (`Ok(false)`) —
/// every other nonzero exit (a transient daemon problem, a malformed ref, anything
/// else) surfaces as an error, per `SandboxBackend::has_checkpoint`'s contract: a
/// probe failure must never masquerade as "not there."
fn is_snapshot_not_found(output: &str) -> bool {
    output.to_lowercase().contains("snapshot not found")
}

/// True if `output` (an `msb snapshot import` child's combined stdout/stderr)
/// names msb's "already imported" wording — `error: snapshot already exists:
/// <path>`. For a content-addressed archive this IS success: the artifact is
/// already sitting there under its digest-derived directory, so
/// [`MsbCliBackend::import_checkpoint`] treats it as one, continuing straight to
/// resolving the effective ref exactly like a fresh, successful import would.
fn is_snapshot_already_exists(output: &str) -> bool {
    output.to_lowercase().contains("snapshot already exists")
}

/// Parses the digest-derived directory name `msb snapshot import` unpacked into,
/// out of its own printed output — verified contract: on both a fresh import and
/// an already-exists "success", the last non-empty printed line ends with the
/// artifact path, and that path's OWN basename (not a file inside it) is the
/// digest-derived directory name (e.g. `sha256-b9c0448ee9d54e33`). `None` when the
/// output has no non-empty line at all — msb printed nothing usable.
fn parse_import_digest_dir_name(output: &str) -> Option<String> {
    let last_line = output.lines().rev().find(|line| !line.trim().is_empty())?;
    let path_str = last_line.split_whitespace().last()?;
    Path::new(path_str)
        .file_name()
        .and_then(|f| f.to_str())
        .map(str::to_string)
}

/// Before retrying a boot that hit msb's state-database error — enough for a winning
/// concurrent invocation's migration transaction to commit; the retry's own `msb run`
/// startup dwarfs this either way.
const STATE_DB_RETRY_DELAY: Duration = Duration::from_millis(500);

#[async_trait::async_trait]
impl SandboxBackend for MsbCliBackend {
    fn name(&self) -> &str {
        "microsandbox"
    }

    fn supports_native_networks(&self) -> bool {
        // Networks are emulated via /etc/hosts + exec-stream tunnels — a
        // microVM has no real bridge/subnet to join on this msb build.
        false
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            // Every microsandbox sandbox is its own microVM with its own kernel.
            hardware_isolated: true,
            // Disk-snapshot checkpointing: see `create_checkpoint` below.
            checkpoint: true,
            // The stop/snapshot/start cycle reboots the guest — the workload
            // restarts, unlike docker's undisturbed image commit.
            checkpoint_restarts_workload: true,
        }
    }

    async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
        let id = spec.name.clone();
        self.handles
            .lock()
            .expect("handles mutex poisoned")
            .insert(id, HandleState::default());
        Ok(Box::new(Handle { spec }))
    }

    async fn start(&self, handle: &dyn SandboxHandle) -> Result<()> {
        let id = handle.id().to_string();
        let spec = handle.spec().clone();
        let keep_alive = spec.keep_alive;
        let msb = self.msb.clone();

        // The actual boot work is blocking (child-process spawn + polling `msb ls`),
        // so it runs on a blocking thread rather than tying up the async runtime's
        // worker threads for up to `FIRST_RUN_TIMEOUT`.
        let attached = tokio::task::spawn_blocking(move || spawn_and_await_running(&msb, &spec))
            .await
            .map_err(|e| RightsizeError::Backend(format!("start task panicked: {e}")))??;

        let mut handles = self.handles.lock().expect("handles mutex poisoned");
        if let Some(state) = handles.get_mut(&id) {
            state.attached = Some(attached);
        }
        drop(handles);
        // A `keep_alive` (reuse) sandbox is never added to `started_names` — that set
        // is exactly what `close()` sweeps on this run's own-process shutdown, and a
        // reuse sandbox must survive that (see `ContainerSpec::keep_alive`'s doc).
        if !keep_alive {
            self.started_names
                .lock()
                .expect("started_names mutex poisoned")
                .insert(id);
        }
        Ok(())
    }

    async fn stop(&self, handle: &dyn SandboxHandle) -> Result<()> {
        let id = handle.id().to_string();
        let (resources, attached) = {
            let mut handles = self.handles.lock().expect("handles mutex poisoned");
            match handles.get_mut(&id) {
                Some(state) => (std::mem::take(&mut state.resources), state.attached.take()),
                None => (Vec::new(), None),
            }
        };
        // Close tunnel resources FIRST — they hold their own `msb exec --stream`
        // children that would otherwise be reaped ungracefully by killing the parent.
        // `ExecTunnel::drop` joins its worker thread, which is itself blocking work —
        // do it on a blocking thread too, not directly on this async task (see the
        // note on `invoke`/`spawn_blocking` below for why that matters).
        let name = id.clone();
        let msb = self.msb.clone();
        // The discarded `JoinError` here means a panic inside the blocking closure is
        // swallowed rather than propagated — intentional: `stop` is best-effort
        // teardown, so a panic there must not fail this call or the caller's own
        // cleanup sequence.
        let _ = tokio::task::spawn_blocking(move || {
            drop(resources);
            let _ = invoke_standalone(&msb, &commands::stop(&name), STOP_TIMEOUT);
            if let Some(mut child) = attached {
                reap_attached_child(&mut child);
            }
        })
        .await;
        Ok(())
    }

    async fn remove(&self, handle: &dyn SandboxHandle) -> Result<()> {
        let id = handle.id().to_string();
        let msb = self.msb.clone();
        let name = id.clone();
        tokio::task::spawn_blocking(move || {
            invoke_standalone(&msb, &commands::rm(&name), STOP_TIMEOUT)
        })
        .await
        .map_err(|e| RightsizeError::Backend(format!("remove task panicked: {e}")))??;
        self.started_names
            .lock()
            .expect("started_names mutex poisoned")
            .remove(&id);
        self.handles
            .lock()
            .expect("handles mutex poisoned")
            .remove(&id);
        Ok(())
    }

    async fn exec(&self, handle: &dyn SandboxHandle, cmd: &[String]) -> Result<ExecResult> {
        let argv = commands::exec(handle.id(), cmd);
        let msb = self.msb.clone();
        tokio::task::spawn_blocking(move || invoke_standalone(&msb, &argv, EXEC_TIMEOUT))
            .await
            .map_err(|e| RightsizeError::Backend(format!("exec task panicked: {e}")))?
    }

    /// A fresh `msb logs <name> --tail 1000` invocation, same on every platform. This
    /// is the workload's own output, as distinct from the attached `msb run` child's
    /// pipe (drained in [`spawn_and_await_running`] into a tail kept only for
    /// pre-`Running` crash diagnostics): on Windows the attached process does not relay
    /// guest stdout at all, while `msb logs` does everywhere, so this is the only
    /// channel this method can source from. Never errors on a missing/removed sandbox —
    /// [`invoke_standalone`] only enforces spawn success and the timeout, not the exit
    /// code, so a failing `msb logs` call yields whatever (possibly empty) stdout it
    /// produced rather than an `Err`.
    async fn logs(&self, handle: &dyn SandboxHandle) -> Result<String> {
        let argv = commands::logs(handle.id());
        let msb = self.msb.clone();
        let result =
            tokio::task::spawn_blocking(move || invoke_standalone(&msb, &argv, LOGS_TIMEOUT))
                .await
                .map_err(|e| RightsizeError::Backend(format!("logs task panicked: {e}")))??;
        Ok(result.stdout)
    }

    /// On Windows, `msb logs -f` stays alive for the sandbox's whole run but never
    /// relays a single line to its stdout pipe while the sandbox is Running (confirmed
    /// against a real `windows-2025` hosted runner) — a live-follow child can never
    /// deliver on that channel there, so this dispatches to
    /// [`crate::watchdog::spawn_follow_polling`] instead of the POSIX pipe-follow path.
    /// Everywhere else, `msb logs -f`'s pipe carries lines live and only its
    /// never-exits-on-its-own defect needs working around (see
    /// [`crate::watchdog::spawn_follow`]).
    async fn follow_logs(
        &self,
        handle: &dyn SandboxHandle,
        consumer: Box<dyn Fn(String) + Send + Sync>,
    ) -> Result<FollowHandle> {
        let msb = self.msb.clone();
        let name = handle.id().to_string();
        if cfg!(windows) {
            crate::watchdog::spawn_follow_polling(msb, name, consumer)
        } else {
            crate::watchdog::spawn_follow(msb, name, consumer)
        }
    }

    async fn ensure_network(&self, _network_id: &str) -> Result<()> {
        Ok(()) // emulated via host gateway; nothing to create.
    }

    async fn remove_network(&self, _network_id: &str) -> Result<()> {
        Ok(())
    }

    async fn install_network_links(
        &self,
        handle: &dyn SandboxHandle,
        links: &[NetworkLink],
    ) -> Result<()> {
        if links.is_empty() {
            return Ok(());
        }
        require_no_duplicate_guest_ports(links)?;
        require_aliases_are_valid(links)?;
        require_nc_available(self, handle).await?;
        install_hosts_aliases(self, handle, links).await?;

        let tunnels: Vec<ExecTunnel> = links
            .iter()
            .map(|link| ExecTunnel::new(self.msb.clone(), handle.id().to_string(), link.clone()))
            .collect();
        let mut handles = self.handles.lock().expect("handles mutex poisoned");
        if let Some(state) = handles.get_mut(handle.id()) {
            state.resources.extend(tunnels);
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let names: Vec<String> = self
            .started_names
            .lock()
            .expect("started_names mutex poisoned")
            .iter()
            .cloned()
            .collect();
        let msb = self.msb.clone();
        let _ = tokio::task::spawn_blocking(move || {
            for name in names {
                let _ = invoke_standalone(&msb, &commands::stop(&name), STOP_TIMEOUT);
                let _ = invoke_standalone(&msb, &commands::rm(&name), STOP_TIMEOUT);
            }
        })
        .await;
        Ok(())
    }

    fn cleanup_sync(&self, container_id: &str) {
        // Blocking std I/O only, no Tokio — this runs on the dedicated cleanup thread
        // (see rightsize::cleanup), never in async context.
        let _ = invoke_standalone(&self.msb, &commands::stop(container_id), STOP_TIMEOUT);
        let _ = invoke_standalone(&self.msb, &commands::rm(container_id), STOP_TIMEOUT);
    }

    fn remove_by_name(&self, name: &str) {
        // Promoted straight from this backend's own former orphan-sweep helper — same
        // stop-then-rm, best-effort shape `close()`/`cleanup_sync` already use.
        self.silently_remove(name);
    }

    /// Checks `msb ls --format json`'s running set for `spec.name` — the reuse
    /// adopt path's own query (`rightsize::reuse`). Registers a default (empty)
    /// `HandleState` for the name if this is the first this backend instance has
    /// heard of it (`Entry::or_default`, never overwriting a same-process entry
    /// that already exists from this backend's own earlier `start()` of it), so
    /// later calls on the returned handle (`stop`, `exec`, `logs`) find the same
    /// `handles`-map lookup shape every other method already assumes.
    async fn find_running(&self, spec: &ContainerSpec) -> Result<Option<Box<dyn SandboxHandle>>> {
        let msb = self.msb.clone();
        let running = tokio::task::spawn_blocking(move || running_names_via(&msb))
            .await
            .map_err(|e| RightsizeError::Backend(format!("find_running task panicked: {e}")))??;
        if !running.contains(&spec.name) {
            return Ok(None);
        }
        self.handles
            .lock()
            .expect("handles mutex poisoned")
            .entry(spec.name.clone())
            .or_default();
        Ok(Some(Box::new(Handle { spec: spec.clone() })))
    }

    fn watchdog_kill_command(&self) -> Vec<String> {
        let msb = self.msb.display().to_string();
        watchdog_kill_script(&msb)
    }

    fn backend_binary_path(&self) -> Option<PathBuf> {
        Some(self.msb.clone())
    }

    /// `msb copy -q <host_path> <name>:<container_path>` — through the same
    /// `invoke_standalone` plumbing every other one-shot subcommand in this backend
    /// uses. A non-zero exit surfaces as a backend error carrying msb's stderr,
    /// never a silent success.
    async fn copy_to_container(
        &self,
        handle: &dyn SandboxHandle,
        host_path: &Path,
        container_path: &str,
    ) -> Result<()> {
        let argv = commands::copy_in(host_path, handle.id(), container_path);
        let name = handle.id().to_string();
        let msb = self.msb.clone();
        let result =
            tokio::task::spawn_blocking(move || invoke_standalone(&msb, &argv, COPY_TIMEOUT))
                .await
                .map_err(|e| RightsizeError::Backend(format!("copy task panicked: {e}")))??;
        if result.exit_code != 0 {
            return Err(RightsizeError::Backend(format!(
                "msb copy into sandbox {name} failed (exit {}): {}",
                result.exit_code,
                result.stderr.trim()
            )));
        }
        Ok(())
    }

    /// `msb copy -q <name>:<container_path> <host_path>` — the reverse direction of
    /// [`Self::copy_to_container`], same plumbing and error-surfacing.
    async fn copy_from_container(
        &self,
        handle: &dyn SandboxHandle,
        container_path: &str,
        host_path: &Path,
    ) -> Result<()> {
        let argv = commands::copy_out(handle.id(), container_path, host_path);
        let name = handle.id().to_string();
        let msb = self.msb.clone();
        let result =
            tokio::task::spawn_blocking(move || invoke_standalone(&msb, &argv, COPY_TIMEOUT))
                .await
                .map_err(|e| RightsizeError::Backend(format!("copy task panicked: {e}")))??;
        if result.exit_code != 0 {
            return Err(RightsizeError::Backend(format!(
                "msb copy out of sandbox {name} failed (exit {}): {}",
                result.exit_code,
                result.stderr.trim()
            )));
        }
        Ok(())
    }

    /// Disk-snapshot checkpointing: `msb stop <name>` → `msb snapshot create --from
    /// <name> rz-ckpt-<nonce>` → `msb rm <name>` → a fresh ATTACHED `msb run
    /// --snapshot <ref>` re-boot under the same name/ports/env/memory — see
    /// `msb_checkpoint_cycle` for the orchestration and its own unit tests for the
    /// failure paths. Runs on a blocking thread, like every other multi-step msb
    /// invocation in this backend. The re-boot reuses [`spawn_and_await_running`],
    /// this backend's own normal boot path (already the shape `Container::
    /// from_checkpoint`'s restores use) — see the module docs for why an msb
    /// `start` resume is not used here. The handle's held attached child is
    /// swapped to the new one on success; the ledger and this handle's identity
    /// (name, spec) are untouched either way.
    async fn create_checkpoint(&self, handle: &dyn SandboxHandle, nonce: &str) -> Result<String> {
        let id = handle.id().to_string();
        let snapshot_name = format!("rz-ckpt-{nonce}");
        let msb = self.msb.clone();
        let mut reboot_spec = handle.spec().clone();
        reboot_spec.checkpoint_ref = Some(snapshot_name.clone());
        let name_for_thread = id.clone();
        let snapshot_for_thread = snapshot_name.clone();
        // Taken now (not inside the blocking closure) so a panic there can't leave
        // this handle's `HandleState` holding a stale reference to a child this
        // method is already about to replace.
        let previous_attached = self
            .handles
            .lock()
            .expect("handles mutex poisoned")
            .get_mut(&id)
            .and_then(|state| state.attached.take());

        let new_child = tokio::task::spawn_blocking(move || {
            let mut invoke =
                |args: &[String]| invoke_standalone(&msb, args, CHECKPOINT_STEP_TIMEOUT);
            let mut reboot = || spawn_and_await_running(&msb, &reboot_spec);
            let result = msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                &name_for_thread,
                &snapshot_for_thread,
            );
            // The cycle's own `stop` step already halted the sandbox by the time
            // this returns — reap the previously-attached child (this handle's
            // old foreground `msb run` process) the same way `stop()` does,
            // regardless of the cycle's outcome.
            if let Some(mut child) = previous_attached {
                reap_attached_child(&mut child);
            }
            result
        })
        .await
        .map_err(|e| RightsizeError::Backend(format!("checkpoint task panicked: {e}")))??;

        let mut handles = self.handles.lock().expect("handles mutex poisoned");
        if let Some(state) = handles.get_mut(&id) {
            state.attached = Some(new_child);
        }
        Ok(snapshot_name)
    }

    /// `msb snapshot rm <ref>` — best-effort, matching [`Self::remove_by_name`]'s
    /// own "not found is fine" contract.
    async fn remove_checkpoint(&self, checkpoint_ref: &str) -> Result<()> {
        let msb = self.msb.clone();
        let snapshot_ref = checkpoint_ref.to_string();
        let _ = tokio::task::spawn_blocking(move || {
            invoke_standalone(&msb, &commands::snapshot_rm(&snapshot_ref), STOP_TIMEOUT)
        })
        .await;
        Ok(())
    }

    /// `msb snapshot inspect <ref>` — the named-checkpoint existence probe
    /// (`SandboxBackend::has_checkpoint`, `Checkpoint::find`'s staleness check).
    /// Exit 0 -> `Ok(true)`. A nonzero exit whose output names msb's "not found"
    /// wording (see [`is_snapshot_not_found`]) -> `Ok(false)`. Any other nonzero
    /// exit, or the invocation itself failing to run, surfaces as a
    /// `RightsizeError::Backend` — this SPI forbids a probe failure from resolving
    /// to "absent."
    async fn has_checkpoint(&self, checkpoint_ref: &str) -> Result<bool> {
        let msb = self.msb.clone();
        let snapshot_ref = checkpoint_ref.to_string();
        let result = tokio::task::spawn_blocking(move || {
            invoke_standalone(
                &msb,
                &commands::snapshot_inspect(&snapshot_ref),
                STOP_TIMEOUT,
            )
        })
        .await
        .map_err(|e| RightsizeError::Backend(format!("checkpoint probe task panicked: {e}")))??;

        if result.exit_code == 0 {
            return Ok(true);
        }
        let combined = format!("{}\n{}", result.stdout, result.stderr);
        if is_snapshot_not_found(&combined) {
            return Ok(false);
        }
        Err(RightsizeError::Backend(format!(
            "msb could not inspect checkpoint '{checkpoint_ref}' (exit {}): {}",
            result.exit_code,
            result.stderr.trim()
        )))
    }

    /// `msb snapshot export <ref> <dest>` (never `--with-image` — see
    /// `crate::commands::snapshot_export`'s own doc for why) — the
    /// checkpoint-archive feature's export primitive
    /// (`rightsize::Checkpoint::export_to`).
    async fn export_checkpoint(&self, checkpoint_ref: &str, dest: &Path) -> Result<()> {
        let msb = self.msb.clone();
        let snapshot_ref = checkpoint_ref.to_string();
        let dest = dest.to_path_buf();
        let dest_for_error = dest.clone();
        let result = tokio::task::spawn_blocking(move || {
            invoke_standalone(
                &msb,
                &commands::snapshot_export(&snapshot_ref, &dest),
                ARCHIVE_TIMEOUT,
            )
        })
        .await
        .map_err(|e| RightsizeError::Backend(format!("checkpoint export task panicked: {e}")))??;
        if result.exit_code != 0 {
            return Err(RightsizeError::Backend(format!(
                "msb snapshot export {checkpoint_ref} {} failed (exit {}): {}",
                dest_for_error.display(),
                result.exit_code,
                result.stderr.trim()
            )));
        }
        Ok(())
    }

    /// `msb snapshot import <src_file>`, treating "already exists" as success,
    /// then `msb snapshot list --format json` to confirm the imported snapshot's
    /// DIGEST-DIR NAME is registered — the checkpoint-archive feature's import
    /// primitive (`rightsize::Checkpoint::import_from`). `ref_hint` (the archive
    /// manifest's original ref) plays no role here: msb's import is
    /// content-addressed, so the effective ref is always the digest-dir name
    /// parsed from the import output, never `ref_hint` and never the full
    /// `sha256:<64hex>` digest (msb does not resolve that as a snapshot ref). See
    /// [`msb_import_checkpoint_cycle`] for the orchestration this delegates to.
    async fn import_checkpoint(&self, src_file: &Path, _ref_hint: &str) -> Result<String> {
        let msb = self.msb.clone();
        let archive_path = src_file.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut invoke_import = || {
                invoke_standalone(
                    &msb,
                    &commands::snapshot_import(&archive_path),
                    ARCHIVE_TIMEOUT,
                )
            };
            let mut invoke_list =
                || invoke_standalone(&msb, &commands::snapshot_list(), ARCHIVE_TIMEOUT);
            msb_import_checkpoint_cycle(&mut invoke_import, &mut invoke_list)
        })
        .await
        .map_err(|e| RightsizeError::Backend(format!("checkpoint import task panicked: {e}")))?
    }
}

/// Builds the external, standalone-process kill command the reaping watchdog uses to
/// remove an msb sandbox by name after this library process has already exited (see
/// `SandboxBackend::watchdog_kill_command`'s doc). Two invocations are needed (`stop`
/// then `rm` — msb has no single "kill" verb), each retried once on msb's own
/// state-database error signature (the same transient race
/// [`is_msb_state_db_error`] classifies during a normal boot) — so both are wrapped
/// in one `sh -c`/PowerShell script rather than expressed as a literal argv prefix,
/// keeping the watchdog's own generic script (see `rightsize::reaper::watchdog`)
/// backend-agnostic: it always just runs ONE external command per sandbox name.
#[cfg(unix)]
fn watchdog_kill_script(msb: &str) -> Vec<String> {
    let msb_q = shell_single_quote(msb);
    let script = format!(
        "try() {{ out=$(\"$@\" 2>&1); case \"$out\" in *'error: database error:'*) sleep 0.5; \"$@\" >/dev/null 2>&1 ;; esac; }}; try {msb_q} stop \"$1\"; try {msb_q} rm \"$1\""
    );
    vec!["sh".to_string(), "-c".to_string(), script, "sh".to_string()]
}

#[cfg(windows)]
fn watchdog_kill_script(msb: &str) -> Vec<String> {
    let script = format!(
        "$m = '{msb}'; function Try-Cmd($a) {{ $o = & $m $a $args[0] 2>&1 | Out-String; if ($o -match 'error: database error:') {{ Start-Sleep -Milliseconds 500; & $m $a $args[0] | Out-Null }} }}; Try-Cmd 'stop'; Try-Cmd 'rm'",
        msb = msb.replace('\'', "''")
    );
    vec![
        "powershell".to_string(),
        "-NoProfile".to_string(),
        "-NonInteractive".to_string(),
        "-Command".to_string(),
        script,
    ]
}

/// Single-quotes `s` for embedding inside a POSIX `sh -c '...'` script — msb's own
/// install path is fully within this process's control (never untrusted input), but
/// quoting it defensively costs nothing and keeps a path containing spaces working.
#[cfg(unix)]
fn shell_single_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// A single `msb run` attempt's outcome when the child exits before reaching
/// `Running`: either a classified error ready to surface, or a cache-corruption
/// signature (see [`is_image_cache_corruption`]) that [`spawn_and_await_running`]
/// gets one chance to heal and retry before giving up.
enum PreRunningFailure {
    CacheCorruption { output: String },
    StateDbError { output: String },
    Other(RightsizeError),
}

/// Runs on a blocking thread: spawns `msb run <spec's argv>` attached (no `-d`),
/// polls `msb ls --format json` until the sandbox reaches `Running`, and returns the
/// live child for `start()` to keep around.
///
/// Two classified transient failures are retried once each. A first attempt that
/// hit msb's state-database error — usually the startup-migration race (see
/// [`is_msb_state_db_error`]) — is retried after a short delay with no heal step;
/// the race is transient by construction.
/// On a first attempt that exits before `Running` with msb's image-cache-corruption
/// signature (see [`is_image_cache_corruption`]), this heals the affected image's
/// cache entry (see [`heal_image_cache`]) and retries the boot exactly once — the
/// failed first attempt never reached `Running`, so it never touched `handles` or
/// `started_names` (both are populated by `start()` only after this function
/// returns `Ok`), and its child has already exited, so there is no live process or
/// registered cleanup state left over to double-register on the retry. A second
/// failure (whether cache corruption again or anything else) surfaces an actionable
/// error naming what was attempted instead of retrying further.
fn spawn_and_await_running(msb: &Path, spec: &ContainerSpec) -> Result<Child> {
    match try_spawn_and_await_running(msb, spec) {
        Ok(child) => Ok(child),
        Err(PreRunningFailure::Other(e)) => Err(e),
        Err(PreRunningFailure::StateDbError { output }) => {
            // Usually the startup-migration race, transient by construction (see
            // is_msb_state_db_error): the winning msb invocation's migration commits
            // and a retried boot finds the schema in place. No heal step, one retry,
            // second failure propagates — the same one-shot policy as the image-cache
            // heal below.
            std::thread::sleep(STATE_DB_RETRY_DELAY);
            match try_spawn_and_await_running(msb, spec) {
                Ok(child) => Ok(child),
                Err(PreRunningFailure::StateDbError {
                    output: retry_output,
                }) => Err(RightsizeError::Backend(format!(
                    "msb run for sandbox {} hit msb's state-database error twice in a \
                         row — the usual cause (concurrent msb invocations racing startup \
                         migrations) is transient and one retry covers it, so this looks \
                         like real state-database trouble on this \
                         host.\nfirst attempt:\n{output}\nafter retry:\n{retry_output}",
                    spec.name,
                ))),
                Err(PreRunningFailure::CacheCorruption {
                    output: retry_output,
                }) => Err(RightsizeError::Backend(format!(
                    "msb run for sandbox {} exited before reaching Running:\n{retry_output}",
                    spec.name,
                ))),
                Err(PreRunningFailure::Other(e)) => Err(e),
            }
        }
        Err(PreRunningFailure::CacheCorruption { output }) => {
            let heal_result = heal_image_cache(msb, &spec.image);
            match try_spawn_and_await_running(msb, spec) {
                Ok(child) => Ok(child),
                Err(PreRunningFailure::Other(e)) => Err(e),
                Err(PreRunningFailure::StateDbError {
                    output: retry_output,
                }) => Err(RightsizeError::Backend(format!(
                    "msb run for sandbox {} exited before reaching Running:\n{retry_output}",
                    spec.name,
                ))),
                Err(PreRunningFailure::CacheCorruption {
                    output: retry_output,
                }) => Err(RightsizeError::Backend(format!(
                    "msb run for sandbox {} hit its image cache error twice in a row for \
                         image '{}', even after removing that image's cache entry ({}) and \
                         retrying — this is likely a deeper cache corruption than this backend's \
                         one-shot heal covers; try clearing the msb image cache by hand \
                         (`msb image prune` or removing the cache directory under MSB_HOME).\n\
                         first attempt:\n{output}\nafter heal + retry:\n{retry_output}",
                    spec.name,
                    spec.image,
                    describe_heal_result(&heal_result),
                ))),
            }
        }
    }
}

/// One `msb run` attempt: spawns the child, polls until `Running`, and returns either
/// the live child or a classified [`PreRunningFailure`]. Never retries by itself —
/// [`spawn_and_await_running`] is the only caller and owns the one-shot heal+retry
/// policy.
///
/// The tail drained here carries msb's own boot output only — registry/pull errors,
/// a crash before the sandbox exists — never the workload's. `logs()` never reads
/// from it; workload output always comes from a `msb logs` invocation.
fn try_spawn_and_await_running(
    msb: &Path,
    spec: &ContainerSpec,
) -> std::result::Result<Child, PreRunningFailure> {
    let argv = commands::run(spec);
    let mut child = spawn_msb_command(|| {
        let mut cmd = Command::new(msb);
        cmd.args(&argv)
            .stdin(Stdio::null()) // msb exec blocks on stdin EOF; give every child a closed stdin.
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd
    })
    .map_err(|e| {
        PreRunningFailure::Other(RightsizeError::Backend(format!(
            "failed to spawn msb {}: {e}",
            argv.join(" ")
        )))
    })?;

    let stdout_pipe = child.stdout.take().expect("piped stdout");
    let stderr_pipe = child.stderr.take().expect("piped stderr");
    let tail: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
    let t_out = spawn_tail_drain(stdout_pipe, tail.clone());
    let t_err = spawn_tail_drain(stderr_pipe, tail.clone());

    let deadline = Instant::now() + FIRST_RUN_TIMEOUT;
    loop {
        let status = child
            .try_wait()
            .map_err(|e| PreRunningFailure::Other(RightsizeError::from(e)))?;
        if let Some(status) = status {
            let _ = t_out.join();
            let _ = t_err.join();
            let output = tail
                .lock()
                .expect("tail mutex poisoned")
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join("\n");
            return Err(if is_image_cache_corruption(&output) {
                PreRunningFailure::CacheCorruption { output }
            } else if is_msb_state_db_error(&output) {
                PreRunningFailure::StateDbError { output }
            } else if is_port_bind_conflict(&output) {
                PreRunningFailure::Other(RightsizeError::PortBindConflict {
                    message: format!(
                        "msb run for sandbox {} could not bind a host port: {output}",
                        spec.name
                    ),
                    source: None,
                })
            } else if is_name_conflict(&output) {
                PreRunningFailure::Other(RightsizeError::NameConflict {
                    message: format!(
                        "msb run for sandbox {} could not start — a sandbox with this name \
                         already exists: {output}",
                        spec.name
                    ),
                    source: None,
                })
            } else {
                PreRunningFailure::Other(RightsizeError::Backend(format!(
                    "msb run for sandbox {} exited (code {}) before reaching Running — check the \
                     image entrypoint and `msb run` output below:\n{output}",
                    spec.name,
                    status.code().unwrap_or(-1)
                )))
            });
        }
        match running_names_via(msb) {
            Ok(names) if names.contains(&spec.name) => return Ok(child),
            _ => {}
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = t_out.join();
            let _ = t_err.join();
            let output = tail
                .lock()
                .expect("tail mutex poisoned")
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join("\n");
            return Err(PreRunningFailure::Other(RightsizeError::Backend(format!(
                "Sandbox {} did not reach Running within {}s — this can mean a slow image pull, \
                 a crash-looping entrypoint, or msb itself being unresponsive; last output:\n{output}",
                spec.name,
                FIRST_RUN_TIMEOUT.as_secs()
            ))));
        }
        std::thread::sleep(READINESS_POLL);
    }
}

/// Heals msb's image-cache-corruption signature by removing the affected image's
/// cache entry (`msb image remove <image>`), scoped to that one image reference —
/// never the whole cache directory, and never any sandbox state (sandboxes live in
/// msb's own `db/msb.db` `sandbox`/`sandbox_rootfs` tables, untouched by `image
/// remove`).
///
/// Two corruption shapes were found empirically and this heals both with the same
/// one command:
///
/// - The failing image's own manifest was never committed to msb's cache database (a
///   concurrent pull lost the race for a shared base layer before its own manifest
///   write landed) — here `image remove` reports "image not found" (nothing to
///   remove) and the retry succeeds anyway, because by the time it runs the
///   concurrent winner has finished materializing the shared layer. This is the
///   common case reproduced locally: racing `msb run`/`msb pull` of the three floci
///   images against one fresh cache hit this in 7 of 10 trials, naming each of the
///   three images as the victim at least once.
/// - The failing image's manifest IS committed but the cache file backing one of its
///   layers is gone (e.g. a CI cache restore that dropped some blobs but kept the
///   database) — here `image remove` actually clears the stale entry, and the retry's
///   `msb run` re-pulls the image from scratch.
///
/// Errors from the `image remove` invocation itself (including "image not found") are
/// intentionally swallowed here — this is a best-effort heal, and the real signal is
/// whether the retried `msb run` succeeds, not whether removal reported success.
fn heal_image_cache(msb: &Path, image: &str) -> Result<ExecResult> {
    invoke_standalone(msb, &commands::image_remove(image), STOP_TIMEOUT)
}

/// Renders a heal attempt's outcome for the second-failure error message — never
/// panics on the heal's own failure (e.g. "image not found"), since that outcome is
/// itself informative to whoever reads the surfaced error.
fn describe_heal_result(result: &Result<ExecResult>) -> String {
    match result {
        Ok(r) if r.exit_code == 0 => "removed".to_string(),
        Ok(r) => format!(
            "`msb image remove` exited {}: {}",
            r.exit_code,
            r.stderr.trim()
        ),
        Err(e) => format!("`msb image remove` itself failed to run: {e}"),
    }
}

/// A standalone (non-`&self`) version of `invoke`, for call sites (the blocking `stop`
/// task, `cleanup_sync`) that don't have easy access to `&MsbCliBackend`.
/// Spawns an msb child, retrying briefly when `execve` refuses with `ETXTBSY`
/// ("text file busy", os error 26 on unix): a fork from another thread can hold a
/// short-lived copy of a write descriptor for a just-written executable — the msb
/// binary right after the provisioner writes it, or a test's fake binary — and the
/// kernel rejects executing any file someone holds open for write. The writer
/// closes within microseconds, so a few paced attempts close the window; every
/// other spawn failure surfaces unchanged on the first try. Windows has no
/// `ETXTBSY`, so the retry arm never matches there. [`build`] must construct a
/// fresh `Command` per call — `Command` is consumed by a failed spawn attempt.
pub(crate) fn spawn_msb_command(build: impl Fn() -> Command) -> std::io::Result<Child> {
    const ETXTBSY: i32 = 26;
    let mut delay = Duration::from_millis(10);
    for _ in 0..5 {
        match build().spawn() {
            Err(e) if e.raw_os_error() == Some(ETXTBSY) => {
                std::thread::sleep(delay);
                delay *= 2;
            }
            other => return other,
        }
    }
    build().spawn()
}

fn invoke_standalone(msb: &Path, args: &[String], timeout: Duration) -> Result<ExecResult> {
    let mut child = spawn_msb_command(|| {
        let mut cmd = Command::new(msb);
        cmd.args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd
    })
    .map_err(|e| RightsizeError::Backend(format!("failed to spawn msb {}: {e}", args.join(" "))))?;

    let stdout_pipe = child.stdout.take().expect("piped stdout");
    let stderr_pipe = child.stderr.take().expect("piped stderr");
    let stdout_buf = Arc::new(Mutex::new(String::new()));
    let stderr_buf = Arc::new(Mutex::new(String::new()));
    let t_out = spawn_line_drain(stdout_pipe, stdout_buf.clone(), |buf, line| {
        buf.push_str(&line);
        buf.push('\n');
    });
    let t_err = spawn_line_drain(stderr_pipe, stderr_buf.clone(), |buf, line| {
        buf.push_str(&line);
        buf.push('\n');
    });

    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(RightsizeError::from)? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = t_out.join();
            let _ = t_err.join();
            return Err(RightsizeError::Backend(format!(
                "msb {} timed out after {}s and was force-killed",
                args.join(" "),
                timeout.as_secs()
            )));
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let _ = t_out.join();
    let _ = t_err.join();
    Ok(ExecResult {
        exit_code: status.code().unwrap_or(-1),
        stdout: stdout_buf.lock().expect("stdout mutex poisoned").clone(),
        stderr: stderr_buf.lock().expect("stderr mutex poisoned").clone(),
    })
}

/// Orchestrates the checkpoint feature's stop → snapshot → rm → re-boot cycle
/// against `name` (a running sandbox), taking a snapshot named `snapshot_name`,
/// via `invoke` (the plain one-shot `stop`/`snapshot create`/`rm` commands) and
/// `reboot` (the actual re-boot of a fresh attached sandbox from that snapshot,
/// under the same name). Both are injected rather than hardcoded — `invoke` as a
/// pure argv-in/`ExecResult`-out closure, `reboot` generic over its success type
/// `T` (production instantiates it with [`spawn_and_await_running`], returning the
/// live [`Child`] this backend needs to hold; tests instantiate it with a bare
/// `Result<()>`) — so this orchestration logic (the ordering, and which steps
/// short-circuit which) is unit-testable without a real `msb` binary or child
/// process.
///
/// This replaces the former `msb stop` → `msb snapshot create` → `msb start`
/// cycle: `msb start` is `Sandbox::start_detached` in upstream microsandbox, whose
/// detached spawn requests `CREATE_BREAKAWAY_FROM_JOB` on Windows — denied with
/// `ERROR_ACCESS_DENIED` whenever msb runs inside a job object that doesn't grant
/// breakaway rights, which is exactly a Gradle/cargo test process on a Windows CI
/// runner. The denial is deterministic, not transient, so no retry fixes it.
/// Attached `msb run` (this backend's normal boot, including `--snapshot` boots)
/// has no such problem, so once the snapshot exists, the stopped sandbox is
/// removed and this backend's own create/boot path re-creates it from that
/// snapshot instead of resuming it.
///
/// Failure handling:
/// - `msb stop` failing short-circuits before any snapshot/rm/reboot attempt.
/// - `msb snapshot create` failing leaves the sandbox stopped (never removed) and
///   surfaces an error naming `msb start <name>` as the by-hand remedy — no
///   best-effort restart-for-the-caller here, since that restart would be exactly
///   the broken `msb start` call this cycle no longer makes.
/// - A failure to reboot after a successful snapshot (whether `rm` silently failed
///   to free the name, or the reboot itself failed) surfaces an error naming
///   `snapshot_name` and `Container::from_checkpoint(...)` as the recovery path —
///   the sandbox is gone, but its state lives on in the snapshot.
fn msb_checkpoint_cycle<T>(
    invoke: &mut dyn FnMut(&[String]) -> Result<ExecResult>,
    reboot: &mut dyn FnMut() -> Result<T>,
    name: &str,
    snapshot_name: &str,
) -> Result<T> {
    let stop = invoke(&commands::stop(name))?;
    if stop.exit_code != 0 {
        return Err(RightsizeError::Backend(format!(
            "msb stop {name} failed before taking a checkpoint (exit {}): {}",
            stop.exit_code,
            stop.stderr.trim()
        )));
    }

    match invoke(&commands::snapshot_create(name, snapshot_name)) {
        Ok(r) if r.exit_code == 0 => {}
        Ok(r) => {
            return Err(RightsizeError::Backend(format!(
                "msb snapshot create --from {name} {snapshot_name} failed (exit {}): {} — the \
                 sandbox is left stopped; run `msb start {name}` by hand to bring it back up",
                r.exit_code,
                r.stderr.trim()
            )));
        }
        Err(e) => {
            return Err(RightsizeError::Backend(format!(
                "msb snapshot create --from {name} {snapshot_name} failed: {e} — the sandbox is \
                 left stopped; run `msb start {name}` by hand to bring it back up"
            )));
        }
    }

    // Disk state now lives in the snapshot — remove the stopped sandbox so the
    // reboot below can recreate it under the same name. Best-effort: even if this
    // itself fails, the reboot's own `msb run --name <name>` surfaces the
    // consequence (a lingering sandbox collides on the name) as the error below.
    let _ = invoke(&commands::rm(name));

    reboot().map_err(|e| {
        RightsizeError::Backend(format!(
            "re-booting sandbox {name} from checkpoint {snapshot_name} failed ({e}) — the \
             sandbox was removed but its state is preserved in checkpoint {snapshot_name}, \
             restorable via Container::from_checkpoint(...)"
        ))
    })
}

/// Orchestrates the checkpoint-archive feature's import cycle: `msb snapshot
/// import` (treating "already exists" as success — see
/// [`is_snapshot_already_exists`]) then `msb snapshot list --format json` to
/// confirm the imported snapshot's digest-dir name is registered, via
/// `invoke_import`/`invoke_list` (both injected, mirroring
/// [`msb_checkpoint_cycle`]'s own shape) so this orchestration is unit-testable
/// without a real `msb` binary. Returns the digest-dir name itself —
/// [`MsbCliBackend::import_checkpoint`]'s effective ref — never the full
/// `sha256:<64hex>` digest, which msb does not accept as a snapshot ref.
///
/// Failure handling: an import failure that ISN'T "already exists" surfaces with
/// its stderr and never reaches `snapshot list` at all. A successful (or
/// already-exists) import whose output has no parseable digest-dir name, or a
/// `snapshot list` failure, or a digest-dir name that matches no listed entry,
/// each surface as their own actionable error.
fn msb_import_checkpoint_cycle(
    invoke_import: &mut dyn FnMut() -> Result<ExecResult>,
    invoke_list: &mut dyn FnMut() -> Result<ExecResult>,
) -> Result<String> {
    let import_result = invoke_import()?;
    let combined = format!("{}\n{}", import_result.stdout, import_result.stderr);
    if import_result.exit_code != 0 && !is_snapshot_already_exists(&combined) {
        return Err(RightsizeError::Backend(format!(
            "msb snapshot import failed (exit {}): {}",
            import_result.exit_code,
            import_result.stderr.trim()
        )));
    }

    let digest_dir_name = parse_import_digest_dir_name(&combined).ok_or_else(|| {
        RightsizeError::Backend(format!(
            "msb snapshot import succeeded but its output did not end with a recognizable \
             artifact path to resolve a digest from:\n{combined}"
        ))
    })?;

    let list_result = invoke_list()?;
    if list_result.exit_code != 0 {
        return Err(RightsizeError::Backend(format!(
            "msb snapshot list --format json failed while resolving the imported checkpoint's \
             digest (exit {}): {}",
            list_result.exit_code,
            list_result.stderr.trim()
        )));
    }

    crate::snapshot_json::confirm_digest_dir_name(&list_result.stdout, &digest_dir_name).ok_or_else(
        || {
            RightsizeError::Backend(format!(
                "msb snapshot list --format json had no entry matching the imported snapshot's \
                 digest directory '{digest_dir_name}'"
            ))
        },
    )
}

/// Waits (bounded by [`ATTACHED_STOP_TIMEOUT`]) for an already-signalled-to-stop
/// attached `msb run` child to exit on its own, force-killing it if the deadline
/// passes first. Shared by [`MsbCliBackend::stop`] (after its own `msb stop`
/// invocation) and `create_checkpoint` (after the checkpoint cycle's `stop` step
/// halts the sandbox out from under its previously-held attached child).
fn reap_attached_child(child: &mut Child) {
    let deadline = Instant::now() + ATTACHED_STOP_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => break,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                break;
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

/// A standalone `running_sandbox_names()`, for the blocking `spawn_and_await_running`
/// helper (and the `follow_logs` watchdog, `crate::watchdog`) which have no
/// `&MsbCliBackend` to call the method form on.
pub(crate) fn running_names_via(msb: &Path) -> Result<HashSet<String>> {
    let out = invoke_standalone(msb, &commands::ls(), LOGS_TIMEOUT)?;
    Ok(ls_json::running_names(&out.stdout))
}

/// Test-only public seam onto [`running_names_via`], for the `sandbox-it` integration
/// suite's parity check against a real `msb ls` — the crate's own unit tests exercise
/// the parse logic in `ls_json` directly and don't need this; an external
/// `tests/*.rs` integration test has no `pub(crate)` access, hence this thin `pub`
/// wrapper.
#[cfg(feature = "sandbox-it")]
pub fn running_sandbox_names(msb: &Path) -> Result<HashSet<String>> {
    running_names_via(msb)
}

/// Test-only public seam onto [`is_image_cache_corruption`], for the `sandbox-it`
/// corrupted-cache integration test's setup helper, which needs to recognize the
/// corruption signature in raw `msb run` output it captures itself (deliberately
/// bypassing this backend, to drive the concurrent pull race that produces the
/// corruption) — an external `tests/*.rs` integration test has no access to this
/// private module function otherwise.
#[cfg(feature = "sandbox-it")]
pub fn is_image_cache_corruption_for_test(output: &str) -> bool {
    is_image_cache_corruption(output)
}

/// A standalone one-shot logs fetch, for the `follow_logs` watchdog's authoritative
/// tail replay (`crate::watchdog::flush_tail_once`), which has no `&MsbCliBackend`
/// either.
pub(crate) fn invoke_logs_for_watchdog(msb: &Path, name: &str) -> Result<String> {
    Ok(invoke_standalone(msb, &commands::logs(name), LOGS_TIMEOUT)?.stdout)
}

/// A one-shot `msb logs` fetch that surfaces a non-zero exit as `Err` instead of
/// silently handing back whatever (possibly empty) stdout accompanied it — unlike
/// [`invoke_logs_for_watchdog`], which callers use precisely because a missing/
/// removed sandbox legitimately exits non-zero with harmless empty stdout there.
/// The Windows log poller (`crate::watchdog::spawn_follow_polling`) needs the
/// distinction the other helper doesn't: an `msb logs` invocation that fails because
/// msb itself hit an internal error (e.g. the Windows sqlite migration/contention
/// race — `error: database error: ... UNIQUE constraint failed`) prints that error
/// to stderr and exits non-zero with EMPTY stdout, which is indistinguishable from a
/// genuinely-empty log unless the exit code is checked — and treating that failure
/// as "confirmed empty content" is exactly what let the poller finalize delivery
/// with nothing, observed on real `windows-2025` CI runs.
pub(crate) fn logs_snapshot_for_poller(msb: &Path, name: &str) -> Result<String> {
    let result = invoke_standalone(msb, &commands::logs(name), LOGS_TIMEOUT)?;
    if result.exit_code != 0 {
        return Err(RightsizeError::Backend(format!(
            "msb logs {name} --tail 1000 exited {}: {}",
            result.exit_code,
            result.stderr.trim()
        )));
    }
    Ok(result.stdout)
}

/// A one-shot `msb ls` fetch that surfaces a non-zero exit as `Err` — the same
/// distinction [`logs_snapshot_for_poller`] draws, and for the same reason: `msb ls`
/// failing on the Windows sqlite race prints its error to stderr and exits non-zero
/// with stdout that is not valid JSON (or empty), which `ls_json::running_names`'s
/// tolerant parser would otherwise silently read as "no sandboxes running" — the
/// Windows log poller must not mistake that for "this sandbox has stopped."
pub(crate) fn running_names_for_poller(msb: &Path) -> Result<HashSet<String>> {
    let result = invoke_standalone(msb, &commands::ls(), LOGS_TIMEOUT)?;
    if result.exit_code != 0 {
        return Err(RightsizeError::Backend(format!(
            "msb ls --format json exited {}: {}",
            result.exit_code,
            result.stderr.trim()
        )));
    }
    Ok(ls_json::running_names(&result.stdout))
}

/// Drains `stream` line-by-line into `tail`, keeping only the last [`TAIL_LINES`] —
/// used for the `start()` boot-diagnostics tail, as opposed to [`spawn_line_drain`]'s
/// full-buffer capture (`invoke`'s stdout/stderr, where nothing is ever discarded).
fn spawn_tail_drain(
    mut stream: impl Read + Send + 'static,
    tail: Arc<Mutex<VecDeque<String>>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let push_line = |tail: &Arc<Mutex<VecDeque<String>>>, line: String| {
            let mut guard = tail.lock().expect("tail mutex poisoned");
            guard.push_back(line);
            if guard.len() > TAIL_LINES {
                guard.pop_front();
            }
        };
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
            while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
                let line = String::from_utf8_lossy(&line_bytes[..line_bytes.len() - 1]).to_string();
                push_line(&tail, line);
            }
        }
        if !buf.is_empty() {
            push_line(&tail, String::from_utf8_lossy(&buf).to_string());
        }
    })
}

/// Rejects two siblings on the same network exposing the same guest port — installing
/// tunnels for both would race the same in-guest listener port.
fn require_no_duplicate_guest_ports(links: &[NetworkLink]) -> Result<()> {
    let mut seen = HashSet::new();
    for link in links {
        if !seen.insert(link.guest_port) {
            return Err(RightsizeError::unsupported(
                format!(
                    "two siblings exposing the same guest port {} on one network",
                    link.guest_port
                ),
                "microsandbox",
            ));
        }
    }
    Ok(())
}

/// Aliases are interpolated straight into `echo '127.0.0.1 $alias' >> /etc/hosts`
/// inside `sh -c` — a shell-metacharacter alias could break out of that quoting.
/// Validated against a permissive DNS-label charset before shelling out at all — a
/// fail-fast guard, not a full hostname grammar check.
fn require_aliases_are_valid(links: &[NetworkLink]) -> Result<()> {
    let mut seen = HashSet::new();
    for link in links {
        if !seen.insert(link.alias.clone()) {
            continue;
        }
        if !ALIAS_CHARSET_OK(&link.alias) {
            return Err(RightsizeError::unsupported_with_remedy(
                format!("network alias '{}'", link.alias),
                "microsandbox",
                "use a valid DNS label instead (allowed: letters, digits, '.', '_', '-')",
            ));
        }
    }
    Ok(())
}

async fn require_nc_available(backend: &MsbCliBackend, handle: &dyn SandboxHandle) -> Result<()> {
    let probe = backend
        .exec(
            handle,
            &[
                "sh".to_string(),
                "-c".to_string(),
                "command -v nc".to_string(),
            ],
        )
        .await?;
    if probe.exit_code != 0 {
        return Err(RightsizeError::unsupported_with_remedy(
            format!(
                "network links (no nc/busybox in consumer image '{}')",
                handle.spec().image
            ),
            "microsandbox",
            "run this test with RIGHTSIZE_BACKEND=docker instead",
        ));
    }
    Ok(())
}

async fn install_hosts_aliases(
    backend: &MsbCliBackend,
    handle: &dyn SandboxHandle,
    links: &[NetworkLink],
) -> Result<()> {
    let mut distinct_aliases = Vec::new();
    for link in links {
        if !distinct_aliases.contains(&link.alias) {
            distinct_aliases.push(link.alias.clone());
        }
    }
    let hosts_entries = distinct_aliases
        .iter()
        .map(|alias| format!("echo '127.0.0.1 {alias}' >> /etc/hosts"))
        .collect::<Vec<_>>()
        .join("; ");
    let result = backend
        .exec(handle, &["sh".to_string(), "-c".to_string(), hosts_entries])
        .await?;
    if result.exit_code != 0 {
        return Err(RightsizeError::Backend(format!(
            "failed to install /etc/hosts aliases in {}: {}",
            handle.id(),
            result.stderr
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn capabilities_report_hardware_isolation_and_checkpoint_that_restarts_the_workload() {
        let backend = MsbCliBackend::new(PathBuf::from("/opt/msb/bin/msb"));
        let caps = backend.capabilities();
        assert!(
            caps.hardware_isolated,
            "each msb sandbox is its own microVM"
        );
        assert!(caps.checkpoint, "disk-snapshot checkpointing is supported");
        assert!(
            caps.checkpoint_restarts_workload,
            "the stop/snapshot/start cycle reboots the guest"
        );
    }

    #[test]
    fn msb_checkpoint_cycle_happy_path_drives_stop_snapshot_rm_then_reboot_in_order() {
        let log: RefCell<Vec<String>> = RefCell::new(Vec::new());
        let result = {
            let mut invoke = |args: &[String]| {
                log.borrow_mut().push(args.join(" "));
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            };
            let mut reboot = || -> Result<()> {
                log.borrow_mut().push("reboot".to_string());
                Ok(())
            };
            msb_checkpoint_cycle(&mut invoke, &mut reboot, "rz-abc-1", "rz-ckpt-deadbeefcafe")
        };
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(
            *log.borrow(),
            vec![
                commands::stop("rz-abc-1").join(" "),
                commands::snapshot_create("rz-abc-1", "rz-ckpt-deadbeefcafe").join(" "),
                commands::rm("rz-abc-1").join(" "),
                "reboot".to_string(),
            ]
        );
    }

    #[test]
    fn msb_checkpoint_cycle_snapshot_failure_leaves_the_sandbox_stopped_and_skips_rm_and_reboot() {
        let mut calls: Vec<Vec<String>> = Vec::new();
        let mut reboot_called = false;
        let result = {
            let mut invoke = |args: &[String]| {
                calls.push(args.to_vec());
                if args[0] == "snapshot" {
                    Ok(ExecResult {
                        exit_code: 1,
                        stdout: String::new(),
                        stderr: "disk full".to_string(),
                    })
                } else {
                    Ok(ExecResult {
                        exit_code: 0,
                        stdout: String::new(),
                        stderr: String::new(),
                    })
                }
            };
            let mut reboot = || -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(&mut invoke, &mut reboot, "rz-abc-1", "rz-ckpt-deadbeefcafe")
        };
        let err = result.expect_err("a failed snapshot step must propagate as an error");
        let msg = err.to_string();
        assert!(msg.contains("disk full"), "{msg}");
        assert!(msg.contains("left stopped"), "{msg}");
        assert!(msg.contains("msb start rz-abc-1"), "{msg}");
        assert_eq!(
            calls.len(),
            2,
            "a failed snapshot step must not be followed by rm: {calls:?}"
        );
        assert!(
            !reboot_called,
            "a failed snapshot step must not be followed by a reboot attempt"
        );
    }

    #[test]
    fn msb_checkpoint_cycle_short_circuits_before_any_snapshot_rm_or_reboot_if_stop_itself_fails() {
        let mut calls: Vec<Vec<String>> = Vec::new();
        let mut reboot_called = false;
        let result = {
            let mut invoke = |args: &[String]| {
                calls.push(args.to_vec());
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: "boom".to_string(),
                })
            };
            let mut reboot = || -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(&mut invoke, &mut reboot, "rz-abc-1", "rz-ckpt-deadbeefcafe")
        };
        assert!(result.is_err());
        assert_eq!(
            calls.len(),
            1,
            "a failed stop must short-circuit before any snapshot/rm/reboot attempt: {calls:?}"
        );
        assert!(!reboot_called);
    }

    #[test]
    fn msb_checkpoint_cycle_reboot_failure_after_a_successful_snapshot_names_the_ref_and_the_recovery_path()
     {
        let mut calls: Vec<Vec<String>> = Vec::new();
        let result = {
            let mut invoke = |args: &[String]| {
                calls.push(args.to_vec());
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            };
            let mut reboot = || -> Result<()> { Err(RightsizeError::Backend("boom".to_string())) };
            msb_checkpoint_cycle(&mut invoke, &mut reboot, "rz-abc-1", "rz-ckpt-deadbeefcafe")
        };
        let err = result.expect_err("a failed reboot must surface, not be swallowed");
        let msg = err.to_string();
        assert!(msg.contains("boom"), "{msg}");
        assert!(msg.contains("rz-ckpt-deadbeefcafe"), "{msg}");
        assert!(msg.contains("Container::from_checkpoint"), "{msg}");
        assert_eq!(
            calls.len(),
            3,
            "rm must still be attempted before a reboot failure surfaces: {calls:?}"
        );
        assert_eq!(calls[2], commands::rm("rz-abc-1"));
    }

    // ---- checkpoint-archive import: digest parsing + already-exists handling ----

    #[test]
    fn is_snapshot_already_exists_matches_the_verified_wording() {
        assert!(is_snapshot_already_exists(
            "error: snapshot already exists: /home/u/.microsandbox/snapshots/sha256-b9c0448ee9d54e33"
        ));
        assert!(is_snapshot_already_exists(
            "ERROR: Snapshot Already Exists: /path"
        ));
    }

    #[test]
    fn is_snapshot_already_exists_negative_cases_do_not_match() {
        assert!(!is_snapshot_already_exists(""));
        assert!(!is_snapshot_already_exists("error: snapshot not found: x"));
        assert!(!is_snapshot_already_exists("imported successfully"));
    }

    #[test]
    fn parse_import_digest_dir_name_takes_the_basename_of_the_last_lines_final_token() {
        assert_eq!(
            parse_import_digest_dir_name(
                "Importing snapshot...\nImported to /home/u/.microsandbox/snapshots/sha256-b9c0448ee9d54e33\n"
            ),
            Some("sha256-b9c0448ee9d54e33".to_string())
        );
    }

    #[test]
    fn parse_import_digest_dir_name_skips_trailing_blank_lines() {
        assert_eq!(
            parse_import_digest_dir_name("/snapshots/sha256-abcdef\n\n\n"),
            Some("sha256-abcdef".to_string())
        );
    }

    #[test]
    fn parse_import_digest_dir_name_none_on_entirely_blank_output() {
        assert_eq!(parse_import_digest_dir_name("\n\n"), None);
        assert_eq!(parse_import_digest_dir_name(""), None);
    }

    #[test]
    fn msb_import_checkpoint_cycle_happy_path_returns_the_digest_dir_name() {
        let mut import_calls = 0;
        let mut list_calls = 0;
        let result = {
            let mut invoke_import = || {
                import_calls += 1;
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: "Imported to /snapshots/sha256-b9c0448ee9d54e33\n".to_string(),
                    stderr: String::new(),
                })
            };
            let mut invoke_list = || {
                list_calls += 1;
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: r#"[{"digest":"sha256:fulldigesthere","name":"sha256-b9c0448ee9d54e33","artifact_path":"/snapshots/sha256-b9c0448ee9d54e33"}]"#
                        .to_string(),
                    stderr: String::new(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import, &mut invoke_list)
        };
        assert_eq!(
            result.unwrap(),
            "sha256-b9c0448ee9d54e33",
            "the effective ref must be the digest-dir name, never the full sha256: digest — msb \
             does not resolve the full digest as a snapshot ref"
        );
        assert_eq!(import_calls, 1);
        assert_eq!(list_calls, 1);
    }

    #[test]
    fn msb_import_checkpoint_cycle_treats_already_exists_as_success_and_still_confirms_the_name() {
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: "error: snapshot already exists: /snapshots/sha256-b9c0448ee9d54e33"
                        .to_string(),
                })
            };
            let mut invoke_list = || {
                Ok(ExecResult {
                    exit_code: 0,
                    stdout:
                        r#"[{"digest":"sha256:fulldigesthere","name":"sha256-b9c0448ee9d54e33"}]"#
                            .to_string(),
                    stderr: String::new(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import, &mut invoke_list)
        };
        assert_eq!(
            result.unwrap(),
            "sha256-b9c0448ee9d54e33",
            "an already-exists import is success for a content-addressed archive, and still \
             resolves to the digest-dir name"
        );
    }

    #[test]
    fn msb_import_checkpoint_cycle_a_non_already_exists_failure_surfaces_stderr_and_never_lists() {
        let mut list_called = false;
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: "error: corrupt archive: bad checksum".to_string(),
                })
            };
            let mut invoke_list = || {
                list_called = true;
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: "[]".to_string(),
                    stderr: String::new(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import, &mut invoke_list)
        };
        let err = result.expect_err("a genuine import failure must surface");
        assert!(err.to_string().contains("bad checksum"), "{err}");
        assert!(
            !list_called,
            "snapshot list must never run once the import itself failed for real"
        );
    }

    #[test]
    fn msb_import_checkpoint_cycle_surfaces_a_snapshot_list_failure() {
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: "/snapshots/sha256-b9c0448ee9d54e33\n".to_string(),
                    stderr: String::new(),
                })
            };
            let mut invoke_list = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: "database is locked".to_string(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import, &mut invoke_list)
        };
        let err = result.expect_err("a failing snapshot list must surface");
        assert!(err.to_string().contains("database is locked"), "{err}");
    }

    #[test]
    fn msb_import_checkpoint_cycle_surfaces_an_unmatched_digest_dir_name() {
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: "/snapshots/sha256-b9c0448ee9d54e33\n".to_string(),
                    stderr: String::new(),
                })
            };
            let mut invoke_list = || {
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: "[]".to_string(),
                    stderr: String::new(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import, &mut invoke_list)
        };
        let err = result.expect_err("no matching entry must surface as an error");
        assert!(err.to_string().contains("sha256-b9c0448ee9d54e33"), "{err}");
    }

    #[test]
    fn is_port_bind_conflict_matches_known_phrasings() {
        assert!(is_port_bind_conflict(
            "Error: address already in use (os error 48)"
        ));
        assert!(is_port_bind_conflict("port is already allocated"));
        assert!(is_port_bind_conflict("bind: address already in use"));
        assert!(is_port_bind_conflict(
            "Bind for 0.0.0.0:32770 failed: something something PORT already in use"
        ));
    }

    #[test]
    fn is_port_bind_conflict_negative_cases_do_not_match() {
        assert!(!is_port_bind_conflict("panic: index out of bounds"));
        assert!(!is_port_bind_conflict(""));
        assert!(!is_port_bind_conflict("connection refused"));
    }

    #[test]
    fn is_image_cache_corruption_matches_the_captured_msb_error_verbatim() {
        // Captured verbatim from a real msb 0.6.3 binary, reproduced locally by racing
        // concurrent `msb run` of images sharing a base layer against one fresh cache
        // (see this function's doc comment for the full repro).
        let output = "   ✗ Pulling      floci/floci-gcp:0.4.0\nerror: image error: cache error at /home/runner/.microsandbox/cache/layers/sha256_2a9a84f53fe64d76a54296ab37a4664aacef9f848d4aa6ad7efd84b135a351c6.tar.gz: No such file or directory (os error 2)\n";
        assert!(is_image_cache_corruption(output));
    }

    #[test]
    fn is_image_cache_corruption_matches_regardless_of_which_image_or_digest() {
        // Path, digest, and image name all vary per host/run — the classifier must
        // match on the stable parts of msb's wording only.
        assert!(is_image_cache_corruption(
            "error: image error: cache error at /tmp/msb-repro/cache/layers/sha256_c01d7b7a3f78972c12a4244ffb10257694b9d989c40172ab6184de42b967ab85.tar.gz: No such file or directory (os error 2)"
        ));
        assert!(is_image_cache_corruption(
            "error: cache error at C:\\Users\\runner\\.microsandbox\\cache\\layers\\sha256_deadbeef.tar.gz: No such file or directory (os error 2)"
        ));
    }

    #[test]
    fn is_msb_state_db_error_matches_the_captured_race_shapes_verbatim() {
        // Both captured verbatim from a real msb 0.6.3 Windows binary: the spawned
        // `msb run` lost the startup-migration race against a concurrent msb
        // invocation — one race, different losing statements (see this function's
        // doc comment).
        assert!(is_msb_state_db_error(
            "error: database error: Execution Error: error returned from database: (code: 1) index idx_manifest_layers_unique already exists"
        ));
        assert!(is_msb_state_db_error(
            "error: database error: Execution Error: error returned from database: (code: 1) duplicate column name: kind"
        ));
    }

    #[test]
    fn is_msb_state_db_error_matches_the_unique_constraint_shape() {
        assert!(is_msb_state_db_error(
            "error: database error: Execution Error: error returned from database: UNIQUE constraint failed: seaql_migrations.version"
        ));
    }

    #[test]
    fn is_msb_state_db_error_matches_any_state_db_failure_not_just_known_wordings() {
        // The classifier keys on msb's own framing, not the SQLite message — chasing
        // individual wordings is how the third race shape slipped through. A one-shot
        // retry on a non-race database error is harmless: it costs a moment and then
        // propagates with both attempts' output.
        assert!(is_msb_state_db_error(
            "error: database error: disk I/O error"
        ));
    }

    #[test]
    fn is_msb_state_db_error_negative_cases_do_not_match() {
        assert!(!is_msb_state_db_error(""));
        // A workload's stderr complaining about ITS database — no msb `error:` framing.
        assert!(!is_msb_state_db_error(
            "app: database error: connection refused"
        ));
        // A name conflict is the start-retry path's concern, not this classifier's.
        assert!(!is_msb_state_db_error(
            "error: sandbox 'rz-abc-1' already exists"
        ));
        // The image-cache corruption signature belongs to its own classifier and heal.
        assert!(!is_msb_state_db_error(
            "error: image error: cache error at /tmp/cache/layers/sha256_dead.tar.gz: No such file or directory (os error 2)"
        ));
    }

    #[test]
    fn is_name_conflict_matches_a_sandbox_already_exists_message() {
        assert!(is_name_conflict(
            "error: sandbox 'rz-reuse-abc123def456' already exists"
        ));
        assert!(is_name_conflict("Sandbox already exists"));
    }

    #[test]
    fn is_name_conflict_negative_cases_do_not_match() {
        assert!(!is_name_conflict(""));
        assert!(!is_name_conflict("connection refused"));
        assert!(!is_name_conflict(
            "error: database error: Execution Error: disk I/O error"
        ));
    }

    #[test]
    fn is_snapshot_not_found_matches_msbs_not_found_wording() {
        assert!(is_snapshot_not_found(
            "error: snapshot not found: rz-ckpt-deadbeefcafe"
        ));
        assert!(is_snapshot_not_found("Snapshot Not Found"));
    }

    #[test]
    fn is_snapshot_not_found_negative_cases_do_not_match() {
        assert!(!is_snapshot_not_found(""));
        assert!(!is_snapshot_not_found("connection refused"));
        // A generic sandbox/image not-found must not false-positive on the
        // snapshot-specific classifier — each backend-noun gets its own signal.
        assert!(!is_snapshot_not_found(
            "error: image not found: floci/floci-az:0.8.0"
        ));
        assert!(!is_snapshot_not_found(
            "error: database error: Execution Error: disk I/O error"
        ));
    }

    #[test]
    fn is_image_cache_corruption_negative_cases_do_not_match() {
        assert!(!is_image_cache_corruption("panic: index out of bounds"));
        assert!(!is_image_cache_corruption(""));
        assert!(!is_image_cache_corruption(
            "error: image not found: floci/floci-az:0.8.0"
        ));
        // A generic "No such file" with no cache-error framing must not false-positive
        // (e.g. a workload's own stderr complaining about a missing file it expected).
        assert!(!is_image_cache_corruption(
            "sh: /app/config.yaml: No such file or directory"
        ));
        // A cache error about something other than a missing file (e.g. a permissions
        // problem) must not be classified as this specific corruption signature.
        assert!(!is_image_cache_corruption(
            "error: cache error at /tmp/x/layers/sha256_abc.tar.gz: Permission denied (os error 13)"
        ));
    }

    #[test]
    fn require_no_duplicate_guest_ports_rejects_a_genuine_duplicate() {
        let links = vec![
            NetworkLink {
                alias: "a".to_string(),
                guest_port: 8000,
                target_host_port: 1,
            },
            NetworkLink {
                alias: "b".to_string(),
                guest_port: 8000,
                target_host_port: 2,
            },
        ];
        let err = require_no_duplicate_guest_ports(&links).unwrap_err();
        assert!(err.to_string().contains("8000"), "{err}");
    }

    #[test]
    fn require_no_duplicate_guest_ports_allows_distinct_ports() {
        let links = vec![
            NetworkLink {
                alias: "a".to_string(),
                guest_port: 8000,
                target_host_port: 1,
            },
            NetworkLink {
                alias: "b".to_string(),
                guest_port: 8001,
                target_host_port: 2,
            },
        ];
        assert!(require_no_duplicate_guest_ports(&links).is_ok());
    }

    #[test]
    fn require_aliases_are_valid_accepts_dns_label_charset() {
        let links = vec![NetworkLink {
            alias: "configuration-stub.local_1".to_string(),
            guest_port: 8000,
            target_host_port: 1,
        }];
        assert!(require_aliases_are_valid(&links).is_ok());
    }

    #[test]
    fn require_aliases_are_valid_rejects_shell_breaking_alias() {
        let links = vec![NetworkLink {
            alias: "bad'alias".to_string(),
            guest_port: 8000,
            target_host_port: 1,
        }];
        let err = require_aliases_are_valid(&links).unwrap_err();
        assert!(err.to_string().contains("bad'alias"), "{err}");
    }

    #[test]
    fn backend_binary_path_returns_the_provisioned_msb_path() {
        let backend = MsbCliBackend::new(PathBuf::from("/opt/msb/bin/msb"));
        assert_eq!(
            backend.backend_binary_path(),
            Some(PathBuf::from("/opt/msb/bin/msb"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn watchdog_kill_command_wraps_stop_then_rm_with_a_retry_on_state_db_error() {
        let backend = MsbCliBackend::new(PathBuf::from("/opt/msb/bin/msb"));
        let cmd = backend.watchdog_kill_command();
        assert_eq!(cmd[0], "sh");
        assert_eq!(cmd[1], "-c");
        let script = &cmd[2];
        assert!(script.contains("/opt/msb/bin/msb"), "{script}");
        assert!(script.contains("stop"), "{script}");
        assert!(script.contains("rm"), "{script}");
        assert!(script.contains("error: database error:"), "{script}");
        assert_eq!(cmd[3], "sh");
    }

    #[cfg(unix)]
    #[test]
    fn shell_single_quote_escapes_an_embedded_single_quote() {
        assert_eq!(shell_single_quote("it's"), "'it'\\''s'");
    }

    /// Writes a stub `msb` replacement to `dir` that logs one call per invocation to
    /// the state file passed as its SECOND argument (`$2`) — matching the shape of
    /// every real argv this backend builds (`["stop", name]`, `["rm", name]`: the
    /// name/state-file is always the last/second element). `$2` holds the prior call
    /// count on entry, so the count on disk after N invocations is N. When
    /// `fail_first_call`, the first invocation additionally prints msb's own
    /// state-database error framing to stderr and exits non-zero (every later
    /// invocation succeeds), letting a test assert exactly how many times
    /// [`MsbCliBackend::invoke_retrying_on_state_db_error`] actually re-ran it.
    #[cfg(unix)]
    fn write_state_db_stub_script(dir: &Path, fail_first_call: bool) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-msb.sh");
        let body = if fail_first_call {
            "#!/bin/sh\n\
             n=$(cat \"$2\" 2>/dev/null || echo 0)\n\
             echo $((n + 1)) > \"$2\"\n\
             if [ \"$n\" = \"0\" ]; then\n\
             echo 'error: database error: Execution Error: index idx already exists' 1>&2\n\
             exit 1\n\
             fi\n\
             exit 0\n"
        } else {
            "#!/bin/sh\n\
             n=$(cat \"$2\" 2>/dev/null || echo 0)\n\
             echo $((n + 1)) > \"$2\"\n\
             exit 0\n"
        };
        std::fs::write(&script, body).expect("write stub msb script");
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod stub msb script");
        script
    }

    #[cfg(unix)]
    fn unique_test_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "rz-msb-backend-test-{label}-{}-{nanos:x}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[cfg(unix)]
    #[test]
    fn spawn_msb_command_outlasts_a_transient_etxtbsy_writer() {
        use std::io::Write as _;
        use std::os::unix::fs::PermissionsExt;

        let dir = unique_test_dir("etxtbsy");
        let script = dir.join("busy-script");
        let mut writer = std::fs::File::create(&script).unwrap();
        writer.write_all(b"#!/bin/sh\nexit 0\n").unwrap();
        writer.flush().unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

        // Hold the write descriptor open — execve refuses with ETXTBSY while it
        // lives — and release it shortly after the first attempts have failed,
        // the same shape as another test thread finishing its own script write.
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            drop(writer);
        });

        let mut child = spawn_msb_command(|| {
            let mut cmd = Command::new(&script);
            cmd.stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            cmd
        })
        .expect("a briefly-busy executable must still spawn once its writer closes");
        assert!(child.wait().unwrap().success());
        release.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn spawn_msb_command_fails_immediately_on_a_missing_binary() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let attempts = AtomicU32::new(0);
        let err = spawn_msb_command(|| {
            attempts.fetch_add(1, Ordering::SeqCst);
            let mut cmd = Command::new("/definitely/not/a/real/msb");
            cmd.stdin(Stdio::null());
            cmd
        })
        .expect_err("a missing binary must not spawn");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "only ETXTBSY is transient; anything else must fail on the first attempt"
        );
    }

    #[cfg(unix)]
    #[test]
    fn invoke_retrying_on_state_db_error_retries_exactly_once_on_a_db_error() {
        let dir = unique_test_dir("db-error");
        let script = write_state_db_stub_script(&dir, true);
        let state_file = dir.join("calls");
        let backend = MsbCliBackend::new(script);

        backend.invoke_retrying_on_state_db_error(
            &["stop".to_string(), state_file.display().to_string()],
            Duration::from_secs(60),
        );

        let calls = std::fs::read_to_string(&state_file).expect("stub must have run");
        assert_eq!(
            calls.trim(),
            "2",
            "a state-database error must be retried exactly once, matching the boot \
             path's and the watchdog script's own one-shot policy"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn invoke_retrying_on_state_db_error_does_not_retry_on_a_clean_success() {
        let dir = unique_test_dir("clean-success");
        let script = write_state_db_stub_script(&dir, false);
        let state_file = dir.join("calls");
        let backend = MsbCliBackend::new(script);

        backend.invoke_retrying_on_state_db_error(
            &["stop".to_string(), state_file.display().to_string()],
            Duration::from_secs(60),
        );

        let calls = std::fs::read_to_string(&state_file).expect("stub must have run");
        assert_eq!(calls.trim(), "1", "a clean run must not be retried");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn silently_remove_retries_the_stop_and_the_rm_independently_on_state_db_errors() {
        // `remove_by_name` -> `silently_remove` is the sweep's removal path (see
        // `SandboxBackend::remove_by_name`'s callers) — this is the finding this test
        // guards: that path must apply the same retry policy as the boot path and the
        // watchdog script, not a bare best-effort call.
        let dir = unique_test_dir("silently-remove");
        let script = write_state_db_stub_script(&dir, true);
        let backend = MsbCliBackend::new(script);

        // `commands::stop`/`commands::rm` both build `["stop"|"rm", name]` — the
        // stub's `$2` convention matches that shape directly, so passing the state
        // file's own path as the sandbox NAME makes `silently_remove` drive the same
        // stub the two tests above drive directly.
        let state_file = dir.join("calls");
        backend.silently_remove(&state_file.display().to_string());

        let calls = std::fs::read_to_string(&state_file).expect("stub must have run");
        // stop: fails (state-db error) then retries and succeeds -> 2 calls.
        // rm: first call on THIS invocation of the stub is now a fresh process each
        // time, so it again sees `n == 0` on disk only for the very first call ever;
        // subsequent calls (the stop retry, and both rm calls) see a non-zero prior
        // count and succeed immediately. Total: stop attempt + stop retry + rm
        // attempt = 3.
        assert_eq!(
            calls.trim(),
            "3",
            "silently_remove must retry a state-db error on the stop call, not skip \
             straight to rm without retrying"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- checkpoint-archive export/import against a fake msb binary ----

    /// Writes a stub `msb` replacement that answers `snapshot export`, `snapshot
    /// import`, and `snapshot list --format json` — the three subcommands
    /// `export_checkpoint`/`import_checkpoint` drive. `import_exit_code`/
    /// `import_stderr` let a test choose between a fresh-import success (exit 0)
    /// and an already-exists "success" (nonzero exit, msb's own wording on
    /// stderr) — both of which [`MsbCliBackend::import_checkpoint`] must resolve
    /// to the same effective ref.
    #[cfg(unix)]
    fn write_fake_msb_for_archives(
        dir: &Path,
        import_exit_code: u8,
        import_stderr: &str,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-msb-archive.sh");
        let body = format!(
            "#!/bin/sh\n\
             case \"$1 $2\" in\n\
             \"snapshot export\")\n\
             printf 'fake-export-payload:%s' \"$3\" > \"$4\"\n\
             exit 0\n\
             ;;\n\
             \"snapshot import\")\n\
             echo '{import_stderr}' 1>&2\n\
             echo 'Imported to /home/u/.microsandbox/snapshots/sha256-fakedigest1234'\n\
             exit {import_exit_code}\n\
             ;;\n\
             \"snapshot list\")\n\
             echo '[{{\"digest\":\"sha256:fakedigest1234full\",\"name\":\"sha256-fakedigest1234\"}}]'\n\
             exit 0\n\
             ;;\n\
             esac\n\
             exit 1\n"
        );
        std::fs::write(&script, body).expect("write fake msb archive script");
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod fake msb archive script");
        script
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn export_checkpoint_and_import_checkpoint_round_trip_via_a_fake_msb_binary() {
        let dir = unique_test_dir("archive-fake-binary-fresh");
        let script = write_fake_msb_for_archives(&dir, 0, "");
        let backend = MsbCliBackend::new(script);

        let dest = dir.join("cp.archive-payload");
        backend
            .export_checkpoint("rz-ckpt-deadbeefcafe", &dest)
            .await
            .expect("export_checkpoint must succeed against the fake binary");
        assert_eq!(
            std::fs::read_to_string(&dest).unwrap(),
            "fake-export-payload:rz-ckpt-deadbeefcafe"
        );

        let effective_ref = backend
            .import_checkpoint(&dest, "rz-ckpt-deadbeefcafe")
            .await
            .expect("import_checkpoint must succeed against the fake binary");
        assert_eq!(
            effective_ref, "sha256-fakedigest1234",
            "the effective ref must be the digest-dir name (the import output path's \
             basename), never the full sha256: digest from `snapshot list`"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn import_checkpoint_treats_already_exists_as_success_via_the_fake_msb_binary() {
        let dir = unique_test_dir("archive-fake-binary-exists");
        let script = write_fake_msb_for_archives(
            &dir,
            1,
            "error: snapshot already exists: /home/u/.microsandbox/snapshots/sha256-fakedigest1234",
        );
        let backend = MsbCliBackend::new(script);

        let effective_ref = backend
            .import_checkpoint(Path::new("/does/not/matter/for/this/stub"), "whatever-ref")
            .await
            .expect(
                "an already-exists import must resolve exactly like a fresh one, not surface \
                 as an error",
            );
        assert_eq!(effective_ref, "sha256-fakedigest1234");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
