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

use rightsize::backend::{FollowHandle, NetworkLink, SandboxBackend, SandboxHandle};
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

    /// Removes leftover `rz-*` sandboxes from crashed earlier runs — never this run's
    /// own (matched by `RunId::value()`), so a live run's containers are untouched.
    pub fn sweep_orphans(&self) -> Result<()> {
        let out = self.invoke(&commands::ls(), LOGS_TIMEOUT)?;
        let this_run_prefix = format!("rz-{}-", rightsize::RunId::value());
        let mut seen = HashSet::new();
        for name in orphan_candidate_names(&out.stdout) {
            if name.starts_with(&this_run_prefix) || !seen.insert(name.clone()) {
                continue;
            }
            self.silently_remove(&name);
        }
        Ok(())
    }

    fn silently_remove(&self, name: &str) {
        let _ = self.invoke(&commands::stop(name), STOP_TIMEOUT);
        let _ = self.invoke(&commands::rm(name), STOP_TIMEOUT);
    }

    /// Spawns `msb <args>`, feeding it a closed/null stdin (`msb exec` blocks on
    /// stdin EOF, and every msb child needs the same treatment to avoid hanging),
    /// drains stdout/stderr on threads, and waits up to `timeout`. The drain threads are
    /// joined **without a bound** after the process exits (not a fixed cap) so a
    /// large-output command's tail is never truncated by a join deadline.
    fn invoke(&self, args: &[String], timeout: Duration) -> Result<ExecResult> {
        let mut child = Command::new(&self.msb)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
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

/// Extracts every `rz-<8 hex>-<seq>`-shaped name appearing anywhere in `ls` output —
/// used by the orphan reaper, which doesn't need the full tolerant JSON parse (any
/// status counts, not just `Running`) but does need to recognize this backend's own
/// naming convention among whatever else `msb ls` prints.
fn orphan_candidate_names(ls_output: &str) -> Vec<String> {
    let bytes = ls_output.as_bytes();
    let mut names = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(b"rz-") {
            let start = i;
            let mut j = i + 3;
            let mut hex_len = 0;
            while j < bytes.len() && (bytes[j] as char).is_ascii_hexdigit() && hex_len < 8 {
                j += 1;
                hex_len += 1;
            }
            if hex_len == 8 && bytes.get(j) == Some(&b'-') {
                let mut k = j + 1;
                let mut digit_len = 0;
                while k < bytes.len() && (bytes[k] as char).is_ascii_digit() {
                    k += 1;
                    digit_len += 1;
                }
                if digit_len > 0 {
                    // Reject a longer identifier-like run continuing past the matched
                    // shape (e.g. alphanumerics glued on) so this stays a whole-token
                    // match, not a prefix of some other name.
                    if k >= bytes.len() || !is_ident_continue(bytes[k]) {
                        names.push(String::from_utf8_lossy(&bytes[start..k]).to_string());
                        i = k;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }
    names
}

fn is_ident_continue(b: u8) -> bool {
    (b as char).is_ascii_alphanumeric() || b == b'-' || b == b'_'
}

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
        self.started_names
            .lock()
            .expect("started_names mutex poisoned")
            .insert(id);
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
                let deadline = Instant::now() + ATTACHED_STOP_TIMEOUT;
                loop {
                    match child.try_wait() {
                        Ok(Some(_)) => break,
                        Ok(None) if Instant::now() >= deadline => {
                            let _ = child.kill();
                            let _ = child.wait();
                            break;
                        }
                        Ok(None) => std::thread::sleep(Duration::from_millis(50)),
                        Err(_) => break,
                    }
                }
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

    async fn logs(&self, handle: &dyn SandboxHandle) -> Result<String> {
        let argv = commands::logs(handle.id());
        let msb = self.msb.clone();
        let result =
            tokio::task::spawn_blocking(move || invoke_standalone(&msb, &argv, LOGS_TIMEOUT))
                .await
                .map_err(|e| RightsizeError::Backend(format!("logs task panicked: {e}")))??;
        Ok(result.stdout)
    }

    async fn follow_logs(
        &self,
        handle: &dyn SandboxHandle,
        consumer: Box<dyn Fn(String) + Send + Sync>,
    ) -> Result<FollowHandle> {
        let msb = self.msb.clone();
        let name = handle.id().to_string();
        crate::watchdog::spawn_follow(msb, name, consumer)
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
}

/// Runs on a blocking thread: spawns `msb run <spec's argv>` attached (no `-d`),
/// polls `msb ls --format json` until the sandbox reaches `Running`, and returns the
/// live child for `start()` to keep around. Classifies a bind-conflict from the
/// child's own combined output if it exits before reaching `Running`.
fn spawn_and_await_running(msb: &Path, spec: &ContainerSpec) -> Result<Child> {
    let argv = commands::run(spec);
    let mut child = Command::new(msb)
        .args(&argv)
        .stdin(Stdio::null()) // msb exec blocks on stdin EOF; give every child a closed stdin.
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            RightsizeError::Backend(format!("failed to spawn msb {}: {e}", argv.join(" ")))
        })?;

    let stdout_pipe = child.stdout.take().expect("piped stdout");
    let stderr_pipe = child.stderr.take().expect("piped stderr");
    let tail: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
    let t_out = spawn_tail_drain(stdout_pipe, tail.clone());
    let t_err = spawn_tail_drain(stderr_pipe, tail.clone());

    let deadline = Instant::now() + FIRST_RUN_TIMEOUT;
    loop {
        if let Some(status) = child.try_wait().map_err(RightsizeError::from)? {
            let _ = t_out.join();
            let _ = t_err.join();
            let output = tail
                .lock()
                .expect("tail mutex poisoned")
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join("\n");
            return if is_port_bind_conflict(&output) {
                Err(RightsizeError::PortBindConflict {
                    message: format!(
                        "msb run for sandbox {} could not bind a host port: {output}",
                        spec.name
                    ),
                    source: None,
                })
            } else {
                Err(RightsizeError::Backend(format!(
                    "msb run for sandbox {} exited (code {}) before reaching Running — check the \
                     image entrypoint and `msb run` output below:\n{output}",
                    spec.name,
                    status.code().unwrap_or(-1)
                )))
            };
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
            return Err(RightsizeError::Backend(format!(
                "Sandbox {} did not reach Running within {}s — this can mean a slow image pull, \
                 a crash-looping entrypoint, or msb itself being unresponsive; last output:\n{output}",
                spec.name,
                FIRST_RUN_TIMEOUT.as_secs()
            )));
        }
        std::thread::sleep(READINESS_POLL);
    }
}

/// A standalone (non-`&self`) version of `invoke`, for call sites (the blocking `stop`
/// task, `cleanup_sync`) that don't have easy access to `&MsbCliBackend`.
fn invoke_standalone(msb: &Path, args: &[String], timeout: Duration) -> Result<ExecResult> {
    let mut child = Command::new(msb)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
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

/// A standalone one-shot logs fetch, for the `follow_logs` watchdog's authoritative
/// tail replay (`crate::watchdog::flush_tail_once`), which has no `&MsbCliBackend`
/// either.
pub(crate) fn invoke_logs_for_watchdog(msb: &Path, name: &str) -> Result<String> {
    Ok(invoke_standalone(msb, &commands::logs(name), LOGS_TIMEOUT)?.stdout)
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
    fn orphan_candidate_names_extracts_rz_shaped_tokens_only() {
        let out = "rz-deadbeef-0  alpine:3.19  Running\nrz-cafebabe-12 redis:8.6-alpine Stopped\nnotarealname\n";
        let names = orphan_candidate_names(out);
        assert_eq!(
            names,
            vec!["rz-deadbeef-0".to_string(), "rz-cafebabe-12".to_string()]
        );
    }

    #[test]
    fn orphan_candidate_names_ignores_non_matching_prefixes() {
        assert!(orphan_candidate_names("rz-short-0").is_empty());
        assert!(orphan_candidate_names("rz-deadbeefzz-0").is_empty());
        assert!(orphan_candidate_names("rz-deadbeef-").is_empty());
    }

    #[test]
    fn sweep_orphans_filters_out_this_runs_own_prefix() {
        let this_run = format!("rz-{}-3", rightsize::RunId::value());
        let other_run = "rz-00000000-1".to_string();
        let out = format!("{this_run}\n{other_run}\n");
        let candidates = orphan_candidate_names(&out);
        let this_run_prefix = format!("rz-{}-", rightsize::RunId::value());
        let survivors: Vec<&String> = candidates
            .iter()
            .filter(|n| !n.starts_with(&this_run_prefix))
            .collect();
        assert_eq!(survivors, vec![&other_run]);
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
}
