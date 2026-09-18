//! `MsbCliBackend`: drives `msb run` as an ATTACHED child process per container
//! (detached mode never starts the image `ENTRYPOINT` on 0.6.2, only attached mode
//! does), classifies port-bind failures from the child's combined output, and works
//! around `msb logs -f` never exiting on its own once a sandbox stops with a watchdog
//! that does one authoritative, at-most-once tail replay.
//!
//! **Checkpoint restore is supervised differently, on purpose.** `msb restore
//! <path> --name <name>` (msb 0.7.1+, both the checkpoint feature's own re-boot and
//! an ordinary `Container::from_checkpoint(...)` restore — see `commands::restore`'s
//! doc) creates a DETACHED sandbox: the `restore` process itself activates it and
//! exits — typically within seconds, exit 0 on success — while the sandbox keeps
//! booting in the background and reaches `Running` on its own (verified live). So
//! `try_restore_and_await_running` waits for that process to exit (classifying a
//! nonzero exit's output through the same [`PreRunningFailure`] cascade `run` uses,
//! plus one restore-only transient — see [`is_restore_access_denied`]), then polls
//! `msb ls` for `Running` the same way the attached path does.
//!
//! **`Running` is not the end of the story, though.** A restored sandbox reaches
//! `Running` with only its guest agent up — verified live against a real msb
//! 0.7.1: the checkpoint's own workload command never re-runs on its own, `msb
//! start` on a restored sandbox boots idle too, and `msb logs` on one returns
//! nothing. Upstream's `restore` simply doesn't restart what the container was
//! doing. So a third phase re-starts it: `try_restore_and_await_running` spawns
//! `msb exec [-e K=V]... <name> -- <argv>` (see `commands::exec_workload`) as a
//! LONG-LIVED attached child and hands IT back as this boot's live child — a
//! restored sandbox's [`HandleState::attached`] is `Some` again, exactly like an
//! ordinary `run` boot's, so child-exit-based death detection, `stop()`'s reap,
//! and every other place that already treats `attached` uniformly needs no
//! changes at all. `<argv>` is the checkpoint's own explicit `command` when it has
//! one, else the guest cmdline `create_checkpoint`'s own capture step recovered
//! at checkpoint time (see [`msb_checkpoint_cycle`]) — a checkpoint with neither
//! fails the restore outright with a typed error rather than booting silently
//! idle.
//!
//! **Handle-side mutable state:** `Handle` itself is immutable — `spec` plus
//! the backend-assigned `id`. Everything that changes over a container's lifetime (the
//! attached child, when there is one, its log tail, the exec-tunnels installed for
//! network links) lives in `MsbCliBackend`'s own `handles: Mutex<HashMap<String,
//! HandleState>>`, keyed by `handle.id()`. No method here downcasts `&dyn
//! SandboxHandle` — every one that needs mutable state looks it up by id under that
//! mutex instead.

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
/// How long an exec keeps retrying while the guest agent's endpoint has not appeared
/// yet, and how long it pauses between attempts — see [`is_agent_endpoint_not_ready`].
/// Costs nothing on the ordinary path, where the first attempt connects.
const AGENT_ENDPOINT_RETRY_BUDGET: Duration = Duration::from_secs(30);
const AGENT_ENDPOINT_RETRY_DELAY: Duration = Duration::from_millis(250);
const LOGS_TIMEOUT: Duration = Duration::from_secs(30);
const ATTACHED_STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// `msb copy` can move an arbitrarily large directory tree — generous headroom
/// over [`EXEC_TIMEOUT`], which only ever runs a single guest command.
const COPY_TIMEOUT: Duration = Duration::from_secs(300);
/// Each step of the checkpoint stop/snapshot/start cycle — a snapshot write is disk
/// I/O over a (typically sparse, small) rootfs, but generous headroom matters more
/// than tightness here since a slow step must not spuriously fail a checkpoint.
const CHECKPOINT_STEP_TIMEOUT: Duration = Duration::from_secs(120);
/// `msb snapshot save`/`snapshot load`/`snapshot list` — moving a
/// (zstd-compressed, typically small) disk snapshot artifact to/from an archive
/// file. Same generous headroom as [`COPY_TIMEOUT`]: a slow disk must not
/// spuriously fail an export/import.
const ARCHIVE_TIMEOUT: Duration = Duration::from_secs(300);
/// How long [`try_restore_and_await_running`]'s phase 3 waits, polling at
/// [`READINESS_POLL`] apart, to see whether the just-spawned workload-revival
/// exec child exits right away — long enough to catch an immediately failing
/// command (a typo'd binary, a permission error, an exec-format error — all of
/// which fail within milliseconds in practice) without holding up every restore
/// by this much. A workload that fails LATER than this window is not treated any
/// differently than an ordinary attached `run` child that dies after `start()`
/// has already returned — this window only decides the boot-time-failure-vs-
/// live-child boundary, not whether a later death is ever noticed.
const WORKLOAD_EXEC_EARLY_EXIT_GRACE: Duration = Duration::from_millis(300);
/// Before retrying a restore that hit the Windows post-teardown access-denied
/// transient (see [`is_restore_access_denied`]) — short, mirroring
/// [`STATE_DB_RETRY_DELAY`]'s own one-shot policy: the deferred file-handle
/// release this works around clears in well under a second in practice, and the
/// retried `restore` itself dwarfs this delay either way.
const RESTORE_ACCESS_DENIED_RETRY_DELAY: Duration = Duration::from_millis(500);
/// [`msb_checkpoint_cycle`]'s post-`rm` guard: how long — polled at
/// [`READINESS_POLL`] intervals — to wait for a just-removed sandbox's name to
/// actually drop out of `msb ls --format json` before rebooting. msb's own
/// `rm` returning success is not the same as the name being free yet: on
/// Windows, msb 0.7.1's sandbox record/name release lags the `rm` process's
/// own exit (the same deferred-teardown-lag family
/// [`is_restore_access_denied`] already works around one step later, for the
/// snapshot artifact's file handle). Unix releases the name synchronously, so
/// the very first poll — taken before any sleep — already sees it gone there.
///
/// **The reboot no longer targets this name at all** — `MsbCliBackend::
/// create_checkpoint` restores under a FRESH sandbox name instead (see that
/// method's own doc: msb's own on-disk directory retention on Windows makes a
/// same-name restore unreliable even once the DB record is confirmed gone, and
/// no amount of waiting here ever bounds that second, independent lag — see
/// below). This wait, and [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_BUDGET`]
/// past it, are kept as DORMANT DEFENSE: they simply have nothing left to
/// collide on, since the reboot never reuses `name`. Left in rather than
/// removed, in case a future change ever reintroduces a same-name path.
///
/// This was always only a cheap FIRST gate, not a guarantee: `msb ls` only
/// speaks to the sandbox's DB record, while msb 0.7.1's own `restore`-time
/// collision check (`prepare_create_target` in
/// `sdk/rust/lib/backend/local/sandbox/create.rs`: `existing.is_some() ||
/// dir_exists`) also blocks on the sandbox's on-disk directory — a second,
/// independent thing to release that `msb ls` says nothing about, and that CI
/// has observed staying held **even 30+ seconds after `rm`** on a Windows
/// runner (not a transient lag to wait out at all — see
/// [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_BUDGET`]'s own doc for the
/// structural evidence). That gap is exactly why the reboot moved off `name`
/// entirely instead of growing this budget further.
const CHECKPOINT_NAME_RELEASE_BUDGET: Duration = Duration::from_secs(3);
/// Before retrying [`msb_checkpoint_cycle`]'s own re-boot when it hits msb's
/// "already exists" refusal despite [`CHECKPOINT_NAME_RELEASE_BUDGET`]'s own
/// wait having already passed — the actual guarantee, not the one-shot,
/// 300ms-delay retry this used to be. That single retry was sized for the
/// gap between the wait passing and the retry running, not for the directory-
/// release lag [`CHECKPOINT_NAME_RELEASE_BUDGET`]'s own doc describes, which
/// CI has observed exceeding 3.5s under load on its own — comfortably past
/// what one 300ms retry could ever cover. This now polls on the same
/// install-lock-poll shape as [`INSTALL_LOCK_RETRY_BUDGET`]/
/// [`INSTALL_LOCK_RETRY_DELAY`]: long enough to outlast every release lag
/// observed, short enough that a genuinely stuck teardown still fails
/// clearly instead of hanging. See [`reboot_with_already_exists_retry`] for
/// the loop itself.
///
/// **Dormant since the reboot moved off `name` onto a fresh sandbox name**
/// (see [`CHECKPOINT_NAME_RELEASE_BUDGET`]'s own doc): a fresh name has never
/// been given to msb before, so it cannot itself trigger "already exists" —
/// this retry loop is kept live as defense-in-depth (a checkpoint reboot is
/// still, structurally, a create under a name this process just picked; a
/// pathological collision against a stale leftover is not impossible, just no
/// longer the expected case this budget was originally sized for) rather than
/// removed.
const CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_BUDGET: Duration = Duration::from_secs(30);
/// The poll interval for [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_BUDGET`] —
/// see that constant's own doc.
const CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_DELAY: Duration = Duration::from_secs(2);

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
    /// The attached `msb run` child, once `start()` has spawned it — `None` for a
    /// sandbox that was booted (or checkpoint-rebooted) via `msb restore` instead:
    /// that command is detached (see the module docs), so there is no child process
    /// for this backend to hold, reap on stop, or death-detect on. Every reader of
    /// this field already treats an absent child as "nothing to do here" rather than
    /// an error — see `stop()` and `reap_attached_child`'s own callers.
    attached: Option<Child>,
    /// Exec-tunnels installed by `install_network_links`, torn down on `stop`.
    resources: Vec<ExecTunnel>,
    /// The workload cmdline `create_checkpoint`'s own guest-cmdline capture step
    /// (see [`msb_checkpoint_cycle`]) most recently recovered for this handle,
    /// when its checkpoint's spec had no explicit `command` — `None` otherwise
    /// (an explicit command needed no capture, the capture failed or produced
    /// nothing parseable, or no checkpoint has been taken yet). Read back once by
    /// [`MsbCliBackend::last_checkpoint_captured_cmdline`], right after
    /// `create_checkpoint` returns — see that trait method's own doc for who
    /// reads it and why.
    captured_cmdline: Option<Vec<String>>,
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

/// True if `output` (a `msb restore` invocation's combined stdout/stderr) is the
/// Windows-only transient observed intermittently in CI, immediately after the
/// source sandbox's own teardown: `restore` fails with exit 1 and output
/// containing
///
/// ```text
/// io error: Access is denied. (os error 5)
/// ```
///
/// The just-written snapshot artifact's file handle hasn't finished being
/// released by the OS yet when `restore` tries to read it — msb's own docs
/// describe deferred file-handle release on Windows — so this is a race, not a
/// real permissions problem, and clears on a short retry (see
/// [`RESTORE_ACCESS_DENIED_RETRY_DELAY`] and the one-shot retry arm in
/// [`spawn_and_await_running`]).
///
/// Matches conservatively — msb's own "Access is denied" phrase together with
/// EITHER "io error" or "os error 5" — so a genuine, unrelated access-denied
/// failure (a real permissions problem elsewhere) is never silently retried
/// away. Written to be checked unconditionally on every platform (this backend
/// has no `#[cfg(windows)]` classifiers) — the signature simply never occurs on
/// unix, where a plain string match on it is a guaranteed no-op, not a risk.
fn is_restore_access_denied(output: &str) -> bool {
    output.contains("Access is denied")
        && (output.contains("io error") || output.contains("os error 5"))
}

/// The marker line msb's guest agent writes to the SYSTEM log once it has actually
/// come up — present only once a sandbox's boot genuinely reached a working guest
/// agent, as opposed to dying before one ever existed.
const SANDBOX_STARTED_MARKER: &str = "--- sandbox started ---";

/// Post-mortem check for [`try_spawn_and_await_running`]'s fast-exit case: an
/// attached `msb run` child for sandbox `name` exited 0 before this backend's own
/// polling ever observed `Running`. On msb 0.6.16+ this is not necessarily a failed
/// boot — msb's convergent-lifecycle rework no longer surfaces `Running` at all for a
/// workload that finishes before the next poll (a short build/test script, e.g.
/// `alpine -- true`), whereas earlier msb releases usually won that race on a fast
/// host and observed `Running` briefly before the same exit; 0.6.16 itself never
/// wins it.
///
/// Confirming the sandbox genuinely finished — rather than dying mid-boot the way
/// msb 0.6.10-0.6.13's Windows agentless-death failures did (also exit 0, but the
/// guest agent never came up) — requires BOTH of:
/// - `msb ls --format json` reports this sandbox's own `status` as `"Stopped"`
///   ([`ls_json::status_is`]);
/// - `msb logs <name> --source system --tail 1000` contains
///   [`SANDBOX_STARTED_MARKER`], the boot-completion line msb's guest agent writes
///   only once it has actually come up.
///
/// Either query itself failing to run, or either signal being absent, returns
/// `false` — this never claims success on incomplete information, so a broken `ls`/
/// `logs` invocation just falls back to today's ordinary failure message rather than
/// masking a real problem.
fn fast_exit_ran_to_completion(msb: &Path, name: &str) -> bool {
    let Ok(ls_result) = invoke_standalone(msb, &commands::ls(), LOGS_TIMEOUT) else {
        return false;
    };
    if !ls_json::status_is(&ls_result.stdout, name, "Stopped") {
        return false;
    }
    let Ok(log_result) = invoke_standalone(msb, &commands::logs_system(name), LOGS_TIMEOUT) else {
        return false;
    };
    log_result.stdout.contains(SANDBOX_STARTED_MARKER)
        || log_result.stderr.contains(SANDBOX_STARTED_MARKER)
}

/// True if `output` (a `msb exec` invocation's stderr) says msb could not reach the
/// guest agent's endpoint at all, as distinct from the guest command itself failing.
///
/// `start()` returns once msb's own `ls` reports the sandbox `Running`, but Running is
/// a statement about the sandbox process, not about the in-guest agent having created
/// the endpoint `msb exec` connects to. The two are ordinarily separated by enough
/// wall-clock that nothing notices: every module waits on a log line, an HTTP probe, or
/// a port before anyone execs. A caller that starts a container and immediately execs
/// has no such gap, and on Windows — where the endpoint is a named pipe rather than a
/// unix socket — loses the race outright:
///
/// ```text
/// error: agent client error: connect \\.\pipe\msb-agent-<id>: The system cannot find
/// the file specified. (os error 2)
/// ```
///
/// Captured verbatim from a `windows-2025` hosted runner exec'ing into a sandbox
/// restored from a checkpoint archive. Matches on msb's own `agent client error`
/// framing plus `connect`, so a failure *after* the connection is established — a real
/// agent error worth surfacing — is never mistaken for this.
fn is_agent_endpoint_not_ready(stderr: &str) -> bool {
    stderr.contains("agent client error") && stderr.contains("connect")
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

/// Mints an absolute checkpoint ref for `nonce_or_name`: a path under
/// `<cache_dir>/checkpoints/`, whose basename is the actual snapshot name `msb`
/// is given (`rz-ckpt-<nonce_or_name>`) — the same naming convention
/// `rightsize::ContainerGuard` itself now applies when it mints this ref up
/// front (see `MsbCliBackend::create_checkpoint`'s doc). Kept here as
/// `MsbCliBackend::create_checkpoint`'s own defensive fallback for a bare
/// `nonce_or_name` reaching it directly (never `ContainerGuard`'s own call
/// path, which always hands down an already-absolute ref). Pure and
/// `cache_dir`-parameterized so the shape is unit-testable without touching
/// process env or the filesystem.
fn mint_checkpoint_ref(nonce_or_name: &str, cache_dir: &Path) -> PathBuf {
    cache_dir
        .join("checkpoints")
        .join(format!("rz-ckpt-{nonce_or_name}"))
}

/// `checkpoint_ref` is a "path ref" — a ref minted by this backend's own
/// `mint_checkpoint_ref` (this version and later) rather than a bare snapshot
/// name minted before it — iff it's an absolute path. Every checkpoint method
/// here branches on this to keep handling both shapes: a bare-name ref minted
/// before this change must keep working everywhere.
fn path_ref_dir(checkpoint_ref: &str) -> Option<PathBuf> {
    let path = Path::new(checkpoint_ref);
    path.is_absolute().then(|| path.to_path_buf())
}

/// `dir` (a path ref's artifact directory) holds a real checkpoint iff it exists
/// AND contains `snapshot.json` — [`MsbCliBackend::has_checkpoint`]'s filesystem
/// check for a path ref, no `msb` call involved.
fn path_ref_artifact_exists(dir: &Path) -> bool {
    dir.is_dir() && dir.join("snapshot.json").is_file()
}

/// True if `output` (an `msb snapshot load` child's combined stdout/stderr)
/// names msb's "already imported" wording — `error: snapshot already exists:
/// <path>`. For a content-addressed archive this IS success: the artifact is
/// already sitting there under its digest-derived directory, so
/// [`MsbCliBackend::import_checkpoint`] treats it as one, continuing straight to
/// resolving the effective ref exactly like a fresh, successful import would.
fn is_snapshot_already_exists(output: &str) -> bool {
    output.to_lowercase().contains("snapshot already exists")
}

/// True if `stderr` (an `msb snapshot save` invocation's) carries Windows'
/// `ERROR_ACCESS_DENIED`, the signature of msb 0.6.7/0.6.8's unconditional
/// snapshot-save failure on Windows.
///
/// msb's own `save_snapshot` writes the finished archive to a staging file beside
/// the destination, reopens that staging file READ-ONLY, and calls `sync_all` on
/// the handle before renaming it into place. On Windows `sync_all` is
/// `FlushFileBuffers`, which requires write access on the handle, so it fails with
/// error 5 every single time and the rename never runs. `fsync` on a read-only
/// descriptor is legal on Unix, so Linux and macOS never see this; msb 0.6.6 wrote
/// straight to the destination with no fsync step at all, which is why it is new.
/// Captured from a `windows-2025` hosted runner:
///
/// ```text
/// msb snapshot save rz-ckpt-fccd7568-archive C:\Users\RUNNER~1\AppData\Local\Temp\rightsize-archive-export-8980-1-1785507050813959600\artifact failed (exit 1): error: io error: Access is denied. (os error 5)
/// ```
///
/// Matches on the `(os error 5)` suffix, never on the message text: Rust renders
/// Windows error text through `FormatMessage`, so "Access is denied." is whatever
/// the machine's display language says, while the numeric suffix is appended by
/// Rust itself and reads the same on every locale. The closing parenthesis is part
/// of the needle so a two-digit code in the fifties (`os error 53`, a missing
/// network path) cannot match on its first digit.
fn is_archive_fsync_access_denied(stderr: &str) -> bool {
    stderr.contains("(os error 5)")
}

/// Moves the staging file msb left behind next to `dest` onto `dest` itself,
/// finishing the rename msb's own `save_snapshot` was about to perform when its
/// fsync failed (see [`is_archive_fsync_access_denied`]). Returns whether the
/// destination is now in place.
///
/// msb names that staging file `.<dest file name>.tmp.<msb pid>.<unix nanos>` and
/// removes it only when the archive WRITE fails — not when the fsync fails. So on
/// the error this works around, a complete, valid archive is sitting in the
/// destination's own directory under that name, and putting it where msb meant to
/// put it is the whole repair.
///
/// Requires EXACTLY ONE candidate. Every export this library performs targets a
/// freshly created staging directory it owns outright (see
/// `rightsize::archive::TempStagingDir`), so one candidate is what the worked-around
/// failure always leaves; zero means the archive never got written, and two or more
/// means something other than this failure produced them — in both cases picking one
/// would be a guess, so neither is salvaged and the original error stands.
fn salvage_archive_staging_file(dest: &Path) -> bool {
    let (Some(parent), Some(dest_name)) = (dest.parent(), dest.file_name()) else {
        return false;
    };
    let Some(dest_name) = dest_name.to_str() else {
        return false;
    };
    let prefix = format!(".{dest_name}.tmp.");

    let Ok(entries) = std::fs::read_dir(parent) else {
        return false;
    };
    // Every entry has to be readable, and a matching one has to be a regular file.
    // An entry this scan skipped could be a second candidate, which would turn an
    // ambiguous directory into a confident one-candidate salvage; a directory under
    // the staging name would consume the single-candidate slot outright. Neither is
    // the failure being worked around, so both decline rather than guess.
    let mut candidates: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else { return false };
        let matches_prefix = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with(&prefix));
        if !matches_prefix {
            continue;
        }
        if !entry.file_type().is_ok_and(|kind| kind.is_file()) {
            return false;
        }
        candidates.push(entry.path());
    }

    let [candidate] = candidates.as_slice() else {
        return false;
    };
    std::fs::rename(candidate, dest).is_ok()
}

/// The shared "last non-empty line, required to parse as an absolute path"
/// contract both `msb snapshot create` ([`parse_snapshot_create_ref`]) and `msb
/// snapshot load` ([`parse_snapshot_load_ref`]) print their artifact's own path
/// through on msb 0.7.1: trims surrounding whitespace, takes the LAST non-empty
/// line, and requires it to parse as an absolute path (`Path::is_absolute()`) —
/// empty output, a relative-looking last line, or anything else that isn't
/// recognizably a path all return `None` rather than guess, so a caller can fail
/// loudly quoting the raw output instead of minting a bogus ref.
fn parse_last_line_as_absolute_path(output: &str) -> Option<String> {
    let last_line = output.lines().rev().find(|line| !line.trim().is_empty())?;
    let candidate = last_line.trim();
    Path::new(candidate)
        .is_absolute()
        .then(|| candidate.to_string())
}

/// Parses the loaded checkpoint artifact's own absolute path out of a successful
/// `msb snapshot load <archive> --dest <dir>` invocation's stdout — verified
/// contract against a real msb 0.7.1 binary: stdout carries a `group msb-<hex>:
/// head snap_<digest> (Initialized)` line, a digest line, then the loaded
/// artifact's absolute path as the LAST line. That full path — not a digest or a
/// directory basename resolved separately via `snapshot list` — IS the effective
/// ref [`MsbCliBackend::import_checkpoint`] returns, the same "the printed path
/// itself is the ref" contract [`parse_snapshot_create_ref`] already uses for
/// `snapshot create`. See [`parse_last_line_as_absolute_path`] for the shared
/// parsing rule both apply.
fn parse_snapshot_load_ref(stdout: &str) -> Option<String> {
    parse_last_line_as_absolute_path(stdout)
}

/// Fallback for [`msb_import_checkpoint_cycle`]'s "already exists" outcome when
/// [`parse_snapshot_load_ref`] finds nothing on stdout: pulls the artifact path
/// out of msb's `error: snapshot already exists: <path>` stderr line (see
/// [`is_snapshot_already_exists`]) the same way the pre-0.7.1 `import` verb's
/// digest-dir parse did — the LAST whitespace-separated token on the last
/// non-empty line, required to parse as an absolute path. Unlike
/// [`parse_last_line_as_absolute_path`], the line itself does not have to be
/// nothing but the path, since here it is always prefixed with msb's own
/// `error: snapshot already exists:` wording.
fn parse_already_exists_stderr_ref(stderr: &str) -> Option<String> {
    let last_line = stderr.lines().rev().find(|line| !line.trim().is_empty())?;
    let candidate = last_line.split_whitespace().last()?;
    Path::new(candidate)
        .is_absolute()
        .then(|| candidate.to_string())
}

/// Before retrying a boot that hit msb's state-database error — enough for a winning
/// concurrent invocation's migration transaction to commit; the retry's own `msb run`
/// startup dwarfs this either way.
const STATE_DB_RETRY_DELAY: Duration = Duration::from_millis(500);

/// True if `output` (an `msb run` invocation's combined output) is msb refusing to run
/// anything while its internal install lock is held. Captured verbatim from a
/// windows-2025 hosted runner, mid-suite with ordinary boots succeeding on both sides
/// of the failure:
///
/// ```text
/// error: runtime error: microsandbox install operation in progress until
/// 2026-07-31 20:35:23.760135600; retry after it completes
/// error: runtime error: another microsandbox install operation is in progress
/// until 2026-08-01 19:26:19.025098100
/// ```
///
/// Two phrasings, one condition — msb words the refusal differently depending on
/// which side holds the lock, so the match tolerates the optional "is".
///
/// The deadline in the message reads ~30 minutes out, but every captured occurrence
/// cleared within the same run — boots seconds later succeeded — so the boot path
/// polls briefly (see [`spawn_and_await_running`]) instead of failing on the first
/// refusal or trusting the deadline. Matches on the stable phrase only; the timestamp
/// varies per occurrence.
fn is_msb_install_lock_active(output: &str) -> bool {
    output.contains("install operation in progress")
        || output.contains("install operation is in progress")
}

/// How long the boot path keeps polling while msb's install-operation lock is held,
/// and the pause between attempts (see [`is_msb_install_lock_active`]) — observed
/// clearing within seconds despite the message's ~30-minute deadline, so a short
/// budget covers the real cases and a lock outliving it is surfaced as stuck.
const INSTALL_LOCK_RETRY_BUDGET: Duration = Duration::from_secs(30);
const INSTALL_LOCK_RETRY_DELAY: Duration = Duration::from_secs(2);

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
            // `attached` is already `Option<Child>` — `Some` for an ordinary `msb
            // run` boot, `None` for a `msb restore` (checkpoint) boot, which has no
            // live child to hold (see the module docs).
            state.attached = attached;
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

    /// Runs `cmd` in the guest, retrying while msb reports it cannot reach the guest
    /// agent's endpoint yet (see [`is_agent_endpoint_not_ready`]). A sandbox is
    /// `Running` before that endpoint necessarily exists, so an exec issued immediately
    /// after `start()` can arrive first; the retry closes that window rather than
    /// leaving it for every caller to discover. Only that one signature is retried —
    /// a guest command's own non-zero exit returns on the first attempt, unchanged.
    async fn exec(&self, handle: &dyn SandboxHandle, cmd: &[String]) -> Result<ExecResult> {
        let argv = commands::exec(handle.id(), cmd);
        let msb = self.msb.clone();
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + AGENT_ENDPOINT_RETRY_BUDGET;
            loop {
                let result = invoke_standalone(&msb, &argv, EXEC_TIMEOUT)?;
                if result.exit_code == 0
                    || !is_agent_endpoint_not_ready(&result.stderr)
                    || Instant::now() >= deadline
                {
                    return Ok(result);
                }
                std::thread::sleep(AGENT_ENDPOINT_RETRY_DELAY);
            }
        })
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

    /// Disk-snapshot checkpointing: `msb stop <name>` → `msb snapshot create
    /// --from-sandbox <name> rz-ckpt-<nonce> --dest-dir <cache>/checkpoints` → `msb
    /// rm <name>` → a fresh ATTACHED `msb restore <ref> --name <candidate>`
    /// re-boot under a FRESH sandbox name (never `name`/`id` — see
    /// `fresh_names`'s own doc on the trait method for why: msb does not
    /// release a removed sandbox's on-disk directory promptly on Windows, so a
    /// same-name restore can refuse "sandbox already exists" well past the
    /// point its DB record is confirmed gone). **Never a candidate a PRIOR
    /// attempt at this SAME checkpoint already touched, either:** a restore
    /// that fails AFTER msb's own artifact-integrity check (e.g. the Windows
    /// post-teardown access-denied transient) can leave that candidate behind
    /// as a stopped sandbox record, so `fresh_names` is a BATCH this method
    /// walks in order — on a classified refusal (msb's "already exists," which
    /// is also where a same-candidate access-denied retry inside
    /// [`spawn_and_await_running`] ends up once msb's own record exists — see
    /// that function's own doc for the access-denied retry itself) it
    /// best-effort `msb rm`s the failed candidate and advances to the next one,
    /// never re-trying the one that just collided. Same ports/memory/env as
    /// before either way (msb 0.7.1 replaced `run --from-snapshot` with this
    /// dedicated `restore` command, and no longer takes `--disk-only` for a
    /// disk-scope snapshot — see `commands::restore`'s own doc for what
    /// changed and why) — see `msb_checkpoint_cycle`/
    /// [`reboot_with_already_exists_retry`] for the orchestration this
    /// method's own `reboot` closure walks candidates underneath, and their
    /// unit tests for the failure paths. Runs on a blocking thread, like every
    /// other multi-step msb invocation in this backend. The re-boot reuses
    /// [`spawn_and_await_running`], this backend's own normal boot path
    /// (already the shape `Container::from_checkpoint`'s restores use — those
    /// have always minted a fresh name of their own, never hitting this
    /// Windows directory-retention issue in the first place) — see the module
    /// docs for why an msb `start` resume is not used here, and for why that
    /// re-boot (an `msb restore`, same as any other checkpoint restore) never
    /// yields a live child: the handle's held attached child is swapped to
    /// whatever the re-boot returns on success — `None` in practice, since this
    /// path always restores — clearing out the pre-checkpoint `msb run` child it
    /// held before, if any. This handle's identity DOES change: the returned
    /// handle carries whichever `fresh_names` candidate actually won, and this
    /// backend's own per-container state (`self.handles`, and
    /// `self.started_names` when this handle isn't `keep_alive`) moves from
    /// `id`'s key to that winner's — see the trait method's own doc for why
    /// the caller must adopt it.
    ///
    /// **The returned ref is NOT `<dest_dir>/<name>`.** msb 0.7.1's dest-dir disk
    /// snapshot store nests the artifact one level deeper than the name it was
    /// given: `<dest_dir>/<source-sandbox-name>/snap_<32-hex-digest>` (verified
    /// live) — `name`/`rz-ckpt-<nonce-or-name>` only ever reaches msb's own INDEX
    /// (as `<source>:<name>` in `snapshot list`) and `snapshot inspect` output, not
    /// the filesystem path. So this method never constructs the final ref itself:
    /// it CAPTURES it by parsing `snapshot create`'s own stdout (the LAST non-empty
    /// line, required to be an absolute path — see [`parse_snapshot_create_ref`]),
    /// and that captured path is what's passed to `restore`/`snapshot rm`/`snapshot
    /// inspect` for this checkpoint from here on, and what's returned to the caller
    /// as the public `Checkpoint.ref`.
    ///
    /// `checkpoint_ref` (this method's own parameter) still plays its previous
    /// role: a HINT this method resolves via [`path_ref_dir`]/[`mint_checkpoint_ref`]
    /// into the `--dest-dir` directory and the `rz-ckpt-<nonce-or-name>` name handed
    /// to `snapshot create` — never the final ref by itself anymore. That absolute
    /// hint is minted by `rightsize::ContainerGuard`, one level up, against whatever
    /// cache-dir override its checkpoint registry is honoring for this call
    /// (`checkpoint_cache_dir_override`, falling back to
    /// [`rightsize::cache_dir::dir`]) — `checkpoint_ref` below arrives already
    /// carrying that hint path, so a test-isolated override still reaches the right
    /// `--dest-dir`, not just the registry file this crate never touches directly.
    /// A BARE name (no directory component) is minted here instead, purely as a
    /// defensive fallback for a caller that reaches this SPI method directly rather
    /// than through `ContainerGuard` — see [`path_ref_dir`], the same
    /// absolute-vs-bare branch `has_checkpoint` and `remove_checkpoint` already use.
    ///
    /// A container started with [`ContainerSpec::tmpfs_root_mb`] set is refused
    /// outright, before `handle` is stopped or anything else here runs: its root
    /// disk is RAM-backed and gone the moment the guest stops, so there is nothing
    /// left to snapshot. (`rightsize::ContainerGuard::checkpoint`/`checkpoint_named`
    /// carry an identical check of their own, run BEFORE any registry work — this
    /// one stays as a defense-in-depth backstop.)
    async fn create_checkpoint(
        &self,
        handle: &dyn SandboxHandle,
        checkpoint_ref: &str,
        fresh_names: &[String],
    ) -> Result<(String, Box<dyn SandboxHandle>)> {
        if handle.spec().tmpfs_root_mb.is_some() {
            return Err(RightsizeError::TmpfsRootCheckpoint);
        }
        assert!(
            !fresh_names.is_empty(),
            "rightsize::ContainerGuard::checkpoint_core always mints at least one candidate"
        );

        let id = handle.id().to_string();
        // Just the HINT this snapshot create call is built from — the dest-dir
        // directory and the name msb is given — never the final ref (see this
        // method's own doc for why the real one has to be parsed back out of
        // `snapshot create`'s stdout instead).
        let hint_path = path_ref_dir(checkpoint_ref)
            .unwrap_or_else(|| mint_checkpoint_ref(checkpoint_ref, &rightsize::cache_dir::dir()));
        let checkpoint_dir = hint_path
            .parent()
            .expect("an absolute ref path always nests under a checkpoints/ directory")
            .to_path_buf();
        let basename = hint_path
            .file_name()
            .expect("an absolute ref path always yields a path with a file name")
            .to_string_lossy()
            .into_owned();
        let msb = self.msb.clone();
        let reboot_spec_template = handle.spec().clone();
        // Mirrors `start()`'s own `keep_alive` read — needed below to keep
        // `started_names` in the same keep_alive-excluded shape `start()` gives it.
        let keep_alive = reboot_spec_template.keep_alive;
        // Only a spec with no explicit command needs the guest's actual cmdline
        // captured at all — an explicit command already tells a later restore
        // everything it needs (see `msb_checkpoint_cycle`'s own doc for where
        // this gates the capture step, before the sandbox is ever stopped).
        let attempt_cmdline_capture = reboot_spec_template.command.is_none();
        let name_for_thread = id.clone();
        let candidates = fresh_names.to_vec();
        // Taken now (not inside the blocking closure) so a panic there can't leave
        // this handle's `HandleState` holding a stale reference to a child this
        // method is already about to replace.
        let previous_attached = self
            .handles
            .lock()
            .expect("handles mutex poisoned")
            .get_mut(&id)
            .and_then(|state| state.attached.take());

        let (snapshot_ref, new_child, captured_cmdline, fresh_name) =
            tokio::task::spawn_blocking(move || {
                std::fs::create_dir_all(&checkpoint_dir).map_err(|e| {
                    RightsizeError::Backend(format!(
                        "could not create checkpoint directory {}: {e}",
                        checkpoint_dir.display()
                    ))
                })?;
                let mut invoke =
                    |args: &[String]| invoke_standalone(&msb, args, CHECKPOINT_STEP_TIMEOUT);
                let mut reboot_spec = reboot_spec_template;
                // How many candidates this closure has actually tried so far —
                // `reboot` (below) is only ever called again by
                // `reboot_with_already_exists_retry` after msb's OWN "already
                // exists" refusal on the previous one (see that function's own
                // doc: it only retries `RightsizeError::NameConflict`), so
                // `attempted > 0` here means exactly "the previous candidate,
                // `candidates[attempted - 1]`, just collided" — never anything
                // else. Reboot under a FRESH sandbox name each time, never `id`
                // (the original) and never a candidate a prior attempt already
                // touched — msb does not release a removed sandbox's on-disk
                // directory promptly on Windows (its own existence check is
                // DB-record OR directory, and only the DB record clears on
                // `rm`), so even a confirmed-absent-from-`msb ls` name can
                // still refuse a same-name restore; worse, a restore that
                // fails AFTER msb's own artifact-integrity check (the Windows
                // post-teardown access-denied transient) can leave THAT name
                // behind as a stopped sandbox record, dooming any retry under
                // it specifically — see `rightsize::backend::SandboxBackend::
                // create_checkpoint`'s own doc for the live-verified evidence.
                // `candidates` is minted by the caller
                // (`rightsize::ContainerGuard::checkpoint_core`) from the SAME
                // `rz-<run-id>-<seq>` generator every ordinary create uses, and
                // every one of them already has its own reaping-ledger entry
                // by the time this runs.
                let mut attempted = 0usize;
                let mut reboot = |real_ref: &str, captured: Option<&[String]>| {
                    if attempted > 0 {
                        // Best-effort: never lets a failed candidate's leftover
                        // sandbox record block some LATER, unrelated attempt
                        // from ever reusing this name space again. The result
                        // (found or not, succeeded or not) is deliberately
                        // ignored — `wait_for_checkpoint_name_release`/
                        // `reboot_with_already_exists_retry`'s own retry is
                        // what actually gates the NEXT attempt, not this rm.
                        // Goes straight to `invoke_standalone` rather than the
                        // `invoke` closure `msb_checkpoint_cycle` was also
                        // handed above — this closure already borrows `msb`
                        // for `spawn_and_await_running` below, and borrowing
                        // `invoke` too (itself a closure over the same `msb`)
                        // while `msb_checkpoint_cycle` holds `&mut invoke` and
                        // `&mut reboot` at once does not borrow-check.
                        let _ = invoke_standalone(
                            &msb,
                            &commands::rm(&candidates[attempted - 1]),
                            CHECKPOINT_STEP_TIMEOUT,
                        );
                    }
                    let Some(candidate) = candidates.get(attempted) else {
                        // Every candidate has already been tried and refused —
                        // a non-`NameConflict` error here ends
                        // `reboot_with_already_exists_retry`'s retry loop
                        // immediately (rather than waiting out the rest of its
                        // time budget re-trying a name that can't possibly
                        // work), surfacing exactly what the trait doc promises:
                        // the last real failure, not a bare timeout.
                        return Err(RightsizeError::Backend(format!(
                            "every candidate sandbox name was tried and refused by msb's \
                             \"already exists\" check ({} candidates)",
                            candidates.len(),
                        )));
                    };
                    attempted += 1;
                    reboot_spec.name = candidate.clone();
                    // The real ref is only known once `snapshot create` has
                    // actually run (see `msb_checkpoint_cycle`) — set it on
                    // `reboot_spec` right before rebooting from it, never up
                    // front like the pre-0.7.1 dest-dir hint could be. The
                    // captured cmdline (if any) rides along the same way, so
                    // this SAME reboot's own `try_restore_and_await_running`
                    // phase 3 has it immediately — no registry round trip
                    // needed for the in-process case.
                    reboot_spec.checkpoint_ref = Some(real_ref.to_string());
                    reboot_spec.checkpoint_captured_cmdline = captured.map(<[String]>::to_vec);
                    spawn_and_await_running(&msb, &reboot_spec)
                };
                let result = msb_checkpoint_cycle(
                    &mut invoke,
                    &mut reboot,
                    &name_for_thread,
                    &basename,
                    &checkpoint_dir,
                    attempt_cmdline_capture,
                );
                // The cycle's own `stop` step already halted the sandbox by the time
                // this returns — reap the previously-attached child (this handle's
                // old foreground `msb run` process) the same way `stop()` does,
                // regardless of the cycle's outcome.
                if let Some(mut child) = previous_attached {
                    reap_attached_child(&mut child);
                }
                result.map(|(checkpoint_ref, rebooted, captured_cmdline)| {
                    // `reboot`'s LAST call is the one that actually succeeded
                    // (or this whole closure would already have returned an
                    // `Err` via `?` in `spawn_and_await_running`'s own caller
                    // above) — `reboot_spec.name` is exactly the candidate that
                    // attempt used, i.e. the winner. The closure's mutable
                    // borrow of `reboot_spec` ends here (its last use), so
                    // reading it back out is fine.
                    (checkpoint_ref, rebooted, captured_cmdline, reboot_spec.name)
                })
            })
            .await
            .map_err(|e| RightsizeError::Backend(format!("checkpoint task panicked: {e}")))??;

        // The reboot succeeded under `fresh_name` (whichever candidate won),
        // not `id` — this handle's per-container runtime state (the attached
        // child, any exec-tunnel resources, the captured cmdline) moves to the
        // fresh key. `id`'s own entry is dropped outright rather than left
        // behind: the sandbox it named is gone (removed mid-cycle), and the
        // reaping ledger one layer up already leaves that name's OWN entry to
        // its existing not-found-tolerant sweep — there is nothing for this
        // backend's in-memory map to keep it for.
        let mut handles = self.handles.lock().expect("handles mutex poisoned");
        handles.remove(&id);
        handles.insert(
            fresh_name.clone(),
            HandleState {
                // Already `Option<Child>` — see `start()`'s own assignment for why.
                attached: new_child,
                resources: Vec::new(),
                // Read back once by `last_checkpoint_captured_cmdline`, right
                // after this call returns — see that method's own doc for who
                // reads it.
                captured_cmdline,
            },
        );
        drop(handles);
        // `started_names` is exactly what `close()` sweeps on this run's own-process
        // shutdown (see `start()`'s own comment) — it has to follow the same `id` ->
        // `fresh_name` re-key `handles` just got above, or `close()` after a
        // checkpoint wastes a stop/rm on the already-removed original name and never
        // touches the sandbox that is actually live. Mirrors `start()`'s own
        // keep_alive exclusion: a reuse sandbox was never added in the first place,
        // so it must not be added here either.
        let mut started_names = self
            .started_names
            .lock()
            .expect("started_names mutex poisoned");
        started_names.remove(&id);
        if !keep_alive {
            started_names.insert(fresh_name.clone());
        }
        drop(started_names);

        // The returned handle's spec carries only the WINNING name changed —
        // never the just-used `checkpoint_ref`/`checkpoint_captured_cmdline`
        // `reboot` set on its own working copy per attempt, which is why this
        // is built from `handle.spec()` fresh rather than from the closure's
        // (now-consumed) `reboot_spec`.
        let mut live_spec = handle.spec().clone();
        live_spec.name = fresh_name;

        Ok((snapshot_ref, Box::new(Handle { spec: live_spec })))
    }

    /// See the trait method's own doc. Looked up under the same `handles` mutex
    /// every other per-handle mutable-state accessor in this backend uses.
    fn last_checkpoint_captured_cmdline(&self, handle: &dyn SandboxHandle) -> Option<Vec<String>> {
        self.handles
            .lock()
            .expect("handles mutex poisoned")
            .get(handle.id())
            .and_then(|state| state.captured_cmdline.clone())
    }

    /// `msb snapshot rm <checkpoint_ref> -f` — best-effort, matching
    /// [`Self::remove_by_name`]'s own "not found is fine" contract; verified live
    /// to delete both msb's index entry and the dest-dir artifact directory for a
    /// path ref. `checkpoint_ref` is passed UNCHANGED, never reduced to a
    /// basename: msb 0.7.1's dest-dir disk-scope snapshots resolve `rm` by their
    /// own artifact path only (a bare name or `group:member` spelling does not
    /// resolve at all, verified live — see [`commands::snapshot_rm`]'s doc), and a
    /// path ref's own ref string already IS that artifact path; a legacy
    /// bare-name ref predates dest-dir checkpoints and is a name either way, which
    /// is exactly what this passes through unchanged for that case too.
    ///
    /// The filesystem sweep below (removing a path ref's artifact directory by
    /// hand) only runs when msb's own removal is known to have left nothing
    /// behind: either it actually succeeded, or it reports the ref as already
    /// gone (see [`is_snapshot_not_found`]) — both cases where msb's index has no
    /// remaining reference to the directory. Every OTHER nonzero exit leaves the
    /// directory alone, most notably msb's HEAD-removal refusal (`invalid
    /// config: cannot remove current head snap_...; first select another
    /// snapshot with 'msb snapshot head src:<snapshot>'`, verified live for the
    /// newest of several snapshots from the same source sandbox): deleting the
    /// directory anyway would leave msb's own index pointing at files that no
    /// longer exist, corrupting its snapshot store rather than cleaning it up.
    /// This backend does not attempt automatic head rotation to work around that
    /// refusal — the checkpoints docs' "Cleanup" section names it as a known
    /// limitation instead. That refusal (and any other non-"not found" failure)
    /// DOES surface as an `Err` here, though — "best-effort" above only means
    /// "not found is success," matching the docker backend's own
    /// `remove_checkpoint` (any real failure but a 404 is an `Err`); a caller
    /// that needs to know whether the artifact is actually gone (without also
    /// caring why a removal failed) has [`Self::has_checkpoint`] for that.
    async fn remove_checkpoint(&self, checkpoint_ref: &str) -> Result<()> {
        let msb = self.msb.clone();
        let rm_target = checkpoint_ref.to_string();
        let artifact_dir = path_ref_dir(checkpoint_ref);
        tokio::task::spawn_blocking(move || {
            let result = invoke_standalone(&msb, &commands::snapshot_rm(&rm_target), STOP_TIMEOUT)?;
            let not_found = is_snapshot_not_found(&format!("{}\n{}", result.stdout, result.stderr));
            let safe_to_sweep = result.exit_code == 0 || not_found;
            if safe_to_sweep {
                if let Some(dir) = &artifact_dir {
                    if dir.exists() {
                        let _ = std::fs::remove_dir_all(dir);
                    }
                }
                return Ok(());
            }
            Err(RightsizeError::Backend(format!(
                "msb could not remove checkpoint '{rm_target}' (exit {}): {}",
                result.exit_code,
                result.stderr.trim()
            )))
        })
        .await
        .map_err(|e| RightsizeError::Backend(format!("checkpoint removal task panicked: {e}")))?
    }

    /// A path ref (see [`path_ref_dir`]) is checked on the filesystem instead —
    /// its artifact directory existing AND containing `snapshot.json` — with no
    /// `msb` call at all. A bare-name ref falls through to `msb snapshot inspect
    /// <ref>` — the named-checkpoint existence probe
    /// (`SandboxBackend::has_checkpoint`, `Checkpoint::find`'s staleness check).
    /// Exit 0 -> `Ok(true)`. A nonzero exit whose output names msb's "not found"
    /// wording (see [`is_snapshot_not_found`]) -> `Ok(false)`. Any other nonzero
    /// exit, or the invocation itself failing to run, surfaces as a
    /// `RightsizeError::Backend` — this SPI forbids a probe failure from resolving
    /// to "absent."
    async fn has_checkpoint(&self, checkpoint_ref: &str) -> Result<bool> {
        if let Some(dir) = path_ref_dir(checkpoint_ref) {
            let exists = tokio::task::spawn_blocking(move || path_ref_artifact_exists(&dir))
                .await
                .map_err(|e| {
                    RightsizeError::Backend(format!("checkpoint probe task panicked: {e}"))
                })?;
            return Ok(exists);
        }

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

    /// `msb snapshot save <ref> <dest>` (never `--with-image` — see
    /// `crate::commands::snapshot_export`'s own doc for why) — the
    /// checkpoint-archive feature's export primitive
    /// (`rightsize::Checkpoint::export_to`), with the Windows salvage described by
    /// [`salvage_archive_staging_file`] wired in as
    /// [`msb_export_checkpoint_cycle`]'s recovery step. The Windows gate is
    /// `cfg!(windows)` rather than `#[cfg(windows)]` so the salvage and its
    /// classifier stay compiled — and unit-tested — on every host, and only
    /// whether they are REACHED is platform-specific. Off Windows the fsync msb
    /// gets wrong is a legal operation, so the failure cannot arise there and
    /// salvaging would only paper over a genuinely different bug.
    async fn export_checkpoint(&self, checkpoint_ref: &str, dest: &Path) -> Result<()> {
        let msb = self.msb.clone();
        let snapshot_ref = checkpoint_ref.to_string();
        let dest = dest.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let mut invoke_export = || {
                invoke_standalone(
                    &msb,
                    &commands::snapshot_export(&snapshot_ref, &dest),
                    ARCHIVE_TIMEOUT,
                )
            };
            let mut salvage = |dest: &Path| {
                if cfg!(windows) {
                    salvage_archive_staging_file(dest)
                } else {
                    false
                }
            };
            msb_export_checkpoint_cycle(&mut invoke_export, &mut salvage, &snapshot_ref, &dest)
        })
        .await
        .map_err(|e| RightsizeError::Backend(format!("checkpoint export task panicked: {e}")))?
    }

    /// `msb snapshot load <src_file> --dest <cache_dir>/checkpoints`, treating
    /// "already exists" as success — the checkpoint-archive feature's import
    /// primitive (`rightsize::Checkpoint::import_from`). `ref_hint` (the archive
    /// manifest's original ref) plays no role here: msb's import is
    /// content-addressed, so the effective ref is always the loaded artifact's own
    /// absolute path, parsed from `load`'s stdout (see
    /// [`msb_import_checkpoint_cycle`] for the orchestration this delegates to) —
    /// never `ref_hint`, and never a `snapshot list`-resolved digest-dir name (msb
    /// 0.7.1 no longer needs that extra round trip; `load` prints the artifact's
    /// own path directly).
    ///
    /// `--dest` is always this backend's own `<cache_dir>/checkpoints` — the same
    /// directory a created checkpoint's artifact lands under (see
    /// `mint_checkpoint_ref`'s doc for the identical fallback there) — never msb's
    /// global default snapshot store, so an imported checkpoint's ref stays under
    /// this library's own cache dir like every other ref it mints or captures.
    /// Created up front (`create_dir_all`, best-effort-idempotent) since `load`
    /// itself does not appear to create a missing `--dest` directory.
    async fn import_checkpoint(&self, src_file: &Path, _ref_hint: &str) -> Result<String> {
        let msb = self.msb.clone();
        let archive_path = src_file.to_path_buf();
        let checkpoints_dir = rightsize::cache_dir::dir().join("checkpoints");
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&checkpoints_dir).map_err(|e| {
                RightsizeError::Backend(format!(
                    "could not create checkpoints directory {}: {e}",
                    checkpoints_dir.display()
                ))
            })?;
            let mut invoke_import = || {
                invoke_standalone(
                    &msb,
                    &commands::snapshot_import(&archive_path, &checkpoints_dir),
                    ARCHIVE_TIMEOUT,
                )
            };
            msb_import_checkpoint_cycle(&mut invoke_import)
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
#[derive(Debug)]
enum PreRunningFailure {
    CacheCorruption {
        output: String,
    },
    StateDbError {
        output: String,
    },
    InstallLockActive {
        output: String,
    },
    /// `msb restore` exited nonzero with the Windows post-teardown "Access is
    /// denied" transient (see [`is_restore_access_denied`]) — restore-only, never
    /// produced by `run`.
    RestoreAccessDenied {
        output: String,
    },
    Other(RightsizeError),
}

/// Runs on a blocking thread: spawns `msb run <spec's argv>` attached (no `-d`),
/// polls `msb ls --format json` until the sandbox reaches `Running`, and returns the
/// live child for `start()` to keep around. [`try_spawn_and_await_running`]'s
/// fast-exit post-mortem (see its doc) also returns `Ok` here, unmodified — that
/// already-exited child passes straight through this match with no retry, same as
/// any other successful attempt.
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
fn spawn_and_await_running(msb: &Path, spec: &ContainerSpec) -> Result<Option<Child>> {
    match try_spawn_and_await_running(msb, spec) {
        Ok(child) => Ok(child),
        Err(PreRunningFailure::Other(e)) => Err(e),
        Err(PreRunningFailure::InstallLockActive { output }) => {
            // msb refuses `run` outright while its internal install lock is held (see
            // is_msb_install_lock_active). The message names a deadline ~30 minutes out,
            // but both captured occurrences cleared within the same test run — boots
            // seconds later succeeded — so this polls briefly rather than trusting the
            // deadline. The budget expiring surfaces the last refusal: a lock held that
            // long really is stuck, and waiting here would only hide it.
            let deadline = std::time::Instant::now() + INSTALL_LOCK_RETRY_BUDGET;
            let mut last = output;
            loop {
                if std::time::Instant::now() >= deadline {
                    return Err(RightsizeError::Backend(format!(
                        "msb run for sandbox {} was refused for {}s by msb's \
                         install-operation lock — both observed occurrences cleared within \
                         seconds, so a lock held this long looks like a genuinely stuck msb \
                         install on this host.\n{last}",
                        spec.name,
                        INSTALL_LOCK_RETRY_BUDGET.as_secs(),
                    )));
                }
                std::thread::sleep(INSTALL_LOCK_RETRY_DELAY);
                match try_spawn_and_await_running(msb, spec) {
                    Ok(child) => return Ok(child),
                    Err(PreRunningFailure::InstallLockActive { output }) => last = output,
                    Err(PreRunningFailure::Other(e)) => return Err(e),
                    Err(PreRunningFailure::StateDbError { output })
                    | Err(PreRunningFailure::CacheCorruption { output })
                    | Err(PreRunningFailure::RestoreAccessDenied { output }) => {
                        return Err(RightsizeError::Backend(format!(
                            "msb run for sandbox {} exited before reaching Running:\n{output}",
                            spec.name,
                        )));
                    }
                }
            }
        }
        Err(PreRunningFailure::RestoreAccessDenied { output }) => {
            // Windows-only transient: the source sandbox's just-written snapshot
            // artifact hasn't finished having its file handle released by the OS
            // yet (see `is_restore_access_denied`). One short retry, no heal
            // step — same one-shot shape as the state-database race above, since
            // this clears the same way: a retry issued a little later simply
            // doesn't race the OS's own deferred release anymore.
            std::thread::sleep(RESTORE_ACCESS_DENIED_RETRY_DELAY);
            match try_spawn_and_await_running(msb, spec) {
                Ok(child) => Ok(child),
                Err(PreRunningFailure::RestoreAccessDenied {
                    output: retry_output,
                }) => Err(RightsizeError::Backend(format!(
                    "msb restore for sandbox {} hit the Windows post-teardown \
                     access-denied transient twice in a row — the usual cause (the OS still \
                     releasing the snapshot artifact's file handle) clears well within one \
                     retry, so this looks like a real access problem on this \
                     host.\nfirst attempt:\n{output}\nafter retry:\n{retry_output}",
                    spec.name,
                ))),
                // A different classified transient on the retry is not this arm's to
                // untangle — surface it plainly rather than nesting retry policies.
                Err(PreRunningFailure::CacheCorruption {
                    output: retry_output,
                })
                | Err(PreRunningFailure::InstallLockActive {
                    output: retry_output,
                })
                | Err(PreRunningFailure::StateDbError {
                    output: retry_output,
                }) => Err(RightsizeError::Backend(format!(
                    "msb run for sandbox {} exited before reaching Running:\n{retry_output}",
                    spec.name,
                ))),
                Err(PreRunningFailure::Other(e)) => Err(e),
            }
        }
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
                // A different classified transient on the retry is not this arm's to
                // untangle — surface it plainly rather than nesting retry policies.
                Err(PreRunningFailure::CacheCorruption {
                    output: retry_output,
                })
                | Err(PreRunningFailure::InstallLockActive {
                    output: retry_output,
                })
                | Err(PreRunningFailure::RestoreAccessDenied {
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
                // A different classified transient on the retry is not this arm's to
                // untangle — surface it plainly rather than nesting retry policies.
                Err(PreRunningFailure::StateDbError {
                    output: retry_output,
                })
                | Err(PreRunningFailure::InstallLockActive {
                    output: retry_output,
                })
                | Err(PreRunningFailure::RestoreAccessDenied {
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

/// One `msb run`/`msb restore` attempt: dispatches on `spec.checkpoint_ref` — `Some`
/// means a checkpoint restore (`commands::restore`, on msb 0.7.1+ — never
/// `--disk-only`, which that command rejects against the disk-scope snapshots this
/// backend creates; see `commands::restore`'s own doc), `None` means an ordinary
/// boot (`commands::run`) — and returns either a classified [`PreRunningFailure`] or
/// the successful attempt's live child, when it has one: `Some` for an attached
/// `run`, `None` for a detached `restore` (see [`try_restore_and_await_running`]'s
/// own doc for why). Both this backend's normal `start()` path (a
/// [`rightsize::Container::from_checkpoint`]-built spec, spec.checkpoint_ref already
/// set) and [`MsbCliBackend::create_checkpoint`]'s own re-boot reach the restore
/// branch. Never retries by itself — [`spawn_and_await_running`] is the only caller
/// and owns the one-shot heal+retry policy, for either branch alike.
fn try_spawn_and_await_running(
    msb: &Path,
    spec: &ContainerSpec,
) -> std::result::Result<Option<Child>, PreRunningFailure> {
    match &spec.checkpoint_ref {
        Some(snapshot_path) => try_restore_and_await_running(msb, spec, snapshot_path),
        None => try_run_and_await_running(msb, spec).map(Some),
    }
}

/// The `spec.checkpoint_ref.is_none()` branch of [`try_spawn_and_await_running`]:
/// spawns an attached `msb run` child, polls until `Running`, and returns either the
/// live child or a classified [`PreRunningFailure`]. Byte-for-byte the same
/// supervision this backend has always given an ordinary boot — see
/// [`try_restore_and_await_running`] for the detached-restore counterpart, which
/// this never falls back to or is called by.
///
/// The tail drained here carries msb's own boot output only — registry/pull errors,
/// a crash before the sandbox exists — never the workload's. `logs()` never reads
/// from it; workload output always comes from a `msb logs` invocation.
///
/// **Fast-exit post-mortem (msb 0.6.16+):** msb's convergent-lifecycle rework stopped
/// surfacing `Running` at all for a workload that finishes before this loop's next
/// poll — a short build/test script, e.g. `alpine -- true` — so the exit-0 case below
/// is no longer necessarily a failed boot. When the child exits 0 and none of the
/// classified failures above match, [`fast_exit_ran_to_completion`] is given one
/// chance to confirm the sandbox genuinely finished (its own state is `Stopped` in
/// `msb ls`, and the system log carries the boot-completion marker only the guest
/// agent writes) before this falls back to today's generic "before reaching Running"
/// error. A non-zero exit never takes this path — see that function's doc for why
/// both signals, not just the exit code, are required.
fn try_run_and_await_running(
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
            if is_image_cache_corruption(&output) {
                return Err(PreRunningFailure::CacheCorruption { output });
            }
            if is_msb_state_db_error(&output) {
                return Err(PreRunningFailure::StateDbError { output });
            }
            if is_msb_install_lock_active(&output) {
                return Err(PreRunningFailure::InstallLockActive { output });
            }
            if is_port_bind_conflict(&output) {
                return Err(PreRunningFailure::Other(RightsizeError::PortBindConflict {
                    message: format!(
                        "msb run for sandbox {} could not bind a host port: {output}",
                        spec.name
                    ),
                    source: None,
                }));
            }
            if is_name_conflict(&output) {
                return Err(PreRunningFailure::Other(RightsizeError::NameConflict {
                    message: format!(
                        "msb run for sandbox {} could not start — a sandbox with this name \
                         already exists: {output}",
                        spec.name
                    ),
                    source: None,
                }));
            }
            // None of the known bad signatures matched. On msb 0.6.16+ a clean exit
            // here is not necessarily a failed boot — see this function's doc for why
            // — so a zero exit gets one post-mortem check before falling back to
            // today's generic error below. A non-zero exit skips straight to it: only
            // a clean exit can mean the workload ran to completion, so nothing about
            // the failure path changes for any other exit code.
            if status.success() && fast_exit_ran_to_completion(msb, &spec.name) {
                return Ok(child);
            }
            return Err(PreRunningFailure::Other(RightsizeError::Backend(format!(
                "msb run for sandbox {} exited (code {}) before reaching Running — check the \
                 image entrypoint and `msb run` output below:\n{output}",
                spec.name,
                status.code().unwrap_or(-1)
            ))));
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

/// The `spec.checkpoint_ref.is_some()` branch of [`try_spawn_and_await_running`]:
/// runs `msb restore <snapshot_path> --name <spec.name> ...` (`snapshot_path` is
/// `spec.checkpoint_ref`'s own value, unwrapped by the caller — see
/// `commands::restore`'s own doc for the argv this builds) and supervises it as the
/// DETACHED boot it actually is, unlike [`try_run_and_await_running`]'s attached one.
///
/// **Why this shape.** `msb restore` (msb 0.7.1+) creates a detached sandbox —
/// restore.rs's own doc says "Restore a snapshot into a new detached sandbox" — so
/// the `restore` invocation itself activates the sandbox and EXITS, typically within
/// seconds and with little or no stdout, once activation succeeds; exit 0 means
/// success, a nonzero exit means the restore failed with the reason on stderr/
/// stdout. The sandbox then keeps booting in the background on its own and reaches
/// `Running` some time after `restore` has already exited (live-verified: `msb ls`
/// shows it `Running`, and exec works, after the `restore` process is long gone).
/// Supervising that the way [`try_run_and_await_running`] supervises an attached
/// `msb run` child — treating the child's exit before this backend has observed
/// `Running` as a failed boot — misreads every successful restore as one: exactly
/// the CI failure this function exists to fix.
///
/// **Three phases, one budget.** [`FIRST_RUN_TIMEOUT`] bounds the whole attempt, the
/// same overall budget [`try_run_and_await_running`]'s single loop gives an attached
/// boot — this just spends it in sequential steps instead of one interleaved loop,
/// since a detached restore genuinely has more than one thing to wait for in order:
/// 1. **Wait for `restore` itself to exit**, polling [`READINESS_POLL`] apart like
///    the attached path does. A nonzero exit is a failed restore — its combined
///    stdout/stderr is classified through the exact same [`PreRunningFailure`]
///    cascade [`try_run_and_await_running`] applies to `run`'s output (install-lock,
///    state-db, image-cache-corruption, port-bind, name-conflict, then a generic
///    fallback), since msb reports those conditions identically for `restore`, PLUS
///    the one restore-only transient [`is_restore_access_denied`] classifies. Exit
///    0 proceeds to the next phase; there is no fast-exit post-mortem here (unlike
///    `run`'s) — a clean, prompt exit is not a fast-exit special case for `restore`,
///    it is the ordinary, expected outcome of a successful one.
/// 2. **Poll `msb ls --format json`** (same [`READINESS_POLL`] interval) until this
///    sandbox reports `Running`. Reaching `Stopped`, or dropping out of `ls`
///    entirely, at any point during this poll is a definite boot failure — nothing
///    is still in flight to wait out the rest of the budget for — surfaced with the
///    sandbox's own system log (`msb logs <name> --source system`, the same command
///    [`fast_exit_ran_to_completion`] already reads) attached as a best-effort
///    diagnostic. Any other status (`Starting`, or the name not listed yet on an
///    early poll) just keeps polling.
/// 3. **Revive the workload.** `Running` here means only the guest agent is up —
///    verified live against a real msb 0.7.1 restore, the checkpoint's own
///    workload command never re-runs on its own — so this phase spawns
///    `msb exec [-e K=V]... <name> -- <argv>` (see [`spawn_workload_exec`]) as a
///    LONG-LIVED attached child and hands it back as this attempt's live child.
///    `<argv>` is `spec.command` when it's `Some` (an explicit command always
///    wins), else `spec.checkpoint_captured_cmdline` (the guest cmdline
///    `create_checkpoint`'s own capture step recovered at checkpoint time — see
///    [`msb_checkpoint_cycle`]); a spec with neither fails the restore outright
///    with a typed error rather than booting silently idle — this is the ONLY
///    way this function can still return an error after `restore` itself and the
///    `Running` poll have both already succeeded.
///
/// **A live child again, on success** — unlike the pre-phase-3 shape of this
/// function, which always returned `Ok(None)`: [`HandleState::attached`] ends up
/// `Some` for a restored sandbox, exactly like an ordinary `run` boot's, so
/// exit-based death detection, `stop()`'s reap, and every other place that already
/// treats `attached` uniformly need no changes — see the module docs. `stop`/`rm`/
/// `logs` are unaffected either way; they always drive the sandbox by name through
/// the `msb` CLI, never through the attached child.
fn try_restore_and_await_running(
    msb: &Path,
    spec: &ContainerSpec,
    snapshot_path: &str,
) -> std::result::Result<Option<Child>, PreRunningFailure> {
    let argv = commands::restore(spec, snapshot_path);
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

    // Phase 1: wait (bounded) for the detached `restore` invocation itself to exit —
    // see this function's own doc for why that, not `Running`, is the event this
    // phase waits on.
    let deadline = Instant::now() + FIRST_RUN_TIMEOUT;
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|e| PreRunningFailure::Other(RightsizeError::from(e)))?
        {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = t_out.join();
            let _ = t_err.join();
            let output = collect_tail(&tail);
            return Err(PreRunningFailure::Other(RightsizeError::Backend(format!(
                "msb restore for sandbox {} did not exit within {}s — a successful detached \
                 restore activates and exits within seconds, so msb itself may be overloaded \
                 or unresponsive; last output:\n{output}",
                spec.name,
                FIRST_RUN_TIMEOUT.as_secs()
            ))));
        }
        std::thread::sleep(READINESS_POLL);
    };
    let _ = t_out.join();
    let _ = t_err.join();
    let output = collect_tail(&tail);

    if !status.success() {
        if is_image_cache_corruption(&output) {
            return Err(PreRunningFailure::CacheCorruption { output });
        }
        if is_msb_state_db_error(&output) {
            return Err(PreRunningFailure::StateDbError { output });
        }
        if is_msb_install_lock_active(&output) {
            return Err(PreRunningFailure::InstallLockActive { output });
        }
        if is_restore_access_denied(&output) {
            return Err(PreRunningFailure::RestoreAccessDenied { output });
        }
        if is_port_bind_conflict(&output) {
            return Err(PreRunningFailure::Other(RightsizeError::PortBindConflict {
                message: format!(
                    "msb restore for sandbox {} could not bind a host port: {output}",
                    spec.name
                ),
                source: None,
            }));
        }
        if is_name_conflict(&output) {
            return Err(PreRunningFailure::Other(RightsizeError::NameConflict {
                message: format!(
                    "msb restore for sandbox {} could not start — a sandbox with this name \
                     already exists: {output}",
                    spec.name
                ),
                source: None,
            }));
        }
        return Err(PreRunningFailure::Other(RightsizeError::Backend(format!(
            "msb restore for sandbox {} exited (code {}) — the restore itself failed, so the \
             sandbox never activated; check the snapshot and `msb restore` output below:\n{output}",
            spec.name,
            status.code().unwrap_or(-1)
        ))));
    }

    // Phase 2: `restore` exited 0 — activation succeeded and this detached sandbox
    // is now booting on its own in the background. Poll `msb ls` the same interval
    // the attached path uses, under the same overall deadline, until it reports
    // Running.
    loop {
        if let Ok(ls_result) = invoke_standalone(msb, &commands::ls(), LOGS_TIMEOUT) {
            let sandbox_status = ls_json::status_of(&ls_result.stdout, &spec.name);
            match sandbox_status.as_deref() {
                Some("Running") => return spawn_workload_exec(msb, spec),
                Some("Stopped") | None => {
                    let reason = if sandbox_status.is_none() {
                        "dropped out of `msb ls` entirely"
                    } else {
                        "reached Stopped"
                    };
                    return Err(PreRunningFailure::Other(RightsizeError::Backend(format!(
                        "sandbox {} never reached Running after a successful `msb restore` — \
                         it {reason} while this backend polled `msb ls`. {}",
                        spec.name,
                        restore_boot_failure_diagnostics(msb, &spec.name),
                    ))));
                }
                Some(_still_booting) => {}
            }
        }
        if Instant::now() >= deadline {
            return Err(PreRunningFailure::Other(RightsizeError::Backend(format!(
                "sandbox {} did not reach Running within {}s after a successful `msb restore` \
                 — this can mean a slow disk restore or msb itself being unresponsive. {}",
                spec.name,
                FIRST_RUN_TIMEOUT.as_secs(),
                restore_boot_failure_diagnostics(msb, &spec.name),
            ))));
        }
        std::thread::sleep(READINESS_POLL);
    }
}

/// Phase 3 of [`try_restore_and_await_running`]: the restored sandbox reached
/// `Running` with only its guest agent up (verified live — upstream's `restore`
/// never re-runs the workload on its own), so this spawns
/// `msb exec [-e K=V]... <name> -- <argv>` (see [`commands::exec_workload`]) to
/// revive it, as a LONG-LIVED attached child this hands back as the boot's own
/// live child.
///
/// **`<argv>` resolution**: `spec.command` when `Some` (an explicit command
/// always wins over a capture — the checkpoint's own spec is authoritative),
/// else `spec.checkpoint_captured_cmdline` (see that field's own doc). A spec
/// with neither is a checkpoint that predates workload capture — this fails the
/// restore outright with a typed [`RightsizeError::Backend`] rather than
/// booting the sandbox silently idle, per the module docs.
///
/// **The agent-connect race, again.** The exec issued here can race the very
/// same window [`is_agent_endpoint_not_ready`]'s doc describes for the generic
/// `exec()` trait method — `Running` says nothing about whether the guest
/// agent's own endpoint exists yet — so an early exit whose output carries that
/// signature is retried (the same [`AGENT_ENDPOINT_RETRY_BUDGET`]/
/// [`AGENT_ENDPOINT_RETRY_DELAY`] this backend's `exec()` already uses), never
/// classified as a failed workload.
///
/// **Otherwise, an early exit is judged like [`try_run_and_await_running`]'s own
/// fast-exit case.** The exec child is watched for
/// [`WORKLOAD_EXEC_EARLY_EXIT_GRACE`] — long enough to catch a command that
/// fails immediately (a typo'd binary, a permission error), short enough to
/// never hold up a restore for the ordinary case, a genuinely long-lived
/// workload, which returns successfully well before the deadline without ever
/// waiting it out. Exiting nonzero within that window is a classified boot
/// failure, its output attached. Exiting 0 gets the SAME one-chance post-mortem
/// [`fast_exit_ran_to_completion`] gives an attached `run` child (the sandbox's
/// own state is `Stopped` and the system log carries the boot-completion
/// marker) before falling back to the same failure path — mirroring the
/// attached run path exactly, since a workload that finishes fast and clean is
/// no more a failure here than it is there.
fn spawn_workload_exec(
    msb: &Path,
    spec: &ContainerSpec,
) -> std::result::Result<Option<Child>, PreRunningFailure> {
    let workload_argv = match (&spec.command, &spec.checkpoint_captured_cmdline) {
        (Some(cmd), _) => cmd.clone(),
        (None, Some(cmd)) => cmd.clone(),
        (None, None) => {
            return Err(PreRunningFailure::Other(RightsizeError::Backend(format!(
                "sandbox {} was restored from a checkpoint that predates workload capture — \
                 it has neither an explicit command nor a captured guest cmdline, so there is \
                 nothing to run; re-checkpoint the source container with this version of \
                 rightsize to record its command, or restore under a backend whose checkpoint \
                 mechanism doesn't restart the workload (docker) instead",
                spec.name,
            ))));
        }
    };
    let exec_argv = commands::exec_workload(&spec.name, &spec.env, &workload_argv);

    let agent_deadline = Instant::now() + AGENT_ENDPOINT_RETRY_BUDGET;
    loop {
        let mut child = spawn_msb_command(|| {
            let mut cmd = Command::new(msb);
            cmd.args(&exec_argv)
                .stdin(Stdio::null()) // msb exec blocks on stdin EOF; give every child a closed stdin.
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            cmd
        })
        .map_err(|e| {
            PreRunningFailure::Other(RightsizeError::Backend(format!(
                "failed to spawn msb {}: {e}",
                exec_argv.join(" ")
            )))
        })?;

        let stdout_pipe = child.stdout.take().expect("piped stdout");
        let stderr_pipe = child.stderr.take().expect("piped stderr");
        let tail: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
        let t_out = spawn_tail_drain(stdout_pipe, tail.clone());
        let t_err = spawn_tail_drain(stderr_pipe, tail.clone());

        let grace_deadline = Instant::now() + WORKLOAD_EXEC_EARLY_EXIT_GRACE;
        let early_exit = loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|e| PreRunningFailure::Other(RightsizeError::from(e)))?
            {
                break Some(status);
            }
            if Instant::now() >= grace_deadline {
                break None;
            }
            std::thread::sleep(READINESS_POLL);
        };

        let Some(status) = early_exit else {
            // Still running past the grace window — the ordinary, expected
            // outcome for a genuinely long-lived workload.
            return Ok(Some(child));
        };

        let _ = t_out.join();
        let _ = t_err.join();
        let output = collect_tail(&tail);

        if is_agent_endpoint_not_ready(&output) && Instant::now() < agent_deadline {
            std::thread::sleep(AGENT_ENDPOINT_RETRY_DELAY);
            continue;
        }

        if status.success() && fast_exit_ran_to_completion(msb, &spec.name) {
            return Ok(Some(child));
        }

        return Err(PreRunningFailure::Other(RightsizeError::Backend(format!(
            "the workload exec for sandbox {} exited (code {}) right after its checkpoint \
             restore reached Running — check the command and its output below:\n{output}",
            spec.name,
            status.code().unwrap_or(-1),
        ))));
    }
}

/// Joins `tail`'s currently buffered lines the same way every boot-output error
/// message in this module does — factored out once [`try_restore_and_await_running`]
/// needed the exact same snapshot-and-join [`try_run_and_await_running`] already
/// does inline at each of its own call sites.
fn collect_tail(tail: &Arc<Mutex<VecDeque<String>>>) -> String {
    tail.lock()
        .expect("tail mutex poisoned")
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .join("\n")
}

/// Best-effort system-log diagnostics for a restore's background boot failing —
/// `msb logs <name> --source system --tail 1000`, the same command
/// [`fast_exit_ran_to_completion`] already reads to confirm a completed attached
/// boot. Never itself turns into an error: an unreadable or empty log is reported as
/// such in the returned string rather than surfacing a nested failure.
fn restore_boot_failure_diagnostics(msb: &Path, name: &str) -> String {
    match invoke_standalone(msb, &commands::logs_system(name), LOGS_TIMEOUT) {
        Ok(result) if !result.stdout.trim().is_empty() || !result.stderr.trim().is_empty() => {
            format!(
                "`msb logs {name} --source system` output:\n{}{}",
                result.stdout, result.stderr
            )
        }
        Ok(_) => format!("`msb logs {name} --source system` returned no output"),
        Err(e) => format!("`msb logs {name} --source system` could not be read: {e}"),
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

/// [`msb_checkpoint_cycle`]'s `reboot` parameter type, factored out (clippy's
/// `type_complexity`) — the real ref plus the captured cmdline (if any, see that
/// function's own doc), returning the caller's chosen success type `T`.
type RebootFn<'a, T> = dyn FnMut(&str, Option<&[String]>) -> Result<T> + 'a;

/// [`msb_checkpoint_cycle`]'s post-`rm` guard: polls `invoke(&commands::ls())`
/// every [`READINESS_POLL`] until `name` no longer appears in `msb ls
/// --format json` at all (see [`ls_json::try_is_listed`]), bounded by
/// [`CHECKPOINT_NAME_RELEASE_BUDGET`] — see that constant's own doc for why
/// `rm` exiting successfully is not the same as the name actually being free.
/// The very first check runs before any sleep, so unix (which releases the
/// name synchronously) always passes on one call.
///
/// A FAILED probe never counts as a confirmed release: `invoke` returning
/// `Err` (spawn failure, timeout), an `Ok` result with a nonzero exit code, and
/// stdout that doesn't parse as `msb ls`'s documented shape are all treated
/// identically — none of them confirm the name is gone, so none of them may
/// end the wait. `invoke_standalone` only ever errors on a spawn failure or a
/// hard timeout, never on a nonzero `msb ls` exit code (it returns that as an
/// ordinary `Ok(ExecResult)`), so the exit code is checked here explicitly
/// rather than trusted to `invoke`'s own `Result` — a transient daemon hiccup
/// on `msb ls` itself is plausible precisely because teardown is still in
/// flight. [`ls_json::try_is_listed`] (unlike [`ls_json::status_of`], whose
/// `None` is deliberately ambiguous between "not listed" and "couldn't parse"
/// for callers that treat both the same way) keeps that ambiguity visible as
/// its own `None` instead of folding it into "absent" — only `Some(false)` (an
/// exit-0, successfully-parsed listing that omits `name`) counts as a
/// confirmed release here; anything else — a nonzero exit, unparseable stdout
/// even on exit 0, or the name still present — is treated as "still
/// inconclusive" and simply polled again, the same as an entry that is still
/// listed.
///
/// A budget-exceeded outcome returns a plain `Err` naming `name` — never a
/// silent success and never an unbounded loop — so [`msb_checkpoint_cycle`]
/// never lets `reboot` even attempt a restore already known to collide.
fn wait_for_checkpoint_name_release(
    invoke: &mut dyn FnMut(&[String]) -> Result<ExecResult>,
    name: &str,
) -> Result<()> {
    let deadline = Instant::now() + CHECKPOINT_NAME_RELEASE_BUDGET;
    loop {
        let confirmed_absent = invoke(&commands::ls())
            .ok()
            .filter(|ls| ls.exit_code == 0)
            .and_then(|ls| ls_json::try_is_listed(&ls.stdout, name))
            .map(|listed| !listed)
            .unwrap_or(false);
        if confirmed_absent {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(RightsizeError::Backend(format!(
                "sandbox {name} still shows up in `msb ls` (or `msb ls` itself never confirmed \
                 it absent) {}s after `msb rm {name}` — msb's own teardown normally frees a \
                 removed sandbox's name well within that, so this looks stuck rather than \
                 merely slow; check `msb ls` and `msb rm {name}` by hand",
                CHECKPOINT_NAME_RELEASE_BUDGET.as_secs(),
            )));
        }
        std::thread::sleep(READINESS_POLL);
    }
}

/// Orchestrates the checkpoint feature's [capture →] stop → snapshot → rm →
/// re-boot cycle against `name` (a running sandbox), taking a snapshot msb is
/// given the name `basename` for and `--dest-dir dest_dir`, via `invoke` (the
/// plain one-shot `exec`/`stop`/`snapshot create`/`rm` commands) and `reboot`
/// (the actual re-boot of a fresh sandbox from the just-created snapshot's REAL
/// ref — see below for where that ref comes from; UNDER A FRESH NAME, never
/// `name` itself — `reboot`'s own caller, `MsbCliBackend::create_checkpoint`,
/// is what sets that on the spec `reboot` closes over, before this function
/// ever runs, so this orchestration stays name-agnostic about the reboot
/// target). Both are injected rather than hardcoded — `invoke` as a pure argv-in/
/// `ExecResult`-out closure, `reboot` generic over its success type `T`
/// (production instantiates it with [`spawn_and_await_running`], returning
/// `Option<Child>` — see the module docs for how phase 3 of a restore now fills
/// that in — for this backend to hold; tests instantiate it with a bare
/// `Result<()>`) — so this orchestration logic (the ordering, and which steps
/// short-circuit which) is unit-testable without a real `msb` binary or child
/// process. `dest_dir` already exists by the time this runs —
/// [`MsbCliBackend::create_checkpoint`] creates it first.
///
/// **Guest cmdline capture, before anything else, when `attempt_cmdline_capture`
/// is `true`** (`MsbCliBackend::create_checkpoint` sets it to
/// `handle.spec().command.is_none()` — an explicit command already tells a later
/// restore everything it needs, so there's nothing to capture): execs
/// [`commands::capture_workload_cmdline`]'s small guest script via `invoke` WHILE
/// the sandbox is still running — this MUST happen before `stop` below, the only
/// point in this cycle a live guest still exists to exec into — and parses its
/// stdout with [`commands::parse_captured_cmdline`]. Best-effort in every
/// direction: the exec itself failing to run, exiting nonzero, or producing
/// nothing parseable all resolve to `None`, NEVER fail the checkpoint — the
/// worst case is a restore later finding no captured cmdline either (see
/// [`spawn_workload_exec`]'s typed error for that), not a checkpoint that used
/// to succeed suddenly failing because a diagnostic exec had a bad day. The
/// captured value (if any) is handed to `reboot` for the SAME cycle's own
/// re-boot to use immediately (see `MsbCliBackend::create_checkpoint`'s own
/// `reboot` closure) and returned alongside `reboot`'s result, since
/// `create_checkpoint` also needs it to persist onto this handle's
/// `HandleState` for [`MsbCliBackend::last_checkpoint_captured_cmdline`] to read
/// back — a LATER restore (a different process, or a named checkpoint's
/// registry entry) has no other way to learn it.
///
/// **The snapshot's real ref is parsed from `snapshot create`'s own stdout**
/// (see [`parse_snapshot_create_ref`]), never assembled as `dest_dir`/`basename`
/// — msb 0.7.1's dest-dir disk snapshot store nests the artifact under
/// `dest_dir/<name's-source-sandbox>/snap_<digest>`, a path `basename` (the name
/// msb was GIVEN) never appears in (verified live; see
/// `MsbCliBackend::create_checkpoint`'s own doc). `reboot` is handed that parsed
/// ref so it can set it as the spec's `checkpoint_ref` before actually rebooting
/// — this function has no spec of its own to set it on. On success, the parsed
/// ref is returned alongside `reboot`'s own result, since
/// [`MsbCliBackend::create_checkpoint`] needs both: the ref to hand back to its
/// own caller as the public `Checkpoint.ref`, and the live child to hold.
///
/// This replaces the former `msb stop` → `msb snapshot create` → `msb start`
/// cycle: `msb start` is `Sandbox::start_detached` in upstream microsandbox, whose
/// detached spawn requests `CREATE_BREAKAWAY_FROM_JOB` on Windows — denied with
/// `ERROR_ACCESS_DENIED` whenever msb runs inside a job object that doesn't grant
/// breakaway rights, which is exactly a Gradle/cargo test process on a Windows CI
/// runner. The denial is deterministic, not transient, so no retry fixes it.
/// Attached `msb run`/`msb restore` (this backend's normal boot) has no such
/// problem, so once the snapshot exists, the stopped sandbox is removed and this
/// backend's own create/boot path re-creates it from that snapshot instead of
/// resuming it.
///
/// Failure handling:
/// - `msb stop` failing short-circuits before any snapshot/rm/reboot attempt.
/// - `msb snapshot create` failing — whether a nonzero exit or a successful exit
///   whose stdout doesn't end in a recognizable absolute artifact path — leaves
///   the sandbox stopped (never removed) and surfaces an error naming `msb start
///   <name>` as the by-hand remedy — no best-effort restart-for-the-caller here,
///   since that restart would be exactly the broken `msb start` call this cycle
///   no longer makes.
/// - After `rm`, [`wait_for_checkpoint_name_release`] polls `msb ls` until
///   `name` is actually gone (bounded by
///   [`CHECKPOINT_NAME_RELEASE_BUDGET`]) before `reboot` is ever called — msb's
///   own `rm` can return before the sandbox's DB record clears `msb ls`, and
///   rebooting into that race would otherwise surface as msb's own "already
///   exists" refusal instead of a clear error naming the stuck sandbox. That
///   wait is only a cheap first gate, though — see
///   [`CHECKPOINT_NAME_RELEASE_BUDGET`]'s own doc for the second, independent
///   thing msb's own collision check blocks on that `msb ls` says nothing
///   about. So a `reboot` that still hits that refusal is retried by
///   [`reboot_with_already_exists_retry`] on a bounded budget —
///   [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_BUDGET`] at
///   [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_DELAY`] intervals — rather than
///   the single short-backoff retry this used to be.
/// - A failure to reboot after a successful snapshot (whether the name-release
///   wait itself timed out, or the reboot's own retry budget ran out)
///   surfaces an error naming the full checkpoint ref (parsed from `snapshot
///   create`'s stdout, per above) and `Container::from_checkpoint(...)` as the
///   recovery path — the sandbox is gone, but its state lives on in the
///   snapshot.
fn msb_checkpoint_cycle<T>(
    invoke: &mut dyn FnMut(&[String]) -> Result<ExecResult>,
    reboot: &mut RebootFn<'_, T>,
    name: &str,
    basename: &str,
    dest_dir: &Path,
    attempt_cmdline_capture: bool,
) -> Result<(String, T, Option<Vec<String>>)> {
    msb_checkpoint_cycle_inner(
        invoke,
        reboot,
        name,
        basename,
        dest_dir,
        attempt_cmdline_capture,
        CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_BUDGET,
        CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_DELAY,
    )
}

/// Test-only seam: identical to [`msb_checkpoint_cycle`], but with the reboot
/// step's "already exists" retry budget/delay overridable instead of hardcoded
/// to [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_BUDGET`]/
/// [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_DELAY`] — lets a red-proof shrink
/// the real ~30s budget down to milliseconds so exhausting it doesn't mean
/// actually waiting 30 real seconds. Production never calls this; it always
/// goes through [`msb_checkpoint_cycle`] itself, which hardcodes the real
/// constants.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn msb_checkpoint_cycle_with_reboot_retry_budget<T>(
    invoke: &mut dyn FnMut(&[String]) -> Result<ExecResult>,
    reboot: &mut RebootFn<'_, T>,
    name: &str,
    basename: &str,
    dest_dir: &Path,
    attempt_cmdline_capture: bool,
    reboot_retry_budget: Duration,
    reboot_retry_delay: Duration,
) -> Result<(String, T, Option<Vec<String>>)> {
    msb_checkpoint_cycle_inner(
        invoke,
        reboot,
        name,
        basename,
        dest_dir,
        attempt_cmdline_capture,
        reboot_retry_budget,
        reboot_retry_delay,
    )
}

/// The actual orchestration both [`msb_checkpoint_cycle`] and its test-only
/// [`msb_checkpoint_cycle_with_reboot_retry_budget`] twin delegate to — see
/// [`msb_checkpoint_cycle`]'s own doc for the full behavior; `reboot_retry_budget`/
/// `reboot_retry_delay` are just the already-exists retry's budget/delay,
/// threaded straight through to [`reboot_with_already_exists_retry`].
#[allow(clippy::too_many_arguments)]
fn msb_checkpoint_cycle_inner<T>(
    invoke: &mut dyn FnMut(&[String]) -> Result<ExecResult>,
    reboot: &mut RebootFn<'_, T>,
    name: &str,
    basename: &str,
    dest_dir: &Path,
    attempt_cmdline_capture: bool,
    reboot_retry_budget: Duration,
    reboot_retry_delay: Duration,
) -> Result<(String, T, Option<Vec<String>>)> {
    let captured_cmdline = attempt_cmdline_capture
        .then(|| invoke(&commands::capture_workload_cmdline(name)))
        .and_then(Result::ok)
        .filter(|r| r.exit_code == 0)
        .and_then(|r| commands::parse_captured_cmdline(&r.stdout));

    let stop = invoke(&commands::stop(name))?;
    if stop.exit_code != 0 {
        return Err(RightsizeError::Backend(format!(
            "msb stop {name} failed before taking a checkpoint (exit {}): {}",
            stop.exit_code,
            stop.stderr.trim()
        )));
    }

    let create = match invoke(&commands::snapshot_create_in(name, basename, dest_dir)) {
        Ok(r) if r.exit_code == 0 => r,
        Ok(r) => {
            return Err(RightsizeError::Backend(format!(
                "msb snapshot create --from-sandbox {name} {basename} --dest-dir {} failed \
                 (exit {}): {} — the sandbox is left stopped; run `msb start {name}` by hand \
                 to bring it back up",
                dest_dir.display(),
                r.exit_code,
                r.stderr.trim()
            )));
        }
        Err(e) => {
            return Err(RightsizeError::Backend(format!(
                "msb snapshot create --from-sandbox {name} {basename} --dest-dir {} failed: \
                 {e} — the sandbox is left stopped; run `msb start {name}` by hand to bring it \
                 back up",
                dest_dir.display()
            )));
        }
    };

    let checkpoint_ref = parse_snapshot_create_ref(&create.stdout).ok_or_else(|| {
        RightsizeError::Backend(format!(
            "msb snapshot create --from-sandbox {name} {basename} --dest-dir {} succeeded but \
             its stdout did not end with a recognizable absolute artifact path — the sandbox \
             is left stopped; run `msb start {name}` by hand to bring it back up\nraw stdout:\n{}",
            dest_dir.display(),
            create.stdout,
        ))
    })?;

    // Disk state now lives in the snapshot — remove the now-stale stopped
    // sandbox; the reboot below restores under a FRESH name instead (see
    // `MsbCliBackend::create_checkpoint`'s own doc for why: msb's own
    // directory-retention behavior on Windows makes a same-name restore
    // unreliable), so this `rm` is no longer what keeps the reboot from
    // colliding on `name` — it is just ordinary teardown of a sandbox this
    // cycle is done with, same as any other stop-and-remove. The wait below
    // (and the reboot's own already-exists retry) are kept as dormant defense
    // regardless — they simply have nothing left to trigger against `name`,
    // since nothing ever tries to reuse it. Best-effort: even if this `rm`
    // itself fails, a genuinely stuck `name` surfaces via the wait below's own
    // error instead.
    let _ = invoke(&commands::rm(name));

    // msb's own `rm` above can return before `name` is actually released on
    // Windows — wait that out before ever attempting the reboot, rather than
    // let it surface as the reboot's own "already exists" refusal. See
    // `wait_for_checkpoint_name_release`'s own doc.
    wait_for_checkpoint_name_release(invoke, name).map_err(|e| {
        RightsizeError::Backend(format!(
            "{e} — the sandbox's disk state is preserved in checkpoint {checkpoint_ref}, \
             restorable via Container::from_checkpoint(...)"
        ))
    })?;

    let rebooted = reboot_with_already_exists_retry(
        reboot,
        &checkpoint_ref,
        captured_cmdline.as_deref(),
        name,
        reboot_retry_budget,
        reboot_retry_delay,
    )?;

    Ok((checkpoint_ref, rebooted, captured_cmdline))
}

/// [`msb_checkpoint_cycle`]'s reboot step, with msb's own "already exists"
/// refusal retried on a bounded budget instead of surfaced immediately. The
/// [`wait_for_checkpoint_name_release`] gate that already ran before this is
/// only a cheap first pass — it proves the sandbox's DB record cleared `msb
/// ls`, never that msb's own restore-time collision check
/// (`prepare_create_target`'s `existing.is_some() || dir_exists`, see
/// [`CHECKPOINT_NAME_RELEASE_BUDGET`]'s own doc) will actually let a `restore`
/// through — the on-disk directory that second check looks at can keep a
/// stale Windows file handle open well past the DB record's own release. So
/// THIS retry, not the `msb ls` wait, is what actually guarantees a reboot
/// eventually gets a fair shot at a name that is merely slow to free, while
/// still failing clearly (never hanging) on a genuinely stuck one.
///
/// `retry_budget`/`retry_delay` are parameters rather than the bare constants
/// so the budget-exhaustion red-proof can shrink them to run in milliseconds
/// instead of the real [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_BUDGET`] —
/// production always calls this with that constant and
/// [`CHECKPOINT_REBOOT_ALREADY_EXISTS_RETRY_DELAY`] (see
/// [`msb_checkpoint_cycle`] above).
///
/// Only [`RightsizeError::NameConflict`] is retried at all — any other error
/// `reboot` returns (on the first attempt or a later one) surfaces
/// immediately, the same as it always has; retrying a non-conflict failure on
/// this budget would just delay reporting a real problem.
///
/// **This function itself still only ever retries the SAME call to `reboot`
/// — it has no notion of "candidates" at all.** `MsbCliBackend::create_checkpoint`'s
/// own `reboot` closure is what actually walks its `fresh_names` batch: each
/// retry this loop drives calls that closure again, and the closure is the
/// one that best-effort `rm`s the candidate that just collided and advances
/// to the next one before attempting the restore — see that method's own
/// doc. Once every candidate has been tried, the closure stops returning
/// [`RightsizeError::NameConflict`] and returns a plain, non-retried `Err`
/// instead, which is what ends this loop early (rather than waiting out the
/// rest of `retry_budget` retrying a name that can no longer possibly work) —
/// "exhausting candidates surfaces the last error," from the caller's own
/// contract. `retry_budget` stays the single outer wall-clock bound across
/// every candidate this walks, exactly as it always bounded every retry of a
/// single name.
fn reboot_with_already_exists_retry<T>(
    reboot: &mut RebootFn<'_, T>,
    checkpoint_ref: &str,
    captured_cmdline: Option<&[String]>,
    name: &str,
    retry_budget: Duration,
    retry_delay: Duration,
) -> Result<T> {
    let deadline = Instant::now() + retry_budget;
    let mut last_message = match reboot(checkpoint_ref, captured_cmdline) {
        Ok(rebooted) => return Ok(rebooted),
        Err(RightsizeError::NameConflict { message, .. }) => message,
        Err(e) => {
            return Err(RightsizeError::Backend(format!(
                "re-booting sandbox {name} from checkpoint {checkpoint_ref} failed ({e}) — the \
                 sandbox was removed but its state is preserved in checkpoint {checkpoint_ref}, \
                 restorable via Container::from_checkpoint(...)"
            )));
        }
    };
    loop {
        if Instant::now() >= deadline {
            return Err(RightsizeError::Backend(format!(
                "re-booting sandbox {name} from checkpoint {checkpoint_ref} kept hitting msb's \
                 \"already exists\" refusal for {}s after `msb ls` had already confirmed the \
                 name clear ({last_message}) — msb's own on-disk directory release can lag its \
                 DB record's own release on a loaded Windows host well past a short wait, but a \
                 refusal that never clears this long looks like a genuinely stuck sandbox \
                 rather than a release race; the sandbox was removed but its state is preserved \
                 in checkpoint {checkpoint_ref}, restorable via Container::from_checkpoint(...)",
                retry_budget.as_secs(),
            )));
        }
        std::thread::sleep(retry_delay);
        match reboot(checkpoint_ref, captured_cmdline) {
            Ok(rebooted) => return Ok(rebooted),
            Err(RightsizeError::NameConflict { message, .. }) => last_message = message,
            Err(e) => {
                return Err(RightsizeError::Backend(format!(
                    "re-booting sandbox {name} from checkpoint {checkpoint_ref} failed ({e}) \
                     after previously hitting msb's \"already exists\" refusal ({last_message}) \
                     — the sandbox was removed but its state is preserved in checkpoint \
                     {checkpoint_ref}, restorable via Container::from_checkpoint(...)"
                )));
            }
        }
    }
}

/// Parses the checkpoint artifact's own absolute path out of a successful `msb
/// snapshot create --from-sandbox ... --dest-dir ...` invocation's stdout —
/// [`msb_checkpoint_cycle`]'s only way to learn where msb 0.7.1 actually put the
/// artifact, since it no longer lands at `dest_dir/basename` (see that
/// function's own doc for why). Verified live: msb prints the snapshot ID line,
/// then the artifact's absolute path as the LAST stdout line.
///
/// Defensive by construction: trims surrounding whitespace, takes the LAST
/// non-empty line, and requires it to parse as an absolute path
/// (`Path::is_absolute`) — empty output, a relative-looking last line, or
/// anything else that isn't recognizably a path all return `None` rather than
/// guess, so a caller can fail loudly quoting the raw output instead of minting
/// a bogus ref.
fn parse_snapshot_create_ref(stdout: &str) -> Option<String> {
    parse_last_line_as_absolute_path(stdout)
}

/// Orchestrates the checkpoint-archive feature's export: one `msb snapshot save`
/// via `invoke_export`, and — only when that fails with Windows' access-denied
/// signature (see [`is_archive_fsync_access_denied`]) — one attempt at `salvage`,
/// which succeeding means the archive is at `dest` after all. Both are injected,
/// mirroring [`msb_import_checkpoint_cycle`]'s own shape, so this orchestration is
/// unit-testable on every platform without a real `msb` binary and without touching
/// the filesystem.
///
/// Any other nonzero exit — and an access-denied exit whose salvage finds nothing to
/// move — surfaces msb's own exit code and stderr unchanged. The workaround is
/// self-disabling: once msb fsyncs a writable handle, the error stops occurring and
/// `salvage` is never reached again, so nothing here is tied to a particular msb
/// version.
fn msb_export_checkpoint_cycle(
    invoke_export: &mut dyn FnMut() -> Result<ExecResult>,
    salvage: &mut dyn FnMut(&Path) -> bool,
    checkpoint_ref: &str,
    dest: &Path,
) -> Result<()> {
    let result = invoke_export()?;
    if result.exit_code == 0 {
        return Ok(());
    }
    if is_archive_fsync_access_denied(&result.stderr) && salvage(dest) {
        return Ok(());
    }
    Err(RightsizeError::Backend(format!(
        "msb snapshot save {checkpoint_ref} {} failed (exit {}): {}",
        dest.display(),
        result.exit_code,
        result.stderr.trim()
    )))
}

/// Orchestrates the checkpoint-archive feature's import cycle: one `msb snapshot
/// load <archive> --dest <dir>` via `invoke_import` (injected, mirroring
/// [`msb_checkpoint_cycle`]'s own shape, so this orchestration is unit-testable
/// without a real `msb` binary), treating msb's "already exists" wording as
/// success (see [`is_snapshot_already_exists`]) the same as the pre-0.7.1 `import`
/// verb did. Returns the loaded artifact's own absolute path as
/// [`MsbCliBackend::import_checkpoint`]'s effective ref.
///
/// **No `snapshot list` round trip any more.** Earlier msb releases' `import`
/// unpacked under an opaque digest-derived directory name that had to be
/// separately confirmed against `msb snapshot list --format json`'s output; msb
/// 0.7.1's `load` prints the artifact's own full path directly (the same
/// "parse the printed path" contract [`msb_checkpoint_cycle`]'s `snapshot create`
/// step already uses) for a fresh, successful load, so nothing needs resolving
/// via `list` in that case.
///
/// **Ref resolution, and the one thing that is NOT verified.** A fresh,
/// successful `load` (exit 0) printing the group/digest/path lines to stdout,
/// last line the artifact's own absolute path, is empirically verified against a
/// real msb 0.7.1 binary — see [`parse_snapshot_load_ref`]. What `load` prints
/// on an "already exists" outcome (nonzero exit, tolerated as success) is NOT
/// verified: the pre-0.7.1 `import` verb it replaces put that path only in the
/// `error: snapshot already exists: <path>` stderr line, with stdout empty, so
/// this still tries `parse_snapshot_load_ref` on stdout first (covering the
/// possibility that `load` prints its usual group/digest/path lines up front
/// even when it then exits nonzero) and, only if that finds nothing, falls back
/// to pulling the path out of the "already exists" stderr line the same way the
/// pre-0.7.1 code did (see [`parse_already_exists_stderr_ref`]). Both shapes are
/// covered by tests; a real msb 0.7.1 binary should confirm which one it
/// actually uses before this ships.
///
/// Failure handling: an import failure that ISN'T "already exists" surfaces with
/// its stderr, never touching stdout. A successful (or already-exists) import
/// for which NEITHER stdout NOR (on "already exists") the stderr line yields a
/// parseable absolute path surfaces its own actionable error quoting the raw
/// output, rather than trusting garbage.
fn msb_import_checkpoint_cycle(
    invoke_import: &mut dyn FnMut() -> Result<ExecResult>,
) -> Result<String> {
    let import_result = invoke_import()?;
    let already_exists = is_snapshot_already_exists(&format!(
        "{}\n{}",
        import_result.stdout, import_result.stderr
    ));
    if import_result.exit_code != 0 && !already_exists {
        return Err(RightsizeError::Backend(format!(
            "msb snapshot load failed (exit {}): {}",
            import_result.exit_code,
            import_result.stderr.trim()
        )));
    }

    if let Some(ref_path) = parse_snapshot_load_ref(&import_result.stdout) {
        return Ok(ref_path);
    }
    if already_exists {
        if let Some(ref_path) = parse_already_exists_stderr_ref(&import_result.stderr) {
            return Ok(ref_path);
        }
    }

    Err(RightsizeError::Backend(format!(
        "msb snapshot load succeeded but its stdout did not end with a recognizable absolute \
         artifact path, and no fallback path was found in stderr either\nraw stdout:\n{}\nraw \
         stderr:\n{}",
        import_result.stdout, import_result.stderr,
    )))
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

    /// The nested artifact path a fake `msb snapshot create --from-sandbox ...
    /// --dest-dir <dest_dir>` prints as its LAST stdout line, matching msb
    /// 0.7.1's real, live-verified shape: `<dest_dir>/<source-sandbox>/
    /// snap_<digest>`, never `<dest_dir>/<name-it-was-given>`. `dest_dir` must be
    /// absolute on every platform this crate targets — [`parse_snapshot_create_ref`]
    /// requires it — so callers build it from `std::env::temp_dir()` rather than a
    /// hand-typed Unix literal such as `/cache/checkpoints`, which
    /// `Path::is_absolute()` rejects on Windows (no drive/prefix component).
    fn fake_snapshot_create_stdout(dest_dir: &Path, source_sandbox: &str) -> String {
        format!(
            "Snapshot ID: deadbeefcafedeadbeefcafedeadbeef\n{}\n",
            dest_dir
                .join(source_sandbox)
                .join("snap_deadbeefcafedeadbeefcafedeadbeef")
                .display()
        )
    }

    #[test]
    fn msb_checkpoint_cycle_happy_path_drives_stop_snapshot_rm_then_reboot_in_order() {
        let log: RefCell<Vec<String>> = RefCell::new(Vec::new());
        let dest_dir_buf = std::env::temp_dir().join("rz-msb-checkpoint-cycle-happy-path");
        let dest_dir = dest_dir_buf.as_path();
        let expected_ref = dest_dir
            .join("rz-abc-1")
            .join("snap_deadbeefcafedeadbeefcafedeadbeef")
            .display()
            .to_string();
        let result = {
            let mut invoke = |args: &[String]| {
                log.borrow_mut().push(args.join(" "));
                // The post-rm name-release wait needs a genuinely parseable `msb
                // ls` reply to confirm the name absent — see
                // `wait_for_checkpoint_name_release`'s own doc for why an
                // unparseable stdout (which this fake's blanket snapshot-create
                // reply below is not) can no longer be read as a release.
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |real_ref: &str, _captured: Option<&[String]>| -> Result<()> {
                log.borrow_mut().push(format!("reboot {real_ref}"));
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
            )
        };
        let (returned_ref, (), captured_cmdline) = result.expect("happy path must succeed");
        assert_eq!(
            captured_cmdline, None,
            "capture was never attempted (attempt_cmdline_capture = false)"
        );
        assert_eq!(returned_ref, expected_ref);
        assert_eq!(
            *log.borrow(),
            vec![
                commands::stop("rz-abc-1").join(" "),
                commands::snapshot_create_in("rz-abc-1", "rz-ckpt-deadbeefcafe", dest_dir)
                    .join(" "),
                commands::rm("rz-abc-1").join(" "),
                // The post-rm name-release poll: this fake's `ls` reply is a
                // genuinely parseable, empty listing, confirmed absent on the
                // very first poll — one poll, no retry, matching unix's real
                // behavior.
                commands::ls().join(" "),
                format!("reboot {expected_ref}"),
            ]
        );
    }

    #[test]
    fn msb_checkpoint_cycle_captures_the_guest_cmdline_before_stop_and_hands_it_to_reboot() {
        // The capture must happen WHILE the guest is still running — before
        // `stop`, not after — and the parsed argv must reach `reboot` for this
        // SAME cycle's own re-boot to use immediately.
        let log: RefCell<Vec<String>> = RefCell::new(Vec::new());
        let dest_dir_buf = std::env::temp_dir().join("rz-msb-checkpoint-cycle-captures-cmdline");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                log.borrow_mut().push(args.join(" "));
                if args == commands::capture_workload_cmdline("rz-abc-1") {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "redis-server\0--port\x006379\0\n".to_string(),
                        stderr: String::new(),
                    });
                }
                // See the happy-path test's identical branch for why the
                // post-rm name-release poll needs a genuinely parseable reply.
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |real_ref: &str, captured: Option<&[String]>| -> Result<()> {
                log.borrow_mut().push(format!(
                    "reboot {real_ref} captured={:?}",
                    captured.map(<[String]>::to_vec)
                ));
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                true,
            )
        };
        let (_ref, (), captured_cmdline) = result.expect("happy path must succeed");
        assert_eq!(
            captured_cmdline,
            Some(vec![
                "redis-server".to_string(),
                "--port".to_string(),
                "6379".to_string()
            ])
        );
        let log = log.borrow();
        assert_eq!(
            log[0],
            commands::capture_workload_cmdline("rz-abc-1").join(" "),
            "the capture exec must be the very first invocation — before `stop`: {log:?}"
        );
        assert_eq!(log[1], commands::stop("rz-abc-1").join(" "));
        let last = log.last().unwrap();
        assert!(last.starts_with("reboot "), "{last}");
        assert!(
            last.contains(
                &dest_dir
                    .join("rz-abc-1")
                    .join("snap_deadbeefcafedeadbeefcafedeadbeef")
                    .display()
                    .to_string()
            ),
            "reboot must receive the parsed artifact ref: {last}"
        );
        assert!(
            last.contains(r#"captured=Some(["redis-server", "--port", "6379"])"#),
            "reboot must receive the captured argv: {last}"
        );
    }

    #[test]
    fn msb_checkpoint_cycle_skips_the_capture_exec_entirely_when_attempt_is_false() {
        let calls: RefCell<Vec<Vec<String>>> = RefCell::new(Vec::new());
        let dest_dir_buf = std::env::temp_dir().join("rz-msb-checkpoint-cycle-no-capture");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                calls.borrow_mut().push(args.to_vec());
                // See the happy-path test's identical branch for why the post-rm
                // name-release poll needs a genuinely parseable reply.
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> { Ok(()) };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
            )
        };
        let (_ref, (), captured) = result.expect("happy path must succeed");
        assert_eq!(captured, None);
        assert!(
            !calls
                .borrow()
                .iter()
                .any(|c| c.first().map(String::as_str) == Some("exec")),
            "no exec call must be made when attempt_cmdline_capture is false: {:?}",
            calls.borrow()
        );
    }

    #[test]
    fn msb_checkpoint_cycle_a_failed_capture_exec_does_not_fail_the_checkpoint() {
        // Best-effort in every direction: the capture exec itself erroring must
        // never fail a checkpoint that would otherwise have succeeded — the
        // worst case is simply no captured cmdline for a later restore to find.
        let dest_dir_buf = std::env::temp_dir().join("rz-msb-checkpoint-cycle-capture-fails");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                if args == commands::capture_workload_cmdline("rz-abc-1") {
                    return Err(RightsizeError::Backend("exec unavailable".to_string()));
                }
                // See the happy-path test's identical branch for why the post-rm
                // name-release poll needs a genuinely parseable reply.
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> { Ok(()) };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                true,
            )
        };
        let (_ref, (), captured) = result.expect("a failed capture must not fail the checkpoint");
        assert_eq!(captured, None);
    }

    #[test]
    fn msb_checkpoint_cycle_a_capture_exiting_nonzero_or_unparseable_yields_no_captured_cmdline() {
        let dest_dir_buf = std::env::temp_dir().join("rz-msb-checkpoint-cycle-capture-nonzero");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                if args == commands::capture_workload_cmdline("rz-abc-1") {
                    return Ok(ExecResult {
                        exit_code: 1,
                        stdout: String::new(),
                        stderr: "no such process".to_string(),
                    });
                }
                // See the happy-path test's identical branch for why the post-rm
                // name-release poll needs a genuinely parseable reply.
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> { Ok(()) };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                true,
            )
        };
        let (_ref, (), captured) =
            result.expect("a nonzero capture exit must not fail the checkpoint");
        assert_eq!(captured, None);
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
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                Path::new("/cache/checkpoints"),
                false,
            )
        };
        let err = result.expect_err("a failed snapshot step must propagate as an error");
        let msg = err.to_string();
        assert!(msg.contains("disk full"), "{msg}");
        assert!(msg.contains("left stopped"), "{msg}");
        assert!(msg.contains("msb start rz-abc-1"), "{msg}");
        assert!(msg.contains("--from-sandbox"), "{msg}");
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
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                Path::new("/cache/checkpoints"),
                false,
            )
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
    fn msb_checkpoint_cycle_a_successful_snapshot_create_with_unparseable_stdout_leaves_the_sandbox_stopped_and_skips_rm_and_reboot()
     {
        let mut calls: Vec<Vec<String>> = Vec::new();
        let mut reboot_called = false;
        let result = {
            let mut invoke = |args: &[String]| {
                calls.push(args.to_vec());
                Ok(ExecResult {
                    exit_code: 0,
                    // No absolute path anywhere in stdout — msb printing
                    // something this backend cannot parse a ref out of must
                    // fail loudly rather than mint a bogus one.
                    stdout: "ok\n".to_string(),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                Path::new("/cache/checkpoints"),
                false,
            )
        };
        let err = result.expect_err("unparseable snapshot-create stdout must not mint a bogus ref");
        let msg = err.to_string();
        assert!(msg.contains("left stopped"), "{msg}");
        assert!(msg.contains("msb start rz-abc-1"), "{msg}");
        assert!(msg.contains("ok"), "the raw stdout must be quoted: {msg}");
        assert_eq!(
            calls.len(),
            2,
            "unparseable output must not be followed by rm: {calls:?}"
        );
        assert!(!reboot_called);
    }

    #[test]
    fn msb_checkpoint_cycle_reboot_failure_after_a_successful_snapshot_names_the_ref_and_the_recovery_path()
     {
        let mut calls: Vec<Vec<String>> = Vec::new();
        let dest_dir_buf = std::env::temp_dir().join("rz-msb-checkpoint-cycle-reboot-failure");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                calls.push(args.to_vec());
                // See the happy-path test's identical branch for why the post-rm
                // name-release poll needs a genuinely parseable reply.
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                Err(RightsizeError::Backend("boom".to_string()))
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
            )
        };
        let err = result.expect_err("a failed reboot must surface, not be swallowed");
        let msg = err.to_string();
        assert!(msg.contains("boom"), "{msg}");
        assert!(
            msg.contains(
                &dest_dir
                    .join("rz-abc-1")
                    .join("snap_deadbeefcafedeadbeefcafedeadbeef")
                    .display()
                    .to_string()
            ),
            "the parsed artifact ref (not the dest_dir/basename guess) must be named: {msg}"
        );
        assert!(msg.contains("Container::from_checkpoint"), "{msg}");
        assert_eq!(
            calls.len(),
            4,
            "rm and the post-rm name-release poll must still run before a reboot failure \
             surfaces: {calls:?}"
        );
        assert_eq!(calls[2], commands::rm("rz-abc-1"));
        assert_eq!(calls[3], commands::ls());
    }

    #[test]
    fn msb_checkpoint_cycle_waits_out_a_transient_post_rm_listing_before_rebooting() {
        // Red-proof (a): msb's own `rm` can return before the name is actually
        // released on Windows — `msb ls` keeps listing it for a couple of polls
        // before it clears. The cycle must wait that out and still reboot, not
        // fail or reboot into a doomed "already exists" restore.
        let ls_calls = RefCell::new(0u32);
        let mut reboot_called = false;
        let dest_dir_buf = std::env::temp_dir().join("rz-msb-checkpoint-cycle-name-release-wait");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                if args == commands::ls() {
                    let mut n = ls_calls.borrow_mut();
                    *n += 1;
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: if *n <= 2 {
                            "[{\"name\":\"rz-abc-1\",\"status\":\"Stopped\"}]".to_string()
                        } else {
                            "[]".to_string()
                        },
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
            )
        };
        result.expect("a transient post-rm listing must not fail the checkpoint once it clears");
        assert!(
            reboot_called,
            "the reboot must still run once the name frees"
        );
        assert_eq!(
            *ls_calls.borrow(),
            3,
            "must poll `msb ls` exactly until the name first reads absent, no more"
        );
    }

    #[test]
    fn msb_checkpoint_cycle_a_failed_or_garbled_post_rm_ls_read_is_not_treated_as_released() {
        // Red-proof for the critical review finding: a poll that comes back as a
        // nonzero exit (a truncated/garbled `msb ls` read — the exact failure
        // mode observed on Windows in the same asynchronous-teardown window this
        // wait exists for) must NOT be read as "the name is free" just because
        // its unparseable stdout would make `ls_json::status_of` return `None`
        // (the ambiguous value `ls_json::try_is_listed` exists specifically to
        // avoid folding into "absent" here). The wait must keep polling past
        // that bad read and only succeed once a genuinely clean (`exit_code ==
        // 0`, parseable) read confirms the name is gone.
        let ls_calls = RefCell::new(0u32);
        let mut reboot_called = false;
        let dest_dir_buf =
            std::env::temp_dir().join("rz-msb-checkpoint-cycle-garbled-ls-not-released");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                if args == commands::ls() {
                    let mut n = ls_calls.borrow_mut();
                    *n += 1;
                    return Ok(match *n {
                        // First poll: msb ls itself fails (nonzero exit) with
                        // garbled/truncated stdout, while the sandbox is still
                        // fully present. This must read as inconclusive, not as
                        // proof of release.
                        1 => ExecResult {
                            exit_code: 1,
                            stdout: "{\"name\":\"rz-a".to_string(),
                            stderr: "unexpected EOF".to_string(),
                        },
                        // Second poll: ls succeeds again, but honestly reports
                        // the sandbox still listed.
                        2 => ExecResult {
                            exit_code: 0,
                            stdout: "[{\"name\":\"rz-abc-1\",\"status\":\"Stopped\"}]".to_string(),
                            stderr: String::new(),
                        },
                        // Third poll: a genuinely clean, successful read
                        // confirming the name is actually gone.
                        _ => ExecResult {
                            exit_code: 0,
                            stdout: "[]".to_string(),
                            stderr: String::new(),
                        },
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
            )
        };
        result
            .expect("a failed/garbled ls read must not short-circuit the wait to a false release");
        assert!(
            reboot_called,
            "the reboot must still run once a clean read confirms release"
        );
        assert_eq!(
            *ls_calls.borrow(),
            3,
            "the failed read must cost one more poll, not be read as release on poll one"
        );
    }

    #[test]
    fn msb_checkpoint_cycle_an_exit_0_but_unparseable_ls_read_is_not_treated_as_released() {
        // A narrower variant of the failed-read red-proof above: a `msb ls` that
        // exits 0 but prints something that still isn't the documented JSON
        // array shape (e.g. a stray log line interleaved with the real output
        // during the same daemon-hiccup window) must ALSO stay inconclusive — a
        // check that only gates on `exit_code != 0` (rather than on the parse
        // itself) would wrongly treat this as a confirmed release.
        let ls_calls = RefCell::new(0u32);
        let mut reboot_called = false;
        let dest_dir_buf =
            std::env::temp_dir().join("rz-msb-checkpoint-cycle-exit-0-unparseable-ls-not-released");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                if args == commands::ls() {
                    let mut n = ls_calls.borrow_mut();
                    *n += 1;
                    return Ok(match *n {
                        // First poll: exit 0, but stdout is not the documented
                        // array shape at all — must not read as "absent".
                        1 => ExecResult {
                            exit_code: 0,
                            stdout: "msb: warming up cache...\n".to_string(),
                            stderr: String::new(),
                        },
                        // Second poll: a genuinely clean, successful read
                        // confirming the name is actually gone.
                        _ => ExecResult {
                            exit_code: 0,
                            stdout: "[]".to_string(),
                            stderr: String::new(),
                        },
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
            )
        };
        result.expect(
            "an exit-0-but-unparseable ls read must not short-circuit the wait to a false release",
        );
        assert!(
            reboot_called,
            "the reboot must still run once a clean read confirms release"
        );
        assert_eq!(
            *ls_calls.borrow(),
            2,
            "the unparseable read must cost one more poll, not be read as release on poll one"
        );
    }

    #[test]
    fn msb_checkpoint_cycle_a_name_that_never_frees_fails_clearly_without_rebooting() {
        // Red-proof (b): if `msb ls` keeps listing the name past the whole
        // release-wait budget, the cycle must fail with a clear, typed message
        // naming the stuck sandbox — never loop forever, and never let the
        // reboot even attempt a restore that is doomed to hit "already exists".
        let mut reboot_called = false;
        let dest_dir_buf = std::env::temp_dir().join("rz-msb-checkpoint-cycle-name-never-frees");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[{\"name\":\"rz-abc-1\",\"status\":\"Stopped\"}]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                reboot_called = true;
                Ok(())
            };
            msb_checkpoint_cycle(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
            )
        };
        let err =
            result.expect_err("a name that never frees must fail, not hang or silently reboot");
        let msg = err.to_string();
        assert!(
            msg.contains("rz-abc-1"),
            "the stuck sandbox must be named: {msg}"
        );
        assert!(
            msg.contains("Container::from_checkpoint"),
            "the snapshot's own recovery path must still be named, since it already succeeded: \
             {msg}"
        );
        assert!(
            !reboot_called,
            "the reboot must never be attempted once the release wait itself has failed"
        );
    }

    #[test]
    fn msb_checkpoint_cycle_retries_the_reboot_on_an_already_exists_refusal_past_the_old_one_shot_budget_then_succeeds()
     {
        // Red-proof (a): defense in depth — even after the release wait passes,
        // msb's own teardown (specifically the on-disk sandbox directory, which
        // `msb ls` says nothing about — see `CHECKPOINT_NAME_RELEASE_BUDGET`'s
        // own doc) can still be finishing well after the retried `restore`
        // starts. Five already-exists refusals in a row — a count the OLD
        // one-shot retry (budget for exactly one extra attempt) could never
        // survive — must still let the checkpoint succeed once the sixth
        // attempt clears, proving this is a real bounded RETRY BUDGET now, not
        // a single retry. The budget/delay are shrunk to run in milliseconds
        // instead of the real ~30s/2s shape.
        let reboot_calls = RefCell::new(0u32);
        const ALREADY_EXISTS_REFUSALS: u32 = 5;
        let dest_dir_buf =
            std::env::temp_dir().join("rz-msb-checkpoint-cycle-reboot-already-exists");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                // See the happy-path test's identical branch for why the post-rm
                // name-release poll needs a genuinely parseable reply.
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                let mut n = reboot_calls.borrow_mut();
                *n += 1;
                if *n <= ALREADY_EXISTS_REFUSALS {
                    return Err(RightsizeError::NameConflict {
                        message: "sandbox 'rz-abc-1' already exists".to_string(),
                        source: None,
                    });
                }
                Ok(())
            };
            msb_checkpoint_cycle_with_reboot_retry_budget(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
                Duration::from_millis(200),
                Duration::from_millis(1),
            )
        };
        result.expect(
            "the reboot budget must survive more already-exists refusals than the old one-shot \
             retry ever could, and still let the checkpoint succeed once they stop",
        );
        assert_eq!(
            *reboot_calls.borrow(),
            ALREADY_EXISTS_REFUSALS + 1,
            "exactly one reboot attempt per refusal, plus the one that finally succeeds"
        );
    }

    #[test]
    fn msb_checkpoint_cycle_an_already_exists_refusal_that_never_clears_fails_clearly_once_the_budget_runs_out()
     {
        // Red-proof (b): a PERSISTENT "already exists" refusal (not just a
        // transient release race) must still surface as a real, actionable
        // error once the retry budget is exhausted — never retry forever, and
        // never silently succeed. The budget/delay are overridden to a few
        // milliseconds so exhausting them doesn't mean actually waiting out the
        // real ~30s budget.
        let reboot_calls = RefCell::new(0u32);
        let budget = Duration::from_millis(200);
        let delay = Duration::from_millis(1);
        let dest_dir_buf =
            std::env::temp_dir().join("rz-msb-checkpoint-cycle-reboot-already-exists-forever");
        let dest_dir = dest_dir_buf.as_path();
        let result = {
            let mut invoke = |args: &[String]| {
                // See the happy-path test's identical branch for why the post-rm
                // name-release poll needs a genuinely parseable reply.
                if args == commands::ls() {
                    return Ok(ExecResult {
                        exit_code: 0,
                        stdout: "[]".to_string(),
                        stderr: String::new(),
                    });
                }
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: fake_snapshot_create_stdout(dest_dir, "rz-abc-1"),
                    stderr: String::new(),
                })
            };
            let mut reboot = |_: &str, _: Option<&[String]>| -> Result<()> {
                *reboot_calls.borrow_mut() += 1;
                Err(RightsizeError::NameConflict {
                    message: "sandbox 'rz-abc-1' already exists".to_string(),
                    source: None,
                })
            };
            msb_checkpoint_cycle_with_reboot_retry_budget(
                &mut invoke,
                &mut reboot,
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                dest_dir,
                false,
                budget,
                delay,
            )
        };
        let err = result.expect_err(
            "an already-exists refusal that never clears must not retry forever, nor succeed",
        );
        let msg = err.to_string();
        assert!(msg.contains("already exists"), "{msg}");
        assert!(msg.contains("Container::from_checkpoint"), "{msg}");
        assert!(
            *reboot_calls.borrow() > 2,
            "the budget must allow strictly more attempts than the old one-shot retry's fixed \
             two, even at this shrunk size: got {}",
            *reboot_calls.borrow()
        );
    }

    // ---- checkpoint dest-dir ref shape: minting, path-vs-bare-name, basename ----

    #[test]
    fn mint_checkpoint_ref_is_an_absolute_path_under_cache_dir_checkpoints() {
        // `std::env::temp_dir()` is absolute on every platform this crate targets
        // (Unix and Windows alike), unlike a hand-typed Unix literal such as
        // "/home/u/.cache/rightsize" — `Path::is_absolute()` is false for a bare
        // leading-slash path on Windows, which needs a drive/prefix component.
        let cache_dir = std::env::temp_dir().join("rz-msb-mint-checkpoint-ref-cache");
        let ref_path = mint_checkpoint_ref("deadbeefcafe", &cache_dir);
        assert_eq!(
            ref_path,
            cache_dir.join("checkpoints").join("rz-ckpt-deadbeefcafe")
        );
        assert!(ref_path.is_absolute());
    }

    #[test]
    fn mint_checkpoint_ref_carries_a_caller_chosen_name_through_unchanged() {
        // `checkpoint_named` passes its caller-chosen name, not a random nonce —
        // this must land in the ref exactly like a nonce would.
        let ref_path = mint_checkpoint_ref("seeded-db", Path::new("/cache"));
        assert_eq!(ref_path, Path::new("/cache/checkpoints/rz-ckpt-seeded-db"));
    }

    #[test]
    fn path_ref_dir_recognizes_an_absolute_path_and_rejects_a_bare_name() {
        // Built from `std::env::temp_dir()` rather than a hand-typed Unix literal
        // like "/cache/checkpoints/rz-ckpt-deadbeefcafe" — `Path::is_absolute()`
        // is false for a bare leading-slash path on Windows, so a literal like
        // that would make this test's own assertion fail there.
        let abs = std::env::temp_dir()
            .join("checkpoints")
            .join("rz-ckpt-deadbeefcafe");
        assert_eq!(path_ref_dir(&abs.display().to_string()), Some(abs.clone()));
        assert_eq!(
            path_ref_dir("rz-ckpt-deadbeefcafe"),
            None,
            "a bare name minted before dest-dir checkpoints must not be mistaken for a path ref"
        );
    }

    #[test]
    fn path_ref_artifact_exists_requires_both_the_directory_and_snapshot_json() {
        let dir = unique_test_dir("artifact-exists");
        let artifact_dir = dir.join("rz-ckpt-deadbeefcafe");

        assert!(
            !path_ref_artifact_exists(&artifact_dir),
            "a directory that was never created is not an artifact"
        );

        std::fs::create_dir_all(&artifact_dir).unwrap();
        assert!(
            !path_ref_artifact_exists(&artifact_dir),
            "a directory with no snapshot.json is not a complete artifact"
        );

        std::fs::write(artifact_dir.join("snapshot.json"), b"{}").unwrap();
        assert!(path_ref_artifact_exists(&artifact_dir));

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- checkpoint dest-dir mechanics: create_checkpoint/has_checkpoint/
    // remove_checkpoint against a fake `msb`, or against no `msb` at all when the
    // path-ref branch never needs one ----

    #[tokio::test]
    async fn create_checkpoint_on_a_tmpfs_root_container_refuses_before_touching_msb() {
        let backend = MsbCliBackend::new(PathBuf::from("/definitely/not/a/real/msb"));
        let handle = Handle {
            spec: ContainerSpec {
                tmpfs_root_mb: Some(256),
                ..ContainerSpec::new("rz-abc-1", "alpine:3.19", "abc")
            },
        };

        // `.err().expect(...)`, not `.expect_err(...)`: the `Ok` side now
        // carries a `Box<dyn SandboxHandle>`, which has no `Debug` impl (a
        // trait object bound `expect_err` requires on the whole `Result` but
        // `Option::expect` does not).
        let err = backend
            .create_checkpoint(&handle, "deadbeefcafe", &["rz-abc-2".to_string()])
            .await
            .err()
            .expect("a tmpfs-root container must never reach msb");
        assert!(
            matches!(err, RightsizeError::TmpfsRootCheckpoint),
            "{err:?}"
        );
    }

    /// A fake `msb` covering the FULL checkpoint cycle end to end: `stop`/`rm`
    /// against whatever name they're given (this cycle's own `old_name`, never
    /// asserted on by the script itself — the TEST reads back `stop-rm-calls`
    /// to check that), `snapshot create` (prints a fake absolute artifact path,
    /// matching [`fake_snapshot_create_stdout`]'s shape), `ls --format json`
    /// (reports the name absent on its first call — confirming the post-`rm`
    /// release wait — then whichever candidate actually won as `Running` from
    /// then on), `restore` (records every invocation's full argv to
    /// `restore-argv`, one line per call — the red-proof's own evidence of
    /// which CANDIDATE each attempt actually targeted, proving the backend
    /// advances to a new one rather than retrying the one that just refused —
    /// and refuses with msb's own "already exists" wording
    /// `already_exists_refusals` times, REGARDLESS of which candidate name is
    /// given, before succeeding on whichever one it is handed next), and
    /// `exec` (the phase-3 workload-revival child: hangs until a `stop`/`rm`
    /// call named EXACTLY the WINNING candidate touches the sentinel it polls
    /// for — gated on the name, unlike [`write_fake_msb_for_restore`]'s
    /// version, so the cycle's OWN mid-cycle `rm <old_name>` (and any
    /// best-effort `rm` of a FAILED candidate — see
    /// `MsbCliBackend::create_checkpoint`'s own `reboot` closure) can't
    /// prematurely release it before the winning `restore` has even run).
    ///
    /// **Deliberately does not take a `fresh_name` parameter at all.** The
    /// candidate batch's actual names are minted by the CALLER now (a real
    /// `rightsize::ContainerGuard::checkpoint_core`'s `next_container_name`
    /// generator in production, or a test's own hand-picked `Vec<String>`
    /// here), never by this script — so `ls`/`stop`/`rm`'s own idea of "the
    /// live sandbox" is read back from `<dir>/winning-name`, a file this
    /// script itself writes the moment a `restore` call actually succeeds
    /// (the first one NOT refused), rather than baked in at script-generation
    /// time.
    #[cfg(unix)]
    fn write_fake_msb_for_checkpoint_reboot(dir: &Path, already_exists_refusals: u32) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-msb-checkpoint-reboot.sh");
        let snapshot_path = dir
            .join("checkpoints")
            .join("source-sandbox")
            .join("snap_fake")
            .display()
            .to_string();
        let body = format!(
            "#!/bin/sh\n\
             dir=\"$(dirname \"$0\")\"\n\
             case \"$1\" in\n\
             snapshot)\n\
             echo 'Snapshot ID: deadbeefcafedeadbeefcafedeadbeef'\n\
             echo '{snapshot_path}'\n\
             exit 0\n\
             ;;\n\
             restore)\n\
             echo \"$*\" >> \"$dir/restore-argv\"\n\
             n=$(cat \"$dir/restore-calls\" 2>/dev/null || echo 0)\n\
             echo $((n + 1)) > \"$dir/restore-calls\"\n\
             if [ \"$n\" -lt {already_exists_refusals} ]; then\n\
             echo 'error: sandbox already exists' 1>&2\n\
             exit 1\n\
             fi\n\
             echo \"$4\" > \"$dir/winning-name\"\n\
             exit 0\n\
             ;;\n\
             ls)\n\
             n=$(cat \"$dir/ls-calls\" 2>/dev/null || echo 0)\n\
             echo $((n + 1)) > \"$dir/ls-calls\"\n\
             if [ \"$n\" -eq 0 ]; then\n\
             echo '[]'\n\
             else\n\
             winner=$(cat \"$dir/winning-name\" 2>/dev/null || echo '')\n\
             echo \"[{{\\\"name\\\":\\\"$winner\\\",\\\"status\\\":\\\"Running\\\"}}]\"\n\
             fi\n\
             exit 0\n\
             ;;\n\
             exec)\n\
             shift\n\
             echo \"$*\" >> \"$dir/exec-calls\"\n\
             while [ ! -f \"$dir/stop-requested\" ]; do sleep 0.05; done\n\
             exit 0\n\
             ;;\n\
             stop|rm)\n\
             echo \"$1 $2\" >> \"$dir/stop-rm-calls\"\n\
             winner=$(cat \"$dir/winning-name\" 2>/dev/null || echo '')\n\
             if [ -n \"$winner\" ] && [ \"$2\" = \"$winner\" ]; then touch \"$dir/stop-requested\"; fi\n\
             exit 0\n\
             ;;\n\
             esac\n\
             exit 0\n"
        );
        std::fs::write(&script, body).expect("write fake msb checkpoint-reboot script");
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod fake msb checkpoint-reboot script");
        script
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_checkpoint_reboots_under_a_fresh_name_never_the_original() {
        // Red-proof (a): the restore argv names the (first, only-needed here)
        // candidate, never `old_name` — read straight back off the fake
        // `msb`'s own recorded invocation, not inferred from the returned
        // handle alone.
        let dir = unique_test_dir("checkpoint-reboot-fresh-name");
        let old_name = "rz-checkpoint-reboot-old";
        let candidates = vec!["rz-checkpoint-reboot-fresh".to_string()];
        let script = write_fake_msb_for_checkpoint_reboot(&dir, 0);
        let backend = MsbCliBackend::new(script);
        let spec = ContainerSpec {
            command: Some(vec!["redis-server".to_string()]),
            ..ContainerSpec::new(old_name, "unused-image", "run-1")
        };
        let handle = backend.create(spec).await.expect("create must succeed");
        let checkpoint_hint = dir
            .join("checkpoints")
            .join("rz-ckpt-test")
            .display()
            .to_string();

        let (checkpoint_ref, new_handle) = backend
            .create_checkpoint(handle.as_ref(), &checkpoint_hint, &candidates)
            .await
            .expect("checkpoint must succeed");
        assert!(checkpoint_ref.ends_with("snap_fake"), "{checkpoint_ref}");
        assert_eq!(
            new_handle.id(),
            candidates[0],
            "the handle create_checkpoint returns must carry the FRESH identity"
        );

        let restore_argv = std::fs::read_to_string(dir.join("restore-argv")).unwrap();
        assert_eq!(
            restore_argv.lines().count(),
            1,
            "exactly one restore attempt: {restore_argv}"
        );
        assert!(
            restore_argv.contains(&format!("--name {}", candidates[0])),
            "the restore argv must target the fresh name: {restore_argv}"
        );
        assert!(
            !restore_argv.contains(old_name),
            "the restore argv must never mention the original (now-removed) \
             name: {restore_argv}"
        );

        let stop_rm_calls = std::fs::read_to_string(dir.join("stop-rm-calls")).unwrap();
        assert!(
            stop_rm_calls.contains(&format!("stop {old_name}")),
            "the cycle's own stop step must still target the ORIGINAL name: {stop_rm_calls}"
        );
        assert!(
            stop_rm_calls.contains(&format!("rm {old_name}")),
            "the cycle's own rm step must still target the ORIGINAL name, going \
             through the normal rm path exactly as before: {stop_rm_calls}"
        );

        // Red-proof (a), continued: a SUBSEQUENT stop() — the caller adopting
        // `new_handle`, exactly as `rightsize::ContainerGuard::checkpoint_core`
        // does — targets the fresh name, not the one the cycle started under.
        backend
            .stop(new_handle.as_ref())
            .await
            .expect("stop on the adopted handle must succeed");
        let stop_rm_calls = std::fs::read_to_string(dir.join("stop-rm-calls")).unwrap();
        assert!(
            stop_rm_calls.contains(&format!("stop {}", candidates[0])),
            "a stop() issued against the returned handle must target the FRESH \
             name: {stop_rm_calls}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_checkpoint_rekeys_started_names_so_close_covers_the_fresh_sandbox() {
        // Regression proof: `started_names` is exactly what `close()` sweeps on
        // this run's own-process shutdown (see `start()`'s own comment). A
        // checkpoint reboot must re-key it from the original name to the
        // WINNING candidate the same way it already re-keys `self.handles`, or
        // `close()` after a checkpoint wastes a stop/rm on the already-removed
        // original name and never touches the sandbox that is actually live.
        let dir = unique_test_dir("checkpoint-started-names-rekey");
        let old_name = "rz-checkpoint-started-names-old";
        let candidates = vec!["rz-checkpoint-started-names-fresh".to_string()];
        let script = write_fake_msb_for_checkpoint_reboot(&dir, 0);
        let backend = MsbCliBackend::new(script);
        let spec = ContainerSpec {
            command: Some(vec!["redis-server".to_string()]),
            ..ContainerSpec::new(old_name, "unused-image", "run-1")
        };
        let handle = backend.create(spec).await.expect("create must succeed");

        // Mirrors what an ordinary (non-keep_alive) `start()` inserts, without
        // going through the full `spawn_and_await_running` dance the fake
        // script above isn't set up to answer for a plain `run`.
        backend
            .started_names
            .lock()
            .unwrap()
            .insert(old_name.to_string());

        let checkpoint_hint = dir
            .join("checkpoints")
            .join("rz-ckpt-test")
            .display()
            .to_string();
        let (_checkpoint_ref, new_handle) = backend
            .create_checkpoint(handle.as_ref(), &checkpoint_hint, &candidates)
            .await
            .expect("checkpoint must succeed");
        assert_eq!(new_handle.id(), candidates[0]);

        {
            let started = backend.started_names.lock().unwrap();
            assert!(
                !started.contains(old_name),
                "the original (now-removed) name must not linger in \
                 started_names after a checkpoint reboot: {started:?}"
            );
            assert!(
                started.contains(&candidates[0]),
                "the fresh, actually-running sandbox must be tracked in \
                 started_names so close() covers it: {started:?}"
            );
        }

        // The ledger update actually matters: close() must stop/rm the FRESH
        // (live) name, not waste its own-process-shutdown safety net on the
        // original name that create_checkpoint's own cycle already removed.
        backend.close().await.expect("close must succeed");
        let stop_rm_calls = std::fs::read_to_string(dir.join("stop-rm-calls")).unwrap();
        assert!(
            stop_rm_calls.contains(&format!("stop {}", candidates[0])),
            "close() must stop the fresh (live) sandbox: {stop_rm_calls}"
        );
        assert!(
            stop_rm_calls.contains(&format!("rm {}", candidates[0])),
            "close() must rm the fresh (live) sandbox: {stop_rm_calls}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn create_checkpoint_reboot_advances_to_a_new_candidate_on_each_already_exists_refusal() {
        // Red-proof (c), rewritten for the candidate-batch policy: an
        // already-exists refusal must NEVER be retried under the SAME name —
        // the live-verified reason the old same-name retry was replaced (a
        // restore that fails after msb's own artifact-integrity check can
        // leave that name behind as a stopped sandbox record, dooming any
        // retry under it specifically to this exact refusal, for the whole
        // budget — rightsize-kotlin run 35292480264's CI failure). So this
        // forces TWO refusals in a row and proves the backend walked THREE
        // distinct candidates (never retrying #0 or #1), best-effort `rm`ing
        // each failed one before moving on, and never falling back to the
        // original name.
        let dir = unique_test_dir("checkpoint-reboot-already-exists-retry");
        let old_name = "rz-checkpoint-retry-old";
        let candidates = vec![
            "rz-checkpoint-retry-cand-0".to_string(),
            "rz-checkpoint-retry-cand-1".to_string(),
            "rz-checkpoint-retry-cand-2".to_string(),
        ];
        let already_exists_refusals = 2;
        let script = write_fake_msb_for_checkpoint_reboot(&dir, already_exists_refusals);
        let backend = MsbCliBackend::new(script);
        let spec = ContainerSpec {
            command: Some(vec!["redis-server".to_string()]),
            ..ContainerSpec::new(old_name, "unused-image", "run-1")
        };
        let handle = backend.create(spec).await.expect("create must succeed");
        let checkpoint_hint = dir
            .join("checkpoints")
            .join("rz-ckpt-test")
            .display()
            .to_string();

        let (_checkpoint_ref, new_handle) = backend
            .create_checkpoint(handle.as_ref(), &checkpoint_hint, &candidates)
            .await
            .expect(
                "two already-exists refusals must still be walked past by advancing \
                 candidates — the retry budget is 30s/2s, comfortably past two",
            );
        assert_eq!(
            new_handle.id(),
            candidates[2],
            "the THIRD candidate is the one that actually won, since the first two \
             were refused"
        );

        let restore_calls: u32 = std::fs::read_to_string(dir.join("restore-calls"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            restore_calls, 3,
            "two refused attempts plus one that succeeds"
        );

        let restore_argv = std::fs::read_to_string(dir.join("restore-argv")).unwrap();
        let argv_lines: Vec<&str> = restore_argv.lines().collect();
        assert_eq!(
            argv_lines.len(),
            3,
            "one line per restore attempt: {restore_argv}"
        );
        for (line, candidate) in argv_lines.iter().zip(candidates.iter()) {
            assert!(
                line.contains(&format!("--name {candidate}")),
                "attempt order must match candidate order exactly — never a repeat, \
                 never a fallback to an earlier one: {restore_argv}"
            );
        }
        assert!(
            !restore_argv.contains(old_name),
            "no restore attempt may ever mention the original (now-removed) \
             name: {restore_argv}"
        );

        // The proof that matters most: every restore attempt's own --name
        // value must be DISTINCT — this is what actually distinguishes the
        // new candidate-advance policy from the old same-name retry it
        // replaced (which this red-proof used to check the opposite of).
        let distinct_names: std::collections::HashSet<&str> = argv_lines
            .iter()
            .map(|line| line.split_whitespace().nth(3).expect("--name value"))
            .collect();
        assert_eq!(
            distinct_names.len(),
            3,
            "every attempt must target a DIFFERENT candidate name, never retry the \
             one that just refused: {restore_argv}"
        );

        // Best-effort cleanup: each of the two FAILED candidates must have
        // been `rm`-ed before the next attempt — never the winner, which is
        // still live.
        let stop_rm_calls = std::fs::read_to_string(dir.join("stop-rm-calls")).unwrap();
        assert!(
            stop_rm_calls.contains(&format!("rm {}", candidates[0])),
            "the first refused candidate must be best-effort removed: {stop_rm_calls}"
        );
        assert!(
            stop_rm_calls.contains(&format!("rm {}", candidates[1])),
            "the second refused candidate must be best-effort removed: {stop_rm_calls}"
        );
    }

    #[tokio::test]
    async fn has_checkpoint_on_a_path_ref_is_true_when_the_artifact_dir_has_snapshot_json() {
        let dir = unique_test_dir("has-checkpoint-path-ref-true");
        let artifact_dir = dir.join("checkpoints").join("rz-ckpt-deadbeefcafe");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::write(artifact_dir.join("snapshot.json"), b"{}").unwrap();
        // No msb binary at this path — a path-ref probe must never invoke it.
        let backend = MsbCliBackend::new(PathBuf::from("/definitely/not/a/real/msb"));

        let present = backend
            .has_checkpoint(&artifact_dir.display().to_string())
            .await
            .expect("a path-ref probe never touches msb, so a bogus msb path can't fail it");
        assert!(present);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn has_checkpoint_on_a_path_ref_is_false_when_snapshot_json_is_missing() {
        let dir = unique_test_dir("has-checkpoint-path-ref-incomplete");
        let artifact_dir = dir.join("checkpoints").join("rz-ckpt-deadbeefcafe");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        let backend = MsbCliBackend::new(PathBuf::from("/definitely/not/a/real/msb"));

        let present = backend
            .has_checkpoint(&artifact_dir.display().to_string())
            .await
            .unwrap();
        assert!(!present);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn has_checkpoint_on_a_path_ref_is_false_when_the_directory_does_not_exist() {
        let backend = MsbCliBackend::new(PathBuf::from("/definitely/not/a/real/msb"));
        let missing = std::env::temp_dir().join("rz-msb-checkpoint-does-not-exist-4f2c9a");

        let present = backend
            .has_checkpoint(&missing.display().to_string())
            .await
            .unwrap();
        assert!(!present);
    }

    /// Writes a stub `msb` replacement that logs its full argv, space-joined, as
    /// one line per invocation to `calls.log` beside the script itself — every
    /// caller of this stub only needs to assert on that log, not juggle a
    /// separate state file.
    #[cfg(unix)]
    fn write_argv_logging_stub(dir: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-msb-log-argv.sh");
        let body = "#!/bin/sh\n\
             echo \"$@\" >> \"$(dirname \"$0\")/calls.log\"\n\
             exit 0\n";
        std::fs::write(&script, body).expect("write argv-logging stub msb script");
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod argv-logging stub msb script");
        script
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn has_checkpoint_on_a_bare_name_ref_still_goes_through_snapshot_inspect() {
        let dir = unique_test_dir("has-checkpoint-bare-name");
        let script = write_argv_logging_stub(&dir);
        let backend = MsbCliBackend::new(script);

        // The stub exits 0 unconditionally, so a bare-name ref reaching it at all
        // is what this test is confirming — the path-ref branch must not have
        // swallowed it.
        let present = backend
            .has_checkpoint("rz-ckpt-deadbeefcafe")
            .await
            .unwrap();
        assert!(present);

        let log = std::fs::read_to_string(dir.join("calls.log")).unwrap();
        assert_eq!(log.trim(), "snapshot inspect rz-ckpt-deadbeefcafe");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn remove_checkpoint_on_a_path_ref_invokes_snapshot_rm_with_the_full_path_and_clears_the_leftover_artifact_dir()
     {
        let dir = unique_test_dir("remove-checkpoint-path-ref");
        let script = write_argv_logging_stub(&dir);
        let backend = MsbCliBackend::new(script);

        let artifact_dir = dir
            .join("checkpoints")
            .join("rz-abc-1")
            .join("snap_deadbeefcafedeadbeefcafedeadbeef");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::write(artifact_dir.join("snapshot.json"), b"{}").unwrap();

        backend
            .remove_checkpoint(&artifact_dir.display().to_string())
            .await
            .unwrap();

        let log = std::fs::read_to_string(dir.join("calls.log")).unwrap();
        assert_eq!(
            log.trim(),
            format!("snapshot rm {} -f", artifact_dir.display()),
            "msb 0.7.1 only resolves a dest-dir snapshot by its own artifact path — \
             never a bare basename"
        );
        assert!(
            !artifact_dir.exists(),
            "a leftover path-ref artifact dir must be cleaned up after a successful snapshot rm"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn remove_checkpoint_on_a_bare_name_ref_invokes_snapshot_rm_with_the_ref_unchanged() {
        let dir = unique_test_dir("remove-checkpoint-bare-name");
        let script = write_argv_logging_stub(&dir);
        let backend = MsbCliBackend::new(script);

        backend
            .remove_checkpoint("rz-ckpt-deadbeefcafe")
            .await
            .unwrap();

        let log = std::fs::read_to_string(dir.join("calls.log")).unwrap();
        assert_eq!(log.trim(), "snapshot rm rz-ckpt-deadbeefcafe -f");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Writes a stub `msb` replacement whose `snapshot rm` invocation always
    /// fails — with the live-verified head-removal refusal wording (see
    /// `commands::snapshot_rm`'s doc) on stderr and a nonzero exit — while every
    /// other subcommand still exits 0. For exercising
    /// [`MsbCliBackend::remove_checkpoint`]'s refusal-propagating behavior (see
    /// the next test).
    #[cfg(unix)]
    fn write_snapshot_rm_head_refusing_stub(dir: &Path) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-msb-refuse-rm.sh");
        let body = "#!/bin/sh\n\
             if [ \"$1\" = snapshot ] && [ \"$2\" = rm ]; then\n\
             \x20\x20echo \"invalid config: cannot remove current head snap_deadbeef; first \
             select another snapshot with msb snapshot head src:snap_deadbeef\" >&2\n\
             \x20\x20exit 1\n\
             fi\n\
             exit 0\n";
        std::fs::write(&script, body).expect("write rm-refusing stub msb script");
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod rm-refusing stub msb script");
        script
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn remove_checkpoint_propagates_the_head_refusal_as_an_error_without_sweeping_the_artifact_dir()
     {
        // This backend does not attempt automatic head rotation to work around
        // msb's head-removal refusal, and must not paper over it by deleting
        // msb's own artifact out from under its still-live index entry either
        // — but the refusal itself must reach the caller as an `Err`, not be
        // swallowed as if the removal had succeeded.
        let dir = unique_test_dir("remove-checkpoint-head-refused");
        let script = write_snapshot_rm_head_refusing_stub(&dir);
        let backend = MsbCliBackend::new(script);

        let artifact_dir = dir
            .join("checkpoints")
            .join("rz-abc-1")
            .join("snap_deadbeefcafedeadbeefcafedeadbeef");
        std::fs::create_dir_all(&artifact_dir).unwrap();
        std::fs::write(artifact_dir.join("snapshot.json"), b"{}").unwrap();

        let err = backend
            .remove_checkpoint(&artifact_dir.display().to_string())
            .await
            .expect_err("msb's head-removal refusal must surface as an Err, not Ok(())");
        let message = err.to_string();
        assert!(
            message.contains("cannot remove current head"),
            "the propagated error must quote msb's own refusal wording, got: {message}"
        );

        assert!(
            artifact_dir.exists(),
            "a refused removal must leave the artifact directory alone — msb's own index \
             still points at it"
        );
        assert!(
            artifact_dir.join("snapshot.json").is_file(),
            "the artifact's own content must be untouched, not just the directory"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- run vs restore: `try_spawn_and_await_running`'s own branch ----
    //
    // The argv-logging stub exits 0 unconditionally for every subcommand, but
    // never fabricates JSON `ls`/`logs` output, so both branches still end in
    // failure against it — `run` (`try_run_and_await_running`) falls through its
    // fast-exit post-mortem to the generic "before reaching Running" error, and
    // `restore` (`try_restore_and_await_running`) reads the empty/unparsable `ls`
    // output as the sandbox having dropped out of `ls` entirely and fails its
    // Phase 2 poll on the first iteration. Either failure is expected and
    // irrelevant here. What these tests assert on is which subcommand was
    // actually spawned first — found by its distinctive leading word rather than
    // by log position, since the readiness-poll loop can race an `ls` check (a
    // SEPARATE `msb` invocation, logged independently) against the primary
    // child's own exit before either write lands.

    /// The one logged call starting with `leading_word` — the primary `run`/
    /// `restore` invocation, told apart from the loop's own incidental `ls`/
    /// `logs` post-mortem calls by its distinctive first word.
    fn find_logged_call(dir: &Path, leading_word: &str) -> Option<String> {
        std::fs::read_to_string(dir.join("calls.log"))
            .unwrap()
            .lines()
            .find(|line| line.starts_with(leading_word))
            .map(ToString::to_string)
    }

    #[cfg(unix)]
    #[test]
    fn try_spawn_and_await_running_emits_msb_run_when_checkpoint_ref_is_unset() {
        let dir = unique_test_dir("run-vs-restore-plain");
        let script = write_argv_logging_stub(&dir);
        let spec = ContainerSpec::new("rz-plain-1", "alpine:3.19", "run-1");

        let _ = try_spawn_and_await_running(&script, &spec);

        assert_eq!(
            find_logged_call(&dir, "run "),
            Some("run --name rz-plain-1 alpine:3.19".to_string())
        );
        assert_eq!(
            find_logged_call(&dir, "restore "),
            None,
            "an unrestored spec must never invoke `msb restore`"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn try_spawn_and_await_running_emits_msb_restore_with_no_disk_only_flag_when_checkpoint_ref_is_set()
     {
        let dir = unique_test_dir("run-vs-restore-checkpoint");
        let script = write_argv_logging_stub(&dir);
        let mut spec = ContainerSpec::new("rz-restored-1", "unused-image", "run-1");
        spec.checkpoint_ref =
            Some("/cache/checkpoints/rz-abc-1/snap_deadbeefcafedeadbeefcafedeadbeef".to_string());

        let _ = try_spawn_and_await_running(&script, &spec);

        assert_eq!(
            find_logged_call(&dir, "restore "),
            Some(
                "restore /cache/checkpoints/rz-abc-1/snap_deadbeefcafedeadbeefcafedeadbeef \
                 --name rz-restored-1"
                    .to_string()
            ),
            "msb 0.7.1 rejects --disk-only against the disk-scope snapshots this backend \
             creates (verified live), so restore must never emit it"
        );
        assert_eq!(
            find_logged_call(&dir, "run "),
            None,
            "must never fall back to `msb run` once checkpoint_ref is set"
        );

        let _ = std::fs::remove_dir_all(&dir);
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

    // ---- parse_snapshot_load_ref: msb 0.7.1's loaded-artifact path, parsed off
    // `snapshot load`'s own stdout the same defensive way `parse_snapshot_create_ref`
    // parses `snapshot create`'s — see `parse_snapshot_create_ref`'s own tests just
    // below for why every fixture is built from `std::env::temp_dir()`.

    #[test]
    fn parse_snapshot_load_ref_takes_the_last_line_when_it_is_an_absolute_path() {
        let artifact = std::env::temp_dir()
            .join("checkpoints")
            .join("msb-deadbeef")
            .join("snap_b9c0448ee9d54e33b9c0448ee9d54e33");
        assert_eq!(
            parse_snapshot_load_ref(&format!(
                "group msb-deadbeef: head snap_b9c0448ee9d54e33b9c0448ee9d54e33 (Initialized)\n\
                 digest: sha256:fulldigesthere\n{}\n",
                artifact.display()
            )),
            Some(artifact.display().to_string())
        );
    }

    #[test]
    fn parse_snapshot_load_ref_skips_trailing_blank_lines() {
        let artifact = std::env::temp_dir()
            .join("checkpoints")
            .join("msb-deadbeef")
            .join("snap_abcdef");
        assert_eq!(
            parse_snapshot_load_ref(&format!("{}\n\n\n", artifact.display())),
            Some(artifact.display().to_string())
        );
    }

    #[test]
    fn parse_snapshot_load_ref_none_when_the_last_line_is_not_an_absolute_path() {
        // Unlike the pre-0.7.1 parse (which pulled the last whitespace-separated
        // token out of any last line, even a full prose sentence), the whole last
        // line must itself be the path — a garbage or relative-looking line is
        // `None`, never a guess.
        assert_eq!(
            parse_snapshot_load_ref("Importing snapshot...\nImported to relative/looking/path\n"),
            None
        );
        assert_eq!(parse_snapshot_load_ref("just some prose, no path"), None);
    }

    #[test]
    fn parse_snapshot_load_ref_none_on_entirely_blank_output() {
        assert_eq!(parse_snapshot_load_ref("\n\n"), None);
        assert_eq!(parse_snapshot_load_ref(""), None);
    }

    // ---- parse_already_exists_stderr_ref: the untested "already exists"
    // fallback, restoring the pre-0.7.1-verified stderr-only shape ----

    #[test]
    fn parse_already_exists_stderr_ref_takes_the_trailing_path_on_the_error_line() {
        let artifact = std::env::temp_dir()
            .join("checkpoints")
            .join("msb-deadbeef")
            .join("snap_b9c0448ee9d54e33b9c0448ee9d54e33");
        assert_eq!(
            parse_already_exists_stderr_ref(&format!(
                "error: snapshot already exists: {}",
                artifact.display()
            )),
            Some(artifact.display().to_string())
        );
    }

    #[test]
    fn parse_already_exists_stderr_ref_none_when_the_trailing_token_is_not_absolute() {
        assert_eq!(
            parse_already_exists_stderr_ref("error: snapshot already exists: relative/path"),
            None
        );
        assert_eq!(
            parse_already_exists_stderr_ref("just some prose, no path"),
            None
        );
    }

    #[test]
    fn parse_already_exists_stderr_ref_none_on_entirely_blank_output() {
        assert_eq!(parse_already_exists_stderr_ref(""), None);
        assert_eq!(parse_already_exists_stderr_ref("\n\n"), None);
    }

    // ---- parse_snapshot_create_ref: msb 0.7.1's dest-dir artifact path, parsed
    // back out of `snapshot create`'s own stdout ----
    //
    // Every absolute-path fixture below is built from `std::env::temp_dir()`
    // rather than a hand-typed Unix literal like "/cache/checkpoints/..." —
    // `Path::is_absolute()`, which this function relies on, is false for a bare
    // leading-slash path on Windows (no drive/prefix component), so a literal
    // like that would make these assertions fail there.

    #[test]
    fn parse_snapshot_create_ref_takes_the_last_line_when_it_is_an_absolute_path() {
        let artifact = std::env::temp_dir()
            .join("checkpoints")
            .join("rz-abc-1")
            .join("snap_deadbeefcafedeadbeefcafedeadbeef");
        assert_eq!(
            parse_snapshot_create_ref(&format!(
                "Snapshot ID: deadbeefcafedeadbeefcafedeadbeef\n{}\n",
                artifact.display()
            )),
            Some(artifact.display().to_string())
        );
    }

    #[test]
    fn parse_snapshot_create_ref_trims_whitespace_and_skips_trailing_blank_lines() {
        let artifact = std::env::temp_dir()
            .join("checkpoints")
            .join("rz-abc-1")
            .join("snap_deadbeef");
        assert_eq!(
            parse_snapshot_create_ref(&format!("  {}  \n\n\n", artifact.display())),
            Some(artifact.display().to_string())
        );
    }

    #[test]
    fn parse_snapshot_create_ref_none_when_the_last_line_is_not_an_absolute_path() {
        assert_eq!(
            parse_snapshot_create_ref("Snapshot ID: deadbeef\nrelative/looking/path\n"),
            None
        );
        assert_eq!(parse_snapshot_create_ref("just some prose, no path"), None);
    }

    #[test]
    fn parse_snapshot_create_ref_none_on_entirely_blank_output() {
        assert_eq!(parse_snapshot_create_ref(""), None);
        assert_eq!(parse_snapshot_create_ref("\n\n"), None);
    }

    // Every absolute-path fixture below is built from `std::env::temp_dir()`
    // rather than a hand-typed Unix literal — see `parse_snapshot_create_ref`'s own
    // tests for why: `Path::is_absolute()` is false for a bare leading-slash path
    // on Windows.

    #[test]
    fn msb_import_checkpoint_cycle_happy_path_returns_the_loaded_artifact_path() {
        let artifact = std::env::temp_dir()
            .join("checkpoints")
            .join("msb-deadbeef")
            .join("snap_b9c0448ee9d54e33b9c0448ee9d54e33");
        let mut import_calls = 0;
        let result = {
            let mut invoke_import = || {
                import_calls += 1;
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: format!(
                        "group msb-deadbeef: head snap_b9c0448ee9d54e33b9c0448ee9d54e33 \
                         (Initialized)\ndigest: sha256:fulldigesthere\n{}\n",
                        artifact.display()
                    ),
                    stderr: String::new(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import)
        };
        assert_eq!(
            result.unwrap(),
            artifact.display().to_string(),
            "the effective ref must be the loaded artifact's own absolute path — msb 0.7.1 \
             prints it directly, never a digest-dir name resolved separately via `snapshot list`"
        );
        assert_eq!(import_calls, 1);
    }

    #[test]
    fn msb_import_checkpoint_cycle_treats_already_exists_as_success_when_stdout_has_the_path() {
        let artifact = std::env::temp_dir()
            .join("checkpoints")
            .join("msb-deadbeef")
            .join("snap_b9c0448ee9d54e33b9c0448ee9d54e33");
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: format!(
                        "group msb-deadbeef: head snap_b9c0448ee9d54e33b9c0448ee9d54e33 \
                         (Initialized)\n{}\n",
                        artifact.display()
                    ),
                    stderr: format!("error: snapshot already exists: {}", artifact.display()),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import)
        };
        assert_eq!(
            result.unwrap(),
            artifact.display().to_string(),
            "an already-exists import is success for a content-addressed archive, resolving to \
             the same artifact path a fresh import would"
        );
    }

    /// The UNVERIFIED branch: if `load` on an "already exists" outcome turns out
    /// to behave the way the pre-0.7.1 `import` verb did — nothing usable on
    /// stdout, the path only in the `error: snapshot already exists: <path>`
    /// stderr line — the cycle must still resolve the ref via
    /// [`parse_already_exists_stderr_ref`] rather than failing outright. This is
    /// the previously-established shape this branch restores coverage for; see
    /// [`msb_import_checkpoint_cycle`]'s own doc.
    #[test]
    fn msb_import_checkpoint_cycle_already_exists_falls_back_to_stderr_when_stdout_has_no_path() {
        let artifact = std::env::temp_dir()
            .join("checkpoints")
            .join("msb-deadbeef")
            .join("snap_b9c0448ee9d54e33b9c0448ee9d54e33");
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: format!("error: snapshot already exists: {}", artifact.display()),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import)
        };
        assert_eq!(
            result.unwrap(),
            artifact.display().to_string(),
            "an already-exists import with an empty stdout must still resolve the ref, parsed \
             off the \"already exists\" stderr line the same way the pre-0.7.1 `import` verb did"
        );
    }

    #[test]
    fn msb_import_checkpoint_cycle_already_exists_with_no_path_anywhere_fails_with_a_clear_error() {
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: "error: snapshot already exists: relative/looking/path".to_string(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import)
        };
        let err = result.expect_err(
            "an already-exists outcome with no parseable absolute path on either stream must \
             never resolve to a bogus ref",
        );
        let message = err.to_string();
        assert!(
            message.contains("did not end with a recognizable"),
            "{message}"
        );
    }

    #[test]
    fn msb_import_checkpoint_cycle_a_genuine_failure_surfaces_stderr() {
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: "error: corrupt archive: bad checksum".to_string(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import)
        };
        let err = result.expect_err("a genuine import failure must surface");
        assert!(err.to_string().contains("bad checksum"), "{err}");
    }

    /// Red-proof for the last-line-must-be-absolute contract: `load` exiting 0
    /// with stdout that never ends in a recognizable path (msb printing nothing
    /// usable, an unexpected wording change, a stub gone wrong) must never be
    /// silently trusted as a ref.
    #[test]
    fn msb_import_checkpoint_cycle_garbage_stdout_fails_with_a_clear_error() {
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: "not a path, just some prose\n".to_string(),
                    stderr: String::new(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import)
        };
        let err = result.expect_err(
            "stdout whose last line isn't an absolute path must never resolve to a ref",
        );
        let message = err.to_string();
        assert!(
            message.contains("did not end with a recognizable"),
            "{message}"
        );
        assert!(message.contains("not a path, just some prose"), "{message}");
    }

    #[test]
    fn msb_import_checkpoint_cycle_blank_stdout_fails_with_a_clear_error() {
        let result = {
            let mut invoke_import = || {
                Ok(ExecResult {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            };
            msb_import_checkpoint_cycle(&mut invoke_import)
        };
        result
            .expect_err("entirely blank stdout on a reported success must never resolve to a ref");
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
    fn is_agent_endpoint_not_ready_matches_the_captured_windows_pipe_failure() {
        // Captured verbatim from a windows-2025 hosted runner: an exec issued against a
        // sandbox restored from a checkpoint archive, before the guest agent had created
        // its named pipe.
        assert!(is_agent_endpoint_not_ready(
            "error: agent client error: connect \\\\.\\pipe\\msb-agent-e7779577a75cc1f89f66c534458bf8fd: The system cannot find the file specified. (os error 2)"
        ));
    }

    #[test]
    fn is_agent_endpoint_not_ready_matches_the_unix_socket_shape() {
        // Same msb framing, unix socket rather than a named pipe — the endpoint is
        // platform-specific but the "couldn't connect at all" classification is not.
        assert!(is_agent_endpoint_not_ready(
            "error: agent client error: connect /run/user/1001/msb-agent-abc123.sock: No such file or directory (os error 2)"
        ));
    }

    #[test]
    fn is_agent_endpoint_not_ready_ignores_a_guest_commands_own_failure() {
        // A command that ran and failed inside the guest must return on the first
        // attempt — retrying it would multiply its runtime for no reason.
        assert!(!is_agent_endpoint_not_ready(
            "cat: /srv/missing.txt: No such file or directory"
        ));
        assert!(!is_agent_endpoint_not_ready(""));
    }

    #[test]
    fn is_agent_endpoint_not_ready_ignores_a_post_connect_agent_error() {
        // Reached the agent, then the agent itself reported a problem — a real error to
        // surface, not a not-yet-listening endpoint to wait out.
        assert!(!is_agent_endpoint_not_ready(
            "error: agent client error: request failed: guest process exited before responding"
        ));
    }

    /// The stderr of the failure being worked around, captured verbatim from a
    /// `windows-2025` hosted runner — every `msb snapshot save` on msb 0.6.7/0.6.8
    /// ends this way (see [`is_archive_fsync_access_denied`]).
    const CAPTURED_WINDOWS_SAVE_FAILURE: &str = "msb snapshot save rz-ckpt-fccd7568-archive C:\\Users\\RUNNER~1\\AppData\\Local\\Temp\\rightsize-archive-export-8980-1-1785507050813959600\\artifact failed (exit 1): error: io error: Access is denied. (os error 5)";

    #[test]
    fn is_archive_fsync_access_denied_matches_the_captured_windows_save_failure() {
        assert!(is_archive_fsync_access_denied(
            CAPTURED_WINDOWS_SAVE_FAILURE
        ));
        // The classifier reads the numeric suffix Rust appends itself, so a machine
        // whose display language renders the message differently classifies the same.
        assert!(is_archive_fsync_access_denied(
            "error: io error: Zugriff verweigert. (os error 5)"
        ));
    }

    #[test]
    fn is_archive_fsync_access_denied_ignores_other_snapshot_save_failures() {
        // A save that failed for a real reason must surface as itself — nothing to
        // salvage, and the staging file this workaround looks for was never left.
        assert!(!is_archive_fsync_access_denied(
            "error: snapshot not found: rz-ckpt-fccd7568-archive"
        ));
        assert!(!is_archive_fsync_access_denied(
            "error: io error: The system cannot find the path specified. (os error 3)"
        ));
        // Shares error 5's leading digit — the needle's closing parenthesis is what
        // keeps it out.
        assert!(!is_archive_fsync_access_denied(
            "error: io error: The network path was not found. (os error 53)"
        ));
        assert!(!is_archive_fsync_access_denied(""));
    }

    #[test]
    fn salvage_archive_staging_file_moves_the_single_staging_file_onto_the_destination() {
        let dir = unique_test_dir("archive-salvage-one");
        let dest = dir.join("artifact");
        let staging = dir.join(".artifact.tmp.8980.1785507050813959600");
        std::fs::write(&staging, b"complete archive bytes").unwrap();

        assert!(
            salvage_archive_staging_file(&dest),
            "one staging file beside the destination is exactly the shape msb's failed \
             fsync leaves behind"
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"complete archive bytes");
        assert!(
            !staging.exists(),
            "the staging file is moved, not copied — msb's own rename would have \
             consumed it too"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn salvage_archive_staging_file_reports_failure_when_there_is_no_staging_file() {
        let dir = unique_test_dir("archive-salvage-none");
        let dest = dir.join("artifact");
        // A sibling that is not a staging file for this destination must not tempt it.
        std::fs::write(dir.join("unrelated"), b"x").unwrap();

        assert!(!salvage_archive_staging_file(&dest));
        assert!(
            !dest.exists(),
            "nothing to salvage means nothing is created either"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn salvage_archive_staging_file_refuses_to_guess_between_two_staging_files() {
        let dir = unique_test_dir("archive-salvage-two");
        let dest = dir.join("artifact");
        let first = dir.join(".artifact.tmp.8980.1785507050813959600");
        let second = dir.join(".artifact.tmp.9042.1785507061112223344");
        std::fs::write(&first, b"first").unwrap();
        std::fs::write(&second, b"second").unwrap();

        assert!(
            !salvage_archive_staging_file(&dest),
            "two candidates is not the failure being worked around; picking one would \
             be a guess"
        );
        assert_eq!(std::fs::read(&first).unwrap(), b"first");
        assert_eq!(std::fs::read(&second).unwrap(), b"second");
        assert!(!dest.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn msb_export_checkpoint_cycle_surfaces_an_unrelated_failure_without_salvaging() {
        let mut salvage_attempts = 0usize;
        let result = {
            let mut invoke_export = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: "error: snapshot not found: rz-ckpt-fccd7568-archive".to_string(),
                })
            };
            let mut salvage = |_: &Path| {
                salvage_attempts += 1;
                true
            };
            msb_export_checkpoint_cycle(
                &mut invoke_export,
                &mut salvage,
                "rz-ckpt-fccd7568-archive",
                Path::new("/tmp/rightsize-archive-export/artifact"),
            )
        };
        let err = result.expect_err("a failure that isn't the Windows fsync bug must propagate");
        let message = err.to_string();
        assert!(
            message.contains("msb snapshot save rz-ckpt-fccd7568-archive"),
            "{message}"
        );
        assert!(message.contains("exit 1"), "{message}");
        assert!(
            message.contains("error: snapshot not found: rz-ckpt-fccd7568-archive"),
            "{message}"
        );
        assert_eq!(
            salvage_attempts, 0,
            "salvage is reserved for the one failure it was written for"
        );
    }

    /// The captured `msb snapshot save` stderr from a windows-2025 runner, whose
    /// only distinguishing mark is the errno suffix Rust appends.
    const WINDOWS_FSYNC_SAVE_STDERR: &str = "error: io error: Access is denied. (os error 5)";

    #[test]
    fn msb_export_checkpoint_cycle_reports_success_once_the_staging_file_is_salvaged() {
        let mut salvage_attempts = 0usize;
        let result = {
            let mut invoke_export = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: WINDOWS_FSYNC_SAVE_STDERR.to_string(),
                })
            };
            let mut salvage = |_: &Path| {
                salvage_attempts += 1;
                true
            };
            msb_export_checkpoint_cycle(
                &mut invoke_export,
                &mut salvage,
                "rz-ckpt-fccd7568-archive",
                Path::new("/tmp/rightsize-archive-export/artifact"),
            )
        };
        result.expect("a salvaged archive is a completed export, not a failure");
        assert_eq!(salvage_attempts, 1);
    }

    #[test]
    fn msb_export_checkpoint_cycle_surfaces_the_original_error_when_the_salvage_declines() {
        // The predicate matching is not on its own enough: if the staging file is
        // absent or ambiguous the salvage declines, and msb's own error is still
        // the truthful outcome.
        let mut salvage_attempts = 0usize;
        let result = {
            let mut invoke_export = || {
                Ok(ExecResult {
                    exit_code: 1,
                    stdout: String::new(),
                    stderr: WINDOWS_FSYNC_SAVE_STDERR.to_string(),
                })
            };
            let mut salvage = |_: &Path| {
                salvage_attempts += 1;
                false
            };
            msb_export_checkpoint_cycle(
                &mut invoke_export,
                &mut salvage,
                "rz-ckpt-fccd7568-archive",
                Path::new("/tmp/rightsize-archive-export/artifact"),
            )
        };
        let message = result
            .expect_err("a declined salvage must not be reported as a successful export")
            .to_string();
        assert!(message.contains(WINDOWS_FSYNC_SAVE_STDERR), "{message}");
        assert_eq!(salvage_attempts, 1);
    }

    #[test]
    fn salvage_archive_staging_file_ignores_a_directory_carrying_the_staging_name() {
        // msb writes a regular file there; a directory under that name would
        // otherwise consume the single-candidate slot.
        let dir = unique_test_dir("archive-salvage-dir");
        let dest = dir.join("artifact");
        std::fs::create_dir(dir.join(".artifact.tmp.8980.1785507050813959600")).unwrap();

        assert!(!salvage_archive_staging_file(&dest));
        assert!(!dest.exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn is_msb_install_lock_active_matches_the_captured_refusal_verbatim() {
        // Captured from a windows-2025 hosted runner: `msb run` refused mid-suite while
        // msb's internal install lock was held, ordinary boots succeeding on both sides.
        assert!(is_msb_install_lock_active(
            "error: runtime error: microsandbox install operation in progress until 2026-07-31 20:35:23.760135600; retry after it completes"
        ));
        // The deadline timestamp varies per occurrence; the classifier keys on the
        // stable phrase only.
        assert!(is_msb_install_lock_active(
            "error: runtime error: microsandbox install operation in progress until 2027-01-01 00:00:00.000000000; retry after it completes"
        ));
        // The second phrasing, also captured from a windows-2025 hosted runner: msb
        // words the refusal with an "is" (and no retry hint) when the other side
        // holds the lock.
        assert!(is_msb_install_lock_active(
            "error: runtime error: another microsandbox install operation is in progress until 2026-08-01 19:26:19.025098100"
        ));
    }

    #[test]
    fn is_msb_install_lock_active_ignores_other_runtime_errors() {
        assert!(!is_msb_install_lock_active(
            "error: runtime error: something else entirely"
        ));
        assert!(!is_msb_install_lock_active(
            "error: failed to start \"rz-abc-1\""
        ));
        assert!(!is_msb_install_lock_active(""));
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

    /// The loaded artifact path a fake `snapshot load` prints, when it prints one
    /// at all — see [`fake_load_stdout_with_path`] and
    /// [`write_fake_msb_for_archives`]'s own doc for why an "already exists" run
    /// does NOT always get this on stdout the way a fresh one does.
    #[cfg(unix)]
    const FAKE_LOADED_ARTIFACT_PATH: &str =
        "/home/u/.microsandbox/checkpoints/msb-fakegroup1234/snap_fakedigest1234fakedigest1234";

    /// The stdout a fresh, successful `snapshot load` empirically prints against
    /// a real msb 0.7.1 binary: a `group ...: head ... (Initialized)` line, a
    /// digest line, then the loaded artifact's absolute path as the LAST line.
    #[cfg(unix)]
    fn fake_load_stdout_with_path() -> String {
        format!(
            "group msb-fakegroup1234: head snap_fakedigest1234fakedigest1234 (Initialized)\n\
             digest: sha256:fakedigest1234fakedigest1234fulldigesthere\n{FAKE_LOADED_ARTIFACT_PATH}\n"
        )
    }

    /// Writes a stub `msb` replacement that answers `snapshot save` and `snapshot
    /// load` — the two subcommands `export_checkpoint`/`import_checkpoint` drive
    /// on msb 0.7.1 (no more `snapshot list` round trip — see
    /// `msb_import_checkpoint_cycle`'s doc for why). `import_exit_code` lets a
    /// test choose between a fresh-import success (exit 0) and an already-exists
    /// "success" (nonzero exit, msb's own wording on `import_stderr`) — both of
    /// which [`MsbCliBackend::import_checkpoint`] must resolve to an effective
    /// ref. `import_stdout` is passed through verbatim (typically
    /// [`fake_load_stdout_with_path`] for a case known to print the artifact
    /// path, or `""` for the UNVERIFIED already-exists case where msb might
    /// print nothing useful on stdout — see [`msb_import_checkpoint_cycle`]'s own
    /// doc on why that branch is not assumed away).
    #[cfg(unix)]
    fn write_fake_msb_for_archives(
        dir: &Path,
        import_exit_code: u8,
        import_stdout: &str,
        import_stderr: &str,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-msb-archive.sh");
        let body = format!(
            "#!/bin/sh\n\
             case \"$1 $2\" in\n\
             \"snapshot save\")\n\
             printf 'fake-export-payload:%s' \"$3\" > \"$4\"\n\
             exit 0\n\
             ;;\n\
             \"snapshot load\")\n\
             echo '{import_stderr}' 1>&2\n\
             printf '%s' '{import_stdout}'\n\
             exit {import_exit_code}\n\
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
        let script = write_fake_msb_for_archives(&dir, 0, &fake_load_stdout_with_path(), "");
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
            effective_ref, FAKE_LOADED_ARTIFACT_PATH,
            "the effective ref must be the loaded artifact's own absolute path, printed \
             directly by `snapshot load` — never a digest-dir name resolved separately via \
             `snapshot list`, which msb 0.7.1's `load` has no need for"
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
            &fake_load_stdout_with_path(),
            &format!("error: snapshot already exists: {FAKE_LOADED_ARTIFACT_PATH}"),
        );
        let backend = MsbCliBackend::new(script);

        let effective_ref = backend
            .import_checkpoint(Path::new("/does/not/matter/for/this/stub"), "whatever-ref")
            .await
            .expect(
                "an already-exists import must resolve exactly like a fresh one, not surface \
                 as an error",
            );
        assert_eq!(effective_ref, FAKE_LOADED_ARTIFACT_PATH);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The UNVERIFIED-against-a-real-binary branch, exercised end to end: an
    /// "already exists" `load` that (like the pre-0.7.1 `import` verb it
    /// replaces) prints nothing usable on stdout, with the artifact path only in
    /// the `error: snapshot already exists: <path>` stderr line. Must still
    /// resolve to the same ref a fresh import would — see
    /// `parse_already_exists_stderr_ref` and `msb_import_checkpoint_cycle`'s own
    /// doc for why this fallback exists.
    #[cfg(unix)]
    #[tokio::test]
    async fn import_checkpoint_already_exists_with_empty_stdout_falls_back_to_stderr() {
        let dir = unique_test_dir("archive-fake-binary-exists-stderr-only");
        let script = write_fake_msb_for_archives(
            &dir,
            1,
            "",
            &format!("error: snapshot already exists: {FAKE_LOADED_ARTIFACT_PATH}"),
        );
        let backend = MsbCliBackend::new(script);

        let effective_ref = backend
            .import_checkpoint(Path::new("/does/not/matter/for/this/stub"), "whatever-ref")
            .await
            .expect(
                "an already-exists import with nothing useful on stdout must still resolve via \
                 the stderr fallback, not surface as an error",
            );
        assert_eq!(effective_ref, FAKE_LOADED_ARTIFACT_PATH);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- fast-exit post-mortem classification (msb 0.6.16+) ----

    /// Writes a stub `msb` replacement for the fast-exit classification's tests.
    /// Dispatches on `$1`:
    /// - `run`  -> exits immediately with `run_exit_code`, before this backend's
    ///   polling loop ever gets a chance to observe `Running` — the scenario itself.
    /// - `ls`   -> answers `msb ls --format json` with this one sandbox reporting
    ///   `status: ls_status`.
    /// - `logs` -> answers `msb logs <name> --source system --tail 1000` with the
    ///   boot-completion marker line iff `marker_present`, otherwise an unrelated
    ///   line.
    #[cfg(unix)]
    fn write_fake_msb_for_fast_exit(
        dir: &Path,
        name: &str,
        run_exit_code: u8,
        ls_status: &str,
        marker_present: bool,
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-msb-fast-exit.sh");
        let marker_line = if marker_present {
            SANDBOX_STARTED_MARKER
        } else {
            "--- some unrelated system log line ---"
        };
        let body = format!(
            "#!/bin/sh\n\
             case \"$1\" in\n\
             run)\n\
             exit {run_exit_code}\n\
             ;;\n\
             ls)\n\
             echo '[{{\"name\":\"{name}\",\"status\":\"{ls_status}\"}}]'\n\
             exit 0\n\
             ;;\n\
             logs)\n\
             echo '{marker_line}'\n\
             exit 0\n\
             ;;\n\
             esac\n\
             exit 1\n"
        );
        std::fs::write(&script, body).expect("write fake msb fast-exit script");
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod fake msb fast-exit script");
        script
    }

    #[cfg(unix)]
    #[test]
    fn fast_exit_with_stopped_state_and_started_marker_classifies_as_success() {
        let dir = unique_test_dir("fast-exit-success");
        let name = "rz-fast-exit-ok";
        let script = write_fake_msb_for_fast_exit(&dir, name, 0, "Stopped", true);
        let spec = ContainerSpec::new(name, "alpine:3.19", "run-1");

        let result = spawn_and_await_running(&script, &spec);
        assert!(
            result.is_ok(),
            "exit 0 + ls Stopped + started marker present must classify as a \
             completed workload, not a failed boot: {:?}",
            result.err()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn fast_exit_without_the_started_marker_still_fails_with_the_existing_error() {
        let dir = unique_test_dir("fast-exit-no-marker");
        let name = "rz-fast-exit-no-marker";
        let script = write_fake_msb_for_fast_exit(&dir, name, 0, "Stopped", false);
        let spec = ContainerSpec::new(name, "alpine:3.19", "run-1");

        let err = spawn_and_await_running(&script, &spec).unwrap_err();
        assert!(
            err.to_string().contains("before reaching Running"),
            "an absent started marker must keep today's failure message unchanged, \
             even with ls reporting Stopped: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn fast_exit_with_a_non_stopped_state_still_fails_with_the_existing_error() {
        let dir = unique_test_dir("fast-exit-not-stopped");
        let name = "rz-fast-exit-not-stopped";
        let script = write_fake_msb_for_fast_exit(&dir, name, 0, "Exited", true);
        let spec = ContainerSpec::new(name, "alpine:3.19", "run-1");

        let err = spawn_and_await_running(&script, &spec).unwrap_err();
        assert!(
            err.to_string().contains("before reaching Running"),
            "a state other than Stopped must keep today's failure message unchanged, \
             even with the started marker present: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn fast_exit_classification_never_applies_to_a_non_zero_exit() {
        let dir = unique_test_dir("fast-exit-nonzero");
        let name = "rz-fast-exit-nonzero";
        let script = write_fake_msb_for_fast_exit(&dir, name, 1, "Stopped", true);
        let spec = ContainerSpec::new(name, "alpine:3.19", "run-1");

        let err = spawn_and_await_running(&script, &spec).unwrap_err();
        assert!(
            err.to_string().contains("before reaching Running"),
            "a non-zero exit must never be classified as a completed workload, \
             whatever ls/logs report: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- restore supervision: `try_restore_and_await_running`'s detached-boot
    // shape (msb 0.7.1's `msb restore` creates a DETACHED sandbox — see that
    // function's own doc) ----
    //
    // Writes a stub `msb` replacement dedicated to these tests. Dispatches on
    // `$1`:
    // - `restore` -> prints `restore_stderr` to stderr and exits immediately with
    //   `restore_exit_code` — the detached restore invocation itself, which on
    //   msb 0.7.1 activates the sandbox and exits, typically within seconds, well
    //   before the sandbox it started necessarily reaches `Running`.
    // - `ls`      -> answers `msb ls --format json` from `ls_statuses`, ADVANCING
    //   one entry per call (a minimal state machine: call 0 answers
    //   `ls_statuses[0]`, call 1 answers `ls_statuses[1]`, and so on, holding at
    //   the last entry once exhausted) — proving the boot poll actually polls
    //   more than once rather than trusting a single snapshot. `"ABSENT"` answers
    //   `[]` (the sandbox not listed at all, e.g. after `msb rm`), anything else
    //   answers a one-entry array reporting that literal status for `name`.
    // - `logs`    -> a fixed one-line system-log tail, for the boot-failure
    //   diagnostic's best-effort fetch.
    // - `stop`/`rm` -> exit 0, so a full `start()`-then-`stop()` round trip
    //   through the real backend never hangs on cleanup.
    #[cfg(unix)]
    /// Builds a fake `msb` for the whole restore + workload-revival flow —
    /// `restore`, `ls`, `logs` (workload and `--source system`), `stop`/`rm`, and
    /// now `exec` (phase 3's workload-revival spawn). `exec`'s own behavior is
    /// controlled by CONTROL FILES a test writes into `dir` BEFORE invoking the
    /// backend, rather than more Rust parameters — every existing call site of
    /// this function keeps working unchanged (its default, with no control files
    /// present, is a LONG-LIVED exec, matching what a real revived workload
    /// looks like):
    /// - every `exec` invocation appends its args (space-joined) as one line to
    ///   `<dir>/exec-calls`, so a test can assert the exact argv (including `-e`
    ///   pairs) `spawn_workload_exec` built;
    /// - `<dir>/exec-exit-code` (if present): `exec` exits with that code
    ///   immediately, first echoing `<dir>/exec-stderr`'s contents (if that file
    ///   also exists) to stderr — the early-exit red-proofs;
    /// - otherwise `exec` BLOCKS until `<dir>/stop-requested` appears (the fake's
    ///   own `stop`/`rm` cases touch it) — the long-lived, successful case; a
    ///   test driving `try_restore_and_await_running`/`spawn_and_await_running`
    ///   directly (never calling `stop`) is responsible for killing the child it
    ///   gets back itself;
    /// - `<dir>/access-denied-remaining` (if present, a decimal count): `restore`
    ///   decrements it and fails with the Windows access-denied transient's
    ///   exact output shape while it's `> 0`, then falls through to the ordinary
    ///   `restore_exit_code`/`restore_stderr` behavior once exhausted — red-proof
    ///   (d). Every `restore` call (denied or not) is counted in
    ///   `<dir>/restore-calls`.
    /// - `<dir>/system-log-marker` (if present): `logs ... --source system`
    ///   returns its contents instead of the plain `fake system log tail` —
    ///   lets a test supply [`SANDBOX_STARTED_MARKER`] for
    ///   [`fast_exit_ran_to_completion`]'s own post-mortem check.
    fn write_fake_msb_for_restore(
        dir: &Path,
        name: &str,
        restore_exit_code: u8,
        restore_stderr: &str,
        ls_statuses: &[&str],
    ) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-msb-restore.sh");

        let render_status = |status: &str| -> String {
            if status == "ABSENT" {
                "echo '[]'".to_string()
            } else {
                format!("echo '[{{\"name\":\"{name}\",\"status\":\"{status}\"}}]'")
            }
        };
        let mut case_arms = String::new();
        for (i, status) in ls_statuses.iter().enumerate() {
            case_arms.push_str(&format!("{i}) {} ;;\n", render_status(status)));
        }
        let last_arm = render_status(ls_statuses.last().copied().unwrap_or("ABSENT"));

        let body = format!(
            "#!/bin/sh\n\
             dir=\"$(dirname \"$0\")\"\n\
             case \"$1\" in\n\
             restore)\n\
             n=$(cat \"$dir/restore-calls\" 2>/dev/null || echo 0)\n\
             echo $((n + 1)) > \"$dir/restore-calls\"\n\
             if [ -f \"$dir/access-denied-remaining\" ]; then\n\
             remaining=$(cat \"$dir/access-denied-remaining\")\n\
             if [ \"$remaining\" -gt 0 ]; then\n\
             echo $((remaining - 1)) > \"$dir/access-denied-remaining\"\n\
             echo 'error: io error: Access is denied. (os error 5)' 1>&2\n\
             exit 1\n\
             fi\n\
             fi\n\
             echo '{restore_stderr}' 1>&2\n\
             exit {restore_exit_code}\n\
             ;;\n\
             ls)\n\
             n=$(cat \"$dir/ls-calls\" 2>/dev/null || echo 0)\n\
             echo $((n + 1)) > \"$dir/ls-calls\"\n\
             case \"$n\" in\n\
             {case_arms}\
             *) {last_arm} ;;\n\
             esac\n\
             exit 0\n\
             ;;\n\
             logs)\n\
             case \"$*\" in\n\
             *'--source system'*)\n\
             if [ -f \"$dir/system-log-marker\" ]; then cat \"$dir/system-log-marker\"; exit 0; fi\n\
             ;;\n\
             esac\n\
             echo 'fake system log tail'\n\
             exit 0\n\
             ;;\n\
             exec)\n\
             shift\n\
             echo \"$*\" >> \"$dir/exec-calls\"\n\
             if [ -f \"$dir/exec-exit-code\" ]; then\n\
             if [ -f \"$dir/exec-stderr\" ]; then cat \"$dir/exec-stderr\" 1>&2; fi\n\
             exit \"$(cat \"$dir/exec-exit-code\")\"\n\
             fi\n\
             while [ ! -f \"$dir/stop-requested\" ]; do sleep 0.05; done\n\
             exit 0\n\
             ;;\n\
             stop|rm)\n\
             touch \"$dir/stop-requested\"\n\
             exit 0\n\
             ;;\n\
             esac\n\
             exit 0\n"
        );
        std::fs::write(&script, body).expect("write fake msb restore script");
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).expect("chmod fake msb restore script");
        script
    }

    #[cfg(unix)]
    #[test]
    fn restore_succeeds_when_the_sandbox_reaches_running_only_after_a_few_polls() {
        // (a) Red-proof, continued: a successful detached restore — `restore`
        // exits 0 fast, `ls` only reports `Running` on its THIRD call — must
        // still succeed, and must actually have polled more than once to get
        // there (never claim success off the first, possibly-stale, `ls`
        // snapshot). Phase 3 must then follow with the workload-revival exec.
        let dir = unique_test_dir("restore-eventually-running");
        let name = "rz-restore-eventually-running";
        let script =
            write_fake_msb_for_restore(&dir, name, 0, "", &["Starting", "Starting", "Running"]);
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());
        spec.command = Some(vec!["redis-server".to_string()]);

        let result =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap());
        let mut child = result
            .unwrap_or_else(|e| {
                panic!("a detached restore reaching Running must succeed, got {e:?}")
            })
            .expect(
                "phase 3 must hand back the workload-revival exec as this boot's live child — a \
                 restore is no longer childless now that this backend revives the workload itself",
            );

        let ls_calls: u32 = std::fs::read_to_string(dir.join("ls-calls"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(
            ls_calls >= 3,
            "must have actually polled `ls` past the first two non-Running answers, not \
             short-circuited: {ls_calls} calls"
        );
        let exec_calls = std::fs::read_to_string(dir.join("exec-calls")).unwrap();
        assert_eq!(exec_calls.trim(), format!("{name} -- redis-server"));

        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_via_the_real_backend_spawns_an_attached_exec_child_and_stop_reaps_it() {
        // (a) continued, end to end through `MsbCliBackend`: `start()` on a
        // checkpoint-ref spec must succeed via the restore path AND leave the
        // handle with a live attached child again — the workload-revival exec,
        // not the old childless-restore shape — and `stop()` must still reap it
        // promptly rather than hanging out `ATTACHED_STOP_TIMEOUT`: the fake's
        // own `stop` case touches the sentinel the long-lived fake `exec` polls
        // for, exactly like a real `msb stop` severing the exec session would.
        let dir = unique_test_dir("restore-start-stop-reaps-exec");
        let name = "rz-restore-start-stop";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Running"]);
        let backend = MsbCliBackend::new(script);
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());
        spec.command = Some(vec!["redis-server".to_string()]);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = backend.create(spec).await.expect("create must succeed");
            backend
                .start(handle.as_ref())
                .await
                .expect("start() over a detached restore must succeed once ls reports Running");

            let started = std::time::Instant::now();
            backend
                .stop(handle.as_ref())
                .await
                .expect("stop() on the restored handle must still succeed");
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "stop() must reap the workload exec promptly via the sandbox-stop-severs-the-\
                 session path, not fall through to ATTACHED_STOP_TIMEOUT's multi-second SIGKILL \
                 fallback"
            );
        });

        let exec_calls = std::fs::read_to_string(dir.join("exec-calls")).unwrap();
        assert_eq!(exec_calls.trim(), format!("{name} -- redis-server"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_exiting_nonzero_with_a_state_db_signature_is_classified_not_treated_as_a_generic_failure()
     {
        // (b) Red-proof: a failed restore's OUTPUT is still routed through the same
        // classification the attached run path uses — proving this isn't a bare
        // "restore failed" catch-all that swallows the state-db/install-lock/
        // cache-corruption signatures `spawn_and_await_running`'s retry logic
        // depends on.
        let dir = unique_test_dir("restore-state-db-classified");
        let name = "rz-restore-state-db";
        let script = write_fake_msb_for_restore(
            &dir,
            name,
            1,
            "error: database error: Execution Error: index idx already exists",
            &["Running"],
        );
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());

        let err =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap())
                .expect_err("a nonzero restore exit must never be classified as success");
        assert!(
            matches!(err, PreRunningFailure::StateDbError { .. }),
            "a restore exit whose output carries msb's state-database error signature must \
             classify as `StateDbError`, the same as it would for `run`, not fall through to \
             a generic failure"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_exiting_nonzero_with_no_known_signature_surfaces_a_plain_classified_failure() {
        // (b) continued, through the public `spawn_and_await_running` entry point:
        // an ordinary (unrecognized) restore failure must surface as an actionable
        // `Err` naming the sandbox and quoting msb's own output, not hang or panic.
        let dir = unique_test_dir("restore-plain-failure");
        let name = "rz-restore-plain-failure";
        let script = write_fake_msb_for_restore(
            &dir,
            name,
            1,
            "error: snapshot corrupt: bad magic bytes",
            &["Running"],
        );
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());

        let err = spawn_and_await_running(&script, &spec)
            .expect_err("a restore that fails to activate at all must surface as an Err");
        let msg = err.to_string();
        assert!(msg.contains(name), "{msg}");
        assert!(msg.contains("bad magic bytes"), "{msg}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_whose_sandbox_reaches_stopped_fails_fast_instead_of_hanging_out_the_budget() {
        // (c) Red-proof: `restore` exits 0 (a successful activation), but the
        // sandbox it started reaches `Stopped` on the very first poll instead of
        // `Running` — a genuine background-boot failure. This must be classified
        // as a boot failure IMMEDIATELY, not by waiting out
        // `FIRST_RUN_TIMEOUT` (600s, far too long for this test to actually wait
        // on) — asserting a tight wall-clock bound is what proves this is a fast
        // failure, not a disguised hang.
        let dir = unique_test_dir("restore-stopped-fast-fail");
        let name = "rz-restore-stopped";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Stopped"]);
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());

        let started = std::time::Instant::now();
        let err =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap())
                .expect_err("a sandbox that reaches Stopped instead of Running has failed to boot");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a Stopped sandbox must fail the poll immediately, not run out the boot budget: \
             took {:?}",
            started.elapsed()
        );
        let msg = match err {
            PreRunningFailure::Other(e) => e.to_string(),
            other => panic!("expected a plain boot-failure error, got {other:?}"),
        };
        assert!(msg.contains(name), "{msg}");
        assert!(msg.contains("Stopped"), "{msg}");
        assert!(
            msg.contains("fake system log tail"),
            "the Stopped-boot-failure message must carry the `msb logs --source system` \
             diagnostic: {msg}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_whose_sandbox_disappears_from_ls_fails_fast_instead_of_hanging_out_the_budget() {
        // (c) continued: the sandbox dropping out of `msb ls` entirely (rather
        // than showing up `Stopped`) must be treated the same way — a definite
        // failure, fast, not a hang.
        let dir = unique_test_dir("restore-absent-fast-fail");
        let name = "rz-restore-absent";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["ABSENT"]);
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());

        let started = std::time::Instant::now();
        let err =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap())
                .expect_err("a sandbox absent from `ls` entirely has failed to boot");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "an absent sandbox must fail the poll immediately, not run out the boot budget: \
             took {:?}",
            started.elapsed()
        );
        let msg = match err {
            PreRunningFailure::Other(e) => e.to_string(),
            other => panic!("expected a plain boot-failure error, got {other:?}"),
        };
        assert!(msg.contains(name), "{msg}");
        assert!(msg.contains("dropped out of"), "{msg}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- workload revival: phase 3's exec argv, env, and the typed no-command-
    // no-capture refusal (the round's own red-proofs (a)-(e)) -------------------

    #[cfg(unix)]
    #[test]
    fn restore_with_an_explicit_command_spawns_the_workload_exec_with_it_and_the_dash_e_env_pairs()
    {
        // (a) Red-proof: an explicit `spec.command` always wins, and every env
        // pair reaches the exec as a repeated `-e KEY=value` flag — the channel
        // `restore` itself has no flag for (see `commands::exec_workload`'s doc).
        let dir = unique_test_dir("restore-explicit-command-exec-argv");
        let name = "rz-restore-explicit-command";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Running"]);
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());
        spec.command = Some(vec![
            "redis-server".to_string(),
            "--port".to_string(),
            "6379".to_string(),
        ]);
        spec.env = vec![
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "2".to_string()),
        ];

        let mut child =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap())
                .expect("a restore reaching Running with an explicit command must succeed")
                .expect("phase 3 must hand back the workload exec as this boot's live child");

        let exec_calls = std::fs::read_to_string(dir.join("exec-calls")).unwrap();
        assert_eq!(
            exec_calls.trim(),
            format!("-e A=1 -e B=2 {name} -- redis-server --port 6379"),
            "exec's argv must carry -e pairs (in spec.env order) before the name, then -- and \
             the explicit command, exactly as commands::exec_workload builds it"
        );

        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_with_no_command_uses_the_captured_cmdline_field_when_present() {
        // (b) Red-proof: no explicit command, but `checkpoint_captured_cmdline`
        // is set (as a restore of a named checkpoint's registry entry — or this
        // same round's own re-boot — would leave it) — the exec must use that
        // captured argv.
        let dir = unique_test_dir("restore-captured-cmdline-argv");
        let name = "rz-restore-captured-cmdline";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Running"]);
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());
        assert!(
            spec.command.is_none(),
            "this red-proof is specifically the no-command case"
        );
        spec.checkpoint_captured_cmdline = Some(vec!["node".to_string(), "server.js".to_string()]);

        let mut child =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap())
                .expect("a restore reaching Running with a captured cmdline must succeed")
                .expect("phase 3 must hand back the workload exec as this boot's live child");

        let exec_calls = std::fs::read_to_string(dir.join("exec-calls")).unwrap();
        assert_eq!(exec_calls.trim(), format!("{name} -- node server.js"));

        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_with_neither_a_command_nor_a_captured_cmdline_fails_typed_instead_of_booting_idle() {
        // (c) Red-proof: an old checkpoint (predating workload capture) has
        // neither `command` nor `checkpoint_captured_cmdline` — this MUST fail
        // the restore with a typed, explanatory error rather than silently
        // leaving the sandbox running idle (the very bug this round fixes).
        let dir = unique_test_dir("restore-no-command-no-capture");
        let name = "rz-restore-no-command-no-capture";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Running"]);
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());
        assert!(spec.command.is_none());
        assert!(spec.checkpoint_captured_cmdline.is_none());

        let err =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap())
                .expect_err("neither command nor captured cmdline must refuse, not boot idle");
        let msg = match err {
            PreRunningFailure::Other(e) => e.to_string(),
            other => panic!("expected a plain typed failure, got {other:?}"),
        };
        assert!(msg.contains(name), "{msg}");
        assert!(msg.contains("predates workload capture"), "{msg}");
        assert!(
            !dir.join("exec-calls").exists(),
            "no exec must ever be attempted when there is nothing to run"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn workload_exec_exiting_nonzero_quickly_is_a_classified_boot_failure() {
        // (e) Red-proof: the revived workload exec exiting nonzero right away
        // (a typo'd binary, a permission error) must surface as a classified
        // boot failure carrying its output, not be mistaken for a successful
        // restore just because `restore` itself and the `Running` poll both
        // already succeeded.
        let dir = unique_test_dir("restore-workload-exec-early-nonzero");
        let name = "rz-restore-exec-early-fail";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Running"]);
        std::fs::write(dir.join("exec-exit-code"), "127").unwrap();
        std::fs::write(dir.join("exec-stderr"), "sh: nope: not found\n").unwrap();
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());
        spec.command = Some(vec!["nope".to_string()]);

        let err =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap())
                .expect_err("an exec that exits nonzero right away must not be treated as success");
        let msg = match err {
            PreRunningFailure::Other(e) => e.to_string(),
            other => panic!("expected a plain classified failure, got {other:?}"),
        };
        assert!(msg.contains(name), "{msg}");
        assert!(msg.contains("code 127"), "{msg}");
        assert!(
            msg.contains("not found"),
            "the exec's own output must be attached: {msg}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn workload_exec_exiting_zero_quickly_succeeds_when_the_sandbox_state_confirms_completion() {
        // Requirement 3's other half: a workload exec that finishes fast AND
        // clean is judged exactly like an attached `run` child's own fast-exit
        // case (`fast_exit_ran_to_completion`) — success, not a failure, when
        // the sandbox's own state backs it up.
        let dir = unique_test_dir("restore-workload-exec-fast-exit-zero");
        let name = "rz-restore-exec-fast-exit-zero";
        // Only ONE `ls` call is needed to observe `Running` in phase 2 (this
        // sandbox's status never changes again); the SECOND `ls` call is
        // `fast_exit_ran_to_completion`'s own `Stopped` check.
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Running", "Stopped"]);
        std::fs::write(dir.join("exec-exit-code"), "0").unwrap();
        std::fs::write(
            dir.join("system-log-marker"),
            format!("{SANDBOX_STARTED_MARKER}\n"),
        )
        .unwrap();
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());
        spec.command = Some(vec!["true".to_string()]);

        let child =
            try_restore_and_await_running(&script, &spec, spec.checkpoint_ref.as_ref().unwrap())
                .expect(
                    "a workload that exits 0 and whose sandbox confirms completion must succeed",
                );
        assert!(
            child.is_some(),
            "even an already-exited fast-exit child is still handed back, matching the attached \
             run path's own fast-exit contract"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_access_denied_once_then_success_boots_with_exactly_two_restore_invocations() {
        // (d) Red-proof: the Windows post-teardown "Access is denied" transient
        // is retried exactly once — a first `restore` invocation that hits it,
        // then a second that succeeds, must still boot successfully, having
        // invoked `restore` exactly twice (never more, never falling back to a
        // generic failure).
        let dir = unique_test_dir("restore-access-denied-once-then-success");
        let name = "rz-restore-access-denied";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Running"]);
        std::fs::write(dir.join("access-denied-remaining"), "1").unwrap();
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());
        spec.command = Some(vec!["true".to_string()]);

        let mut child = spawn_and_await_running(&script, &spec)
            .expect("one access-denied hit followed by a clean retry must still boot")
            .expect("phase 3 must hand back the workload exec as this boot's live child");

        let restore_calls: u32 = std::fs::read_to_string(dir.join("restore-calls"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            restore_calls, 2,
            "exactly one retry: the first attempt hit the transient, the second succeeded"
        );

        let _ = child.kill();
        let _ = child.wait();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn restore_access_denied_twice_in_a_row_surfaces_a_real_error_not_an_infinite_retry() {
        let dir = unique_test_dir("restore-access-denied-twice");
        let name = "rz-restore-access-denied-twice";
        let script = write_fake_msb_for_restore(&dir, name, 0, "", &["Running"]);
        std::fs::write(dir.join("access-denied-remaining"), "99").unwrap();
        let mut spec = ContainerSpec::new(name, "unused-image", "run-1");
        spec.checkpoint_ref = Some(dir.join("snap_fake").display().to_string());

        let err = spawn_and_await_running(&script, &spec)
            .expect_err("a persistent access-denied failure must not retry forever");
        let msg = err.to_string();
        assert!(msg.contains(name), "{msg}");
        assert!(msg.contains("Access is denied"), "{msg}");

        let restore_calls: u32 = std::fs::read_to_string(dir.join("restore-calls"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_eq!(
            restore_calls, 2,
            "exactly one retry, then give up: {restore_calls}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn is_restore_access_denied_matches_the_captured_windows_wording() {
        assert!(is_restore_access_denied(
            "error: io error: Access is denied. (os error 5)"
        ));
        assert!(is_restore_access_denied("Access is denied. (os error 5)"));
    }

    #[test]
    fn is_restore_access_denied_negative_cases_do_not_match() {
        assert!(!is_restore_access_denied(
            "error: snapshot corrupt: bad magic bytes"
        ));
        assert!(
            !is_restore_access_denied("Access is denied."),
            "no io-error/os-error-5 co-signal"
        );
        assert!(!is_restore_access_denied(
            "io error: something else (os error 13)"
        ));
    }
}
