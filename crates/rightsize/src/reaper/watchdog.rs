//! The per-run watchdog: a small detached script under `<cache_dir>/reaper/` that
//! blocks reading stdin and, on EOF (this process exited — cleanly or via SIGKILL),
//! best-effort removes this run's sandboxes/networks via backend-CLI commands and
//! deletes the run's ledger files.
//!
//! **The critical correctness detail** (per the feature spec): the pipe's write end
//! must be held ONLY by this process and never inherited by any other child this
//! process spawns — a leaked write fd (e.g. a duplicate inherited by an `msb`/`docker`
//! child process this library itself spawns) means the watchdog's stdin never reaches
//! EOF, and it never fires. This crate is `#![forbid(unsafe_code)]` with no `libc`
//! dependency, so there is no direct `fcntl`/`CreateProcess`-flag call available here —
//! this relies on `std::process::Command`'s own documented behavior of marking the
//! pipe ends it creates and keeps on the PARENT side as close-on-exec, which is exactly
//! the property this needs (every other child this process spawns goes through
//! `std::process::Command` too, backend CLI subprocesses included).
//!
//! The spawned [`std::process::Child`] (specifically its `stdin`) is kept alive in a
//! process-wide static for the rest of this process's lifetime — dropping it would
//! close the pipe immediately, firing the watchdog right away instead of on real
//! process death.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, OnceLock};

use crate::backend::SandboxBackend;

use super::ledger::Ledger;

/// Holds the spawned watchdog's [`std::process::Child`] alive (specifically its
/// `stdin`) for the rest of this process — see the module doc.
static WATCHDOG_CHILD: OnceLock<std::process::Child> = OnceLock::new();

/// Test-only observation point: counts every REAL `sh`/`powershell` process this
/// function has actually forked, across this test binary's entire lifetime — the
/// concurrency regression test in `crate::reaper`'s test module asserts this never
/// exceeds 1 even when many threads race `before_create` for the first time at once.
/// `#[cfg(test)]`-gated, so it adds nothing to the production binary.
#[cfg(test)]
pub(crate) static SPAWN_CALL_COUNT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Writes the watchdog script (idempotent — same content, same path, every time) and
/// spawns it detached, wired to this run's ledger files and `backend`'s CLI kill
/// commands. Best-effort: any failure (can't write the script, can't spawn) is
/// swallowed — a process that can't set up its own watchdog still gets init-time-sweep
/// coverage from whichever process starts next.
pub(crate) fn spawn(cache_dir: &Path, backend: &Arc<dyn SandboxBackend>, ledger: &Ledger) {
    let Ok(script_path) = write_script(cache_dir) else {
        return;
    };
    let sandbox_kill = tab_join(&backend.watchdog_kill_command());
    let network_kill = tab_join(&backend.watchdog_network_kill_command());

    let mut command = build_command(&script_path);
    command
        .arg(ledger.sandboxes_path())
        .arg(ledger.networks_path())
        .arg(ledger.record_path())
        .arg(&sandbox_kill)
        .arg(&network_kill)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let Ok(child) = command.spawn() else {
        return;
    };
    #[cfg(test)]
    SPAWN_CALL_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    // This function must never run more than once per process — `crate::reaper`'s
    // call site (`before_create`) gates it behind an in-process `OnceLock`, which is
    // load-bearing, not a nicety: if `spawn` ever did run twice, dropping the losing
    // `Child` here would close ITS stdin immediately, and this process's watchdog
    // script blocks reading exactly that pipe — an instant, spurious EOF would make
    // it force-remove every sandbox already in this run's ledger, including ones a
    // sibling thread still has running (see `before_create`'s doc for the concurrent
    // double-spawn this crate used to be able to hit before that gate existed). This
    // `set` should therefore always succeed; it stays `OnceLock`-guarded rather than
    // `expect`ed only so a future caller-site regression fails safe (an orphaned,
    // immediately-reaping watchdog is far worse than a merely-redundant one) instead
    // of panicking a test/production process.
    if WATCHDOG_CHILD.set(child).is_err() {
        // Should be unreachable given `before_create`'s gate — see above.
    }
}

#[cfg(unix)]
fn build_command(script_path: &Path) -> Command {
    let mut c = Command::new("sh");
    c.arg(script_path);
    // A new process group so a SIGKILL sent to this process's own group (a common CI
    // "kill the whole test process tree" pattern) doesn't also take the watchdog down
    // with it — the watchdog's entire reason to exist is to outlive that signal.
    use std::os::unix::process::CommandExt;
    c.process_group(0);
    c
}

#[cfg(windows)]
fn build_command(script_path: &Path) -> Command {
    let mut c = Command::new("powershell");
    c.args(["-NoProfile", "-NonInteractive", "-File"]);
    c.arg(script_path);
    c
}

/// TAB-joins `words` into one argv slot — the watchdog script re-splits on TAB via
/// `IFS` right before invoking, which lets an individual word (e.g. msb's kill
/// command, which embeds a small `sh -c` script containing spaces) survive intact
/// while still letting the parent process pass a variable-length command as a single,
/// exact `std::process::Command` argv element (no shell involved in that hop, so no
/// quoting/escaping concerns there).
fn tab_join(words: &[String]) -> String {
    words.join("\t")
}

/// The watchdog script's on-disk path — named by a hash of the script's own content.
/// The `reaper/` directory is shared with the sibling rightsize libraries (Kotlin,
/// TypeScript) and with other versions of this crate, each shipping its own script
/// with its own argv contract; content-derived names make it impossible for any of
/// them to execute a script whose contract it doesn't match, and impossible for a
/// rewrite to clobber a script another process's `sh` is currently executing.
fn script_path(cache_dir: &Path) -> PathBuf {
    let ext = if cfg!(windows) { "ps1" } else { "sh" };
    let name = format!(
        "watchdog-{:012x}.{ext}",
        content_fingerprint(script_contents())
    );
    cache_dir.join("reaper").join(name)
}

/// FNV-1a (64-bit, truncated to 48 bits for a 12-hex filename) — distinctness across
/// a handful of script revisions is all that's needed here, not cryptography.
fn content_fingerprint(content: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in content.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash & 0xffff_ffff_ffff
}

/// Writes the watchdog script to `<cache_dir>/reaper/` if this exact content isn't
/// already there (the filename encodes the content, so presence means identity),
/// via tmp + atomic rename so a concurrent writer can never expose a torn file.
fn write_script(cache_dir: &Path) -> std::io::Result<PathBuf> {
    let path = script_path(cache_dir);
    if path.exists() {
        return Ok(path);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    std::fs::write(&tmp, script_contents())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&tmp)?.permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&tmp, perms)?;
    }
    if let Err(err) = std::fs::rename(&tmp, &path) {
        let _ = std::fs::remove_file(&tmp);
        if !path.exists() {
            return Err(err);
        }
    }
    Ok(path)
}

#[cfg(unix)]
fn script_contents() -> &'static str {
    POSIX_SCRIPT
}

#[cfg(windows)]
fn script_contents() -> &'static str {
    POWERSHELL_SCRIPT
}

/// POSIX `sh` watchdog. argv: `$1`=sandboxes file, `$2`=networks file, `$3`=run
/// record json, `$4`=sandbox kill command (TAB-separated words; empty = nothing to
/// do), `$5`=network kill command (same shape).
#[cfg(unix)]
const POSIX_SCRIPT: &str = r#"#!/bin/sh
# rightsize orphan-reaper watchdog (POSIX). Blocks reading stdin; on EOF (the owning
# rightsize process exited, cleanly or via SIGKILL) removes that run's sandboxes and
# networks via the backend-CLI commands passed as argv, then deletes the run's ledger
# files. See docs/reaping.md.
sandboxes_file="$1"
networks_file="$2"
record_json="$3"
sandbox_kill="$4"
network_kill="$5"

# Block until stdin reaches EOF -- see crate::reaper::watchdog's doc for the
# non-inheritable pipe contract this depends on.
cat >/dev/null

tab="$(printf '\t')"

reap_lines() {
    file="$1"
    kill_cmd="$2"
    [ -n "$kill_cmd" ] || return 0
    [ -f "$file" ] || return 0
    while IFS= read -r item || [ -n "$item" ]; do
        [ -z "$item" ] && continue
        IFS="$tab"
        set -- $kill_cmd "$item"
        unset IFS
        "$@" >/dev/null 2>&1
    done < "$file"
}

reap_lines "$sandboxes_file" "$sandbox_kill"
reap_lines "$networks_file" "$network_kill"

rm -f "$sandboxes_file" "$networks_file" "$record_json"
"#;

/// PowerShell watchdog, same argv contract as the POSIX script (positional
/// parameters). Windows script correctness is exercised by the msb-windows CI job's
/// own integration test, not this crate's unit suite (no PowerShell host here).
#[cfg(windows)]
const POWERSHELL_SCRIPT: &str = r#"param(
    [string]$SandboxesFile,
    [string]$NetworksFile,
    [string]$RecordJson,
    [string]$SandboxKill,
    [string]$NetworkKill
)

# Block until stdin reaches EOF -- see crate::reaper::watchdog's doc for the
# non-inheritable pipe contract this depends on.
[Console]::In.ReadToEnd() | Out-Null

function Reap-Lines([string]$File, [string]$KillCmd) {
    if ([string]::IsNullOrEmpty($KillCmd)) { return }
    if (-not (Test-Path $File)) { return }
    $words = $KillCmd -split "`t"
    Get-Content $File | ForEach-Object {
        $item = $_.Trim()
        if ($item -ne "") {
            $argv = $words[1..($words.Length - 1)] + $item
            try { & $words[0] @argv | Out-Null } catch {}
        }
    }
}

Reap-Lines -File $SandboxesFile -KillCmd $SandboxKill
Reap-Lines -File $NetworksFile -KillCmd $NetworkKill

Remove-Item -ErrorAction SilentlyContinue $SandboxesFile, $NetworksFile, $RecordJson
"#;

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The `reaper/` dir is shared across sibling libraries and versions — the name
    /// must derive from content so differing scripts can never collide, and the same
    /// content must always map to the same (stable, reusable) filename.
    #[test]
    fn script_name_derives_from_content() {
        let name = script_path(Path::new("/tmp"))
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(
            name.starts_with("watchdog-")
                && name.ends_with(".sh")
                && name.len() == "watchdog-.sh".len() + 12,
            "unexpected script name shape: {name}"
        );
        assert_ne!(content_fingerprint("a"), content_fingerprint("b"));
        assert_eq!(
            content_fingerprint(POSIX_SCRIPT),
            content_fingerprint(POSIX_SCRIPT)
        );
    }

    fn temp_cache_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rz-watchdog-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// End-to-end proof of the POSIX script itself, independent of `spawn`/the
    /// process-wide `OnceLock` (which can only usefully run once per test binary):
    /// writes the real script, spawns `sh` against it directly with a stub "kill"
    /// recorder script standing in for a backend CLI, feeds it two sandbox names and
    /// one network id, closes stdin, and asserts the recorder was invoked with each
    /// name/id and that the ledger files were deleted.
    #[test]
    fn posix_script_invokes_the_kill_command_per_line_and_deletes_ledger_files_on_stdin_eof() {
        let cache = temp_cache_dir("e2e");
        let script = write_script(&cache).expect("write watchdog script");

        let runs_dir = cache.join("runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let sandboxes = runs_dir.join("r1.sandboxes");
        let networks = runs_dir.join("r1.networks");
        let record = runs_dir.join("r1.json");
        std::fs::write(&sandboxes, "rz-a-0\nrz-a-1\n").unwrap();
        std::fs::write(&networks, "net-1\n").unwrap();
        std::fs::write(&record, "{}").unwrap();

        // A stub "kill command": a tiny script that appends every argv it receives
        // (space-joined) as one line to a log file, standing in for the real
        // backend-CLI command the production watchdog would invoke.
        let log = cache.join("invocations.log");
        let stub = cache.join("stub-kill.sh");
        std::fs::write(
            &stub,
            format!(
                "#!/bin/sh\necho \"$@\" >> {}\n",
                shell_quote(&log.display().to_string())
            ),
        )
        .unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&stub).unwrap().permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&stub, perms).unwrap();
        }
        let kill_cmd = stub.display().to_string();

        let mut child = Command::new("sh")
            .arg(&script)
            .arg(&sandboxes)
            .arg(&networks)
            .arg(&record)
            .arg(&kill_cmd)
            .arg(&kill_cmd)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn watchdog script");

        // Close the write end -> stdin EOF -> the script proceeds.
        drop(child.stdin.take());

        let status = child.wait().expect("watchdog script must exit");
        assert!(status.success(), "watchdog script exited with {status:?}");

        let log_contents = std::fs::read_to_string(&log).unwrap_or_default();
        assert!(log_contents.contains("rz-a-0"), "{log_contents}");
        assert!(log_contents.contains("rz-a-1"), "{log_contents}");
        assert!(log_contents.contains("net-1"), "{log_contents}");

        assert!(!sandboxes.exists(), "sandboxes ledger file must be deleted");
        assert!(!networks.exists(), "networks ledger file must be deleted");
        assert!(!record.exists(), "record json must be deleted");
    }

    #[test]
    fn posix_script_with_empty_ledger_files_just_deletes_the_record_files() {
        let cache = temp_cache_dir("empty");
        let script = write_script(&cache).expect("write watchdog script");
        let runs_dir = cache.join("runs");
        std::fs::create_dir_all(&runs_dir).unwrap();
        let sandboxes = runs_dir.join("r2.sandboxes");
        let networks = runs_dir.join("r2.networks");
        let record = runs_dir.join("r2.json");
        std::fs::write(&record, "{}").unwrap();
        // sandboxes/networks intentionally never created — the clean-shutdown shape.

        let mut child = Command::new("sh")
            .arg(&script)
            .arg(&sandboxes)
            .arg(&networks)
            .arg(&record)
            .arg("true") // a no-op kill command that always succeeds if ever invoked
            .arg("")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn watchdog script");
        drop(child.stdin.take());
        let status = child.wait().unwrap();
        assert!(status.success());
        assert!(!record.exists());
    }

    fn shell_quote(s: &str) -> String {
        format!("'{}'", s.replace('\'', "'\\''"))
    }

    #[test]
    fn tab_join_preserves_a_word_containing_spaces() {
        let words = vec!["sh".to_string(), "-c".to_string(), "a b c".to_string()];
        let joined = tab_join(&words);
        let split: Vec<&str> = joined.split('\t').collect();
        assert_eq!(split, vec!["sh", "-c", "a b c"]);
    }
}
