//! Pure `msb` CLI argv construction — no process spawning here, just building the
//! argument vectors [`crate::backend::MsbCliBackend`] hands to `std::process::Command`.
//! Keeping this pure makes every flag spelling a plain data-in/data-out unit test,
//! independent of a real `msb` binary.
//!
//! **Attached mode, always** (no `-d`): microsandbox's detached mode never starts the
//! image's `ENTRYPOINT` on 0.6.2 — only attached mode does — so [`run`] never
//! emits `-d`, and the backend supervises the resulting child directly.

use std::path::Path;

use rightsize::model::{ContainerSpec, Protocol};

/// Builds the argv for `msb run`, in the pinned order: name, memory (if set),
/// root-disk (if a disk limit or tmpfs root is set), net (if network is disabled),
/// ports, env, mounts, image, then `-- <command>` iff `spec.command`
/// is `Some` — a `None` command means "run the image's default
/// `ENTRYPOINT`/`CMD`", which requires omitting the trailing `--` entirely rather
/// than passing it with no arguments after it.
///
/// A spec with `checkpoint_ref` set (built from
/// [`rightsize::Container::from_checkpoint`]) must never reach this function —
/// msb 0.7.1 removed `run --from-snapshot` entirely (clap now rejects it outright);
/// the restore path is [`restore`], a dedicated command with its own, narrower
/// flag surface. The `debug_assert!` below exists to catch a caller that forgets
/// this and routes a restore-shaped spec through `run` by mistake.
pub fn run(spec: &ContainerSpec) -> Vec<String> {
    debug_assert!(
        spec.checkpoint_ref.is_none(),
        "commands::run must never be called for a spec with checkpoint_ref set — msb 0.7.1 \
         removed `run --from-snapshot`; use commands::restore instead"
    );
    let mut argv = vec!["run".to_string(), "--name".to_string(), spec.name.clone()];

    // `msb run --help`: -m/--memory <MEMORY>, e.g. 512M/1G — right after --name.
    if let Some(mb) = spec.memory_limit_mb {
        argv.push("-m".to_string());
        argv.push(format!("{mb}M"));
    }

    // `--root-disk` covers both a size-capped writable root disk and a tmpfs one —
    // `ContainerSpec`'s own validation refuses a spec that sets both: `Container::
    // start`'s pre-flight checks, re-applied (`validate_spec_conflicts`) against
    // the FINISHED spec after any `.with_spec_customizer(...)` hook runs, so at
    // most one of these fires even when a customizer built this spec.
    if let Some(mb) = spec.disk_limit_mb {
        argv.push("--root-disk".to_string());
        argv.push(format!("{mb}M"));
    }
    if let Some(mb) = spec.tmpfs_root_mb {
        argv.push("--root-disk".to_string());
        argv.push(format!("tmpfs:{mb}M"));
    }
    if spec.network_disabled {
        argv.push("--net".to_string());
        argv.push("private".to_string());
    }

    for port in &spec.ports {
        argv.push("-p".to_string());
        argv.push(port_flag_value(port));
    }

    for (k, v) in &spec.env {
        argv.push("-e".to_string());
        argv.push(format!("{k}={v}"));
    }

    // The option block is always spelled out, never left to msb's defaults, for two
    // reasons on top of each other.
    //
    // The access token (`ro`/`rw`) carries `FileMount::read_only`, which msb enforces
    // as a genuine guest-side write block — and it keeps OUR spec parseable on
    // Windows. msb stages each mount into a temp directory and canonicalizes it,
    // which there yields the extended-length `\\?\C:\...` form; its splitter skips a
    // drive prefix only for a bare drive letter, so with no trailing option block it
    // splits at the drive's colon and rejects the rest of the path as options.
    //
    // `nodev` exists because msb then rebuilds an INTERNAL `tag:staged_path[:opts]`
    // spec for the same mount, carrying over only the non-default option tokens —
    // `rw` is its default and is dropped, which on Windows strips the internal spec's
    // option block and re-creates the exact same drive-colon misparse one layer down
    // (captured: `--mount "fm_…:\\?\C:\…": expected flag or key=value option`).
    // `nodev` always survives the carry-over, and for a single-file mount it is
    // meaningless (no device nodes to block): verified against a real msb 0.6.8 —
    // `rw,nodev` mounts `rw,nodev` and accepts an in-guest write, `ro,nodev` rejects
    // one with `Read-only file system`.
    for mount in &spec.mounts {
        argv.push("--mount-file".to_string());
        argv.push(format!(
            "{}:{}:{},nodev",
            mount.host_path.display(),
            mount.guest_path,
            if mount.read_only { "ro" } else { "rw" }
        ));
    }

    argv.push(spec.image.clone());

    if let Some(command) = &spec.command {
        argv.push("--".to_string());
        argv.extend(command.iter().cloned());
    }

    argv
}

/// Builds the argv for `msb restore <path> --name <name> ...` — the checkpoint
/// restore path on msb 0.7.1+, which replaced `msb run --from-snapshot <ref>`
/// with a dedicated `restore` subcommand (upstream removed `--from-snapshot`
/// outright; see the crate `CHANGELOG`). Used for both re-boot paths that resume a
/// disk snapshot under the same name: the checkpoint feature's own stop → snapshot
/// → rm → re-boot cycle (`MsbCliBackend::create_checkpoint`), and an ordinary
/// [`rightsize::Container::from_checkpoint`] restore — both funnel through the same
/// `spawn_and_await_running`/`try_spawn_and_await_running` call site in
/// `crate::backend`, which is what decides `run` vs `restore` by checking
/// `spec.checkpoint_ref`.
///
/// `snapshot_path` is the absolute path to the snapshot artifact — the same
/// string `spec.checkpoint_ref` carries — passed as restore's
/// `SNAPSHOT-OR-ARCHIVE-PATH` positional, exactly where `--from-snapshot` used to
/// take it.
///
/// Never passes `--disk-only`: this backend's own `--from-sandbox`/`--dest-dir`
/// snapshot create always produces a DISK-scope snapshot (never a full
/// RAM+processes one), and msb 0.7.1 REJECTS `--disk-only` against a disk-scope
/// snapshot outright (`invalid config: disk_only requires a full snapshot with
/// checkpoint state`, verified live) — restoring one is inherently a cold boot of
/// the captured disk already, the same semantics `run --from-snapshot` and the
/// old, now-removed `--disk-only` flag both used to spell out explicitly.
///
/// Carries over `-p` port mappings and `-m` memory (if set), matching [`run`].
/// Deliberately does NOT carry over:
/// - **env** — `restore` itself has no `-e`/`--env` flag at all, so `spec.env`
///   never reaches THIS argv. That no longer means the restored workload never
///   sees it, though: `restore` only brings the guest agent up (verified live —
///   the captured/default workload command does NOT re-run on its own), and
///   [`crate::backend::try_restore_and_await_running`]'s own phase 3 re-starts
///   the workload right after via [`exec_workload`], which DOES carry `spec.env`
///   as repeated `-e` flags. `Container::start()`, one layer up, still refuses a
///   `Container::from_checkpoint(...)` restore whose final `env` no longer
///   matches the checkpoint's own captured one (a genuine `.with_env`/
///   `.remove_env` override, not just a replay) with a typed
///   `RightsizeError::UnsupportedByBackend` — that gate is about `restore`'s own
///   disk-scope replay having no way to honor a CHANGED env, not about env
///   reaching the workload at all.
/// - **mounts** — `--mount-file` has no restore equivalent; restore's `-v`/
///   `--volume` is a different, unrelated concept (selecting a captured private
///   disk, or binding an external source), not this backend's host-file bind
///   mount. `Container::from_checkpoint` never carries a checkpoint's own mounts
///   over in the first place (they're already baked into the captured disk), and
///   no caller-added mount on a restored container was ever exercised by a test
///   before this migration.
/// - **`--net private`** — restore has no equivalent "profile" flag; `run`'s
///   `--net private` and restore's `--no-net` are different policies (see
///   `Container::with_network_disabled`'s doc), and `network_disabled` is not one
///   of the fields `Container::from_checkpoint` carries over either.
/// - **`--root-disk`** — restore has no such flag at all; the snapshot pins its
///   own root-disk geometry. This matches the OLD behavior in spirit: msb itself
///   already rejected `--root-disk` combined with `--from-snapshot` at its own CLI
///   layer (see `Container::with_disk_limit`/`with_tmpfs_root`'s docs), so this
///   was never a combination a caller could rely on either.
pub fn restore(spec: &ContainerSpec, snapshot_path: &str) -> Vec<String> {
    let mut argv = vec![
        "restore".to_string(),
        snapshot_path.to_string(),
        "--name".to_string(),
        spec.name.clone(),
    ];

    if let Some(mb) = spec.memory_limit_mb {
        argv.push("-m".to_string());
        argv.push(format!("{mb}M"));
    }

    for port in &spec.ports {
        argv.push("-p".to_string());
        argv.push(port_flag_value(port));
    }

    argv
}

/// Formats one `-p` flag's value for [`run`]/[`restore`]: `HOST:GUEST` for a TCP
/// binding, `HOST:GUEST/udp` for a UDP one — msb's own convention (mirroring
/// `-p`'s Docker-CLI-alike spelling) for tagging a published port's transport.
/// TCP carries no suffix, so a spec that never declares a UDP port (every spec
/// built before UDP exposure existed, and the overwhelming majority since)
/// produces byte-identical argv to before this function existed.
fn port_flag_value(port: &rightsize::model::PortBinding) -> String {
    match port.protocol {
        Protocol::Tcp => format!("{}:{}", port.host_port, port.guest_port),
        Protocol::Udp => format!("{}:{}/udp", port.host_port, port.guest_port),
    }
}

/// Builds the argv for `msb copy <src> <name>:<dst>` — copying a host file or
/// directory INTO a running sandbox.
pub fn copy_in(host_path: &Path, sandbox_name: &str, container_path: &str) -> Vec<String> {
    vec![
        "copy".to_string(),
        "-q".to_string(),
        host_path.display().to_string(),
        format!("{sandbox_name}:{container_path}"),
    ]
}

/// Builds the argv for `msb copy <name>:<src> <dst>` — copying a file or directory
/// OUT of a running sandbox to the host.
pub fn copy_out(sandbox_name: &str, container_path: &str, host_path: &Path) -> Vec<String> {
    vec![
        "copy".to_string(),
        "-q".to_string(),
        format!("{sandbox_name}:{container_path}"),
        host_path.display().to_string(),
    ]
}

/// Builds the argv for `msb snapshot create --from-sandbox <sandbox> <snapshot>` — requires
/// the sandbox to be STOPPED first (the checkpoint feature's own responsibility;
/// this function only builds the argv). The plain 2-arg form msb's own default
/// snapshot store uses; see [`snapshot_create_in`] for the dest-dir variant.
pub fn snapshot_create(sandbox_name: &str, snapshot_name: &str) -> Vec<String> {
    vec![
        "snapshot".to_string(),
        "create".to_string(),
        "--from-sandbox".to_string(),
        sandbox_name.to_string(),
        snapshot_name.to_string(),
    ]
}

/// [`snapshot_create`]'s dest-dir counterpart: appends `--dest-dir <dest_dir>`,
/// storing the snapshot artifact under that directory instead of msb's own
/// default snapshot store — the checkpoint feature's dest-dir mechanics
/// (`MsbCliBackend::create_checkpoint`).
pub fn snapshot_create_in(sandbox_name: &str, snapshot_name: &str, dest_dir: &Path) -> Vec<String> {
    let mut argv = snapshot_create(sandbox_name, snapshot_name);
    argv.push("--dest-dir".to_string());
    argv.push(dest_dir.display().to_string());
    argv
}

/// Builds the argv for `msb snapshot rm <snapshot> -f` — the checkpoint feature's
/// cleanup primitive (`SandboxBackend::remove_checkpoint`).
///
/// `-f` is verified live as part of the working invocation, not carried over
/// from an older spelling — pass it. Separately (and `-f` does NOT change
/// this): msb 0.7.1's dest-dir disk-scope snapshots resolve `rm` (and
/// `inspect`) by their own ARTIFACT PATH only — a bare name or a
/// `group:member` spelling does not resolve at all (verified live) — so
/// `snapshot_name` here is expected to be that path for any ref this backend
/// minted after the dest-dir migration, a bare legacy name otherwise (see
/// `crate::backend::path_ref_dir`'s doc). msb also refuses to remove the
/// current HEAD of a group with older siblings from the same source sandbox
/// even with `-f` (`invalid config: cannot remove current head snap_...; first
/// select another snapshot with 'msb snapshot head src:<snapshot>'`, verified
/// live) — this backend does not attempt automatic head rotation to work
/// around that (see `MsbCliBackend::remove_checkpoint`'s doc and the
/// checkpoints docs' "Cleanup" section for the resulting limitation).
pub fn snapshot_rm(snapshot_name: &str) -> Vec<String> {
    vec![
        "snapshot".to_string(),
        "rm".to_string(),
        snapshot_name.to_string(),
        "-f".to_string(),
    ]
}

/// Builds the argv for `msb snapshot inspect <snapshot>` — the named-checkpoint
/// existence probe (`SandboxBackend::has_checkpoint`): exit 0 means the snapshot
/// still exists. Only reached for a bare legacy ref (a path ref is probed on the
/// filesystem instead, never via this command — see
/// `crate::backend::MsbCliBackend::has_checkpoint`'s doc) — worth calling out
/// because, same as [`snapshot_rm`], msb 0.7.1 resolves a dest-dir disk-scope
/// snapshot by its own artifact path only; a name or `group:member` spelling
/// does not resolve (verified live).
pub fn snapshot_inspect(snapshot_name: &str) -> Vec<String> {
    vec![
        "snapshot".to_string(),
        "inspect".to_string(),
        snapshot_name.to_string(),
    ]
}

/// Builds the argv for `msb snapshot save <ref> <dest>` — the checkpoint-archive
/// feature's export primitive (`SandboxBackend::export_checkpoint`). Deliberately
/// never includes `--with-image`: its import fails an integrity check ("raw
/// manifest digest mismatch") on msb 0.6.6, so archives never bundle the OCI
/// image — the destination machine pulls it fresh on the restored sandbox's first
/// boot instead.
pub fn snapshot_export(snapshot_ref: &str, dest: &Path) -> Vec<String> {
    vec![
        "snapshot".to_string(),
        "save".to_string(),
        snapshot_ref.to_string(),
        dest.display().to_string(),
    ]
}

/// Builds the argv for `msb snapshot load <archive> --dest <dest_dir>` — the
/// checkpoint-archive feature's import primitive (`SandboxBackend::import_checkpoint`).
/// `--dest` is ALWAYS passed, pointed at this backend's own checkpoints directory
/// (`MsbCliBackend::import_checkpoint`'s own `<cache_dir>/checkpoints` — the same
/// tree [`snapshot_create_in`] writes a created checkpoint under) rather than left
/// to msb's own default (global) snapshot store, so an imported artifact's ref
/// lands under the same tree a created one's does. msb's import is
/// content-addressed — the archive's own original ref plays no role in where the
/// artifact lands, only in what msb prints back on success: a `group msb-<hex>:
/// head snap_<digest> (Initialized)` line, a digest line, then the loaded
/// artifact's own absolute path as the LAST line (verified live against msb
/// 0.7.1) — see `MsbCliBackend::import_checkpoint`'s doc for how that printed path
/// becomes the returned ref, the same "parse the last line, require it absolute"
/// contract [`snapshot_create`]'s own ref already uses.
pub fn snapshot_import(archive_path: &Path, dest_dir: &Path) -> Vec<String> {
    vec![
        "snapshot".to_string(),
        "load".to_string(),
        archive_path.display().to_string(),
        "--dest".to_string(),
        dest_dir.display().to_string(),
    ]
}

/// Builds the argv for a plain (non-streaming) `msb exec`.
pub fn exec(name: &str, cmd: &[String]) -> Vec<String> {
    let mut argv = vec!["exec".to_string(), name.to_string(), "--".to_string()];
    argv.extend(cmd.iter().cloned());
    argv
}

/// Builds the argv for `msb exec [-e KEY=value]... <name> -- <argv...>` — the
/// workload-revival exec [`crate::backend::try_restore_and_await_running`] spawns
/// as a LONG-LIVED attached child once a checkpoint restore reaches `Running`.
/// Upstream's own `restore` (msb 0.7.1+) boots the sandbox with only `agentd`
/// inside — the captured workload command never re-runs on its own, verified live
/// — so this backend re-starts it itself, the same role a `run`'s trailing `--
/// <command>` plays for an ordinary boot, but through `exec` since the sandbox is
/// already running.
///
/// `env` pairs are emitted as repeated `-e KEY=value` flags, BEFORE the sandbox
/// name (`msb exec --help`: `-e, --env <ENV>`, repeatable — same flag spelling as
/// [`run`]'s own env, same "flags before the positional name" placement
/// [`exec_stream`]'s `--stream` already uses). This is how a checkpoint's captured
/// env reaches the revived workload even though [`restore`] itself has no
/// `-e`/`--env` flag at all — see that function's own doc for why `restore` can't
/// carry it and this can.
pub fn exec_workload(name: &str, env: &[(String, String)], cmd: &[String]) -> Vec<String> {
    let mut argv = vec!["exec".to_string()];
    for (k, v) in env {
        argv.push("-e".to_string());
        argv.push(format!("{k}={v}"));
    }
    argv.push(name.to_string());
    argv.push("--".to_string());
    argv.extend(cmd.iter().cloned());
    argv
}

/// Builds the argv for the guest cmdline capture msb `exec`:
/// `msb exec <name> -- sh -c '<CAPTURE_CMDLINE_SCRIPT>'`, run once by
/// `MsbCliBackend::create_checkpoint` — via `msb_checkpoint_cycle` — right BEFORE
/// stopping the source sandbox, and only when the checkpoint's own spec has no
/// explicit `command` (see [`CAPTURE_CMDLINE_SCRIPT`]'s own doc for what the
/// script does and [`parse_captured_cmdline`] for how its stdout is read back).
pub fn capture_workload_cmdline(name: &str) -> Vec<String> {
    exec(
        name,
        &[
            "sh".to_string(),
            "-c".to_string(),
            CAPTURE_CMDLINE_SCRIPT.to_string(),
        ],
    )
}

/// A small POSIX-`sh` script, deliberately using no non-`sh`-builtin tool beyond
/// `cat` and `sed` (both present on the busybox-based guest images `msb` boots —
/// no `awk`/`cut`/`pgrep`/`ps` assumed), that finds the first non-kernel child of
/// PID 1 and prints its `/proc/<pid>/cmdline` — the workload command a checkpoint
/// with no explicit `command` was actually running, straight from the guest's own
/// process table, since that's the only place it still exists once the container
/// was booted from the image's default entrypoint rather than a caller-supplied
/// override.
///
/// For each numeric `/proc/<pid>`: reads `stat`, extracts `comm` (the text between
/// the FIRST `(` and the LAST `)` — the kernel's own convention for a name that may
/// itself contain spaces or parens) and, from what follows it, the `ppid` field
/// (`/proc/<pid>/stat`'s field 4 in the conventional 1-indexed numbering: pid, comm,
/// state, ppid, ...). Skips `init.krun` (msb's own guest init, the direct parent of
/// everything else including this exec's own shell), anything whose `comm` is
/// itself bracketed (`[kworker/0:1]`-style — the kernel's own convention for marking
/// a kernel thread, as opposed to `init.krun`'s plain unbracketed name), and — before
/// either of those — the script's OWN pid (`/proc/$$`, checked first so a self-match
/// never even reads its own `stat`). That self-exclusion matters because this script
/// runs as `msb exec <name> -- sh -c '<script>'`: it is itself injected as a new
/// child of PID 1 into the very process set it is walking, and the plain `for d in
/// /proc/[0-9]*` glob visits pids in lexicographic string order, not spawn order — so
/// without the exclusion this shell could discover itself before the real workload
/// and capture its own `sh -c <script>` invocation instead. None of the three
/// exclusions is ever the workload. The first pid whose `ppid` is `1` and that
/// survives all three is printed via `cat /proc/<pid>/cmdline`, NUL-separated exactly
/// as the kernel writes it (never re-joined or re-quoted, so [`parse_captured_cmdline`]
/// can split on `\0` byte-for-byte); `exec` hands the whole script process over to
/// `cat` so its exit status is `cat`'s.
///
/// Not itself a byte-for-byte replacement for `ps`/`pgrep` (skips setuid concerns,
/// multiple non-kernel children, zombies) — deliberately minimal for exactly the
/// shape a checkpointed container's guest actually has: `init.krun`, some kernel
/// threads, and ONE workload process tree, since `ContainerSpec` never runs more
/// than one top-level command to begin with.
pub const CAPTURE_CMDLINE_SCRIPT: &str = concat!(
    "for d in /proc/[0-9]*; do ",
    "[ \"$d\" = \"/proc/$$\" ] && continue; ",
    "[ -r \"$d/stat\" ] || continue; ",
    "stat=$(cat \"$d/stat\") || continue; ",
    "comm=$(printf '%s' \"$stat\" | sed -n 's/^[0-9]*[[:space:]]*(\\(.*\\))[[:space:]].*/\\1/p'); ",
    "case \"$comm\" in ",
    "init.krun|'['*']') continue ;; ",
    "esac; ",
    "rest=$(printf '%s' \"$stat\" | sed 's/^[0-9]*[[:space:]]*(.*)[[:space:]]*//'); ",
    "set -- $rest; ",
    "ppid=$2; ",
    "if [ \"$ppid\" = \"1\" ]; then ",
    "pid=${d#/proc/}; ",
    "exec cat \"/proc/$pid/cmdline\"; ",
    "fi; ",
    "done",
);

/// Parses [`CAPTURE_CMDLINE_SCRIPT`]'s stdout back into a `Vec<String>`. The exec
/// invocation that captured it (`MsbCliBackend::invoke`/`invoke_standalone`, which
/// this always goes through) buffers output a LINE at a time and re-appends a `\n`
/// after whatever it captured — including the final flush at EOF, which is what
/// this sees for output that (like the script's own, NUL-separated) never
/// contained a `\n` to begin with — so a single trailing `\n` is stripped first,
/// never required.
///
/// What's left is split on `\0`, exactly as the kernel wrote
/// `/proc/<pid>/cmdline` — every `argv` element NUL-terminated, including the
/// last, so splitting always yields one trailing empty segment that's dropped
/// (never more than one: a real empty-string argument elsewhere in `argv` is
/// preserved). `None` for anything that isn't a genuine, non-empty argv after
/// that — empty output (no matching process; the script found nothing) or a bare
/// NUL terminator with nothing before it.
pub fn parse_captured_cmdline(stdout: &str) -> Option<Vec<String>> {
    let trimmed = stdout.strip_suffix('\n').unwrap_or(stdout);
    let mut argv: Vec<String> = trimmed.split('\0').map(str::to_string).collect();
    if argv.last().is_some_and(String::is_empty) {
        argv.pop();
    }
    if argv.is_empty() { None } else { Some(argv) }
}

/// Builds the argv for `msb exec --stream` — the only guest data path microsandbox
/// exposes, used exclusively by the exec-tunnel network-link emulation.
pub fn exec_stream(name: &str, cmd: &[String]) -> Vec<String> {
    let mut argv = vec![
        "exec".to_string(),
        "--stream".to_string(),
        name.to_string(),
        "--".to_string(),
    ];
    argv.extend(cmd.iter().cloned());
    argv
}

/// Builds the argv for a one-shot logs fetch (the last 1000 lines).
pub fn logs(name: &str) -> Vec<String> {
    vec![
        "logs".to_string(),
        name.to_string(),
        "--tail".to_string(),
        "1000".to_string(),
    ]
}

/// Builds the argv for a following logs stream. Never exits on its own once the
/// sandbox stops — the backend's watchdog is what reclaims this child.
pub fn follow_logs(name: &str) -> Vec<String> {
    vec!["logs".to_string(), name.to_string(), "-f".to_string()]
}

/// Builds the argv for a one-shot SYSTEM-log fetch (`--source system`, the last 1000
/// lines) — [`logs`]'s sibling, pointed at msb's own boot/lifecycle log instead of the
/// workload's. Used only by the backend's post-mortem fast-exit classification: when
/// an attached `msb run` child exits 0 before this backend ever observes `Running`,
/// the system log's boot-completion marker line (written only once the guest agent
/// has actually come up) is what distinguishes a workload that ran to completion from
/// a genuinely dead boot.
pub fn logs_system(name: &str) -> Vec<String> {
    vec![
        "logs".to_string(),
        name.to_string(),
        "--source".to_string(),
        "system".to_string(),
        "--tail".to_string(),
        "1000".to_string(),
    ]
}

/// Builds the argv for `msb stop`.
pub fn stop(name: &str) -> Vec<String> {
    vec!["stop".to_string(), name.to_string()]
}

/// Builds the argv for `msb rm`.
pub fn rm(name: &str) -> Vec<String> {
    vec!["rm".to_string(), name.to_string()]
}

/// Builds the argv for listing sandboxes as JSON. Note: no `--json` flag exists on
/// `ls` — it's `--format json`.
pub fn ls() -> Vec<String> {
    vec!["ls".to_string(), "--format".to_string(), "json".to_string()]
}

/// Builds the argv for `msb image remove <reference>` — deletes one cached image's
/// entry (manifest + layer bookkeeping) so the next `run`/`pull` re-fetches it from
/// scratch. Scoped to a single image reference; never touches sandbox state or any
/// other cached image, including ones sharing layers with this one (confirmed
/// empirically: removing one floci variant and re-pulling it left a sibling variant's
/// already-materialized shared base layer untouched and bootable).
pub fn image_remove(reference: &str) -> Vec<String> {
    vec![
        "image".to_string(),
        "remove".to_string(),
        reference.to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use rightsize::model::{FileMount, PortBinding};
    use std::path::PathBuf;

    fn full_spec() -> ContainerSpec {
        ContainerSpec {
            name: "rz-abc-1".to_string(),
            image: "redis:8.6-alpine".to_string(),
            env: vec![("A".to_string(), "1".to_string())],
            command: Some(vec![
                "redis-server".to_string(),
                "--port".to_string(),
                "6379".to_string(),
            ]),
            ports: vec![PortBinding {
                host_port: 12345,
                guest_port: 6379,
                protocol: Protocol::Tcp,
            }],
            mounts: vec![FileMount::new(
                PathBuf::from("/tmp/f.conf"),
                "/etc/f.conf".to_string(),
            )],
            network_id: Some("rz-net-1".to_string()),
            aliases: vec!["redis".to_string()],
            run_id: "abc".to_string(),
            memory_limit_mb: None,
            keep_alive: false,
            checkpoint_ref: None,
            disk_limit_mb: None,
            tmpfs_root_mb: None,
            network_disabled: false,
            checkpoint_captured_cmdline: None,
            restore_name_candidates: None,
        }
    }

    #[test]
    fn run_command_carries_all_spec_parts_attached_no_d() {
        let cmd = run(&full_spec());
        assert_eq!(
            cmd,
            vec![
                "run",
                "--name",
                "rz-abc-1",
                "-p",
                "12345:6379",
                "-e",
                "A=1",
                "--mount-file",
                "/tmp/f.conf:/etc/f.conf:rw,nodev",
                "redis:8.6-alpine",
                "--",
                "redis-server",
                "--port",
                "6379",
            ]
        );
        assert!(!cmd.contains(&"-d".to_string()));
    }

    #[test]
    fn mount_file_always_carries_an_explicit_access_token() {
        // Both spellings matter. `ro` is what makes `read_only` mean anything on this
        // backend, and an always-present token is what keeps the spec parseable on
        // Windows, where a bare `host:guest` splits at the drive letter's colon.
        let mut spec = full_spec();
        spec.mounts = vec![
            FileMount::new(PathBuf::from("/tmp/rw.conf"), "/etc/rw.conf".to_string()),
            FileMount::new(PathBuf::from("/tmp/ro.conf"), "/etc/ro.conf".to_string()).read_only(),
        ];
        let cmd = run(&spec);
        assert!(
            cmd.contains(&"/tmp/rw.conf:/etc/rw.conf:rw,nodev".to_string()),
            "{cmd:?}"
        );
        assert!(
            cmd.contains(&"/tmp/ro.conf:/etc/ro.conf:ro,nodev".to_string()),
            "{cmd:?}"
        );
        // Never a two-segment spec, whatever the flag says.
        assert!(
            !cmd.iter().any(|a| a == "/tmp/rw.conf:/etc/rw.conf"),
            "{cmd:?}"
        );
    }

    #[test]
    fn image_default_entrypoint_runs_when_command_is_none() {
        let mut spec = full_spec();
        spec.command = None;
        let cmd = run(&spec);
        // No trailing `--`: attached mode runs the image default.
        assert_eq!(cmd.last().unwrap(), "redis:8.6-alpine");
        assert!(!cmd.contains(&"--".to_string()));
    }

    #[test]
    fn run_command_includes_dash_m_when_memory_limit_is_set_absent_when_none() {
        let mut spec = full_spec();
        spec.memory_limit_mb = Some(1024);
        let with_limit = run(&spec);
        let m_index = with_limit
            .iter()
            .position(|a| a == "-m")
            .expect("expected -m flag");
        assert_eq!(with_limit[m_index + 1], "1024M");
        // -m comes right after --name, before ports/env/mounts.
        assert_eq!(with_limit[3], "-m");

        let without_limit = run(&full_spec()); // memory_limit_mb defaults to None
        assert!(!without_limit.contains(&"-m".to_string()));
    }

    #[test]
    fn run_command_includes_root_disk_when_disk_limit_is_set_absent_when_none() {
        let mut spec = full_spec();
        spec.disk_limit_mb = Some(2048);
        let with_limit = run(&spec);
        assert_eq!(
            with_limit,
            vec![
                "run",
                "--name",
                "rz-abc-1",
                "--root-disk",
                "2048M",
                "-p",
                "12345:6379",
                "-e",
                "A=1",
                "--mount-file",
                "/tmp/f.conf:/etc/f.conf:rw,nodev",
                "redis:8.6-alpine",
                "--",
                "redis-server",
                "--port",
                "6379",
            ]
        );

        let without_limit = run(&full_spec());
        assert!(!without_limit.contains(&"--root-disk".to_string()));
    }

    #[test]
    fn run_command_includes_tmpfs_root_disk_when_tmpfs_root_is_set() {
        let mut spec = full_spec();
        spec.tmpfs_root_mb = Some(512);
        let cmd = run(&spec);
        let index = cmd
            .iter()
            .position(|a| a == "--root-disk")
            .expect("expected --root-disk flag");
        assert_eq!(cmd[index + 1], "tmpfs:512M");
    }

    #[test]
    fn run_command_includes_net_private_when_network_disabled_absent_when_false() {
        let mut spec = full_spec();
        spec.network_disabled = true;
        let disabled = run(&spec);
        let index = disabled
            .iter()
            .position(|a| a == "--net")
            .expect("expected --net flag");
        assert_eq!(disabled[index + 1], "private");

        let enabled = run(&full_spec());
        assert!(!enabled.contains(&"--net".to_string()));
    }

    #[test]
    fn run_command_orders_root_disk_and_net_between_memory_and_ports() {
        let mut spec = full_spec();
        spec.memory_limit_mb = Some(1024);
        spec.disk_limit_mb = Some(2048);
        spec.network_disabled = true;
        let cmd = run(&spec);
        assert_eq!(
            cmd,
            vec![
                "run",
                "--name",
                "rz-abc-1",
                "-m",
                "1024M",
                "--root-disk",
                "2048M",
                "--net",
                "private",
                "-p",
                "12345:6379",
                "-e",
                "A=1",
                "--mount-file",
                "/tmp/f.conf:/etc/f.conf:rw,nodev",
                "redis:8.6-alpine",
                "--",
                "redis-server",
                "--port",
                "6379",
            ]
        );
    }

    #[test]
    fn exec_logs_stop_rm_ls_spellings() {
        assert_eq!(
            exec("rz-abc-1", &["redis-cli".to_string(), "ping".to_string()]),
            vec!["exec", "rz-abc-1", "--", "redis-cli", "ping"]
        );
        assert_eq!(
            exec_stream("rz-abc-1", &["nc".to_string(), "-l".to_string()]),
            vec!["exec", "--stream", "rz-abc-1", "--", "nc", "-l"]
        );
        assert_eq!(logs("rz-abc-1"), vec!["logs", "rz-abc-1", "--tail", "1000"]);
        assert_eq!(follow_logs("rz-abc-1"), vec!["logs", "rz-abc-1", "-f"]);
        assert_eq!(
            logs_system("rz-abc-1"),
            vec!["logs", "rz-abc-1", "--source", "system", "--tail", "1000"]
        );
        assert_eq!(stop("rz-abc-1"), vec!["stop", "rz-abc-1"]);
        assert_eq!(rm("rz-abc-1"), vec!["rm", "rz-abc-1"]);
        // Confirmed empirically against the real msb binary: no `--json` flag on `ls`.
        assert_eq!(ls(), vec!["ls", "--format", "json"]);
        assert_eq!(
            image_remove("floci/floci-az:0.8.0"),
            vec!["image", "remove", "floci/floci-az:0.8.0"]
        );
    }

    #[test]
    fn run_command_with_no_ports_env_or_mounts_omits_their_flags() {
        let spec = ContainerSpec::new("rz-bare-1", "alpine:3.19", "bare");
        let cmd = run(&spec);
        assert_eq!(cmd, vec!["run", "--name", "rz-bare-1", "alpine:3.19"]);
    }

    #[test]
    #[should_panic(expected = "commands::run must never be called")]
    fn run_panics_in_debug_when_checkpoint_ref_is_set() {
        let mut spec = full_spec();
        spec.checkpoint_ref = Some("/cache/checkpoints/rz-ckpt-deadbeefcafe".to_string());
        let _ = run(&spec);
    }

    #[test]
    fn restore_command_carries_name_ports_and_memory_with_no_disk_only_flag() {
        let mut spec = full_spec();
        spec.memory_limit_mb = Some(1024);
        let cmd = restore(&spec, "/cache/checkpoints/rz-ckpt-deadbeefcafe");
        assert_eq!(
            cmd,
            vec![
                "restore",
                "/cache/checkpoints/rz-ckpt-deadbeefcafe",
                "--name",
                "rz-abc-1",
                "-m",
                "1024M",
                "-p",
                "12345:6379",
            ]
        );
    }

    #[test]
    fn restore_command_omits_dash_m_when_memory_limit_is_unset() {
        let cmd = restore(&full_spec(), "/cache/checkpoints/rz-ckpt-deadbeefcafe");
        assert!(!cmd.contains(&"-m".to_string()));
        assert_eq!(cmd.last().unwrap(), "12345:6379");
    }

    #[test]
    fn restore_command_never_carries_env_mounts_net_root_disk_or_disk_only_flags() {
        // `full_spec()` sets env, a mount, and a command — none of them have a
        // restore equivalent (see `restore`'s own doc), and none may leak through.
        // `--disk-only` is checked here too: msb 0.7.1 rejects it outright against
        // the disk-scope snapshots this backend creates (verified live), so it must
        // never come back.
        let mut spec = full_spec();
        spec.network_disabled = true;
        spec.disk_limit_mb = Some(2048);
        let cmd = restore(&spec, "/cache/checkpoints/rz-ckpt-deadbeefcafe");
        for forbidden in [
            "-e",
            "A=1",
            "--mount-file",
            "--net",
            "--root-disk",
            "--disk-only",
            "--",
        ] {
            assert!(
                !cmd.iter().any(|a| a == forbidden),
                "restore argv must never contain {forbidden:?}: {cmd:?}"
            );
        }
    }

    #[test]
    fn restore_command_with_no_ports_or_memory_is_just_path_and_name() {
        let spec = ContainerSpec::new("rz-bare-1", "alpine:3.19", "bare");
        let cmd = restore(&spec, "/cache/checkpoints/rz-ckpt-bare");
        assert_eq!(
            cmd,
            vec![
                "restore",
                "/cache/checkpoints/rz-ckpt-bare",
                "--name",
                "rz-bare-1",
            ]
        );
    }

    // -- UDP port emission (spec item 7): "/udp" suffix in both run and restore --

    #[test]
    fn run_command_appends_slash_udp_for_a_udp_binding_and_leaves_tcp_unsuffixed() {
        let mut spec = full_spec();
        spec.ports.push(PortBinding {
            host_port: 40000,
            guest_port: 53,
            protocol: Protocol::Udp,
        });
        let cmd = run(&spec);
        assert!(
            cmd.windows(2).any(|w| w[0] == "-p" && w[1] == "12345:6379"),
            "the tcp binding must stay byte-identical to before UDP existed: {cmd:?}"
        );
        assert!(
            cmd.windows(2)
                .any(|w| w[0] == "-p" && w[1] == "40000:53/udp"),
            "a udp binding must emit the /udp suffix: {cmd:?}"
        );
    }

    #[test]
    fn restore_command_appends_slash_udp_for_a_udp_binding_and_leaves_tcp_unsuffixed() {
        let mut spec = full_spec();
        spec.ports.push(PortBinding {
            host_port: 40000,
            guest_port: 53,
            protocol: Protocol::Udp,
        });
        let cmd = restore(&spec, "/cache/checkpoints/rz-ckpt-deadbeefcafe");
        assert!(
            cmd.windows(2).any(|w| w[0] == "-p" && w[1] == "12345:6379"),
            "the tcp binding must stay byte-identical to before UDP existed: {cmd:?}"
        );
        assert!(
            cmd.windows(2)
                .any(|w| w[0] == "-p" && w[1] == "40000:53/udp"),
            "a udp binding must emit the /udp suffix: {cmd:?}"
        );
    }

    #[test]
    fn run_command_with_only_a_udp_port_emits_exactly_one_suffixed_flag() {
        let mut spec = ContainerSpec::new("rz-udp-only", "alpine:3.19", "run-1");
        spec.ports = vec![PortBinding {
            host_port: 51000,
            guest_port: 5353,
            protocol: Protocol::Udp,
        }];
        let cmd = run(&spec);
        assert!(
            cmd.windows(2)
                .any(|w| w[0] == "-p" && w[1] == "51000:5353/udp"),
            "{cmd:?}"
        );
        assert_eq!(
            cmd.iter().filter(|a| a.as_str() == "-p").count(),
            1,
            "{cmd:?}"
        );
    }

    #[test]
    fn copy_in_and_out_and_snapshot_spellings() {
        assert_eq!(
            copy_in(
                std::path::Path::new("/host/src.txt"),
                "rz-abc-1",
                "/guest/dst.txt"
            ),
            vec!["copy", "-q", "/host/src.txt", "rz-abc-1:/guest/dst.txt"]
        );
        assert_eq!(
            copy_out(
                "rz-abc-1",
                "/guest/src.txt",
                std::path::Path::new("/host/dst.txt")
            ),
            vec!["copy", "-q", "rz-abc-1:/guest/src.txt", "/host/dst.txt"]
        );
        assert_eq!(
            snapshot_create("rz-abc-1", "rz-ckpt-deadbeefcafe"),
            vec![
                "snapshot",
                "create",
                "--from-sandbox",
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe"
            ]
        );
        assert_eq!(
            snapshot_create_in(
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                std::path::Path::new("/cache/checkpoints")
            ),
            vec![
                "snapshot",
                "create",
                "--from-sandbox",
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                "--dest-dir",
                "/cache/checkpoints"
            ]
        );
        assert_eq!(
            snapshot_rm("rz-ckpt-deadbeefcafe"),
            vec!["snapshot", "rm", "rz-ckpt-deadbeefcafe", "-f"]
        );
        assert_eq!(
            snapshot_rm("/cache/checkpoints/rz-abc-1/snap_deadbeefcafedeadbeefcafedeadbeef"),
            vec![
                "snapshot",
                "rm",
                "/cache/checkpoints/rz-abc-1/snap_deadbeefcafedeadbeefcafedeadbeef",
                "-f"
            ],
            "an artifact-path ref must pass through unchanged — msb 0.7.1 resolves a \
             dest-dir disk-scope snapshot by its own path only"
        );
        assert_eq!(
            snapshot_inspect("rz-ckpt-deadbeefcafe"),
            vec!["snapshot", "inspect", "rz-ckpt-deadbeefcafe"]
        );
    }

    #[test]
    fn exec_workload_emits_dash_e_pairs_before_the_name_then_dash_dash_and_the_argv() {
        let cmd = exec_workload(
            "rz-abc-1",
            &[
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "2".to_string()),
            ],
            &[
                "redis-server".to_string(),
                "--port".to_string(),
                "6379".to_string(),
            ],
        );
        assert_eq!(
            cmd,
            vec![
                "exec",
                "-e",
                "A=1",
                "-e",
                "B=2",
                "rz-abc-1",
                "--",
                "redis-server",
                "--port",
                "6379",
            ]
        );
    }

    #[test]
    fn exec_workload_with_no_env_omits_dash_e_entirely() {
        let cmd = exec_workload("rz-abc-1", &[], &["true".to_string()]);
        assert_eq!(cmd, vec!["exec", "rz-abc-1", "--", "true"]);
        assert!(!cmd.contains(&"-e".to_string()));
    }

    #[test]
    fn capture_workload_cmdline_execs_sh_c_with_the_capture_script() {
        let cmd = capture_workload_cmdline("rz-abc-1");
        assert_eq!(
            cmd,
            vec!["exec", "rz-abc-1", "--", "sh", "-c", CAPTURE_CMDLINE_SCRIPT]
        );
        // Every branch this script depends on must actually be present — a
        // regression here would silently turn the capture into a no-op.
        assert!(CAPTURE_CMDLINE_SCRIPT.contains("/proc/"));
        assert!(CAPTURE_CMDLINE_SCRIPT.contains("init.krun"));
        assert!(CAPTURE_CMDLINE_SCRIPT.contains("cmdline"));
        assert!(
            CAPTURE_CMDLINE_SCRIPT.contains("/proc/$$"),
            "must exclude the script's own pid — it runs as a new sibling child of \
             PID 1 in the exact process set it walks (msb exec <name> -- sh -c \
             '<script>'), so without this the lexicographic /proc/[0-9]* glob order \
             can land on the capture script's own `sh` before the real workload"
        );
    }

    /// Regression test for the finding that the capture script can discover ITSELF
    /// instead of the real workload: it runs as `msb exec <name> -- sh -c '<script>'`,
    /// which injects a new sibling child of PID 1 into the very process set the
    /// script walks, and `for d in /proc/[0-9]*` visits entries in lexicographic
    /// order, not spawn order. This drives a real `sh` over a fake `/proc`-shaped
    /// tree — substituting the literal `"/proc"` prefix for a temp directory (the
    /// only way the script ever names it), so the parsing and exclusion logic under
    /// test is exactly [`CAPTURE_CMDLINE_SCRIPT`], untouched — with two `ppid == 1`
    /// candidates: the script's own `$$` and the real workload, with the workload's
    /// fake pid chosen to sort lexicographically AFTER any pid a real OS could hand
    /// `sh` (max ~7 digits even at Linux's largest configurable `pid_max`), so
    /// without the `/proc/$$` exclusion the self-match would always be visited first
    /// and this test would fail.
    #[test]
    #[cfg(unix)]
    fn capture_workload_cmdline_script_never_captures_its_own_invocation() {
        use std::process::Command;

        let root = std::env::temp_dir().join(format!(
            "rightsize-capture-cmdline-it-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create fake /proc root");
        let root_str = root.to_str().expect("temp dir path is valid UTF-8");
        let script = CAPTURE_CMDLINE_SCRIPT.replace("/proc", root_str);

        // The real workload: ppid 1, a plain (unbracketed, non-"init.krun") comm.
        let workload_dir = root.join("999999999");
        std::fs::create_dir_all(&workload_dir).unwrap();
        std::fs::write(
            workload_dir.join("stat"),
            "999999999 (workload) S 1 999999999 999999999 0 -1",
        )
        .unwrap();
        std::fs::write(workload_dir.join("cmdline"), b"workload\0--flag\0value\0").unwrap();

        // init.krun and a kernel thread — both must be skipped by name regardless of
        // where they sort.
        std::fs::create_dir_all(root.join("1")).unwrap();
        std::fs::write(root.join("1").join("stat"), "1 (init.krun) S 0 1 1 0 -1").unwrap();
        std::fs::write(root.join("1").join("cmdline"), b"init.krun\0").unwrap();
        std::fs::create_dir_all(root.join("2")).unwrap();
        std::fs::write(
            root.join("2").join("stat"),
            "2 ([kworker/0:1]) S 1 2 2 0 -1",
        )
        .unwrap();
        std::fs::write(root.join("2").join("cmdline"), b"").unwrap();

        // The fixture that fabricates the script's OWN entry runs in the exact same
        // `sh` process as the script itself (one combined `-c` argument), so `$$`
        // inside the fixture and inside the script are the identical pid — the only
        // way to reproduce "the capture script discovers itself" deterministically,
        // since a separate shell's pid can't be known ahead of time.
        let fixture = format!(
            "selfdir=\"{root_str}/$$\"; mkdir -p \"$selfdir\"; \
             printf '%s (sh) S 1 %s %s 0 -1' \"$$\" \"$$\" \"$$\" > \"$selfdir/stat\"; \
             printf 'sh\\0-c\\0<the-capture-script-itself>\\0' > \"$selfdir/cmdline\"; "
        );

        let output = Command::new("sh")
            .arg("-c")
            .arg(format!("{fixture}{script}"))
            .output()
            .expect("spawn sh");
        std::fs::remove_dir_all(&root).ok();

        assert!(
            output.status.success(),
            "script exited non-zero: {output:?}"
        );
        assert_eq!(
            parse_captured_cmdline(&String::from_utf8_lossy(&output.stdout)),
            Some(vec![
                "workload".to_string(),
                "--flag".to_string(),
                "value".to_string(),
            ]),
            "must capture the real workload's cmdline, never the capture script's own \
             `sh -c` invocation — even though the script's own pid directory sorts \
             before the workload's fake one"
        );
    }

    #[test]
    fn parse_captured_cmdline_splits_on_nul_and_drops_the_terminator() {
        assert_eq!(
            parse_captured_cmdline("redis-server\0--port\x006379\0\n"),
            Some(vec![
                "redis-server".to_string(),
                "--port".to_string(),
                "6379".to_string(),
            ])
        );
    }

    #[test]
    fn parse_captured_cmdline_tolerates_a_missing_trailing_newline_or_nul() {
        // Not every capture goes through the line-draining `\n` normalization the
        // same way, and a `cmdline` file is not strictly guaranteed to end in a
        // NUL on every kernel — both shapes must still parse.
        assert_eq!(
            parse_captured_cmdline("redis-server\0--port\x006379"),
            Some(vec![
                "redis-server".to_string(),
                "--port".to_string(),
                "6379".to_string(),
            ])
        );
    }

    #[test]
    fn parse_captured_cmdline_preserves_a_genuine_empty_argument() {
        // Only ONE trailing empty segment (the NUL terminator) is ever dropped —
        // a real empty-string argument elsewhere in argv must survive.
        assert_eq!(
            parse_captured_cmdline("prog\0\0arg\0\n"),
            Some(vec!["prog".to_string(), String::new(), "arg".to_string()])
        );
    }

    #[test]
    fn parse_captured_cmdline_is_none_when_the_script_found_no_process_at_all() {
        // The script's own "found nothing" output — no matching pid, so it never
        // reaches its `cat`. Neither shape carries a NUL at all.
        assert_eq!(parse_captured_cmdline(""), None);
        assert_eq!(parse_captured_cmdline("\n"), None);
    }

    #[test]
    fn parse_captured_cmdline_a_lone_nul_is_one_empty_argument_not_nothing() {
        // Never produced by the real script (a process always has a non-empty
        // argv0), but the parser's own rule is consistent either way: exactly
        // ONE trailing NUL terminator is dropped, so what's left of a lone NUL
        // is a single empty-string argument, not "nothing captured".
        assert_eq!(parse_captured_cmdline("\0"), Some(vec![String::new()]));
        assert_eq!(parse_captured_cmdline("\0\n"), Some(vec![String::new()]));
    }

    #[test]
    fn snapshot_save_load_spellings() {
        assert_eq!(
            snapshot_export(
                "rz-ckpt-deadbeefcafe",
                std::path::Path::new("/tmp/cp.archive")
            ),
            vec![
                "snapshot",
                "save",
                "rz-ckpt-deadbeefcafe",
                "/tmp/cp.archive"
            ]
        );
        assert!(
            !snapshot_export(
                "rz-ckpt-deadbeefcafe",
                std::path::Path::new("/tmp/cp.archive")
            )
            .contains(&"--with-image".to_string()),
            "archives must never bundle the OCI image — its import fails an integrity check on \
             msb 0.6.6"
        );
        assert_eq!(
            snapshot_import(
                std::path::Path::new("/tmp/cp.archive"),
                std::path::Path::new("/cache/checkpoints")
            ),
            vec![
                "snapshot",
                "load",
                "/tmp/cp.archive",
                "--dest",
                "/cache/checkpoints"
            ],
            "load must always carry an explicit --dest so an imported ref lands under this \
             backend's own checkpoints dir, never msb's global default snapshot store"
        );
    }
}
