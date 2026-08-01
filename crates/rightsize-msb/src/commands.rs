//! Pure `msb` CLI argv construction — no process spawning here, just building the
//! argument vectors [`crate::backend::MsbCliBackend`] hands to `std::process::Command`.
//! Keeping this pure makes every flag spelling a plain data-in/data-out unit test,
//! independent of a real `msb` binary.
//!
//! **Attached mode, always** (no `-d`): microsandbox's detached mode never starts the
//! image's `ENTRYPOINT` on 0.6.2 — only attached mode does — so [`run`] never
//! emits `-d`, and the backend supervises the resulting child directly.

use std::path::Path;

use rightsize::model::ContainerSpec;

/// Builds the argv for `msb run`, in the pinned order: name, memory (if set),
/// root-disk (if a disk limit or tmpfs root is set), net (if network is disabled),
/// ports, env, mounts, image-or-snapshot, then `-- <command>` iff `spec.command`
/// is `Some` — a `None` command means "run the image's default
/// `ENTRYPOINT`/`CMD`", which requires omitting the trailing `--` entirely rather
/// than passing it with no arguments after it.
///
/// When `spec.checkpoint_ref` is set (this container was built from
/// [`rightsize::Container::from_checkpoint`]), `--from-snapshot <ref>` replaces the plain
/// image argument — `--from-snapshot` exists only on `msb run`, mutually exclusive with
/// the IMAGE positional, and boots a fresh sandbox from that disk snapshot instead
/// of pulling an image. Every other flag (name, memory, ports, env, mounts, the
/// trailing command) stays identical either way.
pub fn run(spec: &ContainerSpec) -> Vec<String> {
    let mut argv = vec!["run".to_string(), "--name".to_string(), spec.name.clone()];

    // `msb run --help`: -m/--memory <MEMORY>, e.g. 512M/1G — right after --name.
    if let Some(mb) = spec.memory_limit_mb {
        argv.push("-m".to_string());
        argv.push(format!("{mb}M"));
    }

    // `--root-disk` covers both a size-capped writable root disk and a tmpfs one —
    // `ContainerSpec`'s own validation (`Container::start`) already refuses a spec
    // that sets both, so at most one of these fires.
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
        argv.push(format!("{}:{}", port.host_port, port.guest_port));
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

    match &spec.checkpoint_ref {
        Some(snapshot_ref) => {
            argv.push("--from-snapshot".to_string());
            argv.push(snapshot_ref.clone());
        }
        None => argv.push(spec.image.clone()),
    }

    if let Some(command) = &spec.command {
        argv.push("--".to_string());
        argv.extend(command.iter().cloned());
    }

    argv
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

/// Builds the argv for `msb snapshot create --from <sandbox> <snapshot>` — requires
/// the sandbox to be STOPPED first (the checkpoint feature's own responsibility;
/// this function only builds the argv). `dest_dir`, when given, appends
/// `--dest-dir <dest_dir>`, storing the snapshot artifact under that directory
/// instead of msb's own default snapshot store — the checkpoint feature's
/// dest-dir mechanics (`MsbCliBackend::create_checkpoint`).
pub fn snapshot_create(
    sandbox_name: &str,
    snapshot_name: &str,
    dest_dir: Option<&Path>,
) -> Vec<String> {
    let mut argv = vec![
        "snapshot".to_string(),
        "create".to_string(),
        "--from".to_string(),
        sandbox_name.to_string(),
        snapshot_name.to_string(),
    ];
    if let Some(dir) = dest_dir {
        argv.push("--dest-dir".to_string());
        argv.push(dir.display().to_string());
    }
    argv
}

/// Builds the argv for `msb snapshot rm <snapshot>` — the checkpoint feature's
/// cleanup primitive (`SandboxBackend::remove_checkpoint`).
pub fn snapshot_rm(snapshot_name: &str) -> Vec<String> {
    vec![
        "snapshot".to_string(),
        "rm".to_string(),
        snapshot_name.to_string(),
    ]
}

/// Builds the argv for `msb snapshot inspect <snapshot>` — the named-checkpoint
/// existence probe (`SandboxBackend::has_checkpoint`): exit 0 means the snapshot
/// still exists.
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

/// Builds the argv for `msb snapshot load <archive>` — the checkpoint-archive
/// feature's import primitive (`SandboxBackend::import_checkpoint`). Takes no ref
/// argument: msb's import is content-addressed, unpacking under a digest-derived
/// directory name this backend resolves separately (see
/// `MsbCliBackend::import_checkpoint`).
pub fn snapshot_import(archive_path: &Path) -> Vec<String> {
    vec![
        "snapshot".to_string(),
        "load".to_string(),
        archive_path.display().to_string(),
    ]
}

/// Builds the argv for `msb snapshot list --format json` — confirms an imported
/// snapshot's digest-derived directory name is registered by matching it against
/// each entry's `name`/`artifact_path` (see `MsbCliBackend::import_checkpoint`).
pub fn snapshot_list() -> Vec<String> {
    vec![
        "snapshot".to_string(),
        "list".to_string(),
        "--format".to_string(),
        "json".to_string(),
    ]
}

/// Builds the argv for a plain (non-streaming) `msb exec`.
pub fn exec(name: &str, cmd: &[String]) -> Vec<String> {
    let mut argv = vec!["exec".to_string(), name.to_string(), "--".to_string()];
    argv.extend(cmd.iter().cloned());
    argv
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
    fn run_command_uses_dash_dash_snapshot_instead_of_the_image_when_checkpoint_ref_is_set() {
        let mut spec = full_spec();
        spec.checkpoint_ref = Some("rz-ckpt-deadbeefcafe".to_string());
        let cmd = run(&spec);
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
                "--from-snapshot",
                "rz-ckpt-deadbeefcafe",
                "--",
                "redis-server",
                "--port",
                "6379",
            ]
        );
        assert!(!cmd.contains(&spec.image));
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
            snapshot_create("rz-abc-1", "rz-ckpt-deadbeefcafe", None),
            vec![
                "snapshot",
                "create",
                "--from",
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe"
            ]
        );
        assert_eq!(
            snapshot_create(
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                Some(std::path::Path::new("/cache/checkpoints"))
            ),
            vec![
                "snapshot",
                "create",
                "--from",
                "rz-abc-1",
                "rz-ckpt-deadbeefcafe",
                "--dest-dir",
                "/cache/checkpoints"
            ]
        );
        assert_eq!(
            snapshot_rm("rz-ckpt-deadbeefcafe"),
            vec!["snapshot", "rm", "rz-ckpt-deadbeefcafe"]
        );
        assert_eq!(
            snapshot_inspect("rz-ckpt-deadbeefcafe"),
            vec!["snapshot", "inspect", "rz-ckpt-deadbeefcafe"]
        );
    }

    #[test]
    fn snapshot_export_import_list_spellings() {
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
            snapshot_import(std::path::Path::new("/tmp/cp.archive")),
            vec!["snapshot", "load", "/tmp/cp.archive"]
        );
        assert_eq!(
            snapshot_list(),
            vec!["snapshot", "list", "--format", "json"]
        );
    }
}
