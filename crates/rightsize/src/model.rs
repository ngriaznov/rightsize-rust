//! The container model: the plain-data types a backend needs to create, describe, and
//! mount things into a container. `ContainerSpec` in particular carries **already
//! chosen** host ports — see [`ContainerSpec::ports`] — a backend binds them, it never
//! allocates.

use std::path::PathBuf;

/// A host↔guest port map entry. The runtime binds `host_port` on loopback and forwards
/// traffic to `guest_port` inside the container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortBinding {
    /// The host-side port, already chosen by the core allocator before the backend ever
    /// sees this spec.
    pub host_port: u16,
    /// The port the workload listens on inside the guest.
    pub guest_port: u16,
}

/// A host file or directory exposed inside the guest at `guest_path`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMount {
    /// The host-side path being mounted in.
    pub host_path: PathBuf,
    /// The absolute path the file appears at inside the guest.
    pub guest_path: String,
    /// Whether the guest may only read it. Defaults to `true` — mounts are read-only
    /// unless a caller opts into read-write explicitly.
    pub read_only: bool,
}

impl FileMount {
    /// Builds a `FileMount` with the default `read_only: true`.
    pub fn new(host_path: impl Into<PathBuf>, guest_path: impl Into<String>) -> Self {
        Self {
            host_path: host_path.into(),
            guest_path: guest_path.into(),
            read_only: true,
        }
    }

    /// Returns a copy of this mount with `read_only` set to `false`.
    pub fn read_write(mut self) -> Self {
        self.read_only = false;
        self
    }
}

/// The outcome of a single `exec` call against a running container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecResult {
    /// The process exit code (0 = success).
    pub exit_code: i32,
    /// Everything written to stdout.
    pub stdout: String,
    /// Everything written to stderr.
    pub stderr: String,
}

/// Everything a backend needs to create one container. Host ports in `ports` are
/// **already chosen** by the core allocator (see `free_ports`) — a backend binds them,
/// it never allocates, so the same spec works identically whether the backend is a
/// microVM or a Docker daemon.
#[derive(Clone, Debug)]
pub struct ContainerSpec {
    /// The backend-facing container name, e.g. `rz-<run_id>-<seq>`.
    pub name: String,
    /// The image reference to run.
    pub image: String,
    /// Environment variables, insertion-ordered — a `Vec` of pairs rather than a
    /// `HashMap` so order and duplicate-key handling stay under the caller's control.
    pub env: Vec<(String, String)>,
    /// The command to run instead of the image's default entrypoint. `None` means "run
    /// the image as built."
    pub command: Option<Vec<String>>,
    /// Already-chosen host↔guest port bindings — see the type-level doc.
    pub ports: Vec<PortBinding>,
    /// Host files/directories to mount into the guest.
    pub mounts: Vec<FileMount>,
    /// The network to join, if any.
    pub network_id: Option<String>,
    /// DNS-style aliases this container is reachable as by other members of its network.
    pub aliases: Vec<String>,
    /// The per-process run id (see `run_id`), used to label/name containers so a crashed
    /// run's leftovers can be told apart from a live run's.
    pub run_id: String,
    /// An optional memory cap in megabytes.
    pub memory_limit_mb: Option<u64>,
    /// Marks this container as a **reuse** sandbox — one meant to outlive this
    /// process's own lifecycle rather than be torn down by it. Defaults to `false`;
    /// no builder in this crate sets it yet (reuse itself is a later wave), but every
    /// own-run cleanup path (the reaping ledger's `.sandboxes` file, the msb backend's
    /// `started_names`, the docker backend's run-id label, this crate's `Drop`-path
    /// cleanup) already knows to leave a `keep_alive` spec's container alone, so
    /// wiring the field in now costs nothing and the reuse wave doesn't need to touch
    /// any of those call sites again.
    pub keep_alive: bool,
    /// Set by [`crate::Container::from_checkpoint`] to the source [`crate::Checkpoint`]'s
    /// `ref` — the checkpoint feature's own signal to the backend that this spec's
    /// `image` is a checkpoint reference, not an ordinary image. docker ignores this
    /// (the ref already IS a normal image tag; the ordinary create path just works);
    /// microsandbox, when this is set, boots via `msb run --snapshot <ref>`
    /// instead of its normal image boot, keeping every other flag identical.
    /// Deliberately NOT part of the reuse identity hash — reuse and
    /// `from_checkpoint` are not a supported combination (see
    /// `RightsizeError::ReuseCheckpointConflict`). Defaults to `None`.
    pub checkpoint_ref: Option<String>,
}

impl ContainerSpec {
    /// Builds a spec with every optional field at its default (no env, no command, no
    /// ports, no mounts, no network, no aliases, no memory limit).
    pub fn new(
        name: impl Into<String>,
        image: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            image: image.into(),
            env: Vec::new(),
            command: None,
            ports: Vec::new(),
            mounts: Vec::new(),
            network_id: None,
            aliases: Vec::new(),
            run_id: run_id.into(),
            memory_limit_mb: None,
            keep_alive: false,
            checkpoint_ref: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn container_spec_new_defaults_every_optional_field() {
        let spec = ContainerSpec::new("rz-deadbeef-0", "redis:8.6-alpine", "deadbeef");
        assert_eq!(spec.name, "rz-deadbeef-0");
        assert_eq!(spec.image, "redis:8.6-alpine");
        assert_eq!(spec.run_id, "deadbeef");
        assert!(spec.env.is_empty());
        assert!(spec.command.is_none());
        assert!(spec.ports.is_empty());
        assert!(spec.mounts.is_empty());
        assert!(spec.network_id.is_none());
        assert!(spec.aliases.is_empty());
        assert_eq!(spec.memory_limit_mb, None);
        assert!(!spec.keep_alive);
        assert!(spec.checkpoint_ref.is_none());
    }

    #[test]
    fn file_mount_defaults_to_read_only() {
        let m = FileMount::new("/host/f.txt", "/guest/f.txt");
        assert!(m.read_only);
        assert_eq!(m.host_path, PathBuf::from("/host/f.txt"));
        assert_eq!(m.guest_path, "/guest/f.txt");
    }

    #[test]
    fn file_mount_read_write_flips_the_flag() {
        let m = FileMount::new("/host/f.txt", "/guest/f.txt").read_write();
        assert!(!m.read_only);
    }

    #[test]
    fn port_binding_and_exec_result_are_plain_value_types() {
        let a = PortBinding {
            host_port: 32768,
            guest_port: 6379,
        };
        let b = a.clone();
        assert_eq!(a, b);

        let r = ExecResult {
            exit_code: 0,
            stdout: "ok".into(),
            stderr: String::new(),
        };
        assert_eq!(r.exit_code, 0);
        assert_eq!(r.stdout, "ok");
    }
}
