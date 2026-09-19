//! The container model: the plain-data types a backend needs to create, describe, and
//! mount things into a container. `ContainerSpec` in particular carries **already
//! chosen** host ports — see [`ContainerSpec::ports`] — a backend binds them, it never
//! allocates.

use std::path::PathBuf;

/// The transport protocol a [`PortBinding`]/[`crate::backend::NetworkLink`] carries.
///
/// Defaults to [`Protocol::Tcp`] — every port/link built before UDP exposure existed
/// is TCP, and every existing producer in this crate either sets this explicitly or
/// gets it for free via [`Default`], so nothing that only ever spoke TCP has to
/// change. A `tcp`-exposed and a `udp`-exposed [`PortBinding`] for the SAME numeric
/// port are deliberately distinct values (this field participates in
/// [`PortBinding`]'s `PartialEq`/`Eq`, and in `crate::reuse`'s identity hash) — a
/// container may expose the same guest port on both protocols at once (DNS's port
/// 53 is the canonical example), and the two must never collide or be silently
/// merged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// Transmission Control Protocol — every port/link in this crate before UDP
    /// exposure existed, and still the default for anything that doesn't say
    /// otherwise.
    #[default]
    Tcp,
    /// User Datagram Protocol. See the module-level UDP exposure docs
    /// (`Container::with_exposed_udp_ports`, `ContainerGuard::get_mapped_udp_port`)
    /// for the Phase 1 scope and its one hard limitation (msb network links).
    Udp,
}

impl Protocol {
    /// The lowercase wire spelling (`"tcp"`/`"udp"`) both backends use: Docker's
    /// `<port>/<proto>` `ExposedPorts`/`PortBindings` keys, and msb's `-p
    /// HOST:GUEST[/udp]` (TCP has no suffix — see `rightsize_msb::commands`).
    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::Tcp => "tcp",
            Protocol::Udp => "udp",
        }
    }
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A host↔guest port map entry. The runtime binds `host_port` on loopback and forwards
/// traffic to `guest_port` inside the container.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortBinding {
    /// The host-side port, already chosen by the core allocator before the backend ever
    /// sees this spec.
    pub host_port: u16,
    /// The port the workload listens on inside the guest.
    pub guest_port: u16,
    /// The transport this binding carries. Defaults to [`Protocol::Tcp`] — see that
    /// type's own doc for the full backward-compatibility and identity story.
    pub protocol: Protocol,
}

/// A host file or directory exposed inside the guest at `guest_path`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileMount {
    /// The host-side path being mounted in.
    pub host_path: PathBuf,
    /// The absolute path the file appears at inside the guest.
    pub guest_path: String,
    /// Whether the guest may only read it. Defaults to `false` — the guest may write,
    /// and on both backends a write reaches the host file itself, so opt into
    /// [`FileMount::read_only`] when the host copy must not be modified.
    pub read_only: bool,
}

impl FileMount {
    /// Builds a `FileMount` with the default `read_only: false`.
    pub fn new(host_path: impl Into<PathBuf>, guest_path: impl Into<String>) -> Self {
        Self {
            host_path: host_path.into(),
            guest_path: guest_path.into(),
            read_only: false,
        }
    }

    /// Returns a copy of this mount with `read_only` set to `true`, so the guest cannot
    /// modify the host file behind it.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
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
    /// microsandbox, when this is set, boots via `msb restore <ref> --name <name>`
    /// instead of its normal image boot (msb 0.7.1 replaced `run --from-snapshot`
    /// with this dedicated `restore` command, never carrying `--disk-only` — msb
    /// rejects that flag against the disk-scope snapshots this backend creates —
    /// and otherwise carrying over only name/ports/memory — see the
    /// `rightsize-msb` crate's own `commands::restore` for the full list of what
    /// it does and doesn't carry over from this spec). Deliberately NOT part of the reuse identity hash — reuse and
    /// `from_checkpoint` are not a supported combination (see
    /// `RightsizeError::ReuseCheckpointConflict`). Defaults to `None`.
    pub checkpoint_ref: Option<String>,
    /// A writable-root-disk ceiling in megabytes — microsandbox-only (emits
    /// `--root-disk <mb>M`); docker runs its normal disk-backed rootfs with no
    /// ceiling and ignores this field. Grows only on an msb reboot — the guest
    /// filesystem never shrinks back down. Mutually exclusive with
    /// `tmpfs_root_mb`, enforced at `Container::start()` before any backend call
    /// (`RightsizeError::RootDiskConflict`). Part of the reuse identity hash,
    /// exactly like `memory_limit_mb`. Defaults to `None`.
    pub disk_limit_mb: Option<u64>,
    /// A RAM-backed writable rootfs in megabytes — microsandbox-only (emits
    /// `--root-disk tmpfs:<mb>M`); docker runs its normal disk-backed rootfs and
    /// ignores this field. Must not exceed `memory_limit_mb` when the latter is
    /// set, and is mutually exclusive with `disk_limit_mb` — both enforced at
    /// `Container::start()` before any backend call
    /// (`RightsizeError::TmpfsRootExceedsMemory`, `RightsizeError::RootDiskConflict`).
    /// A tmpfs root is ephemeral and cannot be checkpointed
    /// (`RightsizeError::TmpfsRootCheckpoint`). Part of the reuse identity hash,
    /// exactly like `memory_limit_mb`. Defaults to `None`.
    pub tmpfs_root_mb: Option<u64>,
    /// Blocks public-internet access on microsandbox (emits `--net private` —
    /// published ports and private-range links keep working); docker ignores this
    /// field and runs with normal networking. Mutually exclusive with
    /// `network_id`, enforced at `Container::start()` before any backend call
    /// (`RightsizeError::NetworkDisabledConflict`). Part of the reuse identity
    /// hash, exactly like `memory_limit_mb`. Defaults to `false`.
    pub network_disabled: bool,
    /// The workload cmdline [`crate::ContainerGuard::checkpoint`]/`checkpoint_named`
    /// captured from the guest at checkpoint time, for a restore whose checkpoint
    /// spec has no explicit `command` — an image's default entrypoint, which the
    /// checkpoint's own capture step recovered by walking the guest's process
    /// table right before stopping it for the snapshot (see the `rightsize-msb`
    /// crate's `commands::capture_workload_cmdline`/`parse_captured_cmdline`).
    /// Threaded through by [`crate::Container::from_checkpoint`] from
    /// [`crate::Checkpoint::spec`] (in turn either the in-memory value an
    /// unnamed `checkpoint()` returned, or a named checkpoint's registry entry
    /// — see `crate::checkpoint::NamedRegistrySpec`'s own additive field).
    ///
    /// A backend whose checkpoint mechanism restarts the workload
    /// (`Capabilities::checkpoint_restarts_workload`) consults this ONLY when
    /// `command` above is `None` — an explicit command always wins — to decide
    /// what to re-run after a restore reaches its idle post-boot state; a
    /// backend that leaves the container undisturbed (docker) never reads it.
    /// Internal plumbing, in the same spirit as `checkpoint_ref` above — no
    /// public builder sets this directly. Defaults to `None`.
    pub checkpoint_captured_cmdline: Option<Vec<String>>,
    /// A BATCH of candidate sandbox names — always including `name` itself as
    /// its first entry — a backend whose ordinary restore boot can hit a
    /// Windows-only access-denied transient should walk instead of retrying
    /// `name` in place. Minted by `rightsize::container::create_started_container`
    /// (the same `rz-<run-id>-<seq>` generator every ordinary create uses,
    /// already appended to the reaping ledger — every candidate, not just
    /// `name` — before `SandboxBackend::create`/`start` ever run) ONLY for a
    /// [`crate::Container::from_checkpoint`] spec (`checkpoint_ref.is_some()`);
    /// `None` for every ordinarily-built container, and for a checkpoint
    /// restore spec that reaches a backend's `start()` directly rather than
    /// through `Container::from_checkpoint(...).start()`.
    ///
    /// Mirrors `create_checkpoint`'s own `fresh_names` parameter (see that
    /// trait method's own doc for the live-verified Windows evidence this
    /// exists to work around) — a backend whose ordinary restore never hits
    /// that transient (docker: the checkpoint mechanism never even reboots)
    /// ignores this field entirely, exactly like `create_checkpoint`'s own
    /// `fresh_names[1..]`. Every candidate beyond whichever one a walking
    /// backend actually attempts is simply left in the reaping ledger —
    /// harmless noise for its own not-found-tolerant sweep, since a name
    /// that was pre-tracked but never handed to the backend at all trivially
    /// resolves as "not found." Internal plumbing, in the same spirit as
    /// `checkpoint_ref` above — no public builder sets this directly.
    /// Defaults to `None`.
    pub restore_name_candidates: Option<Vec<String>>,
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
            disk_limit_mb: None,
            tmpfs_root_mb: None,
            network_disabled: false,
            checkpoint_captured_cmdline: None,
            restore_name_candidates: None,
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
        assert_eq!(spec.disk_limit_mb, None);
        assert_eq!(spec.tmpfs_root_mb, None);
        assert!(!spec.network_disabled);
        assert!(spec.checkpoint_captured_cmdline.is_none());
        assert!(spec.restore_name_candidates.is_none());
    }

    #[test]
    fn file_mount_defaults_to_read_write() {
        let m = FileMount::new("/host/f.txt", "/guest/f.txt");
        assert!(!m.read_only);
        assert_eq!(m.host_path, PathBuf::from("/host/f.txt"));
        assert_eq!(m.guest_path, "/guest/f.txt");
    }

    #[test]
    fn file_mount_read_only_flips_the_flag() {
        let m = FileMount::new("/host/f.txt", "/guest/f.txt").read_only();
        assert!(m.read_only);
    }

    #[test]
    fn file_mount_read_write_flips_the_flag() {
        let m = FileMount::new("/host/f.txt", "/guest/f.txt")
            .read_only()
            .read_write();
        assert!(!m.read_only);
    }

    #[test]
    fn port_binding_and_exec_result_are_plain_value_types() {
        let a = PortBinding {
            host_port: 32768,
            guest_port: 6379,
            protocol: Protocol::Tcp,
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

    #[test]
    fn protocol_defaults_to_tcp() {
        assert_eq!(Protocol::default(), Protocol::Tcp);
    }

    #[test]
    fn protocol_as_str_and_display_are_the_lowercase_wire_spelling() {
        assert_eq!(Protocol::Tcp.as_str(), "tcp");
        assert_eq!(Protocol::Udp.as_str(), "udp");
        assert_eq!(Protocol::Tcp.to_string(), "tcp");
        assert_eq!(Protocol::Udp.to_string(), "udp");
    }

    #[test]
    fn port_binding_equality_distinguishes_protocol_on_the_same_numeric_port() {
        // DNS on port 53: a tcp-exposed and a udp-exposed binding for the SAME
        // guest/host ports must never compare equal or collide — see
        // `Protocol`'s own doc.
        let tcp = PortBinding {
            host_port: 32768,
            guest_port: 53,
            protocol: Protocol::Tcp,
        };
        let udp = PortBinding {
            host_port: 32768,
            guest_port: 53,
            protocol: Protocol::Udp,
        };
        assert_ne!(tcp, udp);
    }
}
