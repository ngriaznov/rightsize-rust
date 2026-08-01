//! The error grammar for `rightsize`: every subprocess/daemon failure surfaces its full
//! stderr or body, every wait timeout carries `describe()` plus a log tail, and
//! `UnsupportedByBackend` reads as one sentence — a fact, an em-dash, a remedy.
//!
//! Keep `feature` a noun phrase (what's missing) and put actionable advice in `remedy`
//! rather than folding it into `feature`; mixing the two renders as a run-on sentence.

/// The error type returned by every fallible `rightsize` operation.
#[derive(Debug, thiserror::Error)]
pub enum RightsizeError {
    /// A backend was asked for a capability it does not implement — e.g. microsandbox
    /// asked to install a network link into an image with no `nc`. `remedy`, when
    /// given, is appended after an em-dash as a hint (try a different backend, fix the
    /// image, etc.).
    #[error("{}", format_unsupported(feature, backend, remedy))]
    UnsupportedByBackend {
        /// A short noun phrase naming what's unsupported (never the advice itself).
        feature: String,
        /// The backend's registered name (e.g. `"microsandbox"`, `"docker"`).
        backend: String,
        /// Optional actionable advice, rendered after an em-dash.
        remedy: Option<String>,
    },

    /// A backend's `start()` failed because a host port it tried to bind was already in
    /// use by something else. The container port-retry loop classifies this
    /// case — typed first, string-matched fallback — and retries with fresh ports.
    #[error("{message}")]
    PortBindConflict {
        /// The rendered failure message (backend-specific wording is fine here).
        message: String,
        /// The underlying error, when a backend can supply one (e.g. the daemon's own
        /// error chain), so `source()` still walks to it.
        #[source]
        source: Option<Box<RightsizeError>>,
    },

    /// A container's wait strategy never became ready before its startup timeout. The
    /// message carries the wait target's `describe()` plus its last 50 log lines, so a
    /// failure is diagnosable from the test output alone.
    #[error("{0}")]
    ContainerLaunch(String),

    /// A subprocess or daemon call failed outright (non-zero exit, non-2xx response).
    /// The message carries the full stderr or response body — never a truncated
    /// summary — so the failure is diagnosable from the test output alone.
    #[error("{0}")]
    Backend(String),

    /// A backend's `create()` failed because another process already created a
    /// sandbox with this name — the reuse start flow's cue (see
    /// `crate::reuse::is_name_conflict`) to re-enter the adopt path once, on the
    /// theory that the winner is about to (or already did) register itself in the
    /// reuse registry. Typed first, string-matched ("already exists") fallback,
    /// mirroring [`RightsizeError::PortBindConflict`]'s own classification shape.
    #[error("{message}")]
    NameConflict {
        /// The rendered failure message (backend-specific wording is fine here).
        message: String,
        /// The underlying error, when a backend can supply one, so `source()` still
        /// walks to it.
        #[source]
        source: Option<Box<RightsizeError>>,
    },

    /// `.reuse(true)` was combined with `.with_network(...)`. Reuse's identity hash
    /// covers only image/env/command/ports/mounts — never cross-container network
    /// topology — so this combination has no well-defined adopt/create behavior.
    #[error(
        "Container reuse cannot be combined with a custom network ('{network_id}') — reuse \
         identity does not cover network topology; drop either .reuse(true) or \
         .with_network(...)"
    )]
    ReuseNetworkConflict {
        /// The network id the container was trying to join.
        network_id: String,
    },

    /// `.reuse(true)` was combined with `Container::from_checkpoint(...)`. Reuse's
    /// identity hash has no concept of a checkpoint reference (`checkpoint_ref`
    /// deliberately does not enter it), so this combination has no well-defined
    /// adopt/create behavior. Raised in `Container::start()`, before any backend
    /// work, once reuse is fully active (both opt-ins) — mirrors
    /// [`RightsizeError::ReuseNetworkConflict`]'s own gating.
    #[error(
        "Container reuse cannot be combined with Container::from_checkpoint(...) — reuse \
         identity does not cover a checkpoint reference; drop either .reuse(true) or start from \
         an ordinary image instead"
    )]
    ReuseCheckpointConflict,

    /// `.with_disk_limit(...)` was combined with `.with_tmpfs_root(...)` on the
    /// same container. Raised in `Container::start()`, before any backend work —
    /// the root disk is either a fixed-size ceiling or RAM-backed, never both.
    #[error(
        "with_disk_limit() cannot be combined with with_tmpfs_root() — the root disk is \
         either size-capped or RAM-backed, not both; drop one"
    )]
    RootDiskConflict,

    /// `.with_tmpfs_root(tmpfs_mb)` was combined with `.with_memory_limit(memory_mb)`
    /// where the tmpfs root would exceed the memory ceiling — a tmpfs root lives
    /// inside guest memory, so it cannot be larger than the memory the guest is
    /// allowed. Raised in `Container::start()`, before any backend work. Not
    /// raised when no memory limit is set at all — msb's own error at boot time is
    /// already precise for that case.
    #[error(
        "with_tmpfs_root({tmpfs_mb}) exceeds with_memory_limit({memory_mb}) — a tmpfs root \
         lives in guest memory and must fit inside it"
    )]
    TmpfsRootExceedsMemory {
        /// The requested tmpfs root size, in megabytes.
        tmpfs_mb: u64,
        /// The configured memory limit, in megabytes.
        memory_mb: u64,
    },

    /// `.with_network_disabled()` was combined with `.with_network(...)` on the
    /// same container. Raised in `Container::start()`, before any backend work —
    /// a network-disabled container has nothing to join a network with.
    #[error(
        "with_network_disabled() cannot be combined with with_network() — a \
         network-disabled container cannot join a network; drop one"
    )]
    NetworkDisabledConflict,

    /// `ContainerGuard::checkpoint()`/`checkpoint_named()` was called on a
    /// container started with `.with_tmpfs_root(...)` — a tmpfs root is
    /// RAM-backed and gone the moment the guest stops, so there is nothing
    /// durable left to snapshot. Raised by the msb backend, first thing in its
    /// checkpoint path, before the guest is ever stopped.
    #[error(
        "this container uses a tmpfs root (with_tmpfs_root), which is ephemeral and cannot \
         be checkpointed — use with_disk_limit or the default root disk for checkpointable \
         containers"
    )]
    TmpfsRootCheckpoint,

    /// `.require_isolation(true)` was set on a `Container` but the active backend's
    /// `capabilities().hardware_isolated` is `false` — e.g. the docker backend, which
    /// shares the host kernel. Raised in `Container::start()`, before any
    /// create/network work: no sandbox is created. The message names the active
    /// backend and the remedy (switch to the msb backend).
    #[error("{}", format_isolation_required(backend))]
    IsolationRequired {
        /// The active backend's registered name (e.g. `"docker"`).
        backend: String,
    },

    /// `ContainerGuard::checkpoint()` was called but the active backend's
    /// `capabilities().checkpoint` is `false` — every real backend has it today
    /// (docker via image commit, microsandbox via disk snapshots); this only fires
    /// for a test double that hasn't opted in. Raised BEFORE any backend call (see
    /// `ContainerGuard::checkpoint`'s doc). The message names the backend and points
    /// at the checkpoints docs, without steering toward a specific other backend.
    #[error("{}", format_checkpoint_unsupported(backend))]
    CheckpointUnsupported {
        /// The active backend's registered name (a test double's, in practice —
        /// see the variant doc).
        backend: String,
    },

    /// `Container::from_checkpoint(&cp)` was started under a different active
    /// backend than the one that created `cp` (`Checkpoint::backend`) — a
    /// docker-committed image cannot boot as a microsandbox snapshot ref, and vice
    /// versa. Raised in `Container::start()`, before any backend work. The same
    /// variant is also raised by `Checkpoint::export_to`/`Checkpoint::import_from`
    /// when the active backend doesn't match the checkpoint's/archive's creator,
    /// before any backend or filesystem work in either case. The message names
    /// both backends and the `RIGHTSIZE_BACKEND=<creator>` remedy.
    #[error(
        "{}",
        format_checkpoint_backend_mismatch(active_backend, checkpoint_backend)
    )]
    CheckpointBackendMismatch {
        /// The currently active backend's registered name.
        active_backend: String,
        /// The backend that created the checkpoint being restored.
        checkpoint_backend: String,
    },

    /// A named checkpoint's name failed the checkpoints feature's validation
    /// regex (`^[a-z0-9][a-z0-9-]{0,40}$`) — raised by
    /// `ContainerGuard::checkpoint_named`, `Checkpoint::find`, and
    /// `Checkpoint::remove`, before any backend call or registry I/O in every
    /// case. The regex is the same across every port of this library (a
    /// cross-language contract), so a name rejected here is rejected everywhere.
    #[error("checkpoint name '{name}' is invalid — names must match ^[a-z0-9][a-z0-9-]{{0,40}}$")]
    InvalidCheckpointName {
        /// The rejected name, verbatim.
        name: String,
    },

    /// `Checkpoint::export_to(...)` was called on a checkpoint whose backend-native
    /// artifact is no longer there (`SandboxBackend::has_checkpoint` returned
    /// `false`) — exporting it would either fail partway through or produce an
    /// archive whose `artifact` member is missing/empty, so this is raised before
    /// any staging or archive work begins.
    #[error("{}", format_checkpoint_artifact_missing(checkpoint_ref, backend))]
    CheckpointArtifactMissing {
        /// The checkpoint ref that no longer has a backend-native artifact.
        checkpoint_ref: String,
        /// The backend it was supposedly created on.
        backend: String,
    },

    /// `Checkpoint::import_from(...)` was given a path that isn't a valid rightsize
    /// checkpoint archive — the file doesn't exist or isn't a tar, it's missing the
    /// `checkpoint.json` member, that member doesn't parse as JSON, or its
    /// `rightsizeArchive` field isn't a version this port understands. Raised
    /// before any backend call or registry write in every case.
    #[error("{}", format_malformed_archive(path, reason))]
    MalformedArchive {
        /// The archive path that was rejected.
        path: std::path::PathBuf,
        /// A short phrase naming exactly what's wrong with it.
        reason: String,
    },

    /// An explicit image was supplied to a module constructor whose repository
    /// (registry host, tag, and digest all stripped — see [`crate::ImageName`])
    /// does not match the repository that module declares it understands. Raised by
    /// [`crate::ImageName::assert_compatible_with`], before any backend work — never
    /// a bare wait-strategy timeout on an image that was never going to behave like
    /// the module expects. The escape hatch is
    /// [`crate::ImageName::as_compatible_substitute_for`], mirroring Testcontainers'
    /// `asCompatibleSubstituteFor`.
    #[error("{}", format_incompatible_image(supplied, expected))]
    IncompatibleImage {
        /// The supplied image's parsed repository (registry host, tag, and digest
        /// stripped).
        supplied: String,
        /// The repository this module declares it understands.
        expected: String,
    },

    /// The msb toolchain provisioner failed (download, checksum, install).
    #[error("{0}")]
    Provision(String),

    /// A host I/O operation failed (binding a port, reading a socket, writing a temp
    /// file). Wrapped rather than matched on, since the original `io::Error` already
    /// carries a precise `Display`.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Renders `UnsupportedByBackend`'s single-sentence grammar: the base sentence, and,
/// only when `remedy` is `Some`, an em-dash followed by the remedy text.
fn format_unsupported(feature: &str, backend: &str, remedy: &Option<String>) -> String {
    let base = format!("Feature '{feature}' is not supported by the '{backend}' backend");
    match remedy {
        Some(r) => format!("{base} — {r}"),
        None => base,
    }
}

/// Renders `IsolationRequired`'s message: the same fact-em-dash-remedy grammar as
/// [`format_unsupported`], naming the active backend and the fix.
fn format_isolation_required(backend: &str) -> String {
    format!(
        "Hardware isolation was required (.require_isolation(true)) but the active '{backend}' \
         backend does not provide it — set RIGHTSIZE_BACKEND=microsandbox to run on a hardware-isolated \
         backend, or drop .require_isolation(true) if this workload does not need it"
    )
}

/// Renders `CheckpointUnsupported`'s message: the same fact-em-dash-remedy grammar as
/// [`format_unsupported`], naming the active backend without steering toward a
/// specific other one — both real backends support checkpointing today, so the only
/// backend that can ever hit this is a test double.
fn format_checkpoint_unsupported(backend: &str) -> String {
    format!(
        "Checkpoint/restore was requested but the active '{backend}' backend does not support \
         it — checkpointing needs a backend whose capabilities().checkpoint is true (see the \
         checkpoints docs)"
    )
}

/// Renders `CheckpointBackendMismatch`'s message: names both the active backend and
/// the checkpoint's creator, plus the `RIGHTSIZE_BACKEND=<creator>` remedy. Kept
/// call-site-neutral (no mention of `from_checkpoint`/`export_to`/`import_from` by
/// name) since all three raise this same variant.
fn format_checkpoint_backend_mismatch(active_backend: &str, checkpoint_backend: &str) -> String {
    format!(
        "the active backend is '{active_backend}', but this checkpoint was created by the \
         '{checkpoint_backend}' backend — set RIGHTSIZE_BACKEND={checkpoint_backend} to use it, \
         or take a fresh checkpoint under '{active_backend}' instead"
    )
}

/// Renders `CheckpointArtifactMissing`'s message: the same fact-em-dash-remedy
/// grammar as [`format_unsupported`].
fn format_checkpoint_artifact_missing(checkpoint_ref: &str, backend: &str) -> String {
    format!(
        "checkpoint '{checkpoint_ref}' has no backend-native artifact left on the '{backend}' \
         backend — it may already have been removed; there is nothing left to export"
    )
}

/// Renders `MalformedArchive`'s message: the archive path plus the specific reason
/// it was rejected.
fn format_malformed_archive(path: &std::path::Path, reason: &str) -> String {
    format!(
        "the checkpoint archive at '{}' is not a usable rightsize archive — {reason}",
        path.display()
    )
}

/// Renders `IncompatibleImage`'s message: the same fact-em-dash-remedy grammar as
/// [`format_unsupported`] — names the supplied repository, the expected one, and how
/// to override.
fn format_incompatible_image(supplied: &str, expected: &str) -> String {
    format!(
        "image repository '{supplied}' does not match this module's expected repository \
         '{expected}' — call ImageName::parse(...).as_compatible_substitute_for(\"{expected}\") \
         if '{supplied}' is a verified drop-in replacement, or supply an image from \
         '{expected}' instead"
    )
}

impl RightsizeError {
    /// Builds a [`RightsizeError::UnsupportedByBackend`] without a remedy.
    pub fn unsupported(feature: impl Into<String>, backend: impl Into<String>) -> Self {
        Self::UnsupportedByBackend {
            feature: feature.into(),
            backend: backend.into(),
            remedy: None,
        }
    }

    /// Builds a [`RightsizeError::UnsupportedByBackend`] with a remedy appended after an em-dash.
    pub fn unsupported_with_remedy(
        feature: impl Into<String>,
        backend: impl Into<String>,
        remedy: impl Into<String>,
    ) -> Self {
        Self::UnsupportedByBackend {
            feature: feature.into(),
            backend: backend.into(),
            remedy: Some(remedy.into()),
        }
    }
}

/// A convenience alias for `Result<T, RightsizeError>`, used throughout the crate.
pub type Result<T> = std::result::Result<T, RightsizeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_without_remedy_renders_base_sentence_only() {
        let e = RightsizeError::unsupported("network alias 'bad'", "microsandbox");
        assert_eq!(
            e.to_string(),
            "Feature 'network alias 'bad'' is not supported by the 'microsandbox' backend"
        );
    }

    #[test]
    fn unsupported_with_remedy_appends_it_after_an_em_dash() {
        let e = RightsizeError::unsupported_with_remedy(
            "network links (no nc/busybox in consumer image 'X')",
            "microsandbox",
            "run this test with RIGHTSIZE_BACKEND=docker instead",
        );
        assert_eq!(
            e.to_string(),
            "Feature 'network links (no nc/busybox in consumer image 'X')' is not supported by \
             the 'microsandbox' backend — run this test with RIGHTSIZE_BACKEND=docker instead"
        );
    }

    #[test]
    fn port_bind_conflict_displays_its_message() {
        let e = RightsizeError::PortBindConflict {
            message: "address already in use".into(),
            source: None,
        };
        assert_eq!(e.to_string(), "address already in use");
    }

    #[test]
    fn container_launch_and_backend_display_their_payload() {
        let e = RightsizeError::ContainerLaunch("timed out waiting for port 6379".into());
        assert_eq!(e.to_string(), "timed out waiting for port 6379");
        let e = RightsizeError::Backend("500: already allocated".into());
        assert_eq!(e.to_string(), "500: already allocated");
    }

    #[test]
    fn name_conflict_displays_its_message() {
        let e = RightsizeError::NameConflict {
            message: "container name 'rz-reuse-abc' is already in use".into(),
            source: None,
        };
        assert_eq!(
            e.to_string(),
            "container name 'rz-reuse-abc' is already in use"
        );
    }

    #[test]
    fn isolation_required_names_the_backend_and_the_remedy() {
        let e = RightsizeError::IsolationRequired {
            backend: "docker".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("docker"), "{msg}");
        assert!(msg.contains("RIGHTSIZE_BACKEND=microsandbox"), "{msg}");
        assert!(msg.contains(".require_isolation(true)"), "{msg}");
    }

    #[test]
    fn checkpoint_unsupported_names_the_backend_without_steering_to_a_specific_other_one() {
        let e = RightsizeError::CheckpointUnsupported {
            backend: "test-double".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("test-double"), "{msg}");
        assert!(msg.contains("capabilities().checkpoint"), "{msg}");
        assert!(
            !msg.contains("RIGHTSIZE_BACKEND=docker"),
            "must not steer toward a specific backend — both real backends support it now: {msg}"
        );
    }

    #[test]
    fn checkpoint_backend_mismatch_names_both_backends_and_the_remedy() {
        let e = RightsizeError::CheckpointBackendMismatch {
            active_backend: "docker".to_string(),
            checkpoint_backend: "microsandbox".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("docker"), "{msg}");
        assert!(msg.contains("microsandbox"), "{msg}");
        assert!(msg.contains("RIGHTSIZE_BACKEND=microsandbox"), "{msg}");
    }

    #[test]
    fn reuse_network_conflict_names_the_network_and_both_knobs() {
        let e = RightsizeError::ReuseNetworkConflict {
            network_id: "rz-net-1".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("rz-net-1"), "{msg}");
        assert!(msg.contains(".reuse(true)"), "{msg}");
        assert!(msg.contains(".with_network(...)"), "{msg}");
    }

    #[test]
    fn reuse_checkpoint_conflict_names_both_knobs() {
        let e = RightsizeError::ReuseCheckpointConflict;
        let msg = e.to_string();
        assert!(msg.contains(".reuse(true)"), "{msg}");
        assert!(msg.contains("from_checkpoint"), "{msg}");
    }

    #[test]
    fn root_disk_conflict_names_both_builders() {
        let e = RightsizeError::RootDiskConflict;
        let msg = e.to_string();
        assert!(msg.contains("with_disk_limit()"), "{msg}");
        assert!(msg.contains("with_tmpfs_root()"), "{msg}");
    }

    #[test]
    fn tmpfs_root_exceeds_memory_names_both_values() {
        let e = RightsizeError::TmpfsRootExceedsMemory {
            tmpfs_mb: 1024,
            memory_mb: 512,
        };
        let msg = e.to_string();
        assert!(msg.contains("with_tmpfs_root(1024)"), "{msg}");
        assert!(msg.contains("with_memory_limit(512)"), "{msg}");
    }

    #[test]
    fn network_disabled_conflict_names_both_builders() {
        let e = RightsizeError::NetworkDisabledConflict;
        let msg = e.to_string();
        assert!(msg.contains("with_network_disabled()"), "{msg}");
        assert!(msg.contains("with_network()"), "{msg}");
    }

    #[test]
    fn tmpfs_root_checkpoint_names_the_offending_builder() {
        let e = RightsizeError::TmpfsRootCheckpoint;
        let msg = e.to_string();
        assert!(msg.contains("with_tmpfs_root"), "{msg}");
        assert!(msg.contains("checkpoint"), "{msg}");
    }

    #[test]
    fn invalid_checkpoint_name_names_the_rejected_name_and_the_pattern() {
        let e = RightsizeError::InvalidCheckpointName {
            name: "Bad Name!".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("Bad Name!"), "{msg}");
        assert!(msg.contains("^[a-z0-9][a-z0-9-]{0,40}$"), "{msg}");
    }

    #[test]
    fn checkpoint_backend_mismatch_message_is_call_site_neutral() {
        // Reused unchanged by `Container::from_checkpoint`, `Checkpoint::export_to`,
        // and `Checkpoint::import_from` — the wording must not imply only one of
        // those raised it.
        let e = RightsizeError::CheckpointBackendMismatch {
            active_backend: "docker".to_string(),
            checkpoint_backend: "microsandbox".to_string(),
        };
        let msg = e.to_string();
        assert!(!msg.contains("from_checkpoint"), "{msg}");
        assert!(!msg.contains("export_to"), "{msg}");
        assert!(!msg.contains("import_from"), "{msg}");
        assert!(msg.contains("docker"), "{msg}");
        assert!(msg.contains("microsandbox"), "{msg}");
        assert!(msg.contains("RIGHTSIZE_BACKEND=microsandbox"), "{msg}");
    }

    #[test]
    fn checkpoint_artifact_missing_names_the_ref_and_the_backend() {
        let e = RightsizeError::CheckpointArtifactMissing {
            checkpoint_ref: "rz-ckpt-deadbeefcafe".to_string(),
            backend: "microsandbox".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("rz-ckpt-deadbeefcafe"), "{msg}");
        assert!(msg.contains("microsandbox"), "{msg}");
    }

    #[test]
    fn incompatible_image_names_the_supplied_and_expected_repositories_and_the_override() {
        let e = RightsizeError::IncompatibleImage {
            supplied: "mysql".to_string(),
            expected: "postgres".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("mysql"), "{msg}");
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("as_compatible_substitute_for"), "{msg}");
    }

    #[test]
    fn malformed_archive_names_the_path_and_the_reason() {
        let e = RightsizeError::MalformedArchive {
            path: std::path::PathBuf::from("/tmp/cp.archive"),
            reason: "unsupported rightsizeArchive version 2 (expected 1)".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("/tmp/cp.archive"), "{msg}");
        assert!(
            msg.contains("unsupported rightsizeArchive version 2 (expected 1)"),
            "{msg}"
        );
    }
}
