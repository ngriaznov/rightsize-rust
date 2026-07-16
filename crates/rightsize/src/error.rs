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
    /// versa. Raised in `Container::start()`, before any backend work. The message
    /// names both backends and the `RIGHTSIZE_BACKEND=<creator>` remedy.
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
/// the checkpoint's creator, plus the `RIGHTSIZE_BACKEND=<creator>` remedy.
fn format_checkpoint_backend_mismatch(active_backend: &str, checkpoint_backend: &str) -> String {
    format!(
        "Container::from_checkpoint(...) was started under the '{active_backend}' backend, but \
         this checkpoint was created by the '{checkpoint_backend}' backend — set \
         RIGHTSIZE_BACKEND={checkpoint_backend} to restore it, or take a fresh checkpoint under \
         '{active_backend}' instead"
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
    fn invalid_checkpoint_name_names_the_rejected_name_and_the_pattern() {
        let e = RightsizeError::InvalidCheckpointName {
            name: "Bad Name!".to_string(),
        };
        let msg = e.to_string();
        assert!(msg.contains("Bad Name!"), "{msg}");
        assert!(msg.contains("^[a-z0-9][a-z0-9-]{0,40}$"), "{msg}");
    }
}
