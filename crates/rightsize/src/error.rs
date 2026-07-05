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
}
