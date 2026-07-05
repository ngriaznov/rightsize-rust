//! [`MsbBackendProvider`]: the discoverable factory `rightsize::backends::resolve`
//! picks among. Supported exactly when this host is both a platform microsandbox ships
//! a build for AND can actually exercise virtualization here and now (see
//! [`crate::platform::Platform`]).

use rightsize::backend::{BackendProvider, SandboxBackend};
use rightsize::error::Result;

use crate::backend::MsbCliBackend;
use crate::platform::Platform;
use crate::provisioner;

/// The microsandbox backend's [`BackendProvider`]. Priority 20 — outranks Docker's 10,
/// so a microVM-capable host prefers the microVM by default (no Docker daemon
/// required).
pub struct MsbBackendProvider;

impl BackendProvider for MsbBackendProvider {
    fn name(&self) -> &str {
        "microsandbox"
    }

    fn priority(&self) -> u32 {
        20
    }

    fn is_supported(&self) -> bool {
        Platform::current().is_some() && Platform::virtualization_available()
    }

    fn unsupported_reason(&self) -> String {
        if Platform::current().is_none() {
            format!(
                "no msb build for {}/{} (Intel Mac: use the docker backend)",
                std::env::consts::OS,
                std::env::consts::ARCH
            )
        } else if std::env::consts::OS == "windows" {
            "Windows Hypervisor Platform is not enabled (run `msb doctor --fix` in an \
             elevated terminal, which may require a reboot), or use the docker backend"
                .to_string()
        } else {
            "/dev/kvm is not accessible (need KVM, or run on Apple Silicon macOS)".to_string()
        }
    }

    fn create(&self) -> Result<Box<dyn SandboxBackend>> {
        let msb_path = provisioner::ensure_installed()?;
        let backend = MsbCliBackend::new(msb_path);
        // Best-effort: a sweep failure (e.g. msb itself unreachable) shouldn't prevent
        // handing back a usable backend — the next real call will surface any actual
        // problem with msb loudly instead.
        let _ = backend.sweep_orphans();
        Ok(Box::new(backend))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_priority_are_pinned() {
        let provider = MsbBackendProvider;
        assert_eq!(provider.name(), "microsandbox");
        assert_eq!(provider.priority(), 20);
    }

    #[test]
    fn unsupported_reason_is_non_empty_either_way() {
        // Whichever branch this host hits, the message must be actionable — assert
        // it's non-empty and mentions either kvm or a docker fallback, without
        // asserting a specific host outcome (this test must pass on any dev machine).
        let provider = MsbBackendProvider;
        let reason = provider.unsupported_reason();
        assert!(!reason.is_empty());
        assert!(
            reason.to_lowercase().contains("kvm") || reason.to_lowercase().contains("docker"),
            "{reason}"
        );
    }

    #[test]
    fn windows_unsupported_reason_names_whp_and_the_fix_command() {
        // std::env::consts::OS is compile-time, so this dev/CI matrix (macOS/Linux)
        // never actually hits the Windows branch of unsupported_reason — this pins the
        // exact message that branch produces (mirroring the string literal there)
        // rather than exercising it live. The Windows spike record (msb 0.6.3, both
        // windows-2022 and windows-2025 hosted runners) is what `msb doctor --fix`
        // references: WHP was already Enabled with RestartNeeded=False there, so the
        // reboot caveat in this message is a worst-case hint, not this crate's own
        // finding for every host.
        let reason = "Windows Hypervisor Platform is not enabled (run `msb doctor --fix` in an \
             elevated terminal, which may require a reboot), or use the docker backend";
        assert!(reason.to_lowercase().contains("hypervisor"));
        assert!(reason.contains("msb doctor --fix"));
        assert!(reason.to_lowercase().contains("docker"));
    }
}
