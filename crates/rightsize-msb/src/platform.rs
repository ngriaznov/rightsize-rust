//! Which `msb`/`libkrunfw` asset pair this host needs, and whether the host can even
//! run a microVM at all.
//!
//! microsandbox ships one prebuilt pair per (OS, arch) combination it supports; this
//! module is the single place that maps `std::env::consts::{OS, ARCH}` to the asset
//! names the provisioner downloads, and separately checks whether the host actually has
//! the virtualization primitive (`/dev/kvm` on Linux, always present on Apple Silicon
//! macOS) msb needs at runtime — a host can be a supported *platform* and still be
//! unable to *run* msb (KVM not exposed to this process, e.g. inside a container).

use std::path::Path;

/// A (OS, arch) pair microsandbox 0.6.2 ships a prebuilt asset pair for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Platform {
    /// Apple Silicon macOS.
    DarwinArm64,
    /// 64-bit x86 Linux.
    LinuxX64,
    /// 64-bit ARM Linux.
    LinuxArm64,
}

impl Platform {
    /// Detects the current platform from `std::env::consts::{OS, ARCH}`. `None` means
    /// microsandbox has no prebuilt asset pair for this host — Intel/AMD macOS and
    /// Windows fall here, since msb 0.6.2 ships neither.
    pub fn current() -> Option<Platform> {
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("macos", "aarch64") => Some(Platform::DarwinArm64),
            ("linux", "x86_64") => Some(Platform::LinuxX64),
            ("linux", "aarch64") => Some(Platform::LinuxArm64),
            _ => None,
        }
    }

    /// The `msb-<os>-<arch>` release asset name for this platform.
    pub fn msb_asset(&self) -> &'static str {
        match self {
            Platform::DarwinArm64 => "msb-darwin-aarch64",
            Platform::LinuxX64 => "msb-linux-x86_64",
            Platform::LinuxArm64 => "msb-linux-aarch64",
        }
    }

    /// The `libkrunfw-<os>-<arch>.{dylib,so}` release asset name for this platform —
    /// what the file is downloaded as, never what it is installed as (see
    /// [`Platform::krun_install_name`]).
    pub fn krun_asset(&self) -> &'static str {
        match self {
            Platform::DarwinArm64 => "libkrunfw-darwin-aarch64.dylib",
            Platform::LinuxX64 => "libkrunfw-linux-x86_64.so",
            Platform::LinuxArm64 => "libkrunfw-linux-aarch64.so",
        }
    }

    /// The exact filename `msb` resolves the library under: it probes `../lib/` next to
    /// its own binary for `libkrunfw.so.<version>` on Linux and `libkrunfw.<abi>.dylib`
    /// on macOS — never the release-asset name — so the provisioner installs the
    /// downloaded asset under this name. The embedded libkrunfw version/ABI is part of
    /// the pinned msb release; re-verify both names when bumping the pin.
    pub fn krun_install_name(&self) -> &'static str {
        match self {
            Platform::DarwinArm64 => "libkrunfw.5.dylib",
            Platform::LinuxX64 | Platform::LinuxArm64 => "libkrunfw.so.5.5.0",
        }
    }

    /// True if this process can actually exercise hardware virtualization here and now
    /// — not just "is this a platform msb ships a build for" (see [`Platform::current`]).
    /// On macOS this is always true for a detected platform (Apple Silicon's Hypervisor
    /// framework needs no extra permission); on Linux it additionally requires
    /// `/dev/kvm` to be readable and writable by this process.
    pub fn virtualization_available() -> bool {
        if std::env::consts::OS == "macos" {
            return Platform::current().is_some();
        }
        if std::env::consts::OS == "linux" {
            return is_read_write(Path::new("/dev/kvm"));
        }
        false
    }
}

/// True if `path` exists and this process can both read and write it — the closest
/// portable-`std` approximation of "can I use `/dev/kvm`" without a `libc`/`nix` dep.
fn is_read_write(path: &Path) -> bool {
    use std::fs::OpenOptions;
    OpenOptions::new().read(true).write(true).open(path).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_names_match_the_pinned_release_shape() {
        assert_eq!(Platform::DarwinArm64.msb_asset(), "msb-darwin-aarch64");
        assert_eq!(
            Platform::DarwinArm64.krun_asset(),
            "libkrunfw-darwin-aarch64.dylib"
        );
        assert_eq!(Platform::LinuxX64.msb_asset(), "msb-linux-x86_64");
        assert_eq!(Platform::LinuxX64.krun_asset(), "libkrunfw-linux-x86_64.so");
        assert_eq!(Platform::LinuxArm64.msb_asset(), "msb-linux-aarch64");
        assert_eq!(
            Platform::LinuxArm64.krun_asset(),
            "libkrunfw-linux-aarch64.so"
        );
    }

    #[test]
    fn current_matches_this_test_runner_on_supported_hosts() {
        // This workspace's CI/dev matrix only runs on hosts msb 0.6.2 supports (Apple
        // Silicon macOS or x86_64/aarch64 Linux) — asserting `current()` is `Some` here
        // catches a regression in the (OS, ARCH) match arms without needing to fake
        // `std::env::consts` (which is compile-time and can't be stubbed in a unit test).
        assert!(Platform::current().is_some());
    }

    #[test]
    fn is_read_write_is_false_for_a_path_that_does_not_exist() {
        assert!(!is_read_write(Path::new(
            "/definitely/not/a/real/path/rightsize-test"
        )));
    }
}
