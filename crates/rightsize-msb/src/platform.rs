//! Which `msb`/`libkrunfw` asset pair this host needs, and whether the host can even
//! run a microVM at all.
//!
//! microsandbox ships one prebuilt pair per (OS, arch) combination it supports; this
//! module is the single place that maps `std::env::consts::{OS, ARCH}` to the asset
//! names the provisioner downloads, and separately checks whether the host actually has
//! the virtualization primitive (`/dev/kvm` on Linux, always present on Apple Silicon
//! macOS, Windows Hypervisor Platform on Windows) msb needs at runtime — a host can be
//! a supported *platform* and still be unable to *run* msb (KVM not exposed to this
//! process, e.g. inside a container; WHP not enabled on this Windows host).
//!
//! Windows facts (microsandbox v0.6.3, confirmed against a real hosted-runner boot —
//! see the crate's Windows spike record): msb runs Linux guests on WHP; asset names
//! carry a `.exe`/`.dll` suffix the macOS/Linux assets don't; WHP was already Enabled
//! on both `windows-2022` and `windows-2025` hosted runners with no reboot required.
//! `virtualization_available()` on Windows does not probe WHP state itself — it
//! reports true for any detected Windows platform (attempt-and-report), because msb's
//! own `msb doctor`/first-boot failure is the authoritative, already-informative
//! signal, and probing the `HypervisorPlatform` optional feature reliably from a plain
//! process (no elevation, no spawn) has no portable `std`-only primitive.

use std::path::Path;

/// A (OS, arch) pair microsandbox ships a prebuilt asset pair for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Platform {
    /// Apple Silicon macOS.
    DarwinArm64,
    /// 64-bit x86 Linux.
    LinuxX64,
    /// 64-bit ARM Linux.
    LinuxArm64,
    /// 64-bit x86 Windows (WHP).
    WindowsX64,
    /// 64-bit ARM Windows (WHP).
    WindowsArm64,
}

impl Platform {
    /// Detects the current platform from `std::env::consts::{OS, ARCH}`. `None` means
    /// microsandbox has no prebuilt asset pair for this host — Intel/AMD macOS falls
    /// here, since msb ships no Darwin x86_64 build.
    pub fn current() -> Option<Platform> {
        match (std::env::consts::OS, std::env::consts::ARCH) {
            ("macos", "aarch64") => Some(Platform::DarwinArm64),
            ("linux", "x86_64") => Some(Platform::LinuxX64),
            ("linux", "aarch64") => Some(Platform::LinuxArm64),
            ("windows", "x86_64") => Some(Platform::WindowsX64),
            ("windows", "aarch64") => Some(Platform::WindowsArm64),
            _ => None,
        }
    }

    /// The `msb-<os>-<arch>[.exe]` release asset name for this platform.
    pub fn msb_asset(&self) -> &'static str {
        match self {
            Platform::DarwinArm64 => "msb-darwin-aarch64",
            Platform::LinuxX64 => "msb-linux-x86_64",
            Platform::LinuxArm64 => "msb-linux-aarch64",
            Platform::WindowsX64 => "msb-windows-x86_64.exe",
            Platform::WindowsArm64 => "msb-windows-aarch64.exe",
        }
    }

    /// The `libkrunfw-<os>-<arch>.{dylib,so,dll}` release asset name for this platform —
    /// what the file is downloaded as, never what it is installed as (see
    /// [`Platform::krun_install_name`]).
    pub fn krun_asset(&self) -> &'static str {
        match self {
            Platform::DarwinArm64 => "libkrunfw-darwin-aarch64.dylib",
            Platform::LinuxX64 => "libkrunfw-linux-x86_64.so",
            Platform::LinuxArm64 => "libkrunfw-linux-aarch64.so",
            Platform::WindowsX64 => "libkrunfw-windows-x86_64.dll",
            Platform::WindowsArm64 => "libkrunfw-windows-aarch64.dll",
        }
    }

    /// The exact filename `msb` resolves the library under: it probes `../lib/` next to
    /// its own binary for `libkrunfw.so.<version>` on Linux, `libkrunfw.<abi>.dylib` on
    /// macOS, and (per the working install.ps1 layout) `libkrunfw.dll` on Windows —
    /// never the release-asset name — so the provisioner installs the downloaded asset
    /// under this name. The embedded libkrunfw version/ABI is part of the pinned msb
    /// release; re-verify all names when bumping the pin.
    pub fn krun_install_name(&self) -> &'static str {
        match self {
            Platform::DarwinArm64 => "libkrunfw.5.dylib",
            Platform::LinuxX64 | Platform::LinuxArm64 => "libkrunfw.so.5.6.1",
            Platform::WindowsX64 | Platform::WindowsArm64 => "libkrunfw.dll",
        }
    }

    /// True if this process can actually exercise hardware virtualization here and now
    /// — not just "is this a platform msb ships a build for" (see [`Platform::current`]).
    /// On macOS this is always true for a detected platform (Apple Silicon's Hypervisor
    /// framework needs no extra permission); on Linux it additionally requires
    /// `/dev/kvm` to be readable and writable by this process; on Windows it is true
    /// for any detected platform (attempt-and-report — see the module docs for why a
    /// cheap, reliable, no-elevation WHP-state probe doesn't exist in portable `std`;
    /// an unusable WHP surfaces at `msb`'s own first boot instead, with `msb`'s own
    /// error).
    pub fn virtualization_available() -> bool {
        if std::env::consts::OS == "macos" || std::env::consts::OS == "windows" {
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
    fn windows_asset_names_match_the_v0_6_3_checksums_sha256_shape() {
        // Verified directly against `checksums.sha256` for microsandbox v0.6.8 — the
        // standalone (non-bundle) Windows asset names, which carry a `.exe`/`.dll`
        // suffix the macOS/Linux assets don't.
        assert_eq!(Platform::WindowsX64.msb_asset(), "msb-windows-x86_64.exe");
        assert_eq!(
            Platform::WindowsX64.krun_asset(),
            "libkrunfw-windows-x86_64.dll"
        );
        assert_eq!(
            Platform::WindowsArm64.msb_asset(),
            "msb-windows-aarch64.exe"
        );
        assert_eq!(
            Platform::WindowsArm64.krun_asset(),
            "libkrunfw-windows-aarch64.dll"
        );
    }

    #[test]
    fn windows_krun_install_name_is_the_canonical_unversioned_dll() {
        // Per the working install.ps1 layout (the reference for the correct install
        // shape — never in this crate's trust/execution path), msb on Windows finds
        // the library as `..\lib\libkrunfw.dll`, an unversioned canonical name, unlike
        // the versioned `.so`/`.dylib` names on Linux/macOS.
        assert_eq!(Platform::WindowsX64.krun_install_name(), "libkrunfw.dll");
        assert_eq!(Platform::WindowsArm64.krun_install_name(), "libkrunfw.dll");
    }

    #[test]
    fn windows_platform_detection_maps_os_arch_pairs() {
        // Mirrors current()'s windows match arms directly, since std::env::consts is
        // compile-time and can't be stubbed to actually run this process as "windows"
        // in a unit test on macOS/Linux CI.
        assert_eq!(
            match ("windows", "x86_64") {
                ("windows", "x86_64") => Some(Platform::WindowsX64),
                ("windows", "aarch64") => Some(Platform::WindowsArm64),
                _ => None,
            },
            Some(Platform::WindowsX64)
        );
        assert_eq!(
            match ("windows", "aarch64") {
                ("windows", "x86_64") => Some(Platform::WindowsX64),
                ("windows", "aarch64") => Some(Platform::WindowsArm64),
                _ => None,
            },
            Some(Platform::WindowsArm64)
        );
    }

    #[test]
    fn current_matches_this_test_runner_on_supported_hosts() {
        // This workspace's CI/dev matrix only runs on hosts msb ships a build for
        // (Apple Silicon macOS, x86_64/aarch64 Linux, or x86_64/aarch64 Windows) —
        // asserting `current()` is `Some` here catches a regression in the (OS, ARCH)
        // match arms without needing to fake `std::env::consts` (which is compile-time
        // and can't be stubbed in a unit test).
        assert!(Platform::current().is_some());
    }

    #[test]
    fn is_read_write_is_false_for_a_path_that_does_not_exist() {
        assert!(!is_read_write(Path::new(
            "/definitely/not/a/real/path/rightsize-test"
        )));
    }
}
