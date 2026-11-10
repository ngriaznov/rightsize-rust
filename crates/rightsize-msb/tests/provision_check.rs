//! A live provisioning check: [`ensure_installed`] downloads the pinned msb release for
//! this host, verifies every asset against the release's `checksums.sha256` and installs
//! it, and the installed binary then has to run and report the pinned version. It needs
//! network access but no VM, which is why the macOS workflow (`.github/workflows/macos.yml`)
//! runs it: GitHub's hosted Apple Silicon runners cannot boot microVMs. It runs on any host
//! msb supports:
//!
//! ```sh
//! cargo test -p rightsize-msb --features msb-provision-check --test provision_check
//! ```
//!
//! Set `RIGHTSIZE_CACHE_DIR` to an empty directory to force a real download; with an
//! install already cached, this only checks that one.

#![cfg(feature = "msb-provision-check")]

use std::process::Command;

use rightsize_msb::provisioner::{MSB_VERSION, ensure_installed};

#[test]
fn provisions_the_pinned_msb_and_it_runs() {
    let msb = ensure_installed().expect("provisioning the pinned msb");
    let out = Command::new(&msb)
        .arg("--version")
        .output()
        .expect("spawning msb --version");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "{} --version exited with {}: {stdout}{stderr}",
        msb.display(),
        out.status
    );
    assert!(
        stdout.contains(MSB_VERSION),
        "expected msb {MSB_VERSION}, got: {stdout}"
    );
}
