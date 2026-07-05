//! `sandbox-it` integration test proving `MsbCliBackend::start` self-heals msb's
//! image-cache-corruption signature (see `backend::is_image_cache_corruption`'s doc
//! comment for the full empirical writeup).
//!
//! This corrupts a real, isolated msb cache the same way the corruption was found in
//! the first place: racing concurrent `msb run` invocations of images that share a
//! base layer against one fresh `MSB_HOME`. Deleting a layer file by hand ahead of
//! time does not reproduce the hard failure on this msb build — msb transparently
//! regenerates a missing per-layer artifact from its still-cached source whenever one
//! is deleted outside of an active pull. The only reliable trigger found is the race
//! itself, so that is what this test drives.
//!
//! Isolation: this file is its own integration-test binary (Cargo's default for each
//! `tests/*.rs` file), so setting `MSB_HOME` for the whole process here cannot leak
//! into `backend_it.rs`/`network_links_it.rs`'s own test functions, which run in a
//! separate process and use the host's real default cache location.
//!
//! Run for real against a real `msb` binary and real microVMs:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-msb --features sandbox-it --test image_cache_heal_it
//! ```

#![cfg(feature = "sandbox-it")]

use std::path::{Path, PathBuf};
use std::time::Duration;

use rightsize::backend::SandboxBackend;
use rightsize::model::ContainerSpec;
use rightsize_msb::MsbBackendProvider;
use rightsize_msb::MsbCliBackend;

/// Three real images sharing a base layer, per `backend::is_image_cache_corruption`'s
/// doc comment — the same trio whose concurrent `sandbox-it` boots reproduced this
/// corruption in CI in the first place.
const SHARED_LAYER_IMAGES: [&str; 3] = [
    "floci/floci:1.5.30",
    "floci/floci-az:0.8.0",
    "floci/floci-gcp:0.4.0",
];

fn msb_runtime_available() -> bool {
    use rightsize::backend::BackendProvider;
    if std::env::var("RIGHTSIZE_BACKEND")
        .map(|v| v.eq_ignore_ascii_case("docker"))
        .unwrap_or(false)
    {
        return false;
    }
    MsbBackendProvider.is_supported()
}

fn provisioned_msb_path() -> PathBuf {
    rightsize_msb::provisioner::ensure_installed()
        .expect("msb must already be provisioned on this host")
}

fn unique_name(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("rz-{label}-{}", &format!("{nanos:x}")[..8])
}

/// Runs `msb pull <image>` directly against `msb_home` (bypassing the backend, since
/// this setup step deliberately drives raw concurrent pulls the backend itself never
/// issues on its own) with a closed stdin, and returns its combined stdout+stderr.
/// `pull` (unlike `run`) always exits on its own whether it succeeds or fails, so this
/// never needs the attached-supervision machinery `MsbCliBackend::start` uses.
fn pull_msb_raw(msb: &Path, msb_home: &Path, image: &str) -> String {
    let output = std::process::Command::new(msb)
        .args(["pull", image])
        .env("MSB_HOME", msb_home)
        .stdin(std::process::Stdio::null())
        .output()
        .expect("msb must spawn");
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Races concurrent `msb pull` of the three shared-base-layer images against a fresh,
/// isolated `msb_home`, up to `attempts` times (a fresh cache per attempt), until one
/// hits the cache-corruption signature. Returns `true` on success. Empirically this
/// reproduced on 7 of 10 local trials (see `backend::is_image_cache_corruption`'s doc
/// comment), so `attempts` gives this a wide enough margin to be reliable in CI
/// without being unbounded. `pull` reproduces the identical corruption signature as
/// `run` does (both go through the same shared-layer cache-write path) while always
/// exiting on its own, which keeps this setup helper simple.
fn corrupt_cache_via_concurrent_pull_race(msb: &Path, msb_home: &Path, attempts: u32) -> bool {
    for _ in 0..attempts {
        let _ = std::fs::remove_dir_all(msb_home);
        std::fs::create_dir_all(msb_home).expect("create fresh isolated MSB_HOME");

        let handles: Vec<_> = SHARED_LAYER_IMAGES
            .iter()
            .map(|image| {
                let msb = msb.to_path_buf();
                let msb_home = msb_home.to_path_buf();
                let image = image.to_string();
                std::thread::spawn(move || pull_msb_raw(&msb, &msb_home, &image))
            })
            .collect();

        let mut corrupted = false;
        for h in handles {
            let output = h.join().expect("setup thread must not panic");
            if rightsize_msb::backend::is_image_cache_corruption_for_test(&output) {
                corrupted = true;
            }
        }

        if corrupted {
            return true;
        }
    }
    false
}

/// The empirical proof this task asked for: deliberately corrupt an isolated msb
/// image cache, then prove `MsbCliBackend::start` still boots a container through it
/// — the backend's classifier catches msb's cache-error signature and heals via
/// [`rightsize_msb::commands::image_remove`] plus one retry, entirely on its own.
#[tokio::test]
async fn start_self_heals_a_racily_corrupted_image_cache() {
    if !msb_runtime_available() {
        eprintln!("skipping: no supported msb runtime on this host (or RIGHTSIZE_BACKEND=docker)");
        return;
    }
    let msb_path = provisioned_msb_path();
    // A short, `/tmp`-rooted path, not `std::env::temp_dir()` — on macOS that resolves
    // under `/var/folders/<hash>/T/...`, and msb derives a per-sandbox Unix socket
    // path from `MSB_HOME` that must stay under the platform's 104-byte `sockaddr_un`
    // limit (confirmed empirically: msb hard-errors with "agent relay socket path is
    // too long" once `MSB_HOME` is that deep).
    let msb_home = PathBuf::from("/tmp").join(format!("rz-mh-{}", &unique_name("h")[3..]));

    // `MsbCliBackend` inherits `MSB_HOME` from this process's environment (it never
    // sets it explicitly on the `Command`s it spawns) — this file is its own
    // integration-test binary, so this process-wide `set_var` cannot race any other
    // test file's own env or its default `~/.microsandbox/cache`.
    //
    // Safety: `set_var`/no other thread in THIS process touches process env, and this
    // is the only test function in this binary — see this file's module doc for why
    // that makes the mutation race-free here specifically.
    unsafe {
        std::env::set_var("MSB_HOME", &msb_home);
    }

    let reproduced = corrupt_cache_via_concurrent_pull_race(&msb_path, &msb_home, 10);
    assert!(
        reproduced,
        "failed to reproduce msb's image-cache race in 10 attempts against a fresh \
         isolated MSB_HOME at {} — see backend::is_image_cache_corruption's doc \
         comment for the repro this mirrors (7 of 10 local trials reproduced it)",
        msb_home.display()
    );

    // The real assertion: boot one of the three images through the actual backend
    // (not the raw setup helper above) against this now-corrupted cache. If the
    // corruption landed on this specific image, the backend's classifier+heal+retry
    // must recover it transparently; if it landed on a sibling instead, this boot
    // either succeeds outright or exercises the same heal path when its own shared
    // layer read hits the same race window — either way `start` must return `Ok`.
    let backend = MsbCliBackend::new(msb_path);
    let spec = ContainerSpec {
        command: Some(vec!["sleep".to_string(), "30".to_string()]),
        ..ContainerSpec::new(unique_name("healed"), SHARED_LAYER_IMAGES[0], "corrupt-it")
    };
    let handle = backend.create(spec).await.expect("create");

    let start_result =
        tokio::time::timeout(Duration::from_secs(120), backend.start(handle.as_ref()))
            .await
            .expect("start must not hang");
    assert!(
        start_result.is_ok(),
        "start must self-heal the corrupted cache and boot successfully, got: {:?}",
        start_result.err()
    );

    backend.stop(handle.as_ref()).await.unwrap();
    backend.remove(handle.as_ref()).await.unwrap();

    let _ = std::fs::remove_dir_all(&msb_home);
}
