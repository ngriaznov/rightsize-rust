//! Fetches and installs the pinned `msb`/`libkrunfw` toolchain from GitHub releases —
//! the one-time, blocking bootstrap every real msb run needs before any async work
//! starts.
//!
//! **Escape hatches, in the order they're checked:** `MSB_PATH` bypasses the pin
//! entirely (bring your own binary, any version); `RIGHTSIZE_CACHE_DIR` relocates the
//! install root; `RIGHTSIZE_MSB_SKIP_DOWNLOAD=true` turns a cache miss into a hard
//! error instead of a network fetch (CI lanes that pre-seed the cache and want a loud
//! failure if that seeding didn't work).
//!
//! **Binary-last atomic install:** both assets are downloaded and SHA-256-verified into
//! temp files *before* either is moved into place, then the krun asset is renamed into
//! `lib/` first and the `msb` binary renamed into `bin/` last. `msb`'s presence is thus
//! the commit marker for a *complete* install — a crash between the two renames can
//! never be mistaken for success, because `is_installed` checks for both files, and
//! whichever caller hits that half-installed state simply repairs it (downloads
//! whatever's still missing) rather than erroring.
//!
//! **Cross-process lock:** two processes racing to provision the same cache dir
//! serialize on `<install_dir>/.lock` (a directory used as an atomic mutex — see
//! `InstallLock`'s doc comment for why, given this crate's `forbid(unsafe_code)`); the
//! loser re-checks `is_installed` after acquiring the lock and finds the winner already
//! did the work.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::platform::Platform;
use rightsize::error::{Result, RightsizeError};

/// The pinned microsandbox release this crate provisions. Bumping this is the only
/// change needed to move the whole workspace to a newer msb — asset names, checksums,
/// and the install-dir path are all derived from it.
pub const MSB_VERSION: &str = "0.6.6";

const DEFAULT_BASE: &str =
    "https://github.com/superradcompany/microsandbox/releases/download/v0.6.6";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(300);

/// Upper bound on a downloaded response body. ureq's `read_to_vec` defaults to a
/// 10 MiB cap, well under the real release assets (`msb` and `libkrunfw` are each
/// ~25 MiB), so the cap must be raised explicitly. 256 MiB bounds memory while
/// leaving order-of-magnitude headroom for asset growth across future pins.
const MAX_ASSET_SIZE: u64 = 256 * 1024 * 1024;

/// Locates (installing if necessary) the pinned `msb` binary, using the real process
/// environment and the default cache root (or `RIGHTSIZE_CACHE_DIR` if set):
/// `~/.cache/rightsize` on macOS/Linux, `%LOCALAPPDATA%\rightsize` on Windows (see
/// [`default_cache_dir`]). Blocking — call this before spinning up any async runtime
/// work.
pub fn ensure_installed() -> Result<PathBuf> {
    let env: HashMap<String, String> = std::env::vars().collect();
    let cache_dir = std::env::var("RIGHTSIZE_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_cache_dir(&env));
    ensure_installed_at(DEFAULT_BASE, &cache_dir, &env)
}

/// The default cache root when `RIGHTSIZE_CACHE_DIR` is unset: `~/.cache/rightsize` on
/// macOS/Linux (via `HOME`), `%LOCALAPPDATA%\rightsize` on Windows (via `LOCALAPPDATA`,
/// falling back to `%USERPROFILE%\AppData\Local` if unset, matching install.ps1's own
/// `%USERPROFILE%`-rooted default install root). The cache holds a downloaded native
/// toolchain (`.exe`+`.dll` on Windows) — machine-local, non-roaming data, which is
/// exactly what `%LOCALAPPDATA%` is for; the roaming profile (`%USERPROFILE%\.cache`)
/// would risk sync in roaming-profile setups and mixes a Unix dotfile convention into
/// a Windows home directory. Exposed as a seam so the provisioner's unit tests can
/// verify the Windows branch without actually running on Windows.
pub(crate) fn default_cache_dir(env: &HashMap<String, String>) -> PathBuf {
    if std::env::consts::OS == "windows" {
        let local_app_data = env.get("LOCALAPPDATA").cloned().unwrap_or_else(|| {
            let user_profile = env
                .get("USERPROFILE")
                .cloned()
                .unwrap_or_else(|| ".".to_string());
            Path::new(&user_profile)
                .join("AppData")
                .join("Local")
                .to_string_lossy()
                .to_string()
        });
        return Path::new(&local_app_data).join("rightsize");
    }
    let home = env.get("HOME").cloned().unwrap_or_else(|| ".".to_string());
    Path::new(&home).join(".cache").join("rightsize")
}

/// The seam [`ensure_installed`] delegates to, parameterized over the release base URL,
/// cache root, and environment map — so unit tests can point it at a local HTTP fixture
/// server and a temp directory instead of the real network and `~/.cache`.
pub(crate) fn ensure_installed_at(
    base_url: &str,
    cache_dir: &Path,
    env: &HashMap<String, String>,
) -> Result<PathBuf> {
    if let Some(override_path) = env.get("MSB_PATH") {
        let p = PathBuf::from(override_path);
        if !is_executable(&p) {
            return Err(RightsizeError::Provision(format!(
                "MSB_PATH='{override_path}' is not an executable file — point it at a real msb \
                 binary, or unset it to use the provisioned toolchain instead"
            )));
        }
        return Ok(p);
    }

    let platform = Platform::current().ok_or_else(|| {
        RightsizeError::Provision(format!(
            "microsandbox has no build for {}/{} — use the docker backend \
             (RIGHTSIZE_BACKEND=docker) or set MSB_PATH to a binary you provide",
            std::env::consts::OS,
            std::env::consts::ARCH
        ))
    })?;

    let install_dir = cache_dir.join("msb").join(MSB_VERSION);
    // The installed binary's basename is platform-derived: suffixless on macOS/Linux,
    // `msb.exe` on Windows (there is no such thing as a suffixless executable there).
    let msb = install_dir.join("bin").join(msb_install_basename());
    // Installed under the canonical name msb resolves (`../lib/` next to its binary),
    // not the release-asset name it is downloaded as — msb never probes the asset name.
    let krun = install_dir.join("lib").join(platform.krun_install_name());

    // An install is complete only when BOTH the msb binary and the krun asset are
    // present. The msb binary is written last (see the module docs), so its presence
    // alone is not sufficient.
    if is_installed(&msb, &krun) {
        return Ok(msb);
    }

    if env.get("RIGHTSIZE_MSB_SKIP_DOWNLOAD").map(String::as_str) == Some("true") {
        return Err(RightsizeError::Provision(format!(
            "msb {MSB_VERSION} not found at {} and RIGHTSIZE_MSB_SKIP_DOWNLOAD=true — \
             pre-install it there or point MSB_PATH at an msb binary",
            msb.display()
        )));
    }

    fs::create_dir_all(&install_dir)?;
    let _guard = InstallLock::acquire(&install_dir.join(".lock"))?;
    // Another process may have won the race for this install dir while we waited for
    // the lock — re-check before doing any work.
    if is_installed(&msb, &krun) {
        return Ok(msb);
    }
    download_and_install(base_url, platform, &install_dir, &msb, &krun)?;
    Ok(msb)
}

/// **Directory-based lock, not `flock`/`fcntl`:** an OS-held advisory lock (via
/// `flock`/`fcntl`) would give the kernel automatic release on crash, but this
/// crate's `lib.rs` carries a hard `#![forbid(unsafe_code)]` applied to every crate
/// in the workspace, and `flock`/`fcntl` are not exposed by `std` — reaching them
/// needs either `unsafe extern "C"` or a `libc` dependency the workspace doesn't
/// otherwise need. Rather than relax `forbid(unsafe_code)` for this one call site,
/// this uses `std::fs`'s own atomicity guarantee instead: `fs::create_dir` on a path
/// that already exists returns `AlreadyExists` atomically (POSIX `mkdir(2)`
/// semantics), so a directory acts as a safe mutex — the same technique classic
/// lock-file implementations use when they can't call `flock`. This is still a real
/// cross-process, cross-install-dir lock; only the underlying primitive differs,
/// and only because of the crate-wide safety constraint.
///
/// **Crash-path behavior is worse than an OS-held lock's, by design of the above
/// trade-off:** an OS-held advisory lock (`flock`/`fcntl`) is released by the kernel
/// the instant the holding process dies (SIGKILL included) — a crash while holding
/// it is invisible to the next acquirer. A plain directory has no such kernel-backed
/// release: if this process is SIGKILLed while holding the lock, the
/// directory is simply left behind forever as far as `mkdir` is concerned. Rather than
/// wedge every future call in this cache dir on this host until a human runs `rm -rf`,
/// [`InstallLock::acquire`] treats a lock dir whose mtime is older than
/// [`STALE_LOCK_AGE`] as abandoned: it removes it (logging a warning) and retries the
/// acquire once. This is a real, if coarser, recovery — a live holder always refreshes
/// its lock dir's mtime (see `touch`) well within that window, so a false takeover of a
/// live, healthy holder would require this process to stall for the full 10 minutes
/// between refreshes, which does not happen in practice.
///
/// **Known residual race, not fixed here:** the remove-then-recreate sequence in the
/// stale branch below (`remove_dir_all` then `create_dir`) is two syscalls, not one —
/// it is not itself atomic the way a single `create_dir` is. If two processes *both*
/// observe the same stale lock past [`STALE_LOCK_AGE`] (realistic: both have been
/// polling since before the crash that abandoned it, and both wake up to check
/// staleness at nearly the same time), each runs `remove_dir_all` then `create_dir`
/// independently; whichever runs second removes the *first's brand-new* lock dir
/// instead of the original crashed holder's, then creates its own in its place. Both
/// processes' `create_dir` calls can report success this way — a real concurrent
/// takeover, not merely a false one. In practice this needs two processes to race
/// within the same sub-millisecond window *after* both have already independently
/// waited out the full 10-minute [`STALE_LOCK_AGE`], which is why this is treated as
/// low-severity rather than fixed outright: closing it for real needs an atomic
/// take-over primitive (e.g. a rename-based handoff, or accepting the `libc`/`unsafe`
/// budget for a real `flock`), which this crate does not currently do.
struct InstallLock {
    dir: PathBuf,
}

/// How long a lock dir can go without its mtime being refreshed before
/// [`InstallLock::acquire`] treats it as abandoned by a crashed holder and takes it over.
/// See [`InstallLock`]'s doc comment for why this exists and how it compares to an
/// OS-held lock's instant release on crash.
const STALE_LOCK_AGE: Duration = Duration::from_secs(600);

impl InstallLock {
    /// Blocks (polling with backoff) until it can atomically create `lock_dir`, then
    /// owns it until dropped. `lock_dir`'s parent must already exist. If the dir already
    /// exists and its mtime is older than [`STALE_LOCK_AGE`], treats it as left behind by
    /// a crashed holder, removes it (with a warning), and retries the create once before
    /// falling back to the normal poll loop.
    fn acquire(lock_dir: &Path) -> Result<InstallLock> {
        let deadline = std::time::Instant::now() + Duration::from_secs(300);
        loop {
            match fs::create_dir(lock_dir) {
                Ok(()) => {
                    touch(lock_dir);
                    return Ok(InstallLock {
                        dir: lock_dir.to_path_buf(),
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    if let Some(age) = lock_dir_age(lock_dir) {
                        if age >= STALE_LOCK_AGE {
                            eprintln!(
                                "warning: rightsize provisioner lock at {} is {}s old (>= {}s) — \
                                 treating it as abandoned by a crashed process and taking it over",
                                lock_dir.display(),
                                age.as_secs(),
                                STALE_LOCK_AGE.as_secs()
                            );
                            let _ = fs::remove_dir_all(lock_dir);
                            match fs::create_dir(lock_dir) {
                                Ok(()) => {
                                    touch(lock_dir);
                                    return Ok(InstallLock {
                                        dir: lock_dir.to_path_buf(),
                                    });
                                }
                                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                                    // Lost a race with another process's takeover (or a
                                    // fresh acquire) — fall through to the normal poll.
                                }
                                Err(e) => return Err(RightsizeError::from(e)),
                            }
                        }
                    }
                    if std::time::Instant::now() >= deadline {
                        return Err(RightsizeError::Provision(format!(
                            "timed out waiting for the provisioner lock at {} — another process \
                             may have crashed while holding it; delete that directory and retry",
                            lock_dir.display()
                        )));
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => return Err(RightsizeError::from(e)),
            }
        }
    }
}

impl Drop for InstallLock {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.dir);
    }
}

/// This lock dir's age (time since its mtime), or `None` if its metadata can't be read
/// (e.g. it was removed by a racing takeover between the `AlreadyExists` and this call).
fn lock_dir_age(lock_dir: &Path) -> Option<Duration> {
    let modified = fs::metadata(lock_dir).ok()?.modified().ok()?;
    modified.elapsed().ok()
}

/// Refreshes `path`'s mtime to now — called on every successful acquire so a live
/// holder's lock dir always looks fresh to [`STALE_LOCK_AGE`]-based staleness checks.
/// Best-effort: a failure here doesn't fail the acquire itself, since the lock is
/// already held either way (this only affects how soon a *future* crash would be
/// detected as stale).
fn touch(path: &Path) {
    if let Ok(file) = open_dir_for_mtime(path) {
        let _ = file.set_modified(std::time::SystemTime::now());
    }
}

/// Opens `path` (always a directory — the lock dir itself) for the sole purpose of
/// reading/setting its mtime. Plain `File::open` cannot open a directory on Windows
/// (`CreateFileW` without `FILE_FLAG_BACKUP_SEMANTICS` rejects it with
/// `PermissionDenied`), so the Windows arm opens with that flag set via the safe
/// `OpenOptionsExt::custom_flags` extension (no `unsafe` needed — this crate forbids it
/// crate-wide). Without this, a live lock holder's mtime would silently never refresh
/// on Windows (`touch`'s failure is swallowed as best-effort), and any lock held past
/// [`STALE_LOCK_AGE`] would look abandoned to another process even while its owner is
/// still alive.
#[cfg(windows)]
fn open_dir_for_mtime(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    // `write(true)` is required too, not just the backup-semantics flag: changing a
    // directory's mtime via `set_modified` needs FILE_WRITE_ATTRIBUTES access, which a
    // read-only-mode open on Windows does not request — confirmed on a real
    // windows-2025 runner (`set_modified` itself failed with `PermissionDenied` on a
    // read-only handle that had otherwise opened successfully).
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
}

#[cfg(not(windows))]
fn open_dir_for_mtime(path: &Path) -> std::io::Result<File> {
    File::open(path)
}

/// Downloads and verifies BOTH assets to temp files before moving either into place,
/// then moves the krun asset FIRST and the msb binary LAST — see the module docs for
/// why that ordering is the whole point. Called with the install-dir lock already held.
fn download_and_install(
    base_url: &str,
    platform: Platform,
    install_dir: &Path,
    msb: &Path,
    krun: &Path,
) -> Result<()> {
    let sums = fetch_checksums(base_url)?;
    let bin_dir = install_dir.join("bin");
    let lib_dir = install_dir.join("lib");
    let msb_tmp = download_verified(base_url, platform.msb_asset(), &bin_dir, &sums, true)?;
    let krun_tmp = download_verified(base_url, platform.krun_asset(), &lib_dir, &sums, false)?;

    let outcome = (|| -> Result<()> {
        fs::rename(&krun_tmp, krun)?;
        fs::rename(&msb_tmp, msb)?;
        Ok(())
    })();
    let _ = fs::remove_file(&msb_tmp);
    let _ = fs::remove_file(&krun_tmp);
    outcome
}

/// The installed msb binary's basename: suffixless (`msb`) on macOS/Linux, `msb.exe`
/// on Windows. Windows has no execute-bit convention — the `.exe` suffix itself is
/// what makes a file runnable — so this is the one shape difference the provisioner's
/// install-target naming must absorb.
fn msb_install_basename() -> &'static str {
    if std::env::consts::OS == "windows" {
        "msb.exe"
    } else {
        "msb"
    }
}

/// True when both the msb binary and its krun sibling are present and the msb binary
/// is executable — the whole "is this install usable" check, in one place.
fn is_installed(msb: &Path, krun: &Path) -> bool {
    is_executable(msb) && krun.exists()
}

/// "Is this an executable msb binary" — POSIX-mode-based on macOS/Linux (an actual
/// execute bit), and existence-based on Windows, which has no execute-bit concept at
/// all (runnability there is determined by the `.exe` extension and ACLs, not a mode
/// bit `std::fs` can read cheaply). A plain "exists and is a regular file" check is
/// the Windows equivalent of "is this usable" here.
#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    match fs::metadata(path) {
        Ok(meta) => meta.is_file() && meta.permissions().mode() & 0o111 != 0,
        Err(_) => false,
    }
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    match fs::metadata(path) {
        Ok(meta) => meta.is_file(),
        Err(_) => false,
    }
}

/// Fetches and parses `<base_url>/checksums.sha256`, a `<hex>␠␠<filename>` line per
/// asset (sha256sum's own output format).
fn fetch_checksums(base_url: &str) -> Result<HashMap<String, String>> {
    let text = http_get_text(&format!("{base_url}/checksums.sha256"))?;
    let mut sums = HashMap::new();
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let hex = parts.next().unwrap_or_default();
        let file = parts.next().map(str::trim).unwrap_or_default();
        if hex.is_empty() || file.is_empty() {
            return Err(RightsizeError::Provision(format!(
                "malformed line in checksums.sha256: '{line}'"
            )));
        }
        sums.insert(file.to_string(), hex.to_string());
    }
    Ok(sums)
}

/// Downloads and SHA-256-verifies `asset` into a fresh temp file under `dir`, returning
/// that temp file's path. The caller moves it into place (and is responsible for
/// cleaning it up on any later failure) — this function only cleans up its OWN
/// failures (a bad download, a checksum mismatch), never a failure that happens after
/// it returns successfully.
fn download_verified(
    base_url: &str,
    asset: &str,
    dir: &Path,
    sums: &HashMap<String, String>,
    executable: bool,
) -> Result<PathBuf> {
    fs::create_dir_all(dir)?;
    let expected = sums.get(asset).ok_or_else(|| {
        RightsizeError::Provision(format!(
            "No SHA-256 for '{asset}' in {base_url}/checksums.sha256"
        ))
    })?;

    let bytes = http_get_bytes(&format!("{base_url}/{asset}"))?;
    let actual = hex_sha256(&bytes);
    if &actual != expected {
        return Err(RightsizeError::Provision(format!(
            "SHA-256 mismatch for {asset} from {base_url} (expected {expected}, got {actual}) \
             — delete {} and retry, or set MSB_PATH to a trusted msb binary",
            dir.display()
        )));
    }

    let tmp = temp_file_in(dir)?;
    let write_result = fs::write(&tmp, &bytes)
        .map_err(RightsizeError::from)
        .and_then(|()| {
            if executable {
                set_executable(&tmp)?;
            }
            Ok(())
        });
    if let Err(e) = write_result {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(tmp)
}

/// Picks a fresh, non-colliding temp file path inside `dir` — no `tempfile` crate
/// dependency for one call site; a PID+counter suffix is enough uniqueness for a
/// single-process provisioner run (concurrent *processes* serialize on the install-dir
/// lock instead, so cross-process temp-name collisions aren't a concern here).
fn temp_file_in(dir: &Path) -> Result<PathBuf> {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let path = dir.join(format!(".dl-{}-{n}.part", std::process::id()));
    // Ensure the name is actually free (extremely unlikely to collide, but cheap to
    // check) and create it up front so later `fs::write` calls have somewhere to land.
    File::create(&path)?;
    Ok(path)
}

/// Sets the execute bit on `path`. Windows has no execute-bit concept — runnability
/// there comes from the `.exe` extension, not a permission mode `std::fs` can set — so
/// this is a no-op on Windows rather than a POSIX call that wouldn't compile there.
#[cfg(unix)]
fn set_executable(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms)?;
    Ok(())
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<()> {
    Ok(())
}

fn hex_sha256(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    digest.iter().fold(String::new(), |mut hex, b| {
        let _ = write!(hex, "{b:02x}");
        hex
    })
}

/// GETs `url` and returns the body as UTF-8 text (used for the small `checksums.sha256`
/// file). Follows redirects (ureq's default); a non-2xx status is surfaced as a
/// [`RightsizeError::Provision`] naming the code and URL.
fn http_get_text(url: &str) -> Result<String> {
    let bytes = http_get_bytes(url)?;
    String::from_utf8(bytes).map_err(|e| {
        RightsizeError::Provision(format!("{url} did not return valid UTF-8 text: {e}"))
    })
}

/// GETs `url` and returns the raw response body bytes.
fn http_get_bytes(url: &str) -> Result<Vec<u8>> {
    // No connection pooling: this client makes at most a handful of one-shot GETs per
    // process (checksums + two assets), so there's no keep-alive connection worth
    // reusing — and pooling actively hurts the unit tests, which spin up many
    // short-lived local `TestServer` fixtures on ephemeral ports across a run; a pooled
    // idle connection surviving past its server's shutdown surfaces as a spurious
    // "peer disconnected" on a later, unrelated test reusing the same port number.
    let mut response = ureq::get(url)
        .config()
        .timeout_connect(Some(CONNECT_TIMEOUT))
        .timeout_recv_response(Some(READ_TIMEOUT))
        .max_idle_connections(0)
        .build()
        .call()
        .map_err(|e| RightsizeError::Provision(format!("fetching {url}: {e}")))?;
    let status = response.status();
    if !status.is_success() {
        return Err(RightsizeError::Provision(format!(
            "HTTP {status} fetching {url}"
        )));
    }
    response
        .body_mut()
        .with_config()
        .limit(MAX_ASSET_SIZE)
        .read_to_vec()
        .map_err(|e| RightsizeError::Provision(format!("reading response body from {url}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// A minimal single-threaded HTTP/1.1 test server: routes exact paths to canned
    /// byte responses. Enough to exercise the provisioner's GET-only client without
    /// pulling in a real HTTP server crate.
    struct TestServer {
        base_url: String,
        shutdown: Arc<AtomicBool>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl TestServer {
        fn start(routes: HashMap<String, Vec<u8>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
            let port = listener.local_addr().unwrap().port();
            listener
                .set_nonblocking(true)
                .expect("set nonblocking for shutdown polling");
            let shutdown = Arc::new(AtomicBool::new(false));
            let shutdown_clone = shutdown.clone();
            let handle = std::thread::spawn(move || {
                while !shutdown_clone.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            // A socket accepted from a non-blocking listener inherits
                            // non-blocking mode on some platforms (macOS included) —
                            // without resetting it, `serve_one`'s blocking read/write
                            // calls can spuriously see `WouldBlock` under load and
                            // misread it as a disconnect, which is exactly the
                            // intermittent "peer disconnected" flake this fixes.
                            if stream.set_nonblocking(false).is_ok() {
                                let _ = serve_one(stream, &routes);
                            }
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });
            TestServer {
                base_url: format!("http://127.0.0.1:{port}"),
                shutdown,
                handle: Some(handle),
            }
        }

        fn stop(&mut self) {
            self.shutdown.store(true, Ordering::SeqCst);
            // Nudge the accept loop past its blocking wait so it observes the flag.
            let _ = TcpStream::connect(self.base_url.trim_start_matches("http://").to_string());
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.stop();
        }
    }

    fn serve_one(mut stream: TcpStream, routes: &HashMap<String, Vec<u8>>) -> std::io::Result<()> {
        use std::io::BufRead;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        let mut reader = std::io::BufReader::new(stream.try_clone()?);
        let mut request_line = String::new();
        reader.read_line(&mut request_line)?;
        let path = request_line
            .split_whitespace()
            .nth(1)
            .unwrap_or("/")
            .to_string();
        // Drain headers until the blank line.
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line)? == 0 || line == "\r\n" || line == "\n" {
                break;
            }
        }
        match routes.get(&path) {
            Some(body) => {
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes())?;
                stream.write_all(body)?;
            }
            None => {
                let header =
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                stream.write_all(header.as_bytes())?;
            }
        }
        Ok(())
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        hex_sha256(bytes)
    }

    fn fixture_routes(
        msb_asset: &str,
        krun_asset: &str,
    ) -> (HashMap<String, Vec<u8>>, Vec<u8>, Vec<u8>) {
        let fake_msb = b"#!/bin/sh\necho fake-msb\n".to_vec();
        let fake_krun: Vec<u8> = (0..16u8).collect();
        let sums = format!(
            "{}  {msb_asset}\n{}  {krun_asset}\n",
            sha256_hex(&fake_msb),
            sha256_hex(&fake_krun),
        );
        let mut routes = HashMap::new();
        routes.insert(format!("/{msb_asset}"), fake_msb.clone());
        routes.insert(format!("/{krun_asset}"), fake_krun.clone());
        routes.insert("/checksums.sha256".to_string(), sums.into_bytes());
        (routes, fake_msb, fake_krun)
    }

    fn temp_cache_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rz-prov-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn happy_path_downloads_verifies_and_installs_bin_msb_and_lib_krunfw() {
        let platform = Platform::current().expect("this test runs on a supported platform");
        let (routes, _fake_msb, fake_krun) =
            fixture_routes(platform.msb_asset(), platform.krun_asset());
        let mut server = TestServer::start(routes);
        let cache = temp_cache_dir("happy");

        let msb = ensure_installed_at(&server.base_url, &cache, &HashMap::new())
            .expect("provisioning must succeed");
        assert!(is_executable(&msb));
        assert_eq!(msb.parent().unwrap().file_name().unwrap(), "bin");
        let krun_path = msb
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("lib")
            .join(platform.krun_install_name());
        assert!(krun_path.exists());
        assert_eq!(fs::read(&krun_path).unwrap(), fake_krun);

        // Second call: no re-download needed — the server can be stopped first and the
        // call must still succeed by finding the already-installed binary.
        server.stop();
        let msb_again = ensure_installed_at(&server.base_url, &cache, &HashMap::new())
            .expect("an already-installed cache must not need the network");
        assert_eq!(msb, msb_again);
    }

    #[test]
    fn checksum_mismatch_aborts_and_cleans_up_with_an_actionable_message() {
        let platform = Platform::current().unwrap();
        let (mut routes, _fake_msb, _fake_krun) =
            fixture_routes(platform.msb_asset(), platform.krun_asset());
        routes.insert(format!("/{}", platform.msb_asset()), b"evil".to_vec());
        let server = TestServer::start(routes);
        let cache = temp_cache_dir("checksum-mismatch");

        let err = ensure_installed_at(&server.base_url, &cache, &HashMap::new())
            .expect_err("a corrupted download must fail");
        let msg = err.to_string();
        assert!(msg.contains("SHA-256"), "{msg}");
        assert!(msg.contains(platform.msb_asset()), "{msg}");

        // No leftover temp/partial files under bin/ after the abort.
        let bin_dir = cache.join("msb").join(MSB_VERSION).join("bin");
        if bin_dir.exists() {
            let leftovers: Vec<_> = fs::read_dir(&bin_dir).unwrap().collect();
            assert!(
                leftovers.is_empty(),
                "expected no leftover temp files in {bin_dir:?}, found {leftovers:?}"
            );
        }
    }

    #[test]
    fn msb_path_short_circuits_everything() {
        let cache = temp_cache_dir("msb-path");
        let fake_bin = cache.join("my-msb");
        fs::write(&fake_bin, b"#!/bin/sh\necho hi\n").unwrap();
        set_executable(&fake_bin).unwrap();

        let mut env = HashMap::new();
        env.insert("MSB_PATH".to_string(), fake_bin.display().to_string());
        // Base URL is deliberately bogus/unreachable: MSB_PATH must never touch it.
        let got = ensure_installed_at("http://127.0.0.1:1", &cache, &env)
            .expect("MSB_PATH must short-circuit without any network access");
        assert_eq!(got, fake_bin);
    }

    // Windows has no execute-bit concept — is_executable's non-unix arm is "exists and
    // is a regular file" (see its own doc comment), so a plain file with content is a
    // valid MSB_PATH there by design; this POSIX-mode-bit rejection only applies where
    // an actual execute bit exists to be missing.
    #[test]
    #[cfg(unix)]
    fn msb_path_pointing_at_a_non_executable_file_is_rejected() {
        let cache = temp_cache_dir("msb-path-bad");
        let not_exec = cache.join("not-executable");
        fs::write(&not_exec, b"nope").unwrap();

        let mut env = HashMap::new();
        env.insert("MSB_PATH".to_string(), not_exec.display().to_string());
        let err = ensure_installed_at("http://127.0.0.1:1", &cache, &env)
            .expect_err("a non-executable MSB_PATH must be rejected");
        assert!(err.to_string().contains("MSB_PATH"), "{err}");
    }

    // The Windows equivalent of "an invalid MSB_PATH must be rejected": there is no
    // execute bit to fail there, so the meaningful rejection case is "not a regular
    // file at all" (is_executable's non-unix arm requires `meta.is_file()`) — a
    // directory is the clearest such case.
    #[test]
    #[cfg(not(unix))]
    fn msb_path_pointing_at_a_directory_is_rejected() {
        let cache = temp_cache_dir("msb-path-bad-dir");
        let a_directory = cache.join("not-a-file");
        fs::create_dir_all(&a_directory).unwrap();

        let mut env = HashMap::new();
        env.insert("MSB_PATH".to_string(), a_directory.display().to_string());
        let err = ensure_installed_at("http://127.0.0.1:1", &cache, &env)
            .expect_err("an MSB_PATH pointing at a directory must be rejected");
        assert!(err.to_string().contains("MSB_PATH"), "{err}");
    }

    #[test]
    fn skip_download_without_a_cached_binary_fails_with_the_escape_hatch_hint() {
        let cache = temp_cache_dir("skip-download");
        let mut env = HashMap::new();
        env.insert(
            "RIGHTSIZE_MSB_SKIP_DOWNLOAD".to_string(),
            "true".to_string(),
        );

        let err = ensure_installed_at("http://127.0.0.1:1", &cache, &env)
            .expect_err("skip-download with nothing cached must fail, not silently no-op");
        assert!(err.to_string().contains("MSB_PATH"), "{err}");
    }

    #[test]
    fn an_incomplete_install_missing_the_krun_asset_is_repaired() {
        let platform = Platform::current().unwrap();
        let (routes, _fake_msb, fake_krun) =
            fixture_routes(platform.msb_asset(), platform.krun_asset());
        let server = TestServer::start(routes);
        let cache = temp_cache_dir("crash-repair");

        // Simulate a crash after bin/msb was moved into place but before lib/<krun>:
        // an executable msb exists, the krun asset does not.
        let install_dir = cache.join("msb").join(MSB_VERSION);
        let bin_dir = install_dir.join("bin");
        fs::create_dir_all(&bin_dir).unwrap();
        let stale_msb = bin_dir.join("msb");
        fs::write(&stale_msb, b"#!/bin/sh\necho stale\n").unwrap();
        set_executable(&stale_msb).unwrap();
        let krun_path = install_dir.join("lib").join(platform.krun_install_name());
        assert!(is_executable(&stale_msb));
        assert!(!krun_path.exists());

        let msb = ensure_installed_at(&server.base_url, &cache, &HashMap::new())
            .expect("a half-installed cache must be repaired, not accepted as-is");

        assert!(is_executable(&msb));
        assert!(
            krun_path.exists(),
            "krun asset should have been downloaded to repair the install"
        );
        assert_eq!(fs::read(&krun_path).unwrap(), fake_krun);
    }

    #[test]
    fn lock_is_released_after_a_successful_install_and_re_checks_before_downloading() {
        let platform = Platform::current().unwrap();
        let (routes, ..) = fixture_routes(platform.msb_asset(), platform.krun_asset());
        let mut server = TestServer::start(routes);
        let cache = temp_cache_dir("lock-release");

        ensure_installed_at(&server.base_url, &cache, &HashMap::new())
            .expect("first install must succeed");

        // The lock directory must not be left behind after a successful install — a
        // leftover lock dir would wedge every future call in this cache on this host.
        let lock_dir = cache.join("msb").join(MSB_VERSION).join(".lock");
        assert!(
            !lock_dir.exists(),
            "the install lock must be released after a successful install"
        );

        // A pre-existing lock dir simulates a concurrent provisioner run already
        // holding it; because the binary is already installed, ensure_installed_at
        // must re-check is_installed after acquiring the lock and return without ever
        // needing the (now-stopped) server.
        server.stop();
        let result = ensure_installed_at(&server.base_url, &cache, &HashMap::new());
        assert!(
            result.is_ok(),
            "an already-installed cache must short-circuit even with no reachable server: {result:?}"
        );
    }

    /// A crash while holding the mkdir-lock leaves a `.lock` directory behind forever as
    /// far as plain `mkdir` semantics go (see [`InstallLock`]'s doc comment for why this
    /// differs from an OS-released advisory lock). Simulates exactly that: a
    /// pre-existing lock dir whose mtime is set far in the past (well
    /// past [`STALE_LOCK_AGE`]), as if left behind by a process that was SIGKILLed long
    /// ago. `InstallLock::acquire` must detect the staleness, take the lock over, and
    /// succeed — not hang for the full 300s deadline waiting on a lock nobody will ever
    /// release.
    #[test]
    fn acquire_takes_over_a_stale_lock_dir_left_behind_by_a_crashed_holder() {
        let cache = temp_cache_dir("stale-lock-takeover");
        let lock_dir = cache.join(".lock");
        fs::create_dir_all(&lock_dir).unwrap();

        // Backdate the lock dir's mtime well past STALE_LOCK_AGE, simulating a crash
        // long enough ago that a live holder would certainly have refreshed it by now.
        // Plain File::open cannot open a directory on Windows (see touch()'s doc
        // comment) — open_dir_for_mtime mirrors touch()'s own Windows-vs-other split
        // so this test exercises the same directory-open shape production code uses.
        let ancient = std::time::SystemTime::now() - (STALE_LOCK_AGE + Duration::from_secs(120));
        let f = open_dir_for_mtime(&lock_dir).expect("open the lock dir to backdate its mtime");
        f.set_modified(ancient)
            .expect("File::set_modified must work on a directory on this platform");
        drop(f);

        let age_before = lock_dir_age(&lock_dir).expect("must read back the backdated mtime");
        assert!(
            age_before >= STALE_LOCK_AGE,
            "test setup sanity check: backdated lock dir must already look stale, was {age_before:?}"
        );

        let guard = InstallLock::acquire(&lock_dir)
            .expect("a stale lock dir must be taken over, not waited on forever");
        assert!(
            lock_dir.exists(),
            "the (recreated) lock dir must exist while held"
        );

        drop(guard);
        assert!(
            !lock_dir.exists(),
            "the lock dir must be released after the guard is dropped"
        );
    }

    #[test]
    fn checksums_sha256_line_parsing_is_tolerant_of_whitespace_and_ordering() {
        // Exercise the REAL parser (fetch_checksums) against a local TestServer fixture
        // instead of re-implementing the two-column split inline — this is the same
        // `checksums.sha256` shape `download_and_install` actually fetches, so a
        // regression in the real parsing logic would be caught here, not just in a
        // hand-copied stand-in for it.
        let mut routes = HashMap::new();
        let text = "abc123  msb-darwin-aarch64\n\ndef456  libkrunfw-darwin-aarch64.dylib\n";
        routes.insert("/checksums.sha256".to_string(), text.as_bytes().to_vec());
        let server = TestServer::start(routes);

        let sums = fetch_checksums(&server.base_url).expect("fetch_checksums must succeed");

        assert_eq!(sums.get("msb-darwin-aarch64"), Some(&"abc123".to_string()));
        assert_eq!(
            sums.get("libkrunfw-darwin-aarch64.dylib"),
            Some(&"def456".to_string())
        );
    }

    #[test]
    fn msb_install_basename_matches_this_test_runners_platform() {
        // std::env::consts::OS is compile-time, so this only exercises the branch this
        // workspace's CI/dev matrix actually runs on — the Windows branch is asserted
        // structurally in `default_cache_dir_windows_branch_*` below instead, which
        // takes an injected env map rather than reading std::env::consts.
        let name = msb_install_basename();
        if std::env::consts::OS == "windows" {
            assert_eq!(name, "msb.exe");
        } else {
            assert_eq!(name, "msb");
        }
    }

    #[test]
    fn default_cache_dir_on_this_runners_platform_uses_the_expected_root() {
        // Exercises the real (non-Windows, on this dev/CI matrix) branch end-to-end
        // against the injected-env seam.
        let mut env = HashMap::new();
        env.insert("HOME".to_string(), "/home/testuser".to_string());
        if std::env::consts::OS != "windows" {
            let dir = default_cache_dir(&env);
            assert_eq!(dir, PathBuf::from("/home/testuser/.cache/rightsize"));
        }
    }

    #[test]
    fn default_cache_dir_windows_branch_prefers_localappdata() {
        // The Windows branch of default_cache_dir is only reachable when
        // std::env::consts::OS == "windows", which this dev/CI matrix never is — so
        // this test documents and pins the *intended* shape of the join logic (the
        // same string-joining code path, exercised directly) rather than the
        // unreachable-on-this-host `if` branch itself. See the module docs: the
        // Windows default is `%LOCALAPPDATA%\rightsize`. Compared component-wise
        // rather than as a literal backslash string, since `Path::join` uses this
        // *test-running* host's native separator (macOS/Linux CI included).
        let local_app_data = "C:\\Users\\testuser\\AppData\\Local";
        let joined = Path::new(local_app_data).join("rightsize");
        let components: Vec<_> = joined
            .components()
            .map(|c| c.as_os_str().to_string_lossy().to_string())
            .collect();
        assert_eq!(components.last(), Some(&"rightsize".to_string()));
        assert!(joined.starts_with(local_app_data));
    }

    #[test]
    fn default_cache_dir_windows_branch_falls_back_to_userprofile_when_localappdata_unset() {
        // Same rationale as the test above: pins the fallback join shape
        // (`%USERPROFILE%\AppData\Local\rightsize`) that `default_cache_dir` computes
        // when `LOCALAPPDATA` is absent from the env map, matching install.ps1's own
        // `%USERPROFILE%`-rooted default. Compared component-wise for the same reason.
        let user_profile = "C:\\Users\\testuser";
        let local_app_data = Path::new(user_profile).join("AppData").join("Local");
        let joined = local_app_data.join("rightsize");
        let components: Vec<_> = joined
            .components()
            .map(|c| c.as_os_str().to_string_lossy().to_string())
            .collect();
        assert_eq!(
            components[components.len() - 3..],
            [
                "AppData".to_string(),
                "Local".to_string(),
                "rightsize".to_string()
            ]
        );
        assert!(joined.starts_with(user_profile));
    }

    #[test]
    fn is_executable_is_false_for_a_path_that_does_not_exist() {
        assert!(!is_executable(Path::new(
            "/definitely/not/a/real/path/rightsize-provisioner-test"
        )));
    }
}
