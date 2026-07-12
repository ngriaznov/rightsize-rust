//! The rightsize cache dir: one location every backend and the reaping ledger (see
//! `crate::reaper`) share, so a Kotlin/Node/Rust process on the same host all agree on
//! where run records and provisioned toolchains live. Historically this logic lived
//! only in `rightsize-msb`'s provisioner (the toolchain download cache); it moved here
//! because the reaping ledger needs it even in docker-only processes, which never link
//! `rightsize-msb` at all.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Resolves the cache dir using the real process environment: `RIGHTSIZE_CACHE_DIR`
/// if set, else the per-OS default (see [`default_dir`]).
pub fn dir() -> PathBuf {
    let env: HashMap<String, String> = std::env::vars().collect();
    resolve(&env)
}

/// The seam [`dir`] delegates to, parameterized over an environment map so callers
/// (and this module's own tests) can resolve a cache dir without touching real
/// process environment.
pub fn resolve(env: &HashMap<String, String>) -> PathBuf {
    if let Some(dir) = env.get("RIGHTSIZE_CACHE_DIR") {
        return PathBuf::from(dir);
    }
    default_dir(env)
}

/// The default cache root when `RIGHTSIZE_CACHE_DIR` is unset: `~/.cache/rightsize` on
/// macOS/Linux (via `HOME`), `%LOCALAPPDATA%\rightsize` on Windows (via
/// `LOCALAPPDATA`, falling back to `%USERPROFILE%\AppData\Local` if unset). The cache
/// holds machine-local, non-roaming data (a downloaded native toolchain, reaping run
/// records) — exactly what `%LOCALAPPDATA%` is for; the roaming profile
/// (`%USERPROFILE%\.cache`) would risk sync in roaming-profile setups and mixes a Unix
/// dotfile convention into a Windows home directory.
pub fn default_dir(env: &HashMap<String, String>) -> PathBuf {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_prefers_the_cache_dir_override() {
        let mut env = HashMap::new();
        env.insert("RIGHTSIZE_CACHE_DIR".to_string(), "/tmp/custom".to_string());
        env.insert("HOME".to_string(), "/home/testuser".to_string());
        assert_eq!(resolve(&env), PathBuf::from("/tmp/custom"));
    }

    #[test]
    fn resolve_falls_back_to_default_dir_when_unset() {
        let mut env = HashMap::new();
        env.insert("HOME".to_string(), "/home/testuser".to_string());
        if std::env::consts::OS != "windows" {
            assert_eq!(
                resolve(&env),
                PathBuf::from("/home/testuser/.cache/rightsize")
            );
        }
    }

    #[test]
    fn default_dir_on_this_runners_platform_uses_the_expected_root() {
        let mut env = HashMap::new();
        env.insert("HOME".to_string(), "/home/testuser".to_string());
        if std::env::consts::OS != "windows" {
            assert_eq!(
                default_dir(&env),
                PathBuf::from("/home/testuser/.cache/rightsize")
            );
        }
    }
}
