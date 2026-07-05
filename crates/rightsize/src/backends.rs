//! Backend resolution: picking one [`crate::backend::SandboxBackend`] out of however many
//! [`crate::backend::BackendProvider`]s the caller compiled in, honoring `RIGHTSIZE_BACKEND`
//! when it's set. [`resolve`] is a pure function precisely so it can be unit-tested
//! without a real msb/Docker runtime — see the tests below.
//!
//! `active` is the cached, process-wide entry point real (non-test) code calls: it
//! reads `RIGHTSIZE_BACKEND` from the real environment, resolves once, and caches the
//! result for the rest of the process. No provider is registered directly in this
//! crate — `rightsize-msb` and `rightsize-docker` each ship a
//! [`crate::backend::BackendProvider`] and are wired in from `rightsize-modules` or a
//! caller's own `Cargo.toml`; until at least one of those crates is linked in, `active()`
//! errors, which is expected in this crate's own tests (they use `Container::with_backend`
//! to bypass it entirely).

use std::sync::{Arc, OnceLock};

use crate::backend::{BackendProvider, SandboxBackend};
use crate::error::{Result, RightsizeError};

/// Picks the [`SandboxBackend`] to use out of `providers`.
///
/// - If `requested` is `Some`, the provider whose name matches case-insensitively is
///   used — even if a higher-priority provider is also present — and it's an error if
///   that provider isn't supported on this host.
/// - If `requested` is `None`, the highest-`priority` supported provider wins.
/// - An empty `providers` list, or a list with no supported provider, is an error
///   listing every provider's `unsupported_reason`.
/// - An unknown `requested` name is an error listing every known provider name.
pub fn resolve(
    providers: &[Box<dyn BackendProvider>],
    requested: Option<&str>,
) -> Result<Box<dyn SandboxBackend>> {
    if providers.is_empty() {
        return Err(RightsizeError::Backend(
            "No rightsize backends compiled in — add rightsize-msb and/or rightsize-docker \
             (rightsize-backend-microsandbox / rightsize-backend-docker)"
                .to_string(),
        ));
    }

    if let Some(requested) = requested {
        let provider = providers
            .iter()
            .find(|p| p.name().eq_ignore_ascii_case(requested))
            .ok_or_else(|| {
                RightsizeError::Backend(format!(
                    "RIGHTSIZE_BACKEND='{requested}' — known backends are: {}",
                    provider_names(providers)
                ))
            })?;
        if !provider.is_supported() {
            return Err(RightsizeError::Backend(format!(
                "Requested backend '{}' unavailable: {}",
                provider.name(),
                provider.unsupported_reason()
            )));
        }
        return provider.create();
    }

    let mut sorted: Vec<&Box<dyn BackendProvider>> = providers.iter().collect();
    sorted.sort_by_key(|p| std::cmp::Reverse(p.priority()));
    let supported = sorted
        .into_iter()
        .find(|p| p.is_supported())
        .ok_or_else(|| {
            RightsizeError::Backend(format!(
                "No sandbox backend can run on this machine:\n{}",
                unsupported_reasons(providers)
            ))
        })?;
    supported.create()
}

/// The process-wide registry of compiled-in providers. `rightsize` itself registers
/// none — `rightsize-msb` and `rightsize-docker` each call [`register_provider`] to
/// announce themselves (typically from the binary/test crate that links them in, since
/// Rust has no automatic plugin discovery equivalent to the JVM's `ServiceLoader`
/// without an extra dependency this workspace doesn't budget for).
static PROVIDERS: std::sync::Mutex<Vec<Box<dyn BackendProvider>>> =
    std::sync::Mutex::new(Vec::new());

/// Registers `provider` so a later `active` call can consider it. Must be called
/// before the first `active` call to have any effect — `active` caches its result
/// after the first successful resolution.
pub fn register_provider(provider: Box<dyn BackendProvider>) {
    PROVIDERS
        .lock()
        .expect("backend provider registry mutex poisoned")
        .push(provider);
}

static ACTIVE: OnceLock<Arc<dyn SandboxBackend>> = OnceLock::new();

/// The process-wide active backend: resolves once (honoring `RIGHTSIZE_BACKEND` from
/// the real environment against every provider registered so far via
/// [`register_provider`]) and caches the result for the rest of the process.
///
/// # Panics
///
/// Panics if resolution fails (no supported backend, or an explicitly requested one is
/// unsupported/unknown) — there is no reasonable fallback for a container test suite
/// that has no working backend, and every call site here is itself inside another
/// fallible operation (`Container::start`), which is where the actual user-facing error
/// reporting happens today; a future task may convert this to a fallible cache instead
/// of a `OnceLock` if that surfaces a friendlier error path.
pub(crate) fn active() -> Arc<dyn SandboxBackend> {
    ACTIVE
        .get_or_init(|| {
            let providers = PROVIDERS
                .lock()
                .expect("backend provider registry mutex poisoned");
            let requested = std::env::var("RIGHTSIZE_BACKEND").ok();
            let backend =
                resolve(&providers, requested.as_deref()).unwrap_or_else(|e| panic!("{e}"));
            Arc::from(backend)
        })
        .clone()
}

/// The process-wide active backend's registered [`SandboxBackend::name`] (e.g.
/// `"microsandbox"`, `"docker"`) — the one sliver of `active`'s result a caller
/// outside this crate can legitimately need *before* booting anything, e.g. a module
/// that only supports part of its surface on one backend and wants to raise a typed
/// [`crate::error::RightsizeError::UnsupportedByBackend`] up front rather than fail
/// partway through `start()`. Same panics-on-unresolvable-backend behavior as the
/// private `active` (this crate's process-wide cached resolver) — see its docs.
pub fn active_name() -> String {
    active().name().to_string()
}

fn provider_names(providers: &[Box<dyn BackendProvider>]) -> String {
    providers
        .iter()
        .map(|p| p.name())
        .collect::<Vec<_>>()
        .join(", ")
}

fn unsupported_reasons(providers: &[Box<dyn BackendProvider>]) -> String {
    providers
        .iter()
        .map(|p| format!("  - {}: {}", p.name(), p.unsupported_reason()))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake provider whose `create()` panics with a sentinel string naming itself,
    /// letting a test assert resolution reached the *right* provider without building
    /// a real backend.
    struct FakeProvider {
        name: &'static str,
        priority: u32,
        supported: bool,
    }

    impl BackendProvider for FakeProvider {
        fn name(&self) -> &str {
            self.name
        }
        fn priority(&self) -> u32 {
            self.priority
        }
        fn is_supported(&self) -> bool {
            self.supported
        }
        fn unsupported_reason(&self) -> String {
            format!("{} not supported on this host", self.name)
        }
        fn create(&self) -> Result<Box<dyn SandboxBackend>> {
            panic!("not needed: {}", self.name)
        }
    }

    fn provider(name: &'static str, priority: u32, supported: bool) -> Box<dyn BackendProvider> {
        Box::new(FakeProvider {
            name,
            priority,
            supported,
        })
    }

    /// `Box<dyn SandboxBackend>` isn't `Debug` (trait objects for an async trait aren't,
    /// and shouldn't be forced to be just for tests), so `Result::unwrap_err` doesn't
    /// work here — this pulls the error out by hand.
    fn expect_err(result: Result<Box<dyn SandboxBackend>>) -> RightsizeError {
        match result {
            Ok(_) => panic!("expected an error, got Ok"),
            Err(e) => e,
        }
    }

    #[test]
    #[should_panic(expected = "not needed: microsandbox")]
    fn picks_highest_priority_supported_provider() {
        let providers = vec![
            provider("docker", 10, true),
            provider("microsandbox", 20, true),
        ];
        let _ = resolve(&providers, None);
    }

    #[test]
    #[should_panic(expected = "not needed: docker")]
    fn env_override_wins_even_at_lower_priority() {
        let providers = vec![
            provider("docker", 10, true),
            provider("microsandbox", 20, true),
        ];
        let _ = resolve(&providers, Some("docker"));
    }

    #[test]
    fn no_supported_provider_gives_every_reason() {
        let providers = vec![
            provider("microsandbox", 20, false),
            provider("docker", 10, false),
        ];
        let err = expect_err(resolve(&providers, None));
        let msg = err.to_string();
        assert!(msg.contains("microsandbox not supported"), "{msg}");
        assert!(msg.contains("docker not supported"), "{msg}");
    }

    #[test]
    fn unknown_requested_backend_lists_known_names() {
        let providers = vec![provider("docker", 10, true)];
        let err = expect_err(resolve(&providers, Some("podman")));
        let msg = err.to_string();
        assert!(msg.contains("podman"), "{msg}");
        assert!(msg.contains("docker"), "{msg}");
    }

    #[test]
    fn empty_provider_list_names_both_known_artifacts() {
        let none_requested = expect_err(resolve(&[], None)).to_string();
        assert!(none_requested.contains("rightsize-msb"), "{none_requested}");
        assert!(
            none_requested.contains("rightsize-docker"),
            "{none_requested}"
        );

        let requested = expect_err(resolve(&[], Some("docker"))).to_string();
        assert!(requested.contains("rightsize-msb"), "{requested}");
        assert!(requested.contains("rightsize-docker"), "{requested}");
    }

    #[test]
    fn requested_backend_that_is_unsupported_names_its_reason() {
        let providers = vec![provider("docker", 10, false)];
        let err = expect_err(resolve(&providers, Some("docker"))).to_string();
        assert!(err.contains("unavailable:"), "{err}");
        assert!(err.contains("docker not supported on this host"), "{err}");
    }

    #[test]
    #[should_panic(expected = "not needed: docker")]
    fn requested_backend_name_match_is_case_insensitive_upper() {
        let providers = vec![provider("docker", 10, true)];
        let _ = resolve(&providers, Some("DOCKER"));
    }

    #[test]
    #[should_panic(expected = "not needed: docker")]
    fn requested_backend_name_match_is_case_insensitive_mixed() {
        let providers = vec![provider("docker", 10, true)];
        let _ = resolve(&providers, Some("Docker"));
    }
}
