//! A single-node MinIO container, an S3-compatible object store. Defaults to a
//! `testuser`/`testpassword` root credential pair so [`MinioGuard::s3_url`] plus that
//! pair is usable with zero configuration; call
//! [`MinioContainer::with_root_user`]/[`MinioContainer::with_root_password`] before
//! `start()` to override either.
//!
//! ### The default entrypoint does not serve — a command is required
//!
//! Unlike every other module in this crate, MinIO's image needs an explicit command:
//! `server /data --console-address :9001`. The bare image entrypoint alone starts the
//! `minio` binary with no subcommand and does not bring up the S3 API at all — this
//! module always sets that command, not just as a documented recommendation.
//!
//! ### Why `testuser`/`testpassword`, not the house `test`/`test` pair
//!
//! MinIO rejects a root password shorter than 8 characters at boot, so the
//! `test`/`test` pair every other credentialed module in this crate defaults to
//! (see [`crate::clickhouse::ClickHouseContainer`], [`crate::mysql::MySqlContainer`])
//! cannot be reused here — `test` is 4 characters. `testuser`/`testpassword` is this
//! module's own default pair instead.
//!
//! ### Readiness — protocol-aware, answered on the first poll
//!
//! `Wait::for_http("/minio/health/live").for_port(9000)` is MinIO's own documented
//! liveness probe. Verified against a real
//! `minio/minio:RELEASE.2025-09-07T16-13-09Z` boot (log confirms `linux/arm64`): the
//! endpoint returned `200` on the very first poll after boot — no restart/double-boot
//! race to account for here.
//!
//! ### Auth is enforced, not just configured
//!
//! Verified directly: with the configured root credentials, `mc mb` (make bucket),
//! `mc cp` (upload a file), then `mc cat` (read it back) round-tripped the written
//! bytes exactly (`mc pipe` was tried first but an exec'd `mc pipe` under the
//! microsandbox backend consumes stdin and either dumps its goroutines and exits
//! non-zero or hangs outright — `mc cp` needs no stdin and round-trips reliably on
//! both backends). A subsequent anonymous `GET /` against the S3 API — no credentials
//! at all — came back `AccessDenied`, proving the credential pair is actually gating
//! access rather than merely being accepted and ignored.
//!
//! ### Memory — no limit needed, verified directly
//!
//! This module sets no memory limit. Verified against a real boot with the limit
//! removed entirely: MinIO came up in a guest reporting ~480 MB total, answered
//! `/minio/health/live` on the first poll, and completed a full bucket-create,
//! upload, and read-back round-trip. Unlike the JVM modules here
//! ([`crate::keycloak::KeycloakContainer`], [`crate::neo4j::Neo4jContainer`]), a
//! single-node MinIO server has no fixed heap region to reserve up front.
//!
//! No control characters were found in the image's baked env (checked via
//! `docker image inspect`).
//!
//! ### Compatibility checking
//!
//! [`MinioContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `minio/minio` (registry
//! host, tag, and digest stripped) before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("minio/minio")` to override
//! for a verified drop-in replacement. [`MinioContainer::new`] goes through the same
//! check against its own floating reference, so it can never fail in practice.
//!
//! ### `new()` floats to `minio/minio:latest`
//!
//! This module used to pin `minio/minio:RELEASE.2025-09-07T16-13-09Z`; `new()` now
//! floats to `minio/minio:latest` so the version tracks upstream rather than this
//! crate's own release cycle. The readiness and auth-enforcement facts above were
//! verified against that `RELEASE.2025-09-07T16-13-09Z` boot specifically.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

const API_PORT: u16 = 9000;
const CONSOLE_PORT: u16 = 9001;

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "minio/minio";

/// A single-node MinIO container, its S3 API on port 9000 (what
/// [`MinioGuard::s3_url`] wraps) and its web console on 9001 (exposed, not wrapped —
/// see [`MinioGuard::console_url`]).
pub struct MinioContainer {
    container: Container,
    image: ImageName,
    root_user: String,
    root_password: String,
}

impl MinioContainer {
    /// Builds a container from the floating default image (`minio/minio:latest`).
    pub fn new() -> Self {
        Self::with_image("minio/minio:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`MinioContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        let root_user = "testuser".to_string();
        let root_password = "testpassword".to_string();
        let container = Container::new(image.as_str())
            .with_exposed_ports(&[API_PORT, CONSOLE_PORT])
            .with_env("MINIO_ROOT_USER", &root_user)
            .with_env("MINIO_ROOT_PASSWORD", &root_password)
            // The bare entrypoint does not serve — see the module doc.
            .with_command(&["server", "/data", "--console-address", ":9001"])
            // No memory limit: verified serving a full round-trip in a guest
            // reporting ~480 MB total — see the module doc.
            .waiting_for(Wait::for_http("/minio/health/live").for_port(API_PORT));
        Self {
            container,
            image,
            root_user,
            root_password,
        }
    }

    /// Overrides `MINIO_ROOT_USER` (default `testuser`).
    pub fn with_root_user(mut self, root_user: &str) -> Self {
        self.root_user = root_user.to_string();
        self.container = self.container.with_env("MINIO_ROOT_USER", root_user);
        self
    }

    /// Overrides `MINIO_ROOT_PASSWORD` (default `testpassword`). MinIO rejects
    /// anything shorter than 8 characters at boot — see the module doc.
    pub fn with_root_password(mut self, root_password: &str) -> Self {
        self.root_password = root_password.to_string();
        self.container = self
            .container
            .with_env("MINIO_ROOT_PASSWORD", root_password);
        self
    }

    /// Boots the container, after checking the image is one this module understands.
    ///
    /// The compatibility check runs here rather than in the constructors so those stay
    /// infallible and match every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server.
    pub async fn start(self) -> Result<MinioGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        let guard = self.container.start().await?;
        Ok(MinioGuard {
            guard,
            root_user: self.root_user,
            root_password: self.root_password,
        })
    }
}

impl Default for MinioContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`MinioContainer`].
pub struct MinioGuard {
    guard: ContainerGuard,
    root_user: String,
    root_password: String,
}

impl MinioGuard {
    /// The configured root user (default `testuser`).
    pub fn root_user(&self) -> &str {
        &self.root_user
    }

    /// The configured root password (default `testpassword`).
    pub fn root_password(&self) -> &str {
        &self.root_password
    }

    /// The S3 API's base URI (port 9000) — sign requests against this with the
    /// configured root credentials, e.g. via `mc alias set` or an SDK's static
    /// credential provider.
    pub fn s3_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.guard.host(),
            self.guard.get_mapped_port(API_PORT).unwrap()
        )
    }

    /// The web console's base URI (port 9001) — human use only, no helper wraps it.
    pub fn console_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.guard.host(),
            self.guard.get_mapped_port(CONSOLE_PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host ports.
    pub async fn stop(self) -> Result<()> {
        self.guard.stop().await
    }
}

impl std::ops::Deref for MinioGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_testuser_pair() {
        let c = MinioContainer::new();
        assert_eq!(c.root_user, "testuser");
        assert_eq!(c.root_password, "testpassword");
    }

    #[test]
    fn builders_override_the_defaults() {
        let c = MinioContainer::new()
            .with_root_user("alice")
            .with_root_password("s3cretpw");
        assert_eq!(c.root_user, "alice");
        assert_eq!(c.root_password, "s3cretpw");
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        MinioContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = MinioContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not minio/minio");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("minio/minio"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/minio-hardened:RELEASE.2025-09-07")
            .as_compatible_substitute_for("minio/minio");
        MinioContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
