//! A single-node Keycloak container in `start-dev` mode (in-memory H2, no external
//! database — fine for tests, never for production).
//!
//! ### Admin bootstrap env — 26.x renamed these, verified against the pinned tag
//!
//! Keycloak 26.x replaced the old `KEYCLOAK_ADMIN`/`KEYCLOAK_ADMIN_PASSWORD` pair with
//! `KC_BOOTSTRAP_ADMIN_USERNAME`/`KC_BOOTSTRAP_ADMIN_PASSWORD`. Verified directly
//! against `quay.io/keycloak/keycloak:26.4` — booting with only the new names produces
//! `KC-SERVICES0077: Created temporary admin user with username admin` in the log; the
//! legacy names are not read by this image. This module sets only the new names.
//!
//! ### Health lives on the MANAGEMENT port (9000), not 8080 — verified, and the path is `/health`
//!
//! Captured verbatim from a real boot: `Listening on: http://0.0.0.0:8080. Management
//! interface listening on http://0.0.0.0:9000.` With `KC_HEALTH_ENABLED=true` set, `GET
//! /health` on port **9000** returns `200 OK` (body: literal `OK`); the same path on
//! 8080 404s, and the commonly assumed `/health/ready` sub-path 404s on this tag too
//! (this image's SmallRye Health root aggregate is served bare at `/health`, not
//! `/health/ready`) — pinned to `/health` on 9000 accordingly, not `/health/ready` on
//! 8080.
//!
//! ### Memory — Quarkus JVM, needed the ladder
//!
//! Booted under msb's default ~450M microVM RAM, the JVM is `Killed` (OOM) partway
//! through startup (captured: `'java' ... -XX:MaxRAMPercentage=70 ... Killed`) — same
//! Paketo/Quarkus-on-microVM story as [`crate::spring_cloud_config::SpringCloudConfigContainer`].
//! Retried with `-m 1024M`: boots clean, `/health` reports `200` well within the
//! startup timeout. `with_memory_limit(1024)` is this module's default.
//!
//! No control characters were found in the image's baked env (checked via
//! `docker image inspect`), so no env override is needed here.
//!
//! ### Compatibility checking
//!
//! [`KeycloakContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `keycloak/keycloak` —
//! the `quay.io` registry host is stripped per the Docker convention (registry host,
//! tag, and digest stripped) — before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("keycloak/keycloak")` to
//! override for a verified drop-in replacement. [`KeycloakContainer::new`] goes
//! through the same check against its own floating reference, so it can never fail
//! in practice.
//!
//! ### `new()` floats to `quay.io/keycloak/keycloak:latest`
//!
//! This module used to pin `quay.io/keycloak/keycloak:26.4`; `new()` now floats to
//! `quay.io/keycloak/keycloak:latest` so the version tracks upstream rather than this
//! crate's own release cycle. The admin-bootstrap env rename, the management-port
//! health path, and the memory-ladder facts above were all verified against that
//! `26.4` boot specifically.

use std::time::Duration;

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

const HTTP_PORT: u16 = 8080;
const MANAGEMENT_PORT: u16 = 9000;

/// The repository this module understands (the `quay.io` registry host stripped) —
/// see the module doc's compatibility section.
const EXPECTED_REPOSITORY: &str = "keycloak/keycloak";

/// A single-node Keycloak container in `start-dev` mode.
pub struct KeycloakContainer {
    container: Container,
    image: ImageName,
    admin_username: String,
    admin_password: String,
}

impl KeycloakContainer {
    /// Builds a container from the floating default image
    /// (`quay.io/keycloak/keycloak:latest`).
    pub fn new() -> Self {
        Self::with_image("quay.io/keycloak/keycloak:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`KeycloakContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        let admin_username = "admin".to_string();
        let admin_password = "admin".to_string();
        let container = Container::new(image.as_str())
            .with_exposed_ports(&[HTTP_PORT, MANAGEMENT_PORT])
            .with_command(&["start-dev"])
            .with_env("KC_BOOTSTRAP_ADMIN_USERNAME", &admin_username)
            .with_env("KC_BOOTSTRAP_ADMIN_PASSWORD", &admin_password)
            .with_env("KC_HEALTH_ENABLED", "true")
            // Quarkus JVM — see the module doc for the measured OOM-at-default /
            // clean-at-1024 story.
            .with_memory_limit(1024)
            // Health is on the MANAGEMENT port, not HTTP — see the module doc for
            // the captured boot line.
            .waiting_for(
                Wait::for_http("/health")
                    .for_port(MANAGEMENT_PORT)
                    .with_startup_timeout(Duration::from_secs(180)),
            );
        Self {
            container,
            image,
            admin_username,
            admin_password,
        }
    }

    /// Overrides `KC_BOOTSTRAP_ADMIN_USERNAME` (default `admin`).
    pub fn with_admin_username(mut self, username: &str) -> Self {
        self.admin_username = username.to_string();
        self.container = self
            .container
            .with_env("KC_BOOTSTRAP_ADMIN_USERNAME", username);
        self
    }

    /// Overrides `KC_BOOTSTRAP_ADMIN_PASSWORD` (default `admin`).
    pub fn with_admin_password(mut self, password: &str) -> Self {
        self.admin_password = password.to_string();
        self.container = self
            .container
            .with_env("KC_BOOTSTRAP_ADMIN_PASSWORD", password);
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
    pub async fn start(self) -> Result<KeycloakGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        let guard = self.container.start().await?;
        Ok(KeycloakGuard {
            guard,
            admin_username: self.admin_username,
            admin_password: self.admin_password,
        })
    }
}

impl Default for KeycloakContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`KeycloakContainer`].
pub struct KeycloakGuard {
    guard: ContainerGuard,
    admin_username: String,
    admin_password: String,
}

impl KeycloakGuard {
    /// The configured bootstrap admin username (default `admin`).
    pub fn admin_username(&self) -> &str {
        &self.admin_username
    }

    /// The configured bootstrap admin password (default `admin`).
    pub fn admin_password(&self) -> &str {
        &self.admin_password
    }

    /// The auth server's base URI (the HTTP port — realm/OIDC endpoints live under
    /// this).
    pub fn auth_server_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.guard.host(),
            self.guard.get_mapped_port(HTTP_PORT).unwrap()
        )
    }

    /// The management interface's base URI (health/metrics — port 9000, not
    /// [`Self::auth_server_url`]'s port).
    pub fn management_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.guard.host(),
            self.guard.get_mapped_port(MANAGEMENT_PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host ports.
    pub async fn stop(self) -> Result<()> {
        self.guard.stop().await
    }
}

impl std::ops::Deref for KeycloakGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_admin_pair() {
        let c = KeycloakContainer::new();
        assert_eq!(c.admin_username, "admin");
        assert_eq!(c.admin_password, "admin");
    }

    #[test]
    fn builders_override_the_defaults() {
        let c = KeycloakContainer::new()
            .with_admin_username("root")
            .with_admin_password("s3cret");
        assert_eq!(c.admin_username, "root");
        assert_eq!(c.admin_password, "s3cret");
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        KeycloakContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn a_registry_qualified_image_strips_the_registry_host() {
        // quay.io is the image's own registry — the compatibility check must strip it
        // to compare against the bare `keycloak/keycloak` repository.
        KeycloakContainer::with_image("quay.io/keycloak/keycloak:26.0")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("quay.io/keycloak/keycloak:26.0 is compatible with 'keycloak/keycloak'");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = KeycloakContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not keycloak/keycloak");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("keycloak/keycloak"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/keycloak-hardened:26.4")
            .as_compatible_substitute_for("keycloak/keycloak");
        KeycloakContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
