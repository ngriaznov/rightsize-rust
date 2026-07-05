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

use std::time::Duration;

use rightsize::{Container, ContainerGuard, Result, Wait};

const HTTP_PORT: u16 = 8080;
const MANAGEMENT_PORT: u16 = 9000;

/// A single-node Keycloak container in `start-dev` mode.
pub struct KeycloakContainer {
    container: Container,
    admin_username: String,
    admin_password: String,
}

impl KeycloakContainer {
    /// Builds a container from the pinned default image
    /// (`quay.io/keycloak/keycloak:26.4`).
    pub fn new() -> Self {
        Self::with_image("quay.io/keycloak/keycloak:26.4")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let admin_username = "admin".to_string();
        let admin_password = "admin".to_string();
        let container = Container::new(image)
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
                    .with_startup_timeout(Duration::from_secs(120)),
            );
        Self {
            container,
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

    /// Boots the container.
    pub async fn start(self) -> Result<KeycloakGuard> {
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
}
