//! A single-node PostgreSQL container. Defaults to a `test`/`test`/`test`
//! user/password/database trio so [`PostgresGuard::connection_string`] is usable with
//! zero configuration; call [`PostgresContainer::with_username`]/
//! [`PostgresContainer::with_password`]/[`PostgresContainer::with_database`] before
//! `start()` to override any of them.
//!
//! ### Compatibility checking
//!
//! [`PostgresContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `postgres` (registry
//! host, tag, and digest stripped) before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("postgres")` to override for
//! a verified drop-in replacement. [`PostgresContainer::new`] goes through the same
//! check against its own floating reference, so it can never fail in practice.
//!
//! ### `new()` floats to `postgres:latest` — now the Debian-based image, not Alpine
//!
//! This module used to pin `postgres:18-alpine`; `new()` now floats to
//! `postgres:latest`, which Docker Hub publishes as a Debian-based image rather than
//! Alpine. Functionally equivalent for this module's own use, just a larger pull. The
//! `DOCKER_PG_LLVM_DEPS` fix below was verified against the `*-alpine` variant
//! specifically (the tab-bearing baked env value is an Alpine-manifest artifact); it
//! is kept unconditionally since it is a documented no-op everywhere else.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "postgres";

/// A single-node PostgreSQL container.
pub struct PostgresContainer {
    container: Container,
    image: ImageName,
    username: String,
    password: String,
    database: String,
}

impl PostgresContainer {
    const PORT: u16 = 5432;

    /// Builds a container from the floating default image (`postgres:latest`) — see
    /// the module doc for the Alpine-to-Debian shift.
    pub fn new() -> Self {
        Self::with_image("postgres:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`PostgresContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        let username = "test".to_string();
        let password = "test".to_string();
        let database = "test".to_string();
        let container = Container::new(image.as_str())
            .with_exposed_ports(&[Self::PORT])
            .with_env("POSTGRES_USER", &username)
            .with_env("POSTGRES_PASSWORD", &password)
            .with_env("POSTGRES_DB", &database)
            // The official postgres:*-alpine image bakes DOCKER_PG_LLVM_DEPS into its
            // manifest with a literal tab character in the value (a package list built
            // with `\t\t` continuation). msb 0.6.2's krun VMM builder panics with
            // InvalidAscii on that boot-env value before the guest ever starts
            // (reproduced with zero rightsize-set env vars — it's the image, not us).
            // Docker is unaffected. Overriding the var here wins over the
            // image default in both backends' env-merge order and is a no-op for the
            // build the image already baked, so it's a safe, backend-portable fix
            // rather than an msb-only special case.
            .with_env("DOCKER_PG_LLVM_DEPS", "")
            // The postgres entrypoint starts the server once to run initdb scripts
            // against it, shuts it down, then starts it again for real — printing
            // "database system is ready to accept connections" BOTH times. Waiting
            // for the first occurrence races that restart: a client can connect to
            // the init-time server just before it's torn down. times=2 waits for the
            // second, durable listen.
            .waiting_for(Wait::for_log_message(
                ".*database system is ready to accept connections.*",
                2,
            ));
        Self {
            container,
            image,
            username,
            password,
            database,
        }
    }

    /// Overrides `POSTGRES_USER` (default `test`).
    pub fn with_username(mut self, username: &str) -> Self {
        self.username = username.to_string();
        self.container = self.container.with_env("POSTGRES_USER", username);
        self
    }

    /// Overrides `POSTGRES_PASSWORD` (default `test`).
    pub fn with_password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self.container = self.container.with_env("POSTGRES_PASSWORD", password);
        self
    }

    /// Overrides `POSTGRES_DB` (default `test`).
    pub fn with_database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self.container = self.container.with_env("POSTGRES_DB", database);
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
    pub async fn start(self) -> Result<PostgresGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        let guard = self.container.start().await?;
        Ok(PostgresGuard {
            guard,
            username: self.username,
            password: self.password,
            database: self.database,
        })
    }
}

impl Default for PostgresContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`PostgresContainer`].
pub struct PostgresGuard {
    guard: ContainerGuard,
    username: String,
    password: String,
    database: String,
}

impl PostgresGuard {
    /// The configured database user (default `test`).
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The configured database password (default `test`).
    pub fn password(&self) -> &str {
        &self.password
    }

    /// The configured database name (default `test`).
    pub fn database_name(&self) -> &str {
        &self.database
    }

    /// A `postgres://` connection string for the running container's
    /// [`Self::database_name`].
    pub fn connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.username,
            self.password,
            self.guard.host(),
            self.guard.get_mapped_port(PostgresContainer::PORT).unwrap(),
            self.database,
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.guard.stop().await
    }
}

impl std::ops::Deref for PostgresGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_test_trio() {
        let c = PostgresContainer::new();
        assert_eq!(c.username, "test");
        assert_eq!(c.password, "test");
        assert_eq!(c.database, "test");
    }

    #[test]
    fn builders_override_the_defaults() {
        let c = PostgresContainer::new()
            .with_username("alice")
            .with_password("s3cret")
            .with_database("app");
        assert_eq!(c.username, "alice");
        assert_eq!(c.password, "s3cret");
        assert_eq!(c.database, "app");
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        PostgresContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = PostgresContainer::with_image("mysql:8")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("mysql is not postgres");
        let msg = err.to_string();
        assert!(msg.contains("mysql"), "{msg}");
        assert!(msg.contains("postgres"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image =
            ImageName::parse("mycorp/pg-hardened:16").as_compatible_substitute_for("postgres");
        PostgresContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
