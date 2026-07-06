//! A single-node PostgreSQL container. Defaults to a `test`/`test`/`test`
//! user/password/database trio so [`PostgresGuard::connection_string`] is usable with
//! zero configuration; call [`PostgresContainer::with_username`]/
//! [`PostgresContainer::with_password`]/[`PostgresContainer::with_database`] before
//! `start()` to override any of them.

use rightsize::{Container, ContainerGuard, Result, Wait};

/// A single-node PostgreSQL container.
pub struct PostgresContainer {
    container: Container,
    username: String,
    password: String,
    database: String,
}

impl PostgresContainer {
    const PORT: u16 = 5432;

    /// Builds a container from the pinned default image (`postgres:18-alpine`) —
    /// 18.4 was current stable at the time of that pin.
    pub fn new() -> Self {
        Self::with_image("postgres:18-alpine")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let username = "test".to_string();
        let password = "test".to_string();
        let database = "test".to_string();
        let container = Container::new(image)
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

    /// Boots the container.
    pub async fn start(self) -> Result<PostgresGuard> {
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
}
