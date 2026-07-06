//! A single-node ClickHouse container, queried over its HTTP interface (port 8123).
//! The native protocol port (9000) is exposed too, but this module's helpers are
//! HTTP-first: the HTTP interface needs no client dependency (`ureq` as a dev-dep, no
//! runtime crate), matching the house convention for HTTP-first modules.
//!
//! Defaults to a `test`/`test` user/password pair and a `test` database so
//! [`ClickHouseGuard::http_url`] plus basic auth is usable with zero configuration;
//! call [`ClickHouseContainer::with_username`]/[`ClickHouseContainer::with_password`]/
//! [`ClickHouseContainer::with_database`] before `start()` to override any of them.
//!
//! ### Env var names — verified against a real boot
//!
//! `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DB` are the image's
//! documented names and were confirmed directly: booting with those three set
//! produces `/entrypoint.sh: create new user 'test' instead 'default'` and
//! `/entrypoint.sh: create database 'test'` in the logs, and `curl -u test:test`
//! against the HTTP interface authenticates and runs queries successfully.
//!
//! ### Readiness — verified against a real 25.8 (LTS) boot
//!
//! `GET /ping` on the HTTP port returns the literal body `Ok.\n` as soon as the HTTP
//! server is accepting connections (protocol-aware — no restart/double-boot race like
//! the Postgres/MySQL/MariaDB entrypoints have), so `for_http("/ping")` at the default
//! 200 status code is the correct and simplest readiness signal. Verified directly:
//! `curl http://127.0.0.1:<port>/ping` returned `Ok.` immediately once the container's
//! logs showed the config merge complete.
//!
//! No control characters were found in the image's baked env (checked via
//! `docker image inspect`). No `with_memory_limit` override was needed at
//! default settings (observed ~12s IT round-trip on msb, no memory-ladder escalation
//! needed) — a single-node ClickHouse server, unlike Pinot's four-JVM QuickStart
//! cluster, is not a JVM process at all.

use std::time::Duration;

use rightsize::{Container, ContainerGuard, Result, Wait};

const HTTP_PORT: u16 = 8123;
const NATIVE_PORT: u16 = 9000;

/// A single-node ClickHouse container.
pub struct ClickHouseContainer {
    container: Container,
    username: String,
    password: String,
    database: String,
}

impl ClickHouseContainer {
    /// Builds a container from the pinned default image
    /// (`clickhouse/clickhouse-server:25.8`).
    pub fn new() -> Self {
        Self::with_image("clickhouse/clickhouse-server:25.8")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let username = "test".to_string();
        let password = "test".to_string();
        let database = "test".to_string();
        let container = Container::new(image)
            .with_exposed_ports(&[HTTP_PORT, NATIVE_PORT])
            .with_env("CLICKHOUSE_USER", &username)
            .with_env("CLICKHOUSE_PASSWORD", &password)
            .with_env("CLICKHOUSE_DB", &database)
            // Protocol-aware HTTP probe: /ping answers "Ok.\n" once the HTTP
            // interface is really up — no double-boot restart race the way the
            // Postgres/MySQL/MariaDB entrypoints have. 180s: the entrypoint's
            // user/database provisioning runs a second server pass before the
            // HTTP interface opens; a loaded Windows CI runner was observed
            // still in early config processing at 120s.
            .waiting_for(
                Wait::for_http("/ping")
                    .for_port(HTTP_PORT)
                    .with_startup_timeout(Duration::from_secs(180)),
            );
        Self {
            container,
            username,
            password,
            database,
        }
    }

    /// Overrides `CLICKHOUSE_USER` (default `test`).
    pub fn with_username(mut self, username: &str) -> Self {
        self.username = username.to_string();
        self.container = self.container.with_env("CLICKHOUSE_USER", username);
        self
    }

    /// Overrides `CLICKHOUSE_PASSWORD` (default `test`).
    pub fn with_password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self.container = self.container.with_env("CLICKHOUSE_PASSWORD", password);
        self
    }

    /// Overrides `CLICKHOUSE_DB` (default `test`).
    pub fn with_database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self.container = self.container.with_env("CLICKHOUSE_DB", database);
        self
    }

    /// Boots the container.
    pub async fn start(self) -> Result<ClickHouseGuard> {
        crate::register_default_backends();
        let guard = self.container.start().await?;
        Ok(ClickHouseGuard {
            guard,
            username: self.username,
            password: self.password,
            database: self.database,
        })
    }
}

impl Default for ClickHouseContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`ClickHouseContainer`].
pub struct ClickHouseGuard {
    guard: ContainerGuard,
    username: String,
    password: String,
    database: String,
}

impl ClickHouseGuard {
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

    /// The HTTP interface's base URI (query via `POST` with a SQL body, basic-auth'd).
    pub fn http_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.guard.host(),
            self.guard.get_mapped_port(HTTP_PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host ports.
    pub async fn stop(self) -> Result<()> {
        self.guard.stop().await
    }
}

impl std::ops::Deref for ClickHouseGuard {
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
        let c = ClickHouseContainer::new();
        assert_eq!(c.username, "test");
        assert_eq!(c.password, "test");
        assert_eq!(c.database, "test");
    }

    #[test]
    fn builders_override_the_defaults() {
        let c = ClickHouseContainer::new()
            .with_username("alice")
            .with_password("s3cret")
            .with_database("app");
        assert_eq!(c.username, "alice");
        assert_eq!(c.password, "s3cret");
        assert_eq!(c.database, "app");
    }
}
