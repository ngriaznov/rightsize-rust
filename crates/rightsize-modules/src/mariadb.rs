//! A single-node MariaDB container. Defaults to a `test`/`test`/`test`
//! user/password/database trio (plus `MARIADB_ROOT_PASSWORD=test`) so
//! [`MariaDbGuard::connection_string`] is usable with zero configuration; call
//! [`MariaDbContainer::with_username`]/[`MariaDbContainer::with_password`]/
//! [`MariaDbContainer::with_database`] before `start()` to override any of them.
//!
//! ### Readiness — empirically pinned, following [`crate::mysql::MySqlContainer`]'s precedent exactly
//!
//! The official `mariadb` entrypoint double-boots exactly like MySQL's: once as a
//! throwaway "temp server" to run init scripts (which prints `ready for connections`
//! with `port: 0`, i.e. no port bound yet), then for real on port 3306. Captured
//! verbatim from a real `docker run mariadb:11.4` boot with this module's env
//! (`MARIADB_USER=test`, `MARIADB_DATABASE=test`, `MARIADB_ROOT_PASSWORD=test`):
//!
//! ```text
//! 2026-07-04  8:47:29 0 [Note] mariadbd: ready for connections.
//! Version: '11.4.12-MariaDB-ubu2404'  socket: '/run/mysqld/mysqld.sock'  port: 0  mariadb.org binary distribution
//! 2026-07-04  8:47:30 0 [Note] Server socket created on IP: '0.0.0.0', port: '3306'.
//! 2026-07-04  8:47:30 0 [Note] Server socket created on IP: '::', port: '3306'.
//! 2026-07-04  8:47:30 0 [Note] mariadbd: ready for connections.
//! Version: '11.4.12-MariaDB-ubu2404'  socket: '/run/mysqld/mysqld.sock'  port: 3306  mariadb.org binary distribution
//! ```
//!
//! Unlike MySQL 8.4, MariaDB has no X Plugin adding a third `ready for connections`
//! line with a decoy `3306`-prefixed port (`33060`), so there's no false-match trap to
//! anchor against — but the temp server's `port: 0` line still means an unanchored
//! `times=2` count would be correct only by coincidence (it happens to work here
//! because there are exactly two `ready for connections` lines total). This module
//! follows the MySQL house precedent anyway and anchors on the literal `port: 3306`
//! immediately followed, somewhere later on the same line, by `mariadb.org binary
//! distribution` — so the wait is robust to the temp-server line's exact wording even
//! if a future MariaDB point release changes it:
//! `.*port: 3306.*mariadb\.org binary distribution.*`, via the ordinary
//! [`Wait::for_log_message`] path.
//!
//! No `with_memory_limit` override needed — same InnoDB-footprint precedent as MySQL
//! 8.4: boots clean on msb's default ~450M microVM RAM (observed ~14.8s IT round-trip
//! on msb).

use std::time::Duration;

use rightsize::{Container, ContainerGuard, Result, Wait};

const PORT: u16 = 3306;

/// A single-node MariaDB container.
pub struct MariaDbContainer {
    container: Container,
    username: String,
    password: String,
    database: String,
}

impl MariaDbContainer {
    /// Builds a container from the pinned default image (`mariadb:11.4`).
    pub fn new() -> Self {
        Self::with_image("mariadb:11.4")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let username = "test".to_string();
        let password = "test".to_string();
        let database = "test".to_string();
        let container = Container::new(image)
            .with_exposed_ports(&[PORT])
            .with_env("MARIADB_USER", &username)
            .with_env("MARIADB_PASSWORD", &password)
            .with_env("MARIADB_DATABASE", &database)
            .with_env("MARIADB_ROOT_PASSWORD", "test")
            // Anchored on the real server's line (see the module doc for the
            // captured log excerpt and why an unanchored "port: 3306" search would
            // only be correct by coincidence here).
            .waiting_for(
                Wait::for_log_message(r".*port: 3306.*mariadb\.org binary distribution.*", 1)
                    .with_startup_timeout(Duration::from_secs(60)),
            );
        Self {
            container,
            username,
            password,
            database,
        }
    }

    /// Overrides `MARIADB_USER` (default `test`).
    pub fn with_username(mut self, username: &str) -> Self {
        self.username = username.to_string();
        self.container = self.container.with_env("MARIADB_USER", username);
        self
    }

    /// Overrides `MARIADB_PASSWORD` (default `test`).
    pub fn with_password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self.container = self.container.with_env("MARIADB_PASSWORD", password);
        self
    }

    /// Overrides `MARIADB_DATABASE` (default `test`).
    pub fn with_database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self.container = self.container.with_env("MARIADB_DATABASE", database);
        self
    }

    /// Boots the container.
    pub async fn start(self) -> Result<MariaDbGuard> {
        crate::register_default_backends();
        let guard = self.container.start().await?;
        Ok(MariaDbGuard {
            guard,
            username: self.username,
            password: self.password,
            database: self.database,
        })
    }
}

impl Default for MariaDbContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`MariaDbContainer`].
pub struct MariaDbGuard {
    guard: ContainerGuard,
    username: String,
    password: String,
    database: String,
}

impl MariaDbGuard {
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

    /// A `mysql://` connection string for the running container's
    /// [`Self::database_name`] — MariaDB speaks the MySQL wire protocol, so this
    /// crate's house `mysql://` scheme (matching [`crate::mysql::MySqlGuard::connection_string`])
    /// applies here too.
    pub fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.username,
            self.password,
            self.guard.host(),
            self.guard.get_mapped_port(PORT).unwrap(),
            self.database,
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.guard.stop().await
    }
}

impl std::ops::Deref for MariaDbGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rightsize::wait::{WaitStrategy, WaitTarget};

    #[test]
    fn defaults_are_the_test_trio() {
        let c = MariaDbContainer::new();
        assert_eq!(c.username, "test");
        assert_eq!(c.password, "test");
        assert_eq!(c.database, "test");
    }

    #[test]
    fn builders_override_the_defaults() {
        let c = MariaDbContainer::new()
            .with_username("alice")
            .with_password("s3cret")
            .with_database("app");
        assert_eq!(c.username, "alice");
        assert_eq!(c.password, "s3cret");
        assert_eq!(c.database, "app");
    }

    const CAPTURED_LOG: &str = "\
2026-07-04  8:47:29 0 [Note] mariadbd: ready for connections.
Version: '11.4.12-MariaDB-ubu2404'  socket: '/run/mysqld/mysqld.sock'  port: 0  mariadb.org binary distribution
2026-07-04  8:47:30 0 [Note] Server socket created on IP: '0.0.0.0', port: '3306'.
2026-07-04  8:47:30 0 [Note] Server socket created on IP: '::', port: '3306'.
2026-07-04  8:47:30 0 [Note] mariadbd: ready for connections.
Version: '11.4.12-MariaDB-ubu2404'  socket: '/run/mysqld/mysqld.sock'  port: 3306  mariadb.org binary distribution";

    struct FakeTarget(std::sync::Mutex<String>);
    #[async_trait::async_trait]
    impl WaitTarget for FakeTarget {
        fn host(&self) -> &str {
            "127.0.0.1"
        }
        fn mapped_port(&self, guest_port: u16) -> u16 {
            guest_port
        }
        fn exposed_guest_ports(&self) -> Vec<u16> {
            vec![PORT]
        }
        async fn current_logs(&self) -> String {
            self.0.lock().unwrap().clone()
        }
        fn describe(&self) -> String {
            "fake-mariadb".to_string()
        }
    }

    // Pins the anchored pattern (`.*port: 3306.*mariadb\.org binary distribution.*`)
    // against the temp server's `port: 0` line only — must not signal ready yet.
    #[tokio::test]
    async fn temp_server_port_zero_line_does_not_signal_ready() {
        let partial: String = CAPTURED_LOG.lines().take(2).collect::<Vec<_>>().join("\n");
        let target = FakeTarget(std::sync::Mutex::new(partial));
        let err = Wait::for_log_message(r".*port: 3306.*mariadb\.org binary distribution.*", 1)
            .with_startup_timeout(Duration::from_millis(300))
            .wait_until_ready(&target)
            .await
            .expect_err("the temp server's port: 0 line must not signal ready");
        let _ = err;
    }

    #[tokio::test]
    async fn only_the_real_servers_port_3306_line_signals_ready() {
        let target = FakeTarget(std::sync::Mutex::new(CAPTURED_LOG.to_string()));
        Wait::for_log_message(r".*port: 3306.*mariadb\.org binary distribution.*", 1)
            .with_startup_timeout(Duration::from_secs(5))
            .wait_until_ready(&target)
            .await
            .expect("the real server's port: 3306 line must signal ready");
    }
}
