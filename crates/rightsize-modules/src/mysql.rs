//! A single-node MySQL container. Defaults to a `test`/`test`/`test`
//! user/password/database trio (plus `MYSQL_ROOT_PASSWORD=test`) so
//! [`MySqlGuard::connection_string`] is usable with zero configuration; call
//! [`MySqlContainer::with_username`]/[`MySqlContainer::with_password`]/
//! [`MySqlContainer::with_database`] before `start()` to override any of them.
//!
//! ### Readiness — empirically pinned, not guessed
//!
//! The official entrypoint boots `mysqld` **twice**: once as a throwaway "temp
//! server" to run init scripts, then for real. Both prints, plus the X Plugin's own
//! "ready for connections" line, contain the substring `ready for connections`, and
//! naively counting occurrences is a trap: the temp server's X Plugin binds `port:
//! 33060` — whose digits *start with* `3306`, so an unanchored `port: 3306` search
//! false-matches it too. Captured verbatim from a real `docker run mysql:8.4` boot
//! with this module's env (`MYSQL_USER=test`, `MYSQL_DATABASE=test`,
//! `MYSQL_ROOT_PASSWORD=test`):
//!
//! ```text
//! [System] [MY-011323] [Server] X Plugin ready for connections. Socket: /var/run/mysqld/mysqlx.sock
//! [System] [MY-010931] [Server] /usr/sbin/mysqld: ready for connections. Version: '8.4.10'  socket: '/var/run/mysqld/mysqld.sock'  port: 0  MySQL Community Server - GPL.
//! ...(init scripts run, temp server shuts down)...
//! [System] [MY-011323] [Server] X Plugin ready for connections. Bind-address: '::' port: 33060, socket: /var/run/mysqld/mysqlx.sock
//! [System] [MY-010931] [Server] /usr/sbin/mysqld: ready for connections. Version: '8.4.10'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  MySQL Community Server - GPL.
//! ```
//!
//! Four lines contain `ready for connections`; only the last is the real server bound
//! to 3306. The temp server prints `port: 0` (no port yet) and the X Plugin lines
//! print `33060`, whose `3306` prefix would satisfy an unanchored match — so simple
//! occurrence-counting is fragile here. The fix is a regex anchored on the real
//! server's `port: 3306` with a trailing non-digit-or-end boundary
//! (`.*mysqld: ready for connections.*port: 3306($|[^0-9]).*`). This module implements
//! that same anchoring *logic* as a small custom [`WaitStrategy`] with hand-written
//! character-boundary checks rather than a regex — predates `Wait::for_log_message`'s
//! move to a real regex engine (`regex-lite`), and the hand-written version is
//! unaffected by that swap since it was never routed through the shared matcher.

use std::time::Duration;

use rightsize::wait::{WaitStrategy, WaitTarget};
use rightsize::{Container, ContainerGuard, Result};

/// A single-node MySQL container.
pub struct MySqlContainer {
    container: Container,
    username: String,
    password: String,
    database: String,
}

impl MySqlContainer {
    const PORT: u16 = 3306;

    /// Builds a container from the pinned default image (`mysql:8.4`).
    pub fn new() -> Self {
        Self::with_image("mysql:8.4")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let username = "test".to_string();
        let password = "test".to_string();
        let database = "test".to_string();
        let container = Container::new(image)
            .with_exposed_ports(&[Self::PORT])
            .with_env("MYSQL_USER", &username)
            .with_env("MYSQL_PASSWORD", &password)
            .with_env("MYSQL_DATABASE", &database)
            .with_env("MYSQL_ROOT_PASSWORD", "test")
            // Anchored on the real server's line (see the module doc for the
            // captured log excerpt and why an unanchored "port: 3306" search
            // misfires on the temp-server boot).
            .waiting_for(MySqlReadyForConnections);
        // No with_memory_limit override: boots clean on msb's default ~450M microVM
        // RAM well under 60s (see the module report for timed evidence) — unlike
        // SpringCloudConfig's Paketo JVM image, MySQL 8.4's InnoDB default footprint
        // fits the default, so no module-level memory floor is warranted here.
        Self {
            container,
            username,
            password,
            database,
        }
    }

    /// Overrides `MYSQL_USER` (default `test`).
    pub fn with_username(mut self, username: &str) -> Self {
        self.username = username.to_string();
        self.container = self.container.with_env("MYSQL_USER", username);
        self
    }

    /// Overrides `MYSQL_PASSWORD` (default `test`).
    pub fn with_password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self.container = self.container.with_env("MYSQL_PASSWORD", password);
        self
    }

    /// Overrides `MYSQL_DATABASE` (default `test`).
    pub fn with_database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self.container = self.container.with_env("MYSQL_DATABASE", database);
        self
    }

    /// Boots the container.
    pub async fn start(self) -> Result<MySqlGuard> {
        let guard = self.container.start().await?;
        Ok(MySqlGuard {
            guard,
            username: self.username,
            password: self.password,
            database: self.database,
        })
    }
}

impl Default for MySqlContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The exact substring the real (non-temp, non-X-Plugin) server line contains, up to
/// but not including the port number.
const READY_PREFIX: &str = "mysqld: ready for connections";
/// The port marker immediately preceded by `READY_PREFIX` somewhere earlier on the
/// same line, per the captured log shape in the module doc.
const PORT_MARKER: &str = "port: 3306";

/// Ready when a log line contains `mysqld: ready for connections` and, later on that
/// same line, `port: 3306` immediately followed by end-of-line or a non-digit — never
/// matching the X Plugin's `port: 33060` (whose digits merely start with `3306`).
struct MySqlReadyForConnections;

impl MySqlReadyForConnections {
    fn line_signals_ready(line: &str) -> bool {
        if !line.contains(READY_PREFIX) {
            return false;
        }
        let Some(idx) = line.find(PORT_MARKER) else {
            return false;
        };
        match line[idx + PORT_MARKER.len()..].chars().next() {
            None => true,                   // end of line right after "port: 3306"
            Some(c) => !c.is_ascii_digit(), // anything but another digit (rules out 33060)
        }
    }

    async fn is_ready(target: &dyn WaitTarget) -> bool {
        let logs = target.current_logs().await;
        logs.lines().any(Self::line_signals_ready)
    }
}

#[async_trait::async_trait]
impl WaitStrategy for MySqlReadyForConnections {
    async fn wait_until_ready(&self, target: &dyn WaitTarget) -> Result<()> {
        rightsize::wait::poll_until_ready(
            target,
            Duration::from_secs(60),
            "mysqld ready for connections on port 3306",
            || Self::is_ready(target),
        )
        .await
    }

    fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
        self
    }
}

/// The running guard for a [`MySqlContainer`].
pub struct MySqlGuard {
    guard: ContainerGuard,
    username: String,
    password: String,
    database: String,
}

impl MySqlGuard {
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
    /// [`Self::database_name`].
    pub fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.username,
            self.password,
            self.guard.host(),
            self.guard.get_mapped_port(MySqlContainer::PORT).unwrap(),
            self.database,
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.guard.stop().await
    }
}

impl std::ops::Deref for MySqlGuard {
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
        let c = MySqlContainer::new();
        assert_eq!(c.username, "test");
        assert_eq!(c.password, "test");
        assert_eq!(c.database, "test");
    }

    #[test]
    fn builders_override_the_defaults() {
        let c = MySqlContainer::new()
            .with_username("alice")
            .with_password("s3cret")
            .with_database("app");
        assert_eq!(c.username, "alice");
        assert_eq!(c.password, "s3cret");
        assert_eq!(c.database, "app");
    }

    // The exact captured log excerpt from the module doc: four "ready for
    // connections" lines, only the last of which must be recognized as ready.
    const CAPTURED_LOG: &str = "\
[System] [MY-011323] [Server] X Plugin ready for connections. Socket: /var/run/mysqld/mysqlx.sock
[System] [MY-010931] [Server] /usr/sbin/mysqld: ready for connections. Version: '8.4.10'  socket: '/var/run/mysqld/mysqld.sock'  port: 0  MySQL Community Server - GPL.
[System] [MY-011323] [Server] X Plugin ready for connections. Bind-address: '::' port: 33060, socket: /var/run/mysqld/mysqlx.sock
[System] [MY-010931] [Server] /usr/sbin/mysqld: ready for connections. Version: '8.4.10'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  MySQL Community Server - GPL.";

    #[test]
    fn temp_server_and_x_plugin_lines_do_not_signal_ready() {
        for line in CAPTURED_LOG.lines().take(3) {
            assert!(
                !MySqlReadyForConnections::line_signals_ready(line),
                "must not misfire on: {line}"
            );
        }
    }

    #[test]
    fn only_the_real_servers_port_3306_line_signals_ready() {
        let real_line = CAPTURED_LOG.lines().nth(3).unwrap();
        assert!(MySqlReadyForConnections::line_signals_ready(real_line));
    }

    #[test]
    fn port_3306_at_end_of_line_with_no_trailing_text_also_matches() {
        assert!(MySqlReadyForConnections::line_signals_ready(
            "mysqld: ready for connections. port: 3306"
        ));
    }

    #[tokio::test]
    async fn is_ready_true_only_once_the_real_line_is_in_the_logs() {
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
                vec![3306]
            }
            async fn current_logs(&self) -> String {
                self.0.lock().unwrap().clone()
            }
            fn describe(&self) -> String {
                "fake-mysql".to_string()
            }
        }

        let partial: String = CAPTURED_LOG.lines().take(3).collect::<Vec<_>>().join("\n");
        let target = FakeTarget(std::sync::Mutex::new(partial));
        assert!(!MySqlReadyForConnections::is_ready(&target).await);

        *target.0.lock().unwrap() = CAPTURED_LOG.to_string();
        assert!(MySqlReadyForConnections::is_ready(&target).await);
    }
}
