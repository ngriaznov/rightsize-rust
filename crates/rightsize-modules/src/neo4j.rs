//! A single-node Neo4j Community container, queried over its HTTP Cypher transaction
//! endpoint (`/db/neo4j/tx/commit`) — no bolt driver dependency needed, matching the
//! house convention for HTTP-first modules ([`crate::clickhouse::ClickHouseContainer`],
//! [`crate::pinot::PinotContainer`]). The bolt port (7687) is still exposed and its URI
//! available via [`Neo4jGuard::bolt_url`] for callers who do want a real driver.
//!
//! Defaults to a `neo4j`/`rightsize-test` username/password pair (the image refuses
//! passwords under 8 characters — `neo4j`/`neo4j` is rejected at boot) so
//! [`Neo4jGuard::http_url`] plus basic auth is usable with zero configuration; call
//! [`Neo4jContainer::with_password`] before `start()` to override it. The username is
//! fixed by the image at `neo4j` — there is no env var to change it.
//!
//! ### Readiness — `Started.` is the exact log line, verified against a real boot
//!
//! Captured verbatim from `neo4j:5-community`:
//!
//! ```text
//! ... INFO  Bolt enabled on 0.0.0.0:7687.
//! ... INFO  HTTP enabled on 0.0.0.0:7474.
//! ... INFO  Remote interface available at http://localhost:7474/
//! ... INFO  Started.
//! ```
//!
//! `Started.` is logged only after both connectors are already listening, so it is
//! both accurate and simpler than a two-port HTTP/bolt race; pinned as the wait
//! strategy over `for_http`.
//!
//! ### Memory — measured, needed the ladder
//!
//! At msb's default ~450 MB microVM RAM, the server logs `ERROR Invalid memory
//! configuration - exceeds physical memory. Check the configured values for
//! server.memory.pagecache.size and server.memory.heap.max_size` and shuts itself down
//! cleanly (`INFO Stopped.`) rather than hanging or getting OOM-killed — Neo4j's own
//! memory-recommendation calculator sizes the page cache and heap off total visible
//! RAM and refuses to start if the sums don't fit. A real docker boot with no memory
//! cap sits at ~468 MiB RSS (`docker stats`), just over that default budget. Retried
//! with `-m 1024M`: boots clean, the HTTP Cypher endpoint answers within the startup
//! timeout. `with_memory_limit(1024)` is this module's default, same number as
//! [`crate::keycloak::KeycloakContainer`] and matching this family's established floor
//! for a single JVM.
//!
//! No control characters were found in the image's baked env (checked via
//! `docker image inspect`), so no env override is needed here.

use std::time::Duration;

use rightsize::{Container, ContainerGuard, Result, Wait};

const HTTP_PORT: u16 = 7474;
const BOLT_PORT: u16 = 7687;

/// A single-node Neo4j Community container.
pub struct Neo4jContainer {
    container: Container,
    password: String,
}

impl Neo4jContainer {
    /// Builds a container from the pinned default image (`neo4j:5-community`).
    pub fn new() -> Self {
        Self::with_image("neo4j:5-community")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let password = "rightsize-test".to_string();
        let container = Container::new(image)
            .with_exposed_ports(&[HTTP_PORT, BOLT_PORT])
            .with_env("NEO4J_AUTH", &format!("neo4j/{password}"))
            // Single JVM, boots clean at 1024M — see the module doc for the
            // measured default-memory refusal (Neo4j's own memory calculator, not
            // an OOM kill) and the fix.
            .with_memory_limit(1024)
            .waiting_for(
                Wait::for_log_message(r".*Started\..*", 1)
                    .with_startup_timeout(Duration::from_secs(120)),
            );
        Self {
            container,
            password,
        }
    }

    /// Overrides `NEO4J_AUTH`'s password half (default `rightsize-test`; the image
    /// requires at least 8 characters).
    pub fn with_password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self.container = self
            .container
            .with_env("NEO4J_AUTH", &format!("neo4j/{password}"));
        self
    }

    /// Boots the container.
    pub async fn start(self) -> Result<Neo4jGuard> {
        crate::register_default_backends();
        let guard = self.container.start().await?;
        Ok(Neo4jGuard {
            guard,
            password: self.password,
        })
    }
}

impl Default for Neo4jContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`Neo4jContainer`].
pub struct Neo4jGuard {
    guard: ContainerGuard,
    password: String,
}

impl Neo4jGuard {
    /// The fixed admin username (`neo4j` — the image has no env var to change it).
    pub fn username(&self) -> &str {
        "neo4j"
    }

    /// The configured admin password (default `rightsize-test`).
    pub fn password(&self) -> &str {
        &self.password
    }

    /// The HTTP interface's base URI (Cypher transactions via `POST
    /// {http_url}/db/neo4j/tx/commit`).
    pub fn http_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.guard.host(),
            self.guard.get_mapped_port(HTTP_PORT).unwrap()
        )
    }

    /// The bolt interface's URI, for callers using a real bolt driver instead of the
    /// HTTP helpers.
    pub fn bolt_url(&self) -> String {
        format!(
            "bolt://{}:{}",
            self.guard.host(),
            self.guard.get_mapped_port(BOLT_PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host ports.
    pub async fn stop(self) -> Result<()> {
        self.guard.stop().await
    }
}

impl std::ops::Deref for Neo4jGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rightsize::wait::WaitTarget;

    #[test]
    fn default_password_is_rightsize_test() {
        let c = Neo4jContainer::new();
        assert_eq!(c.password, "rightsize-test");
    }

    #[test]
    fn with_password_overrides_the_default() {
        let c = Neo4jContainer::new().with_password("s3cret123");
        assert_eq!(c.password, "s3cret123");
    }

    // Pins this module's shipped pattern — `.*Started\..*`, a literal escaped dot,
    // now that `for_log_message` runs on a real regex engine — against the captured
    // line. (An earlier revision shipped the unescaped `.*Started..*` as a
    // workaround for a hand-rolled mini-matcher with no escape syntax; that
    // matcher is gone, so the pattern is back to meaning what it reads.)
    #[tokio::test]
    async fn readiness_pattern_matches_the_captured_started_line() {
        struct FakeTarget(String);
        #[async_trait::async_trait]
        impl WaitTarget for FakeTarget {
            fn host(&self) -> &str {
                "127.0.0.1"
            }
            fn mapped_port(&self, guest_port: u16) -> u16 {
                guest_port
            }
            fn exposed_guest_ports(&self) -> Vec<u16> {
                vec![HTTP_PORT, BOLT_PORT]
            }
            async fn current_logs(&self) -> String {
                self.0.clone()
            }
            fn describe(&self) -> String {
                "fake-neo4j".to_string()
            }
        }

        let captured_log = "\
... INFO  Bolt enabled on 0.0.0.0:7687.
... INFO  HTTP enabled on 0.0.0.0:7474.
... INFO  Remote interface available at http://localhost:7474/
... INFO  Started.";
        let target = FakeTarget(captured_log.to_string());
        let strategy = rightsize::Wait::for_log_message(r".*Started\..*", 1);
        strategy
            .wait_until_ready(&target)
            .await
            .expect("the escaped pattern must match the real captured log line");
    }

    // The escape is only meaningful if it actually narrows the match: this pins that
    // `\.` in the shipped pattern rejects a line where that position holds a
    // different character, distinguishing it from the unescaped `.*Started..*`
    // this module shipped while the mini-matcher had no escape syntax.
    #[tokio::test]
    async fn escaped_dot_does_not_match_an_arbitrary_character_in_its_place() {
        struct FakeTarget(&'static str);
        #[async_trait::async_trait]
        impl WaitTarget for FakeTarget {
            fn host(&self) -> &str {
                "127.0.0.1"
            }
            fn mapped_port(&self, guest_port: u16) -> u16 {
                guest_port
            }
            fn exposed_guest_ports(&self) -> Vec<u16> {
                vec![]
            }
            async fn current_logs(&self) -> String {
                self.0.to_string()
            }
            fn describe(&self) -> String {
                "fake-neo4j".to_string()
            }
        }

        let target = FakeTarget("... INFO  Startedx");
        let err = rightsize::Wait::for_log_message(r".*Started\..*", 1)
            .with_startup_timeout(Duration::from_millis(300))
            .wait_until_ready(&target)
            .await
            .expect_err("a non-dot character where the escaped dot is must not match");
        assert!(err.to_string().contains(r"Started\..*' x1"));
    }
}
