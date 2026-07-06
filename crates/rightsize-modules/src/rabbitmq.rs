//! A single-node RabbitMQ container with the management plugin enabled. Defaults to a
//! `guest`/`guest` credential pair (the image's own default) so
//! [`RabbitMqGuard::amqp_url`] is usable with zero configuration; call
//! [`RabbitMqContainer::with_username`]/[`RabbitMqContainer::with_password`] before
//! `start()` to override either.
//!
//! ### Readiness — verified against a real 4.x boot
//!
//! `rabbitmq:4-management-alpine` still prints the same `"Server startup complete"`
//! line the 3.x series used (captured verbatim from a real boot with this module's
//! env):
//!
//! ```text
//! ...
//! 2026-07-04 08:47:17.936423+00:00 [info] <0.1036.0> started TCP listener on [::]:5672
//!  completed with 4 plugins.
//! 2026-07-04 08:47:18.001311+00:00 [info] <0.900.0> Server startup complete; 4 plugins started.
//! 2026-07-04 08:47:18.001311+00:00 [info] <0.900.0>  * rabbitmq_prometheus
//! 2026-07-04 08:47:18.001311+00:00 [info] <0.900.0>  * rabbitmq_management
//! 2026-07-04 08:47:18.001311+00:00 [info] <0.900.0>  * rabbitmq_management_agent
//! 2026-07-04 08:47:18.001311+00:00 [info] <0.900.0>  * rabbitmq_web_dispatch
//! ```
//!
//! The line appears exactly once, so `for_log_message` at `times=1` is unambiguous —
//! unlike Postgres/MySQL/MariaDB, there is no double-boot restart to race here. The
//! management API's own `/api/health/checks/...` endpoints require authenticated
//! requests, so the log line is the simpler and equally reliable readiness signal.
//!
//! No control characters were found in the image's baked env (checked
//! via `docker image inspect`), so no env override is needed here — unlike
//! [`crate::postgres::PostgresContainer`].
//!
//! No `with_memory_limit` override: booted clean on msb's default ~450M microVM RAM
//! (observed ~5.5s IT round-trip on both backends — an Erlang VM, not a JVM, so no
//! Paketo/QuickStart-style heap demand; no memory-ladder escalation was needed).
//!
//! ### A 4.x behavior change worth knowing (not this module's concern, but bites naive clients)
//!
//! RabbitMQ 4.x deprecates `transient_nonexcl_queues` and, per the broker's own startup
//! warning, "this feature can still be used for now" but a client that declares a
//! **non-durable, non-exclusive** queue (`durable=false, exclusive=false`) may be
//! rejected with `reply-code=541 INTERNAL_ERROR` depending on the deployed policy —
//! reproduced directly against this module's pinned image. Declare durable,
//! non-exclusive queues (or exclusive transient ones) from client code exercising this
//! container; this module itself declares no queues.

use rightsize::{Container, ContainerGuard, Result, Wait};

const AMQP_PORT: u16 = 5672;
const MANAGEMENT_PORT: u16 = 15672;

/// A single-node RabbitMQ container with the management plugin enabled.
pub struct RabbitMqContainer {
    container: Container,
    username: String,
    password: String,
}

impl RabbitMqContainer {
    /// Builds a container from the pinned default image
    /// (`rabbitmq:4-management-alpine`).
    pub fn new() -> Self {
        Self::with_image("rabbitmq:4-management-alpine")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let username = "guest".to_string();
        let password = "guest".to_string();
        let container = Container::new(image)
            .with_exposed_ports(&[AMQP_PORT, MANAGEMENT_PORT])
            .with_env("RABBITMQ_DEFAULT_USER", &username)
            .with_env("RABBITMQ_DEFAULT_PASS", &password)
            // Exactly-once log line (see the module doc for the captured excerpt) —
            // no restart race.
            .waiting_for(Wait::for_log_message(".*Server startup complete.*", 1));
        Self {
            container,
            username,
            password,
        }
    }

    /// Overrides `RABBITMQ_DEFAULT_USER` (default `guest`).
    pub fn with_username(mut self, username: &str) -> Self {
        self.username = username.to_string();
        self.container = self.container.with_env("RABBITMQ_DEFAULT_USER", username);
        self
    }

    /// Overrides `RABBITMQ_DEFAULT_PASS` (default `guest`).
    pub fn with_password(mut self, password: &str) -> Self {
        self.password = password.to_string();
        self.container = self.container.with_env("RABBITMQ_DEFAULT_PASS", password);
        self
    }

    /// Boots the container.
    pub async fn start(self) -> Result<RabbitMqGuard> {
        crate::register_default_backends();
        let guard = self.container.start().await?;
        Ok(RabbitMqGuard {
            guard,
            username: self.username,
            password: self.password,
        })
    }
}

impl Default for RabbitMqContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`RabbitMqContainer`].
pub struct RabbitMqGuard {
    guard: ContainerGuard,
    username: String,
    password: String,
}

impl RabbitMqGuard {
    /// The configured management/AMQP user (default `guest`).
    pub fn username(&self) -> &str {
        &self.username
    }

    /// The configured management/AMQP password (default `guest`).
    pub fn password(&self) -> &str {
        &self.password
    }

    /// An `amqp://` URL (with credentials) for the running container's AMQP listener.
    pub fn amqp_url(&self) -> String {
        format!(
            "amqp://{}:{}@{}:{}",
            self.username,
            self.password,
            self.guard.host(),
            self.guard.get_mapped_port(AMQP_PORT).unwrap()
        )
    }

    /// The management UI/API base URI for the running container.
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

impl std::ops::Deref for RabbitMqGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.guard
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_the_guest_pair() {
        let c = RabbitMqContainer::new();
        assert_eq!(c.username, "guest");
        assert_eq!(c.password, "guest");
    }

    #[test]
    fn builders_override_the_defaults() {
        let c = RabbitMqContainer::new()
            .with_username("alice")
            .with_password("s3cret");
        assert_eq!(c.username, "alice");
        assert_eq!(c.password, "s3cret");
    }
}
