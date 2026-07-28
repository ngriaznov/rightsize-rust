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
//! reproduced directly against this module's previously pinned
//! `rabbitmq:4-management-alpine` image (see below — `new()` floats since then, but
//! this behavior is documented RabbitMQ 4.x entrypoint behavior, not something tied to
//! that specific tag). Declare durable, non-exclusive queues (or exclusive transient
//! ones) from client code exercising this container; this module itself declares no
//! queues.
//!
//! ### Compatibility checking
//!
//! [`RabbitMqContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `rabbitmq` (registry
//! host, tag, and digest stripped) before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("rabbitmq")` to override for
//! a verified drop-in replacement. [`RabbitMqContainer::new`] goes through the same
//! check against its own floating reference, so it can never fail in practice.
//!
//! ### `new()` floats to `rabbitmq:management`, not `rabbitmq:latest`
//!
//! This module used to pin `rabbitmq:4-management-alpine`. Every other module in this
//! crate that has a `new()` floats to `<repository>:latest`, but plain
//! `rabbitmq:latest` carries no management plugin at all — this module is built
//! around it (its readiness wait and every helper below depend on the management API
//! and its startup log lines). `rabbitmq:management` is the floating tag that keeps
//! the plugin while still tracking upstream rather than a version pinned to this
//! crate's own release cycle. The readiness/behavior facts above were verified
//! against that previous `rabbitmq:4-management-alpine` boot specifically.

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

const AMQP_PORT: u16 = 5672;
const MANAGEMENT_PORT: u16 = 15672;

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "rabbitmq";

/// A single-node RabbitMQ container with the management plugin enabled.
pub struct RabbitMqContainer {
    container: Container,
    image: ImageName,
    username: String,
    password: String,
}

impl RabbitMqContainer {
    /// Builds a container from the floating default image (`rabbitmq:management`) —
    /// see the module doc for why this is `management`, not `latest`.
    pub fn new() -> Self {
        Self::with_image("rabbitmq:management")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`RabbitMqContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        let username = "guest".to_string();
        let password = "guest".to_string();
        let container = Container::new(image.as_str())
            .with_exposed_ports(&[AMQP_PORT, MANAGEMENT_PORT])
            .with_env("RABBITMQ_DEFAULT_USER", &username)
            .with_env("RABBITMQ_DEFAULT_PASS", &password)
            // Exactly-once log line (see the module doc for the captured excerpt) —
            // no restart race.
            .waiting_for(Wait::for_log_message(".*Server startup complete.*", 1));
        Self {
            container,
            image,
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

    /// Boots the container, after checking the image is one this module understands.
    ///
    /// The compatibility check runs here rather than in the constructors so those stay
    /// infallible and match every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server.
    pub async fn start(self) -> Result<RabbitMqGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
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

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        RabbitMqContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = RabbitMqContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not rabbitmq");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("rabbitmq"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/rabbitmq-hardened:4-management")
            .as_compatible_substitute_for("rabbitmq");
        RabbitMqContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
