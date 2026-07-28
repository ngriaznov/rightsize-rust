//! A single-node MongoDB container running as a one-member replica set (required for
//! transactions/change streams). The [`Container::with_post_start`] hook initiates the
//! replica set and waits for a primary to be elected before `start()` returns, so
//! [`MongoDbGuard::connection_string`] is always usable immediately after `start()`.
//!
//! ### Compatibility checking
//!
//! [`MongoDbContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `mongo` (registry host,
//! tag, and digest stripped) before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("mongo")` to override for a
//! verified drop-in replacement. [`MongoDbContainer::new`] goes through the same check
//! against its own floating reference, so it can never fail in practice.
//!
//! ### `new()` floats to `mongo:latest`
//!
//! This module used to pin `mongo:8.0`; `new()` now floats to `mongo:latest` so the
//! version tracks upstream rather than this crate's own release cycle.

use std::time::Duration;

use rightsize::{BoxFuture, Container, ContainerGuard, ImageName, Result, RightsizeError, Wait};

/// 180s, not the 60s this module used while it pinned `mongo:8.0`. A loaded Windows CI
/// runner was observed failing `rs.initiate` at the 60s mark against the floating
/// default (`mongo:latest`, 8.2.12 at the time), on a run whose whole suite took 28
/// minutes. This matches the budget [`crate::mysql::MySqlContainer`] and
/// [`crate::clickhouse::ClickHouseContainer`] already carry for the same reason: a
/// first-boot sequence that is comfortable locally and marginal on a contended runner.
const REPLICA_SET_TIMEOUT: Duration = Duration::from_secs(180);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "mongo";

/// A single-node MongoDB container, started as a one-member replica set named
/// `docker-rs`.
pub struct MongoDbContainer {
    container: Container,
    image: ImageName,
}

impl MongoDbContainer {
    /// The guest port `mongod` listens on.
    const PORT: u16 = 27017;

    /// Builds a container from the floating default image (`mongo:latest`).
    pub fn new() -> Self {
        Self::with_image("mongo:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`MongoDbContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        let container = Container::new(image.as_str())
            .with_exposed_ports(&[Self::PORT])
            .with_command(&["mongod", "--replSet", "docker-rs", "--bind_ip_all"])
            .waiting_for(Wait::for_listening_port())
            .with_post_start(|guard: &ContainerGuard| -> BoxFuture<'_, Result<()>> {
                Box::pin(async move {
                    initiate_replica_set(guard).await?;
                    await_primary_elected(guard).await
                })
            });
        Self { container, image }
    }

    /// Boots the container, after checking the image is one this module understands.
    /// Does not return until the replica set has a primary.
    ///
    /// The compatibility check runs here rather than in the constructors so those stay
    /// infallible and match every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server.
    pub async fn start(self) -> Result<MongoDbGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(MongoDbGuard(self.container.start().await?))
    }
}

impl Default for MongoDbContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// Retries `rs.initiate()` (via `rs.status()` first, so a retry after a partial
/// initiate doesn't re-initiate) through the proxy-accepts-before-mongod-listens race:
/// the listening-port wait can return before `mongod` is far enough along to
/// accept a real client command, so the first few `mongosh` invocations may fail —
/// that's expected and retried, not a fatal error.
async fn initiate_replica_set(guard: &ContainerGuard) -> Result<()> {
    poll_until(guard, "rs.initiate to succeed", |guard| {
        Box::pin(async move {
            let result = guard
                .exec(&[
                    "mongosh",
                    "--quiet",
                    "--eval",
                    "try { rs.status() } catch (e) { rs.initiate() }",
                ])
                .await;
            matches!(result, Ok(r) if r.exit_code == 0)
        })
    })
    .await
}

/// Polls until `db.hello().isWritablePrimary` reports `true`.
async fn await_primary_elected(guard: &ContainerGuard) -> Result<()> {
    poll_until(guard, "a PRIMARY to be elected", |guard| {
        Box::pin(async move {
            let result = guard
                .exec(&[
                    "mongosh",
                    "--quiet",
                    "--eval",
                    "db.hello().isWritablePrimary",
                ])
                .await;
            matches!(result, Ok(r) if r.stdout.trim().ends_with("true"))
        })
    })
    .await
}

/// A tiny deadline/poll-interval loop, local to this module. `cond` returning
/// `false` (including on an `exec` error, swallowed here) means "not yet"; the loop
/// keeps retrying until `cond` is `true` or the deadline passes.
async fn poll_until<F>(guard: &ContainerGuard, what: &str, mut cond: F) -> Result<()>
where
    F: FnMut(&ContainerGuard) -> BoxFuture<'_, bool>,
{
    let deadline = tokio::time::Instant::now() + REPLICA_SET_TIMEOUT;
    loop {
        if cond(guard).await {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    Err(RightsizeError::ContainerLaunch(format!(
        "Mongo replica set on {}:{} did not reach '{what}' within {}s",
        guard.host(),
        guard.get_mapped_port(MongoDbContainer::PORT).unwrap_or(0),
        REPLICA_SET_TIMEOUT.as_secs(),
    )))
}

/// The running guard for a [`MongoDbContainer`].
pub struct MongoDbGuard(ContainerGuard);

impl MongoDbGuard {
    /// A `mongodb://` connection string for the running container's `test` database.
    pub fn connection_string(&self) -> String {
        format!(
            "mongodb://{}:{}/test?directConnection=true",
            self.0.host(),
            self.0.get_mapped_port(MongoDbContainer::PORT).unwrap()
        )
    }

    /// Alias for [`Self::connection_string`]; the container is always a (single-node)
    /// replica set.
    pub fn replica_set_url(&self) -> String {
        self.connection_string()
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for MongoDbGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_image_smoke() {
        let _ = MongoDbContainer::new();
        let _ = MongoDbContainer::with_image("mongo:8.0");
    }

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        MongoDbContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = MongoDbContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not mongo");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("mongo"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image =
            ImageName::parse("mycorp/mongo-hardened:8.0").as_compatible_substitute_for("mongo");
        MongoDbContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
