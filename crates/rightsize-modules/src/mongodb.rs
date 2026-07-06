//! A single-node MongoDB container running as a one-member replica set (required for
//! transactions/change streams). The [`Container::with_post_start`] hook initiates the
//! replica set and waits for a primary to be elected before `start()` returns, so
//! [`MongoDbGuard::connection_string`] is always usable immediately after `start()`.

use std::time::Duration;

use rightsize::{BoxFuture, Container, ContainerGuard, Result, RightsizeError, Wait};

const REPLICA_SET_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// A single-node MongoDB container, started as a one-member replica set named
/// `docker-rs`.
pub struct MongoDbContainer(Container);

impl MongoDbContainer {
    /// The guest port `mongod` listens on.
    const PORT: u16 = 27017;

    /// Builds a container from the pinned default image (`mongo:8.0`).
    pub fn new() -> Self {
        Self::with_image("mongo:8.0")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let container = Container::new(image)
            .with_exposed_ports(&[Self::PORT])
            .with_command(&["mongod", "--replSet", "docker-rs", "--bind_ip_all"])
            .waiting_for(Wait::for_listening_port())
            .with_post_start(|guard: &ContainerGuard| -> BoxFuture<'_, Result<()>> {
                Box::pin(async move {
                    initiate_replica_set(guard).await?;
                    await_primary_elected(guard).await
                })
            });
        Self(container)
    }

    /// Boots the container. Does not return until the replica set has a primary.
    pub async fn start(self) -> Result<MongoDbGuard> {
        crate::register_default_backends();
        Ok(MongoDbGuard(self.0.start().await?))
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
}
