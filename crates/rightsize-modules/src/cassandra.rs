//! A single-node Apache Cassandra container.
//!
//! ### `GPG_KEYS` must be overridden to a tab-free value — this is not optional
//!
//! `cassandra:5.0.8`'s baked env includes a `GPG_KEYS` value containing a literal TAB
//! character (a package-signing key list built with tab-separated continuation,
//! same shape as the `DOCKER_PG_LLVM_DEPS`/`postgres:*-alpine` case documented on
//! [`crate::postgres::PostgresContainer`]). msb 0.6.6's krun VMM builder panics on any
//! baked env value containing a control character, and it panics *before the guest
//! ever boots* — reproduced directly, identical `msb run` invocation:
//!
//! ```text
//! sandbox process exited (signal: 6 (SIGABRT)) before agent relay became available
//! ```
//!
//! with `msb logs --source system` showing the actual panic site:
//!
//! ```text
//! panicked at msb_krun_vmm-0.1.25/src/builder.rs:1154: ... Err value: InvalidAscii
//! ```
//!
//! `.with_env("GPG_KEYS", "")` sidesteps it: `GPG_KEYS` is build-time-only in this
//! image (used only when the image itself is built, to import signing keys), so
//! overriding it has zero effect at container-run time. Verified directly: the
//! identical `msb run` aborts with the panic above without this override and boots
//! clean with it. Docker is unaffected either way — this override is a no-op there,
//! not a workaround for a docker-specific problem. This module always sets it; it is
//! not exposed as a builder override, because there is no reason a caller would ever
//! want the tab-bearing value back.
//!
//! ### Heap — kept small on purpose
//!
//! `MAX_HEAP_SIZE=512M` and `HEAP_NEWSIZE=128M` keep the JVM's own heap modest rather
//! than letting it size itself off host memory, the same reasoning this crate's other
//! JVM modules apply via `with_memory_limit` alone — here the image's own env knobs
//! are the more direct lever.
//!
//! ### Memory limit — 2560 MB, verified at that value
//!
//! `with_memory_limit(2560)` is this module's default, verified against a real boot
//! with the heap settings above.
//!
//! ### Readiness — `Starting listening for CQL clients`, observed at 58s
//!
//! That line is Cassandra's own log signal that the CQL native protocol port (9042)
//! is accepting connections. Startup timeout is 300s: 58s was observed on a quiet
//! local machine, and this crate's precedent for a single heavyweight JVM server —
//! 180s for [`crate::keycloak::KeycloakContainer`] and
//! [`crate::mysql::MySqlContainer`]'s loaded-CI-runner case — undershoots a server
//! this much heavier than either, so the budget here is wider rather than reused
//! as-is.
//!
//! Verified end to end: `cqlsh` ran `CREATE KEYSPACE` → `CREATE TABLE` → `INSERT` →
//! `SELECT`, and the row came back.

use std::time::Duration;

use rightsize::{Container, ContainerGuard, Result, Wait};

const CQL_PORT: u16 = 9042;

/// Cassandra's own default local datacenter name under `SimpleSnitch`, unconfigurable
/// by this module — see [`CassandraGuard::local_datacenter`].
const LOCAL_DATACENTER: &str = "datacenter1";

/// A single-node Apache Cassandra container.
pub struct CassandraContainer(Container);

impl CassandraContainer {
    /// Builds a container from the pinned default image (`cassandra:5.0.8`).
    pub fn new() -> Self {
        Self::with_image("cassandra:5.0.8")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        Self(
            Container::new(image)
                .with_exposed_ports(&[CQL_PORT])
                // Required to boot on msb at all — see the module doc's GPG_KEYS
                // section for the exact panic this sidesteps and why it's a no-op on
                // Docker and at Cassandra's own runtime.
                .with_env("GPG_KEYS", "")
                .with_env("MAX_HEAP_SIZE", "512M")
                .with_env("HEAP_NEWSIZE", "128M")
                .with_memory_limit(2560)
                .waiting_for(
                    Wait::for_log_message(".*Starting listening for CQL clients.*", 1)
                        .with_startup_timeout(Duration::from_secs(300)),
                ),
        )
    }

    /// Boots the container.
    pub async fn start(self) -> Result<CassandraGuard> {
        crate::register_default_backends();
        Ok(CassandraGuard(self.0.start().await?))
    }
}

impl Default for CassandraContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`CassandraContainer`].
pub struct CassandraGuard(ContainerGuard);

impl CassandraGuard {
    /// The `<host>:<port>` CQL contact point, for drivers that take one directly
    /// (e.g. the Datastax/Apache Cassandra drivers' contact-point list).
    pub fn contact_point(&self) -> String {
        format!("{}:{}", self.0.host(), self.cql_port())
    }

    /// The mapped host port for the CQL native protocol (guest port 9042).
    pub fn cql_port(&self) -> u16 {
        self.0.get_mapped_port(CQL_PORT).unwrap()
    }

    /// The local datacenter name a driver's load-balancing policy needs
    /// (`datacenter1`) — this single-node image's own default under `SimpleSnitch`,
    /// not configurable by this module.
    pub fn local_datacenter(&self) -> &str {
        LOCAL_DATACENTER
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for CassandraGuard {
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
        let _ = CassandraContainer::new();
        let _ = CassandraContainer::with_image("cassandra:5.0.8");
    }
}
