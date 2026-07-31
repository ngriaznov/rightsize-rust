//! A single-node Apache Cassandra container.
//!
//! ### `GPG_KEYS` must be overridden to a tab-free value — this is not optional
//!
//! `cassandra:5.0.8`'s baked env includes a `GPG_KEYS` value containing a literal TAB
//! character (a package-signing key list built with tab-separated continuation,
//! same shape as the `DOCKER_PG_LLVM_DEPS`/`postgres:*-alpine` case documented on
//! [`crate::postgres::PostgresContainer`]). msb's krun VMM builder — 0.6.6 and the pinned 0.6.8 alike — panics on any
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
//!
//! ### Compatibility checking
//!
//! [`CassandraContainer::with_image`] parses the supplied image with
//! [`rightsize::ImageName`] and checks its repository against `cassandra` (registry
//! host, tag, and digest stripped) before ever touching a backend, returning
//! [`rightsize::RightsizeError::IncompatibleImage`] on a mismatch rather than letting
//! an unrelated image run all the way to a wait-strategy timeout. Pass
//! `ImageName::parse(image).as_compatible_substitute_for("cassandra")` to override for
//! a verified drop-in replacement. [`CassandraContainer::new`] goes through the same
//! check against its own floating reference, so it can never fail in practice.
//!
//! ### `new()` floats to `cassandra:latest`
//!
//! This module used to pin `cassandra:5.0.8`; `new()` now floats to
//! `cassandra:latest` so the version tracks upstream rather than this crate's own
//! release cycle. The `GPG_KEYS`/heap/memory/readiness facts above were all verified
//! against that `5.0.8` boot specifically — kept unconditionally (the `GPG_KEYS`
//! override remains required to boot on microsandbox at all, and is a no-op
//! elsewhere), but do not assume they still hold exactly at whatever version
//! `latest` resolves to without re-measuring.

use std::time::Duration;

use rightsize::{Container, ContainerGuard, ImageName, Result, Wait};

const CQL_PORT: u16 = 9042;

/// Cassandra's own default local datacenter name under `SimpleSnitch`, unconfigurable
/// by this module — see [`CassandraGuard::local_datacenter`].
const LOCAL_DATACENTER: &str = "datacenter1";

/// The repository this module understands — see the module doc's compatibility
/// section.
const EXPECTED_REPOSITORY: &str = "cassandra";

/// A single-node Apache Cassandra container.
pub struct CassandraContainer {
    container: Container,
    image: ImageName,
}

impl CassandraContainer {
    /// Builds a container from the floating default image (`cassandra:latest`).
    pub fn new() -> Self {
        Self::with_image("cassandra:latest")
    }

    /// Builds a container from a caller-chosen image. The repository is checked when
    /// the container starts, not here, so this constructor stays infallible like every
    /// other module's — see [`CassandraContainer::start`].
    pub fn with_image(image: impl Into<ImageName>) -> Self {
        let image = image.into();
        Self {
            container: Container::new(image.as_str())
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
            image,
        }
    }

    /// Boots the container, after checking the image is one this module understands.
    ///
    /// The compatibility check runs here rather than in the constructors so those stay
    /// infallible and match every other module in this crate. It is still the first
    /// thing to happen — before any backend is resolved or any sandbox is created — so
    /// a mismatched image fails with
    /// [`rightsize::RightsizeError::IncompatibleImage`] naming both repositories,
    /// never a bare wait-strategy timeout against the wrong server.
    pub async fn start(self) -> Result<CassandraGuard> {
        self.image.assert_compatible_with(EXPECTED_REPOSITORY)?;
        crate::register_default_backends();
        Ok(CassandraGuard(self.container.start().await?))
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

    // The compatibility check runs in `start()`, which needs a live backend, so these
    // exercise the exact condition `start()` evaluates against the stored image.

    #[test]
    fn the_floating_default_is_compatible() {
        CassandraContainer::new()
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("the floating default must satisfy this module's own check");
    }

    #[test]
    fn an_incompatible_repository_is_rejected_naming_both() {
        let err = CassandraContainer::with_image("postgres:16")
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect_err("postgres is not cassandra");
        let msg = err.to_string();
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("cassandra"), "{msg}");
    }

    #[test]
    fn a_declared_compatible_substitute_passes() {
        let image = ImageName::parse("mycorp/cassandra-hardened:5.0.8")
            .as_compatible_substitute_for("cassandra");
        CassandraContainer::with_image(image)
            .image
            .assert_compatible_with(EXPECTED_REPOSITORY)
            .expect("a declared compatible substitute must be accepted");
    }
}
