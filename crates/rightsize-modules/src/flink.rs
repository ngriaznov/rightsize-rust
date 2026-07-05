//! An Apache Flink **JobManager**, optionally paired with a companion **TaskManager**
//! via [`FlinkContainer::with_task_manager`] for a real session cluster that can
//! actually run jobs (a bare JobManager has zero task slots and can only accept/reject
//! submissions, never execute them).
//!
//! ### Topology
//!
//! A real Flink session cluster is two processes that must find each other over a
//! **persistent bidirectional RPC connection** (Pekko/Akka remoting): the TaskManager
//! dials the JobManager's RPC port (6123) at boot and *stays connected* —
//! registration is not a one-shot request/response, it is the first message on a
//! connection the TaskManager keeps open for as long as it runs, carrying heartbeats
//! and slot offers/task deployments both ways for the cluster's whole lifetime.
//! [`FlinkContainer::with_task_manager`] puts both containers on one
//! internally-created [`rightsize::Network`], aliases this JobManager as
//! `"jobmanager"`, and sets `FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager` on
//! **both** containers (the image's own env-driven config mechanism) — not just the
//! TaskManager. Verified directly: setting it on the TaskManager alone leaves the
//! JobManager's own Pekko actor system bound under its container hostname rather than
//! the alias, so every registration attempt from the TaskManager (correctly dialing
//! `pekko.tcp://flink@jobmanager:6123`) gets silently dropped as a non-local recipient
//! (`dropping message ... arriving at [pekko.tcp://flink@jobmanager:6123] inbound
//! addresses are [pekko.tcp://flink@<container-id>:6123]`) — the JobManager must be
//! told its own address is the alias too.
//!
//! ### Backend support — full on docker, JobManager-only on msb
//!
//! **Docker**: verified end-to-end — the TaskManager registers with the JobManager
//! (`Successful registration at resource manager ...` in its own log) and `GET
//! /taskmanagers` on the JobManager's REST port shows one slot-bearing TM within
//! seconds of both containers starting.
//!
//! **microsandbox**: [`FlinkContainer::with_task_manager`] returns
//! [`rightsize::RightsizeError::UnsupportedByBackend`] before ever booting anything.
//! The actual blocker is more basic than a Pekko/tunnel incompatibility: msb's
//! `install_network_links` requires `nc`/busybox *inside the consumer image* to serve
//! the tunnel's in-guest listener, and the official `flink:1.20.5` image is a bare
//! JRE + Flink install with neither — the attempt failed immediately with
//! `UnsupportedByBackend: network links (no nc/busybox in consumer image
//! 'flink:1.20.5')`, thrown from the msb backend's own `nc` prerequisite probe before
//! a single byte of Pekko traffic could be exchanged. Whether Pekko's
//! persistent-connection RPC registration would work over the tunnel's
//! single-connection-at-a-time model was never reached or tested — the missing
//! `nc`/busybox prerequisite stops the attempt before that question is even in play.
//! A bare JobManager (REST `/overview` only, no TM) works fine on msb — it needs no
//! network-link emulation at all, just the ordinary published-port HTTP path — so this
//! module still supports msb for JobManager-only use; only
//! [`FlinkContainer::with_task_manager`] is gated.
//!
//! ### Memory — JVM, ladder applies to both roles
//!
//! A JobManager settles around ~310 MiB RSS and a TaskManager around ~375 MiB RSS at
//! rest on docker with no cap (`docker stats`, real boot) — both comfortably over
//! msb's ~450 MB default *individually*, and this module runs the JobManager on msb
//! too (see above), so `with_memory_limit(1024)` is this module's default for both
//! roles, matching the family's established single-JVM floor
//! ([`crate::keycloak::KeycloakContainer`], [`crate::neo4j::Neo4jContainer`]).
//!
//! No control characters were found in the image's baked env (checked via
//! `docker image inspect`).

use std::sync::Arc;
use std::time::Duration;

use rightsize::{Container, ContainerGuard, Network, Result, RightsizeError, Wait};

const REST_PORT: u16 = 8081;
const RPC_PORT: u16 = 6123;
const JOBMANAGER_ALIAS: &str = "jobmanager";

/// An Apache Flink JobManager, with an optional companion TaskManager (see
/// [`FlinkContainer::with_task_manager`]).
pub struct FlinkContainer {
    container: Container,
    image: String,
    network: Option<Arc<Network>>,
    task_manager: Option<Container>,
}

impl FlinkContainer {
    /// Builds a container from the pinned default image (`flink:1.20.5`).
    pub fn new() -> Self {
        Self::with_image("flink:1.20.5")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        let container = Container::new(image)
            .with_exposed_ports(&[REST_PORT, RPC_PORT])
            .with_command(&["jobmanager"])
            .with_memory_limit(1024)
            .waiting_for(
                Wait::for_http("/overview")
                    .for_port(REST_PORT)
                    .with_startup_timeout(Duration::from_secs(120)),
            );
        Self {
            container,
            image: image.to_string(),
            network: None,
            task_manager: None,
        }
    }

    /// Adds a companion TaskManager, giving the cluster real task slots. **Docker
    /// only** — returns [`RightsizeError::UnsupportedByBackend`] on microsandbox (see
    /// the module doc's backend-support section); the remedy is to run this test
    /// under the docker backend instead. Must be called before [`FlinkContainer::start`].
    pub fn with_task_manager(mut self) -> Result<Self> {
        if rightsize::backends::active_name() == "microsandbox" {
            return Err(RightsizeError::unsupported_with_remedy(
                "Flink TaskManager topology (network links; no nc/busybox in the flink image)",
                "microsandbox",
                "run this test with RIGHTSIZE_BACKEND=docker instead; JobManager-only \
                 (no with_task_manager()) is still supported on microsandbox",
            ));
        }

        let net = self
            .network
            .get_or_insert_with(|| Arc::new(Network::new_network()))
            .clone();
        self.container = self
            .container
            .with_network(&net)
            .with_network_aliases(&[JOBMANAGER_ALIAS])
            // The JobManager must ALSO be told its own rpc.address is the alias, not
            // its container hostname — otherwise its Pekko actor system binds as
            // "flink@<container-id>" while the TaskManager (below) dials
            // "flink@jobmanager", and every registration attempt is silently dropped
            // as a non-local recipient (reproduced directly; see the module doc).
            .with_env(
                "FLINK_PROPERTIES",
                &format!("jobmanager.rpc.address: {JOBMANAGER_ALIAS}"),
            );
        self.task_manager = Some(
            Container::new(&self.image)
                .with_command(&["taskmanager"])
                .with_network(&net)
                .with_env(
                    "FLINK_PROPERTIES",
                    &format!("jobmanager.rpc.address: {JOBMANAGER_ALIAS}"),
                )
                .with_memory_limit(1024)
                // The TaskManager has no REST port of its own to wait on; it's ready
                // enough to start once its process is up, and the real proof of
                // life is its JobManager registration, which the module's own IT
                // polls for over the JobManager's REST API instead of a wait
                // strategy here.
                .waiting_for(Wait::for_log_message(".*", 0)),
        );
        Ok(self)
    }

    /// Boots the JobManager, then (if [`FlinkContainer::with_task_manager`] was
    /// called) the companion TaskManager.
    pub async fn start(self) -> Result<FlinkGuard> {
        let jobmanager = self.container.start().await?;
        let task_manager = match self.task_manager {
            Some(tm) => match tm.start().await {
                Ok(guard) => Some(guard),
                Err(e) => {
                    jobmanager.stop().await.ok();
                    return Err(e);
                }
            },
            None => None,
        };
        Ok(FlinkGuard {
            jobmanager,
            task_manager,
            network: self.network,
        })
    }
}

impl Default for FlinkContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`FlinkContainer`].
pub struct FlinkGuard {
    jobmanager: ContainerGuard,
    task_manager: Option<ContainerGuard>,
    network: Option<Arc<Network>>,
}

impl FlinkGuard {
    /// The JobManager REST base URI (`/overview`, `/taskmanagers`, job submission,
    /// etc.).
    pub fn rest_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.jobmanager.host(),
            self.jobmanager.get_mapped_port(REST_PORT).unwrap()
        )
    }

    /// Stops and removes the TaskManager (if any) and the JobManager, then closes the
    /// network created by [`FlinkContainer::with_task_manager`] (if any) — this module
    /// created that network internally, not the caller, so it owns closing it too, or
    /// a repeated-boot test/process leaks one per run.
    pub async fn stop(self) -> Result<()> {
        if let Some(tm) = self.task_manager {
            tm.stop().await?;
        }
        self.jobmanager.stop().await?;
        if let Some(net) = self.network {
            net.close().await?;
        }
        Ok(())
    }
}

impl std::ops::Deref for FlinkGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.jobmanager
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_image_smoke() {
        let _ = FlinkContainer::new();
        let _ = FlinkContainer::with_image("flink:1.20.5");
    }
}
