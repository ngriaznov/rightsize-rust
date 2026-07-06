//! A single-container Apache Pinot "QuickStart" cluster: controller, broker, server,
//! minion, and an embedded ZooKeeper, all as one process tree inside one image,
//! started with `QuickStart -type EMPTY` (a clean cluster with no demo tables — this
//! module is a real-cluster smoke fixture, not a data-loading harness).
//!
//! ### Ports — empirically verified, not the QuickStart docs' assumption
//!
//! The controller REST API is on 9000 as documented. The broker's **query** port is
//! **8000**, not 8099 — confirmed from a real `QuickStart -type EMPTY` boot log:
//!
//! ```text
//! StartControllerCommand ... -controllerPort 9000 ...
//! INFO: Started listener bound to [0.0.0.0:9000]
//! StartBrokerCommand ... -brokerPort 8000 -brokerGrpcPort 8010 ...
//! INFO: Started listener bound to [0.0.0.0:8000]
//! ```
//!
//! `curl http://<host>:8000/health` returns `503` while the broker is still
//! registering with the cluster and `200` once it's live; 8099 is not opened by
//! QuickStart at all. This module exposes 8000 and names the helper `broker_url`
//! accordingly.
//!
//! ### Memory — measured, not the originally-planned 2048 MB
//!
//! QuickStart runs a ZooKeeper + controller + broker + server + minion JVM in one
//! container, and the image itself bakes in `JAVA_OPTS=-Xms4G -Xmx4G` (confirmed via
//! `docker inspect`) — the QuickStartRunner driver JVM alone wants a 4 GiB heap before
//! the four sub-JVMs it spawns take anything. `with_memory_limit(2048)` (the
//! SpringCloudConfig/Paketo analogy, scaled up for four JVMs instead of one) badly
//! under-shot. Measured directly against a real `apachepinot/pinot:1.5.1
//! QuickStart -type EMPTY` boot:
//!
//! ```text
//! --memory=2048m -> OOMKilled=true (timed out at 180s waiting for /health, reaped by the kernel)
//! --memory=2560m -> OOMKilled=true
//! --memory=3072m -> OOMKilled=false; /health 200 within ~15s; BUT settles at ~99% of the 3 GiB
//!                   limit, and under that pressure the controller's Helix-backed schema/table
//!                   RPCs intermittently time out (a schema POST returns
//!                   {"code":500,"error":"java.util.concurrent.TimeoutException"}) even though
//!                   /health reports 200. Reproduced repeatedly at 3072m.
//! --memory=4096m -> stable: settles at ~73-75% of the limit, comfortable headroom; schema POST
//!                   succeeded on every attempt across a 60s repeated-POST probe.
//! ```
//!
//! So [`Container::with_memory_limit`] is **4096 MB** — the lowest round number with
//! real headroom above the image's own 4 GiB heap request, not merely enough to dodge
//! the OOM killer. Verified stable on both Docker and microsandbox.

use std::time::Duration;

use rightsize::{Container, ContainerGuard, Result, Wait};

const CONTROLLER_PORT: u16 = 9000;
const BROKER_PORT: u16 = 8000;

/// A single-container Apache Pinot QuickStart cluster.
pub struct PinotContainer(Container);

impl PinotContainer {
    /// Builds a container from the pinned default image (`apachepinot/pinot:1.5.1`).
    pub fn new() -> Self {
        Self::with_image("apachepinot/pinot:1.5.1")
    }

    /// Builds a container from a caller-chosen image.
    pub fn with_image(image: &str) -> Self {
        Self(
            Container::new(image)
                .with_exposed_ports(&[CONTROLLER_PORT, BROKER_PORT])
                .with_command(&["QuickStart", "-type", "EMPTY"])
                // Four JVMs (ZK, controller, broker, server/minion) in one container,
                // image bakes in -Xmx4G — see the module doc for the measured
                // 2048/2560/3072 MB failures and the 4096 MB fix.
                .with_memory_limit(4096)
                // A four-JVM cluster booting cold on a laptop is legitimately slow
                // (observed 60-120s); the controller's REST listener is up well
                // before the whole cluster has stabilized, but it's the only
                // readiness signal that's both meaningful and doesn't require
                // polling the broker separately before every test can proceed.
                .waiting_for(
                    Wait::for_http("/health")
                        .for_port(CONTROLLER_PORT)
                        .with_startup_timeout(Duration::from_secs(180)),
                ),
        )
    }

    /// Boots the container.
    pub async fn start(self) -> Result<PinotGuard> {
        crate::register_default_backends();
        Ok(PinotGuard(self.0.start().await?))
    }
}

impl Default for PinotContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for a [`PinotContainer`].
pub struct PinotGuard(ContainerGuard);

impl PinotGuard {
    /// The controller's REST base URI (schema/table/segment admin).
    pub fn controller_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.0.host(),
            self.0.get_mapped_port(CONTROLLER_PORT).unwrap()
        )
    }

    /// The broker's query base URI (port 8000 — see the module doc on the 8099
    /// assumption).
    pub fn broker_url(&self) -> String {
        format!(
            "http://{}:{}",
            self.0.host(),
            self.0.get_mapped_port(BROKER_PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host ports.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for PinotGuard {
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
        let _ = PinotContainer::new();
    }
}
