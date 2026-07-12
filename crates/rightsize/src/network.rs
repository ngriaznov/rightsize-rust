//! Alias-based connectivity between containers, on either backend.
//!
//! Attach containers with `Container::with_network` and `Container::with_network_aliases`,
//! then reach a sibling via [`Network::resolve`]. On Docker this is a native network
//! alias; on microsandbox — where microVMs are fully isolated from each other —
//! rightsize emulates it with an `/etc/hosts` entry plus an exec-tunneled TCP relay (see
//! the msb backend's `install_network_links`).

use std::sync::{Arc, Mutex};

use crate::backend::{NetworkLink, SandboxBackend};
use crate::error::{Result, RightsizeError};

/// The view of a running container `Network` needs to compute links for later joiners —
/// implemented by `ContainerGuard`. Kept as a small trait rather than a
/// direct dependency on the container module so `network.rs` has no upward dependency
/// on `container.rs`; it's `container.rs` that depends on `network.rs`.
pub(crate) trait NetworkMember: Send + Sync {
    /// Whether this member is currently running (a stopped/never-started member
    /// contributes no links).
    fn is_running(&self) -> bool;
    /// This member's exposed guest port → mapped host port pairs, as of now.
    fn mapped_ports(&self) -> Vec<(u16, u16)>;
}

struct Member {
    container: Arc<dyn NetworkMember>,
    aliases: Vec<String>,
}

/// A named group of containers that can reach each other by alias. Created with
/// [`Network::new_network`], attached to containers via the `Container` builder, and
/// released with [`Network::close`] once no longer needed.
pub struct Network {
    id: String,
    members: Mutex<Vec<Member>>,
    backend_used: Mutex<Option<Arc<dyn SandboxBackend>>>,
}

impl Network {
    /// Creates a new, empty network with a random id. Attach containers to it before
    /// starting them.
    pub fn new_network() -> Network {
        Network {
            id: format!("rz-net-{}", crate::run_id::RunId::value()),
            members: Mutex::new(Vec::new()),
            backend_used: Mutex::new(None),
        }
    }

    /// This network's id, as passed to a backend's `ensure_network`/`remove_network`.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The `alias:guest_port` address a sibling uses to reach the member registered
    /// under `alias` — identical on both backends (native DNS on Docker, `/etc/hosts`
    /// alias on msb).
    ///
    /// Errors, naming `alias`, if no registered member carries it.
    pub fn resolve(&self, alias: &str, guest_port: u16) -> Result<String> {
        let members = self.members.lock().expect("Network members mutex poisoned");
        if !members.iter().any(|m| m.aliases.iter().any(|a| a == alias)) {
            return Err(RightsizeError::Backend(format!(
                "No container with alias '{alias}' registered on this network — check with_network_aliases(\"{alias}\")"
            )));
        }
        Ok(format!("{alias}:{guest_port}"))
    }

    /// Registers a newly-started member under `aliases`, remembering `backend` as the
    /// one to call `remove_network` on when this network closes. Must be called *after*
    /// [`Network::links_for_new_member`] for the same member, so a container never
    /// links to itself.
    pub(crate) fn register(
        &self,
        container: Arc<dyn NetworkMember>,
        aliases: Vec<String>,
        backend: Arc<dyn SandboxBackend>,
    ) {
        let mut members = self.members.lock().expect("Network members mutex poisoned");
        members.push(Member { container, aliases });
        *self
            .backend_used
            .lock()
            .expect("Network backend_used mutex poisoned") = Some(backend);
    }

    /// The links a newly-starting member needs installed: one per `(alias, guest port)`
    /// of every currently RUNNING sibling. Called *before* the new member itself is
    /// registered, so it can never produce a self-link.
    pub(crate) fn links_for_new_member(&self) -> Vec<NetworkLink> {
        let members = self.members.lock().expect("Network members mutex poisoned");
        let mut links = Vec::new();
        for member in members.iter().filter(|m| m.container.is_running()) {
            let ports = member.container.mapped_ports();
            for alias in &member.aliases {
                for &(guest_port, target_host_port) in &ports {
                    links.push(NetworkLink {
                        alias: alias.clone(),
                        guest_port,
                        target_host_port,
                    });
                }
            }
        }
        links
    }

    /// Releases the network on whichever backend last used it. Safe to call even if the
    /// network was never actually used by a backend.
    pub async fn close(&self) -> Result<()> {
        let backend = self
            .backend_used
            .lock()
            .expect("Network backend_used mutex poisoned")
            .clone();
        if let Some(backend) = backend {
            // Best-effort: a network that's already gone (or never created) isn't an error.
            let _ = backend.remove_network(&self.id).await;
            // No `Network`-level reaper-cache-dir test seam exists (or is needed —
            // no test asserts against the ledger through `Network::close`): every
            // caller here is production or a test that doesn't touch the ledger,
            // so this always uses the real, process-wide ledger. See
            // `crate::reaper::ledger_for`'s doc for what `None` means here.
            crate::reaper::after_remove_network(&self.id, None);
        }
        Ok(())
    }
}

impl Default for Network {
    fn default() -> Self {
        Self::new_network()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ContainerSpec, ExecResult};

    struct FakeMember {
        running: bool,
        ports: Vec<(u16, u16)>,
    }
    impl NetworkMember for FakeMember {
        fn is_running(&self) -> bool {
            self.running
        }
        fn mapped_ports(&self) -> Vec<(u16, u16)> {
            self.ports.clone()
        }
    }

    struct FakeBackend;
    #[async_trait::async_trait]
    impl SandboxBackend for FakeBackend {
        fn name(&self) -> &str {
            "fake"
        }
        fn supports_native_networks(&self) -> bool {
            true
        }
        async fn create(
            &self,
            _spec: ContainerSpec,
        ) -> Result<Box<dyn crate::backend::SandboxHandle>> {
            unimplemented!()
        }
        async fn start(&self, _handle: &dyn crate::backend::SandboxHandle) -> Result<()> {
            unimplemented!()
        }
        async fn stop(&self, _handle: &dyn crate::backend::SandboxHandle) -> Result<()> {
            unimplemented!()
        }
        async fn remove(&self, _handle: &dyn crate::backend::SandboxHandle) -> Result<()> {
            unimplemented!()
        }
        async fn exec(
            &self,
            _handle: &dyn crate::backend::SandboxHandle,
            _cmd: &[String],
        ) -> Result<ExecResult> {
            unimplemented!()
        }
        async fn logs(&self, _handle: &dyn crate::backend::SandboxHandle) -> Result<String> {
            unimplemented!()
        }
        async fn follow_logs(
            &self,
            _handle: &dyn crate::backend::SandboxHandle,
            _consumer: Box<dyn Fn(String) + Send + Sync>,
        ) -> Result<crate::backend::FollowHandle> {
            unimplemented!()
        }
        async fn ensure_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        async fn remove_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        fn cleanup_sync(&self, _container_id: &str) {}
        fn remove_by_name(&self, _name: &str) {}
        fn watchdog_kill_command(&self) -> Vec<String> {
            vec!["true".to_string()]
        }
    }

    #[test]
    fn new_network_id_has_the_rz_net_prefix() {
        let net = Network::new_network();
        assert!(net.id().starts_with("rz-net-"), "{}", net.id());
    }

    #[test]
    fn resolve_returns_alias_port_for_a_registered_alias() {
        let net = Network::new_network();
        let member: Arc<dyn NetworkMember> = Arc::new(FakeMember {
            running: true,
            ports: vec![(6379, 32768)],
        });
        net.register(member, vec!["redis".to_string()], Arc::new(FakeBackend));

        assert_eq!(net.resolve("redis", 6379).unwrap(), "redis:6379");
    }

    #[test]
    fn resolve_errors_naming_the_alias_when_unregistered() {
        let net = Network::new_network();
        let err = net.resolve("nope", 1234).unwrap_err();
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn links_for_new_member_never_self_links() {
        let net = Network::new_network();
        // Nothing registered yet: a first member joining sees no links, and computing
        // its links (before it registers) can't possibly include itself.
        let links = net.links_for_new_member();
        assert!(links.is_empty());
    }

    #[test]
    fn links_for_new_member_is_one_per_alias_and_guest_port_of_each_running_sibling() {
        let net = Network::new_network();
        let running = Arc::new(FakeMember {
            running: true,
            ports: vec![(6379, 32768), (16379, 32769)],
        });
        net.register(
            running,
            vec!["redis".to_string(), "cache".to_string()],
            Arc::new(FakeBackend),
        );

        let not_running = Arc::new(FakeMember {
            running: false,
            ports: vec![(80, 40000)],
        });
        net.register(
            not_running,
            vec!["stopped".to_string()],
            Arc::new(FakeBackend),
        );

        let links = net.links_for_new_member();
        // 2 aliases * 2 ports for the running sibling = 4; the not-running sibling
        // contributes nothing.
        assert_eq!(links.len(), 4, "{links:?}");
        assert!(
            links
                .iter()
                .any(|l| l.alias == "redis" && l.guest_port == 6379 && l.target_host_port == 32768)
        );
        assert!(
            links.iter().any(|l| l.alias == "cache"
                && l.guest_port == 16379
                && l.target_host_port == 32769)
        );
        assert!(!links.iter().any(|l| l.alias == "stopped"));
    }

    #[tokio::test]
    async fn close_is_safe_to_call_when_never_used() {
        let net = Network::new_network();
        net.close()
            .await
            .expect("close on an unused network must be a harmless no-op");
    }
}
