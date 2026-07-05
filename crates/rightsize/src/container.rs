//! The `Container` builder and the `ContainerGuard` RAII handle it returns.
//!
//! This module owns the hardest moment in the core crate: the two-tier cleanup story.
//! **Happy path:** `ContainerGuard::stop(self)` is an explicit `async fn` that awaits an
//! ordered teardown (backend `stop` then `remove`, then port release). **Fallback:**
//! `Drop` cannot be `async` and must not assume a Tokio runtime is anywhere in the
//! process, so it does the least synchronous work that's still correct — release ports
//! synchronously (`FreePorts` is plain `std::sync::Mutex`-guarded, no runtime needed)
//! and hand the container off to the dedicated cleanup thread in `crate::cleanup`, which
//! tears it down with blocking std I/O only. See `crate::cleanup`'s module docs for why
//! that thread exists and what it promises.
//!
//! `Container::start()`'s ordering (allocate → create → start, with a port-bind-conflict
//! retry loop; then network-link-install → register → wait, as one guarded unit whose
//! failure triggers a full awaited teardown before the error reaches the caller) is
//! deliberate: any partial failure after resources are allocated must still reach a
//! fully-torn-down state before the error propagates to the caller.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::backend::{SandboxBackend, SandboxHandle};
use crate::backends;
use crate::cleanup::{self, CleanupJob};
use crate::error::{Result, RightsizeError};
use crate::free_ports::FreePorts;
use crate::model::{ContainerSpec, ExecResult, FileMount};
use crate::mountable_file::MountableFile;
use crate::network::{Network, NetworkMember};
use crate::run_id::RunId;
use crate::wait::{Wait, WaitStrategy, WaitTarget};

/// How many times `Container::start()` retries the create+start step with fresh host
/// ports before giving up, when every failure is a port-bind conflict.
const PORT_BIND_ATTEMPTS: usize = 5;

/// A process-wide counter for container name suffixes. Retries advance it too (a
/// discarded attempt's name is never reused), so names aren't dense — only unique per
/// process, which is all a name needs to be.
static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A per-process free-port allocator, shared by every `Container` in this process.
static FREE_PORTS: std::sync::OnceLock<FreePorts> = std::sync::OnceLock::new();

fn free_ports() -> &'static FreePorts {
    FREE_PORTS.get_or_init(FreePorts::new)
}

/// A closure that rewrites a `ContainerSpec` with knowledge of this container's mapped
/// host ports, before `create()` (e.g. Redpanda/Kafka's advertised-listener rewrite).
type SpecCustomizer = dyn Fn(ContainerSpec, &dyn Fn(u16) -> u16) -> ContainerSpec + Send + Sync;

/// A closure run once a guard exists and the wait strategy is satisfied (e.g.
/// Mongo's replica-set init).
type PostStartHook = dyn Fn(&ContainerGuard) -> crate::BoxFuture<'_, Result<()>> + Send + Sync;

/// A single sandboxed container, built with a fluent API and run by whichever
/// [`crate::backend::SandboxBackend`] is active. Configure it with the `with_*`
/// builders, call [`Container::start`] to boot it and get back a [`ContainerGuard`].
pub struct Container {
    image: String,
    env: Vec<(String, String)>,
    exposed_ports: Vec<u16>,
    command: Option<Vec<String>>,
    network: Option<Arc<Network>>,
    aliases: Vec<String>,
    mounts: Vec<FileMount>,
    wait_strategy: Box<dyn WaitStrategy>,
    memory_limit_mb: Option<u64>,
    backend_override: Option<Arc<dyn SandboxBackend>>,
    spec_customizer: Option<Arc<SpecCustomizer>>,
    post_start: Option<Arc<PostStartHook>>,
}

impl Container {
    /// Starts building a container from `image`, with no exposed ports, no env, the
    /// default [`Wait::for_listening_port`] readiness check, and every other option at
    /// its default.
    pub fn new(image: &str) -> Container {
        Container {
            image: image.to_string(),
            env: Vec::new(),
            exposed_ports: Vec::new(),
            command: None,
            network: None,
            aliases: Vec::new(),
            mounts: Vec::new(),
            wait_strategy: Wait::for_listening_port(),
            memory_limit_mb: None,
            backend_override: None,
            spec_customizer: None,
            post_start: None,
        }
    }

    /// Sets a single environment variable for the container process. Call again with
    /// the same key to overwrite it — the *value* from the last call wins, but the
    /// key keeps the position of its *first* `with_env` call in the final spec's
    /// iteration order (see `dedup_env_last_wins`, applied once at `start()` time,
    /// right before the spec reaches a backend — this builder itself still just
    /// appends, so a spec-customizer pushing more env entries is resolved the same way).
    pub fn with_env(mut self, k: &str, v: &str) -> Self {
        self.env.push((k.to_string(), v.to_string()));
        self
    }

    /// Removes every entry for `key` set so far via [`Self::with_env`]. The core-level
    /// primitive a module needs when a later env var must ALSO retract an earlier
    /// one's effect on the running process, not merely be shadowed by last-wins
    /// value resolution (e.g.
    /// `ArangoContainer::with_root_password` clearing `ARANGO_NO_AUTH`, whose
    /// entrypoint checks presence/absence of the variable itself, not just its
    /// value — last-wins on `ARANGO_NO_AUTH`'s own value can't turn it "off").
    /// No-op if `key` was never set.
    pub fn remove_env(mut self, key: &str) -> Self {
        self.env.retain(|(k, _)| k != key);
        self
    }

    /// The env entries set so far, in call order — a plain read-only introspection
    /// point (e.g. for a module's own unit tests asserting what its builder produced,
    /// such as `ArangoContainer::with_root_password`'s NO_AUTH-removed/password-added
    /// contract) rather than a way to mutate the builder. Reflects [`Self::with_env`]
    /// calls made so far exactly, duplicates and all — the last-wins dedup
    /// (`dedup_env_last_wins`) only runs once, at `start()` time.
    pub fn env(&self) -> &[(String, String)] {
        &self.env
    }

    /// Declares guest ports to publish; each gets a host port assigned before boot.
    pub fn with_exposed_ports(mut self, ports: &[u16]) -> Self {
        self.exposed_ports.extend_from_slice(ports);
        self
    }

    /// Overrides the image's default entrypoint/command.
    pub fn with_command(mut self, cmd: &[&str]) -> Self {
        self.command = Some(cmd.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Joins `net`, making this container's exposed ports reachable at its
    /// [`Container::with_network_aliases`].
    pub fn with_network(mut self, net: &Arc<Network>) -> Self {
        self.network = Some(net.clone());
        self
    }

    /// Names this container is reachable as on its network (see [`Network::resolve`]).
    pub fn with_network_aliases(mut self, names: &[&str]) -> Self {
        self.aliases.extend(names.iter().map(|s| s.to_string()));
        self
    }

    /// Mounts `file` read-only into the guest at `guest_path`; takes effect at the next
    /// `start()`.
    pub fn with_copy_file_to_container(mut self, file: MountableFile, guest_path: &str) -> Self {
        self.mounts.push(FileMount::new(file.path(), guest_path));
        self
    }

    /// Overrides the readiness check run after boot; defaults to
    /// [`Wait::for_listening_port`]. Takes the strategy by value — a bare
    /// `Wait::for_http("/health").for_port(80)` or a caller's own `impl WaitStrategy`
    /// works directly, no `Box::new(..)` wart at the call site; this boxes internally
    /// (a `Box<dyn WaitStrategy>` from a built-in factory that already returns one
    /// satisfies the bound too, via the blanket impl in `crate::wait`, so it is never
    /// double-boxed).
    pub fn waiting_for(mut self, strategy: impl WaitStrategy + 'static) -> Self {
        self.wait_strategy = Box::new(strategy);
        self
    }

    /// Caps the container's guest memory at `megabytes`. Leaving this unset lets each
    /// backend apply its own default.
    pub fn with_memory_limit(mut self, megabytes: u64) -> Self {
        self.memory_limit_mb = Some(megabytes);
        self
    }

    /// Test/module seam: overrides the backend this container starts on, instead of the
    /// process-wide active backend. Every current caller is a unit test in this crate —
    /// `#[cfg_attr(not(test), allow(dead_code))]` reflects that precisely instead of
    /// blanket-silencing dead-code analysis on this method.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn with_backend(mut self, b: Arc<dyn SandboxBackend>) -> Self {
        self.backend_override = Some(b);
        self
    }

    /// Module hook: rewrites the `ContainerSpec` with knowledge of this container's own
    /// mapped host ports, right before `create()` — e.g. Redpanda/Kafka's
    /// advertised-listener rewrite, which needs to know its own mapped port to advertise
    /// it.
    pub fn with_spec_customizer(
        mut self,
        f: impl Fn(ContainerSpec, &dyn Fn(u16) -> u16) -> ContainerSpec + Send + Sync + 'static,
    ) -> Self {
        self.spec_customizer = Some(Arc::new(f));
        self
    }

    /// Module hook: runs once the guard exists and the wait strategy is satisfied — e.g.
    /// Mongo's replica-set initialization.
    pub fn with_post_start(
        mut self,
        f: impl for<'a> Fn(&'a ContainerGuard) -> crate::BoxFuture<'a, Result<()>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.post_start = Some(Arc::new(f));
        self
    }

    fn active_backend(&self) -> Arc<dyn SandboxBackend> {
        match &self.backend_override {
            Some(b) => b.clone(),
            None => backends::active(),
        }
    }

    fn describe(id: &str, image: &str) -> String {
        format!("container(image={image}, id={id})")
    }

    /// Boots the container and returns an RAII guard. On ANY failure partway (create,
    /// start, install_network_links, register, OR wait), teardown runs and nothing
    /// leaks — `start()` does not return its error to the caller until that teardown
    /// has finished.
    pub async fn start(self) -> Result<ContainerGuard> {
        let backend = self.active_backend();

        if let Some(net) = &self.network {
            backend.ensure_network(net.id()).await?;
        }

        let (handle, mapped_ports) = create_started_container(
            &backend,
            &self.image,
            &self.env,
            &self.command,
            &self.exposed_ports,
            &self.mounts,
            self.network.as_deref(),
            &self.aliases,
            self.memory_limit_mb,
            self.spec_customizer.as_deref(),
        )
        .await?;

        let name = handle.id().to_string();
        let guard = ContainerGuard {
            handle: Some(handle),
            backend: backend.clone(),
            mapped_ports: Mutex::new(mapped_ports),
            network: self.network.clone(),
            image: self.image.clone(),
            exposed_ports: self.exposed_ports.clone(),
            name,
        };

        // Guarded block: install_network_links -> register -> wait. On ANY error here,
        // await the guard's own stop to completion, then propagate the original error —
        // a half-started container never leaks regardless of where in this block it
        // failed, and start() does not return until teardown has finished.
        if let Err(e) = link_register_and_wait(
            &guard,
            &backend,
            self.network.as_deref(),
            &self.aliases,
            self.wait_strategy.as_ref(),
        )
        .await
        {
            let _ = guard.stop().await;
            return Err(e);
        }

        if let Some(post_start) = &self.post_start {
            if let Err(e) = post_start(&guard).await {
                let _ = guard.stop().await;
                return Err(e);
            }
        }

        Ok(guard)
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_started_container(
    backend: &Arc<dyn SandboxBackend>,
    image: &str,
    env: &[(String, String)],
    command: &Option<Vec<String>>,
    exposed_ports: &[u16],
    mounts: &[FileMount],
    network: Option<&Network>,
    aliases: &[String],
    memory_limit_mb: Option<u64>,
    spec_customizer: Option<&SpecCustomizer>,
) -> Result<(Box<dyn SandboxHandle>, Vec<(u16, u16)>)> {
    let mut last_conflict: Option<RightsizeError> = None;

    for _ in 0..PORT_BIND_ATTEMPTS {
        let mapped_ports = allocate_ports(exposed_ports)?;
        let seq = NAME_COUNTER.fetch_add(1, Ordering::SeqCst);
        let name = format!("rz-{}-{seq}", RunId::value());

        let mut spec = ContainerSpec {
            name: name.clone(),
            image: image.to_string(),
            env: env.to_vec(),
            command: command.clone(),
            ports: mapped_ports
                .iter()
                .map(|&(guest_port, host_port)| crate::model::PortBinding {
                    host_port,
                    guest_port,
                })
                .collect(),
            mounts: mounts.to_vec(),
            network_id: network.map(|n| n.id().to_string()),
            aliases: aliases.to_vec(),
            run_id: RunId::value().to_string(),
            memory_limit_mb,
        };

        if let Some(customizer) = spec_customizer {
            let lookup: std::collections::HashMap<u16, u16> =
                mapped_ports.iter().copied().collect();
            let mapped_fn = move |guest: u16| -> u16 {
                *lookup
                    .get(&guest)
                    .expect("customizer looked up an unexposed port")
            };
            spec = customizer(spec, &mapped_fn);
        }

        // Last-wins, insertion-order-of-first-occurrence dedup — see
        // `dedup_env_last_wins`'s doc for why this runs here (after the
        // spec-customizer, which may itself push more env entries) rather than
        // earlier.
        spec.env = dedup_env_last_wins(spec.env);

        let handle = backend.create(spec).await?;
        match backend.start(handle.as_ref()).await {
            Ok(()) => return Ok((handle, mapped_ports)),
            Err(e) => {
                let _ = backend.stop(handle.as_ref()).await;
                let _ = backend.remove(handle.as_ref()).await;
                release_ports(&mapped_ports);
                if is_port_bind_conflict(&e) {
                    last_conflict = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(RightsizeError::Backend(format!(
        "Could not bind free host ports for {} after {PORT_BIND_ATTEMPTS} attempts — another \
         process kept grabbing the allocated ports first; if this persists, check for a port \
         scanner/leaked process racing the allocator on this host{}",
        Container::describe("<unstarted>", image),
        last_conflict
            .map(|c| format!(" (last conflict: {c})"))
            .unwrap_or_default(),
    )))
}

/// Collapses `env` to last-wins-per-key, keeping each key at the position of its
/// **first** occurrence — an overwrite updates the value in place without moving
/// the key to the end of iteration order, the way an insertion-ordered map's `put`
/// behaves.
///
/// `Container::with_env` (and a spec-customizer pushing straight onto
/// `ContainerSpec::env`) is append-only — a `Vec<(String, String)>`, not a map — so
/// calling it twice with the same key previously left *both* entries in the spec,
/// and which one "won" was whatever the backend's own env-merge order happened to
/// do with a duplicate key (Docker and msb are not guaranteed to agree — see the
/// fix commit this function landed in). This function is the single seam that
/// restores map-like last-wins semantics without changing `with_env`'s append-only
/// builder shape or `ContainerSpec::env`'s `Vec` type (both stay exactly as
/// documented elsewhere — order and duplicate-key handling stay under the caller's
/// control up to this point; this is where they get resolved, once, right before
/// the spec reaches a backend).
fn dedup_env_last_wins(env: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut order: Vec<String> = Vec::new();
    let mut values: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for (k, v) in env {
        if !values.contains_key(&k) {
            order.push(k.clone());
        }
        values.insert(k, v);
    }
    order
        .into_iter()
        .map(|k| {
            let v = values.remove(&k).expect("key was just recorded in order");
            (k, v)
        })
        .collect()
}

fn allocate_ports(exposed_ports: &[u16]) -> Result<Vec<(u16, u16)>> {
    let mut mapped = Vec::with_capacity(exposed_ports.len());
    for &guest_port in exposed_ports {
        match free_ports().allocate() {
            Ok(host_port) => mapped.push((guest_port, host_port)),
            Err(e) => {
                // Roll back this attempt's partial allocation before propagating.
                release_ports(&mapped);
                return Err(e);
            }
        }
    }
    Ok(mapped)
}

fn release_ports(mapped_ports: &[(u16, u16)]) {
    for &(_, host_port) in mapped_ports {
        free_ports().release(host_port);
    }
}

/// True if `e` represents a host-port bind conflict worth retrying with fresh ports.
/// Prefers the typed [`RightsizeError::PortBindConflict`], walking the `source` chain;
/// falls back to matching known message phrasings (case-insensitive) for backends that
/// don't throw the typed variant. Negatives — any other error — must NOT retry.
pub(crate) fn is_port_bind_conflict(e: &RightsizeError) -> bool {
    let mut current: Option<&RightsizeError> = Some(e);
    while let Some(err) = current {
        if matches!(err, RightsizeError::PortBindConflict { .. }) {
            return true;
        }
        let msg = err.to_string().to_lowercase();
        if msg.contains("address already in use") || msg.contains("already allocated") {
            return true;
        }
        current = match err {
            RightsizeError::PortBindConflict {
                source: Some(s), ..
            } => Some(s.as_ref()),
            _ => None,
        };
    }
    false
}

async fn link_register_and_wait(
    guard: &ContainerGuard,
    backend: &Arc<dyn SandboxBackend>,
    network: Option<&Network>,
    aliases: &[String],
    wait_strategy: &dyn WaitStrategy,
) -> Result<()> {
    if let Some(net) = network {
        let links = net.links_for_new_member();
        backend
            .install_network_links(guard.handle_ref(), &links)
            .await?;
    }
    if let Some(net) = network {
        net.register(guard.as_network_member(), aliases.to_vec(), backend.clone());
    }
    let target = GuardWaitTarget { guard };
    wait_strategy.wait_until_ready(&target).await
}

/// Adapts a [`ContainerGuard`] to the [`WaitTarget`] a [`WaitStrategy`] needs.
struct GuardWaitTarget<'a> {
    guard: &'a ContainerGuard,
}

#[async_trait::async_trait]
impl WaitTarget for GuardWaitTarget<'_> {
    fn host(&self) -> &str {
        self.guard.host()
    }
    fn mapped_port(&self, guest_port: u16) -> u16 {
        self.guard.get_mapped_port(guest_port).unwrap_or(guest_port)
    }
    fn exposed_guest_ports(&self) -> Vec<u16> {
        self.guard.exposed_ports.clone()
    }
    async fn current_logs(&self) -> String {
        self.guard.logs().await.unwrap_or_default()
    }
    fn describe(&self) -> String {
        self.guard.describe()
    }
}

/// The RAII guard for a running container. Dropping it without calling
/// [`ContainerGuard::stop`] still tears the container down — see the module docs for
/// the two-tier cleanup story.
pub struct ContainerGuard {
    handle: Option<Box<dyn SandboxHandle>>,
    backend: Arc<dyn SandboxBackend>,
    mapped_ports: Mutex<Vec<(u16, u16)>>,
    network: Option<Arc<Network>>,
    image: String,
    exposed_ports: Vec<u16>,
    name: String,
}

impl ContainerGuard {
    fn handle_ref(&self) -> &dyn SandboxHandle {
        self.handle
            .as_deref()
            .expect("ContainerGuard invariant: handle is only None after being consumed by stop()")
    }

    fn as_network_member(&self) -> Arc<dyn NetworkMember> {
        Arc::new(GuardMemberSnapshot {
            mapped_ports: self
                .mapped_ports
                .lock()
                .expect("mapped_ports mutex poisoned")
                .clone(),
        })
    }

    /// The network this container joined, if any — a module may use this to
    /// resolve a sibling's alias from the guard itself rather than needing to keep a
    /// separate `Network` reference around.
    pub fn network(&self) -> Option<&Arc<Network>> {
        self.network.as_ref()
    }

    /// The host address published ports are reachable on — always loopback.
    pub fn host(&self) -> &str {
        "127.0.0.1"
    }

    /// The host port `guest_port` is published on.
    ///
    /// Distinguishes two failure causes: if this guard isn't running (stopped, or never
    /// successfully started), the error says so; if it IS running but `guest_port` was
    /// never declared via `with_exposed_ports`, the error says the port isn't exposed.
    pub fn get_mapped_port(&self, guest_port: u16) -> Result<u16> {
        let mapped = self
            .mapped_ports
            .lock()
            .expect("mapped_ports mutex poisoned");
        if let Some(&(_, host_port)) = mapped.iter().find(|&&(g, _)| g == guest_port) {
            return Ok(host_port);
        }
        if !self.is_running() {
            Err(RightsizeError::Backend(format!(
                "Cannot get mapped port {guest_port} on {}: the container is not running — call \
                 start() first, or check that it did not stop/fail after start()",
                self.describe()
            )))
        } else {
            Err(RightsizeError::Backend(format!(
                "Port {guest_port} is not exposed on {} — call with_exposed_ports({guest_port}) \
                 before start(), or check exposed_ports for the port you actually declared",
                self.describe()
            )))
        }
    }

    /// The full logs captured so far. Requires the container to be running.
    pub async fn logs(&self) -> Result<String> {
        self.backend.logs(self.require_handle()?).await
    }

    /// Runs `cmd` inside the running container and returns its exit code and captured
    /// output.
    pub async fn exec(&self, cmd: &[&str]) -> Result<ExecResult> {
        let cmd: Vec<String> = cmd.iter().map(|s| s.to_string()).collect();
        self.backend.exec(self.require_handle()?, &cmd).await
    }

    /// Streams log lines to `consumer` as they're produced. Closing (or dropping) the
    /// returned [`crate::backend::FollowHandle`] halts delivery — no further lines
    /// reach `consumer` afterward, even if the container keeps running.
    pub async fn follow_output(
        &self,
        consumer: impl Fn(String) + Send + Sync + 'static,
    ) -> Result<crate::backend::FollowHandle> {
        self.backend
            .follow_logs(self.require_handle()?, Box::new(consumer))
            .await
    }

    /// True from a successful `start()` until `stop()`; false before the first `start()`
    /// and after.
    pub fn is_running(&self) -> bool {
        self.handle.is_some()
    }

    fn require_handle(&self) -> Result<&dyn SandboxHandle> {
        self.handle.as_deref().ok_or_else(|| {
            RightsizeError::Backend(format!(
                "{} is not running — call start() first",
                self.describe()
            ))
        })
    }

    fn describe(&self) -> String {
        Container::describe(&self.name, &self.image)
    }

    /// Happy-path explicit teardown: stops and removes the container on the backend
    /// (both best-effort — errors are swallowed, since a failed cleanup step
    /// shouldn't block release of the ones after it), then releases its mapped host
    /// ports. Idempotent — a no-op if already stopped
    /// (never re-calls the backend, never double-releases ports), and consumes the
    /// guard so it cannot be used again after this returns.
    pub async fn stop(mut self) -> Result<()> {
        self.stop_inner().await;
        Ok(())
    }

    /// The actual teardown logic, factored out so both `stop(self)` and `Drop` (via the
    /// synchronous fallback) converge on the same "idempotent, ports-then-clear"
    /// contract — `Drop` cannot call this directly (it's async), but it follows the
    /// same shape with blocking primitives instead.
    async fn stop_inner(&mut self) {
        let Some(handle) = self.handle.take() else {
            return; // already stopped (or never started): no-op, no backend call.
        };
        let _ = self.backend.stop(handle.as_ref()).await;
        let _ = self.backend.remove(handle.as_ref()).await;
        let mut mapped = self
            .mapped_ports
            .lock()
            .expect("mapped_ports mutex poisoned");
        for &(_, host_port) in mapped.iter() {
            free_ports().release(host_port);
        }
        mapped.clear();
    }
}

struct GuardMemberSnapshot {
    mapped_ports: Vec<(u16, u16)>,
}
impl NetworkMember for GuardMemberSnapshot {
    fn is_running(&self) -> bool {
        true // only constructed for a guard that just successfully started.
    }
    fn mapped_ports(&self) -> Vec<(u16, u16)> {
        self.mapped_ports.clone()
    }
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        // Best-effort SYNCHRONOUS fallback (decision 1). MUST NOT panic; MUST work with
        // no Tokio runtime in context.
        let Some(handle) = self.handle.take() else {
            return; // already stopped via stop(self): nothing to do.
        };
        // Release ports synchronously here — FreePorts is a plain std Mutex, no runtime
        // needed — so a dropped-not-stopped guard doesn't leak its ports even if the
        // cleanup thread is slow or (in a crash) never runs at all.
        if let Ok(mut mapped) = self.mapped_ports.lock() {
            for &(_, host_port) in mapped.iter() {
                free_ports().release(host_port);
            }
            mapped.clear();
        }
        cleanup::enqueue(CleanupJob {
            backend: self.backend.clone(),
            container_id: handle.id().to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait::{WaitStrategy, WaitTarget};
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    /// `ContainerGuard` deliberately isn't `Debug` (it holds a `Box<dyn SandboxHandle>`
    /// and friends), so `Result::expect_err`/`unwrap_err` don't work directly on
    /// `Result<ContainerGuard, _>` — this pulls the error out by hand.
    fn expect_start_err(result: Result<ContainerGuard>, msg: &str) -> RightsizeError {
        match result {
            Ok(_) => panic!("{msg}: expected an error, got Ok"),
            Err(e) => e,
        }
    }

    struct FakeHandle {
        id: String,
        spec: ContainerSpec,
    }
    impl SandboxHandle for FakeHandle {
        fn id(&self) -> &str {
            &self.id
        }
        fn spec(&self) -> &ContainerSpec {
            &self.spec
        }
    }

    /// A wait strategy that's immediately ready — the fake backend runs nothing to
    /// actually connect to.
    struct ReadyImmediately;
    #[async_trait::async_trait]
    impl WaitStrategy for ReadyImmediately {
        async fn wait_until_ready(&self, _target: &dyn WaitTarget) -> Result<()> {
            Ok(())
        }
        fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
            self
        }
    }

    /// A wait strategy that always fails — forces the start()-then-teardown cleanup
    /// path. Captures the mapped port it saw (via `probe`) before failing, so a test
    /// can assert on the *exact* port that was released, not just "some" port.
    struct NeverReady {
        precaptured_port: Arc<StdMutex<Option<u16>>>,
    }
    #[async_trait::async_trait]
    impl WaitStrategy for NeverReady {
        async fn wait_until_ready(&self, target: &dyn WaitTarget) -> Result<()> {
            *self.precaptured_port.lock().unwrap() = Some(target.mapped_port(6379));
            Err(RightsizeError::ContainerLaunch("never ready".to_string()))
        }
        fn with_startup_timeout(self: Box<Self>, _timeout: Duration) -> Box<dyn WaitStrategy> {
            self
        }
    }

    #[derive(Default)]
    struct FakeBackendState {
        created: Vec<ContainerSpec>,
        started: Vec<String>,
        stopped: Vec<String>,
        installed_links: Vec<(String, Vec<crate::backend::NetworkLink>)>,
    }

    struct FakeBackend {
        state: StdMutex<FakeBackendState>,
        fail_install_network_links: bool,
    }
    impl FakeBackend {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: false,
            })
        }
        fn failing_install_network_links() -> Arc<Self> {
            Arc::new(Self {
                state: StdMutex::new(FakeBackendState::default()),
                fail_install_network_links: true,
            })
        }
    }
    #[async_trait::async_trait]
    impl SandboxBackend for FakeBackend {
        fn name(&self) -> &str {
            "fake"
        }
        fn supports_native_networks(&self) -> bool {
            false
        }
        async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
            self.state.lock().unwrap().created.push(spec.clone());
            Ok(Box::new(FakeHandle {
                id: spec.name.clone(),
                spec,
            }))
        }
        async fn start(&self, handle: &dyn SandboxHandle) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .started
                .push(handle.id().to_string());
            Ok(())
        }
        async fn stop(&self, handle: &dyn SandboxHandle) -> Result<()> {
            self.state
                .lock()
                .unwrap()
                .stopped
                .push(handle.id().to_string());
            Ok(())
        }
        async fn remove(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn exec(&self, _handle: &dyn SandboxHandle, cmd: &[String]) -> Result<ExecResult> {
            Ok(ExecResult {
                exit_code: 0,
                stdout: cmd.join(" "),
                stderr: String::new(),
            })
        }
        async fn logs(&self, _handle: &dyn SandboxHandle) -> Result<String> {
            Ok(String::new())
        }
        async fn follow_logs(
            &self,
            _handle: &dyn SandboxHandle,
            _consumer: Box<dyn Fn(String) + Send + Sync>,
        ) -> Result<crate::backend::FollowHandle> {
            unimplemented!("not exercised by this test suite")
        }
        async fn ensure_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        async fn remove_network(&self, _network_id: &str) -> Result<()> {
            Ok(())
        }
        async fn install_network_links(
            &self,
            handle: &dyn SandboxHandle,
            links: &[crate::backend::NetworkLink],
        ) -> Result<()> {
            if self.fail_install_network_links {
                return Err(RightsizeError::unsupported_with_remedy(
                    "network links (no nc in image; try docker)",
                    "fake",
                    "run with a different backend",
                ));
            }
            if !links.is_empty() {
                self.state
                    .lock()
                    .unwrap()
                    .installed_links
                    .push((handle.id().to_string(), links.to_vec()));
            }
            Ok(())
        }
        fn cleanup_sync(&self, _container_id: &str) {}
    }

    fn container_on(backend: &Arc<FakeBackend>) -> Container {
        Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(ReadyImmediately)
    }

    // U1: port allocate/create/wait/map.
    #[tokio::test]
    async fn u1_start_allocates_ports_creates_spec_waits_and_maps_ports() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_env("A", "1");
        let guard = c.start().await.expect("start must succeed");

        let spec = {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.created.len(), 1);
            state.created[0].clone()
        };
        assert_eq!(spec.image, "redis:8.6-alpine");
        assert_eq!(spec.env, vec![("A".to_string(), "1".to_string())]);
        assert_eq!(spec.ports.len(), 1);
        assert_eq!(spec.ports[0].guest_port, 6379);
        assert!(spec.ports[0].host_port > 0);
        assert_eq!(
            guard.get_mapped_port(6379).unwrap(),
            spec.ports[0].host_port
        );
        assert!(guard.is_running());

        guard.stop().await.unwrap();
    }

    // Not-running vs not-exposed disambiguation.
    #[tokio::test]
    async fn get_mapped_port_reports_not_running_after_stop_clears_the_mappings() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        assert!(guard.get_mapped_port(6379).unwrap() > 0);
        guard.stop().await.unwrap();

        // The guard was consumed by stop(); re-derive a fresh one to exercise the
        // stopped-state error message via a boot that we then let the wait strategy
        // fail after capturing the port — simpler: build a second guard, call stop via
        // an explicit helper path. Since `stop` consumes `self`, we instead assert the
        // not-running message shape directly against a guard we stop through the
        // internal helper without consuming it, mirroring what Drop/stop share.
        let backend2 = FakeBackend::new();
        let c2 = container_on(&backend2).with_exposed_ports(&[6379]);
        let mut guard2 = c2.start().await.unwrap();
        guard2.stop_inner().await;
        let err = guard2.get_mapped_port(6379).unwrap_err().to_string();
        assert!(err.contains("not running"), "{err}");
        assert!(!err.contains("not exposed"), "{err}");
    }

    // U8 (part 2): not-exposed for a port never declared.
    #[tokio::test]
    async fn get_mapped_port_reports_not_exposed_for_an_undeclared_port() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        let err = guard.get_mapped_port(9999).unwrap_err().to_string();
        assert!(err.contains("not exposed"), "{err}");
        guard.stop().await.unwrap();
    }

    // U2: network links installed on the new member for the running sibling, before
    // wait, after registering siblings only (never self).
    #[tokio::test]
    async fn u2_starting_on_a_network_installs_links_to_running_siblings() {
        let backend = FakeBackend::new();
        let net = Arc::new(Network::new_network());
        let stub = container_on(&backend)
            .with_exposed_ports(&[8888])
            .with_network(&net)
            .with_network_aliases(&["configuration-stub"]);
        let stub_guard = stub.start().await.unwrap();

        let app = container_on(&backend)
            .with_exposed_ports(&[8080])
            .with_network(&net);
        let app_guard = app.start().await.unwrap();

        let (consumer_id, links) = {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.installed_links.len(), 1);
            state.installed_links[0].clone()
        };
        assert_eq!(
            consumer_id,
            backend.state.lock().unwrap().created.last().unwrap().name
        );
        assert_eq!(
            links,
            vec![crate::backend::NetworkLink {
                alias: "configuration-stub".to_string(),
                guest_port: 8888,
                target_host_port: stub_guard.get_mapped_port(8888).unwrap(),
            }]
        );
        assert_eq!(
            net.resolve("configuration-stub", 8888).unwrap(),
            "configuration-stub:8888"
        );
        assert!(net.resolve("nope", 1).is_err());

        app_guard.stop().await.unwrap();
        stub_guard.stop().await.unwrap();
    }

    // U8 (part 3): a single container on a network installs no links but is still
    // registered, so a later joiner links back to it.
    #[tokio::test]
    async fn single_container_on_network_installs_no_links_but_is_registered() {
        let backend = FakeBackend::new();
        let net = Arc::new(Network::new_network());
        let solo = container_on(&backend)
            .with_exposed_ports(&[9999])
            .with_network(&net)
            .with_network_aliases(&["solo"]);
        let solo_guard = solo.start().await.unwrap();
        assert!(
            backend.state.lock().unwrap().installed_links.is_empty(),
            "a lone container must not link to itself"
        );

        let joiner = container_on(&backend)
            .with_exposed_ports(&[8080])
            .with_network(&net);
        let joiner_guard = joiner.start().await.unwrap();
        let (_, links) = {
            let state = backend.state.lock().unwrap();
            assert_eq!(state.installed_links.len(), 1);
            state.installed_links[0].clone()
        };
        assert_eq!(
            links,
            vec![crate::backend::NetworkLink {
                alias: "solo".to_string(),
                guest_port: 9999,
                target_host_port: solo_guard.get_mapped_port(9999).unwrap(),
            }]
        );

        joiner_guard.stop().await.unwrap();
        solo_guard.stop().await.unwrap();
    }

    // U5 (injection point 1/2): wait-strategy failure stops the container and releases
    // its host ports — proven against FreePorts' own issued_view(), not just "the port
    // is bindable" (which would pass even if release were never called, since the fake
    // backend never binds OS ports either).
    #[tokio::test]
    async fn u5_wait_strategy_failure_stops_the_container_and_releases_ports() {
        let backend = FakeBackend::new();
        let precaptured_port = Arc::new(StdMutex::new(None));
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(NeverReady {
                precaptured_port: precaptured_port.clone(),
            })
            .with_exposed_ports(&[6379]);

        let err = expect_start_err(c.start().await, "wait strategy must fail start()");
        assert!(err.to_string().contains("never ready"), "{err}");

        let name = backend.state.lock().unwrap().created[0].name.clone();
        assert!(
            backend.state.lock().unwrap().stopped.contains(&name),
            "started container must be stopped when the wait strategy fails"
        );

        let port = precaptured_port
            .lock()
            .unwrap()
            .expect("wait strategy must have observed a real mapped port");
        assert!(port > 0);
        assert!(
            !free_ports().issued_view().contains(&port),
            "port {port} must be released by the wait-strategy-failure cleanup path"
        );
    }

    // U5 (injection point 2/2): install_network_links failure stops the container too,
    // and — like the wait-strategy injection point above — releases its host ports back
    // to FreePorts. Proven against issued_view() directly, not just "stop was called",
    // so this fails the same way U5's first injection point would if `start()` ever
    // stopped releasing ports on this seam specifically.
    #[tokio::test]
    async fn u5_install_network_links_failure_stops_the_container() {
        let backend = FakeBackend::failing_install_network_links();
        let net = Arc::new(Network::new_network());
        let c = container_on(&backend)
            .with_exposed_ports(&[8080])
            .with_network(&net);

        let err = expect_start_err(
            c.start().await,
            "install_network_links failure must propagate",
        );
        assert!(err.to_string().contains("nc"), "{err}");

        let created = backend.state.lock().unwrap().created[0].clone();
        assert!(
            backend
                .state
                .lock()
                .unwrap()
                .stopped
                .contains(&created.name),
            "started container must be stopped on link-install failure"
        );

        let port = created
            .ports
            .first()
            .expect("spec must carry the allocated host port")
            .host_port;
        assert!(
            !free_ports().issued_view().contains(&port),
            "port {port} must be released when install_network_links fails, \
             not just when the wait strategy fails"
        );
    }

    // U7: stop is a no-op before start, and Drop after an explicit stop() doesn't
    // double-release ports or double-call the backend (stop() itself can't be called
    // twice in Rust: it consumes the guard by value, so "calling stop() twice" is a
    // compile-time guarantee here instead of a runtime assertion — what remains to
    // prove is that Drop, which still runs after stop() returns, doesn't redo any of
    // stop()'s work).
    #[tokio::test]
    async fn u7_stop_is_idempotent_across_the_stop_then_drop_sequence() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        let name = backend.state.lock().unwrap().created[0].name.clone();
        let mapped_port = guard.get_mapped_port(6379).unwrap();

        guard.stop().await.unwrap(); // guard is consumed here; Drop still runs after.

        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .stopped
                .iter()
                .filter(|n| **n == name)
                .count(),
            1,
            "backend.stop must be called exactly once"
        );
        assert!(
            !free_ports().issued_view().contains(&mapped_port),
            "port must be released by stop()"
        );
    }

    #[tokio::test]
    async fn stop_before_start_is_a_no_op() {
        // There is no "unstarted guard" in this API shape (a guard only exists after a
        // successful start()) — the closest analogue is starting then immediately
        // stopping via the internal helper twice, proving the second call is a no-op.
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let mut guard = c.start().await.unwrap();
        let name = backend.state.lock().unwrap().created[0].name.clone();

        guard.stop_inner().await;
        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .stopped
                .iter()
                .filter(|n| **n == name)
                .count(),
            1
        );
        guard.stop_inner().await; // second call: must not re-call backend.stop or double-release.
        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .stopped
                .iter()
                .filter(|n| **n == name)
                .count(),
            1,
            "a second stop must not re-call backend.stop"
        );
        assert!(!guard.is_running());
    }

    // U6: port-bind-conflict retry — a fake backend that fails start with a typed
    // PortBindConflict on attempts 1-2 then succeeds; ports are reallocated per attempt.
    struct PortConflictBackend {
        fail_first: usize,
        conflict: Box<dyn Fn(u16) -> RightsizeError + Send + Sync>,
        created: StdMutex<Vec<ContainerSpec>>,
        started_ports: StdMutex<Vec<u16>>,
        start_attempts: std::sync::atomic::AtomicUsize,
    }
    impl PortConflictBackend {
        fn new(fail_first: usize) -> Arc<Self> {
            Self::with_conflict(fail_first, |port| {
                RightsizeError::Backend(format!(
                    "driver failed programming external connectivity: failed to bind host port 127.0.0.1:{port}/tcp: address already in use"
                ))
            })
        }
        fn with_conflict(
            fail_first: usize,
            conflict: impl Fn(u16) -> RightsizeError + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                fail_first,
                conflict: Box::new(conflict),
                created: StdMutex::new(Vec::new()),
                started_ports: StdMutex::new(Vec::new()),
                start_attempts: std::sync::atomic::AtomicUsize::new(0),
            })
        }
        fn create_count(&self) -> usize {
            self.created.lock().unwrap().len()
        }
    }
    #[async_trait::async_trait]
    impl SandboxBackend for PortConflictBackend {
        fn name(&self) -> &str {
            "port-conflict"
        }
        fn supports_native_networks(&self) -> bool {
            false
        }
        async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
            self.created.lock().unwrap().push(spec.clone());
            Ok(Box::new(FakeHandle {
                id: spec.name.clone(),
                spec,
            }))
        }
        async fn start(&self, handle: &dyn SandboxHandle) -> Result<()> {
            let attempt = self.start_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let port = handle.spec().ports[0].host_port;
            self.started_ports.lock().unwrap().push(port);
            if attempt <= self.fail_first {
                return Err((self.conflict)(port));
            }
            Ok(())
        }
        async fn stop(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn remove(&self, _handle: &dyn SandboxHandle) -> Result<()> {
            Ok(())
        }
        async fn exec(&self, _handle: &dyn SandboxHandle, _cmd: &[String]) -> Result<ExecResult> {
            Ok(ExecResult {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
        async fn logs(&self, _handle: &dyn SandboxHandle) -> Result<String> {
            Ok(String::new())
        }
        async fn follow_logs(
            &self,
            _handle: &dyn SandboxHandle,
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
    }

    #[tokio::test]
    async fn u6_start_retries_with_fresh_host_ports_on_a_bind_conflict() {
        let backend = PortConflictBackend::new(2);
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let guard = c.start().await.expect("must eventually succeed");
        assert!(guard.is_running());
        assert_eq!(
            backend.create_count(),
            3,
            "each attempt recreates the container"
        );
        let started_ports = backend.started_ports.lock().unwrap().clone();
        assert_eq!(started_ports.len(), 3, "start attempted three times");
        let distinct: std::collections::HashSet<u16> = started_ports.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            started_ports.len(),
            "ports are reallocated per attempt, not reused after a conflict"
        );
        guard.stop().await.unwrap();
    }

    #[tokio::test]
    async fn u6_start_retries_on_the_typed_port_bind_conflict_bare_or_nested() {
        let bare = PortConflictBackend::with_conflict(1, |port| RightsizeError::PortBindConflict {
            message: format!("could not bind host port {port}"),
            source: None,
        });
        let c = Container::new("redis:8.6-alpine")
            .with_backend(bare.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let guard = c
            .start()
            .await
            .expect("must retry exactly once for the typed exception");
        assert!(guard.is_running());
        assert_eq!(bare.create_count(), 2);
        guard.stop().await.unwrap();

        // Wrapped: a typed PortBindConflict nested two levels deep under other
        // PortBindConflicts whose own messages say nothing about ports — only walking
        // the `source` chain (not the string fallback) finds it.
        let nested =
            PortConflictBackend::with_conflict(1, |port| RightsizeError::PortBindConflict {
                message: "start failed".to_string(),
                source: Some(Box::new(RightsizeError::PortBindConflict {
                    message: "io error".to_string(),
                    source: Some(Box::new(RightsizeError::PortBindConflict {
                        message: format!("could not bind host port {port}"),
                        source: None,
                    })),
                })),
            });
        let c = Container::new("redis:8.6-alpine")
            .with_backend(nested.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let guard = c
            .start()
            .await
            .expect("must unwrap to find a nested typed exception");
        assert!(guard.is_running());
        assert_eq!(nested.create_count(), 2);
        guard.stop().await.unwrap();
    }

    #[tokio::test]
    async fn u6_truth_table_known_phrasings_retry_negative_does_not() {
        let phrasings = [
            "address already in use",
            "port is already allocated",
            "bind: address already in use",
            "Bind for 0.0.0.0:32770 failed: PORT IS ALREADY ALLOCATED",
        ];
        for phrasing in phrasings {
            let backend = PortConflictBackend::with_conflict(1, move |_port| {
                RightsizeError::Backend(phrasing.to_string())
            });
            let c = Container::new("redis:8.6-alpine")
                .with_backend(backend.clone())
                .waiting_for(ReadyImmediately)
                .with_exposed_ports(&[6379]);
            let guard = c.start().await.unwrap_or_else(|e| {
                panic!("must retry and succeed for phrasing '{phrasing}': {e}")
            });
            assert!(guard.is_running());
            assert_eq!(
                backend.create_count(),
                2,
                "must retry exactly once for phrasing: {phrasing}"
            );
            guard.stop().await.unwrap();
        }

        // Negative: an unrelated failure must NOT be treated as a conflict.
        let boom = PortConflictBackend::with_conflict(99, |_port| {
            RightsizeError::Backend("boom".to_string())
        });
        let c = Container::new("redis:8.6-alpine")
            .with_backend(boom.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let err = expect_start_err(
            c.start().await,
            "a non-conflict exception must fail fast, no retry",
        );
        assert_eq!(err.to_string(), "boom");
        assert_eq!(
            boom.create_count(),
            1,
            "a non-conflict exception must fail fast, no retry"
        );
    }

    #[tokio::test]
    async fn u4_and_u6_start_gives_up_after_the_retry_budget_is_exhausted_and_releases_every_attempts_ports()
     {
        let backend = PortConflictBackend::new(99);
        let c = Container::new("redis:8.6-alpine")
            .with_backend(backend.clone())
            .waiting_for(ReadyImmediately)
            .with_exposed_ports(&[6379]);
        let err = expect_start_err(c.start().await, "must give up after the retry budget");
        assert!(err.to_string().contains("free host ports"), "{err}");

        let started_ports = backend.started_ports.lock().unwrap().clone();
        assert_eq!(
            started_ports.len(),
            PORT_BIND_ATTEMPTS,
            "all attempts must have been tried"
        );
        let distinct: std::collections::HashSet<u16> = started_ports.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            PORT_BIND_ATTEMPTS,
            "each retry must allocate a fresh port"
        );

        let issued = free_ports().issued_view();
        for port in started_ports {
            assert!(
                !issued.contains(&port),
                "port {port} from a discarded attempt must be released — mutation-verified: this is the U4 port-release gate"
            );
        }
    }

    // Memory-limit knob: with_memory_limit reaches the ContainerSpec; None when unset.
    #[tokio::test]
    async fn with_memory_limit_carries_through_to_the_container_spec() {
        let backend = FakeBackend::new();
        let limited = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_memory_limit(1024);
        let guard = limited.start().await.unwrap();
        assert_eq!(
            backend.state.lock().unwrap().created[0].memory_limit_mb,
            Some(1024)
        );
        guard.stop().await.unwrap();

        let unset = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = unset.start().await.unwrap();
        assert_eq!(
            backend
                .state
                .lock()
                .unwrap()
                .created
                .last()
                .unwrap()
                .memory_limit_mb,
            None
        );
        guard.stop().await.unwrap();
    }

    // dedup_env_last_wins in isolation — last value wins, key keeps the position
    // of its FIRST occurrence: an insertion-ordered map's put never moves an
    // existing key.
    #[test]
    fn dedup_env_last_wins_keeps_first_position_but_last_value() {
        let env = vec![
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "2".to_string()),
            ("A".to_string(), "override".to_string()),
            ("C".to_string(), "3".to_string()),
            ("B".to_string(), "final".to_string()),
        ];
        let deduped = dedup_env_last_wins(env);
        assert_eq!(
            deduped,
            vec![
                ("A".to_string(), "override".to_string()),
                ("B".to_string(), "final".to_string()),
                ("C".to_string(), "3".to_string()),
            ],
            "A and B must keep their FIRST-occurrence position; each must carry its LAST value"
        );
    }

    #[test]
    fn dedup_env_last_wins_is_a_no_op_on_already_unique_keys() {
        let env = vec![
            ("X".to_string(), "1".to_string()),
            ("Y".to_string(), "2".to_string()),
        ];
        assert_eq!(dedup_env_last_wins(env.clone()), env);
    }

    // Fix 3 (end-to-end through start()): a duplicate key set via with_env twice
    // reaches the backend's ContainerSpec exactly once, with the LAST value, in the
    // position of the FIRST with_env call — proving the dedup runs on the real
    // start() path, not just as a unit in isolation.
    #[tokio::test]
    async fn duplicate_with_env_calls_resolve_last_wins_in_the_spec_reaching_the_backend() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_env("MODE", "first")
            .with_env("OTHER", "x")
            .with_env("MODE", "second");
        let guard = c.start().await.unwrap();

        let spec = backend.state.lock().unwrap().created[0].clone();
        assert_eq!(
            spec.env,
            vec![
                ("MODE".to_string(), "second".to_string()),
                ("OTHER".to_string(), "x".to_string()),
            ],
            "MODE must appear exactly once, in its first-occurrence position, with its last value"
        );
        guard.stop().await.unwrap();
    }

    // Fix 3 (spec-customizer interaction): a customizer pushing a key that
    // duplicates one already set via with_env must ALSO resolve last-wins — the
    // dedup runs after the customizer, not just on the pre-customizer env.
    #[tokio::test]
    async fn a_spec_customizer_overriding_an_existing_key_also_resolves_last_wins() {
        let backend = FakeBackend::new();
        let c = container_on(&backend)
            .with_exposed_ports(&[6379])
            .with_env("KAFKA_ADVERTISED_LISTENERS", "PLACEHOLDER")
            .with_spec_customizer(|mut spec, _mapped| {
                spec.env.push((
                    "KAFKA_ADVERTISED_LISTENERS".to_string(),
                    "PLAINTEXT://127.0.0.1:9999".to_string(),
                ));
                spec
            });
        let guard = c.start().await.unwrap();

        let spec = backend.state.lock().unwrap().created[0].clone();
        assert_eq!(
            spec.env,
            vec![(
                "KAFKA_ADVERTISED_LISTENERS".to_string(),
                "PLAINTEXT://127.0.0.1:9999".to_string()
            )],
            "the customizer's later push must win, deduped to a single entry"
        );
        guard.stop().await.unwrap();
    }

    // U3: exec/get_mapped_port on a not-running guard errors.
    #[tokio::test]
    async fn u3_exec_and_mapped_port_require_a_running_container() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let mut guard = c.start().await.unwrap();
        guard.stop_inner().await;

        assert!(guard.exec(&["ls"]).await.is_err());
        assert!(guard.get_mapped_port(6379).is_err());
    }

    #[tokio::test]
    async fn exec_returns_the_backends_result() {
        let backend = FakeBackend::new();
        let c = container_on(&backend);
        let guard = c.start().await.unwrap();
        let result = guard.exec(&["ls", "-la"]).await.unwrap();
        assert_eq!(result.stdout, "ls -la");
        guard.stop().await.unwrap();
    }

    #[tokio::test]
    async fn dropping_a_guard_without_stop_releases_its_ports_synchronously() {
        let backend = FakeBackend::new();
        let c = container_on(&backend).with_exposed_ports(&[6379]);
        let guard = c.start().await.unwrap();
        let port = guard.get_mapped_port(6379).unwrap();
        assert!(free_ports().issued_view().contains(&port));

        drop(guard); // no explicit stop(): Drop's synchronous fallback must still release the port.

        assert!(
            !free_ports().issued_view().contains(&port),
            "Drop must release mapped ports synchronously even without an explicit stop()"
        );
    }
}
