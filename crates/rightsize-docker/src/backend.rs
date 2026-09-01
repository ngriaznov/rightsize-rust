//! `DockerBackend`: maps `SandboxBackend` onto the daemon HTTP endpoints this
//! backend needs, over the hand-rolled unix-socket client and the frame demuxer —
//! with one deliberate exception: runtime file copy shells out to the `docker` CLI
//! (see [`DockerBackend::copy_to_container`]'s doc) rather than hand-rolling the
//! daemon's tar-archive copy endpoints. This is also the correctness oracle other
//! backends are checked against, since Docker enforces semantics (read-only mounts,
//! native networks) microsandbox only emulates.
//!
//! **`Handle` has no per-container mutable state** (contrast microsandbox's
//! `HandleState` map): every operation here is a stateless HTTP call keyed by the
//! daemon-assigned container id already carried on the handle, so there's nothing to
//! look up in a side table.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use rightsize::backend::{Capabilities, FollowHandle, SandboxBackend, SandboxHandle};
use rightsize::error::{Result, RightsizeError};
use rightsize::model::{ContainerSpec, ExecResult};

use crate::client::DockerClient;
use crate::frames::{BodyReader, LineAssembler, StreamType, demux_into, read_frame};
use crate::json::{
    ConnectNetworkBody, CreateContainerBody, CreateExecBody, CreateNetworkBody, EmptyObject,
    EndpointConfig, ExecInspect, HostConfig, IdResponse, LabelFilter, ListedEntry, NameFilter,
    PortBinding as JsonPortBinding, StartExecBody,
};

/// How long `POST /containers/{id}/stop` gives the entrypoint to exit before the
/// daemon SIGKILLs it.
const STOP_TIMEOUT_SECS: u64 = 10;

/// The label every non-`keep_alive` container this backend creates carries, so
/// `close()`/the reaper can find exactly this run's containers without touching
/// anyone else's. This is a cross-process wire format, not Rust API surface, so it
/// doesn't need to match this crate's own naming conventions.
const RUN_ID_LABEL_KEY: &str = "dev.rightsize.run_id";

/// The label a `keep_alive` (reuse) container carries INSTEAD of
/// [`RUN_ID_LABEL_KEY`] — deliberately a different key, so `close()`'s run-id label
/// filter (and any run-id-scoped listing) structurally never matches a reuse
/// container. See `ContainerSpec::keep_alive`'s doc.
const REUSE_LABEL_KEY: &str = "dev.rightsize.reuse";

/// A Docker container reference: the daemon-assigned id plus the spec that created
/// it. No mutable per-container state (see the module docs) — every operation is a
/// stateless call keyed by `id`.
struct Handle {
    id: String,
    spec: ContainerSpec,
}

impl SandboxHandle for Handle {
    fn id(&self) -> &str {
        &self.id
    }
    fn spec(&self) -> &ContainerSpec {
        &self.spec
    }
}

/// Drives the Docker daemon over `DockerClient`. See the module docs for the shape.
pub struct DockerBackend {
    client: DockerClient,
    /// Caches network-id lookups so `ensure_network`/`install`/`remove_network` agree
    /// on which daemon-side network a given `rightsize` network id maps to, without a
    /// list-by-name round trip on every call.
    network_ids: Mutex<HashMap<String, String>>,
}

impl DockerBackend {
    /// Builds a backend talking to the daemon through `client`. Crate-internal:
    /// [`crate::DockerBackendProvider`] (via `rightsize::backends::resolve`) is the
    /// usual way to get one; [`Self::connecting_to_env`] is the public escape hatch
    /// for callers (integration tests, mainly) that want a `DockerBackend` directly
    /// without going through backend resolution.
    pub(crate) fn new(client: DockerClient) -> Self {
        DockerBackend {
            client,
            network_ids: Mutex::new(HashMap::new()),
        }
    }

    /// Builds a backend pointed at the daemon `DOCKER_HOST` (or the default socket)
    /// names — the same resolution `crate::DockerBackendProvider`'s `create` uses,
    /// exposed directly for callers that want a concrete `DockerBackend` without
    /// going through `rightsize::backends::resolve` (integration tests, mainly).
    pub fn connecting_to_env() -> Self {
        Self::new(DockerClient::from_env())
    }

    /// Exposed for the transport regression test: the socket path this
    /// backend's client actually dials, so a test can assert it's a unix path and
    /// never `localhost:2375` or similar.
    #[cfg(test)]
    pub(crate) fn socket_path_for_test(&self) -> std::path::PathBuf {
        self.client.socket_path().to_path_buf()
    }

    /// `GET /images/{name}/json` to check presence, `POST /images/create` to pull if
    /// a 404 says it's missing. The pull response is a stream of progress JSON lines
    /// this backend doesn't parse — it drains the body to completion (the daemon
    /// itself decides when the pull is done; a stream that closes IS "pull finished
    /// or failed") and doesn't fail loudly on inspect trouble, only reacts to
    /// affirmatively-absent.
    async fn pull_if_missing(&self, image: &str) -> Result<()> {
        let inspect_path = format!("/images/{}/json", encode_path_segment(image));
        let inspect = self.client.request("GET", &inspect_path, None).await?;
        if inspect.status == 200 {
            return Ok(());
        }
        let (repo, tag) = split_repo_tag(image);
        let pull_path = format!(
            "/images/create?fromImage={}&tag={}",
            encode_query_value(&repo),
            encode_query_value(&tag)
        );
        let resp = self.client.request("POST", &pull_path, None).await?;
        if resp.status >= 400 {
            return Err(RightsizeError::Backend(format!(
                "docker could not pull image '{image}' (HTTP {}): {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }
        Ok(())
    }

    /// `POST /networks/{id}/connect` with the aliases this container should be
    /// reachable as.
    async fn connect_network(
        &self,
        container_id: &str,
        network_id: &str,
        aliases: &[String],
    ) -> Result<()> {
        let body = ConnectNetworkBody {
            container: container_id.to_string(),
            endpoint_config: EndpointConfig {
                aliases: aliases.to_vec(),
            },
        };
        let body = serde_json::to_string(&body)
            .expect("ConnectNetworkBody has no non-serializable fields");
        let path = format!("/networks/{network_id}/connect");
        let resp = self.client.request("POST", &path, Some(&body)).await?;
        if resp.status >= 400 {
            return Err(RightsizeError::Backend(format!(
                "docker could not connect container {container_id} to network {network_id} \
                 (HTTP {}): {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }
        Ok(())
    }

    /// `GET /networks?filters=...` then `POST /networks/create` — cached by
    /// `network_id` so repeat calls (e.g. a second container joining the same
    /// `rightsize` network) don't re-list/re-create.
    async fn ensure_network_get_id(&self, network_id: &str) -> Result<String> {
        if let Some(id) = self
            .network_ids
            .lock()
            .expect("network_ids mutex poisoned")
            .get(network_id)
        {
            return Ok(id.clone());
        }

        let filters = NameFilter {
            name: vec![network_id.to_string()],
        };
        let filters =
            serde_json::to_string(&filters).expect("NameFilter has no non-serializable fields");
        let list_path = format!("/networks?filters={}", encode_query_value(&filters));
        let list = self.client.request("GET", &list_path, None).await?;
        if list.status == 200 {
            let entries: Vec<ListedEntry> = serde_json::from_slice(&list.body).unwrap_or_default();
            if let Some(entry) = entries.into_iter().next() {
                self.network_ids
                    .lock()
                    .expect("network_ids mutex poisoned")
                    .insert(network_id.to_string(), entry.id.clone());
                return Ok(entry.id);
            }
        }

        let create_body = CreateNetworkBody {
            name: network_id.to_string(),
        };
        let create_body = serde_json::to_string(&create_body)
            .expect("CreateNetworkBody has no non-serializable fields");
        let created = self
            .client
            .request("POST", "/networks/create", Some(&create_body))
            .await?;
        if created.status >= 400 {
            return Err(RightsizeError::Backend(format!(
                "docker could not create network '{network_id}' (HTTP {}): {}",
                created.status,
                String::from_utf8_lossy(&created.body)
            )));
        }
        let id = serde_json::from_slice::<IdResponse>(&created.body)
            .map_err(|e| {
                RightsizeError::Backend(format!(
                    "docker's network-create response for '{network_id}' had no Id field: {e} \
                     (body: {})",
                    String::from_utf8_lossy(&created.body)
                ))
            })?
            .id;
        self.network_ids
            .lock()
            .expect("network_ids mutex poisoned")
            .insert(network_id.to_string(), id.clone());
        Ok(id)
    }

    /// A live `GET /networks?filters={"name":[name]}` lookup, independent of the
    /// in-memory `network_ids` cache — [`Self::remove_network`]'s cache-miss
    /// fallback, so removing a network created by a DIFFERENT process (the reaping
    /// sweep's whole purpose) doesn't silently no-op just because THIS backend
    /// instance never itself called [`Self::ensure_network_get_id`] for it.
    /// Best-effort: `None` on any failure (request error, non-200, no match,
    /// unparseable body), matching `remove_network`'s own best-effort contract.
    async fn lookup_network_id_by_name(&self, name: &str) -> Option<String> {
        let filters = NameFilter {
            name: vec![name.to_string()],
        };
        let filters = serde_json::to_string(&filters).ok()?;
        let list_path = format!("/networks?filters={}", encode_query_value(&filters));
        let list = self.client.request("GET", &list_path, None).await.ok()?;
        if list.status != 200 {
            return None;
        }
        let entries: Vec<ListedEntry> = serde_json::from_slice(&list.body).ok()?;
        entries.into_iter().next().map(|e| e.id)
    }
}

/// Builds the `POST /containers/create` JSON body: `Image`, `Env[]`, `Cmd[]?`,
/// `ExposedPorts{}`, `Labels`, and a `HostConfig` carrying port bindings (bound to
/// `127.0.0.1` — never the wildcard address), read-only/read-write binds, the
/// `host.docker.internal:host-gateway` extra host (lets a container reach services
/// running on the host, e.g. a test's own HTTP server, the same way it would under a
/// real Docker Desktop install), and an optional memory cap. `disk_limit_mb`,
/// `tmpfs_root_mb`, and `network_disabled` are msb-only spec fields and are
/// deliberately not read here — this backend runs with its normal disk-backed
/// rootfs and normal networking regardless of their value.
fn build_create_body(spec: &ContainerSpec) -> CreateContainerBody {
    let env = spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect();

    let exposed_ports = spec
        .ports
        .iter()
        .map(|p| (format!("{}/tcp", p.guest_port), EmptyObject {}))
        .collect();

    let port_bindings = spec
        .ports
        .iter()
        .map(|p| {
            (
                format!("{}/tcp", p.guest_port),
                vec![JsonPortBinding {
                    host_ip: "127.0.0.1".to_string(),
                    host_port: p.host_port.to_string(),
                }],
            )
        })
        .collect();

    let binds = spec
        .mounts
        .iter()
        .map(|m| {
            let mode = if m.read_only { "ro" } else { "rw" };
            format!("{}:{}:{}", m.host_path.display(), m.guest_path, mode)
        })
        .collect();

    let labels = if spec.keep_alive {
        BTreeMap::from([(REUSE_LABEL_KEY.to_string(), reuse_label_value(spec))])
    } else {
        BTreeMap::from([(RUN_ID_LABEL_KEY.to_string(), spec.run_id.clone())])
    };

    CreateContainerBody {
        image: spec.image.clone(),
        env,
        cmd: spec.command.clone(),
        exposed_ports,
        labels,
        host_config: HostConfig {
            port_bindings,
            binds,
            extra_hosts: vec!["host.docker.internal:host-gateway".to_string()],
            memory: spec.memory_limit_mb.map(|mb| mb * 1024 * 1024),
        },
    }
}

/// The 12-hex-char value a `keep_alive` container's [`REUSE_LABEL_KEY`] label
/// carries. Every real reuse container `rightsize::container`'s reuse flow builds
/// always carries the exact `rz-reuse-<12hex>` name that hash is derived from (see
/// `rightsize::reuse::compute_identity`) — this extracts that suffix directly,
/// rather than re-deriving the hash itself, so the label always agrees with the
/// name regardless of which language built the spec. Falls back to a cheap,
/// dependency-free FNV-1a-style hash of `spec.name` for a `keep_alive` spec built
/// directly with some OTHER name shape (only ever a unit test constructing
/// `ContainerSpec` by hand) — deterministic, just not meaningfully tied to any
/// cross-language identity.
fn reuse_label_value(spec: &ContainerSpec) -> String {
    if let Some(hex) = spec.name.strip_prefix("rz-reuse-") {
        if hex.len() == 12 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return hex.to_string();
        }
    }
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in spec.name.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    format!("{:012x}", hash & 0xFFFF_FFFF_FFFF)
}

/// True if `message` (a daemon error body/exception message) names a host-port bind
/// conflict — the daemon has no distinct exception type for this, only free-text like
/// "driver failed programming external connectivity ... address already in use" or
/// "Bind for 0.0.0.0:PORT failed: port is already allocated".
fn is_port_bind_conflict_message(message: &str) -> bool {
    let m = message.to_lowercase();
    m.contains("already in use") || m.contains("already allocated")
}

/// Percent-encodes a path segment (just enough for image references, which can
/// contain `/` that must stay literal in this position but nothing else exotic in
/// practice) — used for `GET /images/{name}/json`.
fn encode_path_segment(s: &str) -> String {
    // Image names/tags are `[a-zA-Z0-9_.\-/:@]`-shaped in practice; none of those need
    // percent-encoding in a URL path segment, so this is intentionally a pass-through
    // rather than a full RFC 3986 encoder — there is nothing in this crate's own
    // input space that would need escaping here.
    s.to_string()
}

/// Percent-encodes a query string value (`fromImage`, `tag`, the JSON `filters` blob)
/// — covers exactly the characters that show up in image references and the small
/// hand-built JSON filter strings this backend sends, not a general URL encoder.
fn encode_query_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Splits `image` into `(repository, tag)` for `POST /images/create?fromImage=&tag=`.
/// A tag-less reference (`"redis"`) defaults to `"latest"`, matching Docker's own
/// convention; a digest reference (`repo@sha256:...`) is passed through as the
/// "repository" half with an empty tag, since `fromImage` accepts the whole
/// `repo@digest` form and an empty `tag` is simply omitted from the query by the
/// caller not being asked to add one — see the call site, which always sends both
/// params, so this returns `""` rather than `"latest"` for a digest reference,
/// avoiding a nonsensical `tag=latest` alongside an explicit digest.
fn split_repo_tag(image: &str) -> (String, String) {
    if image.contains('@') {
        return (image.to_string(), String::new());
    }
    // A tag separator is the LAST colon that appears after the last slash (so a
    // registry host:port prefix like `localhost:5000/redis` isn't mistaken for a
    // tag separator).
    let slash_idx = image.rfind('/').map(|i| i + 1).unwrap_or(0);
    match image[slash_idx..].rfind(':') {
        Some(rel_colon) => {
            let colon = slash_idx + rel_colon;
            (image[..colon].to_string(), image[colon + 1..].to_string())
        }
        None => (image.to_string(), "latest".to_string()),
    }
}

#[async_trait::async_trait]
impl SandboxBackend for DockerBackend {
    fn name(&self) -> &str {
        "docker"
    }

    fn supports_native_networks(&self) -> bool {
        true
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            // Containers share the host kernel — no per-sandbox microVM boundary.
            hardware_isolated: false,
            checkpoint: true,
            // An image commit touches nothing running — the container is left
            // undisturbed, unlike microsandbox's stop/snapshot/start cycle.
            checkpoint_restarts_workload: false,
        }
    }

    async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
        self.pull_if_missing(&spec.image).await?;

        let body = build_create_body(&spec);
        let body = serde_json::to_string(&body)
            .expect("CreateContainerBody has no non-serializable fields");
        let path = format!("/containers/create?name={}", encode_query_value(&spec.name));
        let resp = self.client.request("POST", &path, Some(&body)).await?;
        if resp.status == 409 {
            let message = String::from_utf8_lossy(&resp.body).into_owned();
            return Err(RightsizeError::NameConflict {
                message: format!(
                    "docker container name '{}' is already in use (HTTP 409): {message}",
                    spec.name
                ),
                source: None,
            });
        }
        if resp.status >= 400 {
            let message = String::from_utf8_lossy(&resp.body).into_owned();
            return Err(RightsizeError::Backend(format!(
                "docker could not create container '{}' (HTTP {}): {message}",
                spec.name, resp.status
            )));
        }
        let id = serde_json::from_slice::<IdResponse>(&resp.body)
            .map_err(|e| {
                RightsizeError::Backend(format!(
                    "docker's container-create response for '{}' had no Id field: {e} (body: {})",
                    spec.name,
                    String::from_utf8_lossy(&resp.body)
                ))
            })?
            .id;

        if let Some(network_id) = &spec.network_id {
            let daemon_network_id = self.ensure_network_get_id(network_id).await?;
            self.connect_network(&id, &daemon_network_id, &spec.aliases)
                .await?;
        }

        Ok(Box::new(Handle { id, spec }))
    }

    async fn start(&self, handle: &dyn SandboxHandle) -> Result<()> {
        let path = format!("/containers/{}/start", handle.id());
        let resp = self.client.request("POST", &path, None).await?;
        if resp.status == 204 || resp.status == 304 {
            return Ok(()); // 304 = already started, treated as success like the daemon intends.
        }
        let message = String::from_utf8_lossy(&resp.body).into_owned();
        if resp.status == 500 && is_port_bind_conflict_message(&message) {
            return Err(RightsizeError::PortBindConflict {
                message: format!(
                    "docker could not bind a host port for {}: {message}",
                    handle.id()
                ),
                source: None,
            });
        }
        Err(RightsizeError::Backend(format!(
            "docker could not start container {} (HTTP {}): {message}",
            handle.id(),
            resp.status
        )))
    }

    async fn stop(&self, handle: &dyn SandboxHandle) -> Result<()> {
        let path = format!("/containers/{}/stop?t={STOP_TIMEOUT_SECS}", handle.id());
        let _ = self.client.request("POST", &path, None).await; // best-effort
        Ok(())
    }

    async fn remove(&self, handle: &dyn SandboxHandle) -> Result<()> {
        let path = format!("/containers/{}?force=true", handle.id());
        let _ = self.client.request("DELETE", &path, None).await; // best-effort
        Ok(())
    }

    async fn exec(&self, handle: &dyn SandboxHandle, cmd: &[String]) -> Result<ExecResult> {
        let create_body = CreateExecBody {
            attach_stdout: true,
            attach_stderr: true,
            cmd: cmd.to_vec(),
        };
        let create_body = serde_json::to_string(&create_body)
            .expect("CreateExecBody has no non-serializable fields");
        let create_path = format!("/containers/{}/exec", handle.id());
        let created = self
            .client
            .request("POST", &create_path, Some(&create_body))
            .await?;
        if created.status >= 400 {
            return Err(RightsizeError::Backend(format!(
                "docker could not create an exec for container {} (HTTP {}): {}",
                handle.id(),
                created.status,
                String::from_utf8_lossy(&created.body)
            )));
        }
        let exec_id = serde_json::from_slice::<IdResponse>(&created.body)
            .map_err(|e| {
                RightsizeError::Backend(format!(
                    "docker's exec-create response for container {} had no Id field: {e} \
                     (body: {})",
                    handle.id(),
                    String::from_utf8_lossy(&created.body)
                ))
            })?
            .id;

        let start_path = format!("/exec/{exec_id}/start");
        let start_body = serde_json::to_string(&StartExecBody { detach: false })
            .expect("StartExecBody has no non-serializable fields");
        let (headers, mut stream) = self
            .client
            .request_stream("POST", &start_path, Some(&start_body))
            .await?;
        if headers.status >= 400 {
            return Err(RightsizeError::Backend(format!(
                "docker could not start exec {exec_id} for container {} (HTTP {})",
                handle.id(),
                headers.status
            )));
        }
        let mut body = BodyReader::new(&mut stream, &headers);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        demux_into(
            &mut body,
            |b| stdout.extend_from_slice(b),
            |b| stderr.extend_from_slice(b),
        )
        .await?;

        let inspect_path = format!("/exec/{exec_id}/json");
        let inspected = self.client.request("GET", &inspect_path, None).await?;
        let exit_code = serde_json::from_slice::<ExecInspect>(&inspected.body)
            .ok()
            .and_then(|r| r.exit_code)
            .unwrap_or(-1);

        Ok(ExecResult {
            exit_code: exit_code as i32,
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
    }

    async fn logs(&self, handle: &dyn SandboxHandle) -> Result<String> {
        let path = format!(
            "/containers/{}/logs?stdout=1&stderr=1&tail=1000",
            handle.id()
        );
        let (headers, mut stream) = self.client.request_stream("GET", &path, None).await?;
        if headers.status >= 400 {
            return Err(RightsizeError::Backend(format!(
                "docker could not fetch logs for container {} (HTTP {})",
                handle.id(),
                headers.status
            )));
        }
        let mut body = BodyReader::new(&mut stream, &headers);
        let mut assembler = LineAssembler::new();
        let mut out = String::new();
        while let Some(frame) = read_frame(&mut body).await? {
            if !matches!(frame.stream, StreamType::Stdout | StreamType::Stderr) {
                continue;
            }
            let text = String::from_utf8_lossy(&frame.payload).into_owned();
            for line in assembler.feed(&text) {
                out.push_str(&line);
                out.push('\n');
            }
        }
        if let Some(tail) = assembler.flush() {
            out.push_str(&tail);
            out.push('\n');
        }
        Ok(out)
    }

    async fn follow_logs(
        &self,
        handle: &dyn SandboxHandle,
        consumer: Box<dyn Fn(String) + Send + Sync>,
    ) -> Result<FollowHandle> {
        let path = format!(
            "/containers/{}/logs?stdout=1&stderr=1&follow=1&tail=all",
            handle.id()
        );
        let (headers, mut stream) = self.client.request_stream("GET", &path, None).await?;
        if headers.status >= 400 {
            return Err(RightsizeError::Backend(format!(
                "docker could not follow logs for container {} (HTTP {})",
                handle.id(),
                headers.status
            )));
        }

        let close_requested = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_close = close_requested.clone();
        let task = tokio::spawn(async move {
            let mut body = BodyReader::new(&mut stream, &headers);
            let mut assembler = LineAssembler::new();
            loop {
                // Checked BEFORE each delivery (not just once per loop) — the
                // resolved P1 forward flag: `abort()` alone can't guarantee no
                // post-close callback, since it only cancels at the next `.await`
                // point. Checking this flag right before every consumer call means
                // the flag and the abort jointly rule that out — see
                // `rightsize::backend::FollowHandle`'s doc for the full contract.
                if task_close.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                let frame = match read_frame(&mut body).await {
                    Ok(Some(f)) => f,
                    Ok(None) => break, // clean stream end — the normal way this loop exits.
                    Err(_) => break,   // best-effort: a stream error just ends delivery.
                };
                if !matches!(frame.stream, StreamType::Stdout | StreamType::Stderr) {
                    continue;
                }
                let text = String::from_utf8_lossy(&frame.payload).into_owned();
                for line in assembler.feed(&text) {
                    if task_close.load(std::sync::atomic::Ordering::SeqCst) {
                        return;
                    }
                    consumer(line);
                }
            }
            if task_close.load(std::sync::atomic::Ordering::SeqCst) {
                return; // stop delivery, never flush — an explicit close beat the stream to the end.
            }
            if let Some(tail) = assembler.flush() {
                consumer(tail);
            }
        });

        Ok(FollowHandle::from_task(close_requested, task))
    }

    async fn ensure_network(&self, network_id: &str) -> Result<()> {
        self.ensure_network_get_id(network_id).await?;
        Ok(())
    }

    async fn remove_network(&self, network_id: &str) -> Result<()> {
        let cached = self
            .network_ids
            .lock()
            .expect("network_ids mutex poisoned")
            .remove(network_id);
        // Cache miss: this backend instance never itself called `ensure_network` for
        // `network_id` — the reaping sweep's whole point is removing resources a
        // DIFFERENT (dead) process created, so `network_ids` (populated only by THIS
        // instance's own `ensure_network_get_id` calls) is empty for it. Fall back to
        // a live daemon lookup by name — the same "must not depend on in-process
        // state" requirement `remove_by_name`'s container-level lookup already
        // satisfies (`blocking_find_container_id_by_name`).
        let daemon_id = match cached {
            Some(id) => Some(id),
            None => self.lookup_network_id_by_name(network_id).await,
        };
        if let Some(id) = daemon_id {
            let path = format!("/networks/{id}");
            let _ = self.client.request("DELETE", &path, None).await; // best-effort
        }
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let run_id = rightsize::RunId::value();
        let filters = LabelFilter {
            label: vec![format!("{RUN_ID_LABEL_KEY}={run_id}")],
        };
        let filters =
            serde_json::to_string(&filters).expect("LabelFilter has no non-serializable fields");
        let path = format!(
            "/containers/json?all=true&filters={}",
            encode_query_value(&filters)
        );
        let listed = self.client.request("GET", &path, None).await?;
        if listed.status != 200 {
            return Ok(()); // best-effort: nothing more to do if even listing fails.
        }
        let entries: Vec<ListedEntry> = serde_json::from_slice(&listed.body).unwrap_or_default();
        for entry in entries {
            let remove_path = format!("/containers/{}?force=true", entry.id);
            let _ = self.client.request("DELETE", &remove_path, None).await;
        }
        Ok(())
    }

    fn cleanup_sync(&self, container_id: &str) {
        // Blocking std I/O only, no Tokio — this runs on the dedicated cleanup thread
        // (see rightsize::cleanup), never in async context (decision 1).
        let _ = blocking_force_remove(self.client.socket_path(), container_id, STOP_TIMEOUT_SECS);
    }

    fn remove_by_name(&self, name: &str) {
        // The reaping ledger persists NAMES, but every other call on this backend is
        // keyed by the daemon-assigned id already carried on a `Handle` — so this
        // resolves name -> id first (a list-by-name-filter GET), then force-removes
        // that id. SYNCHRONOUS, blocking std I/O only (see
        // `SandboxBackend::remove_by_name`'s doc for why) — the same transport shape
        // `cleanup_sync`/`blocking_force_remove` already use, extended with a body
        // read since this call needs the list response, not just a status code.
        let socket = self.client.socket_path();
        let Some(id) = blocking_find_container_id_by_name(socket, name) else {
            return; // not found (or the lookup itself failed): silently fine.
        };
        let _ = blocking_force_remove(socket, &id, STOP_TIMEOUT_SECS);
    }

    /// `GET /containers/json?filters={"name":["^/<name>$"],"status":["running"]}` —
    /// the reuse adopt path's own query (`rightsize::reuse`). The `name` filter is
    /// anchored the same way `blocking_find_container_id_by_name` anchors it
    /// (Docker's `name` filter is substring-by-default); `status` is explicit
    /// rather than relying on the daemon's own "no `all=true`" default meaning
    /// running-only, so the intent reads directly off the request. `Ok(None)` for
    /// no match OR a non-200 response — matching this backend's other best-effort
    /// list-then-not-found reads (e.g. `lookup_network_id_by_name`).
    async fn find_running(&self, spec: &ContainerSpec) -> Result<Option<Box<dyn SandboxHandle>>> {
        let filters = format!(r#"{{"name":["^/{}$"],"status":["running"]}}"#, spec.name);
        let path = format!("/containers/json?filters={}", encode_query_value(&filters));
        let resp = self.client.request("GET", &path, None).await?;
        if resp.status != 200 {
            return Ok(None);
        }
        let entries: Vec<ListedEntry> = serde_json::from_slice(&resp.body).unwrap_or_default();
        Ok(entries.into_iter().next().map(|entry| {
            Box::new(Handle {
                id: entry.id,
                spec: spec.clone(),
            }) as Box<dyn SandboxHandle>
        }))
    }

    /// `POST /commit?container=&repo=&tag=` — the checkpoint feature's own backend
    /// primitive (`rightsize::ContainerGuard::checkpoint`). Formats `nonce` (a
    /// random 12-lowercase-hex string the generic layer generates fresh per call)
    /// into this backend's own ref shape, `rightsize/checkpoint:<nonce>`, splitting
    /// it into the `repo`/`tag` pair the endpoint wants (the same split
    /// `split_repo_tag` already does for image pulls), and returns the ref.
    async fn create_checkpoint(&self, handle: &dyn SandboxHandle, nonce: &str) -> Result<String> {
        let checkpoint_ref = format!("rightsize/checkpoint:{nonce}");
        let (repo, tag) = split_repo_tag(&checkpoint_ref);
        let path = format!(
            "/commit?container={}&repo={}&tag={}",
            encode_query_value(handle.id()),
            encode_query_value(&repo),
            encode_query_value(&tag)
        );
        let resp = self.client.request("POST", &path, None).await?;
        if resp.status >= 400 {
            return Err(RightsizeError::Backend(format!(
                "docker could not commit container {} to image '{checkpoint_ref}' (HTTP {}): {}",
                handle.id(),
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }
        Ok(checkpoint_ref)
    }

    /// `DELETE /images/{ref}?force=true` — best-effort; a 404 (already gone) is
    /// success, matching [`Self::remove_by_name`]'s own "not found is fine"
    /// contract.
    async fn remove_checkpoint(&self, checkpoint_ref: &str) -> Result<()> {
        let path = format!("/images/{}?force=true", encode_path_segment(checkpoint_ref));
        let resp = self.client.request("DELETE", &path, None).await?;
        if resp.status >= 400 && resp.status != 404 {
            return Err(RightsizeError::Backend(format!(
                "docker could not remove checkpoint image '{checkpoint_ref}' (HTTP {}): {}",
                resp.status,
                String::from_utf8_lossy(&resp.body)
            )));
        }
        Ok(())
    }

    /// `GET /images/{ref}/json` — the named-checkpoint existence probe
    /// (`SandboxBackend::has_checkpoint`, `Checkpoint::find`'s staleness check).
    /// A 200 means the tagged image still exists; a 404 is a definite "not
    /// there." Any OTHER status (or a transport-level request failure, via the
    /// `?` above) surfaces as an error — this SPI forbids a probe failure from
    /// resolving to "absent," unlike [`Self::pull_if_missing`]'s own inspect,
    /// which only reacts to affirmatively-404 and doesn't fail loudly on
    /// inspect trouble.
    async fn has_checkpoint(&self, checkpoint_ref: &str) -> Result<bool> {
        let path = format!("/images/{}/json", encode_path_segment(checkpoint_ref));
        let resp = self.client.request("GET", &path, None).await?;
        match resp.status {
            200 => Ok(true),
            404 => Ok(false),
            status => Err(RightsizeError::Backend(format!(
                "docker could not inspect checkpoint image '{checkpoint_ref}' (HTTP {status}): {}",
                String::from_utf8_lossy(&resp.body)
            ))),
        }
    }

    /// Shells out to the `docker` CLI (`docker save -o <dest> <ref>`) — same
    /// rationale as [`Self::copy_to_container`]'s own CLI escape hatch: the daemon
    /// has no single HTTP endpoint for this, and `docker` is already a hard
    /// requirement for this backend regardless. The checkpoint-archive feature's
    /// export primitive (`rightsize::Checkpoint::export_to`).
    async fn export_checkpoint(&self, checkpoint_ref: &str, dest: &Path) -> Result<()> {
        let args = docker_save_args(checkpoint_ref, dest);
        let result = run_docker_cli(&args).await?;
        if result.exit_code != 0 {
            return Err(RightsizeError::Backend(format!(
                "docker save -o {} {checkpoint_ref} failed (exit {}): {}",
                dest.display(),
                result.exit_code,
                result.stderr.trim()
            )));
        }
        Ok(())
    }

    /// `docker load -i <src_file>` — the checkpoint-archive feature's import
    /// primitive (`rightsize::Checkpoint::import_from`). Unlike microsandbox's
    /// content-addressed import, `docker load` preserves the tag baked into the
    /// save file, so the effective ref is simply `ref_hint` (the archive
    /// manifest's original ref) unchanged — loading over an already-existing tag
    /// just re-points it.
    async fn import_checkpoint(&self, src_file: &Path, ref_hint: &str) -> Result<String> {
        let args = docker_load_args(src_file);
        let result = run_docker_cli(&args).await?;
        if result.exit_code != 0 {
            return Err(RightsizeError::Backend(format!(
                "docker load -i {} failed (exit {}): {}",
                src_file.display(),
                result.exit_code,
                result.stderr.trim()
            )));
        }
        Ok(ref_hint.to_string())
    }

    /// Shells out to the `docker` CLI (`docker cp <host_path> <id>:<container_path>`)
    /// rather than the daemon HTTP API this backend otherwise speaks exclusively —
    /// the daemon's copy endpoints are tar-archive-in/tar-archive-out, and adding
    /// tar encode/decode here would mean either a new third-party dependency or a
    /// hand-rolled tar implementation, both worse than shelling out. The `docker`
    /// CLI is already a hard requirement for this backend regardless (the reaping
    /// watchdog's own kill command is a plain `docker rm -f`), so this adds no new
    /// external dependency.
    async fn copy_to_container(
        &self,
        handle: &dyn SandboxHandle,
        host_path: &Path,
        container_path: &str,
    ) -> Result<()> {
        let args = docker_cp_in_args(handle.id(), host_path, container_path);
        let result = run_docker_cli(&args).await?;
        if result.exit_code != 0 {
            return Err(RightsizeError::Backend(format!(
                "docker cp into container {} failed (exit {}): {}",
                handle.id(),
                result.exit_code,
                result.stderr.trim()
            )));
        }
        Ok(())
    }

    /// The reverse direction of [`Self::copy_to_container`], same `docker cp`
    /// shape and error-surfacing.
    async fn copy_from_container(
        &self,
        handle: &dyn SandboxHandle,
        container_path: &str,
        host_path: &Path,
    ) -> Result<()> {
        let args = docker_cp_out_args(handle.id(), container_path, host_path);
        let result = run_docker_cli(&args).await?;
        if result.exit_code != 0 {
            return Err(RightsizeError::Backend(format!(
                "docker cp out of container {} failed (exit {}): {}",
                handle.id(),
                result.exit_code,
                result.stderr.trim()
            )));
        }
        Ok(())
    }

    fn watchdog_kill_command(&self) -> Vec<String> {
        vec!["docker".to_string(), "rm".to_string(), "-f".to_string()]
    }

    fn watchdog_network_kill_command(&self) -> Vec<String> {
        vec![
            "docker".to_string(),
            "network".to_string(),
            "rm".to_string(),
        ]
    }
}

/// Builds the argv for `docker cp <host_path> <container_id>:<container_path>` —
/// pure argv construction, kept separate from [`run_docker_cli`] so the shape is a
/// plain data-in/data-out unit test, independent of a real `docker` binary (the
/// same split `rightsize-msb`'s own `commands` module uses for its `msb` argv).
fn docker_cp_in_args(container_id: &str, host_path: &Path, container_path: &str) -> Vec<String> {
    vec![
        "cp".to_string(),
        host_path.display().to_string(),
        format!("{container_id}:{container_path}"),
    ]
}

/// Builds the argv for `docker cp <container_id>:<container_path> <host_path>` —
/// the reverse direction of [`docker_cp_in_args`].
fn docker_cp_out_args(container_id: &str, container_path: &str, host_path: &Path) -> Vec<String> {
    vec![
        "cp".to_string(),
        format!("{container_id}:{container_path}"),
        host_path.display().to_string(),
    ]
}

/// Builds the argv for `docker save -o <dest> <checkpoint_ref>` — the
/// checkpoint-archive feature's export primitive
/// ([`DockerBackend::export_checkpoint`]).
fn docker_save_args(checkpoint_ref: &str, dest: &Path) -> Vec<String> {
    vec![
        "save".to_string(),
        "-o".to_string(),
        dest.display().to_string(),
        checkpoint_ref.to_string(),
    ]
}

/// Builds the argv for `docker load -i <src_file>` — the checkpoint-archive
/// feature's import primitive ([`DockerBackend::import_checkpoint`]).
fn docker_load_args(src_file: &Path) -> Vec<String> {
    vec![
        "load".to_string(),
        "-i".to_string(),
        src_file.display().to_string(),
    ]
}

/// Runs the `docker` CLI (never the daemon HTTP API this backend otherwise speaks
/// exclusively — see [`DockerBackend::copy_to_container`]'s doc for why copy is the
/// one exception) with a closed/null stdin, matching every other child process this
/// workspace spawns under the hood (see `rightsize-msb`'s own convention), and
/// captures its exit code plus stdout/stderr.
async fn run_docker_cli(args: &[String]) -> Result<ExecResult> {
    let output = tokio::process::Command::new("docker")
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .map_err(|e| {
            RightsizeError::Backend(format!("failed to spawn docker {}: {e}", args.join(" ")))
        })?;
    Ok(ExecResult {
        exit_code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

/// The `cleanup_sync` path's blocking counterpart to `stop`+`remove`: issues a plain
/// `POST /containers/{id}/stop?t=` then `DELETE /containers/{id}?force=true` over a
/// blocking [`crate::stream::BlockingDockerStream`] — no Tokio, since this runs on the
/// dedicated `Drop`-path cleanup thread with no async runtime in context. Best-effort:
/// errors are swallowed by the caller, matching this backend's own async `stop`/
/// `remove`.
fn blocking_force_remove(
    socket_path: &std::path::Path,
    container_id: &str,
    stop_timeout_secs: u64,
) -> std::io::Result<()> {
    let _ = blocking_request(
        socket_path,
        "POST",
        &format!("/containers/{container_id}/stop?t={stop_timeout_secs}"),
    );
    blocking_request(
        socket_path,
        "DELETE",
        &format!("/containers/{container_id}?force=true"),
    )
    .map(|_| ())
}

/// Resolves `name` to a daemon-assigned container id via a blocking
/// `GET /containers/json?all=true&filters={"name":["^/<name>$"]}` — the `^/...$`
/// anchoring matches Docker's own convention for an exact-name filter (Docker's
/// `name` filter is substring-by-default; anchoring is what makes it exact). Returns
/// `None` on any failure (lookup error, no match, unparseable body) — the caller
/// treats that as "nothing to remove", matching `remove_by_name`'s best-effort,
/// not-found-is-fine contract.
fn blocking_find_container_id_by_name(socket_path: &std::path::Path, name: &str) -> Option<String> {
    let filters = format!(r#"{{"name":["^/{name}$"]}}"#);
    let path = format!(
        "/containers/json?all=true&filters={}",
        encode_query_value(&filters)
    );
    let body = blocking_get_body(socket_path, &path).ok()?;
    let entries: Vec<ListedEntry> = serde_json::from_slice(&body).ok()?;
    entries.into_iter().next().map(|e| e.id)
}

/// A minimal blocking HTTP GET over a blocking transport, returning the response
/// body — the read-the-body counterpart to [`blocking_request`] (which discards it),
/// needed by [`blocking_find_container_id_by_name`]. Honors `Content-Length` when
/// present, dechunks a `Transfer-Encoding: chunked` response (`GET
/// /containers/json` — this call's only caller — comes back chunked on every
/// dockerd/Docker Desktop build observed; empirically NOT the "Content-Length in
/// practice" case an earlier version of this function assumed, which made
/// `blocking_find_container_id_by_name` silently find nothing on every real
/// daemon), and otherwise falls back to reading until the peer closes (this client
/// always sends `Connection: close`, matching [`blocking_request`]'s own
/// assumption).
fn blocking_get_body(socket_path: &std::path::Path, path: &str) -> std::io::Result<Vec<u8>> {
    use std::io::{BufRead, BufReader, Read, Write};

    let mut stream =
        crate::stream::connect_blocking(socket_path, Duration::from_secs(stop_timeout_budget()))?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader.read_line(&mut status_line)?;

    let mut content_length: Option<usize> = None;
    let mut chunked = false;
    loop {
        let mut line = String::new();
        let n = reader.read_line(&mut line)?;
        if n == 0 || line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim();
            let value = value.trim();
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.parse::<usize>().ok();
            } else if name.eq_ignore_ascii_case("transfer-encoding") {
                chunked = value.to_ascii_lowercase().contains("chunked");
            }
        }
    }

    if chunked {
        return blocking_read_chunked_body(&mut reader);
    }

    let mut body = Vec::new();
    match content_length {
        Some(len) => {
            body.resize(len, 0);
            reader.read_exact(&mut body)?;
        }
        None => {
            reader.read_to_end(&mut body)?;
        }
    }
    Ok(body)
}

/// Dechunks an HTTP `Transfer-Encoding: chunked` body from `reader` — the blocking
/// counterpart to `client::read_chunked_body`, needed because [`blocking_get_body`]
/// runs on a plain OS thread with no Tokio runtime available. Same framing: a hex
/// chunk-size line, that many bytes, a trailing `\r\n`, repeated until a zero-size
/// chunk (optionally followed by trailer headers, drained and discarded) ends the
/// body.
fn blocking_read_chunked_body(
    reader: &mut std::io::BufReader<crate::stream::BlockingDockerStream>,
) -> std::io::Result<Vec<u8>> {
    use std::io::{BufRead, Read};

    let mut out = Vec::new();
    loop {
        let mut size_line = String::new();
        reader.read_line(&mut size_line)?;
        let size_str = size_line.trim().split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_str, 16).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "could not parse a chunk size from the Docker daemon's response: \
                     {size_line:?}"
                ),
            )
        })?;
        if size == 0 {
            // Drain trailer headers up to the final blank line, then stop.
            loop {
                let mut trailer = String::new();
                let n = reader.read_line(&mut trailer)?;
                if n == 0 || trailer == "\r\n" || trailer == "\n" {
                    break;
                }
            }
            break;
        }
        let mut chunk = vec![0u8; size];
        reader.read_exact(&mut chunk)?;
        out.extend_from_slice(&chunk);
        // Each chunk is followed by a trailing \r\n that isn't part of the payload.
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf)?;
    }
    Ok(out)
}

/// A minimal blocking HTTP/1.1 request over a blocking transport — the Drop-path
/// analogue of [`DockerClient::request`], deliberately NOT reusing any of that async
/// client's code (which is all [`crate::stream::DockerStream`]-shaped, tokio-based):
/// this runs on a plain OS thread with no Tokio runtime available, so it needs its own
/// blocking transport. Doesn't need to handle chunked responses — the
/// only calls made here (`stop`, `force=true remove`) return small `Content-Length`
/// (or empty) bodies in practice — so this reads until the peer closes and returns
/// whatever arrived, without trying to interpret framing at all.
fn blocking_request(
    socket_path: &std::path::Path,
    method: &str,
    path: &str,
) -> std::io::Result<()> {
    use std::io::{Read, Write};

    let mut stream =
        crate::stream::connect_blocking(socket_path, Duration::from_secs(stop_timeout_budget()))?;
    let request = format!("{method} {path} HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes())?;
    let mut discard = Vec::new();
    let _ = stream.read_to_end(&mut discard);
    Ok(())
}

/// A generous fixed budget for the blocking cleanup-path request's read timeout —
/// this path has no caller to propagate a timeout error to (it's best-effort, fired
/// from `Drop`), so it just needs to not hang the cleanup thread forever if the
/// daemon never responds.
fn stop_timeout_budget() -> u64 {
    STOP_TIMEOUT_SECS + 20
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_report_no_hardware_isolation_but_checkpoint_support() {
        let backend = DockerBackend::connecting_to_env();
        let caps = backend.capabilities();
        assert!(!caps.hardware_isolated, "containers share the host kernel");
        assert!(caps.checkpoint);
        assert!(
            !caps.checkpoint_restarts_workload,
            "an image commit leaves the running container undisturbed"
        );
    }

    /// A minimal, dependency-free temp-directory helper for the blocking-socket
    /// tests below — see the async test module's own `tempdir_shim` for why this
    /// crate hand-rolls one per test module rather than sharing it. Unix-only, like
    /// the tests that use it (see `blocking_get_body_dechunks_a_transfer_encoding_
    /// chunked_response`'s doc for why there is no Windows named-pipe counterpart
    /// here).
    #[cfg(unix)]
    mod blocking_tempdir_shim {
        use std::path::{Path, PathBuf};

        pub(super) struct TempDir(PathBuf);

        impl TempDir {
            pub(super) fn new() -> Self {
                static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
                let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let unique = format!("rzdbb-{:x}-{:x}", std::process::id() as u16, seq);
                let path = Path::new("/tmp").join(unique);
                std::fs::create_dir_all(&path).expect("create temp dir");
                TempDir(path)
            }

            pub(super) fn path(&self) -> &Path {
                &self.0
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }

    /// `blocking_get_body` must dechunk a `Transfer-Encoding: chunked` response —
    /// this is the real shape `GET /containers/json` comes back in on every
    /// dockerd/Docker Desktop build observed, not the `Content-Length` shape an
    /// earlier version of this function assumed, which made
    /// `blocking_find_container_id_by_name` (and therefore `remove_by_name`) find
    /// nothing on every real daemon. Unix-only: this drives `blocking_get_body`
    /// (and so `crate::stream::connect_blocking`) through a real unix-socket fixture
    /// server; there is no equivalent named-pipe *server* fixture, since faking the
    /// Windows pipe transport's actual I/O is exactly what this crate's Windows work
    /// deliberately does NOT unit-test (see `crate::client`'s test module for the same
    /// convention).
    #[cfg(unix)]
    #[test]
    fn blocking_get_body_dechunks_a_transfer_encoding_chunked_response() {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixListener;

        let dir = blocking_tempdir_shim::TempDir::new();
        let sock_path = dir.path().join("docker.sock");
        let listener = UnixListener::bind(&sock_path).expect("bind fixture socket");

        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept fixture connection");
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf); // drain the request line/headers.
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\n\
                      Content-Type: application/json\r\n\
                      Transfer-Encoding: chunked\r\n\
                      \r\n\
                      5\r\nhello\r\n\
                      6\r\n world\r\n\
                      0\r\n\r\n",
                )
                .expect("write fixture response");
        });

        let body =
            blocking_get_body(&sock_path, "/containers/json").expect("dechunking must succeed");
        assert_eq!(body, b"hello world");
        server.join().expect("fixture server thread must not panic");
    }

    /// The `Content-Length`-framed shape must keep working too — the dechunking
    /// addition must be additive, not a regression on the framing that already
    /// worked. Unix-only, same reason as the test above.
    #[cfg(unix)]
    #[test]
    fn blocking_get_body_honors_content_length_when_present() {
        use std::io::{Read, Write};
        use std::os::unix::net::UnixListener;

        let dir = blocking_tempdir_shim::TempDir::new();
        let sock_path = dir.path().join("docker.sock");
        let listener = UnixListener::bind(&sock_path).expect("bind fixture socket");

        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept fixture connection");
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\nhello world!!")
                .expect("write fixture response");
        });

        let body = blocking_get_body(&sock_path, "/containers/json")
            .expect("Content-Length read must succeed");
        assert_eq!(body, b"hello world!!");
        server.join().expect("fixture server thread must not panic");
    }

    #[test]
    fn is_port_bind_conflict_message_matches_known_phrasings() {
        assert!(is_port_bind_conflict_message(
            "driver failed programming external connectivity: address already in use"
        ));
        assert!(is_port_bind_conflict_message(
            "Bind for 0.0.0.0:6379 failed: port is already allocated"
        ));
        assert!(is_port_bind_conflict_message(
            "ALREADY ALLOCATED (case-insensitive)"
        ));
    }

    #[test]
    fn is_port_bind_conflict_message_negative_cases_do_not_match() {
        assert!(!is_port_bind_conflict_message("no such image"));
        assert!(!is_port_bind_conflict_message("container already stopped"));
        assert!(!is_port_bind_conflict_message(""));
    }

    #[test]
    fn split_repo_tag_defaults_to_latest_when_untagged() {
        assert_eq!(
            split_repo_tag("redis"),
            ("redis".to_string(), "latest".to_string())
        );
        assert_eq!(
            split_repo_tag("redis:8.6-alpine"),
            ("redis".to_string(), "8.6-alpine".to_string())
        );
    }

    #[test]
    fn split_repo_tag_handles_a_registry_host_with_a_port() {
        assert_eq!(
            split_repo_tag("localhost:5000/redis"),
            ("localhost:5000/redis".to_string(), "latest".to_string())
        );
        assert_eq!(
            split_repo_tag("localhost:5000/redis:8.6-alpine"),
            ("localhost:5000/redis".to_string(), "8.6-alpine".to_string())
        );
    }

    #[test]
    fn split_repo_tag_passes_through_a_digest_reference_with_no_tag() {
        assert_eq!(
            split_repo_tag("redis@sha256:abcdef"),
            ("redis@sha256:abcdef".to_string(), String::new())
        );
    }

    /// `build_create_body` returns a struct now (see `crate::json::CreateContainerBody`);
    /// these tests still assert on the actual bytes sent over the wire, serializing it
    /// exactly like `create()` does, so a field-rename typo or a `skip_serializing_if`
    /// slip is still caught here rather than only in `crate::json`'s own struct-shape
    /// tests.
    fn serialize(spec: &ContainerSpec) -> String {
        serde_json::to_string(&build_create_body(spec)).unwrap()
    }

    #[test]
    fn build_create_body_includes_port_bindings_on_loopback() {
        let mut spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        spec.ports.push(rightsize::model::PortBinding {
            host_port: 32768,
            guest_port: 6379,
        });
        let body = serialize(&spec);
        assert!(body.contains("\"HostIp\":\"127.0.0.1\""));
        assert!(body.contains("\"HostPort\":\"32768\""));
        assert!(body.contains("\"6379/tcp\""));
    }

    #[test]
    fn build_create_body_includes_the_run_id_label_and_extra_host() {
        let spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        let body = serialize(&spec);
        assert!(body.contains("\"dev.rightsize.run_id\":\"deadbeef\""));
        assert!(body.contains("host.docker.internal:host-gateway"));
    }

    #[test]
    fn build_create_body_honors_mount_read_only_flag() {
        let mut spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        // A plain `new` mount is read-write — the default — and `read_only()` is the
        // opt-in. Both spellings are pinned so neither default nor override can drift.
        spec.mounts
            .push(rightsize::model::FileMount::new("/host/a", "/guest/a"));
        spec.mounts
            .push(rightsize::model::FileMount::new("/host/b", "/guest/b").read_only());
        let body = serialize(&spec);
        assert!(body.contains("/host/a:/guest/a:rw"), "{body}");
        assert!(body.contains("/host/b:/guest/b:ro"), "{body}");
    }

    #[test]
    fn build_create_body_ignores_the_msb_only_spec_fields() {
        let plain = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");

        let mut with_msb_fields = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        with_msb_fields.disk_limit_mb = Some(2048);
        with_msb_fields.tmpfs_root_mb = Some(512);
        with_msb_fields.network_disabled = true;

        assert_eq!(serialize(&plain), serialize(&with_msb_fields));
    }

    #[test]
    fn build_create_body_omits_memory_when_unset_and_includes_it_when_set() {
        let spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        assert!(!serialize(&spec).contains("Memory"));

        let mut spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        spec.memory_limit_mb = Some(512);
        assert!(serialize(&spec).contains("\"Memory\":536870912"));
    }

    #[test]
    fn build_create_body_omits_cmd_when_command_is_none_and_includes_it_when_some() {
        let spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        assert!(!serialize(&spec).contains("\"Cmd\""));

        let mut spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        spec.command = Some(vec![
            "redis-server".to_string(),
            "--port".to_string(),
            "6379".to_string(),
        ]);
        let body = serialize(&spec);
        assert!(body.contains("\"Cmd\":[\"redis-server\",\"--port\",\"6379\"]"));
    }

    #[test]
    fn docker_backend_dials_a_unix_socket_never_a_tcp_host() {
        // Standing regression guard: this backend's transport must always be
        // a unix socket path, never a `host:port` TCP endpoint — the exact failure
        // mode a shared/bumped HTTP stack can cause if a dependency update
        // silently changes the default transport.
        let backend = DockerBackend::new(DockerClient::from_docker_host(None));
        let path = backend.socket_path_for_test();
        assert!(
            path.is_absolute(),
            "expected an absolute unix socket path, got {path:?}"
        );
        let path_str = path.to_string_lossy();
        assert!(
            !path_str.contains(':'),
            "a unix socket path must not look like a host:port TCP address — got {path_str}"
        );

        let backend_env =
            DockerBackend::new(DockerClient::from_docker_host(Some("tcp://127.0.0.1:2375")));
        let fallback_path = backend_env.socket_path_for_test();
        assert!(
            !fallback_path.to_string_lossy().contains("2375"),
            "a tcp:// DOCKER_HOST must not leak a TCP port into this backend's transport — \
             got {fallback_path:?}"
        );
    }

    #[test]
    fn keep_alive_spec_gets_the_reuse_label_instead_of_the_run_id_label() {
        let mut spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        spec.keep_alive = true;
        let body = serialize(&spec);
        assert!(body.contains("\"dev.rightsize.reuse\""), "{body}");
        assert!(!body.contains("\"dev.rightsize.run_id\""), "{body}");
    }

    #[test]
    fn non_keep_alive_spec_still_gets_the_run_id_label_only() {
        let spec = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        let body = serialize(&spec);
        assert!(
            body.contains("\"dev.rightsize.run_id\":\"deadbeef\""),
            "{body}"
        );
        assert!(!body.contains("\"dev.rightsize.reuse\""), "{body}");
    }

    #[test]
    fn watchdog_kill_command_is_a_plain_docker_rm_dash_f() {
        let backend = DockerBackend::new(DockerClient::from_docker_host(None));
        assert_eq!(
            backend.watchdog_kill_command(),
            vec!["docker".to_string(), "rm".to_string(), "-f".to_string()]
        );
    }

    #[test]
    fn watchdog_network_kill_command_is_docker_network_rm() {
        let backend = DockerBackend::new(DockerClient::from_docker_host(None));
        assert_eq!(
            backend.watchdog_network_kill_command(),
            vec![
                "docker".to_string(),
                "network".to_string(),
                "rm".to_string()
            ]
        );
    }

    #[test]
    fn reuse_label_value_extracts_the_hash_straight_from_an_rz_reuse_name() {
        let spec = ContainerSpec::new("rz-reuse-799aad5a3338", "redis:7-alpine", "deadbeef");
        assert_eq!(reuse_label_value(&spec), "799aad5a3338");
    }

    #[test]
    fn reuse_label_value_is_twelve_hex_chars_and_deterministic() {
        let spec_a = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        let spec_b = ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef");
        let spec_c = ContainerSpec::new("rz-x-1", "redis:8.6-alpine", "deadbeef");
        let a = reuse_label_value(&spec_a);
        let b = reuse_label_value(&spec_b);
        let c = reuse_label_value(&spec_c);
        assert_eq!(a.len(), 12);
        assert!(a.chars().all(|ch| ch.is_ascii_hexdigit()));
        assert_eq!(a, b, "same spec.name must hash the same every time");
        assert_ne!(a, c, "a different spec.name must hash differently");
    }

    // --- remove_network's cache-miss fallback -------------------------------------
    //
    // The reaping sweep's whole point is removing resources a DIFFERENT (dead)
    // process created — a network this backend INSTANCE never itself called
    // `ensure_network` for, so `network_ids` (populated only by THIS instance's own
    // `ensure_network_get_id` calls) has nothing cached for it. These tests drive
    // `DockerBackend` against a real (fixture) unix socket rather than a fake
    // `SandboxBackend`, since the bug lives in the daemon-call sequence itself, not
    // in anything a fake could stand in for.

    // The rest of this module's fixtures bind a real unix listener (`multi_request_
    // fixture` below), so they — and the tests that drive `DockerBackend` through
    // them — are unix-only for the same reason `crate::client`'s and `crate::frames`'s
    // own socket fixtures are: faking the Windows pipe transport's actual I/O is
    // exactly what this crate's Windows work deliberately does NOT unit-test.
    #[cfg(unix)]
    use std::sync::Arc;
    #[cfg(unix)]
    use tokio::net::UnixListener;

    /// A minimal, dependency-free temp-directory helper — duplicated from
    /// `crate::client`'s own test-only `tempdir_shim` rather than shared, since that
    /// one is nested inside a private `#[cfg(test)] mod tests` in a different file
    /// and this crate's own convention is small hand-rolled scaffolding per test
    /// module over an extra dependency or cross-module test plumbing.
    #[cfg(unix)]
    mod tempdir_shim {
        use std::path::{Path, PathBuf};

        pub(super) struct TempDir(PathBuf);

        impl TempDir {
            /// Rooted at `/tmp`, not `std::env::temp_dir()` — see
            /// `crate::client`'s `tempdir_shim` for why (`AF_UNIX`'s `SUN_LEN` cap).
            pub(super) fn new() -> Self {
                static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
                let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let unique = format!("rzdb-{:x}-{:x}", std::process::id() as u16, seq);
                let path = Path::new("/tmp").join(unique);
                std::fs::create_dir_all(&path).expect("create temp dir");
                TempDir(path)
            }

            pub(super) fn path(&self) -> &Path {
                &self.0
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }

    /// Spawns a fixture unix-socket "server" that accepts connections IN SEQUENCE —
    /// one per entry in `responses` — replying with the given canned bytes and
    /// recording each request's status line, so a test can drive (and assert on) a
    /// `DockerBackend` method that makes more than one sequential HTTP call. Unlike
    /// `crate::client`'s own single-request fixture, this one is needed because
    /// `remove_network`'s cache-miss fallback issues a `GET` lookup THEN a `DELETE`
    /// — two separate connections, since this client never keeps one alive across
    /// calls (see `crate::client`'s module doc).
    #[cfg(unix)]
    async fn multi_request_fixture(
        responses: Vec<Vec<u8>>,
    ) -> (DockerClient, tempdir_shim::TempDir, Arc<Mutex<Vec<String>>>) {
        let dir = tempdir_shim::TempDir::new();
        let sock_path = dir.path().join("docker.sock");
        let listener = UnixListener::bind(&sock_path).expect("bind fixture socket");
        let received: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received.clone();
        tokio::spawn(async move {
            for response in responses {
                if let Ok((mut conn, _)) = listener.accept().await {
                    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
                    let mut buf = vec![0u8; 4096];
                    let n = tokio::time::timeout(
                        std::time::Duration::from_millis(200),
                        conn.read(&mut buf),
                    )
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .unwrap_or(0);
                    let request_line = String::from_utf8_lossy(&buf[..n])
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    received_clone.lock().unwrap().push(request_line);
                    let _ = conn.write_all(&response).await;
                    let _ = conn.shutdown().await;
                }
            }
        });
        (DockerClient::at_socket(sock_path), dir, received)
    }

    /// A canned `200 OK` response listing exactly one network whose daemon id is
    /// `id` — the shape `GET /networks?filters=...` returns.
    #[cfg(unix)]
    fn network_list_response(id: &str) -> Vec<u8> {
        let payload = format!(r#"[{{"Id":"{id}"}}]"#);
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        )
        .into_bytes()
    }

    /// A canned `200 OK` response listing exactly one container whose daemon id is
    /// `id` — the shape `GET /containers/json?filters=...` returns (same `[{"Id":
    /// "..."}]` shape as [`network_list_response`], reused as-is by
    /// [`ListedEntry`], but named separately since it stands for a different
    /// daemon resource at each call site).
    #[cfg(unix)]
    fn container_list_response(id: &str) -> Vec<u8> {
        network_list_response(id)
    }

    /// A canned `200 OK` empty-array response — "nothing matched this filter."
    #[cfg(unix)]
    fn empty_list_response() -> Vec<u8> {
        let payload = "[]";
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            payload.len(),
            payload
        )
        .into_bytes()
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn find_running_returns_a_handle_when_the_daemon_lists_a_running_match() {
        let (client, _dir, received) =
            multi_request_fixture(vec![container_list_response("daemon-c-1")]).await;
        let backend = DockerBackend::new(client);
        let spec = ContainerSpec::new("rz-reuse-799aad5a3338", "redis:7-alpine", "deadbeef");

        let found = backend
            .find_running(&spec)
            .await
            .unwrap()
            .expect("must find the running container");
        assert_eq!(found.id(), "daemon-c-1");

        let requests = received.lock().unwrap().clone();
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].starts_with("GET /containers/json?filters="),
            "{requests:?}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn find_running_returns_none_when_nothing_matches() {
        let (client, _dir, _received) = multi_request_fixture(vec![empty_list_response()]).await;
        let backend = DockerBackend::new(client);
        let spec = ContainerSpec::new("rz-reuse-799aad5a3338", "redis:7-alpine", "deadbeef");

        assert!(backend.find_running(&spec).await.unwrap().is_none());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn create_maps_a_409_conflict_to_the_typed_name_conflict_error() {
        // Two canned responses: `create()` first checks the image is present
        // (`pull_if_missing`'s `GET /images/{name}/json`, answered `200 OK` so it
        // never attempts a pull), THEN issues the actual `POST /containers/create`
        // that this test cares about, answered with the 409 conflict.
        let (client, _dir, _received) = multi_request_fixture(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_vec(),
            b"HTTP/1.1 409 Conflict\r\nContent-Length: 44\r\n\r\n{\"message\":\"Conflict. Name already in use.\"}".to_vec(),
        ])
        .await;
        let backend = DockerBackend::new(client);
        let spec = ContainerSpec::new("rz-reuse-799aad5a3338", "redis:7-alpine", "deadbeef");

        let err = match backend.create(spec).await {
            Ok(_) => panic!("a 409 must surface as an error"),
            Err(e) => e,
        };
        assert!(
            matches!(err, rightsize::error::RightsizeError::NameConflict { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn remove_network_falls_back_to_a_live_lookup_when_the_in_memory_cache_never_saw_it() {
        let (client, _dir, received) = multi_request_fixture(vec![
            network_list_response("daemon-net-9"),
            b"HTTP/1.1 204 No Content\r\n\r\n".to_vec(),
        ])
        .await;
        let backend = DockerBackend::new(client);

        // No prior `ensure_network` call on THIS instance — `network_ids` starts
        // empty, exactly as it would for a process reaping a DIFFERENT (dead)
        // process's leftover network.
        backend.remove_network("rz-net-deadrun").await.unwrap();

        let requests = received.lock().unwrap().clone();
        assert_eq!(
            requests.len(),
            2,
            "a cache miss must fall back to a live GET lookup, then DELETE the id it \
             finds — got: {requests:?}"
        );
        assert!(
            requests[0].starts_with("GET /networks?filters="),
            "{requests:?}"
        );
        assert!(
            requests[1].starts_with("DELETE /networks/daemon-net-9 "),
            "the DELETE must target the id the live lookup resolved, not the \
             rightsize-side network name: {requests:?}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn remove_network_uses_the_cached_id_without_a_lookup_when_this_instance_created_it() {
        // Only ONE canned response: if this test needed a second connection (a GET
        // lookup it shouldn't be making), the fixture would have nothing left to
        // accept and the DELETE call would hang until the client's own timeout —
        // a real failure signal, not a false pass.
        let (client, _dir, received) =
            multi_request_fixture(vec![b"HTTP/1.1 204 No Content\r\n\r\n".to_vec()]).await;
        let backend = DockerBackend::new(client);
        backend
            .network_ids
            .lock()
            .unwrap()
            .insert("rz-net-ownrun".to_string(), "daemon-net-owned".to_string());

        backend.remove_network("rz-net-ownrun").await.unwrap();

        let requests = received.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(
            requests[0].starts_with("DELETE /networks/daemon-net-owned "),
            "{requests:?}"
        );
    }

    // --- create_checkpoint (checkpoint) ---------------------------------------

    #[tokio::test]
    #[cfg(unix)]
    async fn create_checkpoint_posts_commit_with_the_split_repo_and_tag_and_returns_the_ref() {
        let (client, _dir, received) = multi_request_fixture(vec![
            b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\n{}".to_vec(),
        ])
        .await;
        let backend = DockerBackend::new(client);
        let handle = Handle {
            id: "daemon-c-checkpoint".to_string(),
            spec: ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef"),
        };

        let checkpoint_ref = backend
            .create_checkpoint(&handle, "abc123def456")
            .await
            .expect("commit must succeed on a 201");
        assert_eq!(checkpoint_ref, "rightsize/checkpoint:abc123def456");

        let requests = received.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(
            requests[0].starts_with("POST /commit?container=daemon-c-checkpoint"),
            "{requests:?}"
        );
        assert!(
            requests[0].contains("repo=rightsize%2Fcheckpoint"),
            "{requests:?}"
        );
        assert!(requests[0].contains("tag=abc123def456"), "{requests:?}");
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn create_checkpoint_surfaces_a_daemon_error_as_a_backend_error() {
        let (client, _dir, _received) = multi_request_fixture(vec![
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 23\r\n\r\n{\"message\":\"no such\"}"
                .to_vec(),
        ])
        .await;
        let backend = DockerBackend::new(client);
        let handle = Handle {
            id: "missing-container".to_string(),
            spec: ContainerSpec::new("rz-x-0", "redis:8.6-alpine", "deadbeef"),
        };

        let err = backend
            .create_checkpoint(&handle, "abc123def456")
            .await
            .expect_err("a 404 must surface as an error");
        assert!(
            matches!(err, rightsize::error::RightsizeError::Backend(_)),
            "{err}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn remove_checkpoint_deletes_the_image_and_treats_not_found_as_success() {
        let (client, _dir, received) = multi_request_fixture(vec![
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\n\r\n{}".to_vec(),
        ])
        .await;
        let backend = DockerBackend::new(client);

        backend
            .remove_checkpoint("rightsize/checkpoint:abc123def456")
            .await
            .expect("a 404 (already gone) must be treated as success");

        let requests = received.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(
            requests[0].starts_with("DELETE /images/rightsize/checkpoint:abc123def456"),
            "{requests:?}"
        );
    }

    // --- has_checkpoint (named checkpoints' existence probe) --------------------

    #[tokio::test]
    #[cfg(unix)]
    async fn has_checkpoint_returns_true_on_a_200() {
        let (client, _dir, received) = multi_request_fixture(vec![
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_vec(),
        ])
        .await;
        let backend = DockerBackend::new(client);

        let exists = backend
            .has_checkpoint("rightsize/checkpoint:abc123def456")
            .await
            .expect("a 200 must resolve to Ok(true)");
        assert!(exists);

        let requests = received.lock().unwrap().clone();
        assert_eq!(requests.len(), 1, "{requests:?}");
        assert!(
            requests[0].starts_with("GET /images/rightsize/checkpoint:abc123def456/json"),
            "{requests:?}"
        );
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn has_checkpoint_returns_false_on_a_404() {
        let (client, _dir, _received) = multi_request_fixture(vec![
            b"HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\n\r\n{}".to_vec(),
        ])
        .await;
        let backend = DockerBackend::new(client);

        let exists = backend
            .has_checkpoint("rightsize/checkpoint:gone")
            .await
            .expect("a 404 must resolve to Ok(false), not an error");
        assert!(!exists);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn has_checkpoint_surfaces_any_other_status_as_an_error() {
        let (client, _dir, _received) = multi_request_fixture(vec![
            b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 15\r\n\r\n{\"message\":\"x\"}"
                .to_vec(),
        ])
        .await;
        let backend = DockerBackend::new(client);

        let err = backend
            .has_checkpoint("rightsize/checkpoint:abc123def456")
            .await
            .expect_err("a probe failure must surface, never resolve to Ok(false)");
        assert!(
            matches!(err, rightsize::error::RightsizeError::Backend(_)),
            "{err}"
        );
    }

    // --- docker cp argv construction -------------------------------------------

    #[test]
    fn docker_cp_in_args_puts_the_host_path_first_and_container_id_colon_path_second() {
        assert_eq!(
            docker_cp_in_args("daemon-id-1", Path::new("/host/src.txt"), "/guest/dst.txt"),
            vec!["cp", "/host/src.txt", "daemon-id-1:/guest/dst.txt"]
        );
    }

    #[test]
    fn docker_cp_out_args_puts_the_container_id_colon_path_first_and_host_path_second() {
        assert_eq!(
            docker_cp_out_args("daemon-id-1", "/guest/src.txt", Path::new("/host/dst.txt")),
            vec!["cp", "daemon-id-1:/guest/src.txt", "/host/dst.txt"]
        );
    }

    // --- checkpoint archive argv construction -----------------------------------

    #[test]
    fn docker_save_args_spelling() {
        assert_eq!(
            docker_save_args(
                "rightsize/checkpoint:abc123def456",
                Path::new("/tmp/cp.archive-payload")
            ),
            vec![
                "save",
                "-o",
                "/tmp/cp.archive-payload",
                "rightsize/checkpoint:abc123def456"
            ]
        );
    }

    #[test]
    fn docker_load_args_spelling() {
        assert_eq!(
            docker_load_args(Path::new("/tmp/cp.archive-payload")),
            vec!["load", "-i", "/tmp/cp.archive-payload"]
        );
    }
}
