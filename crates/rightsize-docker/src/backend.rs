//! `DockerBackend`: maps `SandboxBackend` onto exactly the daemon HTTP endpoints this
//! backend needs, over the hand-rolled unix-socket client and the frame demuxer. This
//! is also the correctness oracle other backends are checked against, since Docker
//! enforces semantics (read-only mounts, native networks) microsandbox only emulates.
//!
//! **`Handle` has no per-container mutable state** (contrast microsandbox's
//! `HandleState` map): every operation here is a stateless HTTP call keyed by the
//! daemon-assigned container id already carried on the handle, so there's nothing to
//! look up in a side table.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::Duration;

use rightsize::backend::{FollowHandle, SandboxBackend, SandboxHandle};
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

/// The label every container this backend creates carries, so `close()`/the reaper
/// can find exactly this run's containers without touching anyone else's. This is
/// a cross-process wire format, not Rust API surface, so it doesn't need to match
/// this crate's own naming conventions.
const RUN_ID_LABEL_KEY: &str = "dev.rightsize.run_id";

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
}

/// Builds the `POST /containers/create` JSON body: `Image`, `Env[]`, `Cmd[]?`,
/// `ExposedPorts{}`, `Labels`, and a `HostConfig` carrying port bindings (bound to
/// `127.0.0.1` — never the wildcard address), read-only/read-write binds, the
/// `host.docker.internal:host-gateway` extra host (lets a container reach services
/// running on the host, e.g. a test's own HTTP server, the same way it would under a
/// real Docker Desktop install), and an optional memory cap.
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

    CreateContainerBody {
        image: spec.image.clone(),
        env,
        cmd: spec.command.clone(),
        exposed_ports,
        labels: BTreeMap::from([(RUN_ID_LABEL_KEY.to_string(), spec.run_id.clone())]),
        host_config: HostConfig {
            port_bindings,
            binds,
            extra_hosts: vec!["host.docker.internal:host-gateway".to_string()],
            memory: spec.memory_limit_mb.map(|mb| mb * 1024 * 1024),
        },
    }
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

    async fn create(&self, spec: ContainerSpec) -> Result<Box<dyn SandboxHandle>> {
        self.pull_if_missing(&spec.image).await?;

        let body = build_create_body(&spec);
        let body = serde_json::to_string(&body)
            .expect("CreateContainerBody has no non-serializable fields");
        let path = format!("/containers/create?name={}", encode_query_value(&spec.name));
        let resp = self.client.request("POST", &path, Some(&body)).await?;
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
        let daemon_id = self
            .network_ids
            .lock()
            .expect("network_ids mutex poisoned")
            .remove(network_id);
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
}

/// The `cleanup_sync` path's blocking counterpart to `stop`+`remove`: issues a plain
/// `POST /containers/{id}/stop?t=` then `DELETE /containers/{id}?force=true` over a
/// blocking `std::os::unix::net::UnixStream` — no Tokio, since this runs on the
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

/// A minimal blocking HTTP/1.1 request over a blocking unix socket — the Drop-path
/// analogue of [`DockerClient::request`], deliberately NOT reusing any of that async
/// client's code (which is all `tokio::net::UnixStream`-shaped): this runs on a plain
/// OS thread with no Tokio runtime available, so it needs its own blocking transport.
/// Doesn't need to handle chunked responses — the
/// only calls made here (`stop`, `force=true remove`) return small `Content-Length`
/// (or empty) bodies in practice — so this reads until the peer closes and returns
/// whatever arrived, without trying to interpret framing at all.
fn blocking_request(
    socket_path: &std::path::Path,
    method: &str,
    path: &str,
) -> std::io::Result<()> {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    let mut stream = UnixStream::connect(socket_path)?;
    stream.set_read_timeout(Some(Duration::from_secs(stop_timeout_budget())))?;
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
        spec.mounts
            .push(rightsize::model::FileMount::new("/host/a", "/guest/a"));
        spec.mounts
            .push(rightsize::model::FileMount::new("/host/b", "/guest/b").read_write());
        let body = serialize(&spec);
        assert!(body.contains("/host/a:/guest/a:ro"));
        assert!(body.contains("/host/b:/guest/b:rw"));
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
}
