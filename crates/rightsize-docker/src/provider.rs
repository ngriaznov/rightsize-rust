//! [`DockerBackendProvider`]: the discoverable factory `rightsize::backends::resolve`
//! picks among. Supported exactly when `GET /version` succeeds against the daemon
//! this process would otherwise talk to AND that daemon reports `"Os":"linux"` — this
//! backend only knows how to run Linux containers, so a daemon that answers but
//! serves Windows containers is correctly "not supported" here rather than a false
//! positive. Docker Desktop on macOS and Windows reports `"linux"` too (its daemon
//! runs inside a VM/WSL2), so this check is a no-op there in practice; no
//! Docker/Podman/Colima running at all is still the common "not supported" case on a
//! microVM-capable host that just prefers msb anyway.

use rightsize::backend::{BackendProvider, SandboxBackend};
use rightsize::error::Result;

use crate::backend::DockerBackend;
use crate::client::DockerClient;

/// The Docker backend's [`BackendProvider`]. Priority 10 — lower than msb's 20, so a
/// microVM-capable host prefers the microVM by default; Docker is the fallback for
/// hosts without one (Intel Macs, Windows, no `/dev/kvm`), and doubles as the
/// contract suite's correctness oracle.
pub struct DockerBackendProvider;

impl BackendProvider for DockerBackendProvider {
    fn name(&self) -> &str {
        "docker"
    }

    fn priority(&self) -> u32 {
        10
    }

    fn is_supported(&self) -> bool {
        // `BackendProvider::is_supported` is a plain synchronous fn — it can run
        // before any Tokio runtime exists yet (early process startup), or from
        // *inside* one already (a `#[tokio::test]` resolving a backend), and
        // `Runtime::block_on` panics in the latter case ("cannot start a runtime from
        // within a runtime"). Rather than juggle "is there already a runtime" logic,
        // this probe is a plain blocking GET /version over
        // [`crate::stream::BlockingDockerStream`] (a unix socket on unix, Docker
        // Desktop's named pipe on Windows) — the same blocking-transport shape
        // `DockerBackend::cleanup_sync` already uses for its own no-Tokio-in-context
        // constraint, and the honest fit for a synchronous trait method that must work
        // in either context. Windows runs the same real connect + `GET /version` as
        // unix, not merely a check that the pipe path exists (a stale or orphaned pipe
        // existing without a daemon actually answering behind it would otherwise
        // report "supported" for a daemon that isn't really there) — bounded to the
        // same 2-second budget on both platforms, though by different means: unix gets
        // it from the socket's own `SO_RCVTIMEO`/`SO_SNDTIMEO`, Windows from
        // `crate::stream::run_with_deadline` wrapping the whole round trip, since a
        // Windows named-pipe handle has no per-read/write deadline knob of its own.
        // The response body's `"Os"` field must also read `"linux"` (case-
        // insensitively) — this backend only runs Linux containers, so a reachable
        // daemon serving Windows containers is not "supported" either, and any
        // failure along the way (connect error, timeout, a body that doesn't parse)
        // degrades to `false` rather than panicking or bubbling an error out of this
        // trait method.
        probe_linux_daemon(DockerClient::from_env().socket_path())
    }

    fn unsupported_reason(&self) -> String {
        "no reachable Docker-API socket (Docker/Podman/Colima not running?)".to_string()
    }

    fn create(&self) -> Result<Box<dyn SandboxBackend>> {
        Ok(Box::new(DockerBackend::connecting_to_env()))
    }
}

/// A minimal blocking `GET /version`, true only when the response is 2xx AND its JSON
/// body's `"Os"` field is `"linux"` (case-insensitively) — see
/// [`DockerBackendProvider::is_supported`]'s doc for why this is blocking std I/O
/// rather than a reused async request, and for why the body must say `"linux"` too.
/// `target` is a unix socket path on unix, a named pipe path on Windows —
/// [`crate::stream::connect_blocking`] is what actually dispatches on that.
///
/// On Windows this runs [`probe_linux_daemon_once`] under
/// [`crate::stream::run_with_deadline`] rather than calling it directly: a Windows
/// named-pipe handle has no per-read/write deadline of its own (see
/// `connect_blocking`'s Windows doc), so without this wrapper a pipe that accepts the
/// connection but sits behind a wedged daemon would hang this probe — and so
/// `is_supported()` — indefinitely. Unix already gets an equivalent bound for free
/// from `connect_blocking`'s `SO_RCVTIMEO`/`SO_SNDTIMEO`, so it calls
/// `probe_linux_daemon_once` straight, unchanged from before this module existed.
fn probe_linux_daemon(target: &std::path::Path) -> bool {
    #[cfg(unix)]
    {
        probe_linux_daemon_once(target).unwrap_or(false)
    }
    #[cfg(windows)]
    {
        let target = target.to_path_buf();
        crate::stream::run_with_deadline(std::time::Duration::from_secs(2), move || {
            probe_linux_daemon_once(&target)
        })
        .unwrap_or(false)
    }
}

/// The actual connect-plus-`GET /version`-plus-parse round trip behind
/// [`probe_linux_daemon`], factored out so both the direct (unix) and
/// deadline-bounded (Windows) call sites run identical logic.
fn probe_linux_daemon_once(target: &std::path::Path) -> std::io::Result<bool> {
    use std::io::{Read, Write};
    use std::time::Duration;

    let mut stream = crate::stream::connect_blocking(target, Duration::from_secs(2))?;
    stream.write_all(b"GET /version HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n")?;
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    Ok(response_reports_linux(&response))
}

/// True only when `response` (the raw bytes of an HTTP response, status line through
/// body) is a 2xx reply whose JSON body deserializes into a
/// [`crate::json::VersionResponse`] with `os` equal to `"linux"`, case-insensitively.
/// Never panics: a non-2xx status, a response with no blank-line header/body
/// separator, a malformed chunked body, or a body that doesn't deserialize all fall
/// through to `false` the same way a connect failure or timeout does one layer up in
/// [`probe_linux_daemon`].
fn response_reports_linux(response: &[u8]) -> bool {
    let Some(header_end) = response.windows(4).position(|w| w == b"\r\n\r\n") else {
        return false;
    };
    let head = String::from_utf8_lossy(&response[..header_end]);
    let mut lines = head.lines();
    let is_2xx_status = lines
        .next()
        .and_then(|status_line| status_line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .is_some_and(|code| (200..300).contains(&code));
    if !is_2xx_status {
        return false;
    }

    let mut chunked = false;
    let mut content_length = None;
    for (name, value) in lines.filter_map(|line| line.split_once(':')) {
        let (name, value) = (name.trim(), value.trim());
        if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = value.to_ascii_lowercase().contains("chunked");
        } else if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse::<usize>().ok();
        }
    }

    // The request sent `Connection: close` and `probe_linux_daemon_once` read to EOF,
    // but the body can still be chunked: Docker Engine 29.x answers `GET /version`
    // with `Transfer-Encoding: chunked`, while older engines send it plain.
    let raw_body = &response[header_end + 4..];
    let body: std::borrow::Cow<'_, [u8]> = if chunked {
        let mut reader = raw_body;
        match crate::backend::blocking_read_chunked_body(&mut reader) {
            Ok(body) => body.into(),
            Err(_) => return false,
        }
    } else {
        match content_length {
            Some(len) => match raw_body.get(..len) {
                Some(body) => body.into(),
                None => return false,
            },
            None => raw_body.into(),
        }
    };
    serde_json::from_slice::<crate::json::VersionResponse>(&body)
        .is_ok_and(|version| version.os.eq_ignore_ascii_case("linux"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_priority_are_pinned() {
        let provider = DockerBackendProvider;
        assert_eq!(provider.name(), "docker");
        assert_eq!(provider.priority(), 10);
    }

    #[test]
    fn unsupported_reason_names_the_daemon_socket() {
        let provider = DockerBackendProvider;
        let reason = provider.unsupported_reason();
        assert!(reason.to_lowercase().contains("docker"));
    }

    #[test]
    fn response_reports_linux_is_true_for_a_2xx_linux_version_body() {
        let response =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"Os\":\"linux\"}";
        assert!(response_reports_linux(response));
    }

    #[test]
    fn response_reports_linux_is_case_insensitive_on_the_os_value() {
        let response =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"Os\":\"Linux\"}";
        assert!(response_reports_linux(response));
    }

    #[test]
    fn response_reports_linux_is_false_for_a_windows_version_body() {
        let response =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"Os\":\"windows\"}";
        assert!(!response_reports_linux(response));
    }

    #[test]
    fn response_reports_linux_is_false_for_a_body_missing_the_os_field() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}";
        assert!(!response_reports_linux(response));
    }

    #[test]
    fn response_reports_linux_is_false_for_a_non_2xx_status() {
        let response = b"HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\r\n{\"Os\":\"linux\"}";
        assert!(!response_reports_linux(response));
    }

    #[test]
    fn response_reports_linux_honors_content_length() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 14\r\n\r\n{\"Os\":\"linux\"}";
        assert!(response_reports_linux(response));
    }

    /// The response shape Docker Engine 29.8 (Docker Desktop 4.91) actually sends for
    /// `GET /version`: these headers verbatim (including `Transfer-Encoding: chunked`
    /// and the `Ostype` header), and a real-shaped JSON body split into `chunk_sizes`
    /// chunks (the last chunk takes whatever is left).
    fn chunked_version_response(os: &str, chunk_sizes: &[usize]) -> Vec<u8> {
        let body = format!(
            "{{\"Platform\":{{\"Name\":\"Docker Desktop 4.91.0 (239619)\"}},\"Version\":\"29.8.0\",\
             \"ApiVersion\":\"1.56\",\"MinAPIVersion\":\"1.40\",\"Os\":\"{os}\",\"Arch\":\"arm64\",\
             \"KernelVersion\":\"6.12.54-linuxkit\",\"BuildTime\":\"2026-09-02T10:11:12.000000000+00:00\"}}"
        );
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nApi-Version: 1.56\r\nContent-Type: application/json\r\n\
             Date: Thu, 24 Sep 2026 07:02:11 GMT\r\nDocker-Experimental: false\r\n\
             Ostype: {os}\r\nServer: Docker/29.8.0 ({os})\r\nConnection: close\r\n\
             Transfer-Encoding: chunked\r\n\r\n"
        )
        .into_bytes();
        let mut rest = body.as_bytes();
        for (i, &size) in chunk_sizes.iter().enumerate() {
            let size = if i + 1 == chunk_sizes.len() {
                rest.len()
            } else {
                size
            };
            let (chunk, tail) = rest.split_at(size);
            response.extend_from_slice(format!("{:x}\r\n", chunk.len()).as_bytes());
            response.extend_from_slice(chunk);
            response.extend_from_slice(b"\r\n");
            rest = tail;
        }
        response.extend_from_slice(b"0\r\n\r\n");
        response
    }

    #[test]
    fn response_reports_linux_is_true_for_a_chunked_linux_version_body() {
        assert!(response_reports_linux(&chunked_version_response(
            "linux",
            &[0]
        )));
    }

    #[test]
    fn response_reports_linux_is_true_when_the_chunked_body_spans_several_chunks() {
        assert!(response_reports_linux(&chunked_version_response(
            "linux",
            &[17, 40, 0]
        )));
    }

    #[test]
    fn response_reports_linux_is_false_for_a_chunked_windows_version_body() {
        assert!(!response_reports_linux(&chunked_version_response(
            "windows",
            &[0]
        )));
    }

    #[test]
    fn response_reports_linux_is_false_for_a_malformed_chunk_size() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n{\"Os\":\"linux\"}\r\n0\r\n\r\n";
        assert!(!response_reports_linux(response));
    }

    /// A minimal, dependency-free temp-directory helper for the blocking-socket
    /// fixtures below — see `crate::client`'s own `tempdir_shim` (and `crate::backend`'s
    /// `blocking_tempdir_shim`) for why this crate hand-rolls one per test module
    /// rather than sharing it. Unix-only, like the fixtures that use it.
    #[cfg(unix)]
    mod blocking_tempdir_shim {
        use std::path::{Path, PathBuf};

        pub(super) struct TempDir(PathBuf);

        impl TempDir {
            pub(super) fn new() -> Self {
                static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
                let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let unique = format!("rzdp-{:x}-{:x}", std::process::id() as u16, seq);
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

    /// Red-proof case (a): a fixture daemon whose `GET /version` reports
    /// `"Os":"linux"` must be reported supported end to end, through the real
    /// blocking-socket probe (not just the pure body-parsing helper tested above).
    #[cfg(unix)]
    #[test]
    fn probe_linux_daemon_is_true_when_the_fixture_daemon_reports_linux() {
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
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n\
                      {\"Version\":\"27.3.1\",\"Os\":\"linux\",\"Arch\":\"amd64\"}",
                )
                .expect("write fixture response");
        });

        assert!(probe_linux_daemon(&sock_path));
        server.join().expect("fixture server thread must not panic");
    }

    /// Red-proof case (b): a fixture daemon whose `GET /version` reports
    /// `"Os":"windows"` — a real, reachable, Windows-containers `dockerd` — must NOT
    /// be reported supported, even though it answers the probe just fine. This is
    /// the case a status-only `GET /_ping` check would get wrong.
    #[cfg(unix)]
    #[test]
    fn probe_linux_daemon_is_false_when_the_fixture_daemon_reports_windows() {
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
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n\
                      {\"Version\":\"27.3.1\",\"Os\":\"windows\"}",
                )
                .expect("write fixture response");
        });

        assert!(!probe_linux_daemon(&sock_path));
        server.join().expect("fixture server thread must not panic");
    }

    /// A fixture daemon answering `GET /version` the way Docker Engine 29.8 does, with a
    /// chunked body, must be reported supported end to end through the real
    /// blocking-socket probe.
    #[cfg(unix)]
    #[test]
    fn probe_linux_daemon_is_true_when_the_fixture_daemon_answers_chunked() {
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
                .write_all(&chunked_version_response("linux", &[0]))
                .expect("write fixture response");
        });

        assert!(probe_linux_daemon(&sock_path));
        server.join().expect("fixture server thread must not panic");
    }

    /// Red-proof case (c): no daemon behind the socket at all (nothing ever bound
    /// it) means the connect itself fails — this must degrade to "not supported"
    /// rather than panicking, the same path a Windows timeout takes one layer up in
    /// [`probe_linux_daemon`].
    #[cfg(unix)]
    #[test]
    fn probe_linux_daemon_is_false_when_the_socket_has_no_listener() {
        let dir = blocking_tempdir_shim::TempDir::new();
        let sock_path = dir.path().join("docker.sock"); // deliberately never bound.

        assert!(!probe_linux_daemon(&sock_path));
    }
}
