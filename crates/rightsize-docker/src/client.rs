//! A from-scratch HTTP/1.1 client over `tokio::net::UnixStream` — the entire reason
//! this backend hand-rolls its own transport: sharing an HTTP stack with
//! whatever the consuming project also depends on can, in practice, misroute a Docker
//! client onto TCP 2375 instead of the daemon's unix socket (a real class of
//! regression seen when a shared HTTP stack changes its defaults on a version bump).
//! This client only ever opens `tokio::net::UnixStream`s — see
//! `docker_backend_dials_a_unix_socket_never_a_tcp_host` in `crate::backend`'s tests
//! for the standing regression guard.
//!
//! One request/response cycle = one connection: dial the socket, write the request,
//! read the status line + headers, then read the body honoring `Transfer-Encoding:
//! chunked` (dechunked here) or `Content-Length`. There is no keep-alive/pooling — the
//! daemon calls this client makes are infrequent enough (container lifecycle, not a
//! request-per-millisecond workload) that a fresh connection per call is the simplest
//! correct thing, and it sidesteps an entire class of connection-reuse bugs a hand-
//! rolled client would otherwise have to get right.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use rightsize::error::{Result, RightsizeError};

/// The daemon's default unix socket path on a host with no `DOCKER_HOST` override.
const DEFAULT_SOCKET_PATH: &str = "/var/run/docker.sock";

/// The ceiling on a single unary request/response cycle — connect, write, read
/// headers, read the whole body. Long enough for a slow daemon under load, short
/// enough that a wedged socket (daemon hung, container half-dead) fails the calling
/// `start()`/`exec()`/etc. instead of hanging the whole test run forever. Deliberately
/// NOT applied
/// to [`DockerClient::request_stream`]'s caller-driven read loop — `follow_logs`
/// streams for as long as the workload runs, by design (see the module docs).
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(600);

/// The ceiling on how many bytes [`read_headers`]/[`read_line`] will accumulate before
/// giving up. A well-behaved daemon's status line + header block is at most a few
/// hundred bytes; a peer that never sends the terminating `\r\n\r\n` (or `\n` for a
/// chunk-size line) would otherwise make the byte-at-a-time reader in this module
/// accumulate unboundedly in memory while it waits — this cap turns that failure mode
/// into a prompt, named error instead of unbounded growth.
const MAX_HEADER_BYTES: usize = 8 * 1024;

/// A parsed HTTP response: status code, headers (lowercased names, original-case
/// values, in receipt order), and the raw (already dechunked) body.
#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
}

/// A minimal HTTP/1.1 client to the Docker daemon's unix socket. Stateless beyond the
/// socket path — every call opens a fresh connection (see the module docs for why).
#[derive(Clone, Debug)]
pub(crate) struct DockerClient {
    socket_path: PathBuf,
    /// Overridable only by tests (see [`Self::at_socket`]/[`Self::with_response_timeout`])
    /// so a fixture that deliberately never completes a response doesn't force a real
    /// test run to wait out the full [`RESPONSE_TIMEOUT`].
    response_timeout: Duration,
}

impl DockerClient {
    /// Builds a client at the default socket path (`/var/run/docker.sock`), or the
    /// path from `DOCKER_HOST` if it names a `unix://` (or bare-path) socket — the
    /// same override real `docker` CLI/SDKs honor. A `DOCKER_HOST` naming a TCP
    /// endpoint (`tcp://…`, `http://…`) is deliberately NOT honored: this client only
    /// ever dials a unix socket — see [`Self::from_env`]'s doc for the exact
    /// fallback behavior in that case.
    pub(crate) fn from_env() -> Self {
        Self::from_docker_host(std::env::var("DOCKER_HOST").ok().as_deref())
    }

    /// The pure seam [`Self::from_env`] delegates to, parameterized over the
    /// `DOCKER_HOST` value so this is unit-testable without touching real process
    /// environment. `host`:
    /// - `None` or unset → the default socket path.
    /// - `Some("unix:///path/to.sock")` → `/path/to.sock`.
    /// - `Some("/path/to.sock")` (a bare path, no scheme) → used as-is.
    /// - `Some("tcp://…")`/`Some("http://…")`/anything else non-unix → falls back to
    ///   the default socket path rather than attempting a TCP connection: this client
    ///   has no TCP transport at all by design, so a non-unix
    ///   `DOCKER_HOST` is treated the same as if it named a socket at the default
    ///   location — this client only ever speaks to a real unix socket, never
    ///   attempting HTTP over a socket path that doesn't exist.
    pub(crate) fn from_docker_host(host: Option<&str>) -> Self {
        let socket_path = match host {
            Some(h) if h.starts_with("unix://") => PathBuf::from(h.trim_start_matches("unix://")),
            Some(h) if h.starts_with('/') => PathBuf::from(h),
            _ => PathBuf::from(DEFAULT_SOCKET_PATH),
        };
        DockerClient {
            socket_path,
            response_timeout: RESPONSE_TIMEOUT,
        }
    }

    /// Builds a client dialing an arbitrary socket path directly — the seam unit tests
    /// use to point this client at a local fixture listener instead of the real
    /// daemon.
    #[cfg(test)]
    pub(crate) fn at_socket(path: impl Into<PathBuf>) -> Self {
        DockerClient {
            socket_path: path.into(),
            response_timeout: RESPONSE_TIMEOUT,
        }
    }

    /// Test-only: shrinks the unary-request timeout so a fixture that deliberately
    /// never completes a response fails promptly instead of after the real
    /// [`RESPONSE_TIMEOUT`] ceiling.
    #[cfg(test)]
    pub(crate) fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    pub(crate) fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Issues one unary request (a request whose whole response body is read before
    /// returning) — every daemon call in this client except log/exec streaming, which
    /// use [`Self::request_stream`] instead so the frame demuxer can consume the body
    /// incrementally.
    ///
    /// The whole cycle — connect, write, read headers, read the body — is bounded by
    /// [`RESPONSE_TIMEOUT`]: a daemon that never finishes a response (wedged, or a
    /// fixture in a test that never completes its header block) fails this call with a
    /// named timeout error rather than hanging the caller forever. `request_stream`'s
    /// caller-driven read loop is deliberately NOT wrapped this way — `follow_logs`
    /// streams for as long as the workload runs.
    pub(crate) async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<Response> {
        tokio::time::timeout(
            self.response_timeout,
            self.request_inner(method, path, body),
        )
        .await
        .map_err(|_| {
            RightsizeError::Backend(format!(
                "{method} {path} to the Docker daemon did not complete within {}s",
                self.response_timeout.as_secs()
            ))
        })?
    }

    async fn request_inner(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<Response> {
        let mut stream = self.connect().await?;
        write_request(&mut stream, method, path, body).await?;
        let headers = read_headers(&mut stream).await?;
        let body = read_body_to_end(&mut stream, &headers).await?;
        Ok(Response {
            status: headers.status,
            body,
        })
    }

    /// Issues a request and returns the status plus the still-open stream positioned
    /// right after the headers, for callers (logs/exec streaming, the frame
    /// demuxer) that need to consume a dechunked body as it arrives rather than
    /// buffering all of it first. The returned [`ParsedHeaders`] tells the caller
    /// whether to read via chunked framing or a fixed `Content-Length`.
    pub(crate) async fn request_stream(
        &self,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> Result<(ParsedHeaders, UnixStream)> {
        let mut stream = self.connect().await?;
        write_request(&mut stream, method, path, body).await?;
        let headers = read_headers(&mut stream).await?;
        Ok((headers, stream))
    }

    async fn connect(&self) -> Result<UnixStream> {
        UnixStream::connect(&self.socket_path).await.map_err(|e| {
            RightsizeError::Backend(format!(
                "could not connect to the Docker daemon at {} — is Docker/Podman/Colima \
                 running? ({e})",
                self.socket_path.display()
            ))
        })
    }
}

/// Writes `METHOD path HTTP/1.1\r\nHost: docker\r\n...\r\n\r\n[body]` to `stream`.
/// `Content-Length`/`Content-Type` headers are included only when `body` is `Some`.
async fn write_request(
    stream: &mut UnixStream,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<()> {
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: docker\r\n");
    if let Some(b) = body {
        req.push_str(&format!(
            "Content-Length: {}\r\nContent-Type: application/json\r\n",
            b.len()
        ));
    }
    req.push_str("Connection: close\r\n\r\n");
    if let Some(b) = body {
        req.push_str(b);
    }
    stream.write_all(req.as_bytes()).await.map_err(|e| {
        RightsizeError::Backend(format!("writing to the Docker daemon socket: {e}"))
    })?;
    Ok(())
}

/// The parsed status line and the subset of headers this client acts on.
pub(crate) struct ParsedHeaders {
    pub(crate) status: u16,
    pub(crate) chunked: bool,
    pub(crate) content_length: Option<usize>,
}

/// Reads bytes from `stream` until the blank line ending the header block
/// (`\r\n\r\n`), byte-at-a-time — simplest-correct for a response whose header block
/// is at most a few hundred bytes, and avoids needing a `BufReader` wrapper that would
/// complicate handing the same stream on to [`Self::request_stream`]'s caller
/// afterward with no bytes lost to internal buffering.
///
/// Bounded at [`MAX_HEADER_BYTES`]: a peer that never sends the terminating blank line
/// (a wedged daemon, or a test fixture that stalls mid-header) would otherwise make
/// this loop accumulate `raw` without limit while it waits for bytes that never
/// complete the block — this turns that into a named error instead of unbounded growth.
async fn read_headers(stream: &mut UnixStream) -> Result<ParsedHeaders> {
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await.map_err(|e| {
            RightsizeError::Backend(format!("reading headers from the Docker daemon: {e}"))
        })?;
        if n == 0 {
            return Err(RightsizeError::Backend(
                "the Docker daemon closed the connection before sending a complete header block"
                    .to_string(),
            ));
        }
        raw.push(byte[0]);
        if raw.len() > MAX_HEADER_BYTES {
            return Err(RightsizeError::Backend(format!(
                "the Docker daemon's response header block exceeded {MAX_HEADER_BYTES} bytes \
                 without a terminating blank line — treating this as a malformed or wedged \
                 response instead of buffering it without limit"
            )));
        }
        if raw.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&raw);
    parse_headers(&text)
}

/// Parses the status line + header block text (already known to be a complete
/// `\r\n`-terminated block) into a [`ParsedHeaders`].
fn parse_headers(text: &str) -> Result<ParsedHeaders> {
    let mut lines = text.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| {
            RightsizeError::Backend(format!(
                "could not parse a status line from the Docker daemon's response: {status_line:?}"
            ))
        })?;

    let mut chunked = false;
    let mut content_length = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "transfer-encoding" => {
                chunked = value.to_ascii_lowercase().contains("chunked");
            }
            "content-length" => {
                content_length = value.parse::<usize>().ok();
            }
            _ => {}
        }
    }
    Ok(ParsedHeaders {
        status,
        chunked,
        content_length,
    })
}

/// Reads the whole response body from `stream` given already-parsed `headers`,
/// honoring `Transfer-Encoding: chunked` (dechunked here) or `Content-Length`. Neither
/// present means "read until the peer closes the connection" (this client always
/// sends `Connection: close`, so the daemon closing at body-end is the expected
/// signal in that case).
async fn read_body_to_end(stream: &mut UnixStream, headers: &ParsedHeaders) -> Result<Vec<u8>> {
    if headers.chunked {
        return read_chunked_body(stream).await;
    }
    if let Some(len) = headers.content_length {
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.map_err(|e| {
            RightsizeError::Backend(format!(
                "reading a Content-Length body from the Docker daemon: {e}"
            ))
        })?;
        return Ok(buf);
    }
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.map_err(|e| {
        RightsizeError::Backend(format!(
            "reading an unbounded body from the Docker daemon: {e}"
        ))
    })?;
    Ok(buf)
}

/// Dechunks an HTTP `Transfer-Encoding: chunked` body from `stream`: repeatedly reads
/// a hex chunk-size line, then that many bytes plus the trailing `\r\n`, until a
/// zero-size chunk (optionally followed by trailer headers, which this daemon never
/// sends anything interesting in, so they're drained and discarded) ends the stream.
pub(crate) async fn read_chunked_body(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let size_line = read_line(stream).await?;
        let size_str = size_line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_str, 16).map_err(|_| {
            RightsizeError::Backend(format!(
                "could not parse a chunk size from the Docker daemon's response: {size_line:?}"
            ))
        })?;
        if size == 0 {
            // Drain trailer headers up to the final blank line, then stop.
            loop {
                let trailer = read_line(stream).await?;
                if trailer.is_empty() {
                    break;
                }
            }
            break;
        }
        let mut chunk = vec![0u8; size];
        stream.read_exact(&mut chunk).await.map_err(|e| {
            RightsizeError::Backend(format!("reading a chunk body from the Docker daemon: {e}"))
        })?;
        out.extend_from_slice(&chunk);
        // Each chunk is followed by a trailing \r\n that isn't part of the payload.
        let mut crlf = [0u8; 2];
        stream.read_exact(&mut crlf).await.map_err(|e| {
            RightsizeError::Backend(format!(
                "reading a chunk trailer from the Docker daemon: {e}"
            ))
        })?;
    }
    Ok(out)
}

/// Reads one `\r\n`-terminated line from `stream`, byte-at-a-time (same rationale as
/// [`read_headers`]), returning it without the line terminator.
///
/// Bounded at [`MAX_HEADER_BYTES`], same reasoning as `read_headers` — a chunk-size
/// line (or trailer line) that never terminates would otherwise accumulate `raw`
/// without limit.
async fn read_line(stream: &mut UnixStream) -> Result<String> {
    let mut raw = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await.map_err(|e| {
            RightsizeError::Backend(format!("reading a line from the Docker daemon: {e}"))
        })?;
        if n == 0 {
            return Err(RightsizeError::Backend(
                "the Docker daemon closed the connection mid-chunked-response".to_string(),
            ));
        }
        if byte[0] == b'\n' {
            if raw.last() == Some(&b'\r') {
                raw.pop();
            }
            break;
        }
        raw.push(byte[0]);
        if raw.len() > MAX_HEADER_BYTES {
            return Err(RightsizeError::Backend(format!(
                "the Docker daemon sent a chunked-response line exceeding {MAX_HEADER_BYTES} \
                 bytes without a terminator — treating this as malformed instead of buffering \
                 it without limit"
            )));
        }
    }
    Ok(String::from_utf8_lossy(&raw).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    /// Spawns a fixture unix-socket "server" in a temp dir that accepts exactly one
    /// connection, reads whatever the client sends (discarded — these tests only
    /// care about what the client parses back out of a canned response), and writes
    /// `response` verbatim before closing. Returns the client already pointed at it.
    async fn fixture_client(response: &'static [u8]) -> (DockerClient, tempdir_shim::TempDir) {
        let dir = tempdir_shim::TempDir::new();
        let sock_path = dir.path().join("docker.sock");
        let listener = UnixListener::bind(&sock_path).expect("bind fixture socket");
        tokio::spawn(async move {
            if let Ok((mut conn, _)) = listener.accept().await {
                // Drain whatever the client sent so `write_all` on its side doesn't
                // block on a full pipe buffer for larger requests.
                let mut discard = [0u8; 4096];
                use tokio::io::AsyncReadExt as _;
                let _ = tokio::time::timeout(
                    std::time::Duration::from_millis(50),
                    conn.read(&mut discard),
                )
                .await;
                use tokio::io::AsyncWriteExt as _;
                let _ = conn.write_all(response).await;
                let _ = conn.shutdown().await;
            }
        });
        (DockerClient::at_socket(sock_path), dir)
    }

    /// A minimal, dependency-free temp-directory helper (this crate takes on no extra
    /// deps beyond its runtime dependency budget, so tests build their own
    /// throwaway-directory scaffolding from `std` rather than pulling in a `tempfile`
    /// crate for this alone).
    mod tempdir_shim {
        use std::path::{Path, PathBuf};

        pub(super) struct TempDir(PathBuf);

        impl TempDir {
            /// Rooted at `/tmp` (not `std::env::temp_dir()`) and kept deliberately
            /// short: a unix socket path is capped at `SUN_LEN` (~104 bytes on macOS),
            /// and `std::env::temp_dir()`'s per-process `TMPDIR` on macOS
            /// (`/var/folders/.../T/`) alone can eat half that budget before this
            /// helper even adds a socket filename.
            pub(super) fn new() -> Self {
                static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
                let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let unique = format!("rzdt-{:x}-{:x}", std::process::id() as u16, seq);
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

    #[tokio::test]
    async fn parses_a_content_length_response() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 12\r\n\r\n{\"Id\":\"abc\"}";
        let (client, _dir) = fixture_client(response).await;
        let resp = client.request("GET", "/_ping", None).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"{\"Id\":\"abc\"}");
    }

    #[tokio::test]
    async fn parses_a_chunked_response() {
        let response: &[u8] =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n";
        let (client, _dir) = fixture_client(response).await;
        let resp = client
            .request("GET", "/containers/x/logs", None)
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, b"hello world");
    }

    #[tokio::test]
    async fn parses_a_non_200_status() {
        let response = b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        let (client, _dir) = fixture_client(response).await;
        let resp = client
            .request("GET", "/images/missing/json", None)
            .await
            .unwrap();
        assert_eq!(resp.status, 404);
        assert!(resp.body.is_empty());
    }

    #[tokio::test]
    async fn parses_a_500_with_a_body() {
        let response = b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 28\r\n\r\n{\"message\":\"already in use\"}";
        let (client, _dir) = fixture_client(response).await;
        let resp = client
            .request("POST", "/containers/x/start", None)
            .await
            .unwrap();
        assert_eq!(resp.status, 500);
        assert_eq!(resp.body, b"{\"message\":\"already in use\"}");
    }

    #[tokio::test]
    async fn writes_a_json_body_with_matching_content_length() {
        let dir = tempdir_shim::TempDir::new();
        let sock_path = dir.path().join("docker.sock");
        let listener = UnixListener::bind(&sock_path).unwrap();
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_clone = received.clone();
        tokio::spawn(async move {
            if let Ok((mut conn, _)) = listener.accept().await {
                let mut buf = vec![0u8; 4096];
                use tokio::io::AsyncReadExt as _;
                if let Ok(n) =
                    tokio::time::timeout(std::time::Duration::from_millis(100), conn.read(&mut buf))
                        .await
                {
                    let n = n.unwrap_or(0);
                    received_clone.lock().unwrap().extend_from_slice(&buf[..n]);
                }
                use tokio::io::AsyncWriteExt as _;
                let _ = conn
                    .write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\n{}")
                    .await;
                let _ = conn.shutdown().await;
            }
        });
        let client = DockerClient::at_socket(sock_path);
        let body = r#"{"Image":"redis"}"#;
        let resp = client
            .request("POST", "/containers/create?name=x", Some(body))
            .await
            .unwrap();
        assert_eq!(resp.status, 201);
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let sent = String::from_utf8(received.lock().unwrap().clone()).unwrap();
        assert!(sent.starts_with("POST /containers/create?name=x HTTP/1.1\r\n"));
        assert!(sent.contains("Content-Length: 17\r\n"));
        assert!(sent.contains("Content-Type: application/json\r\n"));
        assert!(sent.ends_with(body));
    }

    #[test]
    fn from_docker_host_none_uses_the_default_socket_path() {
        let client = DockerClient::from_docker_host(None);
        assert_eq!(client.socket_path(), Path::new(DEFAULT_SOCKET_PATH));
    }

    #[test]
    fn from_docker_host_parses_a_unix_scheme_path() {
        let client = DockerClient::from_docker_host(Some("unix:///run/user/1000/docker.sock"));
        assert_eq!(
            client.socket_path(),
            Path::new("/run/user/1000/docker.sock")
        );
    }

    #[test]
    fn from_docker_host_accepts_a_bare_path() {
        let client = DockerClient::from_docker_host(Some("/custom/docker.sock"));
        assert_eq!(client.socket_path(), Path::new("/custom/docker.sock"));
    }

    #[test]
    fn from_docker_host_falls_back_to_default_for_a_tcp_host() {
        // This client has no TCP transport at all — a tcp:// DOCKER_HOST
        // falls back to the default unix socket path rather than attempting to speak
        // HTTP over a nonexistent socket path derived from a TCP URL.
        let client = DockerClient::from_docker_host(Some("tcp://127.0.0.1:2375"));
        assert_eq!(client.socket_path(), Path::new(DEFAULT_SOCKET_PATH));
    }

    #[test]
    fn parse_headers_extracts_status_and_transfer_encoding() {
        let text = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
        let headers = parse_headers(text).unwrap();
        assert_eq!(headers.status, 200);
        assert!(headers.chunked);
        assert_eq!(headers.content_length, None);
    }

    #[test]
    fn parse_headers_extracts_content_length() {
        let text = "HTTP/1.1 201 Created\r\nContent-Length: 42\r\n\r\n";
        let headers = parse_headers(text).unwrap();
        assert_eq!(headers.status, 201);
        assert!(!headers.chunked);
        assert_eq!(headers.content_length, Some(42));
    }

    #[test]
    fn parse_headers_rejects_a_missing_status_line() {
        assert!(parse_headers("").is_err());
    }

    /// A fixture that accepts the connection, reads nothing back to the client, and
    /// then sends bytes forever without ever completing a header block (no `\r\n\r\n`)
    /// — the "wedged daemon" case the response-timeout ceiling exists for. Bytes are
    /// paced slowly so the connection stays open (not EOF) for the whole test.
    async fn spawn_never_completing_headers_fixture() -> (DockerClient, tempdir_shim::TempDir) {
        let dir = tempdir_shim::TempDir::new();
        let sock_path = dir.path().join("docker.sock");
        let listener = UnixListener::bind(&sock_path).expect("bind fixture socket");
        tokio::spawn(async move {
            if let Ok((mut conn, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt as _;
                loop {
                    if conn.write_all(b"HTTP/1.1 200 OK\r\n").await.is_err() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
            }
        });
        (DockerClient::at_socket(sock_path), dir)
    }

    #[tokio::test]
    async fn request_times_out_instead_of_hanging_on_a_response_that_never_completes_headers() {
        let (client, _dir) = spawn_never_completing_headers_fixture().await;
        let client = client.with_response_timeout(Duration::from_millis(200));

        let started = std::time::Instant::now();
        let err = client
            .request("GET", "/_ping", None)
            .await
            .expect_err("a response that never finishes its header block must time out");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "must fail promptly, not hang: took {:?}",
            started.elapsed()
        );
        assert!(err.to_string().contains("did not complete within"), "{err}");
    }

    /// A fixture that accepts the connection and floods it with header-line bytes that
    /// never terminate the block — proves the [`MAX_HEADER_BYTES`] cap fires (a named
    /// error) well before the response-timeout ceiling would, and without unbounded
    /// memory growth.
    async fn spawn_oversized_header_fixture() -> (DockerClient, tempdir_shim::TempDir) {
        let dir = tempdir_shim::TempDir::new();
        let sock_path = dir.path().join("docker.sock");
        let listener = UnixListener::bind(&sock_path).expect("bind fixture socket");
        tokio::spawn(async move {
            if let Ok((mut conn, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt as _;
                let _ = conn.write_all(b"HTTP/1.1 200 OK\r\n").await;
                // Keep sending header-line bytes with no blank-line terminator, well
                // past MAX_HEADER_BYTES, without ever closing the connection.
                let filler = vec![b'X'; 4096];
                loop {
                    if conn.write_all(&filler).await.is_err() {
                        break;
                    }
                }
            }
        });
        (DockerClient::at_socket(sock_path), dir)
    }

    #[tokio::test]
    async fn read_headers_caps_accumulation_instead_of_growing_without_bound() {
        let (client, _dir) = spawn_oversized_header_fixture().await;
        // The header-size cap should fire well within the (much larger) response
        // timeout — use a generous timeout here so only the byte-cap is under test.
        let client = client.with_response_timeout(Duration::from_secs(30));

        let started = std::time::Instant::now();
        let err = client
            .request("GET", "/_ping", None)
            .await
            .expect_err("an oversized, never-terminated header block must be rejected");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the byte cap must fire promptly: took {:?}",
            started.elapsed()
        );
        assert!(
            err.to_string().contains("exceeded")
                && err.to_string().contains(&MAX_HEADER_BYTES.to_string()),
            "{err}"
        );
    }
}
