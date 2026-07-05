//! Wait strategies: pluggable readiness checks run after a container starts, before
//! `Container::start()` returns its guard to the caller.
//!
//! The single highest-leverage correctness fix in this module is the **read-probe**
//! in [`Wait::for_listening_port`] — see its doc for why a bare TCP connect is not
//! enough.

use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::Instant;

use crate::error::RightsizeError;

/// How long the read-probe waits for data (or an EOF/RST) after a bare connect
/// succeeds, before concluding "a real peer is on the other end, just not talking
/// yet". See [`Wait::for_listening_port`] for the full rationale.
const READ_PROBE_TIMEOUT: Duration = Duration::from_millis(200);

/// How often a [`WaitStrategy`] polls its target between probes.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How many trailing log lines a timeout error includes.
const LOG_TAIL_LINES: usize = 50;

/// The minimal view of a running container a [`WaitStrategy`] needs — backend-agnostic
/// by design, so a wait strategy never has to know which backend booted the container
/// it's probing.
#[async_trait::async_trait]
pub trait WaitTarget: Send + Sync {
    /// The host address the container's ports are reachable on (typically `127.0.0.1`).
    fn host(&self) -> &str;
    /// The host port the given guest port is published on.
    fn mapped_port(&self, guest_port: u16) -> u16;
    /// Every guest port the container declared with `with_exposed_ports`.
    fn exposed_guest_ports(&self) -> Vec<u16>;
    /// The container's logs so far, for readiness probes and timeout diagnostics.
    async fn current_logs(&self) -> String;
    /// A short human-readable identifier, used in timeout/error messages.
    fn describe(&self) -> String;
}

/// A pluggable readiness check. Built-in strategies come from [`Wait`]; a module (or a
/// caller) may implement this directly for a strategy that speaks a protocol handshake
/// instead of just checking a port (e.g. the Memcached module's `VERSION` probe).
#[async_trait::async_trait]
pub trait WaitStrategy: Send + Sync {
    /// Blocks until `target` is ready, or returns
    /// [`RightsizeError::ContainerLaunch`] on timeout.
    async fn wait_until_ready(&self, target: &dyn WaitTarget) -> crate::error::Result<()>;
    /// Returns a copy of this strategy with a different startup timeout.
    fn with_startup_timeout(self: Box<Self>, timeout: Duration) -> Box<dyn WaitStrategy>;
}

/// Forwards to the boxed strategy inside — lets an already-boxed strategy (what every
/// built-in [`Wait`] factory that needs one returns) satisfy `Container::waiting_for`'s
/// `impl WaitStrategy + 'static` bound directly, with no caller ever having to write
/// `Box::new(..)` themselves. `waiting_for` still re-boxes its argument
/// unconditionally, so passing an already-`Box<dyn WaitStrategy>` here does produce a
/// `Box<Box<dyn WaitStrategy>>` — one extra allocation, not avoided by this impl — but
/// delegation through it is correct: every call forwards straight to the inner
/// strategy with no behavior change.
#[async_trait::async_trait]
impl WaitStrategy for Box<dyn WaitStrategy> {
    async fn wait_until_ready(&self, target: &dyn WaitTarget) -> crate::error::Result<()> {
        (**self).wait_until_ready(target).await
    }

    fn with_startup_timeout(self: Box<Self>, timeout: Duration) -> Box<dyn WaitStrategy> {
        (*self).with_startup_timeout(timeout)
    }
}

/// Owns the deadline/poll-interval/log-tail plumbing shared by every built-in
/// [`WaitStrategy`]. A concrete strategy supplies `is_ready` (a single readiness probe)
/// and `what` (a short human description for the timeout message); this helper performs
/// the actual polling loop, including the **do-first-probe-then-check-deadline**
/// ordering — a plain `while before(deadline)` guard can exit having performed zero
/// polls when the timeout is shorter than one loop iteration (sub-millisecond
/// timeouts), under-honoring the caller's intent that a wait strategy always gets at
/// least one real chance to observe readiness.
///
/// Public (not crate-private) because a module implementing its own protocol-level
/// probe — e.g. `rightsize-modules`' Memcached `VERSION` handshake — is exactly the
/// kind of custom [`WaitStrategy`] this polling loop exists to serve; it would be
/// wasteful for every such module to hand-roll its own deadline/poll-interval loop.
pub async fn poll_until_ready<F, Fut>(
    target: &dyn WaitTarget,
    timeout: Duration,
    what: &str,
    mut is_ready: F,
) -> crate::error::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if is_ready().await {
            return Ok(());
        }
        if Instant::now() < deadline {
            tokio::time::sleep(POLL_INTERVAL).await;
        }
        if Instant::now() >= deadline {
            break;
        }
    }
    let logs = target.current_logs().await;
    let tail: Vec<&str> = logs.lines().collect();
    let tail = if tail.len() > LOG_TAIL_LINES {
        &tail[tail.len() - LOG_TAIL_LINES..]
    } else {
        &tail[..]
    };
    Err(RightsizeError::ContainerLaunch(format!(
        "Timed out after {}s waiting for {what} on {}.\nLast log lines:\n{}",
        timeout.as_secs(),
        target.describe(),
        tail.join("\n")
    )))
}

/// Does a bare TCP connect, then a short zero-byte read, to distinguish a real listener
/// from a userland proxy that accepted the connection before the guest server itself
/// was listening.
///
/// Docker's userland proxy (and microsandbox's loopback forwarder) can accept a TCP
/// connection well before the workload inside the container is actually listening —
/// they exist precisely to forward traffic, and accepting doesn't require anyone to be
/// home yet. A bare `connect()` succeeding is therefore not proof of readiness. This
/// probe adds one more signal: after connecting, set a short read timeout and attempt a
/// read.
///
/// - **EOF (read returns 0 bytes) or a reset** — the peer closed the stream right after
///   accepting, the hallmark of a proxy with nothing listening behind it. **Not ready.**
/// - **Data arrives, or the read simply times out with the connection still open** —
///   either a real server that speaks first, or one that's waiting for the client to
///   speak first. Both are **ready**: a closed connection is the only failure signal.
async fn connect_and_probe(host: &str, port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect((host, port)).await else {
        return false;
    };
    let mut buf = [0u8; 1];
    match tokio::time::timeout(READ_PROBE_TIMEOUT, stream.read(&mut buf)).await {
        Ok(Ok(0)) => false,    // EOF right after accept: proxy, nobody home.
        Ok(Ok(_)) => true,     // data arrived: real peer, speaks first.
        Ok(Err(_)) => false,   // read error (e.g. RST): treat as not-ready.
        Err(_elapsed) => true, // timed out with the connection open: real peer, waits for us.
    }
}

/// Ready when every exposed guest port accepts a TCP connection and passes the
/// read-probe. Built via [`Wait::for_listening_port`].
struct ListeningPortWait {
    timeout: Duration,
}

impl ListeningPortWait {
    async fn is_ready(&self, target: &dyn WaitTarget) -> bool {
        for gp in target.exposed_guest_ports() {
            if !connect_and_probe(target.host(), target.mapped_port(gp)).await {
                return false;
            }
        }
        true
    }
}

#[async_trait::async_trait]
impl WaitStrategy for ListeningPortWait {
    async fn wait_until_ready(&self, target: &dyn WaitTarget) -> crate::error::Result<()> {
        poll_until_ready(target, self.timeout, "a listening TCP port", || {
            self.is_ready(target)
        })
        .await
    }

    fn with_startup_timeout(mut self: Box<Self>, timeout: Duration) -> Box<dyn WaitStrategy> {
        self.timeout = timeout;
        self
    }
}

/// Ready when a log line matching `re` has appeared at least `times` times. Built via
/// [`Wait::for_log_message`].
struct LogMessageWait {
    re: regex_lite::Regex,
    times: usize,
    what: String,
    timeout: Duration,
}

impl LogMessageWait {
    async fn is_ready(&self, target: &dyn WaitTarget) -> bool {
        let logs = target.current_logs().await;
        let count = logs.lines().filter(|line| self.re.is_match(line)).count();
        count >= self.times
    }
}

#[async_trait::async_trait]
impl WaitStrategy for LogMessageWait {
    async fn wait_until_ready(&self, target: &dyn WaitTarget) -> crate::error::Result<()> {
        poll_until_ready(target, self.timeout, &self.what, || self.is_ready(target)).await
    }

    fn with_startup_timeout(mut self: Box<Self>, timeout: Duration) -> Box<dyn WaitStrategy> {
        self.timeout = timeout;
        self
    }
}

/// Polls an HTTP endpoint until it returns the expected status code. Built via
/// [`Wait::for_http`].
pub struct HttpWaitStrategy {
    path: String,
    guest_port: Option<u16>,
    status: u16,
    timeout: Duration,
}

impl HttpWaitStrategy {
    fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            guest_port: None,
            status: 200,
            timeout: Duration::from_secs(60),
        }
    }

    /// The guest port to probe. Defaults to the container's first exposed port if this
    /// is never called.
    pub fn for_port(mut self, guest_port: u16) -> Self {
        self.guest_port = Some(guest_port);
        self
    }

    /// The response status code that counts as ready. Defaults to 200.
    pub fn for_status_code(mut self, code: u16) -> Self {
        self.status = code;
        self
    }

    /// A by-value fluent equivalent of [`WaitStrategy::with_startup_timeout`], so
    /// callers can chain it directly on the concrete `HttpWaitStrategy` `Wait::for_http`
    /// returns without boxing first.
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn what(&self) -> String {
        format!("HTTP {} on {}", self.status, self.path)
    }

    async fn is_ready(&self, target: &dyn WaitTarget) -> bool {
        let guest_port = match self.guest_port {
            Some(p) => p,
            None => match target.exposed_guest_ports().first().copied() {
                Some(p) => p,
                None => return false,
            },
        };
        let port = target.mapped_port(guest_port);
        matches!(probe_http_status(target.host(), port, &self.path).await, Some(code) if code == self.status)
    }
}

#[async_trait::async_trait]
impl WaitStrategy for HttpWaitStrategy {
    async fn wait_until_ready(&self, target: &dyn WaitTarget) -> crate::error::Result<()> {
        let what = self.what();
        poll_until_ready(target, self.timeout, &what, || self.is_ready(target)).await
    }

    fn with_startup_timeout(mut self: Box<Self>, timeout: Duration) -> Box<dyn WaitStrategy> {
        self.timeout = timeout;
        self
    }
}

/// A tiny hand-rolled HTTP/1.1 GET, connect+read timeout 1s, returning the numeric
/// status code — core takes no HTTP-client dependency (see the dependency budget), and
/// this is the only thing a readiness probe needs.
async fn probe_http_status(host: &str, port: u16, path: &str) -> Option<u16> {
    let connect = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect((host, port)))
        .await
        .ok()?;
    let mut stream = connect.ok()?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    tokio::time::timeout(
        Duration::from_secs(1),
        tokio::io::AsyncWriteExt::write_all(&mut stream, request.as_bytes()),
    )
    .await
    .ok()?
    .ok()?;

    let mut response = Vec::new();
    let _ = tokio::time::timeout(
        Duration::from_secs(1),
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut response),
    )
    .await;
    let text = String::from_utf8_lossy(&response);
    let status_line = text.lines().next()?;
    // "HTTP/1.1 200 OK" -> 200
    let code_str = status_line.split_whitespace().nth(1)?;
    code_str.parse::<u16>().ok()
}

/// Factory for the built-in [`WaitStrategy`] implementations; pass the result to
/// `Container::waiting_for`.
pub struct Wait;

impl Wait {
    /// Ready when every exposed guest port accepts a TCP connection, with the read-probe
    /// described on `connect_and_probe` to see past a userland proxy/forwarder
    /// accepting before the guest server is actually listening.
    ///
    /// The default strategy when `Container::waiting_for` is never called. Vacuously
    /// ready immediately if the container exposes no ports.
    pub fn for_listening_port() -> Box<dyn WaitStrategy> {
        Box::new(ListeningPortWait {
            timeout: Duration::from_secs(60),
        })
    }

    /// Ready when an HTTP GET to `path` returns the expected status; see
    /// [`HttpWaitStrategy`].
    pub fn for_http(path: &str) -> HttpWaitStrategy {
        HttpWaitStrategy::new(path)
    }

    /// Ready when a log line matching `regex` has appeared at least `times` times.
    /// `times = 0` means "ready immediately, without ever needing a matching line" —
    /// used as a bare boot-wait.
    ///
    /// `regex` is matched as a substring search anywhere in the line (`Regex::is_match`,
    /// not an implicit whole-line anchor). A single line satisfying the pattern counts
    /// once, even if it would also satisfy a stricter whole-line match — there is only
    /// one counting rule here, not two redundant ones.
    ///
    /// # Panics
    ///
    /// If `regex` fails to compile. Every built-in module pattern is a fixed string
    /// literal verified at write time, so this is a programmer error caught immediately
    /// at startup, not a runtime data condition a caller needs to handle.
    pub fn for_log_message(regex: &str, times: usize) -> Box<dyn WaitStrategy> {
        let re = regex_lite::Regex::new(regex)
            .unwrap_or_else(|e| panic!("Wait::for_log_message: invalid regex '{regex}': {e}"));
        let what = format!("log line matching '{regex}' x{times}");
        Box::new(LogMessageWait {
            re,
            times,
            what,
            timeout: Duration::from_secs(60),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener as StdTcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct FakeTarget {
        ports: std::collections::HashMap<u16, u16>,
        logs: Arc<std::sync::Mutex<String>>,
        probes: Arc<AtomicUsize>,
    }

    impl FakeTarget {
        fn new(ports: &[(u16, u16)]) -> Self {
            Self {
                ports: ports.iter().copied().collect(),
                logs: Arc::new(std::sync::Mutex::new(String::new())),
                probes: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_logs(ports: &[(u16, u16)], logs: &str) -> Self {
            let t = Self::new(ports);
            *t.logs.lock().unwrap() = logs.to_string();
            t
        }
    }

    #[async_trait::async_trait]
    impl WaitTarget for FakeTarget {
        fn host(&self) -> &str {
            "127.0.0.1"
        }
        fn mapped_port(&self, guest_port: u16) -> u16 {
            self.probes.fetch_add(1, Ordering::SeqCst);
            *self.ports.get(&guest_port).unwrap_or(&guest_port)
        }
        fn exposed_guest_ports(&self) -> Vec<u16> {
            self.ports.keys().copied().collect()
        }
        async fn current_logs(&self) -> String {
            self.logs.lock().unwrap().clone()
        }
        fn describe(&self) -> String {
            "fake-target".to_string()
        }
    }

    fn free_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn for_listening_port_succeeds_on_open_port_and_times_out_on_closed() {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        // A listening socket with nobody calling accept() yet still yields a connection
        // that stays open once connected (the OS backlog completes the handshake) — the
        // read-probe must treat "connected, nothing read, read times out" as ready.
        let target = FakeTarget::new(&[(80, port)]);
        Wait::for_listening_port()
            .wait_until_ready(&target)
            .await
            .expect("open port must be ready");

        let closed_port = free_port(); // bound then immediately dropped: nothing listens.
        let target = FakeTarget::with_logs(&[(80, closed_port)], "boot log line");
        let err = Wait::for_listening_port()
            .with_startup_timeout(Duration::from_millis(500))
            .wait_until_ready(&target)
            .await
            .expect_err("closed port must time out");
        let msg = err.to_string();
        assert!(msg.contains("fake-target"), "{msg}");
        assert!(msg.contains("boot log line"), "{msg}");
    }

    #[tokio::test]
    async fn read_probe_rejects_accept_then_eof_and_accepts_hold_open_peer() {
        let behave_like_real_server = Arc::new(AtomicBool::new(false));
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let closed = Arc::new(AtomicBool::new(false));
        let closed_clone = closed.clone();
        let behave_clone = behave_like_real_server.clone();
        let acceptor = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            let held: std::sync::Mutex<Vec<std::net::TcpStream>> =
                std::sync::Mutex::new(Vec::new());
            while !closed_clone.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if behave_clone.load(Ordering::SeqCst) {
                            held.lock().unwrap().push(stream); // hold it open: real server.
                        } else {
                            drop(stream); // proxy-with-nobody-home: accept then immediate EOF.
                        }
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(5)),
                }
            }
        });

        let target = FakeTarget::new(&[(80, port)]);
        let err = Wait::for_listening_port()
            .with_startup_timeout(Duration::from_millis(400))
            .wait_until_ready(&target)
            .await
            .expect_err("accept-then-EOF must not be ready");
        assert!(err.to_string().contains("a listening TCP port"));

        behave_like_real_server.store(true, Ordering::SeqCst);
        Wait::for_listening_port()
            .with_startup_timeout(Duration::from_secs(2))
            .wait_until_ready(&target)
            .await
            .expect("a peer that holds the connection open must be ready");

        closed.store(true, Ordering::SeqCst);
        acceptor.join().unwrap();
    }

    fn spawn_http_server(status: u16) -> (u16, Arc<AtomicBool>) {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = stop.clone();
        std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            while !stop_clone.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buf = [0u8; 1024];
                        let _ = stream.read(&mut buf);
                        let body = "";
                        let response = format!(
                            "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        (port, stop)
    }

    #[tokio::test]
    async fn for_http_matches_path_and_status() {
        let (port, stop) = spawn_http_server(200);
        let target = FakeTarget::new(&[(8080, port)]);
        Wait::for_http("/health")
            .for_port(8080)
            .for_status_code(200)
            .wait_until_ready(&target)
            .await
            .expect("must be ready");
        stop.store(true, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn for_http_falls_back_to_first_exposed_port_when_for_port_never_called() {
        let (port, stop) = spawn_http_server(200);
        let target = FakeTarget::new(&[(8080, port)]);
        Wait::for_http("/health")
            .wait_until_ready(&target)
            .await
            .expect("must fall back to first exposed port");
        stop.store(true, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn for_http_timeout_message_includes_log_tail_and_what() {
        let closed_port = free_port();
        let target = FakeTarget::with_logs(&[(8080, closed_port)], "http boot log");
        let err = Wait::for_http("/health")
            .for_port(8080)
            .for_status_code(200)
            .with_startup_timeout(Duration::from_millis(500))
            .wait_until_ready(&target)
            .await
            .expect_err("must time out");
        let msg = err.to_string();
        assert!(msg.contains("http boot log"), "{msg}");
        assert!(msg.contains("HTTP 200 on /health"), "{msg}");
    }

    #[tokio::test]
    async fn for_log_message_polls_until_pattern_seen_n_times() {
        let target = FakeTarget::new(&[]);
        let logs = target.logs.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            *logs.lock().unwrap() = "a\nready\nb\nready\n".to_string();
        });
        Wait::for_log_message(".*ready.*", 2)
            .wait_until_ready(&target)
            .await
            .expect("must see the pattern twice");
    }

    #[tokio::test]
    async fn for_log_message_times_zero_succeeds_immediately() {
        let target = FakeTarget::new(&[]);
        Wait::for_log_message(".*", 0)
            .wait_until_ready(&target)
            .await
            .expect("times=0 must be immediately ready");
    }

    #[tokio::test]
    async fn for_log_message_counts_a_line_matching_both_ways_only_once() {
        let target = FakeTarget::with_logs(&[], "ready\nnoise\nready\n");
        Wait::for_log_message(".*ready.*", 2)
            .wait_until_ready(&target)
            .await
            .expect("exactly 2 matches, not miscounted");
        let err = Wait::for_log_message(".*ready.*", 3)
            .with_startup_timeout(Duration::from_millis(500))
            .wait_until_ready(&target)
            .await
            .expect_err("must still time out at times=3");
        assert!(err.to_string().contains("x3"));
    }

    #[tokio::test]
    async fn for_log_message_timeout_message_includes_log_tail_and_what() {
        let target = FakeTarget::with_logs(&[], "line one\nline two");
        let err = Wait::for_log_message(".*ready.*", 3)
            .with_startup_timeout(Duration::from_millis(500))
            .wait_until_ready(&target)
            .await
            .expect_err("must time out");
        let msg = err.to_string();
        assert!(msg.contains("line one"), "{msg}");
        assert!(msg.contains("line two"), "{msg}");
        assert!(msg.contains("log line matching '.*ready.*' x3"), "{msg}");
    }

    #[tokio::test]
    async fn for_listening_port_with_no_exposed_ports_is_vacuously_ready() {
        let target = FakeTarget::new(&[]);
        assert!(target.exposed_guest_ports().is_empty());
        Wait::for_listening_port()
            .wait_until_ready(&target)
            .await
            .expect("must not block/timeout");
    }

    #[tokio::test]
    async fn do_while_performs_at_least_one_probe_even_on_a_short_timeout() {
        let target = FakeTarget::new(&[(1, 1)]); // never a real listener at port 1
        let start = tokio::time::Instant::now();
        let err = Wait::for_listening_port()
            .with_startup_timeout(Duration::from_millis(1))
            .wait_until_ready(&target)
            .await
            .expect_err("must time out");
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "should time out promptly, took {elapsed:?}"
        );
        assert!(
            target.probes.load(Ordering::SeqCst) >= 1,
            "must probe at least once even at a short timeout"
        );
        let _ = err;
    }
}
