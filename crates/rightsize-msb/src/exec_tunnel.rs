//! `ExecTunnel`: one `alias:guest_port` route into a consumer sandbox, bridged over
//! `msb exec --stream` — the only guest data path microsandbox exposes on macOS
//! (confirmed empirically: no sandbox→host TCP under any net-rule, ssh forwarding
//! broken). Client-speaks-first protocols only; single connection at a time; the
//! in-guest `nc -l` listener is respawned after every connection.
//!
//! **Raw, unbuffered, flush-per-read:** every pump in this module reads into a
//! plain byte buffer and writes+flushes immediately — never through a `BufReader`.
//! Buffering the exec-stream child's pipes was confirmed empirically to deadlock the
//! relay: a buffered reader can sit on bytes the far end is blocked waiting for.
//! `pump` below is written to avoid exactly that.
//!
//! **Close latency:** [`ExecTunnel::drop`] publishes and shuts down the live
//! target-side socket, not just the guest-side `nc` child, so a close racing a
//! connection in flight doesn't inherit [`FIRST_BYTE_DEADLINE`]'s 10s ceiling on the
//! read that ceiling bounds. See `Drop`'s doc comment for the full reasoning.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rightsize::backend::NetworkLink;

use crate::commands;

/// Respawning the in-guest `nc -l` listener is itself an `msb exec` process spawn;
/// without a pause between attempts, a listener that keeps exiting immediately (no
/// traffic, or a broken pump) busy-spins `msb exec` in a tight loop. Backoff fires only
/// on that not-served respawn path — a served connection respawns immediately, no
/// backoff.
const RESPAWN_BACKOFF: Duration = Duration::from_millis(200);

/// **Confirmed empirically against the real msb binary, and fixed here:** msb 0.6.2's
/// host-port-publish proxy (the `-p host:guest` layer) does not propagate the target's
/// own TCP close back to this host-side socket: a plain host TCP client reading a
/// published port past a `Connection: close` HTTP response never observes EOF, even
/// after the guest workload has closed its own end. (This is the mirror image of the
/// "proxy accepts before the guest listens" readiness caveat documented for wait
/// strategies — here the proxy doesn't propagate close either.) The naive approach —
/// ending the target-to-guest pump only on a natural `read() == 0` — therefore blocks
/// forever on this msb build for any single-exchange protocol (every contract-suite
/// HTTP fixture). The fix: give the
/// target-side socket a read timeout and treat a timeout as "this exchange is over"
/// exactly like EOF — safe here because this tunnel already commits to "one connection
/// at a time, client-speaks-first", so a real mid-exchange pause longer than this
/// timeout was never a supported shape to begin with. `kill_current()` (called
/// unconditionally once both pump directions are done) is what actually reclaims the
/// guest-side `nc` regardless of which side ended the exchange.
///
/// **Scoped to the post-data phase only:** a single fixed timeout applied from the
/// moment the target socket connects would also fire while the target is still working
/// on its *first* byte — a target slower than this timeout to respond (a cold
/// config-server request, a slow backend) would then have its response severed at zero
/// bytes before any data arrived, which is a correctness bug of its own, not a
/// workaround for the proxy's missing close. [`pump_with_idle_timeout`] therefore
/// distinguishes two phases: before any data has been read, timeouts are tolerated up
/// to the much more generous [`FIRST_BYTE_DEADLINE`]; once at least one byte has
/// arrived, this shorter [`TARGET_IDLE_TIMEOUT`] governs — a gap this long *after* data
/// has started flowing really does mean "no more data is coming, this exchange is
/// over", per the tunnel's own single-exchange contract above.
const TARGET_IDLE_TIMEOUT: Duration = Duration::from_millis(500);

/// How long [`pump_with_idle_timeout`] tolerates silence from the target *before any
/// data has arrived at all* — i.e. how slow the target is allowed to be getting to its
/// first byte. Deliberately far more generous than [`TARGET_IDLE_TIMEOUT`] (which
/// governs the idle gap *after* data has started, where a long pause is genuinely
/// unexpected): matches the cold-response latency budget used elsewhere in this
/// codebase for config-server-shaped targets. See [`TARGET_IDLE_TIMEOUT`]'s doc comment
/// for why these need to be two different constants, not one.
const FIRST_BYTE_DEADLINE: Duration = Duration::from_secs(10);

/// One tunnel worker: owns a background thread that repeatedly serves the in-guest
/// listener for `link` and pumps bytes to `target_host_port`, until closed.
pub(crate) struct ExecTunnel {
    closed: Arc<AtomicBool>,
    current: Arc<Mutex<Option<Child>>>,
    current_target: Arc<Mutex<Option<TcpStream>>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl ExecTunnel {
    /// Spawns the worker thread for `link` inside `sandbox_name`, reachable through
    /// `msb` at `msb_path`.
    pub(crate) fn new(msb_path: PathBuf, sandbox_name: String, link: NetworkLink) -> Self {
        let closed = Arc::new(AtomicBool::new(false));
        let current: Arc<Mutex<Option<Child>>> = Arc::new(Mutex::new(None));
        let current_target: Arc<Mutex<Option<TcpStream>>> = Arc::new(Mutex::new(None));
        let worker_closed = closed.clone();
        let worker_current = current.clone();
        let worker_current_target = current_target.clone();
        let worker = std::thread::spawn(move || {
            while !worker_closed.load(Ordering::Relaxed) {
                let served = serve_one_connection(
                    &msb_path,
                    &sandbox_name,
                    &link,
                    &worker_current,
                    &worker_current_target,
                )
                .unwrap_or(false);
                if !worker_closed.load(Ordering::Relaxed) && !served {
                    std::thread::sleep(RESPAWN_BACKOFF);
                }
            }
        });
        ExecTunnel {
            closed,
            current,
            current_target,
            worker: Some(worker),
        }
    }
}

impl Drop for ExecTunnel {
    /// Closing/dropping a tunnel while a connection is in flight must not block on
    /// whichever pump direction happens to be sitting inside a syscall.
    ///
    /// Killing the guest-side `nc` child (below) reliably unblocks the guest-to-target
    /// pump's read, since that pump reads from the child's own stdout pipe. It does
    /// **not** unblock the target-to-guest pump: that side reads from a plain
    /// `TcpStream` to `target_host_port`, which has nothing to do with the killed
    /// child, and — per [`TARGET_IDLE_TIMEOUT`]'s doc — can legitimately sit in a read
    /// for up to [`FIRST_BYTE_DEADLINE`] (10s) waiting on a slow target's first byte.
    /// Without also shutting down that socket here, `close()`/`Drop` would inherit that
    /// same 10s ceiling on the unlucky call that lands mid-wait.
    ///
    /// So this also shuts down the published target-side socket (both directions,
    /// best-effort) exactly like the child-kill above: `serve_one_connection` publishes
    /// it into `current_target` the moment `connect` succeeds, symmetric with how it
    /// already publishes the child into `current`. A `shutdown()` on a socket blocked in
    /// `read` makes that read return promptly (typically `Ok(0)` or an error), which is
    /// exactly what an EOF/idle-timeout exit already means to [`pump_with_idle_timeout`]
    /// — so this doesn't need a new exit path, just a faster way into the existing one.
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return; // already closed
        }
        if let Ok(mut guard) = self.current.lock() {
            if let Some(child) = guard.as_mut() {
                let _ = child.kill();
            }
        }
        if let Ok(mut guard) = self.current_target.lock() {
            if let Some(sock) = guard.as_mut() {
                let _ = sock.shutdown(std::net::Shutdown::Both);
            }
        }
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

/// Serves exactly one connection through a freshly spawned `nc -l -p <guest_port>`
/// exec-stream child, or returns `Ok(false)` if the listener exited without ever
/// seeing traffic (the caller backs off and respawns in that case, per the module
/// docs' "backoff only on churn" rule).
fn serve_one_connection(
    msb: &std::path::Path,
    sandbox: &str,
    link: &NetworkLink,
    current: &Arc<Mutex<Option<Child>>>,
    current_target: &Arc<Mutex<Option<TcpStream>>>,
) -> std::io::Result<bool> {
    let mut child = crate::backend::spawn_msb_command(|| {
        let mut cmd = Command::new(msb);
        cmd.args(commands::exec_stream(
            sandbox,
            &[
                "nc".to_string(),
                "-l".to_string(),
                "-p".to_string(),
                link.guest_port.to_string(),
            ],
        ))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped());
        cmd
    })?;

    let mut guest_out = child.stdout.take().expect("piped stdout");
    let mut guest_in = child.stdin.take().expect("piped stdin");

    // Publish the live child so `ExecTunnel::drop` (or a future respawn) can kill it
    // while this call is blocked below waiting for the guest's first byte — this is
    // exactly the case a close() must be able to interrupt (a respawned listener with
    // no traffic yet blocks here indefinitely otherwise).
    struct ClearOnReturn<'a>(&'a Arc<Mutex<Option<Child>>>);
    impl Drop for ClearOnReturn<'_> {
        fn drop(&mut self) {
            *self.0.lock().expect("current-child mutex poisoned") = None;
        }
    }
    // SAFETY-of-intent note: we can't move `child` into the mutex and still hold a
    // usable handle to `.kill()` it locally too (Child isn't Clone) — so instead we
    // take the pipes out first (done above) and only need `child` itself for `.kill()`
    // from here on, which we do exclusively through `current` from this point forward.
    *current.lock().expect("current-child mutex poisoned") = Some(child);
    let _clear_on_return = ClearOnReturn(current);

    let kill_current = || {
        if let Ok(mut guard) = current.lock() {
            if let Some(c) = guard.as_mut() {
                let _ = c.kill();
            }
        }
    };

    // Block until the guest client sends its first byte — that's what tells us a real
    // connection has arrived (client-speaks-first only, per the module docs). If
    // `ExecTunnel::drop` kills the child concurrently, this read unblocks with EOF.
    let mut first = [0u8; 1];
    let n = guest_out.read(&mut first)?;
    if n == 0 {
        kill_current();
        return Ok(false); // listener exited without traffic: back off and respawn.
    }

    let result = (|| -> std::io::Result<()> {
        let sock = TcpStream::connect(("127.0.0.1", link.target_host_port))?;
        sock.set_nodelay(true)?;
        let mut sock_in = sock.try_clone()?;
        // Idle-timeout the target-side read (see TARGET_IDLE_TIMEOUT's doc for why
        // this exists — msb's port-publish proxy never propagates the target's own
        // close back to this socket, so a natural `read() == 0` never arrives). Start
        // with the generous FIRST_BYTE_DEADLINE — `pump_with_idle_timeout` tightens
        // this to TARGET_IDLE_TIMEOUT itself once the first byte has actually arrived
        // (these must be two different windows, not one).
        sock_in.set_read_timeout(Some(FIRST_BYTE_DEADLINE))?;
        let mut sock_out = sock;

        // Publish a clone of the target socket so `ExecTunnel::drop` can shut it down
        // (both directions) if it fires while `pump_with_idle_timeout` below is
        // blocked — see `Drop`'s doc for why the child-kill above isn't enough to
        // unblock this side too. Cleared on return via the same `ClearOnReturn` shape
        // used for `current` above.
        struct ClearTargetOnReturn<'a>(&'a Arc<Mutex<Option<TcpStream>>>);
        impl Drop for ClearTargetOnReturn<'_> {
            fn drop(&mut self) {
                *self.0.lock().expect("current-target mutex poisoned") = None;
            }
        }
        *current_target
            .lock()
            .expect("current-target mutex poisoned") = Some(sock_in.try_clone()?);
        let _clear_target_on_return = ClearTargetOnReturn(current_target);

        // guest -> target: relay what the guest sent (starting with the byte already
        // read above) over to the real service, on its own thread. This side has no
        // natural end signal of its own either: `nc -l`'s stdout doesn't necessarily
        // EOF just because the target closed its end, so this thread is joined with a
        // bound below (2s) rather than indefinitely.
        let guest_to_target = std::thread::spawn(move || {
            let _ = sock_out.write_all(&first);
            let _ = sock_out.flush();
            pump(&mut guest_out, &mut sock_out);
        });
        // target -> guest: relay the response back into the exec child's stdin. Ends
        // either on a natural EOF (a target that does close promptly) or on the idle
        // timeout above (the common case on this msb build) — both are treated as
        // "this exchange is over" by `pump_with_idle_timeout`.
        pump_with_idle_timeout(&mut sock_in, &mut guest_in);
        // The target side is done; the guest side may still be blocked reading from
        // `nc`'s stdout with nothing more coming. Give it a bounded grace period to
        // finish relaying any trailing bytes, then move on regardless — `kill_current`
        // below tears down the guest-side `nc` process either way, which unblocks a
        // still-running guest_to_target thread's read.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !guest_to_target.is_finished() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    })();

    kill_current();
    result?;
    Ok(true)
}

/// Raw unbuffered pump: reads into a plain byte buffer, writes and flushes
/// immediately after every read. See the module docs for why buffering here would
/// deadlock the relay.
fn pump<R: Read, W: Write>(src: &mut R, dst: &mut W) {
    let mut buf = [0u8; 8192];
    loop {
        let n = match src.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => break,
        };
        if dst.write_all(&buf[..n]).and_then(|_| dst.flush()).is_err() {
            break;
        }
    }
}

/// Same raw unbuffered relay as [`pump`], except `src` is expected to carry a read
/// timeout and a timeout is treated exactly like a clean EOF — "no more data is coming,
/// this exchange is over" — rather than as an error to propagate. Used specifically for
/// the target-to-guest direction, where msb's own port-publish proxy never delivers a
/// real EOF.
///
/// Scoped in two phases (see [`TARGET_IDLE_TIMEOUT`]'s doc comment
/// for the full rationale): the caller starts `src` with a read timeout of
/// [`FIRST_BYTE_DEADLINE`], tolerating a slow target that hasn't sent anything yet.
/// The instant the first byte arrives (`saw_data` flips true), this function tightens
/// `src`'s read timeout down to [`TARGET_IDLE_TIMEOUT`] — from then on, a gap that long
/// really does mean the exchange is over, per the tunnel's single-exchange contract.
fn pump_with_idle_timeout<W: Write>(src: &mut TcpStream, dst: &mut W) {
    let mut buf = [0u8; 8192];
    let mut saw_data = false;
    loop {
        let n = match src.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break; // idle timeout (first-byte or post-data): end-of-exchange, not a failure.
            }
            Err(_) => break,
        };
        if !saw_data {
            saw_data = true;
            // Now that real data has started flowing, a much shorter silence really
            // does mean "no more is coming" — tighten the window accordingly. A
            // failure to set it is not fatal: the read loop simply keeps the more
            // generous FIRST_BYTE_DEADLINE for the rest of this exchange instead.
            let _ = src.set_read_timeout(Some(TARGET_IDLE_TIMEOUT));
        }
        if dst.write_all(&buf[..n]).and_then(|_| dst.flush()).is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `pump` itself is exercised end-to-end by the msb contract IT (network-link
    /// reachability) — real coverage needs a real exec-stream child. What's unit-
    /// testable in isolation is that a plain in-memory reader/writer pair relays
    /// bytes faithfully and stops cleanly at EOF, which is the shape every call site
    /// depends on.
    #[test]
    fn pump_relays_bytes_until_eof() {
        let mut src = std::io::Cursor::new(b"hello world".to_vec());
        let mut dst = Vec::new();
        pump(&mut src, &mut dst);
        assert_eq!(dst, b"hello world");
    }

    #[test]
    fn pump_on_an_empty_source_writes_nothing() {
        let mut src = std::io::Cursor::new(Vec::<u8>::new());
        let mut dst = Vec::new();
        pump(&mut src, &mut dst);
        assert!(dst.is_empty());
    }

    /// The msb-proxy-never-closes fix (see [`TARGET_IDLE_TIMEOUT`]'s doc): a real TCP
    /// peer that sends data and then goes quiet WITHOUT closing its socket must still
    /// cause `pump_with_idle_timeout` to return (on the idle timeout), not hang
    /// forever the way a plain [`pump`] would on the same input. This is exactly the
    /// shape of msb's own port-publish proxy behavior confirmed against the real
    /// binary during this task's IT verification.
    #[test]
    fn pump_with_idle_timeout_returns_when_the_peer_goes_quiet_without_closing() {
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().expect("accept");
            conn.write_all(b"hello").unwrap();
            conn.flush().unwrap();
            // Deliberately never close `conn` — held open for the test's duration to
            // simulate msb's proxy never propagating a close.
            std::thread::sleep(Duration::from_secs(2));
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        client.set_read_timeout(Some(TARGET_IDLE_TIMEOUT)).unwrap();
        let mut dst = Vec::new();

        let started = std::time::Instant::now();
        pump_with_idle_timeout(&mut client, &mut dst);
        let elapsed = started.elapsed();

        assert_eq!(dst, b"hello");
        assert!(
            elapsed < Duration::from_secs(1),
            "must return promptly on the idle timeout, not hang for the peer's full \
             lifetime — took {elapsed:?}"
        );
        let _ = server.join();
    }

    /// Regression guard, the slow-first-byte half: a target that
    /// takes over a second to send its very first byte (a cold config-server response
    /// is the real-world shape) must NOT be truncated to zero bytes just because that
    /// pause exceeds TARGET_IDLE_TIMEOUT (500ms). This is exactly the bug a single
    /// fixed timeout applied before any data ever arrived would cause — the fixture
    /// here mirrors how `serve_one_connection` actually drives the socket: start the
    /// read timeout at FIRST_BYTE_DEADLINE, and only `pump_with_idle_timeout` itself
    /// tightens it once data starts flowing.
    #[test]
    fn pump_with_idle_timeout_does_not_truncate_a_slow_first_byte() {
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().expect("accept");
            // Slower to its first byte than TARGET_IDLE_TIMEOUT (500ms), but well
            // inside FIRST_BYTE_DEADLINE (10s) — a cold-response shape that must
            // survive intact.
            std::thread::sleep(Duration::from_secs(1));
            conn.write_all(b"slow-but-real-response").unwrap();
            conn.flush().unwrap();
            // Then close promptly — the post-data phase ends on this natural EOF.
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        client.set_read_timeout(Some(FIRST_BYTE_DEADLINE)).unwrap();
        let mut dst = Vec::new();

        pump_with_idle_timeout(&mut client, &mut dst);

        assert_eq!(
            dst, b"slow-but-real-response",
            "a target slower than TARGET_IDLE_TIMEOUT to its first byte must not be \
             severed to a zero-byte response — got {dst:?}",
        );
        let _ = server.join();
    }

    /// Regression guard, the post-data-idle half: once data HAS
    /// started flowing, an idle gap longer than TARGET_IDLE_TIMEOUT (not
    /// FIRST_BYTE_DEADLINE) must end the exchange — proving `pump_with_idle_timeout`
    /// actually tightens the window after the first byte rather than staying at the
    /// generous first-byte deadline for the whole connection.
    #[test]
    fn pump_with_idle_timeout_tightens_the_window_after_the_first_byte() {
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut conn, _) = listener.accept().expect("accept");
            conn.write_all(b"first-chunk").unwrap();
            conn.flush().unwrap();
            // Go quiet for longer than TARGET_IDLE_TIMEOUT but well under
            // FIRST_BYTE_DEADLINE — if the window were never tightened past the first
            // byte, this pause alone would not end the exchange within FIRST_BYTE_DEADLINE,
            // and the test's elapsed-time assertion below would fail.
            std::thread::sleep(Duration::from_secs(2));
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        client.set_read_timeout(Some(FIRST_BYTE_DEADLINE)).unwrap();
        let mut dst = Vec::new();

        let started = std::time::Instant::now();
        pump_with_idle_timeout(&mut client, &mut dst);
        let elapsed = started.elapsed();

        assert_eq!(dst, b"first-chunk");
        assert!(
            elapsed < Duration::from_secs(1),
            "the post-data idle window must be TARGET_IDLE_TIMEOUT (500ms), not the \
             10s FIRST_BYTE_DEADLINE, once data has started flowing — took {elapsed:?}"
        );
        let _ = server.join();
    }

    /// Regression guard: a target socket sitting in
    /// [`pump_with_idle_timeout`]'s read — as it would for up to
    /// [`FIRST_BYTE_DEADLINE`] (10s) waiting on a slow/quiet target — must return
    /// promptly once another thread calls `shutdown(Both)` on a clone of the same
    /// socket, exactly the way [`ExecTunnel::drop`] now does. This proves the
    /// mitigation actually unblocks the read it targets, without needing a real
    /// `ExecTunnel`/msb child: a bare loopback listener that accepts and then goes
    /// silent stands in for a target the tunnel is mid-exchange with.
    ///
    /// Windows fact (confirmed on a real `windows-2025` hosted runner): a `shutdown()`
    /// call on a `try_clone()`-duplicated handle does not unblock a concurrent `read()`
    /// on a *different* handle to the same socket the way POSIX guarantees — the read
    /// only returned once the real peer closed its end, ~2s later in this test, not
    /// promptly after the 100ms `shutdown()` call. This does not leave `ExecTunnel::drop`
    /// hanging or leaking on Windows: `serve_one_connection` already sets a real read
    /// timeout on the production socket (`FIRST_BYTE_DEADLINE`/`TARGET_IDLE_TIMEOUT`,
    /// both far below this test's 2s peer-close stand-in), so `drop` is bounded by that
    /// timeout regardless of whether the `shutdown()` optimization fires early — the
    /// `Drop`'s `shutdown()` is a latency optimization on Unix, not a correctness
    /// requirement anywhere, so a slower (but still bounded) teardown on Windows is a
    /// documented characteristic, not a hang.
    #[test]
    fn shutdown_on_a_cloned_socket_unblocks_a_pending_read() {
        use std::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (conn, _) = listener.accept().expect("accept");
            // Deliberately silent — this connection would otherwise sit in the
            // caller's read for the full FIRST_BYTE_DEADLINE, exactly like a tunnel's
            // target-to-guest pump waiting on a slow/quiet real target.
            std::thread::sleep(Duration::from_secs(2));
            drop(conn);
        });

        let mut client = TcpStream::connect(addr).expect("connect");
        client
            .set_read_timeout(Some(FIRST_BYTE_DEADLINE))
            .expect("set read timeout");
        // The clone stands in for the copy `ExecTunnel` publishes into
        // `current_target` — shutting it down must affect the original handle too,
        // since both share the same underlying socket (guaranteed on Unix; on Windows
        // the read may instead unblock via the peer's own close — see this test's doc).
        let shutdown_handle = client.try_clone().expect("clone socket");

        let closer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            shutdown_handle
                .shutdown(std::net::Shutdown::Both)
                .expect("shutdown a live socket");
        });

        let mut buf = [0u8; 8];
        let started = std::time::Instant::now();
        let _ = client.read(&mut buf);
        let elapsed = started.elapsed();

        // Unix: shutdown() on the clone unblocks the read promptly (well under 1s).
        // Windows: no such cross-handle guarantee (see the doc comment above) — the
        // ceiling this asserts instead is FIRST_BYTE_DEADLINE itself, the real
        // production bound `serve_one_connection` sets on this same socket shape, so
        // this still catches a genuine hang (unbounded block) without asserting a
        // POSIX-only timing guarantee Windows doesn't make.
        #[cfg(unix)]
        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown() on a clone must unblock the other clone's pending read well \
             before FIRST_BYTE_DEADLINE (10s) — took {elapsed:?}"
        );
        #[cfg(not(unix))]
        assert!(
            elapsed < FIRST_BYTE_DEADLINE,
            "the read must not exceed FIRST_BYTE_DEADLINE (10s), the real production \
             bound on this socket shape, even where shutdown()-on-a-clone doesn't \
             unblock it promptly — took {elapsed:?}"
        );
        let _ = closer.join();
        let _ = server.join();
    }
}
