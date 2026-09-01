//! The platform transport underneath the Docker daemon connection: a unix domain
//! socket on unix, Docker Desktop's named pipe (`\\.\pipe\docker_engine`) on Windows.
//!
//! **Shape chosen, and why:** `client.rs`/`frames.rs` are written against a single
//! crate-internal type, [`DockerStream`] (async) and [`BlockingDockerStream`]
//! (blocking), each a two-armed enum — one arm per platform — implementing
//! `AsyncRead`/`AsyncWrite` (respectively `Read`/`Write`) by delegating to whichever
//! arm is actually compiled in. The alternative the brief also floated — making
//! `DockerClient`/`BodyReader`/the blocking helpers generic over `AsyncRead +
//! AsyncWrite + Unpin + Send` (or their blocking equivalents) — would have meant
//! threading a type parameter through every function signature in `client.rs` and
//! `frames.rs` (a dozen-plus `fn`s taking `&mut Stream`) purely to describe a choice
//! that is actually fixed once per process, at `DockerClient` construction, and never
//! varies per call. The enum keeps every one of those signatures a one-word rename
//! (`UnixStream` → `DockerStream`) instead of a generic parameter, with no
//! monomorphization blowup and no `Box<dyn ...>` indirection either — the smallest
//! diff that still gives both platforms a real transport.
//!
//! Both enums are two-armed only in the sense that the source mentions both variants;
//! on any given target only one variant actually exists (`#[cfg(unix)]`/
//! `#[cfg(windows)]` on each), so there is nothing to match on the "wrong" platform —
//! the enum is really "the one stream type this platform has," named the same way on
//! both.
//!
//! **Blocking-path timeouts:** unix arms a per-handle deadline directly on the socket
//! (`SO_RCVTIMEO`/`SO_SNDTIMEO`, in [`connect_blocking`]); a Windows named-pipe handle
//! opened without `FILE_FLAG_OVERLAPPED` has no equivalent knob, so the Windows callers
//! of `connect_blocking` (`DockerBackendProvider::is_supported`, the `Drop`-path
//! cleanup helpers) instead bound the *calling* thread's wait with
//! [`run_with_deadline`], which runs the whole connect-plus-request round trip on a
//! detached thread and times out the wait on it rather than the I/O itself.

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(windows)]
use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeClient};

/// `ERROR_PIPE_BUSY` (WinError.h) — every named-pipe instance is currently claimed by
/// another client. Hardcoded rather than pulling in `windows-sys` for this one stable,
/// long-documented constant: this crate takes on no dependency beyond serde/tokio/
/// thiserror/async-trait (see `client.rs`'s module doc for the same budget applied to
/// the HTTP stack itself), and a well-known Win32 error code numeral doesn't earn an
/// exception to that.
#[cfg(windows)]
const ERROR_PIPE_BUSY: i32 = 231;

/// The async, per-request connection to the Docker daemon — see the module docs for
/// why this is a two-armed enum rather than a generic type parameter.
pub(crate) enum DockerStream {
    #[cfg(unix)]
    Unix(UnixStream),
    #[cfg(windows)]
    Pipe(NamedPipeClient),
}

impl AsyncRead for DockerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            DockerStream::Unix(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(windows)]
            DockerStream::Pipe(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for DockerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            #[cfg(unix)]
            DockerStream::Unix(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            DockerStream::Pipe(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            DockerStream::Unix(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            DockerStream::Pipe(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            #[cfg(unix)]
            DockerStream::Unix(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            DockerStream::Pipe(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Opens a fresh [`DockerStream`] to `target` (a unix socket path on unix, a named
/// pipe path such as `\\.\pipe\docker_engine` on Windows) — the one place either
/// platform's connect primitive is actually invoked; everything above and in
/// `client.rs` is written against the resulting [`DockerStream`] alone.
#[cfg(unix)]
pub(crate) async fn connect_async(target: &Path) -> io::Result<DockerStream> {
    UnixStream::connect(target).await.map(DockerStream::Unix)
}

/// Windows counterpart of [`connect_async`] (unix arm above). `ClientOptions::open` is
/// the standard tokio connect primitive for a named pipe client — it can fail with
/// `ERROR_PIPE_BUSY` when every instance of the pipe the daemon created is momentarily
/// claimed by another client; retrying after a short sleep is the pattern tokio's own
/// docs recommend for exactly this. The loop has no attempt cap of its own because its
/// only caller ([`crate::client::DockerClient::connect`]) already runs inside the
/// per-request [`crate::client::RESPONSE_TIMEOUT`] budget, which bounds it from the
/// outside instead.
#[cfg(windows)]
pub(crate) async fn connect_async(target: &Path) -> io::Result<DockerStream> {
    loop {
        match ClientOptions::new().open(target) {
            Ok(client) => return Ok(DockerStream::Pipe(client)),
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// The blocking (`std`, no Tokio) counterpart to [`DockerStream`] — used by the
/// handful of synchronous transport paths that run with no async runtime in scope: the
/// `Drop`-path cleanup thread (`DockerBackend::cleanup_sync`/`remove_by_name`) and
/// `DockerBackendProvider::is_supported`'s pre-runtime probe. Same rationale as
/// `DockerStream` for why this is an enum rather than a generic parameter.
pub(crate) enum BlockingDockerStream {
    #[cfg(unix)]
    Unix(std::os::unix::net::UnixStream),
    #[cfg(windows)]
    Pipe(std::fs::File),
}

impl io::Read for BlockingDockerStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            BlockingDockerStream::Unix(s) => s.read(buf),
            #[cfg(windows)]
            BlockingDockerStream::Pipe(s) => s.read(buf),
        }
    }
}

impl io::Write for BlockingDockerStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            #[cfg(unix)]
            BlockingDockerStream::Unix(s) => s.write(buf),
            #[cfg(windows)]
            BlockingDockerStream::Pipe(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            BlockingDockerStream::Unix(s) => s.flush(),
            #[cfg(windows)]
            BlockingDockerStream::Pipe(s) => s.flush(),
        }
    }
}

/// Opens a blocking [`BlockingDockerStream`] to `target`. On unix this also arms
/// `timeout` as the socket's read/write deadline (`SO_RCVTIMEO`/`SO_SNDTIMEO`) so a
/// wedged daemon can't hang the caller — the same behavior these call sites relied on
/// before this module existed. Windows has no equivalent per-handle timeout knob for a
/// synchronously-opened named pipe (that needs overlapped I/O, which a `Drop`-path
/// thread with no async runtime has no use for), so `timeout` is accepted but unused
/// here; every caller of this function instead gets its deadline enforced one layer up,
/// by wrapping the whole connect-plus-request round trip in [`run_with_deadline`] — see
/// that function's doc for why a per-handle knob isn't what actually matters here.
#[cfg(unix)]
pub(crate) fn connect_blocking(
    target: &Path,
    timeout: Duration,
) -> io::Result<BlockingDockerStream> {
    let stream = std::os::unix::net::UnixStream::connect(target)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    Ok(BlockingDockerStream::Unix(stream))
}

/// Windows counterpart of [`connect_blocking`] (unix arm above). Opening a named pipe
/// path with plain `read+write` `OpenOptions` (no `FILE_FLAG_OVERLAPPED`) is the
/// standard way to get a synchronous, blocking duplex handle to it — `std::fs::File`'s
/// `Read`/`Write` impls call `ReadFile`/`WriteFile` under the hood, which work
/// identically on a pipe handle as on a real file. No `ERROR_PIPE_BUSY` retry here
/// (contrast [`connect_async`]'s loop): every caller of this function is a best-effort,
/// meant-to-be-cheap path (a `Drop`-path teardown, a supportability probe), not a
/// request the rest of the program is blocked on, so a momentarily-busy pipe should
/// surface as "didn't work this time" rather than retry-and-block the caller's thread.
#[cfg(windows)]
pub(crate) fn connect_blocking(
    target: &Path,
    _timeout: Duration,
) -> io::Result<BlockingDockerStream> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(target)?;
    Ok(BlockingDockerStream::Pipe(file))
}

/// Bounds how long the *calling* thread waits for `f` (a blocking connect-plus-request
/// round trip over [`BlockingDockerStream`]) on Windows, where [`connect_blocking`]'s
/// plain synchronous pipe handle has no per-read/write deadline of its own (that needs
/// `FILE_FLAG_OVERLAPPED` plus `GetOverlappedResult`, raw WinAPI FFI this crate doesn't
/// otherwise carry — see [`connect_blocking`]'s Windows doc). Runs `f` on a fresh,
/// detached OS thread and waits on a channel for at most `timeout`: if `f` finishes in
/// time its result is returned as-is; if it doesn't, this returns a `TimedOut` error
/// immediately and abandons the spawned thread to finish (or stay wedged against the
/// daemon) on its own. Every caller of this function already treats the whole
/// operation as best-effort — errors swallowed, no state the rest of the program
/// depends on the abandoned thread ever releasing — so a leaked thread here costs
/// nothing a caller relies on; what it buys is the actual property unix gets for free
/// from `SO_RCVTIMEO`/`SO_SNDTIMEO`: `DockerBackendProvider::is_supported()` and the
/// `Drop`-path cleanup calls can no longer hang the calling thread forever against a
/// named pipe that accepts connections but sits behind a wedged daemon.
#[cfg(windows)]
pub(crate) fn run_with_deadline<T: Send + 'static>(
    timeout: Duration,
    f: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        // The receiver may already be gone (we timed out and returned) — a failed
        // send just means nobody's listening any more, which is fine to ignore.
        let _ = tx.send(f());
    });
    rx.recv_timeout(timeout).unwrap_or_else(|_| {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "Docker named-pipe request did not complete before the deadline",
        ))
    })
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn run_with_deadline_returns_the_inner_result_when_it_finishes_in_time() {
        let result = run_with_deadline(Duration::from_secs(5), || Ok::<_, io::Error>(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn run_with_deadline_propagates_an_inner_error_when_it_finishes_in_time() {
        let result = run_with_deadline(Duration::from_secs(5), || {
            Err::<(), _>(io::Error::new(io::ErrorKind::NotFound, "nope"))
        });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn run_with_deadline_times_out_a_closure_that_never_finishes_in_the_budget() {
        let start = std::time::Instant::now();
        let result = run_with_deadline(Duration::from_millis(20), || {
            std::thread::sleep(Duration::from_secs(60));
            Ok::<_, io::Error>(())
        });
        // The calling thread must come back well before the closure's own sleep does —
        // that's the entire property this function exists to provide.
        assert!(start.elapsed() < Duration::from_secs(5));
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }
}
