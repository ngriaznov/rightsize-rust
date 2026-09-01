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
/// there; every caller of this function already treats the whole operation as
/// best-effort (errors swallowed, no caller left waiting on a specific deadline), so an
/// unbounded blocking read on a genuinely wedged Windows daemon is a corner case, not a
/// regression from a guarantee this path ever made on unix's own best-effort callers.
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
