//! The Docker daemon's log-stream multiplexing frame format, and [`LineAssembler`],
//! which turns a sequence of raw payload chunks back into complete lines.
//!
//! **The frame format** (present whenever a container is created without a TTY — this
//! backend never allocates one, so this is the only shape it ever sees):
//! ```text
//! frame   = header ++ payload
//! header  = [ stream_type: u8, 0u8, 0u8, 0u8, len: u32_be ]   // 8 bytes
//! payload = [u8; len]
//! stream_type: 0 = stdin, 1 = stdout, 2 = stderr
//! ```
//! `exec` accumulates stdout/stderr into separate buffers (→ [`crate::client`]'s
//! caller builds an `ExecResult` from them). `logs`/`follow_logs` concatenate payloads
//! regardless of stream and hand the text to a [`LineAssembler`], since the contract
//! surface (`ContainerGuard::logs`/`follow_output`) doesn't distinguish stdout from
//! stderr.
//!
//! **Why a `LineAssembler` at all:** frame boundaries are a chunking artifact
//! of however the daemon happened to flush its write buffer — they have nothing to do
//! with line boundaries in the underlying text. A single log line can straddle two (or
//! more) frames. [`LineAssembler::feed`] buffers a trailing partial line across calls
//! and returns only the lines a given call completes; [`LineAssembler::flush`] hands
//! back that trailing fragment once at stream end (idempotent, since a terminal
//! callback in some callers can legitimately fire more than once for the same close).

use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;

use rightsize::error::{Result, RightsizeError};

use crate::client::ParsedHeaders;

/// Which of the container's own streams a frame's payload came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StreamType {
    Stdin,
    Stdout,
    Stderr,
}

impl StreamType {
    fn from_byte(b: u8) -> Option<StreamType> {
        match b {
            0 => Some(StreamType::Stdin),
            1 => Some(StreamType::Stdout),
            2 => Some(StreamType::Stderr),
            _ => None,
        }
    }
}

/// One demultiplexed frame: which stream it came from, and its payload bytes.
pub(crate) struct Frame {
    pub(crate) stream: StreamType,
    pub(crate) payload: Vec<u8>,
}

/// Wraps a still-open `UnixStream` positioned right after the response headers (as
/// returned by [`crate::client::DockerClient::request_stream`]) and reads raw bytes
/// off it regardless of whether the body is `Transfer-Encoding: chunked` or a fixed
/// `Content-Length` — the frame demuxer below only ever wants "give me N more raw body
/// bytes," not two different framings to worry about.
pub(crate) struct BodyReader<'a> {
    stream: &'a mut UnixStream,
    chunked: bool,
    /// Bytes remaining in the current unit: the whole body for `Content-Length` (or
    /// the read-until-close fallback, seeded to `usize::MAX`), or the current chunk
    /// for `chunked` — refilled by [`Self::next_chunk_size`] once it hits zero.
    remaining_in_unit: usize,
    /// Set once no more bytes will ever be read: the `Content-Length` budget ran out,
    /// a zero-size chunk terminator was seen, or (the read-until-close fallback) the
    /// peer closed the connection.
    exhausted: bool,
}

impl<'a> BodyReader<'a> {
    pub(crate) fn new(stream: &'a mut UnixStream, headers: &ParsedHeaders) -> Self {
        BodyReader {
            stream,
            chunked: headers.chunked,
            remaining_in_unit: if headers.chunked {
                0 // nothing pulled yet — the first read_exact call will read a chunk-size line.
            } else {
                headers.content_length.unwrap_or(usize::MAX)
            },
            exhausted: false,
        }
    }

    /// Reads exactly `buf.len()` raw body bytes, dechunking transparently as needed.
    /// Returns `Ok(false)` if the body ended (cleanly) before `buf` could be filled at
    /// all — i.e. zero bytes were available, which the frame demuxer above treats as
    /// "no more frames." A partial fill followed by an unexpected end is an error, not
    /// a clean stop, since that can only mean a truncated/malformed body.
    ///
    /// **Why this uses `read()` in a loop rather than `read_exact()` directly:** in
    /// the no-`Content-Length`/non-chunked mode (`follow_logs`-style "read until the
    /// daemon closes the connection"), `remaining_in_unit` starts at `usize::MAX` and
    /// never naturally reaches zero — the *only* signal that the body has ended is the
    /// peer closing the socket mid-read. `read_exact()` treats that as
    /// `UnexpectedEof`, indistinguishable from a genuinely truncated body; a plain
    /// `read()` returning `Ok(0)` is how this function tells a clean close (at a frame
    /// boundary — the expected, common case) apart from a truncated one (mid-frame —
    /// still an error, via the `filled < buf.len()` check below).
    async fn read_exact_or_eof(&mut self, buf: &mut [u8]) -> Result<bool> {
        let mut filled = 0;
        while filled < buf.len() {
            if self.remaining_in_unit == 0 {
                if self.exhausted {
                    break;
                }
                if self.chunked {
                    self.remaining_in_unit = self.next_chunk_size().await?;
                    if self.remaining_in_unit == 0 {
                        self.drain_trailers().await?;
                        self.exhausted = true;
                        break;
                    }
                } else {
                    self.exhausted = true;
                    break;
                }
            }
            let want = (buf.len() - filled).min(self.remaining_in_unit);
            let n = self
                .stream
                .read(&mut buf[filled..filled + want])
                .await
                .map_err(|e| {
                    RightsizeError::Backend(format!(
                        "reading a frame body from the Docker daemon: {e}"
                    ))
                })?;
            if n == 0 {
                self.exhausted = true;
                break; // clean close — see this method's doc for why this isn't an error here.
            }
            filled += n;
            self.remaining_in_unit -= n;
            if self.chunked && self.remaining_in_unit == 0 {
                // Consume the chunk's trailing CRLF before it could be mistaken for
                // the start of the next chunk-size line.
                let mut crlf = [0u8; 2];
                self.stream.read_exact(&mut crlf).await.map_err(|e| {
                    RightsizeError::Backend(format!(
                        "reading a chunk trailer from the Docker daemon: {e}"
                    ))
                })?;
            }
        }
        if filled == 0 {
            return Ok(false);
        }
        if filled < buf.len() {
            return Err(RightsizeError::Backend(
                "the Docker daemon's response body ended mid-frame".to_string(),
            ));
        }
        Ok(true)
    }

    async fn next_chunk_size(&mut self) -> Result<usize> {
        let line = self.read_line().await?;
        let size_str = line.split(';').next().unwrap_or("").trim();
        usize::from_str_radix(size_str, 16).map_err(|_| {
            RightsizeError::Backend(format!(
                "could not parse a chunk size from the Docker daemon's response: {line:?}"
            ))
        })
    }

    async fn drain_trailers(&mut self) -> Result<()> {
        loop {
            let trailer = self.read_line().await?;
            if trailer.is_empty() {
                return Ok(());
            }
        }
    }

    async fn read_line(&mut self) -> Result<String> {
        let mut raw = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = self.stream.read(&mut byte).await.map_err(|e| {
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
        }
        Ok(String::from_utf8_lossy(&raw).into_owned())
    }
}

/// Reads and demultiplexes exactly one frame from `body`, returning `None` at a clean
/// end of stream (no more frames — this is the normal way a `logs`/`exec-start` stream
/// ends).
pub(crate) async fn read_frame(body: &mut BodyReader<'_>) -> Result<Option<Frame>> {
    let mut header = [0u8; 8];
    if !body.read_exact_or_eof(&mut header).await? {
        return Ok(None);
    }
    let stream = StreamType::from_byte(header[0]).ok_or_else(|| {
        RightsizeError::Backend(format!(
            "unrecognized Docker log-frame stream type byte {}",
            header[0]
        ))
    })?;
    let len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;
    let mut payload = vec![0u8; len];
    if len > 0 && !body.read_exact_or_eof(&mut payload).await? {
        return Err(RightsizeError::Backend(
            "the Docker daemon's response body ended mid-frame".to_string(),
        ));
    }
    Ok(Some(Frame { stream, payload }))
}

/// Reads every remaining frame from `body`, routing each payload to `on_stdout` or
/// `on_stderr` by [`StreamType`] — the shape `exec` needs (separate stdout/stderr
/// accumulation for [`rightsize::model::ExecResult`]).
pub(crate) async fn demux_into(
    body: &mut BodyReader<'_>,
    mut on_stdout: impl FnMut(&[u8]),
    mut on_stderr: impl FnMut(&[u8]),
) -> Result<()> {
    while let Some(frame) = read_frame(body).await? {
        match frame.stream {
            StreamType::Stdout => on_stdout(&frame.payload),
            StreamType::Stderr => on_stderr(&frame.payload),
            StreamType::Stdin => {} // never produced by the daemon in a response stream
        }
    }
    Ok(())
}

/// Reassembles complete lines out of a stream of text chunks whose boundaries are
/// chunking artifacts, not line breaks — see the module docs for the full
/// rationale. `feed` returns only the lines completed by this call, buffering any
/// trailing partial line to prepend to the next one; empty lines are dropped (an
/// artifact of a chunk boundary landing exactly on `\n`, not meaningful log content).
/// `flush` hands back that trailing fragment once at stream end — idempotent, since a
/// terminal callback can fire more than once for the same stream close.
#[derive(Default)]
pub(crate) struct LineAssembler {
    pending: String,
    flushed_once: bool,
}

impl LineAssembler {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Feeds `text` (already-decoded, possibly-partial-line) into the assembler,
    /// returning the non-empty lines this call completes. Any trailing partial line
    /// (no terminating `\n` yet) is buffered for the next `feed` or the final
    /// [`Self::flush`].
    pub(crate) fn feed(&mut self, text: &str) -> Vec<String> {
        self.pending.push_str(text);
        if !self.pending.contains('\n') {
            return Vec::new();
        }
        let ends_with_newline = self.pending.ends_with('\n');
        let mut parts: Vec<String> = self.pending.split('\n').map(str::to_string).collect();
        // `split('\n')` on "a\nb\n" yields ["a", "b", ""] — the trailing "" is the
        // "nothing pending" marker when the chunk ended exactly on a newline; on
        // "a\nb" (no trailing newline) it yields ["a", "b"], where "b" is genuinely
        // pending. Either way, the last element is never a completed line.
        let tail = parts.pop().unwrap_or_default();
        self.pending = if ends_with_newline {
            debug_assert!(tail.is_empty());
            String::new()
        } else {
            tail
        };
        parts.into_iter().filter(|l| !l.is_empty()).collect()
    }

    /// Returns the trailing unterminated fragment exactly once (`None` on every call
    /// after the first, or if there was never a trailing fragment to return).
    pub(crate) fn flush(&mut self) -> Option<String> {
        if self.flushed_once {
            return None;
        }
        self.flushed_once = true;
        if self.pending.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.pending))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- LineAssembler -----------------------------------------------------

    #[test]
    fn feed_returns_complete_lines_and_buffers_the_trailing_partial() {
        let mut a = LineAssembler::new();
        assert_eq!(a.feed("line1\nline2\npart"), vec!["line1", "line2"]);
        assert_eq!(a.feed("ial\n"), vec!["partial"]);
    }

    #[test]
    fn a_line_straddling_two_frames_reassembles() {
        let mut a = LineAssembler::new();
        assert!(a.feed("hello ").is_empty());
        assert_eq!(a.feed("world\n"), vec!["hello world"]);
    }

    #[test]
    fn feed_drops_empty_lines_from_a_chunk_boundary_landing_on_a_newline() {
        let mut a = LineAssembler::new();
        assert_eq!(a.feed("a\n\nb\n"), vec!["a", "b"]);
    }

    #[test]
    fn a_byte_at_a_time_fragmented_stream_still_reassembles() {
        let mut a = LineAssembler::new();
        let mut completed = Vec::new();
        for byte in "ab\ncd\n".bytes() {
            completed.extend(a.feed(&(byte as char).to_string()));
        }
        assert_eq!(completed, vec!["ab", "cd"]);
    }

    #[test]
    fn flush_returns_the_final_unterminated_fragment_exactly_once() {
        let mut a = LineAssembler::new();
        assert_eq!(a.feed("line1\ntrailing-no-newline"), vec!["line1"]);
        assert_eq!(a.flush(), Some("trailing-no-newline".to_string()));
        assert_eq!(a.flush(), None, "a second flush must not re-deliver it");
    }

    #[test]
    fn flush_on_a_stream_that_ended_exactly_on_a_newline_yields_nothing() {
        let mut a = LineAssembler::new();
        assert_eq!(a.feed("line1\n"), vec!["line1"]);
        assert_eq!(a.flush(), None);
    }

    #[test]
    fn flush_on_a_stream_with_no_input_at_all_yields_nothing() {
        let mut a = LineAssembler::new();
        assert_eq!(a.flush(), None);
    }

    #[test]
    fn feed_after_flush_is_harmless_but_flush_stays_at_most_once() {
        let mut a = LineAssembler::new();
        a.feed("first-partial");
        assert_eq!(a.flush(), Some("first-partial".to_string()));
        // A caller that (incorrectly) keeps feeding after flush doesn't panic; the
        // module's own contract is "call flush once at stream end," and this just
        // proves the at-most-once guarantee holds regardless of what happens after.
        a.feed("more\n");
        assert_eq!(a.flush(), None);
    }

    // --- frame demux (StreamType routing via demux_into over a fixed body) --

    /// Builds a raw frame: 8-byte header + payload.
    fn frame_bytes(stream_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![stream_type, 0, 0, 0];
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// A short-lived unix-socket fixture: binds a listener at a short `/tmp` path (see
    /// `client.rs`'s own fixture helper for why not `std::env::temp_dir()` — `SUN_LEN`),
    /// spawns a server task that writes `bytes` out in `chunk_size`-sized pieces with a
    /// tiny delay between each (simulating bytes arriving in separate reads), and
    /// hands back a connected client `UnixStream`. `read_frame`/`demux_into` are
    /// generic over `BodyReader`, which is itself hard-wired to `UnixStream` — this
    /// exercises them through a real socket rather than faking the reader type, so
    /// these tests take the exact code path production traffic does.
    struct FrameFixture {
        stream: UnixStream,
        server: tokio::task::JoinHandle<()>,
        dir: std::path::PathBuf,
    }

    impl FrameFixture {
        async fn serving(bytes: Vec<u8>, chunk_size: usize) -> Self {
            use tokio::io::AsyncWriteExt as _;
            use tokio::net::UnixListener;

            static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
            let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let dir = std::path::Path::new("/tmp").join(format!(
                "rzdf-{:x}-{:x}",
                std::process::id() as u16,
                seq
            ));
            std::fs::create_dir_all(&dir).expect("create fixture dir");
            let sock_path = dir.join("d.sock");
            let listener = UnixListener::bind(&sock_path).expect("bind fixture socket");

            let server = tokio::spawn(async move {
                let (mut conn, _) = listener.accept().await.expect("accept");
                for chunk in bytes.chunks(chunk_size.max(1)) {
                    conn.write_all(chunk).await.expect("write chunk");
                    if chunk_size < bytes.len() {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                }
                let _ = conn.shutdown().await;
            });

            let stream = UnixStream::connect(&sock_path).await.expect("connect");
            FrameFixture {
                stream,
                server,
                dir,
            }
        }

        /// A body reader over this fixture's connection, in "read until close" mode
        /// (no `Content-Length`/chunking) — matches how logs/exec streams behave.
        fn body(&mut self) -> BodyReader<'_> {
            BodyReader::new(
                &mut self.stream,
                &ParsedHeaders {
                    status: 200,
                    chunked: false,
                    content_length: None,
                },
            )
        }

        async fn finish(self) {
            self.server.await.expect("server task");
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test]
    async fn stdout_and_stderr_frames_are_routed_by_stream_type() {
        let mut bytes = Vec::new();
        bytes.extend(frame_bytes(1, b"out-line\n"));
        bytes.extend(frame_bytes(2, b"err-line\n"));
        let total_len = bytes.len();
        let mut fixture = FrameFixture::serving(bytes, total_len).await;

        let mut body = fixture.body();
        let mut frames = Vec::new();
        while let Some(f) = read_frame(&mut body).await.unwrap() {
            frames.push((f.stream, f.payload));
        }
        assert_eq!(
            frames,
            vec![
                (StreamType::Stdout, b"out-line\n".to_vec()),
                (StreamType::Stderr, b"err-line\n".to_vec()),
            ]
        );
        fixture.finish().await;
    }

    #[tokio::test]
    async fn empty_payload_frames_are_handled() {
        let mut bytes = Vec::new();
        bytes.extend(frame_bytes(1, b""));
        bytes.extend(frame_bytes(1, b"after-empty\n"));
        let total_len = bytes.len();
        let mut fixture = FrameFixture::serving(bytes, total_len).await;

        let mut body = fixture.body();
        let mut frames = Vec::new();
        while let Some(f) = read_frame(&mut body).await.unwrap() {
            frames.push((f.stream, f.payload));
        }
        assert_eq!(
            frames,
            vec![
                (StreamType::Stdout, Vec::new()),
                (StreamType::Stdout, b"after-empty\n".to_vec()),
            ]
        );
        fixture.finish().await;
    }

    #[tokio::test]
    async fn a_partial_frame_split_across_reads_still_reassembles() {
        // Bytes dribbled out 3 at a time (well inside a single 8-byte header, let
        // alone the payload) — `BodyReader`'s `read_exact` semantics must not care
        // that a frame's header and payload arrive in separate underlying reads.
        let bytes = frame_bytes(1, b"hello-across-frames");
        let mut fixture = FrameFixture::serving(bytes, 3).await;

        let mut body = fixture.body();
        let frame = read_frame(&mut body).await.unwrap().expect("one frame");
        assert_eq!(frame.stream, StreamType::Stdout);
        assert_eq!(frame.payload, b"hello-across-frames");
        assert!(read_frame(&mut body).await.unwrap().is_none());
        fixture.finish().await;
    }

    #[tokio::test]
    async fn demux_into_separates_stdout_and_stderr_for_exec_style_accumulation() {
        let mut bytes = Vec::new();
        bytes.extend(frame_bytes(1, b"out1"));
        bytes.extend(frame_bytes(2, b"err1"));
        bytes.extend(frame_bytes(1, b"out2"));
        let total_len = bytes.len();
        let mut fixture = FrameFixture::serving(bytes, total_len).await;

        let mut body = fixture.body();
        let mut out = Vec::new();
        let mut err = Vec::new();
        demux_into(
            &mut body,
            |b| out.extend_from_slice(b),
            |b| err.extend_from_slice(b),
        )
        .await
        .unwrap();
        assert_eq!(out, b"out1out2");
        assert_eq!(err, b"err1");
        fixture.finish().await;
    }
}
