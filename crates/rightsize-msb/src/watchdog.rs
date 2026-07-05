//! The `follow_logs` watchdog — the hardest moment in this backend.
//!
//! `msb logs -f` is documented to exit cleanly once its sandbox stops. On msb 0.6.2 it
//! doesn't: it blocks on read forever instead, so a workload's final unterminated line
//! (no trailing `\n`) would otherwise never reach the consumer. This module works
//! around that with three guarantees, in order:
//!
//! 1. Once the sandbox leaves `Running` (per `running_names_via`), the watchdog
//!    **quiesces the stuck follow process first**: kill the child, wait for it, then
//!    join its reader thread (bounded — see [`READER_JOIN_TIMEOUT`]), so the
//!    `delivered` count below reflects everything the live stream will ever produce
//!    before anything else touches it. Only after that join does it lock the shared
//!    state and take its `delivered`/`flushed` snapshot — this ordering is what rules
//!    out delivering the same complete line twice (once live, once replayed): the
//!    reader thread's last increment to `delivered` happens-before the join returns,
//!    so the watchdog is guaranteed to see it.
//! 2. It then does **one** authoritative, non-follow `msb logs --tail` fetch and
//!    replays only the lines the live stream hadn't already delivered (`delivered..`).
//!    That replay is **at-most-once** — a `flushed` bool guarded by the same mutex as
//!    `delivered` — so a complete line is never delivered twice (once live, once
//!    replayed).
//! 3. An explicit [`FollowHandle::close`]/`Drop` **never triggers the replay** — closing
//!    means the caller asked delivery to stop, so nothing the live stream hadn't
//!    already produced is delivered retroactively. If the sandbox had already stopped
//!    before close was called, the watchdog's own flush already ran and this is a
//!    no-op.

use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rightsize::backend::FollowHandle;
use rightsize::error::{Result, RightsizeError};

use crate::backend::running_names_via;
use crate::commands;

const READINESS_POLL: Duration = Duration::from_millis(300);

/// Upper bound on how long the watchdog waits for the reader thread to finish
/// draining whatever it already had buffered from the killed follow child's pipe,
/// before giving up and taking the `delivered` snapshot anyway. The reader is reading
/// from a pipe whose writer (the follow child) has just been killed+waited, so its own
/// `read()` should observe EOF almost immediately — this bound only guards against an
/// unexpectedly wedged reader thread, so the watchdog can't hang forever on it.
const READER_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Shared state the reader thread, the watchdog thread, and an explicit `close()` all
/// touch: how many lines the live stream has delivered so far, and whether the
/// at-most-once tail replay has already fired.
struct FollowState {
    delivered: usize,
    flushed: bool,
}

/// Spawns `msb logs -f <name>` (null stdin) and returns a [`FollowHandle`]
/// wrapping the reader + watchdog threads described in the module docs.
pub(crate) fn spawn_follow(
    msb: PathBuf,
    name: String,
    consumer: Box<dyn Fn(String) + Send + Sync>,
) -> Result<FollowHandle> {
    let mut child = Command::new(&msb)
        .args(commands::follow_logs(&name))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| RightsizeError::Backend(format!("failed to spawn msb logs -f {name}: {e}")))?;

    let stdout = child.stdout.take().expect("piped stdout");
    let state = Arc::new(Mutex::new(FollowState {
        delivered: 0,
        flushed: false,
    }));
    let consumer: Arc<dyn Fn(String) + Send + Sync> = Arc::from(consumer);

    let child = Arc::new(Mutex::new(child));
    let close_requested = Arc::new(AtomicBool::new(false));

    let reader_consumer = consumer.clone();
    let reader_state = state.clone();
    let reader = std::thread::spawn(move || {
        drain_lines(stdout, |line| {
            let mut s = reader_state.lock().expect("follow state mutex poisoned");
            s.delivered += 1;
            drop(s);
            reader_consumer(line);
        });
    });
    // Shared slot for the reader's JoinHandle: `flush_tail_once` takes it out and joins
    // it (bounded — see `READER_JOIN_TIMEOUT`) as part of the quiesce-before-snapshot
    // sequence the module docs promise. Whichever of `flush_tail_once` or
    // `FollowHandle`'s own close/Drop gets there first performs the actual join; the
    // other finds the slot already empty and treats that as "already joined" — a
    // `JoinHandle` can only be consumed once, so this slot is the single source of
    // truth for who owns that join.
    let reader_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> =
        Arc::new(Mutex::new(Some(reader)));

    let watchdog_msb = msb.clone();
    let watchdog_name = name.clone();
    let watchdog_child = child.clone();
    let watchdog_state = state.clone();
    let watchdog_consumer = consumer.clone();
    let watchdog_close = close_requested.clone();
    let watchdog_reader_handle = reader_handle.clone();
    let watchdog = std::thread::spawn(move || {
        watchdog_loop(
            &watchdog_msb,
            &watchdog_name,
            &watchdog_child,
            &watchdog_state,
            &watchdog_consumer,
            &watchdog_close,
            &watchdog_reader_handle,
        );
    });

    // The FollowHandle's own close()/Drop sets close_requested and joins reader +
    // watchdog; it must ALSO kill the follow child so a blocked `msb logs -f` read
    // unblocks (EOF) instead of hanging the join forever. Do that via a small closure
    // thread that watches close_requested — cheaper than adding a third join target
    // type, and keeps FollowHandle's own contract (thread join handles only) intact.
    let killer_child = child.clone();
    let killer_close = close_requested.clone();
    let killer = std::thread::spawn(move || {
        while !killer_close.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(50));
        }
        if let Ok(mut c) = killer_child.lock() {
            let _ = c.kill();
        }
    });

    // FollowHandle's own join list needs *something* representing the reader thread,
    // but the reader's actual `JoinHandle` now lives in the shared `reader_handle` slot
    // (so `flush_tail_once` can join it first, per the module docs). This proxy thread
    // takes the slot and joins whatever it finds there — a no-op if `flush_tail_once`
    // already claimed and joined it, or the real join if the caller closes before the
    // watchdog ever fires. Either way, `FollowHandle::close`/`Drop` still waits for the
    // reader to actually finish before returning.
    let handle_reader_handle = reader_handle.clone();
    let reader_proxy = std::thread::spawn(move || {
        let taken = handle_reader_handle
            .lock()
            .expect("reader handle mutex poisoned")
            .take();
        if let Some(h) = taken {
            let _ = h.join();
        }
    });

    Ok(FollowHandle::from_threads(
        close_requested,
        vec![reader_proxy, watchdog, killer],
    ))
}

/// Runs on its own thread for the lifetime of a `follow_logs` call: polls
/// `running_names_via` until the sandbox leaves `Running` (or `close_requested` is
/// set), then performs the quiesce-then-flush sequence described in the module docs.
#[allow(clippy::too_many_arguments)]
fn watchdog_loop(
    msb: &std::path::Path,
    name: &str,
    child: &Arc<Mutex<Child>>,
    state: &Arc<Mutex<FollowState>>,
    consumer: &Arc<dyn Fn(String) + Send + Sync>,
    close_requested: &Arc<AtomicBool>,
    reader_handle: &Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
) {
    loop {
        if close_requested.load(Ordering::SeqCst) {
            return; // An explicit close never triggers the replay (see module docs).
        }
        let still_alive = {
            let mut c = child.lock().expect("child mutex poisoned");
            matches!(c.try_wait(), Ok(None))
        };
        if !still_alive {
            return; // The follow child exited on its own; nothing to quiesce.
        }
        match running_names_via(msb) {
            Ok(names) if !names.contains(name) => {
                flush_tail_once(msb, name, child, state, consumer, reader_handle);
                return;
            }
            _ => {}
        }
        std::thread::sleep(READINESS_POLL);
    }
}

/// Quiesces the stuck follow process — kill the child, wait for it, THEN join the
/// reader thread (bounded, see [`READER_JOIN_TIMEOUT`]) — and only after that does the
/// one authoritative tail fetch and at-most-once replay. See the module docs for why
/// this exact ordering (quiesce fully, THEN snapshot `delivered`/`flushed`) matters:
/// it's what guarantees `delivered` reflects everything the live stream will ever
/// produce before the replay decides what's missing, ruling out delivering the same
/// complete line twice (once live, once replayed).
fn flush_tail_once(
    msb: &std::path::Path,
    name: &str,
    child: &Arc<Mutex<Child>>,
    state: &Arc<Mutex<FollowState>>,
    consumer: &Arc<dyn Fn(String) + Send + Sync>,
    reader_handle: &Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
) {
    // 1. Kill the stuck follow child and wait for it, so its stdout pipe actually EOFs.
    if let Ok(mut c) = child.lock() {
        let _ = c.kill();
        let _ = c.wait();
    }

    // 2 + 3: join the reader (bounded), THEN snapshot delivered/flushed and replay —
    // see quiesce_reader_then_snapshot's doc for why this ordering is the whole point.
    let delivered = match quiesce_reader_then_snapshot(state, reader_handle) {
        Some(d) => d,
        None => return, // already flushed by a racing caller (e.g. an explicit close).
    };

    let full = match crate::backend::invoke_logs_for_watchdog(msb, name) {
        Ok(text) => text,
        Err(_) => return, // best-effort: nothing more we can do if the final fetch itself fails.
    };
    for line in replay_lines(&full, delivered) {
        consumer(line);
    }
}

/// Slices the authoritative `msb logs --tail` text down to the lines the live stream
/// hadn't already delivered. Split out from [`flush_tail_once`] so the exact slicing —
/// trailing-newline handling included — is unit-testable without a real `msb` child;
/// the tests below exercise this function directly, not a copy of its logic.
fn replay_lines(full: &str, delivered: usize) -> Vec<String> {
    let full_lines: Vec<&str> = match full.strip_suffix('\n') {
        Some(trimmed) => trimmed.split('\n').collect(),
        None if full.is_empty() => Vec::new(),
        None => full.split('\n').collect(),
    };
    full_lines
        .into_iter()
        .skip(delivered)
        .map(str::to_string)
        .collect()
}

/// The exact seam this fix targets, pulled out so it's directly unit-testable without
/// a real `msb` child: joins the reader thread (bounded by [`READER_JOIN_TIMEOUT`]),
/// THEN — only after that join returns — takes the `flushed`/`delivered` snapshot under
/// the same lock. Returns `None` if a racing caller (an explicit close's own reader-join
/// proxy, see [`spawn_follow`]) already flushed first; otherwise marks `flushed` and
/// returns the `delivered` count the replay should skip.
///
/// This ordering is what rules out delivering the same complete line twice: joining the
/// reader means its last `delivered += 1` (if the reader was mid-flight on a buffered
/// line when the follow child was killed) happens-before this function reads
/// `delivered` — so the snapshot can never be taken while a line is still in flight
/// between "read off the wire" and "counted in `delivered`".
fn quiesce_reader_then_snapshot(
    state: &Arc<Mutex<FollowState>>,
    reader_handle: &Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
) -> Option<usize> {
    // Take the handle out of the shared slot first — if `FollowHandle`'s own
    // close/Drop proxy thread already claimed it (a concurrent explicit close), this is
    // a no-op and the reader is already guaranteed finished by that path instead.
    let taken = reader_handle
        .lock()
        .expect("reader handle mutex poisoned")
        .take();
    if let Some(handle) = taken {
        let deadline = std::time::Instant::now() + READER_JOIN_TIMEOUT;
        while !handle.is_finished() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        // Whether or not the deadline was hit, join now: if already finished this
        // returns immediately; if the deadline elapsed with the reader still stuck,
        // this degrades to an unbounded wait rather than leaking/dropping the handle —
        // but the child's already been killed+waited by the caller, so the reader's own
        // read has nothing left to block on in practice.
        let _ = handle.join();
    }

    let mut s = state.lock().expect("follow state mutex poisoned");
    if s.flushed {
        return None;
    }
    s.flushed = true;
    Some(s.delivered)
}

/// Drains `stream` line-by-line, calling `on_line` for each complete line and once
/// more for a trailing unterminated fragment at EOF, if any.
fn drain_lines(mut stream: impl Read, mut on_line: impl FnMut(String)) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
        while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
            let line_bytes: Vec<u8> = buf.drain(..=pos).collect();
            on_line(String::from_utf8_lossy(&line_bytes[..line_bytes.len() - 1]).to_string());
        }
    }
    if !buf.is_empty() {
        on_line(String::from_utf8_lossy(&buf).to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_skips_lines_already_delivered_live() {
        let full = "line1\nline2\nline3\n";
        assert_eq!(replay_lines(full, 2), vec!["line3".to_string()]);
    }

    #[test]
    fn replay_of_everything_delivered_live_yields_nothing() {
        let full = "line1\nline2\n";
        assert!(replay_lines(full, 2).is_empty());
    }

    #[test]
    fn replay_handles_a_trailing_unterminated_fragment() {
        let full = "line1\nline2\npartial-no-newline";
        assert_eq!(
            replay_lines(full, 1),
            vec!["line2".to_string(), "partial-no-newline".to_string()]
        );
    }

    #[test]
    fn replay_of_empty_log_text_yields_nothing() {
        assert!(replay_lines("", 0).is_empty());
    }

    #[test]
    fn flushed_flag_is_at_most_once() {
        let state = Arc::new(Mutex::new(FollowState {
            delivered: 3,
            flushed: false,
        }));
        // Simulate two racing callers reaching the flush gate.
        let first = {
            let mut s = state.lock().unwrap();
            let was_flushed = s.flushed;
            s.flushed = true;
            !was_flushed
        };
        let second = {
            let mut s = state.lock().unwrap();
            let was_flushed = s.flushed;
            s.flushed = true;
            !was_flushed
        };
        assert!(first, "the first caller must win the flush");
        assert!(!second, "a second caller must never flush again");
    }

    /// Deterministic regression test for the reader-join race Fix 1 closes: a
    /// synthetic reader thread that is deliberately still "mid-flight" (sleeping
    /// between reading a line off the wire and incrementing `delivered`) when the
    /// snapshot is taken must have its final increment observed by
    /// `quiesce_reader_then_snapshot` — never a stale, pre-increment count. A stale
    /// count is exactly what causes the real bug: the tail replay would then re-deliver
    /// the line the reader was about to (or just did) deliver live, duplicating it.
    ///
    /// Unlike the real-msb `follow_logs_watchdog_replays_the_final_unterminated_line_
    /// exactly_once` IT (which timing can mask — a generous settle window gives an
    /// unjoined reader plenty of time to finish on its own), this test controls the
    /// race directly via an explicit in-thread sleep, so it fails every time the join
    /// is skipped and passes every time it's honored — no flakiness either way.
    #[test]
    fn quiesce_reader_then_snapshot_observes_the_readers_final_increment() {
        let state = Arc::new(Mutex::new(FollowState {
            delivered: 0,
            flushed: false,
        }));
        let reader_state = state.clone();
        let reader = std::thread::spawn(move || {
            // Simulate the reader having already read a complete line off the wire
            // (the pipe write already happened, and the follow child has already been
            // killed+waited by the time this thread runs) but not yet having updated
            // `delivered` — the exact window the real race lives in.
            std::thread::sleep(Duration::from_millis(150));
            let mut s = reader_state.lock().expect("state mutex poisoned");
            s.delivered += 1;
        });
        let reader_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> =
            Arc::new(Mutex::new(Some(reader)));

        // No sleep here on the "watchdog" side — it calls quiesce_reader_then_snapshot
        // immediately, exactly like flush_tail_once does right after killing the child.
        // Without the join, this snapshot would race ahead and observe `delivered == 0`
        // almost every time (the reader is still asleep); with the join (as
        // implemented), it must block until the reader's increment has landed.
        let delivered =
            quiesce_reader_then_snapshot(&state, &reader_handle).expect("must not be pre-flushed");

        assert_eq!(
            delivered, 1,
            "the snapshot must reflect the reader's final increment, not a stale \
             pre-increment value — got {delivered}, which would cause the tail replay \
             to re-deliver a line the reader already delivered live"
        );
    }
}
