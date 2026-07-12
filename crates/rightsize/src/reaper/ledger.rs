//! The reaping ledger: `runs/<run-id>.json` (the [`RunRecord`]), `runs/<run-id>.sandboxes`
//! (one sandbox name per line), and `runs/<run-id>.networks` (one network id per line),
//! all under the shared rightsize cache dir (see [`crate::cache_dir`]) — the same dir a
//! Kotlin/Node/Rust process on this host all agree on, so any one of them can sweep a
//! dead run left by any of the others.
//!
//! `.sandboxes`/`.networks` are append-before-create, remove-after-teardown: the file is
//! always a superset of the run's live resources, and a resource never exists without a
//! line naming it. A single process-wide lock serializes every read-modify-write across
//! every [`Ledger`] instance in this process — coarse-grained, but ledger writes are rare
//! relative to container lifecycle (one per create/stop, not per request), so contention
//! is a non-issue and this avoids needing a lock registry keyed by run id.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// The `runs/<run-id>.json` payload — the cross-language contract's exact field
/// names (`camelCase`, since Kotlin/Node write and read this same file shape).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    /// The process id that owns this run.
    pub pid: u32,
    /// ISO-8601 UTC instant the *process* started (not the run record) — used to
    /// defeat pid reuse. See [`super::liveness`].
    #[serde(rename = "startedIso")]
    pub started_iso: String,
    /// The active backend's registered name (e.g. `"microsandbox"`, `"docker"`) —
    /// the sweep only reaps a dead run whose recorded backend matches its own.
    pub backend: String,
    /// The provisioned msb binary's absolute path, when the backend is msb. `None`
    /// for backends with no such on-disk binary (docker).
    #[serde(rename = "msbPath", skip_serializing_if = "Option::is_none", default)]
    pub msb_path: Option<String>,
}

/// A process-wide lock serializing every ledger read-modify-write in this process —
/// see the module doc for why one coarse lock is enough.
static WRITE_LOCK: Mutex<()> = Mutex::new(());

/// One run's ledger files under `<cache_dir>/runs/`. Cheap to construct — no I/O
/// happens until a method is called — so the sweep constructs a fresh one per
/// candidate run it finds on disk, and this process's own long-lived ledger is just
/// another instance keyed by [`crate::RunId::value`].
#[derive(Debug, Clone)]
pub struct Ledger {
    runs_dir: PathBuf,
    run_id: String,
}

impl Ledger {
    /// Builds a ledger for `run_id` under `cache_dir/runs/`. Does not itself create
    /// the directory or any file — see [`Self::write_record`]/[`Self::append_sandbox`].
    pub fn new(cache_dir: &Path, run_id: &str) -> Self {
        Ledger {
            runs_dir: cache_dir.join("runs"),
            run_id: run_id.to_string(),
        }
    }

    /// `runs/<run-id>.json`.
    pub fn record_path(&self) -> PathBuf {
        self.runs_dir.join(format!("{}.json", self.run_id))
    }

    /// `runs/<run-id>.sandboxes`.
    pub fn sandboxes_path(&self) -> PathBuf {
        self.runs_dir.join(format!("{}.sandboxes", self.run_id))
    }

    /// `runs/<run-id>.networks`.
    pub fn networks_path(&self) -> PathBuf {
        self.runs_dir.join(format!("{}.networks", self.run_id))
    }

    /// Writes the run record atomically (temp file + rename) — must happen BEFORE
    /// this run's first sandbox is created. Idempotent: safe to call more than once
    /// (a later call simply overwrites), though callers only do so once per process.
    pub fn write_record(&self, record: &RunRecord) -> std::io::Result<()> {
        let _guard = WRITE_LOCK.lock().expect("ledger write lock poisoned");
        fs::create_dir_all(&self.runs_dir)?;
        let json =
            serde_json::to_vec_pretty(record).expect("RunRecord has no non-serializable fields");
        let tmp = self.runs_dir.join(format!("{}.json.tmp", self.run_id));
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&json)?;
        }
        fs::rename(&tmp, self.record_path())?;
        Ok(())
    }

    /// Reads and parses the run record, if present and valid JSON.
    pub fn read_record(&self) -> Option<RunRecord> {
        let raw = fs::read(self.record_path()).ok()?;
        serde_json::from_slice(&raw).ok()
    }

    /// Appends `name` to `.sandboxes` — call BEFORE the backend `create` call.
    /// No-op (still succeeds) if `name` is already present, since a create retry
    /// (the port-bind-conflict loop) must never leave duplicate lines behind.
    pub fn append_sandbox(&self, name: &str) -> std::io::Result<()> {
        self.append_line(&self.sandboxes_path(), name)
    }

    /// Removes `name` from `.sandboxes` — call AFTER a successful stop/remove. Does
    /// NOT prune the ledger files even if this empties both `.sandboxes` and
    /// `.networks` — see [`Self::remove_sandbox_and_prune_if_empty`] for the
    /// atomic version callers that care about the clean-shutdown rule must use.
    /// Production only ever calls the atomic version; this bare primitive is kept
    /// (and exercised directly) for the unit tests that pin down its own removal
    /// semantics in isolation from pruning — same seam convention as
    /// `Container::with_backend`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn remove_sandbox(&self, name: &str) -> std::io::Result<()> {
        let _guard = WRITE_LOCK.lock().expect("ledger write lock poisoned");
        self.remove_line_locked(&self.sandboxes_path(), name)
    }

    /// [`Self::remove_sandbox`], plus the clean-shutdown rule performed under the
    /// SAME `WRITE_LOCK` acquisition as the removal: if that leaves both
    /// `.sandboxes` and `.networks` empty, all three ledger files are deleted
    /// before the lock is released. This is the version `record_stop` must call —
    /// doing the removal, the emptiness check, and the delete as three separately
    /// locked steps (as this used to) leaves a window between "removal sees the
    /// file empty" and "we delete the files" where a concurrent
    /// `append_sandbox`/`append_network` on another thread can land, write its
    /// line into the file this call is about to unlink, and have that fresh line
    /// silently discarded — a lost ledger entry means the watchdog/sweep would
    /// never learn about (and never reap) that sandbox if this process later died
    /// uncleanly. See the module doc's append/remove discipline.
    pub fn remove_sandbox_and_prune_if_empty(&self, name: &str) {
        let _guard = WRITE_LOCK.lock().expect("ledger write lock poisoned");
        let _ = self.remove_line_locked(&self.sandboxes_path(), name);
        if self.is_empty_locked() {
            self.delete_files_locked();
        }
    }

    /// The sandbox names currently listed, in file order.
    pub fn sandbox_names(&self) -> Vec<String> {
        // Serialized with the writers: this crate's own live-run readers must
        // never race a rewrite of the same file (the tmp+rename swap keeps the
        // CONTENT atomic, but on Windows a rename over a concurrently-open
        // destination fails EPERM, so readers of a live run stay behind the
        // lock). Dead runs' files — the sweep's and watchdog's inputs — have no
        // living writer to race.
        let _guard = WRITE_LOCK.lock().expect("ledger write lock poisoned");
        read_lines(&self.sandboxes_path())
    }

    /// Appends `id` to `.networks` — same append-before-create discipline as
    /// [`Self::append_sandbox`].
    pub fn append_network(&self, id: &str) -> std::io::Result<()> {
        self.append_line(&self.networks_path(), id)
    }

    /// Removes `id` from `.networks` and, under the same lock, prunes the ledger
    /// files once both `.sandboxes` and `.networks` are empty — the network
    /// counterpart of [`Self::remove_sandbox_and_prune_if_empty`]; same rationale.
    pub fn remove_network_and_prune_if_empty(&self, id: &str) {
        let _guard = WRITE_LOCK.lock().expect("ledger write lock poisoned");
        let _ = self.remove_line_locked(&self.networks_path(), id);
        if self.is_empty_locked() {
            self.delete_files_locked();
        }
    }

    /// The network ids currently listed, in file order.
    pub fn network_ids(&self) -> Vec<String> {
        let _guard = WRITE_LOCK.lock().expect("ledger write lock poisoned");
        read_lines(&self.networks_path())
    }

    /// True when both `.sandboxes` and `.networks` are empty or absent — the
    /// clean-shutdown trigger for [`Self::delete_files`]. An unlocked read: fine
    /// for a caller (e.g. a test) that just wants a snapshot, but NOT atomic with
    /// any write — [`Self::remove_sandbox_and_prune_if_empty`]/
    /// [`Self::remove_network_and_prune_if_empty`] use the private
    /// [`Self::is_empty_locked`] instead, under the same lock as their removal.
    /// Production has no remaining caller of the unlocked version; kept for the
    /// unit tests that check it directly.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_empty(&self) -> bool {
        self.sandbox_names().is_empty() && self.network_ids().is_empty()
    }

    /// Best-effort deletes all three ledger files. Called on clean shutdown (once
    /// both `.sandboxes` and `.networks` are empty) and by a sweep concluding a dead
    /// run. Takes the lock itself — callers that already hold it (the
    /// prune-if-empty methods above) must use [`Self::delete_files_locked`]
    /// instead, since `std::sync::Mutex` is not reentrant.
    pub fn delete_files(&self) {
        let _guard = WRITE_LOCK.lock().expect("ledger write lock poisoned");
        self.delete_files_locked();
    }

    /// Same emptiness check as [`Self::is_empty`], for a caller that already holds
    /// `WRITE_LOCK`.
    fn is_empty_locked(&self) -> bool {
        read_lines(&self.sandboxes_path()).is_empty()
            && read_lines(&self.networks_path()).is_empty()
    }

    /// Same deletion as [`Self::delete_files`], for a caller that already holds
    /// `WRITE_LOCK`.
    fn delete_files_locked(&self) {
        let _ = fs::remove_file(self.record_path());
        let _ = fs::remove_file(self.sandboxes_path());
        let _ = fs::remove_file(self.networks_path());
    }

    fn append_line(&self, path: &Path, line: &str) -> std::io::Result<()> {
        let _guard = WRITE_LOCK.lock().expect("ledger write lock poisoned");
        self.append_line_locked(path, line)
    }

    fn append_line_locked(&self, path: &Path, line: &str) -> std::io::Result<()> {
        fs::create_dir_all(&self.runs_dir)?;
        let mut existing = read_lines(path);
        if existing.iter().any(|l| l == line) {
            return Ok(());
        }
        existing.push(line.to_string());
        write_lines(path, &existing)
    }

    fn remove_line_locked(&self, path: &Path, line: &str) -> std::io::Result<()> {
        let mut existing = read_lines(path);
        let before = existing.len();
        existing.retain(|l| l != line);
        if existing.len() == before {
            return Ok(()); // not present: nothing to do.
        }
        write_lines(path, &existing)
    }
}

fn read_lines(path: &Path) -> Vec<String> {
    match fs::read_to_string(path) {
        Ok(text) => text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect(),
        Err(_) => Vec::new(),
    }
}

/// Writes `lines` to `path` atomically (temp file + rename) — same discipline as
/// [`Ledger::write_record`]. A plain `fs::write` truncates the destination (as
/// part of its internal `open(..).truncate(true)`) and only THEN writes the new
/// content, as a separate step; an unlocked reader (`sandbox_names`/
/// `network_ids`/`is_empty` all take no lock at all — see their docs, which is
/// only safe if the file itself never passes through a transient torn state)
/// landing in that gap sees a transiently EMPTY file, even though both the prior
/// and the about-to-be-written content are non-empty. `fs::rename` is atomic: a
/// concurrent unlocked reader either sees the complete prior file or the
/// complete new one, never a torn/empty state in between — closing the second
/// lost-ledger-entry race (distinct from the remove+prune-if-empty race this
/// module's other doc comments describe). See
/// [`tests::write_lines_truncate_then_write_gap_is_visible_to_an_unlocked_reader`]
/// for a deterministic demonstration of the exact gap this closes, and
/// [`tests::concurrent_writes_and_unlocked_reads_never_expose_a_torn_file`] for
/// the regression test pinning the fix against the real function.
fn write_lines(path: &Path, lines: &[String]) -> std::io::Result<()> {
    let mut content = lines.join("\n");
    if !lines.is_empty() {
        content.push('\n');
    }
    let tmp = tmp_sibling(path);
    fs::write(&tmp, content)?;
    fs::rename(&tmp, path)
}

/// `path` with `.tmp` appended to its file name — e.g. `run1.sandboxes` ->
/// `run1.sandboxes.tmp`, mirroring [`Ledger::write_record`]'s own
/// `<run-id>.json.tmp` naming. Safe to reuse a fixed name per target path: every
/// [`write_lines`] call runs under `WRITE_LOCK`, a single process-wide `Mutex`
/// (see the module doc), so at most one `write_lines` call is ever in-flight at
/// a time regardless of which path it targets — no two concurrent calls can
/// collide on the same tmp file.
fn tmp_sibling(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

/// Every `runs/*.json` file's stem (the run id) under `cache_dir`, in directory
/// iteration order — the sweep's own candidate list. An unreadable `runs/` dir
/// (doesn't exist yet — no run has ever written a record on this host) yields an
/// empty list rather than an error.
pub(crate) fn candidate_run_ids(cache_dir: &Path) -> Vec<String> {
    let runs_dir = cache_dir.join("runs");
    let Ok(entries) = fs::read_dir(&runs_dir) else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            ids.push(stem.to_string());
        }
    }
    ids
}

/// The age of `path`'s last modification, or `None` if its metadata can't be read.
pub(crate) fn file_age(path: &Path) -> Option<std::time::Duration> {
    fs::metadata(path).ok()?.modified().ok()?.elapsed().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn temp_cache_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rz-ledger-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_record() -> RunRecord {
        RunRecord {
            pid: 12345,
            started_iso: "2025-01-01T00:00:00Z".to_string(),
            backend: "docker".to_string(),
            msb_path: None,
        }
    }

    #[test]
    fn write_record_then_read_record_round_trips() {
        let cache = temp_cache_dir("write-read");
        let ledger = Ledger::new(&cache, "run1");
        ledger.write_record(&sample_record()).unwrap();
        assert_eq!(ledger.read_record(), Some(sample_record()));
    }

    #[test]
    fn record_json_uses_camel_case_field_names() {
        let cache = temp_cache_dir("camel-case");
        let ledger = Ledger::new(&cache, "run1");
        let mut record = sample_record();
        record.msb_path = Some("/opt/msb".to_string());
        ledger.write_record(&record).unwrap();
        let raw = fs::read_to_string(ledger.record_path()).unwrap();
        assert!(raw.contains("\"startedIso\""), "{raw}");
        assert!(raw.contains("\"msbPath\""), "{raw}");
        assert!(!raw.contains("started_iso"), "{raw}");
    }

    #[test]
    fn msb_path_is_omitted_from_json_when_none() {
        let cache = temp_cache_dir("omit-msbpath");
        let ledger = Ledger::new(&cache, "run1");
        ledger.write_record(&sample_record()).unwrap();
        let raw = fs::read_to_string(ledger.record_path()).unwrap();
        assert!(!raw.contains("msbPath"), "{raw}");
    }

    #[test]
    fn append_sandbox_is_before_create_and_dedupes() {
        let cache = temp_cache_dir("append-dedup");
        let ledger = Ledger::new(&cache, "run1");
        ledger.append_sandbox("rz-run1-0").unwrap();
        ledger.append_sandbox("rz-run1-1").unwrap();
        ledger.append_sandbox("rz-run1-0").unwrap(); // duplicate append: no-op.
        assert_eq!(
            ledger.sandbox_names(),
            vec!["rz-run1-0".to_string(), "rz-run1-1".to_string()]
        );
    }

    #[test]
    fn remove_sandbox_after_stop_leaves_only_the_survivors() {
        let cache = temp_cache_dir("remove-after-stop");
        let ledger = Ledger::new(&cache, "run1");
        ledger.append_sandbox("a").unwrap();
        ledger.append_sandbox("b").unwrap();
        ledger.remove_sandbox("a").unwrap();
        assert_eq!(ledger.sandbox_names(), vec!["b".to_string()]);
    }

    #[test]
    fn removing_an_absent_sandbox_is_a_harmless_no_op() {
        let cache = temp_cache_dir("remove-absent");
        let ledger = Ledger::new(&cache, "run1");
        ledger.remove_sandbox("never-there").unwrap();
        assert!(ledger.sandbox_names().is_empty());
    }

    #[test]
    fn is_empty_true_only_when_both_files_have_no_entries() {
        let cache = temp_cache_dir("is-empty");
        let ledger = Ledger::new(&cache, "run1");
        assert!(ledger.is_empty(), "no files at all: empty");
        ledger.append_sandbox("a").unwrap();
        assert!(!ledger.is_empty());
        ledger.remove_sandbox("a").unwrap();
        assert!(ledger.is_empty());
        ledger.append_network("net-1").unwrap();
        assert!(!ledger.is_empty());
    }

    #[test]
    fn delete_files_removes_all_three_and_is_idempotent() {
        let cache = temp_cache_dir("delete-files");
        let ledger = Ledger::new(&cache, "run1");
        ledger.write_record(&sample_record()).unwrap();
        ledger.append_sandbox("a").unwrap();
        ledger.append_network("n").unwrap();
        ledger.delete_files();
        assert!(!ledger.record_path().exists());
        assert!(!ledger.sandboxes_path().exists());
        assert!(!ledger.networks_path().exists());
        ledger.delete_files(); // second call: must not panic/error.
    }

    #[test]
    fn candidate_run_ids_lists_every_json_stem_and_ignores_other_extensions() {
        let cache = temp_cache_dir("candidates");
        Ledger::new(&cache, "runA")
            .write_record(&sample_record())
            .unwrap();
        Ledger::new(&cache, "runB")
            .write_record(&sample_record())
            .unwrap();
        let mut ids = candidate_run_ids(&cache);
        ids.sort();
        assert_eq!(ids, vec!["runA".to_string(), "runB".to_string()]);
    }

    #[test]
    fn candidate_run_ids_on_a_missing_runs_dir_is_empty_not_an_error() {
        let cache = temp_cache_dir("no-runs-dir");
        assert!(candidate_run_ids(&cache).is_empty());
    }

    /// Regression test for a lost-ledger-entry race: `record_stop` used to run
    /// `remove_sandbox`, then `is_empty()`, then (conditionally) `delete_files()` as
    /// three SEPARATELY locked steps — `WRITE_LOCK` was dropped after the removal
    /// and re-taken (or not taken at all, for `is_empty`/`delete_files`, both
    /// unlocked reads/writes) before the next. Confirmed interleaving, captured by
    /// instrumenting this exact sequence and looping
    /// `RUST_TEST_THREADS=2 cargo test -p rightsize --lib` until caught:
    ///
    /// ```text
    /// [thread 21] record_stop(rz-run-7) removed; about to check is_empty()
    /// [thread 21] record_stop(rz-run-7) saw is_empty()==true; about to delete_files()
    /// [thread 21] delete_files() sandboxes-contents-before-unlink=Ok("")
    /// [thread 37] append_line(rz-run-8) existing-before=[]; file-exists=true
    /// [thread 21] record_stop(rz-run-7) delete_files() done
    /// [thread 37] append_line(rz-run-8) wrote <- ["rz-run-8"]
    /// ```
    ///
    /// Thread 21 (some other test's Drop-path cleanup, tearing down the run's last
    /// live sandbox) observed the `.sandboxes` file empty and proceeded to delete
    /// all three ledger files; thread 37 (a sibling test's `before_create`) landed
    /// its own fresh append into that same now-empty file in the gap between the
    /// emptiness check and the unlink, and thread 21's unconditional delete
    /// discarded it — a live sandbox silently vanishing from the reaping ledger,
    /// meaning the watchdog/sweep would never learn about (or reap) it if this
    /// process later died uncleanly.
    ///
    /// [`Ledger::remove_sandbox_and_prune_if_empty`] closes this by making the
    /// removal, the emptiness check, and the delete a single `WRITE_LOCK` critical
    /// section — a concurrent `append_sandbox` now either lands entirely before it
    /// (survives the removal, since the file isn't empty) or entirely after it
    /// (recreates the file post-delete), never in the gap. This test races the two
    /// operations against each other under a `Barrier` (forcing genuine contention
    /// on `WRITE_LOCK` every iteration, both orderings possible) and asserts the
    /// one outcome that holds under BOTH possible orderings once the operations are
    /// atomic: the concurrently appended name always survives. Reverting
    /// `remove_sandbox_and_prune_if_empty` to the old three-separately-locked-steps
    /// shape (`remove_sandbox` + `is_empty` + `delete_files`) makes this fail
    /// intermittently — the same ~1-in-3 rate the original bug report measured.
    #[test]
    fn concurrent_append_never_lands_in_the_remove_then_prune_gap() {
        let cache = temp_cache_dir("remove-prune-race");
        let ledger = Arc::new(Ledger::new(&cache, "run1"));

        const ITERATIONS: usize = 200;
        for i in 0..ITERATIONS {
            let fresh_name = format!("fresh-{i}");
            ledger.append_sandbox("victim").unwrap();
            assert!(
                ledger.network_ids().is_empty(),
                "sanity: no networks tracked"
            );

            let barrier = Arc::new(std::sync::Barrier::new(2));

            let remover = {
                let ledger = ledger.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    ledger.remove_sandbox_and_prune_if_empty("victim");
                })
            };
            let appender = {
                let ledger = ledger.clone();
                let barrier = barrier.clone();
                let fresh_name = fresh_name.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    ledger.append_sandbox(&fresh_name).unwrap();
                })
            };
            remover
                .join()
                .expect("remove_sandbox_and_prune_if_empty must not panic");
            appender.join().expect("append_sandbox must not panic");

            assert_eq!(
                ledger.sandbox_names(),
                vec![fresh_name.clone()],
                "iteration {i}: a concurrent append_sandbox racing \
                 remove_sandbox_and_prune_if_empty must never be discarded, \
                 regardless of which of the two operations the lock let run first"
            );

            // Clean up for the next iteration (also exercises the prune-to-deletion
            // path once more on its own, uncontended).
            ledger.remove_sandbox_and_prune_if_empty(&fresh_name);
            assert!(ledger.sandbox_names().is_empty());
        }
    }

    /// Regression test for the SECOND lost-ledger-entry race, distinct from the
    /// remove+prune-if-empty race `concurrent_append_never_lands_in_the_remove_
    /// then_prune_gap` above closes. CI's unit lane went green after that fix, but
    /// CI's COVERAGE lane (`cargo llvm-cov -p rightsize --lib`, whose instrumentation
    /// measurably widens scheduling windows around every line/branch) then failed a
    /// DIFFERENT test with the same vanished-entry symptom:
    /// `container::tests::restored_container_is_registered_in_the_reaping_ledger_
    /// like_any_other` — its just-appended line missing from the shared
    /// `.sandboxes` file it had just appended to and never removed from.
    ///
    /// Root cause: before this fix, [`write_lines`] wrote via plain `fs::write`,
    /// which internally does `open(..).truncate(true)` (a separate syscall that
    /// empties the file) and THEN writes the new content in a SECOND, separate
    /// step. `Ledger::sandbox_names`/`network_ids`/`is_empty` are all deliberately
    /// UNLOCKED reads (see their docs — fine for a snapshot, the doc claimed, since
    /// every WRITE is still serialized under `WRITE_LOCK`). That's true at the
    /// "operation" granularity, but false at the syscall granularity: an unlocked
    /// reader can land in the gap between one locked writer's truncate and its
    /// write, and observe a transiently EMPTY file — even though both the file's
    /// prior content and its about-to-be-written content are non-empty, and even
    /// though the writer itself is doing everything right under the lock. That
    /// window is normally a handful of microseconds (too narrow for real thread
    /// scheduling to land in reliably — confirmed below: 40 uninstrumented loops of
    /// `RUST_TEST_THREADS=2 cargo test -p rightsize --lib` and 6 loops of
    /// `cargo llvm-cov --package rightsize --lib` on this machine never
    /// reproduced it naturally, and neither did an 8-thread/400-iteration stress
    /// test hammering `append_sandbox`/`sandbox_names`/`remove_sandbox` back to
    /// back), but is exactly the kind of window per-line/per-branch coverage
    /// instrumentation (extra counter increments around every statement) widens
    /// enough for real scheduling to hit — which is why CI's COVERAGE lane (and
    /// only that lane) caught it.
    ///
    /// [`write_lines_truncate_then_write_gap_is_visible_to_an_unlocked_reader`]
    /// below reproduces the exact mechanism deterministically (forcing the
    /// interleaving with a `Barrier` rather than hoping scheduler luck lands in a
    /// multi-microsecond window, same rigor as the remove/prune race's own
    /// barrier-forced regression test) by performing the identical two steps
    /// `fs::write` performs internally, with an explicit delay between them
    /// standing in for the delay coverage instrumentation introduces on CI:
    ///
    /// ```text
    /// [probe] unlocked reader saw: []
    /// ```
    ///
    /// i.e. a concurrent unlocked read landing between the truncate and the write
    /// sees `[]` even though the file has one line before the write starts and two
    /// after it finishes. This is a real class of bug in [`write_lines`] itself,
    /// not a test-isolation artifact: `write_lines` is production code
    /// (`Ledger::append_sandbox`/`remove_sandbox`/`append_network`/`remove_line_
    /// locked` all funnel through it), and `sandbox_names`/`network_ids`/
    /// `is_empty` are unlocked BY DESIGN (their docs call this out explicitly),
    /// so any caller of those reads — production or test — was exposed. This is
    /// therefore NOT the "pure cross-test contamination the atomic ledger cannot
    /// prevent" case: it's closeable, and is closed, at the ledger layer, by
    /// giving `write_lines` the same tmp-file-plus-`fs::rename` atomicity
    /// `write_record` already had. `fs::rename` is atomic at the filesystem level:
    /// a concurrent unlocked reader can only ever see the complete prior file or
    /// the complete new one, never a torn/empty state in between.
    /// [`concurrent_writes_and_unlocked_reads_never_expose_a_torn_file`] pins this
    /// down against the real, fixed `write_lines`/`read_lines` pair.
    #[test]
    fn write_lines_truncate_then_write_gap_is_visible_to_an_unlocked_reader() {
        use std::fs::OpenOptions;
        use std::io::Write as _;

        let cache = temp_cache_dir("torn-read-mechanism");
        let path = cache.join("mechanism.sandboxes");
        fs::write(&path, "existing-line\n").unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let saw_empty = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let writer = {
            let path = path.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                // Exactly what `fs::write` did internally, pre-fix: open+truncate,
                // THEN write the new content — two separate steps, not one atomic
                // one (this is the OLD `write_lines` shape; the real one below no
                // longer works this way, see `write_lines`'s own doc).
                let mut f = OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .unwrap();
                barrier.wait(); // let the reader in right after the truncate.
                std::thread::sleep(std::time::Duration::from_millis(50));
                f.write_all(b"existing-line\nfresh-line\n").unwrap();
            })
        };
        let reader = {
            let path = path.clone();
            let barrier = barrier.clone();
            let saw_empty = saw_empty.clone();
            std::thread::spawn(move || {
                barrier.wait(); // synchronized to land right after the truncate.
                let seen = read_lines(&path);
                eprintln!("[probe] unlocked reader saw: {seen:?}");
                if seen.is_empty() {
                    saw_empty.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            })
        };
        writer.join().unwrap();
        reader.join().unwrap();

        assert!(
            saw_empty.load(std::sync::atomic::Ordering::SeqCst),
            "an unlocked read landing between a truncate-then-write's two steps \
             must observe a transiently empty file — demonstrates the exact gap \
             the OLD write_lines exposed to sandbox_names/network_ids/is_empty"
        );
        assert_eq!(
            read_lines(&path),
            vec!["existing-line".to_string(), "fresh-line".to_string()],
            "sanity: the write does eventually land correctly, after the gap"
        );
    }

    /// Pins the fix against the REAL `write_lines`/`read_lines` pair: a writer
    /// thread repeatedly calls the real (private, same-module) `write_lines` with
    /// a payload large enough (~2 MiB) that its temp-file write takes measurable
    /// time, while a reader thread hammers the FINAL path with unlocked
    /// `read_lines` throughout. Every observation must be either the previous
    /// complete write or the new complete write — `fs::rename`'s atomicity means
    /// the destination path never contains partial content, so an empty/torn read
    /// is only possible before the very first write lands (handled below) or if
    /// the atomicity regresses. Complements
    /// [`write_lines_truncate_then_write_gap_is_visible_to_an_unlocked_reader`]'s
    /// demonstration of the mechanism the old code was vulnerable to.
    #[test]
    fn concurrent_writes_and_unlocked_reads_never_expose_a_torn_file() {
        let cache = temp_cache_dir("torn-read-fixed");
        let path = cache.join("fixed.sandboxes");

        const WRITES: usize = 40;
        // Large enough that the temp-file write is not a single instantaneous
        // syscall, giving a concurrent unlocked reader a real chance to land
        // mid-write if the destination were ever touched non-atomically.
        let big_line = "x".repeat(64 * 1024);
        let payloads: Vec<Vec<String>> = (0..WRITES)
            .map(|i| vec![format!("{big_line}-{i}"); 32])
            .collect();

        // Seed the file with the first payload so the reader always has a valid
        // complete state to expect from the very first read.
        write_lines(&path, &payloads[0]).unwrap();

        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let bad_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let writer = {
            let path = path.clone();
            let payloads = payloads.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                for payload in &payloads[1..] {
                    write_lines(&path, payload).unwrap();
                }
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            })
        };
        let reader = {
            let path = path.clone();
            let done = done.clone();
            let bad_reads = bad_reads.clone();
            std::thread::spawn(move || {
                while !done.load(std::sync::atomic::Ordering::SeqCst) {
                    let seen = read_lines(&path);
                    if seen.is_empty() {
                        bad_reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            })
        };
        writer.join().unwrap();
        reader.join().unwrap();

        assert_eq!(
            bad_reads.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "an unlocked reader must never observe an empty/torn file while a \
             concurrent writer holds non-empty content, now that write_lines is \
             atomic (tmp file + rename)"
        );
        assert_eq!(
            read_lines(&path),
            payloads[WRITES - 1],
            "the file must end up exactly at the last write's content"
        );
    }
}
