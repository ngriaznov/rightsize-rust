//! The cross-language liveness protocol: a run is ALIVE iff a process with its
//! recorded `pid` exists AND that process's start time matches the recorded
//! `startedIso` within 2 seconds (defeats PID reuse — a different process that
//! happens to have been assigned the same pid later has a different start time).
//!
//! No `libc`/`unsafe` is available in this workspace (`#![forbid(unsafe_code)]`,
//! `rightsize-rs`'s stated no-`libc` constraint), so this shells out: `ps -p <pid> -o
//! etime=` on macOS/Linux (POSIX `etime`, not the GNU-only `etimes`, so this works on
//! BSD `ps` too) gives elapsed time as `[[DD-]HH:]MM:SS`, which combined with "now"
//! gives the process's start instant without ever parsing a calendar-text column —
//! `ps -o lstart=`'s format is locale-dependent and awkward to parse portably, and
//! this workspace has no date-parsing crate. On Windows, PowerShell's `StartTime`
//! property is asked for directly in ISO-8601 form, sidestepping the same problem.

use std::time::{SystemTime, UNIX_EPOCH};

/// True iff a process with `pid` is currently running and its own start time is
/// within 2 seconds of `started_iso` (an ISO-8601 UTC instant, as written by
/// [`crate::reaper::ledger::RunRecord`]). Any failure to determine the process's
/// actual start time (not running, `ps`/PowerShell unavailable, unparseable output)
/// is treated as NOT alive — the sweep's caller then treats the run as dead.
pub(crate) fn is_alive(pid: u32, started_iso: &str) -> bool {
    let Some(actual_iso) = process_started_iso(pid) else {
        return false;
    };
    let (Some(recorded), Some(actual)) = (
        iso8601_to_unix_secs(started_iso),
        iso8601_to_unix_secs(&actual_iso),
    ) else {
        return false;
    };
    (recorded - actual).abs() <= 2
}

/// The ISO-8601 UTC instant `pid` itself started, or `None` if that can't be
/// determined (the process isn't running, or the platform probe failed/is
/// unavailable).
pub(crate) fn process_started_iso(pid: u32) -> Option<String> {
    #[cfg(windows)]
    {
        windows_process_started_iso(pid)
    }
    #[cfg(not(windows))]
    {
        let etime = unix_process_etime_secs(pid)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs() as i64;
        Some(unix_secs_to_iso8601(now - etime as i64))
    }
}

#[cfg(not(windows))]
fn unix_process_etime_secs(pid: u32) -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "etime="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().next()?.trim();
    if line.is_empty() {
        return None;
    }
    parse_etime(line)
}

/// Parses `ps -o etime=`'s `[[DD-]HH:]MM:SS` format (the POSIX-portable shape both
/// GNU and BSD `ps` agree on) into total elapsed seconds.
#[cfg(not(windows))]
fn parse_etime(s: &str) -> Option<u64> {
    let (days, rest) = match s.split_once('-') {
        Some((d, r)) => (d.parse::<u64>().ok()?, r),
        None => (0, s),
    };
    let parts: Vec<&str> = rest.split(':').collect();
    let (h, m, sec) = match parts.as_slice() {
        [h, m, s] => (
            h.parse::<u64>().ok()?,
            m.parse::<u64>().ok()?,
            s.parse::<u64>().ok()?,
        ),
        [m, s] => (0, m.parse::<u64>().ok()?, s.parse::<u64>().ok()?),
        _ => return None,
    };
    Some(days * 86400 + h * 3600 + m * 60 + sec)
}

/// Asks PowerShell for `pid`'s start time directly in ISO-8601 UTC form — avoids
/// hand-parsing a locale-dependent `.ToString()` rendering of a .NET `DateTime`.
/// Returns `None` if the process doesn't exist or PowerShell itself is unavailable.
#[cfg(windows)]
fn windows_process_started_iso(pid: u32) -> Option<String> {
    let script = format!(
        "(Get-Process -Id {pid} -ErrorAction Stop).StartTime.ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')"
    );
    let output = std::process::Command::new("powershell")
        .args(["-NoProfile", "-NonInteractive", "-Command", &script])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text.lines().next()?.trim();
    if line.is_empty() {
        None
    } else {
        Some(line.to_string())
    }
}

/// Days since the Unix epoch (1970-01-01) for a proleptic-Gregorian calendar date —
/// Howard Hinnant's `days_from_civil` algorithm (public domain, exact for any year,
/// integer arithmetic only). The inverse of [`civil_from_days`].
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // [0, 11]
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Inverse of [`days_from_civil`]: the proleptic-Gregorian `(year, month, day)` for
/// `days` days since the Unix epoch.
#[cfg(not(windows))]
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Formats a Unix timestamp (seconds since the epoch, UTC) as an ISO-8601
/// `YYYY-MM-DDTHH:MM:SSZ` string.
#[cfg(not(windows))]
fn unix_secs_to_iso8601(secs: i64) -> String {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    let hh = rem / 3600;
    let mm = (rem % 3600) / 60;
    let ss = rem % 60;
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// Parses a fixed-position `YYYY-MM-DDTHH:MM:SS...` prefix (fractional seconds or a
/// trailing `Z`/offset, if any, are ignored) into Unix seconds. `None` for anything
/// too short or non-numeric to be this shape.
fn iso8601_to_unix_secs(s: &str) -> Option<i64> {
    if s.len() < 19 {
        return None;
    }
    let y: i64 = s.get(0..4)?.parse().ok()?;
    let mo: u32 = s.get(5..7)?.parse().ok()?;
    let d: u32 = s.get(8..10)?.parse().ok()?;
    let hh: i64 = s.get(11..13)?.parse().ok()?;
    let mi: i64 = s.get(14..16)?.parse().ok()?;
    let ss: i64 = s.get(17..19)?.parse().ok()?;
    let days = days_from_civil(y, mo, d);
    Some(days * 86400 + hh * 3600 + mi * 60 + ss)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unix_epoch_formats_as_the_epoch_instant() {
        #[cfg(not(windows))]
        assert_eq!(unix_secs_to_iso8601(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn iso8601_round_trips_through_unix_secs_for_arbitrary_instants() {
        #[cfg(not(windows))]
        for secs in [0_i64, 86_399, 86_400, 1_000_000_000, 2_000_000_000] {
            let iso = unix_secs_to_iso8601(secs);
            assert_eq!(
                iso8601_to_unix_secs(&iso),
                Some(secs),
                "round trip failed for {secs} -> {iso}"
            );
        }
    }

    #[test]
    fn iso8601_to_unix_secs_rejects_garbage() {
        assert_eq!(iso8601_to_unix_secs(""), None);
        assert_eq!(iso8601_to_unix_secs("not-a-date"), None);
    }

    #[cfg(not(windows))]
    #[test]
    fn parse_etime_handles_all_three_shapes() {
        assert_eq!(parse_etime("00:05"), Some(5));
        assert_eq!(parse_etime("02:03:04"), Some(2 * 3600 + 3 * 60 + 4));
        assert_eq!(
            parse_etime("1-02:03:04"),
            Some(86400 + 2 * 3600 + 3 * 60 + 4)
        );
    }

    #[test]
    fn own_process_is_alive_with_its_own_actual_start_time() {
        let pid = std::process::id();
        let iso = process_started_iso(pid).expect("this process's own start time must resolve");
        assert!(is_alive(pid, &iso));
    }

    #[test]
    fn own_pid_with_a_wildly_different_recorded_start_time_is_dead() {
        // Same pid (definitely alive), but a bogus recorded start instant far outside
        // the 2-second tolerance — proves PID-reuse defeat: same pid, different start
        // time counts as dead.
        let pid = std::process::id();
        assert!(!is_alive(pid, "1999-01-01T00:00:00Z"));
    }

    #[test]
    fn a_pid_that_does_not_exist_is_dead() {
        // PID 1 exists but is never this test's own process; a very large, almost
        // certainly-unassigned pid is a more portable "definitely not running" probe
        // than assuming a specific pid is free.
        assert!(!is_alive(u32::MAX - 1, "2025-01-01T00:00:00Z"));
    }
}
