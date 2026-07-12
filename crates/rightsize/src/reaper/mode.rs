//! The `RIGHTSIZE_REAPER` environment switch: `on` (default) runs both the init-time
//! sweep and the per-run watchdog; `sweep` runs the sweep only (no watchdog process);
//! `off` disables both. Unknown values are treated as `on`.

/// How much of the reaping machinery is active for this process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReaperMode {
    /// Sweep + watchdog (the default).
    On,
    /// Sweep only — no watchdog process is spawned.
    Sweep,
    /// Neither runs. No run record is written either — there'd be nothing to sweep it.
    Off,
}

impl ReaperMode {
    /// Reads `RIGHTSIZE_REAPER` from the real process environment.
    pub(crate) fn from_env() -> Self {
        Self::parse(std::env::var("RIGHTSIZE_REAPER").ok().as_deref())
    }

    /// The seam [`Self::from_env`] delegates to — pure, for unit tests.
    pub(crate) fn parse(value: Option<&str>) -> Self {
        match value {
            Some(v) if v.eq_ignore_ascii_case("off") => ReaperMode::Off,
            Some(v) if v.eq_ignore_ascii_case("sweep") => ReaperMode::Sweep,
            // Unset, "on", or any unrecognized value: treat as the default.
            _ => ReaperMode::On,
        }
    }

    /// Whether the reaping ledger (run record + `.sandboxes`/`.networks`) should be
    /// written at all — false only for `off`, since nothing would ever read it.
    pub(crate) fn ledger_enabled(self) -> bool {
        self != ReaperMode::Off
    }

    /// Whether the init-time sweep should run — same gate as [`Self::ledger_enabled`]
    /// (a named predicate of its own for readability at each call site).
    pub(crate) fn sweep_enabled(self) -> bool {
        self != ReaperMode::Off
    }

    /// Whether the per-run watchdog process should be spawned — true only for the
    /// default `on`.
    pub(crate) fn watchdog_enabled(self) -> bool {
        self == ReaperMode::On
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_defaults_to_on() {
        assert_eq!(ReaperMode::parse(None), ReaperMode::On);
    }

    #[test]
    fn recognizes_sweep_and_off_case_insensitively() {
        assert_eq!(ReaperMode::parse(Some("sweep")), ReaperMode::Sweep);
        assert_eq!(ReaperMode::parse(Some("SWEEP")), ReaperMode::Sweep);
        assert_eq!(ReaperMode::parse(Some("off")), ReaperMode::Off);
        assert_eq!(ReaperMode::parse(Some("OFF")), ReaperMode::Off);
    }

    #[test]
    fn unknown_values_fall_back_to_on() {
        assert_eq!(ReaperMode::parse(Some("banana")), ReaperMode::On);
        assert_eq!(ReaperMode::parse(Some("")), ReaperMode::On);
    }

    #[test]
    fn explicit_on_is_on() {
        assert_eq!(ReaperMode::parse(Some("on")), ReaperMode::On);
    }

    #[test]
    fn on_enables_the_ledger_the_sweep_and_the_watchdog() {
        assert!(ReaperMode::On.ledger_enabled());
        assert!(ReaperMode::On.sweep_enabled());
        assert!(ReaperMode::On.watchdog_enabled());
    }

    #[test]
    fn sweep_mode_enables_the_ledger_and_the_sweep_but_not_the_watchdog() {
        assert!(ReaperMode::Sweep.ledger_enabled());
        assert!(ReaperMode::Sweep.sweep_enabled());
        assert!(!ReaperMode::Sweep.watchdog_enabled());
    }

    #[test]
    fn off_disables_everything() {
        assert!(!ReaperMode::Off.ledger_enabled());
        assert!(!ReaperMode::Off.sweep_enabled());
        assert!(!ReaperMode::Off.watchdog_enabled());
    }
}
