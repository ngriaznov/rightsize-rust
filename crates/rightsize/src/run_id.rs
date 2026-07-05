//! A per-process identifier used to name and label every container/network this
//! process creates, so a reaper (see `crate::backends` and the msb/docker backends'
//! orphan sweeps) can tell a crashed prior run's leftovers apart from the live run's
//! own resources.

use std::sync::OnceLock;

/// The current process's stable 8-hex-character run id.
pub struct RunId;

static RUN_ID: OnceLock<String> = OnceLock::new();

impl RunId {
    /// The stable, 8-hex-character id for this process. Computed once (from the
    /// current time and process id, which is unique enough for a naming/labeling
    /// scheme rather than a security token) and cached for the process's lifetime —
    /// every call within one process sees the same value.
    pub fn value() -> &'static str {
        RUN_ID.get_or_init(generate)
    }
}

/// Builds the 8-hex-character id: hashes the current time plus the process id into a
/// `u64` and hex-encodes it, keeping the last 8 hex digits (32 bits) — plenty of
/// entropy for telling one process's run apart from another's on the same host, which
/// is all a naming/labeling scheme needs.
fn generate() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    // A simple, dependency-free mix: multiply the time by an odd constant, fold in the
    // pid, keep the low 32 bits. Not cryptographic — just enough spread that two
    // processes started in the same nanosecond (pid differs) or the same pid at
    // different times (time differs) don't collide in practice.
    let mixed = nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(pid);
    let low32 = (mixed & 0xFFFF_FFFF) as u32;
    format!("{low32:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_is_eight_hex_chars() {
        let v = RunId::value();
        assert_eq!(v.len(), 8, "expected 8 hex chars, got '{v}'");
        assert!(
            v.chars().all(|c| c.is_ascii_hexdigit()),
            "expected all hex digits, got '{v}'"
        );
    }

    #[test]
    fn value_is_stable_within_a_process() {
        let a = RunId::value();
        let b = RunId::value();
        assert_eq!(a, b);
    }

    #[test]
    fn generate_produces_eight_hex_chars_deterministically_shaped() {
        // Not asserting a specific value (time-derived), just the shape: exactly 8 lowercase hex chars.
        let v = generate();
        assert_eq!(v.len(), 8);
        assert!(
            v.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }
}
