//! Checkpoint/restore: `ContainerGuard::checkpoint()` and `Container::from_checkpoint`
//! (see `crate::container`, which owns both — this module owns only the plain
//! [`Checkpoint`] value and its image-ref generator, the same split `crate::reuse`
//! uses for the reuse feature).
//!
//! **Be honest about what this is**: a checkpoint is a FILESYSTEM capture of a
//! running container (via the active backend's image-commit primitive), not a memory
//! snapshot — restore boots a FRESH container whose filesystem starts where the
//! checkpoint left off, but every process inside it restarts from scratch. True
//! microVM memory snapshots need upstream microsandbox support and stay on the
//! roadmap (see `docs/roadmap.md`).

use crate::model::ContainerSpec;

/// The outcome of [`crate::ContainerGuard::checkpoint`]: the committed image plus the
/// source container's full spec, so [`crate::Container::from_checkpoint`] has
/// everything it needs to boot an equivalent container from the new image.
#[derive(Clone, Debug)]
pub struct Checkpoint {
    /// The committed image reference, `rightsize/checkpoint:<12-hex>` — random per
    /// checkpoint (see `generate_image_ref`). NOT auto-reaped: this is an image,
    /// not a container, so it survives process exit and every own-run cleanup path
    /// in this crate. See the checkpoints docs for the cleanup one-liner.
    pub image_ref: String,
    /// The source container's full spec at the moment it was checkpointed — env,
    /// command, exposed ports, memory limit, and everything else `ContainerSpec`
    /// carries. `Container::from_checkpoint` reads env/command/exposed-ports/memory
    /// limit from this; it deliberately does NOT re-apply `mounts` (the checkpoint
    /// image already has whatever those mounts wrote, baked in) or `network_id`/
    /// `aliases` (network topology never survives a restore).
    pub spec: ContainerSpec,
}

/// Generates a fresh `rightsize/checkpoint:<12-hex>` image reference. The 12 hex
/// characters (48 bits) are mixed from the current time, this process's id, and a
/// process-wide monotonic counter — the same dependency-free mixing shape
/// `crate::run_id::generate` uses for the per-process run id, extended with a counter
/// so two checkpoints taken back-to-back in the same process (even within the same
/// timer tick) never collide. Not cryptographic — just enough spread that a random
/// per-checkpoint tag never collides with another one on the same host in practice.
pub(crate) fn generate_image_ref() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::SeqCst) as u128;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    let mixed = nanos
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(pid)
        .wrapping_add(seq);
    let low48 = (mixed & 0xFFFF_FFFF_FFFF) as u64;
    format!("rightsize/checkpoint:{low48:012x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_twelve_lowercase_hex(s: &str) -> bool {
        s.len() == 12
            && s.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    }

    #[test]
    fn generate_image_ref_has_the_pinned_repo_and_a_twelve_hex_tag() {
        let r = generate_image_ref();
        let tag = r
            .strip_prefix("rightsize/checkpoint:")
            .unwrap_or_else(|| panic!("expected the rightsize/checkpoint: prefix, got {r}"));
        assert!(is_twelve_lowercase_hex(tag), "{r}");
    }

    #[test]
    fn generate_image_ref_differs_across_back_to_back_calls() {
        let a = generate_image_ref();
        let b = generate_image_ref();
        assert_ne!(a, b, "two checkpoints must never share an image ref");
    }
}
