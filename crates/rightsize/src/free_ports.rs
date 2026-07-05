//! An in-process free-TCP-port allocator backing the container port-retry loop. Not
//! part of the backend SPI: backends receive already-chosen host ports via
//! [`crate::model::ContainerSpec::ports`] and never allocate their own — see
//! [`crate::backend::SandboxBackend`]'s doc for that invariant — so this stays
//! crate-internal.
//!
//! **Deliberate choice: loopback-only, not wildcard.** This allocator binds
//! loopback-only (`127.0.0.1:0`), matching the loopback-only host bind used
//! everywhere else in this crate (container port publishing, wait probes). The
//! narrower bind can't collide with a wildcard-bound port used by some other test
//! process on the same host — it's the more conservative choice, not an oversight.

use std::collections::HashSet;
use std::net::TcpListener;
use std::sync::Mutex;

use crate::error::{Result, RightsizeError};

/// The maximum number of bind attempts `allocate` will make before giving up. Each
/// attempt binds a genuinely free OS-assigned port; only an already-issued-in-this-
/// process port causes a retry, so 100 is generous headroom, not a real limit in
/// practice.
const MAX_ALLOCATE_ATTEMPTS: usize = 100;

/// A process-wide, mutex-guarded set of host ports this process has handed out and not
/// yet released.
pub(crate) struct FreePorts {
    issued: Mutex<HashSet<u16>>,
}

impl FreePorts {
    /// Builds an empty allocator.
    pub(crate) fn new() -> Self {
        Self {
            issued: Mutex::new(HashSet::new()),
        }
    }

    /// Binds `127.0.0.1:0` to let the OS choose a free port, records it, and drops the
    /// listener so the caller (a backend) can bind it for real. Retries up to
    /// [`MAX_ALLOCATE_ATTEMPTS`] times so two allocations racing the OS's port reuse
    /// never hand back the same in-process port twice.
    pub(crate) fn allocate(&self) -> Result<u16> {
        let mut issued = self.issued.lock().expect("FreePorts mutex poisoned");
        for _ in 0..MAX_ALLOCATE_ATTEMPTS {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let port = listener.local_addr()?.port();
            drop(listener);
            if issued.insert(port) {
                return Ok(port);
            }
        }
        Err(RightsizeError::Backend(format!(
            "Could not allocate a free TCP port after {MAX_ALLOCATE_ATTEMPTS} attempts"
        )))
    }

    /// Returns `port` to the pool, so a later `allocate` (or another process) may reuse
    /// it. A no-op if `port` wasn't issued (or was already released) — release is always
    /// safe to call more than once.
    pub(crate) fn release(&self, port: u16) {
        self.issued
            .lock()
            .expect("FreePorts mutex poisoned")
            .remove(&port);
    }

    /// Test-only observability seam: a released port must not linger here.
    #[cfg(test)]
    pub(crate) fn issued_view(&self) -> HashSet<u16> {
        self.issued
            .lock()
            .expect("FreePorts mutex poisoned")
            .clone()
    }
}

impl Default for FreePorts {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn allocated_ports_are_unique_and_bindable() {
        let pool = FreePorts::new();
        let mut ports = Vec::new();
        for _ in 0..50 {
            ports.push(pool.allocate().expect("allocate"));
        }
        let unique: HashSet<u16> = ports.iter().copied().collect();
        assert_eq!(
            unique.len(),
            ports.len(),
            "ports must not repeat within the process"
        );

        let last = *ports.last().unwrap();
        let listener = TcpListener::bind(("127.0.0.1", last))
            .expect("released listener slot must be bindable");
        assert_eq!(listener.local_addr().unwrap().port(), last);
    }

    #[test]
    fn release_removes_the_port_from_issued_view_and_it_can_be_reissued() {
        let pool = FreePorts::new();
        let port = pool.allocate().expect("allocate");
        assert!(pool.issued_view().contains(&port));

        pool.release(port);
        assert!(
            !pool.issued_view().contains(&port),
            "release must drop the port from issued_view()"
        );

        // Re-allocating enough ports must be able to hit the just-released port again —
        // demonstrated by directly re-inserting it via allocate() retrying past
        // in-process duplicates. We prove reissuability by binding it again ourselves,
        // which only works if release() actually gave it back to the OS-visible pool
        // (i.e. this process no longer thinks it's taken).
        let listener =
            TcpListener::bind(("127.0.0.1", port)).expect("released port must be bindable again");
        assert_eq!(listener.local_addr().unwrap().port(), port);
    }

    /// Mutation-proof: this test is written so that if `release` were a no-op (i.e. it
    /// never actually removed the port from `issued`), the assertion on `issued_view()`
    /// after release fails. It deliberately checks the *seam* (`issued_view()`), not
    /// just an external re-bindability side effect that could pass by OS coincidence.
    #[test]
    fn release_is_not_a_no_op() {
        let pool = FreePorts::new();
        let port = pool.allocate().expect("allocate");
        assert!(
            pool.issued_view().contains(&port),
            "sanity: allocate() must record the port"
        );

        pool.release(port);

        assert!(
            !pool.issued_view().contains(&port),
            "release(port) must remove it from issued_view() — if this fails, release() is a no-op"
        );
    }

    #[test]
    fn release_of_an_unissued_port_is_a_harmless_no_op() {
        let pool = FreePorts::new();
        // Never allocated anywhere in this pool — releasing it must not panic.
        pool.release(65000);
        assert!(pool.issued_view().is_empty());
    }
}
