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

    /// Test-only book-keeping seam: marks `port` issued directly, via the same
    /// `HashSet::insert` [`Self::allocate`] relies on to reject an already-issued
    /// port — without needing a fresh OS-assigned port to land on that exact number.
    /// Returns whether `port` was newly inserted (`false` means it was already
    /// considered issued). This lets a test prove a released port is reissuable
    /// against the pool's own accounting, rather than against the OS actually
    /// handing that same port number back, which it has no obligation to do.
    #[cfg(test)]
    pub(crate) fn reserve(&self, port: u16) -> bool {
        self.issued
            .lock()
            .expect("FreePorts mutex poisoned")
            .insert(port)
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

        // The property: an allocated port's probe listener is released, so the caller
        // can bind it. On a shared CI runner any other process may legitimately grab a
        // port in the gap between the allocator's release and this bind — one AddrInUse
        // is environmental noise, not a disproof (observed exactly so on a loaded
        // runner under coverage instrumentation). A fresh allocation is retried a few
        // times; every attempt failing, or any error other than AddrInUse, still fails.
        let mut last_err = None;
        let bound = (0..5).find_map(|_| {
            let port = pool.allocate().expect("allocate");
            match TcpListener::bind(("127.0.0.1", port)) {
                Ok(listener) => Some((port, listener)),
                Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                    last_err = Some(e);
                    None
                }
                Err(e) => panic!("released listener slot must be bindable: {e}"),
            }
        });
        let (port, listener) = bound.unwrap_or_else(|| {
            panic!(
                "five consecutive released slots were unbindable — that is the \
                 allocator holding ports, not neighbor noise: {last_err:?}"
            )
        });
        assert_eq!(listener.local_addr().unwrap().port(), port);
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

        // Reissuability used to be proven by rebinding this exact port with a raw
        // `TcpListener::bind`, but that asserted something the OS never promised: it
        // has no obligation to hand this exact port number back on the next bind, and
        // on a shared CI host any other process can grab it in the gap between
        // release and this bind (observed exactly as an AddrInUse under coverage
        // instrumentation on a loaded runner) — environmental noise, not a defect in
        // `release`. The property this test actually owns is in-process: `release`
        // must give the pool's own book-keeping the port back. `reserve` performs the
        // same `issued.insert(port)` `allocate()` relies on to reject an
        // already-issued port, so a `true` result here means the slot is genuinely
        // free in the pool's accounting, with no OS bind involved.
        assert!(
            pool.reserve(port),
            "a released port must be freshly issuable again in the pool's own book-keeping"
        );
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
