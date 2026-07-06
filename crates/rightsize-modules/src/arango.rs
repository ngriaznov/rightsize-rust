//! A single-node ArangoDB container. Auth is disabled by default; see
//! [`ArangoContainer::with_root_password`] to enable it.

use rightsize::{Container, ContainerGuard, Result, Wait};

/// A single-node ArangoDB container.
pub struct ArangoContainer(Container);

impl ArangoContainer {
    /// The guest port ArangoDB's HTTP API listens on.
    const PORT: u16 = 8529;

    /// Builds a container from the pinned default image (`arangodb:3.11`), with auth
    /// disabled (`ARANGO_NO_AUTH=1`).
    pub fn new() -> Self {
        Self::with_image("arangodb:3.11")
    }

    /// Builds a container from a caller-chosen image, with auth disabled.
    pub fn with_image(image: &str) -> Self {
        Self(
            Container::new(image)
                .with_exposed_ports(&[Self::PORT])
                .with_env("ARANGO_NO_AUTH", "1")
                .waiting_for(
                    Wait::for_http("/_api/version")
                        .for_port(Self::PORT)
                        .for_status_code(200),
                ),
        )
    }

    /// Enables auth with the given root password, instead of the default no-auth
    /// setup. `ARANGO_NO_AUTH` was already set by `new`/`with_image`; this call
    /// removes it before adding `ARANGO_ROOT_PASSWORD`.
    ///
    /// This removal is required, not cosmetic: the official ArangoDB entrypoint
    /// checks `ARANGO_NO_AUTH` for mere *presence* (`if [ ! -z "$ARANGO_NO_AUTH" ];
    /// then AUTHENTICATION="false"; fi`), unconditionally, near the very end of the
    /// script, right before it execs `arangod --server.authentication=$AUTHENTICATION`
    /// — this check does not care whether `ARANGO_ROOT_PASSWORD` is also set.
    /// Verified directly against the real `arangodb:3.11` entrypoint (`docker run
    /// --entrypoint /bin/cat arangodb:3.11 /entrypoint.sh`): with both vars set, the
    /// root password gets initialized (the `+x`-guarded init block cares only that
    /// `ARANGO_ROOT_PASSWORD` is set), but `AUTHENTICATION` still ends up `"false"`
    /// because `ARANGO_NO_AUTH` is still present — auth stays off regardless of the
    /// password, so leaving `ARANGO_NO_AUTH` in the spec made the password a no-op.
    pub fn with_root_password(mut self, password: &str) -> Self {
        self.0 = self
            .0
            .remove_env("ARANGO_NO_AUTH")
            .with_env("ARANGO_ROOT_PASSWORD", password);
        self
    }

    /// Boots the container.
    pub async fn start(self) -> Result<ArangoGuard> {
        crate::register_default_backends();
        Ok(ArangoGuard(self.0.start().await?))
    }
}

impl Default for ArangoContainer {
    fn default() -> Self {
        Self::new()
    }
}

/// The running guard for an [`ArangoContainer`].
pub struct ArangoGuard(ContainerGuard);

impl ArangoGuard {
    /// The HTTP API endpoint for the running container.
    pub fn endpoint(&self) -> String {
        format!(
            "http://{}:{}",
            self.0.host(),
            self.0.get_mapped_port(ArangoContainer::PORT).unwrap()
        )
    }

    /// Stops and removes the container, releasing its host port.
    pub async fn stop(self) -> Result<()> {
        self.0.stop().await
    }
}

impl std::ops::Deref for ArangoGuard {
    type Target = ContainerGuard;
    fn deref(&self) -> &ContainerGuard {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_image_and_root_password_smoke() {
        let _ = ArangoContainer::new();
        let _ = ArangoContainer::with_image("arangodb:3.11").with_root_password("s3cret");
    }

    /// Fix 3 (arango override-path assertion): `with_root_password` must remove
    /// `ARANGO_NO_AUTH` entirely — not merely blank its value — and add
    /// `ARANGO_ROOT_PASSWORD` with the given password, in the final builder state.
    ///
    /// Mutation-style evidence: reverting `with_root_password` to the old
    /// `self.0.with_env("ARANGO_ROOT_PASSWORD", password)` (no `remove_env` call)
    /// makes this test fail — `ARANGO_NO_AUTH` would still be present (`entries`
    /// would contain `("ARANGO_NO_AUTH", "1")`), which is exactly the bug: the real
    /// entrypoint checks `ARANGO_NO_AUTH` for presence regardless of
    /// `ARANGO_ROOT_PASSWORD` (see the module doc), so the old code's password was a
    /// no-op against a live container.
    #[test]
    fn with_root_password_removes_no_auth_and_sets_the_password() {
        let container = ArangoContainer::with_image("arangodb:3.11").with_root_password("s3cret");
        let entries = container.0.env();

        assert!(
            entries.iter().all(|(k, _)| k != "ARANGO_NO_AUTH"),
            "ARANGO_NO_AUTH must be entirely absent once a root password is set, not just \
             blanked — the entrypoint checks for presence, not value; found: {entries:?}"
        );
        assert!(
            entries
                .iter()
                .any(|(k, v)| k == "ARANGO_ROOT_PASSWORD" && v == "s3cret"),
            "ARANGO_ROOT_PASSWORD must be set to the given password; found: {entries:?}"
        );
    }

    /// The no-auth default path (no `with_root_password` call) must be unaffected:
    /// `ARANGO_NO_AUTH=1` stays present and no root password entry is added.
    #[test]
    fn without_root_password_no_auth_stays_set() {
        let container = ArangoContainer::with_image("arangodb:3.11");
        let entries = container.0.env();

        assert!(
            entries
                .iter()
                .any(|(k, v)| k == "ARANGO_NO_AUTH" && v == "1"),
            "the no-auth default must be untouched when with_root_password is never called; \
             found: {entries:?}"
        );
        assert!(
            entries.iter().all(|(k, _)| k != "ARANGO_ROOT_PASSWORD"),
            "no root password should be set by default; found: {entries:?}"
        );
    }
}
