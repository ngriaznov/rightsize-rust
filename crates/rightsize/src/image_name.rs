//! [`ImageName`]: a parsed Docker image reference, used to check that an explicitly
//! supplied image belongs to the repository a module constructor declares it
//! understands — before that constructor ever touches a backend.
//!
//! ### Registry-host stripping follows the Docker convention
//!
//! [`ImageName::parse`] extracts the repository component — registry host, tag, and
//! digest all stripped. The first `/`-separated path segment is treated as a
//! registry host only when it contains a `.` or a `:` — the same rule Docker itself
//! uses to distinguish `quay.io/keycloak/keycloak` (registry `quay.io`, repository
//! `keycloak/keycloak`) from a bare two-part namespace like `floci/floci`, which has
//! neither and stays whole.
//!
//! ### The compatibility check and its escape hatch
//!
//! A module constructor that declares a repository it understands (e.g.
//! [`crate`]'s own callers — see `rightsize-modules`' `QdrantContainer`,
//! `qdrant/qdrant`) takes `impl Into<ImageName>` and calls
//! [`ImageName::assert_compatible_with`] with that repository before ever starting a
//! container. A mismatch fails fast with
//! [`crate::error::RightsizeError::IncompatibleImage`] — never a bare wait-strategy
//! timeout on an image that was never going to behave like the module expects.
//!
//! The escape hatch, mirroring Testcontainers' `asCompatibleSubstituteFor`:
//!
//! ```
//! use rightsize::ImageName;
//!
//! let image = ImageName::parse("mycorp/es-hardened:9.4.4")
//!     .as_compatible_substitute_for("elasticsearch");
//! assert!(image.assert_compatible_with("elasticsearch").is_ok());
//! ```

use crate::error::{Result, RightsizeError};

/// A parsed Docker image reference — see the module doc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageName {
    /// The original string passed to [`Self::parse`], used verbatim — no tag
    /// rewriting, no substitution (see [`Self::as_str`]).
    original: String,
    /// The parsed repository component — registry host, tag, and digest stripped.
    repository: String,
    /// Set by [`Self::as_compatible_substitute_for`] — an additional repository this
    /// image is declared compatible with, bypassing the ordinary repository match
    /// for that name specifically.
    substitute_repository: Option<String>,
}

impl ImageName {
    /// Parses `image` into its repository component (registry host, tag, and digest
    /// stripped) while keeping the original string intact for [`Self::as_str`].
    pub fn parse(image: &str) -> Self {
        Self {
            original: image.to_string(),
            repository: extract_repository(image),
            substitute_repository: None,
        }
    }

    /// Declares this image a verified drop-in replacement for `repository`,
    /// bypassing [`Self::assert_compatible_with`]'s ordinary repository match for
    /// that name specifically — mirrors Testcontainers' `asCompatibleSubstituteFor`.
    /// `repository` is parsed the same way [`Self::parse`] parses a full image
    /// reference, so a bare repository (`"postgres"`) and a full reference
    /// (`"docker.io/library/postgres:16"`) name the same override identically.
    pub fn as_compatible_substitute_for(mut self, repository: &str) -> Self {
        self.substitute_repository = Some(extract_repository(repository));
        self
    }

    /// The parsed repository component — registry host, tag, and digest all
    /// stripped.
    pub fn repository(&self) -> &str {
        &self.repository
    }

    /// The original string passed to [`Self::parse`], used verbatim to build the
    /// container — no tag rewriting, no substitution.
    pub fn as_str(&self) -> &str {
        &self.original
    }

    /// Fails fast with [`RightsizeError::IncompatibleImage`] when this image's
    /// repository — and its declared [`Self::as_compatible_substitute_for`]
    /// override, if any — does not match `expected_repository`, the repository a
    /// module constructor declares it understands. Called before any backend work,
    /// so a mismatch never surfaces later as a bare wait-strategy timeout on an
    /// image that was never going to behave like the module expects.
    pub fn assert_compatible_with(&self, expected_repository: &str) -> Result<()> {
        if self.repository == expected_repository {
            return Ok(());
        }
        if self.substitute_repository.as_deref() == Some(expected_repository) {
            return Ok(());
        }
        Err(RightsizeError::IncompatibleImage {
            supplied: self.repository.clone(),
            expected: expected_repository.to_string(),
        })
    }
}

impl std::fmt::Display for ImageName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.original)
    }
}

impl From<&str> for ImageName {
    fn from(image: &str) -> Self {
        Self::parse(image)
    }
}

impl From<String> for ImageName {
    fn from(image: String) -> Self {
        Self::parse(&image)
    }
}

impl From<&String> for ImageName {
    fn from(image: &String) -> Self {
        Self::parse(image)
    }
}

/// Extracts the repository component from a Docker image reference: strips a
/// trailing `@digest` first, then a leading `registry[:port]/` — only when the
/// first path segment contains a `.` or a `:`, the Docker convention that tells a
/// registry host apart from a plain namespace like `floci/floci` — then a trailing
/// `:tag`.
fn extract_repository(image: &str) -> String {
    let without_digest = image.rsplit_once('@').map_or(image, |(rest, _digest)| rest);

    let mut segments: Vec<&str> = without_digest.split('/').collect();
    // `localhost` is a registry host despite carrying neither a dot nor a port — Docker
    // special-cases it, and without this `localhost/myimage` would parse as the two-segment
    // repository `localhost/myimage` and fail an otherwise valid compatibility check.
    let without_registry = if segments.len() > 1
        && (segments[0].contains('.') || segments[0].contains(':') || segments[0] == "localhost")
    {
        segments.remove(0);
        segments.join("/")
    } else {
        without_digest.to_string()
    };

    match without_registry.rsplit_once(':') {
        Some((repo, _tag)) => repo.to_string(),
        None => without_registry,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_name_with_no_tag_parses_to_itself() {
        assert_eq!(ImageName::parse("postgres").repository(), "postgres");
    }

    #[test]
    fn plain_name_with_a_tag_strips_the_tag() {
        assert_eq!(ImageName::parse("postgres:16").repository(), "postgres");
    }

    #[test]
    fn namespaced_name_with_no_registry_stays_whole() {
        // "floci" has neither a `.` nor a `:` — not a registry host, so the whole
        // two-part name is the repository (the Docker convention this module
        // implements — see the module doc).
        assert_eq!(ImageName::parse("floci/floci").repository(), "floci/floci");
        assert_eq!(
            ImageName::parse("floci/floci:latest").repository(),
            "floci/floci"
        );
    }

    #[test]
    fn namespaced_name_with_a_tag_strips_only_the_tag() {
        assert_eq!(
            ImageName::parse("qdrant/qdrant:v1.18.3").repository(),
            "qdrant/qdrant"
        );
    }

    #[test]
    fn bare_localhost_is_a_registry_host() {
        // Docker special-cases `localhost`: a registry host even without a dot or a port, so
        // this must not parse as the two-segment repository `localhost/postgres`.
        assert_eq!(
            ImageName::parse("localhost/postgres:16").repository(),
            "postgres"
        );
        ImageName::parse("localhost/postgres:16")
            .assert_compatible_with("postgres")
            .expect("a local-registry image must still satisfy its module's check");
    }

    #[test]
    fn registry_qualified_name_strips_the_registry_host() {
        // "quay.io" contains a `.` — a registry host, stripped from the repository.
        assert_eq!(
            ImageName::parse("quay.io/keycloak/keycloak").repository(),
            "keycloak/keycloak"
        );
        assert_eq!(
            ImageName::parse("quay.io/keycloak/keycloak:26.0").repository(),
            "keycloak/keycloak"
        );
    }

    #[test]
    fn registry_with_a_port_and_no_dot_is_still_recognized_as_a_registry() {
        // "localhost:5000" has no `.` but does have a `:` — still a registry host
        // per the Docker convention, not a namespace/tag.
        assert_eq!(
            ImageName::parse("localhost:5000/postgres:16").repository(),
            "postgres"
        );
    }

    #[test]
    fn digest_reference_strips_the_digest() {
        assert_eq!(
            ImageName::parse(
                "postgres@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
            .repository(),
            "postgres"
        );
    }

    #[test]
    fn registry_qualified_digest_reference_strips_both_registry_and_digest() {
        assert_eq!(
            ImageName::parse(
                "quay.io/keycloak/keycloak@sha256:\
                 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
            .repository(),
            "keycloak/keycloak"
        );
    }

    #[test]
    fn tag_and_digest_together_strip_to_the_bare_repository() {
        assert_eq!(
            ImageName::parse(
                "postgres:16@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            )
            .repository(),
            "postgres"
        );
    }

    #[test]
    fn as_str_returns_the_original_string_verbatim() {
        let image = ImageName::parse("quay.io/keycloak/keycloak:26.0");
        assert_eq!(image.as_str(), "quay.io/keycloak/keycloak:26.0");
    }

    #[test]
    fn matching_repository_is_compatible() {
        assert!(
            ImageName::parse("postgres:16")
                .assert_compatible_with("postgres")
                .is_ok()
        );
        assert!(
            ImageName::parse("quay.io/keycloak/keycloak:26.0")
                .assert_compatible_with("keycloak/keycloak")
                .is_ok()
        );
    }

    #[test]
    fn mismatched_repository_fails_fast_with_a_typed_error() {
        let err = ImageName::parse("mysql:8")
            .assert_compatible_with("postgres")
            .expect_err("mysql is not postgres");
        match &err {
            RightsizeError::IncompatibleImage { supplied, expected } => {
                assert_eq!(supplied, "mysql");
                assert_eq!(expected, "postgres");
            }
            other => panic!("expected IncompatibleImage, got {other:?}"),
        }
        let msg = err.to_string();
        assert!(msg.contains("mysql"), "{msg}");
        assert!(msg.contains("postgres"), "{msg}");
        assert!(msg.contains("as_compatible_substitute_for"), "{msg}");
    }

    #[test]
    fn substitute_escape_hatch_overrides_the_automatic_check() {
        let image =
            ImageName::parse("mycorp/pg-hardened:16").as_compatible_substitute_for("postgres");
        assert!(image.assert_compatible_with("postgres").is_ok());
        // The escape hatch is scoped to the declared name — a DIFFERENT expected
        // repository is still rejected.
        assert!(image.assert_compatible_with("mysql").is_err());
    }

    #[test]
    fn substitute_escape_hatch_parses_its_argument_the_same_way_as_parse() {
        // Declaring compatibility with a full reference (tag included) names the
        // same repository as declaring it with the bare repository.
        let image = ImageName::parse("mycorp/es-hardened:9.4.4")
            .as_compatible_substitute_for("elasticsearch:9.4.4");
        assert!(image.assert_compatible_with("elasticsearch").is_ok());
    }

    #[test]
    fn from_str_and_from_string_both_parse() {
        let a: ImageName = "postgres:16".into();
        let b: ImageName = "postgres:16".to_string().into();
        assert_eq!(a.repository(), "postgres");
        assert_eq!(b.repository(), "postgres");
    }

    #[test]
    fn display_renders_the_original_string() {
        assert_eq!(ImageName::parse("postgres:16").to_string(), "postgres:16");
    }
}
