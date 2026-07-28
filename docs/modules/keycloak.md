# KeycloakContainer

A single-node Keycloak container in `start-dev` mode (in-memory H2, no external
database — fine for tests, never for production).

**Default image:** floats to `quay.io/keycloak/keycloak:latest` — this module
previously pinned `quay.io/keycloak/keycloak:26.4`.
**Guest ports:** `8080` (HTTP / auth server), `9000` (management — health lives here, see below)
**Expected repository:** `keycloak/keycloak` (the `quay.io` registry host is
stripped per the Docker convention)

| Method | On | Effect |
|---|---|---|
| `KeycloakContainer::new()` | builder | Floating default image, `admin`/`admin`, `with_memory_limit(1024)`. |
| `KeycloakContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.with_admin_username(u)` / `.with_admin_password(p)` | builder | Override either bootstrap admin credential before `start()`. |
| `.start()` | builder → `Result<KeycloakGuard>` | Checks the image's repository, then boots the container. |
| `.admin_username()` / `.admin_password()` | guard | The configured bootstrap admin credentials. |
| `.auth_server_url()` | guard | The auth server's base URI (HTTP port) — realm/OIDC endpoints live under this. |
| `.management_url()` | guard | The management interface's base URI (health/metrics — port 9000, a different port than `auth_server_url()`). |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository against `keycloak/keycloak` before any
backend is resolved or any sandbox is created, which keeps the constructors
infallible like every other module's. The `quay.io` registry host is stripped
before the comparison — `quay.io/keycloak/keycloak` and `keycloak/keycloak`
both satisfy the check. A mismatch returns `RightsizeError::IncompatibleImage`;
`ImageName::parse(image).as_compatible_substitute_for("keycloak/keycloak")` is
the escape hatch for a verified drop-in replacement from another registry.
`new()` goes through this same check against its own floating reference, so it
can never fail in practice.

## Admin bootstrap env — 26.x renamed these, verified against the pinned tag

Keycloak 26.x replaced the old `KEYCLOAK_ADMIN`/`KEYCLOAK_ADMIN_PASSWORD` pair with
`KC_BOOTSTRAP_ADMIN_USERNAME`/`KC_BOOTSTRAP_ADMIN_PASSWORD`. Verified directly
against `quay.io/keycloak/keycloak:26.4`: booting with only the new names produces
`KC-SERVICES0077: Created temporary admin user with username admin` in the log; the
legacy names are not read by this image at all. This module sets only the new names.

## Health lives on the management port (9000), not 8080

Captured verbatim from a real boot:

```text
Listening on: http://0.0.0.0:8080. Management interface listening on http://0.0.0.0:9000.
```

With `KC_HEALTH_ENABLED=true` set, `GET /health` on port **9000** returns `200 OK`
(body: literal `OK`); the same path on 8080 404s, and the commonly assumed
`/health/ready` sub-path 404s on this tag too — this image's SmallRye Health root
aggregate is served bare at `/health`, not `/health/ready` — pinned to `/health` on
9000 accordingly.

## Memory — Quarkus JVM, needed the ladder

Booted under msb's default ~450M microVM RAM, the JVM is `Killed` (OOM) partway
through startup (captured: `'java' ... -XX:MaxRAMPercentage=70 ... Killed`) — same
Paketo/Quarkus-on-microVM story as
[`SpringCloudConfigContainer`](./spring-cloud-config.md). Retried with `-m 1024M`:
boots clean, `/health` reports `200` well within the startup timeout.
`with_memory_limit(1024)` is this module's default.

## Complete example

```rust,ignore
use rightsize_modules::KeycloakContainer;

#[tokio::test]
async fn oidc_discovery_document_asserts_the_issuer() -> Result<(), Box<dyn std::error::Error>> {
    let guard = KeycloakContainer::new().start().await?;

    let mut resp = ureq::Agent::new_with_defaults()
        .get(format!(
            "{}/realms/master/.well-known/openid-configuration",
            guard.auth_server_url()
        ))
        .call()?;
    let body = resp.body_mut().read_to_string()?;
    let expected = format!("\"issuer\":\"{}/realms/master\"", guard.auth_server_url());
    assert!(body.contains(&expected));

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

`with_memory_limit(1024)` is set unconditionally by the module — see Memory above.
