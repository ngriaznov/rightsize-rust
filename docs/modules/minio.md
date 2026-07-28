# MinioContainer

A single-node MinIO container, an S3-compatible object store. Defaults to a
`testuser`/`testpassword` root credential pair so `s3_url()` plus that pair is
usable with zero configuration.

**Default image:** floats to `minio/minio:latest` — this module previously
pinned `minio/minio:RELEASE.2025-09-07T16-13-09Z`.
**Guest ports:** `9000` (S3 API — what the helpers use), `9001` (web console, exposed but not wrapped)
**Expected repository:** `minio/minio`

| Method | On | Effect |
|---|---|---|
| `MinioContainer::new()` | builder | Floating default image, `testuser`/`testpassword`. |
| `MinioContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.with_root_user(u)` / `.with_root_password(p)` | builder | Override either credential before `start()`. |
| `.start()` | builder → `Result<MinioGuard>` | Checks the image's repository, then boots the container. |
| `.root_user()` / `.root_password()` | guard | The configured root credentials. |
| `.s3_url()` | guard | The S3 API's base URI (port 9000) — sign requests against this with the configured credentials. |
| `.console_url()` | guard | The web console's base URI (port 9001) — human use only. |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `minio/minio` before any backend is resolved or any sandbox is created,
which keeps the constructors infallible like every other module's. A mismatch
returns `RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("minio/minio")` is the escape hatch for a verified
drop-in replacement from another registry. `new()` goes through this same check
against its own floating reference, so it can never fail in practice.

## The default entrypoint does not serve — a command is required

Unlike every other module in this crate, MinIO's image needs an explicit command:
`server /data --console-address :9001`. The bare image entrypoint alone starts the
`minio` binary with no subcommand and does not bring up the S3 API at all — this
module always sets that command.

## Why `testuser`/`testpassword`, not the house `test`/`test` pair

MinIO rejects a root password shorter than 8 characters at boot, so the `test`/`test`
pair every other credentialed module in this crate defaults to (see
[`ClickHouseContainer`](./clickhouse.md), [`MySqlContainer`](./mysql.md)) cannot be
reused here — `test` is 4 characters. `testuser`/`testpassword` is this module's own
default pair instead.

## Readiness — protocol-aware, answered on the first poll

`Wait::for_http("/minio/health/live").for_port(9000)` is MinIO's own documented
liveness probe. Verified against a real `minio/minio:RELEASE.2025-09-07T16-13-09Z`
boot (log confirms `linux/arm64`): the endpoint returned `200` on the very first poll
after boot — no restart/double-boot race to account for here.

## Auth is enforced, not just configured

Verified directly: with the configured root credentials, `mc mb` (make bucket), `mc
cp` (upload a file), then `mc cat` (read it back) round-tripped the written bytes
exactly (`mc pipe` was tried first but an exec'd `mc pipe` under the microsandbox
backend consumes stdin and either dumps its goroutines and exits non-zero or hangs
outright — `mc cp` needs no stdin and round-trips reliably on both backends). A
subsequent anonymous `GET /` against the S3 API — no credentials at all — came back
`AccessDenied`, proving the credential pair is actually gating access rather than
merely being accepted and ignored.

## Memory — no limit needed, verified directly

This module sets no memory limit. Verified against a real boot with the limit removed
entirely: MinIO came up in a guest reporting ~480 MB total, answered
`/minio/health/live` on the first poll, and completed a full bucket-create, upload,
and read-back round-trip. Unlike the JVM modules here
([`KeycloakContainer`](./keycloak.md), [`Neo4jContainer`](./neo4j.md)), a single-node
MinIO server has no fixed heap region to reserve up front.

## Complete example

```rust,ignore
use rightsize_modules::MinioContainer;

#[tokio::test]
async fn bucket_round_trips_through_mc() -> Result<(), Box<dyn std::error::Error>> {
    let guard = MinioContainer::new().start().await?;

    let cmd = format!(
        "export MC_HOST_local=http://{}:{}@127.0.0.1:9000 && \
         mc mb local/smoke >/dev/null && \
         printf 'rightsize' > /srv/object && \
         mc cp /srv/object local/smoke/object >/dev/null && \
         mc cat local/smoke/object",
        guard.root_user(),
        guard.root_password(),
    );
    let result = guard.exec(&["sh", "-c", &cmd]).await?;
    assert_eq!(result.stdout.trim(), "rightsize");

    guard.stop().await?;
    Ok(())
}
```

(The round-trip goes through the image's own bundled `mc` binary via `exec` rather
than an S3 SDK — see the integration test for the full picture, including the
HTTP-level health and anonymous-denial checks:
`crates/rightsize-modules/tests/minio_it.rs`.)

## Backend notes

No memory limit is set by the module — see Memory above.
