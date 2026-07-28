# ArangoContainer

A single-node ArangoDB container. Auth is **disabled by default**
(`ARANGO_NO_AUTH=1`); call `.with_root_password(...)` to enable it instead.

**Default image:** floats to `arangodb:latest` — this module previously pinned
`arangodb:3.11`.
**Guest port:** `8529`
**Expected repository:** `arangodb`

| Method | On | Effect |
|---|---|---|
| `ArangoContainer::new()` | builder | Floating default image, auth disabled. |
| `ArangoContainer::with_image(image)` | builder | Caller-chosen image, auth disabled, kept verbatim. |
| `.with_root_password(password)` | builder | Enables auth with the given root password instead of no-auth. |
| `.start()` | builder → `Result<ArangoGuard>` | Checks the image's repository, then boots the container. |
| `.endpoint()` | guard | HTTP API base URI, e.g. `http://127.0.0.1:<port>`. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `arangodb` before any backend is resolved or any sandbox is created,
which keeps the constructors infallible like every other module's. A mismatch
returns `RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("arangodb")` is the escape hatch for a verified
drop-in replacement from another registry. `new()` goes through this same check
against its own floating reference, so it can never fail in practice.

## `with_root_password` — why it removes `ARANGO_NO_AUTH` instead of just setting a password

This is worth understanding before you reach for it: the official ArangoDB
entrypoint checks `ARANGO_NO_AUTH` for mere **presence**, unconditionally, near the
very end of the script (`if [ ! -z "$ARANGO_NO_AUTH" ]; then AUTHENTICATION="false";
fi`) — right before it execs `arangod --server.authentication=$AUTHENTICATION`. This
check does not care whether `ARANGO_ROOT_PASSWORD` is also set. Verified directly
against the real entrypoint (`docker run --entrypoint /bin/cat arangodb:3.11
/entrypoint.sh`): with both variables set, the root password *does* get initialized
(the init block only cares that `ARANGO_ROOT_PASSWORD` is set), but `AUTHENTICATION`
still ends up `"false"` because `ARANGO_NO_AUTH` is still present — auth stays off
regardless of the password.

So `with_root_password` calls `remove_env("ARANGO_NO_AUTH")` before setting
`ARANGO_ROOT_PASSWORD` — a core-level primitive
([`Container::remove_env`](../core-concepts/containers-and-guards.md)) that exists
specifically for cases like this, where a later env var must retract an earlier
one's effect on the entrypoint, not merely be shadowed by last-wins value
resolution.

## Complete example

```rust,ignore
use rightsize_modules::ArangoContainer;

#[tokio::test]
async fn arango_serves_version() -> Result<(), Box<dyn std::error::Error>> {
    let guard = ArangoContainer::new().start().await?;

    let body = ureq::get(format!("{}/_api/version", guard.endpoint()))
        .call()?
        .body_mut()
        .read_to_string()?;
    assert!(body.contains("version"));

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

No memory-limit override and no known quirks beyond the auth-flag behavior above,
which is purely an ArangoDB entrypoint detail — identical on both backends.
