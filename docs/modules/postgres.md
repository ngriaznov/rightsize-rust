# PostgresContainer

A single-node PostgreSQL container. Defaults to a `test`/`test`/`test`
user/password/database trio so `connection_string()` is usable with zero
configuration.

**Default image:** `postgres:18-alpine`
**Guest port:** `5432`

| Method | On | Effect |
|---|---|---|
| `PostgresContainer::new()` | builder | Pinned default image, `test`/`test`/`test`. |
| `PostgresContainer::with_image(image)` | builder | Caller-chosen image. |
| `.with_username(u)` / `.with_password(p)` / `.with_database(d)` | builder | Override any of the trio before `start()`. |
| `.start()` | builder → `Result<PostgresGuard>` | Boots the container. |
| `.username()` / `.password()` / `.database_name()` | guard | The configured trio. |
| `.connection_string()` | guard | `postgres://user:pass@host:port/db`. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## Readiness: why `times = 2`

The postgres entrypoint starts the server **twice**: once to run `initdb` scripts
against it, then shuts it down and starts it again for real — printing "database
system is ready to accept connections" **both times**. Waiting for the first
occurrence races the restart: a client can connect to the init-time server just
before it's torn down. This module's wait strategy is
`Wait::for_log_message(".*database system is ready to accept connections.*", 2)` —
it waits for the *second* occurrence, the durable listen. This is the canonical
showcase for `Wait::for_log_message`'s `times` parameter — see
[Wait Strategies](../core-concepts/wait-strategies.md).

## The control-character env fix (`DOCKER_PG_LLVM_DEPS`)

The official `postgres:*-alpine` image bakes `DOCKER_PG_LLVM_DEPS` into its manifest
with a **literal tab character** in the value (a package list built with `\t\t`
continuation). msb 0.6.2's krun VMM builder panics with `InvalidAscii` on that boot-env
value before the guest ever starts — reproduced with zero rightsize-set env vars, so
this is the image's own baked value, not anything this module or your test adds.
Docker is unaffected. The module works around it by overriding the variable to an
empty string (`.with_env("DOCKER_PG_LLVM_DEPS", "")`) — a no-op for the build the
image already baked, and harmless on Docker.

If you hit `InvalidAscii` on a *different* image under `RIGHTSIZE_BACKEND=microsandbox`,
suspect a baked env var with a control character the same way — see
[Troubleshooting](../troubleshooting.md).

## Complete example

```rust,ignore
use rightsize_modules::PostgresContainer;

#[tokio::test]
async fn postgres_round_trips_a_row() -> Result<(), Box<dyn std::error::Error>> {
    let guard = PostgresContainer::new().start().await?;

    let conn_str = guard.connection_string().replacen("postgres://", "postgresql://", 1);
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection task ended: {e}");
        }
    });

    client
        .execute("CREATE TABLE smoke (id INT PRIMARY KEY, note TEXT)", &[])
        .await?;
    client
        .execute("INSERT INTO smoke (id, note) VALUES ($1, $2)", &[&1i32, &"hello-rightsize"])
        .await?;
    let rows = client.query("SELECT note FROM smoke WHERE id = $1", &[&1i32]).await?;
    let note: &str = rows[0].get(0);
    assert_eq!(note, "hello-rightsize");

    guard.stop().await?;
    Ok(())
}
```

Note the `postgres://` → `postgresql://` rewrite: `tokio-postgres` requires the
`postgresql://` scheme; `connection_string()` returns `postgres://` to match the
conventional URI form used elsewhere in this crate's docs and other clients.

## Backend notes

No memory-limit override needed — Postgres's default footprint fits microsandbox's
~450 MB microVM default. The only backend-relevant quirk is the
`DOCKER_PG_LLVM_DEPS` fix above, which is microsandbox-only in *cause* but applied
unconditionally (harmless on Docker) so the module behaves identically on both.
