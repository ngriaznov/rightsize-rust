# MySqlContainer

A single-node MySQL container. Defaults to a `test`/`test`/`test`
user/password/database trio (plus `MYSQL_ROOT_PASSWORD=test`) so
`connection_string()` is usable with zero configuration.

**Default image:** `mysql:8.4`
**Guest port:** `3306`

| Method | On | Effect |
|---|---|---|
| `MySqlContainer::new()` | builder | Pinned default image, `test`/`test`/`test`. |
| `MySqlContainer::with_image(image)` | builder | Caller-chosen image. |
| `.with_username(u)` / `.with_password(p)` / `.with_database(d)` | builder | Override any of the trio before `start()`. |
| `.start()` | builder → `Result<MySqlGuard>` | Boots the container. |
| `.username()` / `.password()` / `.database_name()` | guard | The configured trio. |
| `.connection_string()` | guard | `mysql://user:pass@host:port/db`. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## Readiness — empirically pinned, not guessed

The official entrypoint boots `mysqld` **twice**: once as a throwaway "temp server"
to run init scripts, then for real. Both prints, plus the X Plugin's own "ready for
connections" line, contain the substring `ready for connections`, and naively
counting occurrences is a trap: the temp server's X Plugin binds `port: 33060` —
whose digits *start with* `3306`, so an unanchored `port: 3306` search false-matches
it too.

Captured verbatim from a real `docker run mysql:8.4` boot with this module's env
(`MYSQL_USER=test`, `MYSQL_DATABASE=test`, `MYSQL_ROOT_PASSWORD=test`):

```text
[System] [MY-011323] [Server] X Plugin ready for connections. Socket: /var/run/mysqld/mysqlx.sock
[System] [MY-010931] [Server] /usr/sbin/mysqld: ready for connections. Version: '8.4.10'  socket: '/var/run/mysqld/mysqld.sock'  port: 0  MySQL Community Server - GPL.
...(init scripts run, temp server shuts down)...
[System] [MY-011323] [Server] X Plugin ready for connections. Bind-address: '::' port: 33060, socket: /var/run/mysqld/mysqlx.sock
[System] [MY-010931] [Server] /usr/sbin/mysqld: ready for connections. Version: '8.4.10'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  MySQL Community Server - GPL.
```

Four lines contain `ready for connections`; only the **last** is the real server bound
to 3306. The temp server prints `port: 0` (no port yet) and the X Plugin lines print
`33060`, whose `3306` prefix would satisfy an unanchored match. This module ships a
small custom `WaitStrategy` (predating `Wait::for_log_message`'s move to a real regex
engine — see [Wait Strategies](../core-concepts/wait-strategies.md)) rather than a
regex: it requires a line to contain `mysqld: ready for connections`, then checks that
`port: 3306` appears later on that same line immediately followed by end-of-line or a
**non-digit** — which is exactly what rules out `33060`. This is equivalent to a regex
anchor, `port: 3306($|[^0-9])`; the hand-written character check is untouched by the
regex-engine swap, since it was never built on the shared matcher in the first place.

## Complete example

```rust,ignore
use mysql_async::prelude::*;
use rightsize_modules::MySqlContainer;

#[tokio::test]
async fn mysql_round_trips_a_row() -> Result<(), Box<dyn std::error::Error>> {
    let guard = MySqlContainer::new().start().await?;

    let pool = mysql_async::Pool::new(guard.connection_string().as_str());
    let mut conn = pool.get_conn().await?;

    conn.query_drop("CREATE TABLE smoke (id INT PRIMARY KEY, note TEXT)").await?;
    conn.exec_drop(
        "INSERT INTO smoke (id, note) VALUES (?, ?)",
        (1i32, "hello-rightsize"),
    ).await?;
    let note: Option<String> = conn
        .exec_first("SELECT note FROM smoke WHERE id = ?", (1i32,))
        .await?;
    assert_eq!(note.as_deref(), Some("hello-rightsize"));

    drop(conn);
    pool.disconnect().await.ok();
    guard.stop().await?;
    Ok(())
}
```

## Backend notes

No memory-limit override: MySQL 8.4's InnoDB default footprint fits msb's
default ~450 MB microVM RAM, unlike
[`SpringCloudConfigContainer`](./spring-cloud-config.md)'s Paketo JVM image — no
module-level memory floor was warranted here after measurement.
