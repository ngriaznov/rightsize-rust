# MariaDbContainer

A single-node MariaDB container. Defaults to a `test`/`test`/`test`
user/password/database trio (plus `MARIADB_ROOT_PASSWORD=test`) so
`connection_string()` is usable with zero configuration.

**Default image:** `mariadb:11.4`
**Guest port:** `3306`

| Method | On | Effect |
|---|---|---|
| `MariaDbContainer::new()` | builder | Pinned default image, `test`/`test`/`test`. |
| `MariaDbContainer::with_image(image)` | builder | Caller-chosen image. |
| `.with_username(u)` / `.with_password(p)` / `.with_database(d)` | builder | Override any of the trio before `start()`. |
| `.start()` | builder → `Result<MariaDbGuard>` | Boots the container. |
| `.username()` / `.password()` / `.database_name()` | guard | The configured trio. |
| `.connection_string()` | guard | `mysql://user:pass@host:port/db` — MariaDB speaks the MySQL wire protocol, so this crate's house `mysql://` scheme applies here too. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## Readiness — empirically pinned, following MySqlContainer's precedent exactly

The official `mariadb` entrypoint double-boots exactly like MySQL's: once as a
throwaway "temp server" to run init scripts (which prints `ready for connections`
with `port: 0`, i.e. no port bound yet), then for real on port 3306. Captured
verbatim from a real `docker run mariadb:11.4` boot with this module's env
(`MARIADB_USER=test`, `MARIADB_DATABASE=test`, `MARIADB_ROOT_PASSWORD=test`):

```text
2026-07-04  8:47:29 0 [Note] mariadbd: ready for connections.
Version: '11.4.12-MariaDB-ubu2404'  socket: '/run/mysqld/mysqld.sock'  port: 0  mariadb.org binary distribution
2026-07-04  8:47:30 0 [Note] Server socket created on IP: '0.0.0.0', port: '3306'.
2026-07-04  8:47:30 0 [Note] Server socket created on IP: '::', port: '3306'.
2026-07-04  8:47:30 0 [Note] mariadbd: ready for connections.
Version: '11.4.12-MariaDB-ubu2404'  socket: '/run/mysqld/mysqld.sock'  port: 3306  mariadb.org binary distribution
```

Unlike MySQL 8.4, MariaDB has no X Plugin adding a third `ready for connections` line
with a decoy `3306`-prefixed port (`33060`), so there's no false-match trap to anchor
against — but the temp server's `port: 0` line still means an unanchored `times=2`
count would be correct only by coincidence (it happens to work here because there are
exactly two `ready for connections` lines total). This module follows the MySQL house
precedent anyway and anchors on the literal `port: 3306` immediately followed, later
on the same line, by `mariadb.org binary distribution` — so the wait is robust to the
temp-server line's exact wording even if a future MariaDB point release changes it.

Ships as a plain `Wait::for_log_message(r".*port: 3306.*mariadb\.org binary distribution.*", 1)`.

No `with_memory_limit` override needed — same InnoDB-footprint precedent as MySQL
8.4: boots clean on msb's default ~450M microVM RAM (observed ~14.8s IT round-trip
on msb).

## Complete example

```rust,ignore
use mysql_async::prelude::*;
use rightsize_modules::MariaDbContainer;

#[tokio::test]
async fn mariadb_round_trips_a_row() -> Result<(), Box<dyn std::error::Error>> {
    let guard = MariaDbContainer::new().start().await?;

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

No memory-limit override is needed on either backend — see Readiness above.
