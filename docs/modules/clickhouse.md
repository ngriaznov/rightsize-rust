# ClickHouseContainer

A single-node ClickHouse container, queried over its HTTP interface (port 8123).
The native protocol port (9000) is exposed too, but this module's helpers are
HTTP-first: the HTTP interface needs no client dependency (`ureq` as a dev-dep, no
runtime crate), matching the house convention for HTTP-first modules.

Defaults to a `test`/`test` user/password pair and a `test` database so
`http_url()` plus basic auth is usable with zero configuration.

**Default image:** `clickhouse/clickhouse-server:25.8`
**Guest ports:** `8123` (HTTP — what the helpers use), `9000` (native protocol, exposed but not wrapped)

| Method | On | Effect |
|---|---|---|
| `ClickHouseContainer::new()` | builder | Pinned default image, `test`/`test`/`test`. |
| `ClickHouseContainer::with_image(image)` | builder | Caller-chosen image. |
| `.with_username(u)` / `.with_password(p)` / `.with_database(d)` | builder | Override any of the trio before `start()`. |
| `.start()` | builder → `Result<ClickHouseGuard>` | Boots the container. |
| `.username()` / `.password()` / `.database_name()` | guard | The configured trio. |
| `.http_url()` | guard | The HTTP interface's base URI — `POST` a SQL body, basic-auth'd. |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## Env var names — verified against a real boot

`CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DB` are the image's
documented names and were confirmed directly: booting with those three set produces
`/entrypoint.sh: create new user 'test' instead 'default'` and `/entrypoint.sh:
create database 'test'` in the logs, and `curl -u test:test` against the HTTP
interface authenticates and runs queries successfully.

## Readiness — verified against a real 25.8 (LTS) boot

`GET /ping` on the HTTP port returns the literal body `Ok.\n` as soon as the HTTP
server is accepting connections (protocol-aware — no restart/double-boot race like
the Postgres/MySQL/MariaDB entrypoints have), so `for_http("/ping")` at the default
200 status code is the correct and simplest readiness signal.

No `with_memory_limit` override was needed at default settings (observed ~12s IT
round-trip on msb, no memory-ladder escalation needed) — a single-node ClickHouse
server, unlike [Pinot's](./pinot.md) QuickStart cluster, is not a JVM process at all.

## Complete example

```rust,ignore
use rightsize_modules::ClickHouseContainer;

#[tokio::test]
async fn create_insert_select_round_trips_over_http() -> Result<(), Box<dyn std::error::Error>> {
    let guard = ClickHouseContainer::new().start().await?;

    let agent = ureq::Agent::new_with_defaults();
    let auth = format!("Basic {}", "dGVzdDp0ZXN0"); // base64("test:test") in the real test
    let query = |sql: &'static str| {
        agent.post(guard.http_url()).header("Authorization", &auth).send(sql)
    };

    query("CREATE TABLE t (x Int32) ENGINE=Memory")?;
    query("INSERT INTO t VALUES (1)")?;
    let mut select = query("SELECT x FROM t")?;
    let body = select.body_mut().read_to_string()?;
    assert_eq!(body.trim(), "1");

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

No memory-limit override is needed on either backend — see Readiness above.
