# Neo4jContainer

A single-node Neo4j Community container, queried over its HTTP Cypher transaction
endpoint (`/db/neo4j/tx/commit`) — no bolt driver dependency needed, matching the
house convention for HTTP-first modules ([ClickHouse](./clickhouse.md),
[Pinot](./pinot.md)). The bolt port (7687) is still exposed and its URI available
via `bolt_url()` for callers who do want a real driver.

**Default image:** floats to `neo4j:latest` — this module previously pinned
`neo4j:5-community`.
**Guest ports:** `7474` (HTTP), `7687` (bolt)
**Expected repository:** `neo4j`

| Method | On | Effect |
|---|---|---|
| `Neo4jContainer::new()` | builder | Floating default image, `neo4j`/`rightsize-test`, `with_memory_limit(1024)`. |
| `Neo4jContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.with_password(p)` | builder | Overrides the password half before `start()` (the image requires ≥8 characters). |
| `.start()` | builder → `Result<Neo4jGuard>` | Checks the image's repository, then boots the container. |
| `.username()` | guard | The fixed admin username (`neo4j` — no env var to change it). |
| `.password()` | guard | The configured admin password. |
| `.http_url()` | guard | The HTTP interface's base URI — Cypher transactions via `POST {http_url}/db/neo4j/tx/commit`. |
| `.bolt_url()` | guard | The bolt interface's URI, for callers using a real bolt driver. |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `neo4j` before any backend is resolved or any sandbox is created, which
keeps the constructors infallible like every other module's. A mismatch returns
`RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("neo4j")` is the escape hatch for a verified
drop-in replacement from another registry. `new()` goes through this same check
against its own floating reference, so it can never fail in practice.

There's no `with_username` — the username is fixed by the image at `neo4j`.

## Readiness — `Started.` is the exact log line, verified against a real boot

Captured verbatim from `neo4j:5-community`:

```text
... INFO  Bolt enabled on 0.0.0.0:7687.
... INFO  HTTP enabled on 0.0.0.0:7474.
... INFO  Remote interface available at http://localhost:7474/
... INFO  Started.
```

`Started.` is logged only after both connectors are already listening, so it's both
accurate and simpler than a two-port HTTP/bolt race.

Ships as `.*Started\..*` — a real regex, so the escaped dot means exactly what it
reads: a literal `.` right after "Started", not a wildcard.

## The image refuses short passwords

`neo4j`/`neo4j` is rejected at boot — passwords must be at least 8 characters — so
this module defaults to `neo4j`/`rightsize-test`, giving zero-configuration use of
`http_url()` plus basic auth. `with_password` still requires an 8+-character value.

## Memory — measured, needed the ladder

At msb's default ~450 MB microVM RAM, the server logs `ERROR Invalid memory
configuration - exceeds physical memory` and shuts itself down cleanly (`INFO
Stopped.`) rather than hanging or getting OOM-killed — Neo4j's own
memory-recommendation calculator sizes the page cache and heap off total visible RAM
and refuses to start if the sums don't fit. Retried with `-m 1024M`: boots clean.
`with_memory_limit(1024)` is this module's default, same number as
[`KeycloakContainer`](./keycloak.md).

## Complete example

```rust,ignore
use rightsize_modules::Neo4jContainer;

#[tokio::test]
async fn create_then_match_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let guard = Neo4jContainer::new().start().await?;

    let agent = ureq::Agent::new_with_defaults();
    let auth = format!("Basic {}", "bmVvNGo6cmlnaHRzaXplLXRlc3Q"); // base64 in the real test
    let commit_url = format!("{}/db/neo4j/tx/commit", guard.http_url());
    let commit = |statement: &str| {
        let body = format!(r#"{{"statements":[{{"statement":"{statement}"}}]}}"#);
        agent
            .post(&commit_url)
            .header("Content-Type", "application/json")
            .header("Authorization", &auth)
            .send(body)
    };

    let mut create = commit("CREATE (n:Test {name: 'hello'}) RETURN n.name AS name")?;
    let create_body = create.body_mut().read_to_string()?;
    assert!(create_body.contains("\"errors\":[]"));

    let mut matched = commit("MATCH (n:Test {name: 'hello'}) RETURN n.name AS name")?;
    let match_body = matched.body_mut().read_to_string()?;
    assert!(match_body.contains("\"hello\""));

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

`with_memory_limit(1024)` is set unconditionally by the module — see Memory above.
