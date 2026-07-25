# CassandraContainer

A single-node Apache Cassandra container.

**Default image:** `cassandra:5.0.8`
**Guest port:** `9042` (CQL native protocol)

| Method | On | Effect |
|---|---|---|
| `CassandraContainer::new()` | builder | Pinned default image, `with_memory_limit(2560)`. |
| `CassandraContainer::with_image(image)` | builder | Caller-chosen image. |
| `.start()` | builder → `Result<CassandraGuard>` | Boots the container. |
| `.contact_point()` | guard | `<host>:<port>` CQL contact point, for drivers that take one directly. |
| `.cql_port()` | guard | The mapped host port for the CQL native protocol. |
| `.local_datacenter()` | guard | The local datacenter name a driver's load-balancing policy needs (`datacenter1`). |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## `GPG_KEYS` must be overridden to a tab-free value — this is not optional

`cassandra:5.0.8`'s baked env includes a `GPG_KEYS` value containing a literal TAB
character (a package-signing key list built with tab-separated continuation, same
shape as the `DOCKER_PG_LLVM_DEPS`/`postgres:*-alpine` case documented on
[`PostgresContainer`](./postgres.md)). msb 0.6.6's krun VMM builder panics on any
baked env value containing a control character, and it panics *before the guest ever
boots* — reproduced directly, identical `msb run` invocation:

```text
sandbox process exited (signal: 6 (SIGABRT)) before agent relay became available
```

with `msb logs --source system` showing the actual panic site:

```text
panicked at msb_krun_vmm-0.1.25/src/builder.rs:1154: ... Err value: InvalidAscii
```

`.with_env("GPG_KEYS", "")` sidesteps it: `GPG_KEYS` is build-time-only in this image
(used only when the image itself is built, to import signing keys), so overriding it
has zero effect at container-run time. Verified directly: the identical `msb run`
aborts with the panic above without this override and boots clean with it. Docker is
unaffected either way — this override is a no-op there, not a workaround for a
docker-specific problem. This module always sets it; it is not exposed as a builder
override, because there is no reason a caller would ever want the tab-bearing value
back.

## Heap — kept small on purpose

`MAX_HEAP_SIZE=512M` and `HEAP_NEWSIZE=128M` keep the JVM's own heap modest rather
than letting it size itself off host memory, the same reasoning this crate's other
JVM modules apply via `with_memory_limit` alone — here the image's own env knobs are
the more direct lever.

## Memory limit — 2560 MB, verified at that value

`with_memory_limit(2560)` is this module's default, verified against a real boot with
the heap settings above.

## Readiness — `Starting listening for CQL clients`, observed at 58s

That line is Cassandra's own log signal that the CQL native protocol port (9042) is
accepting connections. Startup timeout is 300s: 58s was observed on a quiet local
machine, and this crate's precedent for a single heavyweight JVM server — 180s for
[`KeycloakContainer`](./keycloak.md) and [`MySqlContainer`](./mysql.md)'s
loaded-CI-runner case — undershoots a server this much heavier than either, so the
budget here is wider rather than reused as-is.

Verified end to end: `cqlsh` ran `CREATE KEYSPACE` → `CREATE TABLE` → `INSERT` →
`SELECT`, and the row came back.

## Complete example

```rust,ignore
use rightsize_modules::CassandraContainer;

#[tokio::test]
async fn keyspace_round_trips_via_cqlsh() -> Result<(), Box<dyn std::error::Error>> {
    let guard = CassandraContainer::new().start().await?;

    let cql = "CREATE KEYSPACE IF NOT EXISTS smoke WITH REPLICATION = \
               {'class': 'SimpleStrategy', 'replication_factor': 1}; \
               CREATE TABLE IF NOT EXISTS smoke.t (id int PRIMARY KEY, val text); \
               INSERT INTO smoke.t (id, val) VALUES (1, 'rightsize');";
    guard.exec(&["cqlsh", "-e", cql]).await?;

    let select = guard
        .exec(&["cqlsh", "-e", "SELECT val FROM smoke.t WHERE id = 1;"])
        .await?;
    assert!(select.stdout.contains("rightsize"));

    guard.stop().await?;
    Ok(())
}
```

(The round-trip goes through the image's own bundled `cqlsh` binary via `exec` rather
than a Cassandra driver crate — see
`crates/rightsize-modules/tests/cassandra_it.rs`.)

## Backend notes

`with_memory_limit(2560)` and the `GPG_KEYS` override are both set unconditionally by
the module — see Memory and the `GPG_KEYS` section above. The `GPG_KEYS` override is
required to boot at all on microsandbox; it is a harmless no-op on Docker.
