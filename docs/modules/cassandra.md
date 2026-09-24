# CassandraContainer

A single-node Apache Cassandra container.

**Default image:** floats to `cassandra:latest` — this module previously pinned
`cassandra:5.0.8`.
**Guest port:** `9042` (CQL native protocol)
**Expected repository:** `cassandra`

| Method | On | Effect |
|---|---|---|
| `CassandraContainer::new()` | builder | Floating default image, `with_memory_limit(2560)`. |
| `CassandraContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.start()` | builder → `Result<CassandraGuard>` | Checks the image's repository, then boots the container. |
| `.contact_point()` | guard | `<host>:<port>` CQL contact point, for drivers that take one directly. |
| `.cql_port()` | guard | The mapped host port for the CQL native protocol. |
| `.local_datacenter()` | guard | The local datacenter name a driver's load-balancing policy needs (`datacenter1`). |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `cassandra` before any backend is resolved or any sandbox is created,
which keeps the constructors infallible like every other module's. A mismatch
returns `RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("cassandra")` is the escape hatch for a verified
drop-in replacement from another registry. `new()` goes through this same check
against its own floating reference, so it can never fail in practice.

## `GPG_KEYS` override — kept as a guard, not required on the pinned msb

`cassandra:5.0.8`'s baked env includes a `GPG_KEYS` value containing a literal TAB
character (a package-signing key list built with tab-separated continuation, same
shape as the `DOCKER_PG_LLVM_DEPS`/`postgres:*-alpine` case documented on
[`PostgresContainer`](./postgres.md)). On older msb releases (0.6.x), the krun VMM
builder panicked on any baked env value containing a control character, before the
guest ever booted — reproduced directly, identical `msb run` invocation:

```text
sandbox process exited (signal: 6 (SIGABRT)) before agent relay became available
```

with `msb logs --source system` showing the actual panic site:

```text
panicked at msb_krun_vmm-0.1.25/src/builder.rs:1154: ... Err value: InvalidAscii
```

On the pinned msb 0.7.1 this is fixed upstream — the image boots with its baked
`GPG_KEYS` and no override at all. `.with_env("GPG_KEYS", "")` costs nothing either
way: `GPG_KEYS` is build-time-only in this image (used only when the image itself is
built, to import signing keys), so overriding it has zero effect at container-run
time. This module still sets it unconditionally, as a harmless guard for anyone
pointing `MSB_PATH` at an older msb. Docker is unaffected either way — the override
is a no-op there too. It is not exposed as a builder override, because there is no
reason a caller would ever want the tab-bearing value back.

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
the module — see Memory and the `GPG_KEYS` section above. On the pinned msb the
`GPG_KEYS` override is a harmless guard, not a boot requirement; it is a no-op on
Docker either way.
