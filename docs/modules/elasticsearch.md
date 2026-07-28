# ElasticsearchContainer

A single-node Elasticsearch container, queried over its REST interface (port 9200).
The transport port (9300) is exposed too, but this module's helpers don't wrap
it — nothing outside the cluster talks the transport protocol directly.

**No default image, and no `new()`.** Elastic publishes no floating tag —
`elasticsearch:latest`, `:9`, and `:8` are all 404 on Docker Hub, verified
directly; Elastic publishes only fully-qualified version tags (402 of them,
actively maintained, e.g. `9.4.4`, `8.19.19`). There is no version this module
could pick on your behalf that wouldn't eventually 404 out from under you, so
`with_image` is the only constructor and an explicit tag is required.

**Guest ports:** `9200` (REST — what the helpers use), `9300` (transport, exposed but not wrapped)
**Expected repository:** `elasticsearch`

| Method | On | Effect |
|---|---|---|
| `ElasticsearchContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.start()` | builder → `Result<ElasticsearchGuard>` | Checks the image's repository, then boots the container. |
| `.rest_url()` | guard | The REST interface's base URI. |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `elasticsearch` before any backend is resolved or any sandbox is created,
which keeps the constructor infallible like every other module's:

```rust,ignore
use rightsize::ImageName;
use rightsize_modules::ElasticsearchContainer;

// Checked on start, common case.
let guard = ElasticsearchContainer::with_image("elasticsearch:9.4.4").start().await?;

// A verified drop-in replacement from another registry — the escape hatch.
let guard = ElasticsearchContainer::with_image(
    ImageName::parse("mycorp/es-hardened:9.4.4").as_compatible_substitute_for("elasticsearch"),
)
.start()
.await?;
```

A mismatch returns `RightsizeError::IncompatibleImage` — naming the supplied
repository, the expected one, and the override above — rather than letting an
unrelated image run all the way to a wait-strategy timeout.

## Env — verified against a real `elasticsearch:9.4.4` (arm64) boot

`discovery.type=single-node` skips the cluster-formation bootstrap a real
multi-node deployment needs. `xpack.security.enabled=false` drops TLS/auth setup
that a smoke-test container has no use for. `ES_JAVA_OPTS=-Xms512m -Xmx512m`
fixes the JVM heap rather than letting it size itself off host memory — the same
lever [Cassandra's module](./cassandra.md) uses via its own heap env vars. No
control characters were found in the image's baked env.

## Cluster health is `yellow` forever on a single node

A single-node cluster has nowhere to place a replica shard, so `cluster_health`
never reaches `green` — it sits at `yellow` indefinitely. A readiness check
waiting for `green` would hang until its startup timeout on every single-node
boot this module ever creates. The module's own readiness wait never checks
cluster health at all, precisely to avoid that trap; poll `GET
/_cluster/health` for `yellow`, not `green`, if you want a health-based check
of your own.

**Memory limit:** 2560 MB, verified at that value — ~1.1 GB used of a 2.48 GB guest.

**Readiness:** `GET /` returning 200 on the REST port, observed at 27s on a real
boot. This is the earliest signal the HTTP layer is serving — it does not imply
`yellow`/`green` cluster health, only that the node answers requests at all.
Startup timeout is 300s, to give a loaded CI runner headroom beyond that
observed 27s.

## Complete example

```rust,ignore
use rightsize_modules::ElasticsearchContainer;

#[tokio::test]
async fn index_then_search_round_trips_over_http() -> Result<(), Box<dyn std::error::Error>> {
    let guard = ElasticsearchContainer::with_image("elasticsearch:9.4.4")
        .start()
        .await?;

    let agent = ureq::Agent::new_with_defaults();
    let rest_url = guard.rest_url();

    // ?refresh=true makes the write visible to search immediately.
    agent
        .put(format!("{rest_url}/books/_doc/1?refresh=true"))
        .header("Content-Type", "application/json")
        .send(r#"{"title":"Snow Crash","author":"Neal Stephenson"}"#)?;

    let mut search = agent.get(format!("{rest_url}/books/_search?q=title:Snow")).call()?;
    let body = search.body_mut().read_to_string()?;
    assert!(body.contains("Snow Crash"));

    guard.stop().await?;
    Ok(())
}
```

Verified end to end: `PUT /books/_doc/1?refresh=true` then `GET /books/_search`
returned the indexed document.

## Backend notes

No control characters were found in the image's baked env on either backend.
