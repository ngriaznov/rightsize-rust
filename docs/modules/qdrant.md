# QdrantContainer

A single-node Qdrant vector database container, queried over its REST interface
(port 6333). The gRPC port (6334) is exposed too, but this module's helpers
don't wrap it — HTTP-first, matching the house convention for HTTP-first
modules (see [ClickHouse's module](./clickhouse.md)).

**Default image:** floats to `qdrant/qdrant:latest` — Qdrant, unlike
[Elasticsearch](./elasticsearch.md), publishes a floating tag, so `new()` tracks
upstream through it rather than a version pinned to this crate's own release
cycle.
**Guest ports:** `6333` (REST — what the helpers use), `6334` (gRPC, exposed but not wrapped)
**Expected repository:** `qdrant/qdrant`

| Method | On | Effect |
|---|---|---|
| `QdrantContainer::new()` | builder | Floating default image (`qdrant/qdrant:latest`). |
| `QdrantContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.start()` | builder → `Result<QdrantGuard>` | Checks the image's repository, then boots the container. |
| `.rest_url()` | guard | The REST interface's base URI. |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `qdrant/qdrant` before any backend is resolved or any sandbox is created,
which keeps the constructors infallible like every other module's:

```rust,ignore
use rightsize::ImageName;
use rightsize_modules::QdrantContainer;

// Checked on start, common case.
let guard = QdrantContainer::with_image("qdrant/qdrant:v1.18.3").start().await?;

// A verified drop-in replacement from another registry — the escape hatch.
let guard = QdrantContainer::with_image(
    ImageName::parse("mycorp/qdrant-hardened:v1.18.3")
        .as_compatible_substitute_for("qdrant/qdrant"),
)
.start()
.await?;
```

A mismatch returns `RightsizeError::IncompatibleImage` — naming the supplied
repository, the expected one, and the override above — rather than letting an
unrelated image run all the way to a wait-strategy timeout. `new()` goes
through this same check against its own floating reference, so it can never
fail in practice.

## Readiness — HTTP 200 on `/readyz`, answered on the first poll

Verified against a real `qdrant/qdrant:v1.18.3` (arm64; note the image's own
tags carry a `v` prefix, unlike `new()`'s `:latest`) boot: `GET /readyz`
returned 200 on the very first poll after boot — no restart/double-boot race
to account for here, so this module keeps the default 120s startup timeout
rather than widening it.

## No memory limit — verified directly

This module sets no memory limit. Verified against a real boot with the limit
removed entirely: the full create-collection/upsert/search round trip below
completed in a guest reporting ~480 MB total.

## Complete example

```rust,ignore
use rightsize_modules::QdrantContainer;

#[tokio::test]
async fn create_upsert_search_round_trips_over_http() -> Result<(), Box<dyn std::error::Error>> {
    let guard = QdrantContainer::new().start().await?;

    let agent = ureq::Agent::new_with_defaults();
    let rest_url = guard.rest_url();

    agent
        .put(format!("{rest_url}/collections/smoke"))
        .header("Content-Type", "application/json")
        .send(r#"{"vectors":{"size":4,"distance":"Dot"}}"#)?;

    agent
        .put(format!("{rest_url}/collections/smoke/points?wait=true"))
        .header("Content-Type", "application/json")
        .send(r#"{"points":[{"id":1,"vector":[1.0,0.0,0.0,0.0],"payload":{"city":"test"}}]}"#)?;

    let mut search = agent
        .post(format!("{rest_url}/collections/smoke/points/search"))
        .header("Content-Type", "application/json")
        .send(r#"{"vector":[1.0,0.0,0.0,0.0],"limit":1}"#)?;
    let body = search.body_mut().read_to_string()?;
    assert!(body.contains("\"score\""));

    guard.stop().await?;
    Ok(())
}
```

Verified end to end: created a collection (vector size 4, `Dot` distance),
upserted a point, and a search against it returned a score.

## Backend notes

No memory-limit override is needed on either backend — see above.
