# WireMockContainer

A WireMock server container for stubbing HTTP dependencies in integration tests.
There is no first-class in-process WireMock story for Rust integration tests, so
this module fills a real gap.

**Default image:** `wiremock/wiremock:3.13.2`
**Guest port:** `8080`

| Method | On | Effect |
|---|---|---|
| `WireMockContainer::new()` | builder | Pinned default image. |
| `WireMockContainer::with_image(image)` | builder | Caller-chosen image. |
| `.start()` | builder → `Result<WireMockGuard>` | Boots the container. |
| `.base_url()` | guard | The stub server's base URI — mount stubbed paths under this. |
| `.admin_url()` | guard | The `/__admin` management API's base URI (stub CRUD, request journal, health). |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## Readiness — verified against a real 3.13.2 boot

WireMock 3.x ships a dedicated `/__admin/health` endpoint (unlike some older 2.x
builds, where `/__admin/mappings` was the only reliable 200). Verified directly
against a real container:

```text
$ curl http://127.0.0.1:<port>/__admin/health
{"status":"healthy","message":"Wiremock is ok","version":"3.13.2","uptimeInSeconds":9,...}
```

so this module waits on that endpoint rather than falling back to
`/__admin/mappings`.

No `with_memory_limit` override was needed — the JVM boots comfortably on msb's
default ~450M microVM RAM (observed ~5.5s IT round-trip on msb; a small
embedded-Jetty app, not a JVM-heavy cluster like Pinot).

## Complete example

```rust,ignore
use rightsize_modules::WireMockContainer;

const STUB: &str = r#"{"request":{"method":"GET","urlPath":"/hello"},
                        "response":{"status":200,"body":"world"}}"#;

#[tokio::test]
async fn stub_mapping_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let guard = WireMockContainer::new().start().await?;

    let agent = ureq::Agent::new_with_defaults();
    agent
        .post(format!("{}/mappings", guard.admin_url()))
        .header("Content-Type", "application/json")
        .send(STUB)?;

    let mut get = agent.get(format!("{}/hello", guard.base_url())).call()?;
    let body = get.body_mut().read_to_string()?;
    assert_eq!(body, "world");

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

No memory-limit override is needed on either backend — see Readiness above.
