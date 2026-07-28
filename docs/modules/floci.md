# FlociContainer

A [floci.io](https://floci.io) cloud emulator — one native Quarkus image per cloud
provider, each speaking that provider's REST APIs against an in-memory backing
store. One module type covers all three variants; pick one with the
`FlociContainer::aws`/`azure`/`gcp` factory functions rather than a bare
constructor — each factory pins the provider's own image and guest port.

**Default images:** float to `floci/floci:latest` (AWS), `floci/floci-az:latest`
(Azure), `floci/floci-gcp:latest` (GCP) — this module previously pinned
`floci/floci:1.5.30`, `floci/floci-az:0.8.0`, and `floci/floci-gcp:0.4.0`
respectively.
**Guest ports:** `4566` (AWS), `4577` (Azure), `4588` (GCP)
**Expected repositories:** `floci/floci` (AWS), `floci/floci-az` (Azure),
`floci/floci-gcp` (GCP) — three distinct repositories, one checked per variant

| Method | On | Effect |
|---|---|---|
| `FlociContainer::aws()` / `aws_with_image(image)` | builder | The AWS emulator — S3, DynamoDB, SQS, etc. |
| `FlociContainer::azure()` / `azure_with_image(image)` | builder | The Azure emulator. |
| `FlociContainer::gcp()` / `gcp_with_image(image)` | builder | The GCP emulator. |
| `.start()` | builder → `Result<FlociGuard>` | Checks the image's repository against the variant's own expected repository, then boots the container. |
| `.endpoint_url()` | guard | This variant's REST endpoint — the base URI for every emulated API call. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

There's no plain constructor to call directly — always go through one of the three
factory functions, which select the right default image and guest port for that
provider.

## Compatibility checking — three distinct repositories, one per variant

Unlike every other module in this crate, `FlociContainer` wraps three unrelated
images behind one type, so there is no single repository it understands: each
factory declares its own. `aws_with_image` takes `impl Into<ImageName>` and
checks against `floci/floci`; `azure_with_image` against `floci/floci-az`;
`gcp_with_image` against `floci/floci-gcp` — each keeps the image verbatim, and
`start()` checks the stored image's repository (registry host, tag, and digest
stripped) against whichever repository the factory that built this container
declared, before any backend is resolved or any sandbox is created. This keeps
the constructors infallible like every other module's. A mismatch returns
`RightsizeError::IncompatibleImage`; e.g.
`ImageName::parse(image).as_compatible_substitute_for("floci/floci")` is the
escape hatch for a verified drop-in replacement for the AWS variant (swap in
the `-az`/`-gcp` repository for the other two). `aws()`/`azure()`/`gcp()` each
go through this same check against their own floating reference, so none of
them can fail in practice.

## Readiness — `/health` works uniformly, unlike the AWS-flavored `/_localstack/health`

The AWS variant ships a LocalStack-compatible `/_localstack/health` endpoint, but
the Azure and GCP variants do not carry that path (Azure: `501`; GCP: `404`). All
three, however, answer a plain `GET /health` with `200` and a small JSON status body
the moment the embedded Quarkus HTTP listener is up — verified directly against real
boots of all three images. `/health` is pinned as the one wait path that works
across all three variants.

## No signing needed — verified against the AWS variant's S3 surface

The AWS variant's S3-shaped REST endpoints accept unsigned requests with no
`Authorization` header at all: `PUT /<bucket>`, `PUT /<bucket>/<key>`, and `GET
/<bucket>/<key>` all round-trip successfully with a bare HTTP client call — no
SigV4, no AWS SDK dependency required.

No `with_memory_limit` override is needed for any variant — all three images are
native (GraalVM) Quarkus binaries that settle at roughly 11-27 MiB RSS (`docker
stats`, real boot).

## Complete example

```rust,ignore
use rightsize_modules::FlociContainer;

#[tokio::test]
async fn aws_variant_s3_round_trips_with_no_signing() -> Result<(), Box<dyn std::error::Error>> {
    let guard = FlociContainer::aws().start().await?;

    let agent = ureq::Agent::new_with_defaults();
    let endpoint = guard.endpoint_url();

    agent.put(format!("{endpoint}/rightsize-test-bucket")).send_empty()?;
    agent
        .put(format!("{endpoint}/rightsize-test-bucket/hello.txt"))
        .send("hello world")?;

    let mut get_obj = agent
        .get(format!("{endpoint}/rightsize-test-bucket/hello.txt"))
        .call()?;
    let body = get_obj.body_mut().read_to_string()?;
    assert_eq!(body, "hello world");

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

No memory-limit override is needed on either backend for any variant — see
Readiness above.
