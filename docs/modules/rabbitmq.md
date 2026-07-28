# RabbitMqContainer

A single-node RabbitMQ container with the management plugin enabled. Defaults to a
`guest`/`guest` credential pair (the image's own default) so `amqp_url()` is usable
with zero configuration.

**Default image:** floats to `rabbitmq:management` — this module previously
pinned `rabbitmq:4-management-alpine`. Every other module in this crate that has
a `new()` floats to `<repository>:latest`, but plain `rabbitmq:latest` carries
no management plugin at all, and this module is built around it — `management`
is the floating tag that keeps the plugin.
**Guest ports:** `5672` (AMQP), `15672` (management)
**Expected repository:** `rabbitmq`

| Method | On | Effect |
|---|---|---|
| `RabbitMqContainer::new()` | builder | Floating default image (`rabbitmq:management`), `guest`/`guest`. |
| `RabbitMqContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.with_username(u)` / `.with_password(p)` | builder | Override either credential before `start()`. |
| `.start()` | builder → `Result<RabbitMqGuard>` | Checks the image's repository, then boots the container. |
| `.username()` / `.password()` | guard | The configured credential pair. |
| `.amqp_url()` | guard | `amqp://user:pass@host:port` — the AMQP listener. |
| `.management_url()` | guard | The management UI/API base URI. |
| `.stop()` | guard | Stops and removes the container, releases its ports. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `rabbitmq` before any backend is resolved or any sandbox is created,
which keeps the constructors infallible like every other module's. A mismatch
returns `RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("rabbitmq")` is the escape hatch for a verified
drop-in replacement from another registry. `new()` goes through this same check
against its own floating reference, so it can never fail in practice.

## Readiness — verified against a real 4.x boot

`rabbitmq:4-management-alpine` still prints the same `"Server startup complete"` line
the 3.x series used (captured verbatim from a real boot with this module's env):

```text
2026-07-04 08:47:17.936423+00:00 [info] <0.1036.0> started TCP listener on [::]:5672
 completed with 4 plugins.
2026-07-04 08:47:18.001311+00:00 [info] <0.900.0> Server startup complete; 4 plugins started.
2026-07-04 08:47:18.001311+00:00 [info] <0.900.0>  * rabbitmq_prometheus
2026-07-04 08:47:18.001311+00:00 [info] <0.900.0>  * rabbitmq_management
2026-07-04 08:47:18.001311+00:00 [info] <0.900.0>  * rabbitmq_management_agent
2026-07-04 08:47:18.001311+00:00 [info] <0.900.0>  * rabbitmq_web_dispatch
```

The line appears exactly once, so `for_log_message` at `times=1` is unambiguous —
unlike Postgres/MySQL/MariaDB, there is no double-boot restart to race here. The
management API's own `/api/health/checks/...` endpoints require authenticated
requests, so the log line is the simpler and equally reliable readiness signal.

No `with_memory_limit` override: booted clean on msb's default ~450M microVM RAM
(observed ~5.5s IT round-trip on both backends — an Erlang VM, not a JVM, so no
Paketo/QuickStart-style heap demand).

## A 4.x behavior change worth knowing

RabbitMQ 4.x deprecates `transient_nonexcl_queues` and, per the broker's own startup
warning, "this feature can still be used for now" — but a client that declares a
**non-durable, non-exclusive** queue (`durable=false, exclusive=false`) may be
rejected with `reply-code=541 INTERNAL_ERROR` depending on the deployed policy,
reproduced directly against this module's previously pinned
`rabbitmq:4-management-alpine` image (see above — `new()` floats since then, but
this behavior is documented RabbitMQ 4.x entrypoint behavior, not something tied to
that specific tag). Declare durable, non-exclusive queues (or exclusive transient
ones) from client code exercising this container; this module itself declares no
queues.

## Complete example

This module's own integration test round-trips over the management HTTP API rather
than an AMQP client library — `lapin` (the natural AMQP client choice) pulls in a
`time`-based transitive tree above this workspace's MSRV. The management API's
`/api/queues/.../publish` and `/api/queues/.../get` endpoints exercise the same
"does the broker actually work" claim at zero extra dependency cost:

```rust,ignore
use rightsize_modules::RabbitMqContainer;

#[tokio::test]
async fn declare_publish_and_get_round_trips() -> Result<(), Box<dyn std::error::Error>> {
    let guard = RabbitMqContainer::new().start().await?;

    let agent = ureq::Agent::new_with_defaults();
    let admin = guard.management_url();
    let auth = format!("Basic {}", "guest:guest"); // base64 in the real test

    // Durable, non-exclusive — RabbitMQ 4.x's transient_nonexcl_queues deprecation
    // (see above) can reject the opposite combination.
    agent
        .put(format!("{admin}/api/queues/%2f/smoke"))
        .header("Content-Type", "application/json")
        .header("Authorization", &auth)
        .send(r#"{"durable":true,"auto_delete":false}"#)?;

    agent
        .post(format!("{admin}/api/exchanges/%2f/amq.default/publish"))
        .header("Content-Type", "application/json")
        .header("Authorization", &auth)
        .send(
            r#"{"properties":{},"routing_key":"smoke","payload":"hello-rightsize",
                "payload_encoding":"string"}"#,
        )?;

    let mut get = agent
        .post(format!("{admin}/api/queues/%2f/smoke/get"))
        .header("Content-Type", "application/json")
        .header("Authorization", &auth)
        // The management API's field is `ackmode`, not `ack_mode` — verified
        // directly against a real 4.x boot.
        .send(r#"{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}"#)?;
    let body = get.body_mut().read_to_string()?;
    assert!(body.contains("hello-rightsize"));

    guard.stop().await?;
    Ok(())
}
```

## Backend notes

No memory-limit override is needed on either backend — see Readiness above.
