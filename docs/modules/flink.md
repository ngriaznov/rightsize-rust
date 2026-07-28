# FlinkContainer

An Apache Flink **JobManager**, optionally paired with a companion **TaskManager**
via `with_task_manager()` for a real session cluster that can actually run jobs (a
bare JobManager has zero task slots and can only accept/reject submissions, never
execute them).

**Default image:** floats to `flink:latest` — this module previously pinned
`flink:1.20.5`.
**Guest ports:** `8081` (REST), `6123` (RPC — only meaningful once a TaskManager joins)
**Expected repository:** `flink`

| Method | On | Effect |
|---|---|---|
| `FlinkContainer::new()` | builder | Floating default image, `with_memory_limit(1024)`. |
| `FlinkContainer::with_image(image)` | builder | Caller-chosen image, kept verbatim. |
| `.with_task_manager()` | builder → `Result<Self>` | Adds a companion TaskManager on a shared network for a real session cluster with task slots — **docker only**, see below. |
| `.start()` | builder → `Result<FlinkGuard>` | Checks the image's repository, then boots the JobManager (and TaskManager, if added). |
| `.rest_url()` | guard | The JobManager REST base URI (`/overview`, `/taskmanagers`, job submission, etc). |
| `.stop()` | guard | Stops and removes both containers (if a TaskManager was added), releases ports, closes the internally-created network. |

## Compatibility checking

`with_image` takes `impl Into<ImageName>` and keeps the image verbatim. `start()`
then checks that image's repository (registry host, tag, and digest stripped)
against `flink` before any backend is resolved or any sandbox is created, which
keeps the constructors infallible like every other module's — this single check
covers both roles, since `with_task_manager()` boots its companion TaskManager
from the same stored image. A mismatch returns
`RightsizeError::IncompatibleImage`; `ImageName::parse(image)
.as_compatible_substitute_for("flink")` is the escape hatch for a verified
drop-in replacement from another registry. `new()` goes through this same check
against its own floating reference, so it can never fail in practice.

## Topology

A real Flink session cluster is two processes bound by a **persistent bidirectional
RPC connection** (Pekko/Akka remoting), not a one-shot request/response: the
TaskManager dials the JobManager's RPC port (6123) at boot and *stays connected*,
carrying heartbeats and slot offers/task deployments both ways for the cluster's
whole lifetime. `with_task_manager()` puts both containers on one
internally-created `Network`, aliases the JobManager as `jobmanager`, and sets
`FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager` on **both** containers — not
just the TaskManager. Verified directly: setting it on the TaskManager alone leaves
the JobManager's own Pekko actor system bound under its container hostname rather
than the alias, so every registration attempt from the TaskManager gets silently
dropped as a non-local recipient. The JobManager must be told its own address is
the alias too.

## Backend support — full on docker, JobManager-only on msb

`with_task_manager()` returns `Ok(Self)` on docker and is verified end-to-end there:
the TaskManager registers with the JobManager (`Successful registration at resource
manager ...` in its own log) and `GET /taskmanagers` on the JobManager's REST port
shows one slot-bearing TM within seconds of both containers starting.

On microsandbox, `with_task_manager()` returns
`Err(RightsizeError::UnsupportedByBackend { .. })` before ever booting anything, and
the reason is more basic than a Pekko/tunnel incompatibility: msb's network-link
emulation requires `nc`/busybox *inside the consumer image* to serve the tunnel's
in-guest listener, and the official `flink:1.20.5` image is a bare JRE + Flink
install with neither — the msb backend's own `nc` prerequisite probe fails
immediately, before a single byte of Pekko traffic could be exchanged. Whether
Pekko's persistent-connection RPC registration would work over the tunnel's
single-connection-at-a-time model was never reached or tested — the missing
`nc`/busybox prerequisite stops the attempt before that question is even in play.

A bare JobManager works fine on both backends — it needs no network-link emulation
at all, just the ordinary published-port HTTP path against `/overview` — so this
module supports msb for JobManager-only use; only `with_task_manager()` is gated to
docker.

## Memory — JVM, the ladder applies to both roles

A JobManager settles around ~310 MiB RSS and a TaskManager around ~375 MiB RSS at
rest on docker with no cap (`docker stats`, real boot) — both comfortably over
msb's ~450 MB default *individually*, and this module runs the JobManager on msb
too (see above), so `with_memory_limit(1024)` is this module's default for both
roles, matching the family's established single-JVM floor
([`KeycloakContainer`](./keycloak.md), [`Neo4jContainer`](./neo4j.md)).

## Complete example

A bare JobManager, which works on both backends:

```rust,ignore
use rightsize_modules::FlinkContainer;

#[tokio::test]
async fn bare_jobmanager_answers_rest_overview() -> Result<(), Box<dyn std::error::Error>> {
    let guard = FlinkContainer::new().start().await?;

    let mut resp = ureq::Agent::new_with_defaults()
        .get(format!("{}/overview", guard.rest_url()))
        .call()?;
    let body = resp.body_mut().read_to_string()?;
    assert!(body.contains("\"taskmanagers\""));

    guard.stop().await?;
    Ok(())
}
```

A full session cluster (docker only):

```rust,ignore
use std::time::Duration;
use rightsize_modules::FlinkContainer;

#[tokio::test]
async fn with_task_manager_registers_a_slot_bearing_taskmanager() -> Result<(), Box<dyn std::error::Error>> {
    let guard = FlinkContainer::new().with_task_manager()?.start().await?;

    let agent = ureq::Agent::new_with_defaults();
    let taskmanagers_url = format!("{}/taskmanagers", guard.rest_url());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut body = String::new();
    while std::time::Instant::now() < deadline {
        if let Ok(mut resp) = agent.get(&taskmanagers_url).call() {
            if resp.status().is_success() {
                body = resp.body_mut().read_to_string()?;
                if body.contains("\"id\"") {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(body.contains("\"id\""));

    guard.stop().await?;
    Ok(())
}
```

On microsandbox, `with_task_manager()` fails fast instead:

```rust,ignore
use rightsize::RightsizeError;
use rightsize_modules::FlinkContainer;

fn with_task_manager_is_rejected_on_microsandbox() {
    let err = match FlinkContainer::new().with_task_manager() {
        Ok(_) => panic!("with_task_manager() must be rejected on microsandbox"),
        Err(e) => e,
    };
    assert!(matches!(err, RightsizeError::UnsupportedByBackend { .. }));
}
```

## Backend notes

`with_memory_limit(1024)` is set unconditionally by the module for both the
JobManager and any TaskManager — see Memory above. `with_task_manager()` is
docker-only; run `RIGHTSIZE_BACKEND=docker` for session-cluster tests.
