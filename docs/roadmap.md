# Roadmap

Ideas under consideration for future releases, roughly ordered by expected impact.
Items graduate off this page when they ship; the CHANGELOG records what landed.

## Native microVM memory snapshots

Filesystem-level checkpoint/restore ships on both backends now (see
[Checkpoint / Restore](./checkpoints.md)): docker via image commit, microsandbox
via disk snapshots (stops the sandbox, snapshots its disk, and boots it back from
that snapshot under the same name and ports) — boot once, seed once, restore per
test instead of re-seeding. What's left: true MEMORY
snapshots, which would resume a sandbox mid-execution — a near-instant restore
where a live process's in-memory state (not just its filesystem) survives too.
That still needs upstream microsandbox support.

## Self-contained archives

[Checkpoint export/import](./checkpoints.md#moving-checkpoints-between-machines)
ships now — a checkpoint seeded once in CI can be shipped as a build artifact and
restored on a fresh runner instead of re-seeding there too. What's left: the
archive doesn't bundle the OCI image, so the destination machine still pulls it on
the restored container's first boot — offline restores need the image baked into
the archive itself. Blocked upstream: msb 0.6.6's own `--with-image` export fails
an integrity check on import ("raw manifest digest mismatch"), so this waits on a
fix there.

## Module breadth

The gaps Testcontainers users will hit first: LocalStack, Elasticsearch /
OpenSearch, Vault, MinIO, NATS, Cassandra, MSSQL, Oracle Free, and Ollama
(LLM-in-a-box testing, which also fits the isolation story).

## Framework integrations

One-annotation setup in the frameworks people actually use: Spring Boot
`@ServiceConnection`-style wiring, Quarkus Dev Services, a pytest-style
fixture story, Vitest/Jest global-setup helpers, Axum/sqlx examples.

## Building images from code

Define an ad-hoc image inline in the test (Dockerfile-from-code) instead of
publishing one — for testing your own service, not just its dependencies.

## Host-directory mounts

Start-time host-directory binds (`with_copy_file_to_container` currently mounts
individual files) alongside the existing copy-in. Runtime copy-out already shipped
— see [Copying Files](./copy.md).

## Declarative multi-service groups

A rightsize-native way to declare "these five services, this network, this
startup order" as one artifact, serving the docker-compose need without the
compose file format.

## Warm pools

A background pool of pre-booted sandboxes so `start()` is near-instant —
paired with reuse, this attacks time-to-first-test directly.

## Fault injection

The backend controls the virtual NIC: latency, packet loss, partitions
between sandboxes, kill-and-revive — first-class API instead of a separate
Toxiproxy container.

## Time control

A VM owns its clock: advance time inside the guest to test TTLs, certificate
expiry, and cron logic faithfully — awkward to impossible on a shared
kernel.

## Private registry authentication

Pulling from authenticated registries, documented and tested — table stakes
for enterprise evaluation.
