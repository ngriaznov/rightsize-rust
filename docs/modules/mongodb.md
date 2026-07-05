# MongoDbContainer

A single-node MongoDB container started as a **one-member replica set** (named
`docker-rs`) — required for transactions and change streams, which is why this
module doesn't just boot a bare standalone `mongod`.

**Default image:** `mongo:8.0`
**Guest port:** `27017`

| Method | On | Effect |
|---|---|---|
| `MongoDbContainer::new()` | builder | Pinned default image. |
| `MongoDbContainer::with_image(image)` | builder | Caller-chosen image. |
| `.start()` | builder → `Result<MongoDbGuard>` | Boots the container; does not return until the replica set has an elected primary. |
| `.connection_string()` | guard | `mongodb://host:port/test?directConnection=true`. |
| `.replica_set_url()` | guard | Alias for `connection_string()`. |
| `.stop()` | guard | Stops and removes the container, releases its port. |

## The post-start hook

`start()` runs a `Container::with_post_start` hook (see
[Containers & Guards](../core-concepts/containers-and-guards.md#the-raii-lifecycle))
that, once the plain listening-port wait passes:

1. Runs `rs.initiate()` via `mongosh` — checking `rs.status()` first, so a retry
   after a partial initiate doesn't re-initiate a set that's already forming.
2. Polls `db.hello().isWritablePrimary` until it reports `true`.

Both steps retry through the proxy-accepts-before-mongod-listens race (the same
read-probe-relevant timing gap covered in
[Wait Strategies](../core-concepts/wait-strategies.md#the-read-probe-story)): the
listening-port wait can return before `mongod` is far enough along to accept a real
client command, so the first few `mongosh` invocations failing is expected and
retried, not a fatal error. The practical upshot: `connection_string()` is always
immediately usable once `start()` returns `Ok` — no separate "wait for the replica set"
step needed in your own test.

## Complete example

```rust,ignore
use rightsize_modules::MongoDbContainer;

#[tokio::test]
async fn mongo_accepts_a_write() -> rightsize::Result<()> {
    let guard = MongoDbContainer::new().start().await?;

    // start() already awaited a writable primary — connection_string() is usable now.
    let insert = guard
        .exec(&["mongosh", "--quiet", "--eval", "db.smoke.insertOne({ok: 1})"])
        .await?;
    assert_eq!(insert.exit_code, 0);

    let count = guard
        .exec(&["mongosh", "--quiet", "--eval", "db.smoke.countDocuments({ok: 1})"])
        .await?;
    assert!(count.stdout.trim().ends_with('1'));

    guard.stop().await?;
    Ok(())
}
```

Swap the `exec`-based `mongosh` calls above for the `mongodb` driver crate of your
choice against `guard.connection_string()` in your own tests — this crate's own
integration suite uses `exec` specifically to avoid taking on a MongoDB
client-library dev-dependency just for a smoke test.

## Backend notes

No memory-limit override. The replica-set initiation retries absorb the same
proxy-timing gap that affects wait strategies generally on both backends — nothing
MongoDB-specific beyond that.
