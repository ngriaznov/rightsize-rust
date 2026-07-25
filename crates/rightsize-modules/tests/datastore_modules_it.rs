//! `sandbox-it` integration tests for the simple datastore modules — Redis, Valkey,
//! Memcached, Arango, and MongoDB: each module boots for real and gets a
//! protocol-level smoke check — not a full client-library round-trip, since none of
//! these five need more than "the server speaks its protocol" to prove the module's
//! wiring (image, ports, env, wait strategy, and — for Mongo — the replica-set
//! post-start hook) is correct.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test datastore_modules_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test datastore_modules_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use rightsize_modules::{
    ArangoContainer, MemcachedContainer, MongoDbContainer, RedisContainer, ValkeyContainer,
};

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

/// A short blocking TCP round-trip: connect, optionally write `send`, read whatever
/// comes back within a short deadline. Good enough for a protocol smoke check without
/// pulling in an async client library this crate has no other reason to depend on.
fn tcp_roundtrip(addr: &str, send: Option<&[u8]>) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect to the mapped port");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    if let Some(bytes) = send {
        stream.write_all(bytes).expect("write to the socket");
    }
    let mut buf = [0u8; 512];
    let n = stream.read(&mut buf).unwrap_or(0);
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[tokio::test]
async fn redis_boots_and_speaks_the_redis_protocol() {
    require_backend!();
    let guard = RedisContainer::new()
        .start()
        .await
        .expect("redis must start");

    let addr = guard.uri().replacen("redis://", "", 1);
    let reply = tcp_roundtrip(&addr, Some(b"PING\r\n"));
    assert!(reply.contains("PONG"), "unexpected reply: {reply:?}");

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn valkey_boots_and_speaks_the_redis_protocol() {
    require_backend!();
    // Valkey is a protocol-compatible Redis fork — the same raw RESP round-trip the
    // Redis module gets above proves this module's wiring (image, port, wait
    // strategy) the same way.
    let guard = ValkeyContainer::new()
        .start()
        .await
        .expect("valkey must start");

    let addr = guard.uri().replacen("redis://", "", 1);
    let reply = tcp_roundtrip(&addr, Some(b"PING\r\n"));
    assert!(reply.contains("PONG"), "unexpected reply: {reply:?}");

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn memcached_boots_and_the_module_itself_already_proved_the_version_probe() {
    require_backend!();
    // MemcachedContainer's own wait strategy already IS the protocol smoke check —
    // start() would not have returned Ok without a real `VERSION` reply. This test
    // additionally proves the module's address() helper points at a live peer.
    let guard = MemcachedContainer::new()
        .start()
        .await
        .expect("memcached must start");

    let reply = tcp_roundtrip(&guard.address(), Some(b"version\r\n"));
    assert!(reply.starts_with("VERSION"), "unexpected reply: {reply:?}");

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn arango_boots_and_serves_the_version_endpoint() {
    require_backend!();
    let guard = ArangoContainer::new()
        .start()
        .await
        .expect("arango must start");

    let body = support::http_agent()
        .get(format!("{}/_api/version", guard.endpoint()))
        .call()
        .expect("GET /_api/version must succeed")
        .body_mut()
        .read_to_string()
        .unwrap();
    assert!(body.contains("version"), "unexpected body: {body}");

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn mongodb_boots_as_a_writable_primary_and_accepts_a_document() {
    require_backend!();
    let guard = MongoDbContainer::new()
        .start()
        .await
        .expect("mongo must start (post_start already awaited a writable primary)");

    // A minimal protocol-level proof beyond the post_start hook's own polling: run a
    // real write through mongosh and read it back, over exec (no mongodb client-crate
    // dependency needed for a one-shot smoke check).
    let insert = guard
        .exec(&[
            "mongosh",
            "--quiet",
            "--eval",
            "db.smoke.insertOne({ok: 1})",
        ])
        .await
        .expect("exec must run");
    assert_eq!(insert.exit_code, 0, "insert failed: {}", insert.stderr);

    let count = guard
        .exec(&[
            "mongosh",
            "--quiet",
            "--eval",
            "db.smoke.countDocuments({ok: 1})",
        ])
        .await
        .expect("exec must run");
    assert_eq!(count.exit_code, 0, "count failed: {}", count.stderr);
    assert!(
        count.stdout.trim().ends_with('1'),
        "unexpected count output: {}",
        count.stdout
    );

    guard.stop().await.unwrap();
}
