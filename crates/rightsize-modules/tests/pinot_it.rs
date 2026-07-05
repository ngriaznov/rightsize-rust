//! `sandbox-it` integration test for the Apache Pinot module: a REST
//! round-trip proving the cluster WORKS, not merely pings — POST a minimal schema to
//! the controller, GET it back and assert, then check `/health` on both the
//! controller and the broker's query path. Data ingestion is out of scope (the module
//! doc explains why) — schema round-trip is the meaningful smoke here.
//!
//! On msb this test is also the de-facto stress test of `with_memory_limit(4096)`
//! and the 180s startup timeout: a four-JVM QuickStart cluster booting cold is
//! legitimately slow. If Pinot-on-msb hits a wall (memory pressure the microVM can't
//! absorb, or an image the msb runtime can't boot at all), this test is expected to
//! be assumption-skipped with the reason recorded, rather than faked.
//!
//! The broker's `/health` becomes reachable slightly *after* the controller's — this
//! module's readiness wait only watches the controller (see the module doc), so a
//! broker request made the instant `start()` returns can race the broker still
//! registering with the cluster (observed directly: an immediate GET occasionally
//! gets `UnexpectedEof`/"Peer disconnected" rather than a clean response) — hence the
//! bounded retry around the broker health check below, not just a single shot.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test pinot_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test pinot_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use std::time::Duration;

use rightsize_modules::PinotContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

const SMOKE_SCHEMA: &str = r#"{
  "schemaName": "smoke",
  "dimensionFieldSpecs": [
    { "name": "id", "dataType": "STRING" }
  ]
}"#;

/// Retries `request` up to `attempts` times (500ms apart), returning the last
/// success or the last error if every attempt failed — the broker's `/health`
/// specifically needs this (see the module doc); the controller and schema
/// round-trip don't, but using the same small helper for both keeps the test
/// uniform.
async fn retry_request(
    attempts: u32,
    mut request: impl FnMut() -> Result<ureq::http::Response<ureq::Body>, ureq::Error>,
) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    let mut last = request();
    for _ in 1..attempts {
        if last.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        last = request();
    }
    last
}

#[tokio::test]
async fn schema_post_get_round_trips_and_both_health_endpoints_are_up() {
    require_backend!();
    let guard = PinotContainer::new()
        .start()
        .await
        .expect("pinot must start — a four-JVM QuickStart cluster, budget the 180s wait");

    let agent = support::http_agent();
    let controller = guard.controller_url();
    let post = agent
        .post(format!("{controller}/schemas"))
        .header("Content-Type", "application/json")
        .send(SMOKE_SCHEMA)
        .expect("POST /schemas must succeed");
    assert!(
        post.status().is_success(),
        "schema POST failed: {}",
        post.status()
    );

    let body = agent
        .get(format!("{controller}/schemas/smoke"))
        .call()
        .expect("GET /schemas/smoke must succeed")
        .body_mut()
        .read_to_string()
        .unwrap();
    assert!(body.contains("smoke"), "unexpected schema body: {body}");

    let controller_health = agent
        .get(format!("{controller}/health"))
        .call()
        .expect("GET /health (controller) must succeed");
    assert!(controller_health.status().is_success());

    // The broker can take well over a minute after the controller's own readiness
    // wait is satisfied to finish registering with the cluster (observed directly:
    // a fresh boot answered 503 "broker_not_available"-style responses for a couple
    // of minutes before flipping to 200) — 120 attempts at 500ms gives it a full
    // 60s beyond whatever the controller wait already spent, without hard-coding a
    // guess at the exact crossover point.
    let broker_url = format!("{}/health", guard.broker_url());
    let broker_health = retry_request(120, || agent.get(&broker_url).call())
        .await
        .expect("GET /health (broker) must eventually succeed");
    assert!(broker_health.status().is_success());

    guard.stop().await.unwrap();
}
