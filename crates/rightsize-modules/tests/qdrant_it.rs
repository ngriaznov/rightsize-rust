//! `sandbox-it` integration test for the Qdrant module: proves the REST API
//! round-trips real vector search, not merely answers `/readyz` — create a
//! collection, upsert a single point, then search against it and assert the
//! returned score.
//!
//! The collection uses `Dot` distance and a unit basis vector for both the upserted
//! point and the query, so the expected score is exactly the dot product of two
//! identical unit vectors — `1.0` — with no floating-point ambiguity to hedge
//! around in the assertion.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test qdrant_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test qdrant_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::QdrantContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn create_collection_upsert_a_point_then_search_returns_its_score() {
    require_backend!();
    let guard = QdrantContainer::new()
        .start()
        .await
        .expect("qdrant must start");

    let agent = support::http_agent();
    let rest_url = guard.rest_url();

    let create = agent
        .put(format!("{rest_url}/collections/smoke"))
        .header("Content-Type", "application/json")
        .send(r#"{"vectors":{"size":4,"distance":"Dot"}}"#)
        .expect("PUT /collections/smoke must succeed");
    assert!(
        create.status().is_success(),
        "collection create failed: {}",
        create.status()
    );

    let upsert = agent
        .put(format!("{rest_url}/collections/smoke/points?wait=true"))
        .header("Content-Type", "application/json")
        .send(r#"{"points":[{"id":1,"vector":[1.0,0.0,0.0,0.0],"payload":{"city":"test"}}]}"#)
        .expect("PUT /collections/smoke/points must succeed");
    assert!(
        upsert.status().is_success(),
        "point upsert failed: {}",
        upsert.status()
    );

    let mut search = agent
        .post(format!("{rest_url}/collections/smoke/points/search"))
        .header("Content-Type", "application/json")
        .send(r#"{"vector":[1.0,0.0,0.0,0.0],"limit":1}"#)
        .expect("POST /collections/smoke/points/search must succeed");
    assert!(search.status().is_success());
    let body = search
        .body_mut()
        .read_to_string()
        .expect("search response body must be readable");

    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("search response must be valid JSON");
    let hits = parsed["result"]
        .as_array()
        .expect("search response must have a 'result' array");
    assert_eq!(hits.len(), 1, "expected exactly one hit, raw body: {body}");
    assert_eq!(
        hits[0]["id"].as_i64(),
        Some(1),
        "unexpected hit id, raw body: {body}"
    );
    let score = hits[0]["score"]
        .as_f64()
        .unwrap_or_else(|| panic!("hit had no numeric score, raw body: {body}"));
    // Dot product of two identical unit vectors — exactly 1.0, not merely close to
    // it — but a float equality assertion still deserves an explicit epsilon rather
    // than `==`.
    assert!(
        (score - 1.0).abs() < 1e-6,
        "expected a score of 1.0, got {score}, raw body: {body}"
    );

    guard.stop().await.unwrap();
}
