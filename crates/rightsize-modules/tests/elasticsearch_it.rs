//! `sandbox-it` integration test for the Elasticsearch module: proves the REST API
//! round-trips a real document, not merely answers `GET /` — index one with
//! `?refresh=true` (so it's searchable immediately, no indexing-delay race), then
//! search for it and assert the hit comes back.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test elasticsearch_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test elasticsearch_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::ElasticsearchContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn index_with_refresh_then_search_returns_the_indexed_document() {
    require_backend!();
    let guard = ElasticsearchContainer::with_image("elasticsearch:9.4.4")
        .start()
        .await
        .expect("elasticsearch must start");

    let agent = support::http_agent();
    let rest_url = guard.rest_url();

    // `?refresh=true` makes the write visible to search immediately — without it,
    // Elasticsearch's near-real-time indexing could leave the very next search a
    // beat behind the write, an avoidable race for a smoke test that only cares
    // about the round trip, not indexing latency.
    let index = agent
        .put(format!("{rest_url}/books/_doc/1?refresh=true"))
        .header("Content-Type", "application/json")
        .send(r#"{"title":"Snow Crash","author":"Neal Stephenson"}"#)
        .expect("PUT /books/_doc/1?refresh=true must succeed");
    assert!(
        index.status().is_success(),
        "index failed: {}",
        index.status()
    );

    let mut search = agent
        .get(format!("{rest_url}/books/_search?q=title:Snow"))
        .call()
        .expect("GET /books/_search must succeed");
    assert!(search.status().is_success());
    let body = search
        .body_mut()
        .read_to_string()
        .expect("search response body must be readable");
    assert!(
        body.contains("Snow Crash") && body.contains("Neal Stephenson"),
        "search response did not contain the indexed document, raw body: {body}"
    );

    guard.stop().await.unwrap();
}
