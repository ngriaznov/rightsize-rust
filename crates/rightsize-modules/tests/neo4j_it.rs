//! `sandbox-it` integration test for the Neo4j module: a real HTTP Cypher
//! transaction round-trip (`CREATE` then `MATCH`) over `/db/neo4j/tx/commit` — no bolt
//! driver dependency, matching the module's HTTP-first house convention.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test neo4j_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test neo4j_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::Neo4jContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn create_then_match_round_trips_over_the_http_cypher_transaction_endpoint() {
    require_backend!();
    let guard = Neo4jContainer::new()
        .start()
        .await
        .expect("neo4j must start");

    let agent = support::http_agent();
    let auth = support::basic_auth_header(guard.username(), guard.password());
    let commit_url = format!("{}/db/neo4j/tx/commit", guard.http_url());
    let commit = |statement: &str| {
        let body = format!(r#"{{"statements":[{{"statement":"{statement}"}}]}}"#);
        agent
            .post(&commit_url)
            .header("Content-Type", "application/json")
            .header("Authorization", &auth)
            .send(body)
    };

    let mut create = commit("CREATE (n:Test {name: 'hello'}) RETURN n.name AS name")
        .expect("CREATE must succeed");
    assert!(create.status().is_success());
    let create_body = create.body_mut().read_to_string().unwrap();
    assert!(
        create_body.contains("\"errors\":[]"),
        "CREATE reported errors: {create_body}"
    );

    let mut matched =
        commit("MATCH (n:Test {name: 'hello'}) RETURN n.name AS name").expect("MATCH must succeed");
    assert!(matched.status().is_success());
    let match_body = matched.body_mut().read_to_string().unwrap();
    assert!(
        match_body.contains("\"hello\""),
        "MATCH did not return the created node: {match_body}"
    );

    guard.stop().await.unwrap();
}
