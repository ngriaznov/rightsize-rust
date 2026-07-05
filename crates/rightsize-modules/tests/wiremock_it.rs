//! `sandbox-it` integration test for the WireMock module: a stub-mapping
//! round-trip over the `/__admin` REST API — POST a stub, GET the stubbed path, and
//! assert the stubbed body comes back.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test wiremock_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test wiremock_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::WireMockContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

const STUB: &str = r#"{"request":{"method":"GET","urlPath":"/hello"},
                        "response":{"status":200,"body":"world"}}"#;

#[tokio::test]
async fn stub_mapping_round_trips() {
    require_backend!();
    let guard = WireMockContainer::new()
        .start()
        .await
        .expect("wiremock must start");

    let agent = support::http_agent();
    let post = agent
        .post(format!("{}/mappings", guard.admin_url()))
        .header("Content-Type", "application/json")
        .send(STUB)
        .expect("POST /mappings must succeed");
    assert!(
        post.status().is_success(),
        "stub creation failed: {}",
        post.status()
    );

    let mut get = agent
        .get(format!("{}/hello", guard.base_url()))
        .call()
        .expect("GET /hello must succeed");
    assert!(get.status().is_success());
    let body = get.body_mut().read_to_string().unwrap();
    assert_eq!(body, "world");

    guard.stop().await.unwrap();
}
