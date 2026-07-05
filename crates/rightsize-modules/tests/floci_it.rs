//! `sandbox-it` integration tests for the Floci module: the AWS variant's
//! S3-shaped REST surface (create-bucket, put-object, get-object, no request signing),
//! plus a plain `/health` check for the Azure and GCP variants.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test floci_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test floci_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::FlociContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn aws_variant_s3_create_bucket_put_and_get_round_trips_with_no_request_signing() {
    require_backend!();
    let guard = FlociContainer::aws()
        .start()
        .await
        .expect("floci (aws) must start");

    let agent = support::http_agent();
    let endpoint = guard.endpoint_url();

    let put_bucket = agent
        .put(format!("{endpoint}/rightsize-test-bucket"))
        .send_empty()
        .expect("create-bucket must succeed");
    assert!(
        put_bucket.status().is_success(),
        "create-bucket failed: {}",
        put_bucket.status()
    );

    let put_obj = agent
        .put(format!("{endpoint}/rightsize-test-bucket/hello.txt"))
        .send("hello world")
        .expect("put-object must succeed");
    assert!(
        put_obj.status().is_success(),
        "put-object failed: {}",
        put_obj.status()
    );

    let mut get_obj = agent
        .get(format!("{endpoint}/rightsize-test-bucket/hello.txt"))
        .call()
        .expect("get-object must succeed");
    assert!(
        get_obj.status().is_success(),
        "get-object failed: {}",
        get_obj.status()
    );
    let body = get_obj.body_mut().read_to_string().unwrap();
    assert_eq!(body, "hello world");

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn azure_variant_answers_health() {
    require_backend!();
    let guard = FlociContainer::azure()
        .start()
        .await
        .expect("floci (azure) must start");

    let mut resp = support::http_agent()
        .get(format!("{}/health", guard.endpoint_url()))
        .call()
        .expect("health check must succeed");
    assert!(resp.status().is_success());
    let body = resp.body_mut().read_to_string().unwrap();
    assert!(
        body.contains("\"status\":\"UP\""),
        "unexpected health body: {body}"
    );

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn gcp_variant_answers_health() {
    require_backend!();
    let guard = FlociContainer::gcp()
        .start()
        .await
        .expect("floci (gcp) must start");

    let mut resp = support::http_agent()
        .get(format!("{}/health", guard.endpoint_url()))
        .call()
        .expect("health check must succeed");
    assert!(resp.status().is_success());
    let body = resp.body_mut().read_to_string().unwrap();
    assert!(
        body.contains("\"services\""),
        "unexpected health body: {body}"
    );

    guard.stop().await.unwrap();
}
