//! `sandbox-it` integration test for the RabbitMQ module: a real round-trip
//! over the management HTTP API (declare a queue, publish a message, get it back) —
//! not an AMQP client library. `lapin` (the natural AMQP client choice) pulls in a
//! `time`-based transitive tree that requires rustc 1.88, above this workspace's MSRV
//! (1.85, `rust-toolchain.toml`) — confirmed directly (`cargo build` against a bare
//! `lapin = "2"` dependency fails with `time@0.3.53 requires rustc 1.88.0` and several
//! `icu_*`/`idna_adapter` crates in the same boat). The management API's own
//! `/api/queues/.../publish` and `/api/queues/.../get` endpoints exercise the same
//! "does the broker actually work" claim with the `ureq` dev-dependency this crate
//! already carries, at zero extra dependency cost — chosen because a real AMQP
//! client dependency here would drag in a rustc-version-incompatible transitive
//! tree (see above) for no proportional benefit.
//!
//! RabbitMQ 4.x deprecates (and, per its own server startup message, no longer
//! permits by default) `transient_nonexcl_queues` — a `durable=false,
//! auto_delete=false` queue declared over the management API risks a
//! `reply-code=541 INTERNAL_ERROR` rejection for clients declaring non-durable,
//! non-exclusive queues; this test declares a durable queue to stay on the accepted
//! side of that policy.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test rabbitmq_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test rabbitmq_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::RabbitMqContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn declare_publish_and_get_round_trips_over_the_management_api() {
    require_backend!();
    let guard = RabbitMqContainer::new()
        .start()
        .await
        .expect("rabbitmq must start");

    let agent = support::http_agent();
    let admin = guard.management_url();
    let auth = support::basic_auth_header(guard.username(), guard.password());

    // Durable, non-exclusive — RabbitMQ 4.x's transient_nonexcl_queues deprecation
    // (see the module doc) can reject the opposite combination.
    let declare = agent
        .put(format!("{admin}/api/queues/%2f/smoke"))
        .header("Content-Type", "application/json")
        .header("Authorization", &auth)
        .send(r#"{"durable":true,"auto_delete":false}"#)
        .expect("PUT queue declare must succeed");
    assert!(
        declare.status().is_success(),
        "queue declare failed: {}",
        declare.status()
    );

    let publish = agent
        .post(format!("{admin}/api/exchanges/%2f/amq.default/publish"))
        .header("Content-Type", "application/json")
        .header("Authorization", &auth)
        .send(
            r#"{"properties":{},"routing_key":"smoke","payload":"hello-rightsize",
                "payload_encoding":"string"}"#,
        )
        .expect("POST publish must succeed");
    assert!(
        publish.status().is_success(),
        "publish failed: {}",
        publish.status()
    );

    let mut get = agent
        .post(format!("{admin}/api/queues/%2f/smoke/get"))
        .header("Content-Type", "application/json")
        .header("Authorization", &auth)
        // Verified directly against a real 4.x boot: the management API's field is
        // `ackmode`, not the `ack_mode` spelling some older docs/examples use — a
        // request with `ack_mode` 400s with `{"error":"bad_request",
        // "reason":"[{key_missing,ackmode}]"}`.
        .send(r#"{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}"#)
        .expect("POST get must succeed");
    assert!(get.status().is_success(), "get failed: {}", get.status());
    let body = get.body_mut().read_to_string().unwrap();
    assert!(
        body.contains("hello-rightsize"),
        "unexpected get body: {body}"
    );

    guard.stop().await.unwrap();
}
