//! `sandbox-it` integration tests for the Flink module: a bare JobManager answers REST
//! `/overview` on both backends; `with_task_manager()` registers a slot-bearing
//! TaskManager on docker (verified via `/taskmanagers`); on microsandbox,
//! `with_task_manager()` returns the typed `UnsupportedByBackend` error up front — see
//! `flink.rs`'s module doc for the full story (the msb backend's own `nc`-probe
//! prerequisite fails against the `flink:1.20.5` image before any Pekko traffic is
//! exchanged).
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test flink_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test flink_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use std::time::Duration;

use rightsize::RightsizeError;
use rightsize_modules::FlinkContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn bare_jobmanager_answers_rest_overview() {
    require_backend!();
    let guard = FlinkContainer::new()
        .start()
        .await
        .expect("flink jobmanager must start");

    let mut resp = support::http_agent()
        .get(format!("{}/overview", guard.rest_url()))
        .call()
        .expect("GET /overview must succeed");
    assert!(resp.status().is_success());
    let body = resp.body_mut().read_to_string().unwrap();
    assert!(
        body.contains("\"taskmanagers\""),
        "unexpected overview body: {body}"
    );

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn with_task_manager_registers_a_slot_bearing_taskmanager_docker_only() {
    require_backend!();
    support::ensure_registered();
    if rightsize::backends::active_name() == "microsandbox" {
        eprintln!(
            "skipping: with_task_manager() is docker-only — see FlinkContainer's module doc \
             for the msb incompatibility"
        );
        return;
    }

    let guard = FlinkContainer::new()
        .with_task_manager()
        .expect("with_task_manager() must succeed on docker")
        .start()
        .await
        .expect("flink cluster must start");

    let agent = support::http_agent();
    let taskmanagers_url = format!("{}/taskmanagers", guard.rest_url());
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let mut body = String::new();
    while std::time::Instant::now() < deadline {
        if let Ok(mut resp) = agent.get(&taskmanagers_url).call() {
            if resp.status().is_success() {
                body = resp.body_mut().read_to_string().unwrap();
                if body.contains("\"id\"") {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(
        body.contains("\"id\""),
        "TaskManager never registered: {body}"
    );

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn with_task_manager_returns_unsupported_by_backend_on_microsandbox() {
    require_backend!();
    support::ensure_registered();
    if rightsize::backends::active_name() != "microsandbox" {
        eprintln!("skipping: this test only asserts the msb-specific guard");
        return;
    }

    let err = match FlinkContainer::new().with_task_manager() {
        Ok(_) => panic!("with_task_manager() must be rejected on microsandbox"),
        Err(e) => e,
    };
    assert!(
        matches!(err, RightsizeError::UnsupportedByBackend { .. }),
        "expected UnsupportedByBackend, got: {err}"
    );
}
