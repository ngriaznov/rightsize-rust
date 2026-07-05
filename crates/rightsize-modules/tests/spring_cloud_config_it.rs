//! `sandbox-it` integration test for the SpringCloudConfig module. The interesting
//! claim here isn't just "it boots" — it's that `with_memory_limit(1024)` is required
//! on msb (measured evidence: timed out at 180s without the limit, ~19s with it,
//! because Paketo's memory calculator sizes this JVM image's fixed regions above
//! microsandbox's default microVM RAM).
//!
//! `SPRING_PROFILES_ACTIVE=native` is set here, test-side — not by the module —
//! because the image's default git-backed environment repository refuses to start
//! with no git URI configured (`BeanCreationException: ... You need to configure a
//! uri for the git repository`), unrelated to memory; the `native` profile switches
//! to a classpath/filesystem-backed repository that needs no external git remote.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test spring_cloud_config_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test spring_cloud_config_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use std::time::Instant;

use rightsize_modules::SpringCloudConfigContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn boots_and_serves_actuator_health() {
    require_backend!();
    let started = Instant::now();
    // "native" profile serves config from the classpath so no external git repo is
    // required; without it the default git EnvironmentRepository refuses to start
    // ("configure a uri...") — set test-side, matching SpringCloudConfigModuleIT.kt.
    let guard = SpringCloudConfigContainer::new()
        .with_env("SPRING_PROFILES_ACTIVE", "native")
        .start()
        .await
        .expect("spring-cloud-config must start — this is also the msb memory-limit proof");
    eprintln!("spring-cloud-config ready after {:?}", started.elapsed());

    let body = support::http_agent()
        .get(format!("{}/actuator/health", guard.uri()))
        .call()
        .expect("GET /actuator/health must succeed")
        .body_mut()
        .read_to_string()
        .unwrap();
    assert!(body.contains("UP"), "unexpected health body: {body}");

    guard.stop().await.unwrap();
}
