//! `sandbox-it` integration test for the MinIO module: proves the S3 API round-trips
//! real bytes through the bundled `mc` binary (no S3 SDK dependency needed for a
//! smoke check — see the module doc), then asserts from the host over plain HTTP that
//! auth is actually enforced, not just configured.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test minio_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test minio_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::MinioContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn mb_cp_cat_round_trips_through_the_bundled_mc_binary() {
    require_backend!();
    let guard = MinioContainer::new()
        .start()
        .await
        .expect("minio must start");

    // A single `sh -c` exec chains alias setup (via MC_HOST_local, mc's own env-based
    // alias mechanism — no `mc alias set` step needed), bucket creation, a write, and a
    // read back — the whole round-trip in one exec call.
    //
    // The write goes through a guest file and `mc cp`, not `mc pipe` over stdin:
    // `mc pipe` reads stdin, and an exec'd `mc pipe` under the microsandbox backend
    // either dumps its goroutines and exits non-zero or hangs outright (both observed
    // directly). `mc cp` needs no stdin and round-trips reliably. /srv, not /tmp — the
    // guest mounts /tmp as a small tmpfs.
    let cmd = format!(
        "export MC_HOST_local=http://{}:{}@127.0.0.1:9000 && \
         mc mb local/smoke >/dev/null && \
         printf 'rightsize' > /srv/object && \
         mc cp /srv/object local/smoke/object >/dev/null && \
         mc cat local/smoke/object",
        guard.root_user(),
        guard.root_password(),
    );
    let result = guard
        .exec(&["sh", "-c", &cmd])
        .await
        .expect("exec must run");
    assert_eq!(
        result.exit_code, 0,
        "mc round-trip failed: stdout={} stderr={}",
        result.stdout, result.stderr
    );
    // Trimmed, like the other module ITs read command output: `mc cat` is the last
    // stage of a chained `sh -c`, and the shell's own line handling is not worth
    // asserting on here — the object's content is.
    assert_eq!(
        result.stdout.trim(),
        "rightsize",
        "unexpected object content, raw stdout: {:?}",
        result.stdout
    );

    guard.stop().await.unwrap();
}

#[tokio::test]
async fn health_live_returns_200_and_anonymous_requests_are_denied() {
    require_backend!();
    let guard = MinioContainer::new()
        .start()
        .await
        .expect("minio must start");

    let health = support::http_agent()
        .get(format!("{}/minio/health/live", guard.s3_url()))
        .call()
        .expect("GET /minio/health/live must succeed");
    assert_eq!(health.status(), 200);

    // No credentials at all — proves the configured root pair is actually gating
    // access rather than merely being accepted and ignored.
    let anon_agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(5)))
        .http_status_as_error(false)
        .build()
        .into();
    let anon = anon_agent
        .get(format!("{}/", guard.s3_url()))
        .call()
        .expect("anonymous GET must at least get a response");
    assert_eq!(
        anon.status(),
        403,
        "anonymous request should be denied, got {}",
        anon.status()
    );

    guard.stop().await.unwrap();
}
