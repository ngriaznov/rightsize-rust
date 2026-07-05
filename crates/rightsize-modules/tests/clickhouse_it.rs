//! `sandbox-it` integration test for the ClickHouse module: an HTTP query
//! round-trip (`CREATE TABLE`, `INSERT`, `SELECT`) over the HTTP interface, basic-auth'd
//! with the module's configured user/password — no client dependency needed, matching
//! the module's HTTP-first house convention.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test clickhouse_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test clickhouse_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::ClickHouseContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn create_insert_select_round_trips_over_http() {
    require_backend!();
    let guard = ClickHouseContainer::new()
        .start()
        .await
        .expect("clickhouse must start");

    let agent = support::http_agent();
    let auth = support::basic_auth_header(guard.username(), guard.password());
    let query = |sql: &'static str| {
        let url = guard.http_url();
        let auth = auth.clone();
        agent.post(url).header("Authorization", &auth).send(sql)
    };

    let create =
        query("CREATE TABLE t (x Int32) ENGINE=Memory").expect("CREATE TABLE must succeed");
    assert!(
        create.status().is_success(),
        "CREATE TABLE failed: {}",
        create.status()
    );

    let insert = query("INSERT INTO t VALUES (1)").expect("INSERT must succeed");
    assert!(
        insert.status().is_success(),
        "INSERT failed: {}",
        insert.status()
    );

    let mut select = query("SELECT x FROM t").expect("SELECT must succeed");
    assert!(
        select.status().is_success(),
        "SELECT failed: {}",
        select.status()
    );
    let body = select.body_mut().read_to_string().unwrap();
    assert_eq!(body.trim(), "1");

    guard.stop().await.unwrap();
}
