//! `sandbox-it` integration test for the Cassandra module: a `cqlsh` round-trip
//! (`CREATE KEYSPACE` → `CREATE TABLE` → `INSERT` → `SELECT`) via the image's bundled
//! `cqlsh` binary — no Cassandra driver dependency needed for a smoke check, matching
//! the module's exec-based house convention for a bundled-client protocol proof.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test cassandra_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test cassandra_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::CassandraContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn create_keyspace_table_insert_select_round_trips_via_cqlsh() {
    require_backend!();
    let guard = CassandraContainer::new()
        .start()
        .await
        .expect("cassandra must start (this also proves the GPG_KEYS override let it boot)");

    let cql = "CREATE KEYSPACE IF NOT EXISTS smoke WITH REPLICATION = \
               {'class': 'SimpleStrategy', 'replication_factor': 1}; \
               CREATE TABLE IF NOT EXISTS smoke.t (id int PRIMARY KEY, val text); \
               INSERT INTO smoke.t (id, val) VALUES (1, 'rightsize');";
    let setup = guard
        .exec(&["cqlsh", "-e", cql])
        .await
        .expect("exec must run");
    assert_eq!(
        setup.exit_code, 0,
        "keyspace/table/insert failed: stdout={} stderr={}",
        setup.stdout, setup.stderr
    );

    let select = guard
        .exec(&["cqlsh", "-e", "SELECT val FROM smoke.t WHERE id = 1;"])
        .await
        .expect("exec must run");
    assert_eq!(
        select.exit_code, 0,
        "select failed: stdout={} stderr={}",
        select.stdout, select.stderr
    );
    assert!(
        select.stdout.contains("rightsize"),
        "unexpected select output: {}",
        select.stdout
    );

    guard.stop().await.unwrap();
}
