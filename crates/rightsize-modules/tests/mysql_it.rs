//! `sandbox-it` integration test for the MySQL module: a real
//! `mysql_async` client round-trip (connect, `CREATE TABLE`, `INSERT`, `SELECT`
//! assert), proving `connection_string()` is usable and the empirically-pinned
//! anchored-log-line readiness wait didn't fire early on the temp server / X Plugin
//! boot (see `mysql.rs`'s module doc for the captured log evidence).
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test mysql_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test mysql_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use mysql_async::prelude::*;
use rightsize_modules::MySqlContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn create_insert_select_round_trips_over_a_real_client() {
    require_backend!();
    let guard = MySqlContainer::new()
        .start()
        .await
        .expect("mysql must start");

    let pool = mysql_async::Pool::new(guard.connection_string().as_str());
    let mut conn = pool
        .get_conn()
        .await
        .expect("mysql_async must connect using connection_string()");

    conn.query_drop("CREATE TABLE smoke (id INT PRIMARY KEY, note TEXT)")
        .await
        .expect("CREATE TABLE must succeed");
    conn.exec_drop(
        "INSERT INTO smoke (id, note) VALUES (?, ?)",
        (1i32, "hello-rightsize"),
    )
    .await
    .expect("INSERT must succeed");
    let note: Option<String> = conn
        .exec_first("SELECT note FROM smoke WHERE id = ?", (1i32,))
        .await
        .expect("SELECT must succeed");
    assert_eq!(note.as_deref(), Some("hello-rightsize"));

    drop(conn);
    pool.disconnect().await.ok();
    guard.stop().await.unwrap();
}
