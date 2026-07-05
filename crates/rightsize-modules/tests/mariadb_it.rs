//! `sandbox-it` integration test for the MariaDB module: a real
//! `mysql_async` client round-trip (MariaDB speaks the MySQL wire protocol, so the
//! same client this crate already uses for [`rightsize_modules::MySqlContainer`]
//! connects here too), proving `connection_string()` is usable and the
//! empirically-pinned anchored-log-line readiness wait doesn't fire early on the temp
//! server boot (see `mariadb.rs`'s module doc for the captured log evidence).
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test mariadb_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test mariadb_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use mysql_async::prelude::*;
use rightsize_modules::MariaDbContainer;

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
    let guard = MariaDbContainer::new()
        .start()
        .await
        .expect("mariadb must start");

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
