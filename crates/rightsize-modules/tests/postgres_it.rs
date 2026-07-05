//! `sandbox-it` integration test for the PostgreSQL module: a real
//! `tokio-postgres` client round-trip (connect, `CREATE TABLE`, `INSERT`, `SELECT`
//! assert), proving `connection_string()` is genuinely usable and the `times=2`
//! readiness wait didn't return before the durable server was actually listening.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test postgres_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test postgres_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::PostgresContainer;

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
    let guard = PostgresContainer::new()
        .start()
        .await
        .expect("postgres must start");

    let conn_str = guard
        .connection_string()
        .replacen("postgres://", "postgresql://", 1);
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres must connect using connection_string()");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection task ended: {e}");
        }
    });

    client
        .execute("CREATE TABLE smoke (id INT PRIMARY KEY, note TEXT)", &[])
        .await
        .expect("CREATE TABLE must succeed");
    client
        .execute(
            "INSERT INTO smoke (id, note) VALUES ($1, $2)",
            &[&1i32, &"hello-rightsize"],
        )
        .await
        .expect("INSERT must succeed");
    let rows = client
        .query("SELECT note FROM smoke WHERE id = $1", &[&1i32])
        .await
        .expect("SELECT must succeed");
    assert_eq!(rows.len(), 1);
    let note: &str = rows[0].get(0);
    assert_eq!(note, "hello-rightsize");

    guard.stop().await.unwrap();
}
