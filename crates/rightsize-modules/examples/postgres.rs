//! PostgreSQL round-trip — RAII guard plus a real client.
//!
//! Boots a real PostgreSQL container, connects with `tokio-postgres` (the same client
//! this crate's own `postgres_it` integration test uses), and does one
//! `CREATE TABLE` + `INSERT` + `SELECT` round-trip against `connection_string()`.
//!
//! Run it with:
//!
//! ```sh
//! cargo run -p rightsize-modules --example postgres
//! ```
//!
//! Force a specific backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo run -p rightsize-modules --example postgres
//! RIGHTSIZE_BACKEND=docker       cargo run -p rightsize-modules --example postgres
//! ```

use rightsize_modules::PostgresContainer;

#[tokio::main]
async fn main() {
    let guard = PostgresContainer::new()
        .start()
        .await
        .expect("postgres must start");
    println!("Postgres is up at {}", guard.connection_string());

    // tokio-postgres wants the "postgresql://" scheme; connection_string() uses the
    // shorter "postgres://" alias for the same thing.
    let conn_str = guard
        .connection_string()
        .replacen("postgres://", "postgresql://", 1);
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("must connect with tokio-postgres");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection task ended: {e}");
        }
    });

    client
        .execute("CREATE TABLE greeting (message TEXT NOT NULL)", &[])
        .await
        .expect("CREATE TABLE must succeed");
    client
        .execute(
            "INSERT INTO greeting (message) VALUES ($1)",
            &[&"hello from rightsize"],
        )
        .await
        .expect("INSERT must succeed");
    let rows = client
        .query("SELECT message FROM greeting", &[])
        .await
        .expect("SELECT must succeed");
    let message: &str = rows[0].get(0);
    println!("SELECT message -> {message}");
    assert_eq!(message, "hello from rightsize");

    guard.stop().await.expect("postgres must stop cleanly");
}
