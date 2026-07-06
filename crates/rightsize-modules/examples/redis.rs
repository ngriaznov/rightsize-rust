//! Redis quickstart — the plain API, no test framework.
//!
//! Shows the smallest possible rightsize round-trip: boot a real Redis container as a
//! microVM (or Docker container, depending on backend), talk RESP directly over a plain
//! `TcpStream` (no Redis client crate — a PING/PONG round-trip is all this needs), then
//! stop it via the RAII guard's `stop()`.
//!
//! Run it with:
//!
//! ```sh
//! cargo run -p rightsize-modules --example redis
//! ```
//!
//! Force a specific backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo run -p rightsize-modules --example redis
//! RIGHTSIZE_BACKEND=docker       cargo run -p rightsize-modules --example redis
//! ```

use rightsize_modules::RedisContainer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let guard = RedisContainer::new()
        .start()
        .await
        .expect("redis must start");
    println!("Redis is up at {}", guard.uri());

    // guard.uri() is "redis://host:port" — split it back into host/port for a bare
    // TcpStream connect, since this example deliberately speaks RESP by hand.
    let addr = guard.uri().replacen("redis://", "", 1);
    let mut stream = TcpStream::connect(&addr)
        .await
        .expect("must connect to redis");

    // RESP: PING as a multi-bulk array of one bulk string.
    stream
        .write_all(b"*1\r\n$4\r\nPING\r\n")
        .await
        .expect("write PING");

    let mut buf = [0u8; 64];
    let n = stream.read(&mut buf).await.expect("read PONG");
    let reply = String::from_utf8_lossy(&buf[..n]);
    println!("PING -> {}", reply.trim_end());
    assert_eq!(reply.trim_end(), "+PONG", "unexpected RESP reply");

    guard.stop().await.expect("redis must stop cleanly");
}
