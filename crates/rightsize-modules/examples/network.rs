//! Two-container Network example — a consumer reaching a stub server by alias.
//!
//! Shows how to wire two containers onto the same `Network` so one can address the
//! other by name, on either backend. A WireMock stub server joins the network under
//! the alias "config-stub"; a second, plain Alpine container reaches it as
//! `config-stub:8080` (the alias, not a mapped host port) via `exec`, exactly as it
//! would reach a sibling service in production.
//!
//! Uses the bare `Container` builder rather than `WireMockContainer` for both sides:
//! `with_network`/`with_network_aliases` are core builder methods, and this keeps the
//! network wiring in one place instead of split across a module wrapper and this file.
//!
//! Run it with:
//!
//! ```sh
//! cargo run -p rightsize-modules --example network
//! ```
//!
//! Force a specific backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=microsandbox cargo run -p rightsize-modules --example network
//! RIGHTSIZE_BACKEND=docker       cargo run -p rightsize-modules --example network
//! ```

use std::sync::Arc;
use std::time::Duration;

use rightsize::backends;
use rightsize::{Container, Network, Wait};
use rightsize_docker::DockerBackendProvider;
use rightsize_msb::MsbBackendProvider;

#[tokio::main]
async fn main() {
    // Registers both backend providers so RIGHTSIZE_BACKEND (or, if unset, whichever one
    // is actually usable on this host) picks the one that resolves.
    backends::register_provider(Box::new(MsbBackendProvider));
    backends::register_provider(Box::new(DockerBackendProvider));

    let net = Arc::new(Network::new_network());

    let server = Container::new("wiremock/wiremock:3.13.2")
        .with_exposed_ports(&[8080])
        .with_network(&net)
        .with_network_aliases(&["config-stub"])
        // A generous timeout: the default 60s can be tight on a cold cache, where this
        // image (~120 MB) still needs to be pulled before the container can boot at all.
        .waiting_for(
            Wait::for_http("/__admin/health")
                .for_port(8080)
                .with_startup_timeout(Duration::from_secs(180)),
        );
    let server_guard = server.start().await.expect("server must start");

    // Register a /config stub via WireMock's admin API, reached here over its mapped
    // host port — the consumer container below instead reaches it over the network alias.
    let mapping = r#"{"request":{"method":"GET","urlPath":"/config"},
        "response":{"status":200,"body":"feature-flags: []"}}"#;
    let admin_url = format!(
        "http://{}:{}/__admin/mappings",
        server_guard.host(),
        server_guard.get_mapped_port(8080).unwrap()
    );
    let response = ureq::post(&admin_url)
        .header("Content-Type", "application/json")
        .send(mapping.as_bytes())
        .expect("must register WireMock stub");
    assert_eq!(
        response.status().as_u16(),
        201,
        "unexpected WireMock admin status"
    );

    // The consumer never learns the mapped host port; it dials the alias rightsize
    // installed on the network.
    let consumer = Container::new("alpine:3.19")
        .with_network(&net)
        .with_command(&["sleep", "300"])
        .waiting_for(Wait::for_log_message(".*", 0));
    let consumer_guard = consumer.start().await.expect("consumer must start");

    let target = net
        .resolve("config-stub", 8080)
        .expect("alias must resolve");
    let result = consumer_guard
        .exec(&[
            "wget",
            "-qO-",
            "-T",
            "10",
            &format!("http://{target}/config"),
        ])
        .await
        .expect("exec must run");
    assert_eq!(result.exit_code, 0, "wget failed: {}", result.stderr);
    println!(
        "Consumer fetched config via alias 'config-stub' -> {}",
        result.stdout.trim()
    );
    assert!(result.stdout.contains("feature-flags"));

    consumer_guard
        .stop()
        .await
        .expect("consumer must stop cleanly");
    server_guard.stop().await.expect("server must stop cleanly");
}
