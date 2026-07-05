//! `sandbox-it` integration test for the Keycloak module: a real OIDC
//! discovery-document fetch asserting the `issuer` field matches this container's own
//! auth server URL.
//!
//! Run for real, once per backend:
//!
//! ```sh
//! RIGHTSIZE_BACKEND=docker cargo test -p rightsize-modules --features sandbox-it --test keycloak_it
//! RIGHTSIZE_BACKEND=microsandbox cargo test -p rightsize-modules --features sandbox-it --test keycloak_it
//! ```

#![cfg(feature = "sandbox-it")]

#[allow(dead_code)]
mod support;

use rightsize_modules::KeycloakContainer;

macro_rules! require_backend {
    () => {
        if !support::requested_backend_available() {
            support::skip_notice();
            return;
        }
    };
}

#[tokio::test]
async fn oidc_discovery_document_asserts_the_issuer() {
    require_backend!();
    let guard = KeycloakContainer::new()
        .start()
        .await
        .expect("keycloak must start");

    let mut resp = support::http_agent()
        .get(format!(
            "{}/realms/master/.well-known/openid-configuration",
            guard.auth_server_url()
        ))
        .call()
        .expect("discovery document fetch must succeed");
    assert!(resp.status().is_success());
    let body = resp.body_mut().read_to_string().unwrap();
    let expected = format!("\"issuer\":\"{}/realms/master\"", guard.auth_server_url());
    assert!(
        body.contains(&expected),
        "discovery document did not carry the expected issuer: {body}"
    );

    guard.stop().await.unwrap();
}
