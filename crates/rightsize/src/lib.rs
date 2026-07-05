#![warn(missing_docs)]
#![forbid(unsafe_code)]

//! `rightsize` is the backend-agnostic core of rightsize-rust: a Tokio-async-native,
//! RAII-guard API for booting integration-test containers and tearing them down
//! without leaks, no matter how the boot fails.
//!
//! This crate owns the public surface — the `Container` builder, the `ContainerGuard`
//! it returns, `Network`, wait strategies, the in-process free-port allocator, and the
//! `SandboxBackend` trait that lets `rightsize-msb` and `rightsize-docker` plug in
//! without this crate ever depending on either.

pub mod backend;
pub mod backends;
mod cleanup;
mod container;
pub mod error;
mod free_ports;
mod futures;
pub mod model;
pub mod mountable_file;
pub mod network;
mod run_id;
pub mod wait;

pub use backend::{BackendProvider, FollowHandle, NetworkLink, SandboxBackend, SandboxHandle};
pub use container::{Container, ContainerGuard};
pub use error::{Result, RightsizeError};
pub use futures::BoxFuture;
pub use model::{ContainerSpec, ExecResult, FileMount, PortBinding};
pub use mountable_file::MountableFile;
pub use network::Network;
pub use run_id::RunId;
pub use wait::{Wait, WaitStrategy, WaitTarget};
