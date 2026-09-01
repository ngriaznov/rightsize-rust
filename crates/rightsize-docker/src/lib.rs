#![warn(missing_docs)]
#![forbid(unsafe_code)]

//! `rightsize-docker` is a from-scratch Docker daemon client over a unix domain
//! socket (unix) or Docker Desktop's named pipe (Windows) — no `bollard`, no `hyper`,
//! no shared HTTP stack the consumer's dependency tree can accidentally reroute onto
//! TCP. It speaks exactly the daemon endpoints `rightsize` needs (containers, exec,
//! logs, networks) and decodes the daemon's chunked transfer encoding and 8-byte
//! log-frame multiplexing format by hand. See [`stream`] for how the two platform
//! transports are unified behind one internal type.
//!
//! This backend also doubles as the correctness oracle other backends are checked
//! against, since Docker enforces the semantics (read-only mounts, native networks)
//! that microsandbox only emulates.

mod backend;
mod client;
mod frames;
mod json;
mod provider;
mod stream;

pub use backend::DockerBackend;
pub use provider::DockerBackendProvider;
