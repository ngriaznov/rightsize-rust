#![warn(missing_docs)]
#![forbid(unsafe_code)]

//! `rightsize-msb` drives [microsandbox](https://github.com/superradcompany/microsandbox)
//! (`msb`) as attached child processes — no Docker daemon required. It provisions the
//! pinned `msb`/`libkrunfw` toolchain from GitHub releases (download, SHA-256 verify,
//! cross-process file lock) and emulates container networking with `/etc/hosts`
//! aliases plus a raw TCP-over-`exec --stream` byte pump, since `exec --stream` is the
//! only guest data path microsandbox exposes.

pub mod backend;
pub mod commands;
mod exec_tunnel;
mod ls_json;
pub mod platform;
pub mod provider;
pub mod provisioner;
mod watchdog;

pub use backend::MsbCliBackend;
pub use platform::Platform;
pub use provider::MsbBackendProvider;
