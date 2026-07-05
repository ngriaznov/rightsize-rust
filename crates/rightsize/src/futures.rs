//! A tiny std-only boxed-future alias, used where a closure needs to return an owned
//! future without pulling in the `futures` crate for one type alias.

use std::future::Future;
use std::pin::Pin;

/// An owned, pinned, `Send` future — the shape a `Box<dyn Fn(...) -> BoxFuture<...>>`
/// closure needs to return, e.g. the container builder's `with_post_start` hook.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
