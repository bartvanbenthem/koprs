// src/tests/mod.rs
//! Tests for koprs-external.
mod error;
mod http;
mod watcher;

#[cfg(feature = "object-store")]
mod store;
