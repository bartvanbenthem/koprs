//! # koprs-external
//!
//! Generic polling watchers for external sources such as HTTP REST APIs and
//! S3 buckets, designed as a companion to `koprs` Kubernetes operators.
//!
//! Kubernetes operators often need to reconcile cluster state with resources
//! that live outside the cluster — a configuration endpoint, an object store,
//! or a remote registry. `koprs-external` provides a lightweight polling
//! abstraction that fits naturally alongside `koprs` controllers.
//!
//! ## Core model
//!
//! The [`watcher::ExternalSource`] trait represents any source that can be
//! polled for changes. Implementations return [`ExternalEvent`] values that
//! distinguish
//! between items being added, modified, or removed. The source tracks its own
//! state between polls so callers do not need to diff results themselves.
//!
//! [`watch_external`] spawns a background task that ticks on a configurable
//! interval and forwards events to an [`tokio::sync::mpsc`] channel, mirroring
//! the pattern used by `koprs::watcher`.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use koprs_external::http::HttpPoller;
//! use koprs_external::watcher::{watch_external, ExternalEvent};
//! use tokio::sync::mpsc;
//!
//! #[tokio::main]
//! async fn main() {
//!     let (tx, mut rx) = mpsc::channel(16);
//!     let poller = HttpPoller::new("https://api.example.com/config")
//!         .with_bearer_token("my-token")
//!         .with_name("config-api");
//!
//!     let _handle = watch_external(poller, Duration::from_secs(30), tx);
//!
//!     while let Some(event) = rx.recv().await {
//!         match event {
//!             ExternalEvent::Added(r)    => println!("config appeared: {} bytes", r.body.len()),
//!             ExternalEvent::Modified(r) => println!("config changed:  {} bytes", r.body.len()),
//!             ExternalEvent::Removed(_)  => println!("config endpoint gone"),
//!         }
//!     }
//! }
//! ```

pub mod error;
pub mod http;
pub mod watcher;

#[cfg(feature = "object-store")]
pub mod store;

pub use error::ExternalError;
pub use watcher::{ExternalEvent, WatchConfig, watch_external, watch_external_with_config};

#[cfg(test)]
mod tests;
