//! # koprs
//!
//! Generic, reusable Kubernetes resource utilities for Rust operators.
//!
//! Provides a thin, ergonomic layer on top of [`kube`] for the most common
//! operator patterns: applying and deleting resources (cluster-scoped and
//! namespaced), patching status subresources, managing finalizers, garbage
//! collecting orphaned resources, watching resources for changes, and listing
//! resources with optional label selectors.
//!
//! All functions are generic over the resource type `T` / `K` so you write the
//! pattern once and reuse it across every CRD or built-in type in your operator.
//!
//! ## Quick start
//!
//! ```no_run
//! use koprs::error::KubeGenericError;
//! use koprs::resources::apply_resource;
//! use koprs::scope::Namespaced;
//! use kube::Client;
//! use k8s_openapi::api::apps::v1::Deployment;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), KubeGenericError> {
//!     let client = Client::try_default().await?;
//!     let deployment: Deployment = todo!("build your desired state");
//!
//!     apply_resource::<Deployment, _>(client.clone(), Namespaced("my-namespace"), &deployment, "my-operator").await?;
//!     Ok(())
//! }
//! ```

pub mod controller;
pub mod error;
pub mod events;
pub mod finalizers;
pub mod gc;
pub mod meta;
pub mod owners;
pub mod resources;
pub mod scope;
pub mod status;
pub mod traits;
pub mod watcher;

pub use error::KubeGenericError;
pub use traits::is_being_deleted;
pub use watcher::WatchEvent;

#[cfg(test)]
mod tests;
