//! # kube-genops
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
//! use kube::Client;
//! use kube_genops::resources::{apply_namespaced_resource, delete_namespaced_resource};
//! use k8s_openapi::api::apps::v1::Deployment;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let client = Client::try_default().await?;
//!     let deployment: Deployment = todo!("build your desired state");
//!
//!     apply_namespaced_resource(client.clone(), "my-namespace", &deployment, "my-operator").await?;
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod finalizers;
pub mod gc;
pub mod resources;
pub mod status;
pub mod watcher;

pub use error::KubeGenericError;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
