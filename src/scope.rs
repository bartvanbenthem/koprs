//! Scope markers for Kubernetes API surface selection.
//!
//! Kubernetes resources are either cluster-scoped (e.g. `ClusterRole`, `Node`)
//! or namespace-scoped (e.g. `Pod`, `Deployment`). This module provides
//! compile-time markers — [`Cluster`] and [`Namespaced`] — that drive the
//! correct [`kube::Api`] constructor without any runtime branching.
//!
//! The [`ApiScope`] trait is sealed, meaning it cannot be implemented outside
//! this crate. Only [`Cluster`] and [`Namespaced`] are valid scopes.
//!
//! # Example
//!
//! ```no_run
//! use kube::Client;
//! use kube_genops::scope::{Cluster, Namespaced};
//! use kube_genops::status::patch_status;
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! struct MyStatus { ready: bool }
//!
//! # async fn example(client: Client) -> anyhow::Result<()> {
//! // patch_status::<MyCR, _, _>(client.clone(), Cluster, "my-cr", MyStatus { ready: true }, "my-op").await?;
//! // patch_status::<MyCR, _, _>(client, Namespaced("my-ns"), "my-cr", MyStatus { ready: true }, "my-op").await?;
//! # Ok(())
//! # }
//! ```

use k8s_openapi::NamespaceResourceScope;
use kube::{Api, Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Marker type for cluster-scoped resources.
///
/// Pass this to any function that accepts [`ApiScope`] when the target resource
/// is cluster-scoped (e.g. `ClusterRole`, `Node`, `PersistentVolume`).
/// Resolves to [`Api::all`] internally.
///
/// # Example
///
/// ```no_run
/// use kube_genops::scope::Cluster;
/// use kube_genops::status::patch_status;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example(client: kube::Client) -> anyhow::Result<()> {
/// // patch_status::<MyCR, _, _>(client, Cluster, "my-cr", MyStatus { ready: true }, "my-op").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy)]
pub struct Cluster;

/// Marker type for namespace-scoped resources, carrying the target namespace.
///
/// Pass this to any function that accepts [`ApiScope`] when the target resource
/// is namespace-scoped (e.g. `Pod`, `Deployment`, `ConfigMap`).
/// Resolves to [`Api::namespaced`] internally.
///
/// The inner `&str` is the namespace to operate in.
///
/// # Example
///
/// ```no_run
/// use kube_genops::scope::Namespaced;
/// use kube_genops::status::patch_status;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example(client: kube::Client) -> anyhow::Result<()> {
/// // patch_status::<MyCR, _, _>(client, Namespaced("my-ns"), "my-cr", MyStatus { ready: true }, "my-op").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy)]
pub struct Namespaced<'a>(pub &'a str);

mod private {
    pub trait Sealed {}
    impl Sealed for super::Cluster {}
    impl Sealed for super::Namespaced<'_> {}
}

/// Sealed trait that constructs the correct [`Api`] for a given scope.
///
/// Implemented by [`Cluster`] and [`Namespaced`] only. Because this trait is
/// sealed, it cannot be implemented outside this crate, ensuring that only the
/// two known scopes are ever passed to functions that accept `ApiScope<K>`.
///
/// You will not need to implement or name this trait directly — use [`Cluster`]
/// or [`Namespaced`] at call sites and the compiler resolves the rest.
pub trait ApiScope<K>: private::Sealed
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    /// Consume this scope marker and produce the appropriate [`Api`] handle.
    fn into_api(self, client: Client) -> Api<K>;
}

impl<K> ApiScope<K> for Cluster
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    fn into_api(self, client: Client) -> Api<K> {
        Api::all(client)
    }
}

impl<K> ApiScope<K> for Namespaced<'_>
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    fn into_api(self, client: Client) -> Api<K> {
        Api::namespaced(client, self.0)
    }
}
