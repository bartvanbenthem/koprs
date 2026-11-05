//! Scope markers for Kubernetes API surface selection.
//!
//! Kubernetes resources are either cluster-scoped (e.g. `ClusterRole`, `Node`)
//! or namespace-scoped (e.g. `Pod`, `Deployment`). This module provides
//! compile-time markers that drive the correct [`kube::Api`] constructor and
//! reject scope / resource mismatches **at compile time**:
//!
//! | Marker | Valid for | Operations |
//! |--------|-----------|------------|
//! | [`Cluster`] | cluster-scoped types only | everything |
//! | [`Namespaced`] | namespace-scoped types only | everything |
//! | [`AllNamespaces`] | namespace-scoped types only | list / watch / GC only |
//!
//! Two sealed traits encode the operations:
//!
//! * [`ListScope`] — where to list or watch. Implemented by all three markers.
//! * [`ApiScope`] — where a *single named object* lives (get, apply, patch,
//!   delete). Implemented by [`Cluster`] and [`Namespaced`] only, because a
//!   name alone does not identify a namespaced object across namespaces.
//!
//! ```compile_fail
//! # use koprs::scope::Cluster;
//! # use koprs::resources::delete_resource;
//! # use k8s_openapi::api::core::v1::ConfigMap;
//! # async fn f(client: kube::Client) {
//! // ConfigMap is namespaced — `Cluster` is rejected at compile time.
//! delete_resource::<ConfigMap, _>(client, Cluster, "x").await;
//! # }
//! ```
//!
//! ```compile_fail
//! # use koprs::scope::AllNamespaces;
//! # use koprs::resources::delete_resource;
//! # use k8s_openapi::api::core::v1::ConfigMap;
//! # async fn f(client: kube::Client) {
//! // A name is not unique across namespaces — single-object ops need `Namespaced`.
//! delete_resource::<ConfigMap, _>(client, AllNamespaces, "x").await;
//! # }
//! ```
//!
//! # Example
//!
//! ```no_run
//! use k8s_openapi::api::core::v1::{ConfigMap, Node};
//! use koprs::error::KubeGenericError;
//! use koprs::resources::{get_resource, list_resources_scoped};
//! use koprs::scope::{AllNamespaces, Cluster, Namespaced};
//!
//! # async fn example(client: kube::Client) -> Result<(), KubeGenericError> {
//! let node = get_resource::<Node, _>(client.clone(), Cluster, "node-1").await?;
//! let cm = get_resource::<ConfigMap, _>(client.clone(), Namespaced("ns"), "cm").await?;
//! let all = list_resources_scoped::<ConfigMap, _>(client, AllNamespaces, Default::default()).await?;
//! # Ok(())
//! # }
//! ```

use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope};
use kube::{Api, Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Marker type for cluster-scoped resources (e.g. `ClusterRole`, `Node`,
/// `PersistentVolume`). Resolves to [`Api::all`].
///
/// Only accepted for types whose `Scope` is [`ClusterResourceScope`]. To list
/// a namespaced type across every namespace, use [`AllNamespaces`].
#[derive(Debug, Clone, Copy)]
pub struct Cluster;

/// Marker type for namespace-scoped resources, carrying the target namespace.
/// Resolves to [`Api::namespaced`].
#[derive(Debug, Clone, Copy)]
pub struct Namespaced<'a>(pub &'a str);

/// Marker for listing or watching a namespace-scoped type across **all**
/// namespaces. Resolves to [`Api::all`].
///
/// Implements [`ListScope`] but not [`ApiScope`]: it cannot be used for
/// get / apply / patch / delete by name.
#[derive(Debug, Clone, Copy)]
pub struct AllNamespaces;

mod private {
    pub trait Sealed {}
    impl Sealed for super::Cluster {}
    impl Sealed for super::Namespaced<'_> {}
    impl Sealed for super::AllNamespaces {}
}

/// Sealed trait for the scope a collection is listed or watched in.
///
/// Implemented by [`Cluster`], [`Namespaced`], and [`AllNamespaces`]. You
/// never need to implement or name this trait — pass a marker at the call
/// site.
pub trait ListScope<K>: private::Sealed + Copy
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    /// The namespace name if this scope is bound to a single namespace.
    fn namespace(&self) -> Option<&str>;

    /// Consume this scope marker and produce the [`Api`] for the collection.
    fn into_api(self, client: Client) -> Api<K>;

    /// The [`Api`] that addresses an individual object returned by a list in
    /// this scope, given that object's own `metadata.namespace`.
    ///
    /// For [`AllNamespaces`] this is the object's namespace, so writes always
    /// target the namespace the object was actually found in.
    fn object_api(&self, client: Client, object_namespace: Option<&str>) -> Api<K>;
}

/// Sealed trait for the scope that identifies a **single named object**.
///
/// Implemented by [`Cluster`] (cluster-scoped types) and [`Namespaced`]
/// (namespace-scoped types) only.
pub trait ApiScope<K>: ListScope<K>
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
}

impl<K> ListScope<K> for Cluster
where
    K: Resource<Scope = ClusterResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    fn namespace(&self) -> Option<&str> {
        None
    }

    fn into_api(self, client: Client) -> Api<K> {
        Api::all(client)
    }

    fn object_api(&self, client: Client, _object_namespace: Option<&str>) -> Api<K> {
        Api::all(client)
    }
}

impl<K> ApiScope<K> for Cluster
where
    K: Resource<Scope = ClusterResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
}

impl<K> ListScope<K> for Namespaced<'_>
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    fn namespace(&self) -> Option<&str> {
        Some(self.0)
    }

    fn into_api(self, client: Client) -> Api<K> {
        Api::namespaced(client, self.0)
    }

    fn object_api(&self, client: Client, _object_namespace: Option<&str>) -> Api<K> {
        // Objects listed in a namespaced scope always live in that namespace.
        Api::namespaced(client, self.0)
    }
}

impl<K> ApiScope<K> for Namespaced<'_>
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
}

impl<K> ListScope<K> for AllNamespaces
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    fn namespace(&self) -> Option<&str> {
        None
    }

    fn into_api(self, client: Client) -> Api<K> {
        Api::all(client)
    }

    fn object_api(&self, client: Client, object_namespace: Option<&str>) -> Api<K> {
        match object_namespace {
            Some(ns) => Api::namespaced(client, ns),
            None => Api::all(client),
        }
    }
}
