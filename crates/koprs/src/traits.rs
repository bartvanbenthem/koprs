use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::{ClusterResourceScope, Metadata, NamespaceResourceScope};
use kube::Resource;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::Debug;

/// Core Kubernetes resource abstraction used across `koprs`.
///
/// `KubeResource` represents the minimal set of capabilities required
/// for a type to be safely used with generic Kubernetes operations such as
/// apply, delete, list, and serialization.
///
/// This trait is automatically implemented for any type that satisfies the
/// required bounds.
///
/// It does **not** encode whether a resource is cluster-scoped or
/// namespaced-scoped. Scope is handled by [`NamespacedResource`] and
/// [`ClusterResource`].
///
/// # Purpose
///
/// This trait exists to:
/// - Eliminate repetitive generic bounds
/// - Standardize Kubernetes resource constraints across the crate
/// - Ensure all resources are safe for async controller usage
///
/// # Non-goals
///
/// This trait does not:
/// - Encode resource scope
/// - Provide runtime behavior
/// - Restrict Kubernetes API usage surface
///
/// # Examples
///
/// ```no_run
/// use koprs::traits::KubeResource;
/// use k8s_openapi::api::core::v1::Pod;
///
/// fn assert_kube_resource<T: KubeResource>() {}
///
/// assert_kube_resource::<Pod>();
/// ```
pub trait KubeResource:
    Clone
    + Debug
    + Resource<DynamicType = ()>
    + Metadata<Ty = ObjectMeta>
    + DeserializeOwned
    + Serialize
    + Send
    + Sync
    + 'static
{
}

impl<T> KubeResource for T where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Serialize
        + Send
        + Sync
        + 'static
{
}

/// Marker trait for Kubernetes resources that are **namespaced-scoped**.
///
/// This trait guarantees that the resource can only be used with APIs that
/// require a namespace (e.g. `Api::namespaced`).
///
/// It enforces at compile time that:
/// - `T::Scope = NamespaceResourceScope`
///
/// # Why this exists
///
/// Without this trait, generic resource utilities cannot safely determine
/// whether a Kubernetes resource supports namespace-scoped operations.
///
/// This prevents accidental misuse such as attempting to call:
/// - namespaced APIs on cluster-scoped resources
///
/// # Examples
///
/// ```no_run
/// use koprs::traits::NamespacedResource;
/// use k8s_openapi::api::core::v1::Pod;
///
/// fn assert_namespaced<T: NamespacedResource>() {}
///
/// assert_namespaced::<Pod>();
/// ```
pub trait NamespacedResource: KubeResource
where
    Self: Resource<Scope = NamespaceResourceScope>,
{
}

impl<T> NamespacedResource for T where T: KubeResource + Resource<Scope = NamespaceResourceScope> {}

/// Marker trait for Kubernetes resources that are **cluster-scoped**.
///
/// This trait guarantees that the resource can only be used with cluster-level
/// APIs such as `Api::all`.
///
/// It enforces at compile time that:
/// - `T::Scope = ClusterResourceScope`
///
/// # Why this exists
///
/// Cluster-scoped resources (e.g. Nodes, ClusterRoles) cannot be addressed
/// within a namespace. This trait prevents accidental misuse of namespaced
/// APIs.
///
/// # Examples
///
/// ```no_run
/// use koprs::traits::ClusterResource;
/// use k8s_openapi::api::rbac::v1::ClusterRole;
///
/// fn assert_cluster<T: ClusterResource>() {}
///
/// assert_cluster::<ClusterRole>();
/// ```
pub trait ClusterResource: KubeResource
where
    Self: Resource<Scope = ClusterResourceScope>,
{
}

impl<T> ClusterResource for T where T: KubeResource + Resource<Scope = ClusterResourceScope> {}
