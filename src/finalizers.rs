use std::fmt::Debug;

use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde::de::DeserializeOwned;
use serde_json::json;
use tracing::info;

use crate::error::Result;
use crate::scope::{ApiScope, Cluster, Namespaced};
use crate::traits::{ClusterResource, KubeResource, NamespacedResource};

// ---------------------------------------------------------------------------
// Private core helpers
// ---------------------------------------------------------------------------

async fn apply_finalizer_patch<T>(api: Api<T>, name: &str, patch: serde_json::Value) -> Result<T>
where
    T: Clone + Debug + DeserializeOwned,
{
    Ok(api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?)
}

// ---------------------------------------------------------------------------
// Generic public API
// ---------------------------------------------------------------------------

/// Add a finalizer to a Kubernetes resource using a strategic merge patch.
///
/// Existing finalizers are preserved — only the new one is appended.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time. Prefer [`add_finalizer_namespaced`]
/// or [`add_finalizer_cluster`] for the common cases — they are thin wrappers
/// around this function.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::finalizers::add_finalizer;
/// use kube_genops::scope::Namespaced;
/// use kube_genops::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> anyhow::Result<()> {
/// add_finalizer::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     "my-resource",
///     "my-operator/cleanup",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_finalizer<T, Scope>(
    client: Client,
    scope: Scope,
    name: &str,
    finalizer: &str,
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match scope.namespace() {
        Some(namespace) => info!(%namespace, %kind, %name, %finalizer, "Adding finalizer"),
        None => info!(%kind, %name, %finalizer, "Adding finalizer"),
    }

    let patch = json!({ "metadata": { "finalizers": [finalizer] } });
    apply_finalizer_patch(scope.into_api(client), name, patch).await
}

/// Remove all finalizers from a Kubernetes resource.
///
/// Sets `metadata.finalizers` to `null`, which unblocks deletion of any
/// resource that was held by finalizers.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time. Prefer [`remove_finalizers_namespaced`]
/// or [`remove_finalizers_cluster`] for the common cases — they are thin
/// wrappers around this function.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::finalizers::remove_finalizers;
/// use kube_genops::scope::Namespaced;
/// use kube_genops::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> anyhow::Result<()> {
/// remove_finalizers::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     "my-resource",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_finalizers<T, Scope>(client: Client, scope: Scope, name: &str) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match scope.namespace() {
        Some(namespace) => info!(%namespace, %kind, %name, "Removing all finalizers"),
        None => info!(%kind, %name, "Removing all finalizers"),
    }

    let patch = json!({ "metadata": { "finalizers": null } });
    apply_finalizer_patch(scope.into_api(client), name, patch).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — namespaced
// ---------------------------------------------------------------------------

/// Add a finalizer to a **namespace-scoped** resource.
///
/// Delegates to [`add_finalizer`] with [`Namespaced`] as the scope. The
/// resource type `T` must implement [`NamespacedResource`], which the compiler
/// enforces — passing a cluster-scoped type is a compile error.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::finalizers::add_finalizer_namespaced;
/// use kube_genops::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> anyhow::Result<()> {
/// add_finalizer_namespaced::<MyCR>(
///     client,
///     "my-namespace",
///     "my-resource",
///     "my-operator/cleanup",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_finalizer_namespaced<T>(
    client: Client,
    namespace: &str,
    name: &str,
    finalizer: &str,
) -> Result<T>
where
    T: NamespacedResource,
{
    add_finalizer::<T, _>(client, Namespaced(namespace), name, finalizer).await
}

/// Remove all finalizers from a **namespace-scoped** resource.
///
/// Delegates to [`remove_finalizers`] with [`Namespaced`] as the scope. Sets
/// `metadata.finalizers` to `null`, unblocking deletion. The resource type
/// `T` must implement [`NamespacedResource`].
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::finalizers::remove_finalizers_namespaced;
/// use kube_genops::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> anyhow::Result<()> {
/// remove_finalizers_namespaced::<MyCR>(
///     client,
///     "my-namespace",
///     "my-resource",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_finalizers_namespaced<T>(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<T>
where
    T: NamespacedResource,
{
    remove_finalizers::<T, _>(client, Namespaced(namespace), name).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — cluster-scoped
// ---------------------------------------------------------------------------

/// Add a finalizer to a **cluster-scoped** resource.
///
/// Delegates to [`add_finalizer`] with [`Cluster`] as the scope. The resource
/// type `T` must implement [`ClusterResource`], which the compiler enforces —
/// passing a namespace-scoped type is a compile error.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::finalizers::add_finalizer_cluster;
/// use kube_genops::traits::ClusterResource;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> anyhow::Result<()> {
/// add_finalizer_cluster::<MyCR>(
///     client,
///     "my-resource",
///     "my-operator/cleanup",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_finalizer_cluster<T>(client: Client, name: &str, finalizer: &str) -> Result<T>
where
    T: ClusterResource,
{
    add_finalizer::<T, _>(client, Cluster, name, finalizer).await
}

/// Remove all finalizers from a **cluster-scoped** resource.
///
/// Delegates to [`remove_finalizers`] with [`Cluster`] as the scope. Sets
/// `metadata.finalizers` to `null`, unblocking deletion. The resource type
/// `T` must implement [`ClusterResource`].
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::finalizers::remove_finalizers_cluster;
/// use kube_genops::traits::ClusterResource;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> anyhow::Result<()> {
/// remove_finalizers_cluster::<MyCR>(
///     client,
///     "my-resource",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_finalizers_cluster<T>(client: Client, name: &str) -> Result<T>
where
    T: ClusterResource,
{
    remove_finalizers::<T, _>(client, Cluster, name).await
}
