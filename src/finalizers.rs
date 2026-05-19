use std::fmt::Debug;

use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope};
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tracing::info;

use crate::error::Result;
use crate::scope::ApiScope;

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
/// or [`add_finalizer_cluster`] for the common cases.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::NamespaceResourceScope;
/// use kube_genops::finalizers::add_finalizer;
/// use kube_genops::scope::Namespaced;
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Clone + std::fmt::Debug + serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
/// # {
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
    T: Clone + Debug + Resource<DynamicType = ()> + DeserializeOwned + Serialize + 'static,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    info!(%kind, %name, %finalizer, "Adding finalizer");
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
/// or [`remove_finalizers_cluster`] for the common cases.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::NamespaceResourceScope;
/// use kube_genops::finalizers::remove_finalizers;
/// use kube_genops::scope::Namespaced;
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Clone + std::fmt::Debug + serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
/// # {
/// remove_finalizers::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     "my-resource",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_finalizers<T, Scope>(
    client: Client,
    scope: Scope,
    name: &str,
) -> Result<T>
where
    T: Clone + Debug + Resource<DynamicType = ()> + DeserializeOwned + Serialize + 'static,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    info!(%kind, %name, "Removing all finalizers");
    let patch = json!({ "metadata": { "finalizers": null } });
    apply_finalizer_patch(scope.into_api(client), name, patch).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — namespaced
// ---------------------------------------------------------------------------

/// Add a finalizer to a **namespace-scoped** resource.
///
/// Convenience wrapper around [`add_finalizer`] that fixes the scope to
/// [`Namespaced`]. The resource type `T` must implement
/// `Resource<Scope = NamespaceResourceScope>`.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::NamespaceResourceScope;
/// use kube_genops::finalizers::add_finalizer_namespaced;
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Clone + std::fmt::Debug + for<'de> serde::Deserialize<'de> + 'static,
/// # {
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
    T: Clone + Debug + Resource<DynamicType = (), Scope = NamespaceResourceScope> + DeserializeOwned + 'static,
{
    let kind = T::kind(&());
    info!(%kind, %name, %namespace, %finalizer, "Adding finalizer");
    let patch = json!({ "metadata": { "finalizers": [finalizer] } });
    apply_finalizer_patch(Api::namespaced(client, namespace), name, patch).await
}

/// Remove all finalizers from a **namespace-scoped** resource.
///
/// Convenience wrapper around [`remove_finalizers`] that fixes the scope to
/// [`Namespaced`]. Sets `metadata.finalizers` to `null`, unblocking deletion.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::NamespaceResourceScope;
/// use kube_genops::finalizers::remove_finalizers_namespaced;
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Clone + std::fmt::Debug + for<'de> serde::Deserialize<'de> + 'static,
/// # {
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
    T: Clone + Debug + Resource<DynamicType = (), Scope = NamespaceResourceScope> + DeserializeOwned + 'static,
{
    let kind = T::kind(&());
    info!(%kind, %name, %namespace, "Removing all finalizers");
    let patch = json!({ "metadata": { "finalizers": null } });
    apply_finalizer_patch(Api::namespaced(client, namespace), name, patch).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — cluster-scoped
// ---------------------------------------------------------------------------

/// Add a finalizer to a **cluster-scoped** resource.
///
/// Convenience wrapper around [`add_finalizer`] that fixes the scope to
/// [`Cluster`]. The resource type `T` must implement
/// `Resource<Scope = ClusterResourceScope>`.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::ClusterResourceScope;
/// use kube_genops::finalizers::add_finalizer_cluster;
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = ClusterResourceScope>
/// #         + Clone + std::fmt::Debug + for<'de> serde::Deserialize<'de> + 'static,
/// # {
/// add_finalizer_cluster::<MyCR>(
///     client,
///     "my-resource",
///     "my-operator/cleanup",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_finalizer_cluster<T>(
    client: Client,
    name: &str,
    finalizer: &str,
) -> Result<T>
where
    T: Clone + Debug + Resource<DynamicType = (), Scope = ClusterResourceScope> + DeserializeOwned + 'static,
{
    let kind = T::kind(&());
    info!(%kind, %name, %finalizer, "Adding cluster finalizer");
    let patch = json!({ "metadata": { "finalizers": [finalizer] } });
    apply_finalizer_patch(Api::all(client), name, patch).await
}

/// Remove all finalizers from a **cluster-scoped** resource.
///
/// Convenience wrapper around [`remove_finalizers`] that fixes the scope to
/// [`Cluster`]. Sets `metadata.finalizers` to `null`, unblocking deletion.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::ClusterResourceScope;
/// use kube_genops::finalizers::remove_finalizers_cluster;
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = ClusterResourceScope>
/// #         + Clone + std::fmt::Debug + for<'de> serde::Deserialize<'de> + 'static,
/// # {
/// remove_finalizers_cluster::<MyCR>(
///     client,
///     "my-resource",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_finalizers_cluster<T>(
    client: Client,
    name: &str,
) -> Result<T>
where
    T: Clone + Debug + Resource<DynamicType = (), Scope = ClusterResourceScope> + DeserializeOwned + 'static,
{
    let kind = T::kind(&());
    info!(%kind, %name, "Removing all cluster finalizers");
    let patch = json!({ "metadata": { "finalizers": null } });
    apply_finalizer_patch(Api::all(client), name, patch).await
}