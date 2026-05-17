use std::fmt::Debug;

use k8s_openapi::NamespaceResourceScope;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use serde_json::json;
use tracing::info;

use crate::error::Result;

// --- Namespaced ---

/// Add a finalizer to a namespaced resource.
///
/// Uses a strategic merge patch so existing finalizers are preserved.
pub async fn add_namespaced_finalizer<T>(
    client: Client,
    namespace: &str,
    name: &str,
    finalizer: &str,
) -> Result<T>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + DeserializeOwned,
{
    let api: Api<T> = Api::namespaced(client, namespace);
    let kind = T::kind(&());

    info!(%kind, %name, %namespace, %finalizer, "Adding finalizer");

    let patch = json!({ "metadata": { "finalizers": [finalizer] } });
    Ok(api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?)
}

/// Remove all finalizers from a namespaced resource.
///
/// Sets `metadata.finalizers` to `null`, which causes Kubernetes to proceed
/// with deletion of any resource that was blocked on finalizers.
pub async fn remove_namespaced_finalizers<T>(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<T>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + DeserializeOwned,
{
    let api: Api<T> = Api::namespaced(client, namespace);
    let kind = T::kind(&());

    info!(%kind, %name, %namespace, "Removing all finalizers");

    let patch = json!({ "metadata": { "finalizers": null } });
    Ok(api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?)
}

// --- Cluster-scoped ---

/// Add a finalizer to a cluster-scoped resource.
///
/// Uses a strategic merge patch so existing finalizers are preserved.
pub async fn add_cluster_finalizer<T>(
    client: Client,
    name: &str,
    finalizer: &str,
) -> Result<T>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = kube::core::ClusterResourceScope>
        + DeserializeOwned,
{
    let api: Api<T> = Api::all(client);
    let kind = T::kind(&());

    info!(%kind, %name, %finalizer, "Adding cluster finalizer");

    let patch = json!({ "metadata": { "finalizers": [finalizer] } });
    Ok(api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?)
}

/// Remove all finalizers from a cluster-scoped resource.
pub async fn remove_cluster_finalizers<T>(client: Client, name: &str) -> Result<T>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = kube::core::ClusterResourceScope>
        + DeserializeOwned,
{
    let api: Api<T> = Api::all(client);
    let kind = T::kind(&());

    info!(%kind, %name, "Removing all cluster finalizers");

    let patch = json!({ "metadata": { "finalizers": null } });
    Ok(api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?)
}
