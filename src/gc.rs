use std::collections::HashSet;
use std::fmt::Debug;

use k8s_openapi::{ClusterResourceScope, Metadata, NamespaceResourceScope};
use kube::api::{DeleteParams, ListParams, Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Api, Client, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tracing::info;

use crate::error::Result;

/// Garbage collect orphaned cluster-scoped resources.
///
/// Lists all resources of type `T` matching `label_selector`. Any resource
/// whose name is not in `desired_names` is deleted. Resources already in
/// termination are unblocked by clearing their finalizers.
///
/// # Example
/// ```no_run
/// use std::collections::HashSet;
/// use kube::Client;
/// use kube_genops::gc::gc_cluster_resources;
/// use k8s_openapi::api::core::v1::PersistentVolume;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// let desired = HashSet::from(["pv-a".to_string(), "pv-b".to_string()]);
/// gc_cluster_resources::<PersistentVolume>(client, "app=my-operator", &desired).await?;
/// # Ok(())
/// # }
/// ```
pub async fn gc_cluster_resources<T>(
    client: Client,
    label_selector: &str,
    desired_names: &HashSet<String>,
) -> Result<()>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = ClusterResourceScope>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Serialize
        + Default
        + Send
        + Sync
        + 'static,
{
    let api: Api<T> = Api::all(client);
    let existing = api
        .list(&ListParams::default().labels(label_selector))
        .await?;

    for resource in existing {
        let name = resource.name_any();

        if desired_names.contains(&name) {
            continue;
        }

        if resource.metadata().deletion_timestamp.is_some() {
            info!(%name, "Cluster resource is terminating — clearing finalizers");
            clear_finalizers(&api, &name).await;
            continue;
        }

        info!(%name, "Deleting orphaned cluster resource");
        match api.delete(&name, &DeleteParams::foreground()).await {
            Ok(_) => clear_finalizers(&api, &name).await,
            Err(kube::Error::Api(e)) if e.code == 404 => {
                info!(%name, "Already deleted, skipping");
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}

/// Garbage collect orphaned namespaced resources across all namespaces.
///
/// Lists all resources of type `T` matching `label_selector`. Any resource
/// whose `(namespace, name)` pair is not in `desired_resources` is deleted.
///
/// # Example
/// ```no_run
/// use std::collections::HashSet;
/// use kube::Client;
/// use kube_genops::gc::gc_namespaced_resources;
/// use k8s_openapi::api::core::v1::PersistentVolumeClaim;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// let desired = HashSet::from([
///     ("default".to_string(), "pvc-a".to_string()),
/// ]);
/// gc_namespaced_resources::<PersistentVolumeClaim>(client, "app=my-operator", &desired).await?;
/// # Ok(())
/// # }
/// ```
pub async fn gc_namespaced_resources<T>(
    client: Client,
    label_selector: &str,
    desired_resources: &HashSet<(String, String)>,
) -> Result<()>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Serialize
        + Default
        + Send
        + Sync
        + 'static,
{
    let api: Api<T> = Api::all(client.clone());
    let existing = api
        .list(&ListParams::default().labels(label_selector))
        .await?;

    for resource in existing {
        let name = resource.name_any();
        let ns = resource.metadata().namespace.clone().unwrap_or_default();

        if desired_resources.contains(&(ns.clone(), name.clone())) {
            continue;
        }

        let ns_api: Api<T> = Api::namespaced(client.clone(), &ns);

        if resource.metadata().deletion_timestamp.is_some() {
            info!(%name, %ns, "Namespaced resource is terminating — clearing finalizers");
            clear_finalizers(&ns_api, &name).await;
            continue;
        }

        info!(%name, %ns, "Deleting orphaned namespaced resource");
        match ns_api.delete(&name, &DeleteParams::foreground()).await {
            Ok(_) => clear_finalizers(&ns_api, &name).await,
            Err(kube::Error::Api(e)) if e.code == 404 => continue,
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}

// --- Internal helpers ---

/// Force-clear all finalizers on a resource to unblock stuck terminations.
/// Errors are intentionally swallowed — the resource may already be gone.
async fn clear_finalizers<T>(api: &Api<T>, name: &str)
where
    T: Clone + Debug + Resource<DynamicType = ()> + DeserializeOwned + Send + Sync + 'static,
{
    let patch = json!({ "metadata": { "finalizers": null } });
    let _ = api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await;
}
