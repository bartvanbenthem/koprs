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
use crate::scope::{ApiScope, Cluster, Namespaced};

// ---------------------------------------------------------------------------
// Private core helper
// ---------------------------------------------------------------------------

/// Shared GC loop. Operates on a pre-built listing `Api<T>` and a
/// per-resource `Api<T>` factory so it works for both cluster and namespaced
/// resources without duplicating the loop body.
///
/// `is_desired` returns `true` for any resource that should be kept.
async fn gc_inner<T, F>(
    list_api: Api<T>,
    make_api: impl Fn(&str) -> Api<T>,
    label_selector: &str,
    is_desired: F,
) -> Result<()>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Serialize
        + Default
        + Send
        + Sync
        + 'static,
    F: Fn(&T) -> bool,
{
    let existing = list_api
        .list(&ListParams::default().labels(label_selector))
        .await?;

    for resource in existing {
        let name = resource.name_any();
        let ns = resource.metadata().namespace.clone().unwrap_or_default();
        let api = make_api(&ns);

        if is_desired(&resource) {
            continue;
        }

        if resource.metadata().deletion_timestamp.is_some() {
            info!(%name, %ns, "Resource is terminating — clearing finalizers");
            clear_finalizers(&api, &name).await;
            continue;
        }

        info!(%name, %ns, "Deleting orphaned resource");
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

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Generic public API
// ---------------------------------------------------------------------------

/// Garbage collect orphaned Kubernetes resources.
///
/// Lists all resources of type `T` matching `label_selector` and deletes any
/// for which `is_desired` returns `false`. Resources already in termination
/// are unblocked by clearing their finalizers.
///
/// This generic form accepts any scope and a predicate so it can express both
/// the cluster case (`name not in set`) and the namespaced case
/// (`(namespace, name) not in set`) uniformly. Prefer
/// [`gc_cluster_resources`] or [`gc_namespaced_resources`] when the scope and
/// desired-set type are known at compile time.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashSet;
/// use kube::Client;
/// use kube::Resource;
/// use kube::ResourceExt;
/// use k8s_openapi::{NamespaceResourceScope, Metadata};
/// use kube::core::ObjectMeta;
/// use kube_genops::gc::gc_resources;
/// use kube_genops::scope::Namespaced;
/// use serde::{Deserialize, Serialize};
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Metadata<Ty = ObjectMeta>
/// #         + Clone + std::fmt::Debug + Default + Send + Sync
/// #         + Serialize + for<'de> Deserialize<'de> + 'static,
/// # {
/// let desired: HashSet<(String, String)> = HashSet::from([
///     ("default".to_string(), "my-resource".to_string()),
/// ]);
/// gc_resources::<MyCR, _>(
///     client,
///     Namespaced("default"),
///     "app=my-operator",
///     |r| {
///         let ns = r.namespace().unwrap_or_default();
///         desired.contains(&(ns, r.name_any()))
///     },
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn gc_resources<T, Scope>(
    client: Client,
    scope: Scope,
    label_selector: &str,
    is_desired: impl Fn(&T) -> bool,
) -> Result<()>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Serialize
        + Default
        + Send
        + Sync
        + 'static,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    info!(%kind, %label_selector, "Starting GC");

    let list_api: Api<T> = Api::all(client.clone());
    let scoped_api: Api<T> = scope.into_api(client);
    gc_inner(
        list_api,
        |_ns| scoped_api.clone(),
        label_selector,
        is_desired,
    )
    .await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — cluster-scoped
// ---------------------------------------------------------------------------

/// Garbage collect orphaned **cluster-scoped** resources.
///
/// Delegates to the shared GC loop with `Api::all` and a name-based predicate.
/// Any resource whose name is not in `desired_names` is deleted. Resources
/// already in termination are unblocked by clearing their finalizers.
///
/// Prefer this over [`gc_resources`] when the scope and desired-set type are
/// known at compile time.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashSet;
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::api::core::v1::PersistentVolume;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// let desired = HashSet::from(["pv-a".to_string(), "pv-b".to_string()]);
/// kube_genops::gc::gc_cluster_resources::<PersistentVolume>(
///     client,
///     "app=my-operator",
///     |pv| desired.contains(pv.meta().name.as_deref().unwrap_or("")),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn gc_cluster_resources<T>(
    client: Client,
    label_selector: &str,
    is_desired: impl Fn(&T) -> bool,
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
    gc_resources::<T, _>(client, Cluster, label_selector, is_desired).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — namespaced
// ---------------------------------------------------------------------------

/// Garbage collect orphaned **namespace-scoped** resources across all namespaces.
///
/// Delegates to the shared GC loop with `Api::namespaced` per resource and a
/// `(namespace, name)` predicate. Any resource whose pair is not in
/// `desired_resources` is deleted. Resources already in termination are
/// unblocked by clearing their finalizers.
///
/// Prefer this over [`gc_resources`] when the scope and desired-set type are
/// known at compile time.
///
/// # Examples
///
/// ```no_run
/// use std::collections::HashSet;
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::api::core::v1::PersistentVolumeClaim;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// let desired = HashSet::from([
///     ("default".to_string(), "pvc-a".to_string()),
/// ]);
/// kube_genops::gc::gc_namespaced_resources::<PersistentVolumeClaim>(
///     client,
///     "default",
///     "app=my-operator",
///     |pvc| {
///         let meta = pvc.meta();
///         let ns = meta.namespace.as_deref().unwrap_or("");
///         let name = meta.name.as_deref().unwrap_or("");
///         desired.contains(&(ns.to_string(), name.to_string()))
///     },
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn gc_namespaced_resources<T>(
    client: Client,
    namespace: &str,
    label_selector: &str,
    is_desired: impl Fn(&T) -> bool,
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
    gc_resources::<T, _>(client, Namespaced(namespace), label_selector, is_desired).await
}
