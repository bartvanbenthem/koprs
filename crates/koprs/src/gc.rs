use kube::api::{DeleteParams, ListParams, Patch, PatchParams};
use kube::{Api, Client, ResourceExt};
use serde_json::json;
use tracing::info;

use crate::error::Result;
use crate::scope::ApiScope;
use crate::traits::KubeResource;

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
    T: KubeResource,
    F: Fn(&T) -> bool,
{
    let existing = list_api
        .list(&ListParams::default().labels(label_selector))
        .await?;

    for resource in existing {
        let name = resource.name_any();
        let ns = resource.namespace().unwrap_or_default();
        let api = make_api(&ns);

        if is_desired(&resource) {
            continue;
        }

        if resource.meta().deletion_timestamp.is_some() {
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
    T: KubeResource,
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
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use kube::ResourceExt;
/// use koprs::gc::gc_resources;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
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
    T: KubeResource,
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
