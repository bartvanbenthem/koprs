//! Label-based garbage collection of resources an operator no longer wants.

use kube::api::{DeleteParams, ListParams, Preconditions};
use kube::{Client, ResourceExt};
use tracing::{debug, info};

use crate::error::{KubeGenericError, Result};
use crate::scope::ListScope;
use crate::traits::KubeResource;

/// Garbage collect orphaned Kubernetes resources.
///
/// Lists resources of type `T` in `scope` that match `label_selector` and
/// deletes every one for which `is_desired` returns `false`.
///
/// * **Scope is respected for both listing and deleting.** With
///   [`Namespaced`][crate::scope::Namespaced] only that namespace is listed;
///   with [`AllNamespaces`][crate::scope::AllNamespaces] each object is
///   deleted in the namespace it was found in; with
///   [`Cluster`][crate::scope::Cluster] cluster-scoped objects are deleted.
/// * **Finalizers are never touched.** Objects that are already terminating
///   are skipped, so the controllers that own their finalizers (and
///   Kubernetes' own `foregroundDeletion` / `pvc-protection` finalizers) can
///   finish cleanup.
/// * **Deletes are UID-guarded.** Each delete carries a UID precondition, so
///   an object that was deleted and re-created under the same name between
///   the list and the delete is left alone.
/// * Deletion uses background propagation (the Kubernetes default): the
///   object is removed immediately and its dependents are garbage collected
///   asynchronously by Kubernetes.
///
/// Make the selector specific to **one owner** (for example
/// `managed-by=my-operator,my-operator/owner=<cr-name>`). A selector shared by
/// several CRs makes each CR's GC delete the others' resources.
///
/// # Errors
///
/// Returns [`KubeGenericError::Config`] if `label_selector` is empty — that
/// would select every `T` in the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use kube::ResourceExt;
/// use koprs::gc::gc_resources;
/// use koprs::scope::Namespaced;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// gc_resources::<ConfigMap, _>(
///     client,
///     Namespaced("default"),
///     "app.kubernetes.io/managed-by=my-operator,my-operator/owner=my-cr",
///     |cm| cm.name_any() == "my-cr-config",
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
    Scope: ListScope<T>,
{
    if label_selector.trim().is_empty() {
        return Err(KubeGenericError::Config(
            "gc_resources requires a non-empty label selector".into(),
        ));
    }

    let kind = T::kind(&());
    debug!(%kind, %label_selector, "Starting GC");

    let existing = scope
        .into_api(client.clone())
        .list(&ListParams::default().labels(label_selector))
        .await?;

    for resource in existing {
        if is_desired(&resource) {
            continue;
        }

        let name = resource.name_any();
        let ns = resource.namespace();
        let ns_display = ns.as_deref().unwrap_or("");

        if resource.meta().deletion_timestamp.is_some() {
            debug!(%kind, %name, ns = %ns_display, "Orphan already terminating, skipping");
            continue;
        }

        let params = DeleteParams {
            preconditions: resource.uid().map(|uid| Preconditions {
                uid: Some(uid),
                resource_version: None,
            }),
            ..DeleteParams::background()
        };

        info!(%kind, %name, ns = %ns_display, "Deleting orphaned resource");
        let api = scope.object_api(client.clone(), ns.as_deref());
        match api.delete(&name, &params).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {
                debug!(%kind, %name, "Already deleted, skipping");
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                debug!(%kind, %name, "Object was replaced since listing, skipping");
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}
