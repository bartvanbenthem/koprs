//! Finalizer management.
//!
//! [`add_finalizer`] and [`remove_finalizer`] only ever touch **your**
//! finalizer and use JSON Patch `test` operations as a precondition, the same
//! technique `kube-runtime`'s finalizer helper uses:
//!
//! * Adding fails (HTTP 422) if `metadata.finalizers` changed since the copy of
//!   the object you passed in was read, instead of overwriting finalizers that
//!   other controllers added concurrently.
//! * Removing fails (HTTP 422) if the finalizer at the expected index is not
//!   yours, instead of removing someone else's.
//!
//! A failed precondition surfaces as a [`KubeGenericError::Kube`] error; let
//! the controller's error policy requeue so the next reconcile works from a
//! fresh copy.

use json_patch::Patch as JsonPatch;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde_json::json;
use tracing::info;

use crate::error::{KubeGenericError, Result};
use crate::scope::{ApiScope, Namespaced};
use crate::traits::{KubeResource, NamespacedResource};

// ---------------------------------------------------------------------------
// Private core helpers
// ---------------------------------------------------------------------------

fn required_name<T: KubeResource>(resource: &T) -> Result<&str> {
    resource
        .meta()
        .name
        .as_deref()
        .ok_or_else(|| KubeGenericError::MissingMetadata("name".into()))
}

fn required_namespace<T: KubeResource>(resource: &T) -> Result<&str> {
    resource
        .meta()
        .namespace
        .as_deref()
        .ok_or_else(|| KubeGenericError::MissingMetadata("namespace".into()))
}

fn current_finalizers<T: KubeResource>(resource: &T) -> &[String] {
    resource.meta().finalizers.as_deref().unwrap_or(&[])
}

/// JSON Patch that appends `finalizer`, guarded by a test that the list is
/// still exactly `current`.
pub(crate) fn add_finalizer_patch(current: &[String], finalizer: &str) -> Result<JsonPatch> {
    let ops = if current.is_empty() {
        json!([
            { "op": "test", "path": "/metadata/finalizers", "value": null },
            { "op": "add", "path": "/metadata/finalizers", "value": [finalizer] },
        ])
    } else {
        json!([
            { "op": "test", "path": "/metadata/finalizers", "value": current },
            { "op": "add", "path": "/metadata/finalizers/-", "value": finalizer },
        ])
    };
    Ok(serde_json::from_value(ops)?)
}

/// JSON Patch that removes the finalizer at `index`, guarded by a test that
/// the entry at that index is still `finalizer`.
pub(crate) fn remove_finalizer_patch(index: usize, finalizer: &str) -> Result<JsonPatch> {
    let path = format!("/metadata/finalizers/{index}");
    Ok(serde_json::from_value(json!([
        { "op": "test", "path": path, "value": finalizer },
        { "op": "remove", "path": path },
    ]))?)
}

async fn apply_json_patch<T>(api: Api<T>, name: &str, patch: JsonPatch) -> Result<T>
where
    T: KubeResource,
{
    Ok(api
        .patch(name, &PatchParams::default(), &Patch::Json::<()>(patch))
        .await?)
}

// ---------------------------------------------------------------------------
// Generic public API
// ---------------------------------------------------------------------------

/// Add `finalizer` to a resource, leaving every other finalizer untouched.
///
/// If the finalizer is already present the function returns immediately
/// without an API call. Otherwise it appends the finalizer with a JSON Patch
/// guarded by a `test` on the current finalizer list, so a concurrent change
/// by another controller makes the call fail (and the reconcile retry) rather
/// than silently overwriting that controller's finalizer.
///
/// Pass [`Cluster`][crate::scope::Cluster] or [`Namespaced`] as the `scope`
/// argument. Use [`add_finalizer_namespaced`] to take the namespace from the
/// resource's own metadata.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::finalizers::add_finalizer;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client, resource: &MyCR) -> Result<(), KubeGenericError> {
/// add_finalizer::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     resource,
///     "my-operator/cleanup",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_finalizer<T, Scope>(
    client: Client,
    scope: Scope,
    resource: &T,
    finalizer: &str,
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let name = required_name(resource)?;
    let current = current_finalizers(resource);
    if current.iter().any(|f| f == finalizer) {
        return Ok(resource.clone());
    }

    let kind = T::kind(&());
    match scope.namespace() {
        Some(namespace) => info!(%namespace, %kind, %name, %finalizer, "Adding finalizer"),
        None => info!(%kind, %name, %finalizer, "Adding finalizer"),
    }

    let patch = add_finalizer_patch(current, finalizer)?;
    apply_json_patch(scope.into_api(client), name, patch).await
}

/// Remove `finalizer` from a resource, leaving every other finalizer untouched.
///
/// Call this at the end of your cleanup path. If the finalizer is not present
/// on `resource` the function returns immediately without an API call.
/// Otherwise it removes the entry with a JSON Patch guarded by a `test` that
/// the entry at that index is still yours, so it can never remove another
/// controller's finalizer (such as `foregroundDeletion` or
/// `kubernetes.io/pvc-protection`).
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::finalizers::remove_finalizer;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client, resource: &MyCR) -> Result<(), KubeGenericError> {
/// remove_finalizer::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     resource,
///     "my-operator/cleanup",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_finalizer<T, Scope>(
    client: Client,
    scope: Scope,
    resource: &T,
    finalizer: &str,
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let name = required_name(resource)?;
    let Some(index) = current_finalizers(resource)
        .iter()
        .position(|f| f == finalizer)
    else {
        return Ok(resource.clone());
    };

    let kind = T::kind(&());
    match scope.namespace() {
        Some(namespace) => info!(%namespace, %kind, %name, %finalizer, "Removing finalizer"),
        None => info!(%kind, %name, %finalizer, "Removing finalizer"),
    }

    let patch = remove_finalizer_patch(index, finalizer)?;
    apply_json_patch(scope.into_api(client), name, patch).await
}

/// Remove **all** finalizers from a Kubernetes resource.
///
/// Sets `metadata.finalizers` to `null`, which also strips finalizers owned by
/// other controllers and by Kubernetes itself (`foregroundDeletion`,
/// `kubernetes.io/pvc-protection`, …) and skips their cleanup. Only use this
/// as a manual break-glass tool for stuck objects.
#[deprecated(
    since = "0.12.5",
    note = "removes finalizers owned by other controllers; use `remove_finalizer` to remove only yours"
)]
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
    Ok(scope
        .into_api(client)
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?)
}

// ---------------------------------------------------------------------------
// Convenience wrappers — namespaced
// ---------------------------------------------------------------------------

/// Add a finalizer to a **namespace-scoped** resource.
///
/// Extracts the namespace from the resource's metadata; otherwise identical to
/// [`add_finalizer`].
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::finalizers::add_finalizer_namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client, resource: &MyCR) -> Result<(), KubeGenericError> {
/// add_finalizer_namespaced::<MyCR>(client, resource, "my-operator/cleanup").await?;
/// # Ok(())
/// # }
/// ```
pub async fn add_finalizer_namespaced<T>(client: Client, resource: &T, finalizer: &str) -> Result<T>
where
    T: NamespacedResource,
{
    let namespace = required_namespace(resource)?;
    add_finalizer::<T, _>(client, Namespaced(namespace), resource, finalizer).await
}

/// Remove a finalizer from a **namespace-scoped** resource.
///
/// Extracts the namespace from the resource's metadata; otherwise identical to
/// [`remove_finalizer`].
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::finalizers::remove_finalizer_namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client, resource: &MyCR) -> Result<(), KubeGenericError> {
/// remove_finalizer_namespaced::<MyCR>(client, resource, "my-operator/cleanup").await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_finalizer_namespaced<T>(
    client: Client,
    resource: &T,
    finalizer: &str,
) -> Result<T>
where
    T: NamespacedResource,
{
    let namespace = required_namespace(resource)?;
    remove_finalizer::<T, _>(client, Namespaced(namespace), resource, finalizer).await
}
