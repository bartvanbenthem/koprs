use std::collections::HashSet;
use std::time::Duration;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, ListParams, ObjectList, Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Api, Client, ResourceExt};
use tracing::{error, info};

use crate::error::Result;
use crate::scope::ApiScope;
use crate::traits::KubeResource;

// ---------------------------------------------------------------------------
// EnsureOutcome
// ---------------------------------------------------------------------------

/// The outcome of an [`ensure_resource`] call.
///
/// Callers can branch on this to skip downstream work (status patches, event
/// emissions) when the resource was already in the desired state.
#[derive(Debug)]
pub enum EnsureOutcome<T> {
    /// The resource did not exist and was created.
    Created(T),
    /// The resource existed but differed from the desired state; SSA corrected it.
    Updated(T),
    /// The resource already matched the desired state; no write was made.
    Unchanged(T),
}

impl<T> EnsureOutcome<T> {
    /// Unwrap the inner resource regardless of outcome.
    pub fn into_resource(self) -> T {
        match self {
            Self::Created(r) | Self::Updated(r) | Self::Unchanged(r) => r,
        }
    }

    /// Returns `true` if the resource was written (created or updated).
    pub fn was_changed(&self) -> bool {
        matches!(self, Self::Created(_) | Self::Updated(_))
    }
}

// ---------------------------------------------------------------------------
// Namespace
// ---------------------------------------------------------------------------

/// Ensure a namespace exists, creating or updating it via Server-Side Apply.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::ensure_namespace;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// ensure_namespace(client, "my-namespace", "my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn ensure_namespace(
    client: Client,
    name: &str,
    field_manager: &str,
) -> Result<Namespace> {
    let api: Api<Namespace> = Api::all(client);
    let ns = Namespace {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    info!(%name, "Ensuring namespace exists");
    let params = PatchParams::apply(field_manager).force();
    Ok(api.patch(name, &params, &Patch::Apply(&ns)).await?)
}

// ---------------------------------------------------------------------------
// Private core helpers
// ---------------------------------------------------------------------------

async fn apply_resource_inner<T>(api: Api<T>, resource: &T, field_manager: &str) -> Result<T>
where
    T: KubeResource,
{
    let name = resource.meta().name.as_deref().unwrap_or("[unnamed]");
    let params = PatchParams::apply(field_manager).force();
    Ok(api.patch(name, &params, &Patch::Apply(resource)).await?)
}

async fn delete_resource_inner<T>(api: Api<T>, name: &str) -> Result<bool>
where
    T: KubeResource,
{
    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(false),
        Err(e) => Err(e.into()),
    }
}

async fn get_resource_inner<T>(api: Api<T>, name: &str) -> Result<Option<T>>
where
    T: KubeResource,
{
    match api.get(name).await {
        Ok(r) => Ok(Some(r)),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
        Err(e) => Err(e.into()),
    }
}

async fn patch_metadata_inner<T>(api: Api<T>, name: &str, patch: serde_json::Value) -> Result<T>
where
    T: KubeResource,
{
    Ok(api
        .patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?)
}

async fn list_inner<T>(api: Api<T>, params: ListParams) -> Result<ObjectList<T>>
where
    T: KubeResource,
{
    Ok(api.list(&params).await?)
}

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

/// Apply (create or update) a Kubernetes resource using Server-Side Apply.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::apply_resource;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client, resource: MyCR) -> Result<(), KubeGenericError>
/// # where MyCR: koprs::traits::KubeResource {
/// apply_resource::<MyCR, _>(client, Namespaced("my-namespace"), &resource, "my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn apply_resource<T, Scope>(
    client: Client,
    scope: Scope,
    resource: &T,
    field_manager: &str,
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let name = resource.meta().name.as_deref().unwrap_or("[unnamed]");
    let kind = T::kind(&());

    match scope.namespace() {
        Some(namespace) => info!(%namespace, %kind, %name, "Applying resource"),
        None => info!(%kind, %name, "Applying resource"),
    }

    apply_resource_inner(scope.into_api(client), resource, field_manager).await
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

/// Delete a Kubernetes resource by name.
///
/// Returns `Ok(true)` if deleted, `Ok(false)` if the resource was already gone
/// (404), so callers can treat "already deleted" as success without additional
/// error handling.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::delete_resource;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// use k8s_openapi::api::core::v1::ConfigMap;
/// let deleted = delete_resource::<ConfigMap, _>(client, Namespaced("my-namespace"), "my-resource").await?;
/// # Ok(())
/// # }
/// ```
pub async fn delete_resource<T, Scope>(client: Client, scope: Scope, name: &str) -> Result<bool>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());

    match scope.namespace() {
        Some(namespace) => info!(%namespace, %kind, %name, "Deleting resource"),
        None => info!(%kind, %name, "Deleting resource"),
    }

    delete_resource_inner(scope.into_api(client), name).await
}

// ---------------------------------------------------------------------------
// Get
// ---------------------------------------------------------------------------

/// Get a single Kubernetes resource by name, returning `None` if it does not exist.
///
/// Returns `Ok(None)` on a 404 response rather than an error, so callers can
/// branch on existence without pattern-matching on [`crate::error::KubeGenericError`].
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::get_resource;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// match get_resource::<MyCR, _>(client, Namespaced("my-namespace"), "my-cr").await? {
///     Some(cr) => println!("found: {}", cr.meta().name.as_deref().unwrap_or("")),
///     None => println!("not found"),
/// }
/// # Ok(())
/// # }
/// ```
pub async fn get_resource<T, Scope>(client: Client, scope: Scope, name: &str) -> Result<Option<T>>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match scope.namespace() {
        Some(namespace) => info!(%namespace, %kind, %name, "Getting resource"),
        None => info!(%kind, %name, "Getting resource"),
    }
    get_resource_inner(scope.into_api(client), name).await
}

// ---------------------------------------------------------------------------
// Exists
// ---------------------------------------------------------------------------

/// Check whether a Kubernetes resource exists.
///
/// Returns `Ok(true)` if the resource is found, `Ok(false)` on a 404.
/// Does not fetch the full resource — use [`get_resource`] if you need the value.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::exists;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// if exists::<MyCR, _>(client, Namespaced("my-namespace"), "my-cr").await? {
///     // resource exists
/// }
/// # Ok(())
/// # }
/// ```
pub async fn exists<T, Scope>(client: Client, scope: Scope, name: &str) -> Result<bool>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    Ok(get_resource_inner(scope.into_api(client), name)
        .await?
        .is_some())
}

// ---------------------------------------------------------------------------
// Ensure
// ---------------------------------------------------------------------------

/// Ensure a resource exists and matches the desired state, using Server-Side Apply.
///
/// Performs a GET before the SSA and compares `resourceVersion` to determine
/// whether the resource was created, updated, or left unchanged. Returns an
/// [`EnsureOutcome`] so callers can skip downstream work on
/// [`EnsureOutcome::Unchanged`].
///
/// This costs 2 API calls (GET + SSA) versus 1 for plain [`apply_resource`].
/// The benefit is knowing the outcome — useful when downstream steps (status
/// patches, event emissions) should only run on actual change.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::{ensure_resource, EnsureOutcome};
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client, resource: MyCR) -> Result<(), KubeGenericError> {
/// match ensure_resource::<MyCR, _>(client, Namespaced("my-namespace"), &resource, "my-operator").await? {
///     EnsureOutcome::Created(_)   => { /* handle create */ }
///     EnsureOutcome::Updated(_)   => { /* handle drift correction */ }
///     EnsureOutcome::Unchanged(_) => { /* skip downstream work */ }
/// }
/// # Ok(())
/// # }
/// ```
pub async fn ensure_resource<T, Scope>(
    client: Client,
    scope: Scope,
    resource: &T,
    field_manager: &str,
) -> Result<EnsureOutcome<T>>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let name = resource.meta().name.as_deref().unwrap_or("[unnamed]");
    let kind = T::kind(&());

    match scope.namespace() {
        Some(ns) => info!(%ns, %kind, %name, "Ensuring resource"),
        None => info!(%kind, %name, "Ensuring resource"),
    }

    let api = scope.into_api(client);

    let live_rv = get_resource_inner(api.clone(), name)
        .await?
        .and_then(|r| r.meta().resource_version.clone());

    let applied = apply_resource_inner(api, resource, field_manager).await?;
    let applied_rv = applied.meta().resource_version.clone();

    let outcome = match live_rv {
        None => {
            info!(%kind, %name, "Resource created");
            EnsureOutcome::Created(applied)
        }
        Some(old_rv) if applied_rv.as_deref() != Some(&old_rv) => {
            info!(%kind, %name, "Resource updated");
            EnsureOutcome::Updated(applied)
        }
        _ => {
            info!(%kind, %name, "Resource unchanged");
            EnsureOutcome::Unchanged(applied)
        }
    };

    Ok(outcome)
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

/// List resources of type `T` within the given scope using arbitrary [`ListParams`].
///
/// Pass [`Cluster`] to list across all namespaces (or for cluster-scoped
/// resources), or [`Namespaced`] to list within a single namespace.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use kube::api::ListParams;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::list_resources_scoped;
/// use koprs::scope::Namespaced;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// let params = ListParams::default().labels("app=my-operator");
/// let pods = list_resources_scoped::<Pod, _>(client, Namespaced("my-ns"), params).await?;
/// # Ok(())
/// # }
/// ```
pub async fn list_resources_scoped<T, Scope>(
    client: Client,
    scope: Scope,
    params: ListParams,
) -> Result<ObjectList<T>>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    list_inner(scope.into_api(client), params).await
}

/// List the names of all resources of type `T` matching a label selector,
/// returned as a `HashSet<String>`. Useful for garbage collection diffing.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::list_resource_names;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// let names = list_resource_names::<Pod>(client, "app=my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn list_resource_names<T>(client: Client, label_selector: &str) -> Result<HashSet<String>>
where
    T: KubeResource,
{
    let list = list_inner(
        Api::<T>::all(client),
        ListParams::default().labels(label_selector),
    )
    .await?;
    Ok(list.items.iter().map(ResourceExt::name_any).collect())
}

// ---------------------------------------------------------------------------
// Polling — wait for resources
// ---------------------------------------------------------------------------

/// Poll until at least one resource of type `T` exists, returning the full list.
///
/// Retries every `interval` on a healthy API returning zero results. On API
/// errors the interval is doubled (capped at 60 s) before retrying. Returns
/// as soon as one or more resources are found.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::wait_for_resources;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use std::time::Duration;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// let resources = wait_for_resources::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     Duration::from_secs(10),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn wait_for_resources<T, Scope>(
    client: Client,
    scope: Scope,
    interval: Duration,
) -> Result<Vec<T>>
where
    T: KubeResource,
    Scope: ApiScope<T> + Clone,
{
    let kind = T::kind(&());
    let namespace = scope.namespace();

    match namespace {
        Some(ns) => info!(namespace = %ns, %kind, "Waiting for at least one resource"),
        None => info!(%kind, "Waiting for at least one resource"),
    }

    loop {
        let api: Api<T> = scope.clone().into_api(client.clone());
        match api.list(&Default::default()).await {
            Ok(list) if !list.items.is_empty() => {
                info!(%kind, count = list.items.len(), "Resources found");
                return Ok(list.items);
            }
            Ok(_) => {
                info!(%kind, ?interval, "No resources found, retrying");
                tokio::time::sleep(interval).await;
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                let backoff = interval.min(Duration::from_secs(60));
                error!(%kind, code = 404, ?backoff, "CRD not found, retrying");
                tokio::time::sleep(backoff).await;
            }
            Err(e) => {
                let backoff = (interval * 2).min(Duration::from_secs(60));
                error!(%kind, error = %e, ?backoff, "API error, retrying");
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Polling — wait for condition
// ---------------------------------------------------------------------------

/// Poll until a single resource satisfies `predicate`, returning it.
///
/// Fetches the resource by `name` every `interval`. Returns as soon as the
/// predicate returns `true`. While the resource does not exist, or while the
/// predicate returns `false`, the loop sleeps `interval` and retries. API
/// errors double the sleep (capped at 60 s) before retrying.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::wait_for_condition;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use std::time::Duration;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// let cr = wait_for_condition::<MyCR, _, _>(
///     client,
///     Namespaced("my-namespace"),
///     "my-cr",
///     Duration::from_secs(5),
///     |r| r.meta().generation.is_some(),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn wait_for_condition<T, Scope, F>(
    client: Client,
    scope: Scope,
    name: &str,
    interval: Duration,
    predicate: F,
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T> + Clone,
    F: Fn(&T) -> bool,
{
    let kind = T::kind(&());
    match scope.namespace() {
        Some(ns) => info!(namespace = %ns, %kind, %name, "Waiting for condition"),
        None => info!(%kind, %name, "Waiting for condition"),
    }

    loop {
        match get_resource_inner(scope.clone().into_api(client.clone()), name).await {
            Ok(Some(r)) if predicate(&r) => return Ok(r),
            Ok(Some(_)) => {
                info!(%kind, %name, ?interval, "Condition not met, retrying");
                tokio::time::sleep(interval).await;
            }
            Ok(None) => {
                info!(%kind, %name, ?interval, "Resource not found, retrying");
                tokio::time::sleep(interval).await;
            }
            Err(e) => {
                let backoff = (interval * 2).min(Duration::from_secs(60));
                error!(%kind, %name, error = %e, ?backoff, "API error, retrying");
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Patch labels
// ---------------------------------------------------------------------------

/// Merge labels onto a Kubernetes resource without replacing existing ones.
///
/// Uses a JSON merge patch so only the specified keys are added or updated —
/// other labels on the resource are preserved. Pass an empty slice to no-op.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::patch_labels;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use kube::Client;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// patch_labels::<MyCR, _>(
///     client,
///     Namespaced("my-ns"),
///     "my-cr",
///     &[("app.kubernetes.io/managed-by", "my-operator")],
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_labels<T, Scope>(
    client: Client,
    scope: Scope,
    name: &str,
    labels: &[(&str, &str)],
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match scope.namespace() {
        Some(ns) => info!(namespace = %ns, %kind, %name, "Patching labels"),
        None => info!(%kind, %name, "Patching labels"),
    }
    let map: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
        .collect();
    let patch = serde_json::json!({ "metadata": { "labels": map } });
    patch_metadata_inner(scope.into_api(client), name, patch).await
}

// ---------------------------------------------------------------------------
// Patch annotations
// ---------------------------------------------------------------------------

/// Merge annotations onto a Kubernetes resource without replacing existing ones.
///
/// Uses a JSON merge patch so only the specified keys are added or updated —
/// other annotations on the resource are preserved.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::patch_annotations;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use kube::Client;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// patch_annotations::<MyCR, _>(
///     client,
///     Namespaced("my-ns"),
///     "my-cr",
///     &[("my-operator/last-synced", "2024-01-01")],
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_annotations<T, Scope>(
    client: Client,
    scope: Scope,
    name: &str,
    annotations: &[(&str, &str)],
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match scope.namespace() {
        Some(ns) => info!(namespace = %ns, %kind, %name, "Patching annotations"),
        None => info!(%kind, %name, "Patching annotations"),
    }
    let map: serde_json::Map<String, serde_json::Value> = annotations
        .iter()
        .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
        .collect();
    let patch = serde_json::json!({ "metadata": { "annotations": map } });
    patch_metadata_inner(scope.into_api(client), name, patch).await
}

// ---------------------------------------------------------------------------
// Remove labels
// ---------------------------------------------------------------------------

/// Remove specific label keys from a Kubernetes resource.
///
/// Uses a JSON merge patch with `null` values — the specified keys are deleted
/// while all other labels are preserved. Pass an empty slice to no-op.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::remove_labels;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use kube::Client;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// remove_labels::<MyCR, _>(
///     client,
///     Namespaced("my-ns"),
///     "my-cr",
///     &["app.kubernetes.io/managed-by"],
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_labels<T, Scope>(
    client: Client,
    scope: Scope,
    name: &str,
    keys: &[&str],
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match scope.namespace() {
        Some(ns) => info!(namespace = %ns, %kind, %name, "Removing labels"),
        None => info!(%kind, %name, "Removing labels"),
    }
    let map: serde_json::Map<String, serde_json::Value> = keys
        .iter()
        .map(|k| (k.to_string(), serde_json::Value::Null))
        .collect();
    let patch = serde_json::json!({ "metadata": { "labels": map } });
    patch_metadata_inner(scope.into_api(client), name, patch).await
}

// ---------------------------------------------------------------------------
// Remove annotations
// ---------------------------------------------------------------------------

/// Remove specific annotation keys from a Kubernetes resource.
///
/// Uses a JSON merge patch with `null` values — the specified keys are deleted
/// while all other annotations are preserved.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::remove_annotations;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use kube::Client;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// remove_annotations::<MyCR, _>(
///     client,
///     Namespaced("my-ns"),
///     "my-cr",
///     &["my-operator/last-synced"],
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_annotations<T, Scope>(
    client: Client,
    scope: Scope,
    name: &str,
    keys: &[&str],
) -> Result<T>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match scope.namespace() {
        Some(ns) => info!(namespace = %ns, %kind, %name, "Removing annotations"),
        None => info!(%kind, %name, "Removing annotations"),
    }
    let map: serde_json::Map<String, serde_json::Value> = keys
        .iter()
        .map(|k| (k.to_string(), serde_json::Value::Null))
        .collect();
    let patch = serde_json::json!({ "metadata": { "annotations": map } });
    patch_metadata_inner(scope.into_api(client), name, patch).await
}
