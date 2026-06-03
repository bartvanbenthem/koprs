use std::collections::HashSet;
use std::time::Duration;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, ListParams, ObjectList, Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Api, Client, ResourceExt};
use tracing::{error, info};

use crate::error::Result;
use crate::scope::{ApiScope, Cluster, Namespaced};
use crate::traits::{ClusterResource, KubeResource, NamespacedResource};

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
// Generic public API — apply
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
/// # async fn example<MyCR>(client: Client, resource: MyCR) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: NamespacedResource,
/// # {
/// apply_resource::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     &resource,
///     "my-operator",
/// ).await?;
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
// Generic public API — delete
// ---------------------------------------------------------------------------

/// Delete a Kubernetes resource by name.
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
/// # async fn example<MyCR>(client: Client) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: NamespacedResource,
/// # {
/// delete_resource::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     "my-resource",
/// ).await?;
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
// Convenience wrappers — cluster-scoped
// ---------------------------------------------------------------------------

/// Apply a cluster-scoped resource.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::apply_cluster_resource;
/// use koprs::traits::ClusterResource;
///
/// # async fn example<MyCR>(client: Client, resource: MyCR) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: ClusterResource,
/// # {
/// apply_cluster_resource::<MyCR>(client, &resource, "my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn apply_cluster_resource<T>(
    client: Client,
    resource: &T,
    field_manager: &str,
) -> Result<T>
where
    T: ClusterResource,
{
    apply_resource::<T, _>(client, Cluster, resource, field_manager).await
}

/// Delete a cluster-scoped resource.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::delete_cluster_resource;
/// use koprs::traits::ClusterResource;
///
/// # async fn example<MyCR>(client: Client) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: ClusterResource,
/// # {
/// delete_cluster_resource::<MyCR>(client, "my-resource").await?;
/// # Ok(())
/// # }
/// ```
pub async fn delete_cluster_resource<T>(client: Client, name: &str) -> Result<bool>
where
    T: ClusterResource,
{
    delete_resource_inner(Api::<T>::all(client), name).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — namespaced
// ---------------------------------------------------------------------------

/// Apply a namespaced resource.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::apply_namespaced_resource;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR>(client: Client, resource: MyCR) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: NamespacedResource,
/// # {
/// apply_namespaced_resource::<MyCR>(client, "my-namespace", &resource, "my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn apply_namespaced_resource<T>(
    client: Client,
    namespace: &str,
    resource: &T,
    field_manager: &str,
) -> Result<T>
where
    T: NamespacedResource,
{
    apply_resource::<T, _>(client, Namespaced(namespace), resource, field_manager).await
}

/// Delete a namespaced resource.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::delete_namespaced_resource;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR>(client: Client) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: NamespacedResource,
/// # {
/// delete_namespaced_resource::<MyCR>(client, "my-namespace", "my-resource").await?;
/// # Ok(())
/// # }
/// ```
pub async fn delete_namespaced_resource<T>(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<bool>
where
    T: NamespacedResource,
{
    delete_resource_inner(Api::<T>::namespaced(client, namespace), name).await
}

// ---------------------------------------------------------------------------
// Generic public API — get
// ---------------------------------------------------------------------------

/// Get a single Kubernetes resource by name, returning `None` if it does not exist.
///
/// Returns `Ok(None)` on a 404 response rather than an error, so callers can
/// branch on existence without pattern-matching on [`crate::error::KubeGenericError`].
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument. Prefer
/// [`get_namespaced_resource`] or [`get_cluster_resource`] for the common cases.
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
// Convenience wrappers — get namespaced
// ---------------------------------------------------------------------------

/// Get a single **namespace-scoped** resource by name, returning `None` if it
/// does not exist.
///
/// Delegates to [`get_resource`] with [`Namespaced`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::get_namespaced_resource;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// if let Some(cr) = get_namespaced_resource::<MyCR>(client, "my-namespace", "my-cr").await? {
///     println!("found: {}", cr.meta().name.as_deref().unwrap_or(""));
/// }
/// # Ok(())
/// # }
/// ```
pub async fn get_namespaced_resource<T>(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<Option<T>>
where
    T: NamespacedResource,
{
    get_resource::<T, _>(client, Namespaced(namespace), name).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — get cluster-scoped
// ---------------------------------------------------------------------------

/// Get a single **cluster-scoped** resource by name, returning `None` if it
/// does not exist.
///
/// Delegates to [`get_resource`] with [`Cluster`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::get_cluster_resource;
/// use koprs::traits::ClusterResource;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError> {
/// if let Some(cr) = get_cluster_resource::<MyCR>(client, "my-cr").await? {
///     println!("found: {}", cr.meta().name.as_deref().unwrap_or(""));
/// }
/// # Ok(())
/// # }
/// ```
pub async fn get_cluster_resource<T>(client: Client, name: &str) -> Result<Option<T>>
where
    T: ClusterResource,
{
    get_resource::<T, _>(client, Cluster, name).await
}

// ---------------------------------------------------------------------------
// Generic public API — exists
// ---------------------------------------------------------------------------

/// Check whether a Kubernetes resource exists.
///
/// Returns `Ok(true)` if the resource is found, `Ok(false)` on a 404.
/// Does not return the resource — use [`get_resource`] if you need the value.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument. Prefer
/// [`exists_namespaced`] or [`exists_cluster`] for the common cases.
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

/// Check whether a **namespace-scoped** resource exists.
///
/// Delegates to [`exists`] with [`Namespaced`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::exists_namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// if exists_namespaced::<MyCR>(client, "my-namespace", "my-cr").await? {
///     // resource exists
/// }
/// # Ok(())
/// # }
/// ```
pub async fn exists_namespaced<T>(client: Client, namespace: &str, name: &str) -> Result<bool>
where
    T: NamespacedResource,
{
    exists::<T, _>(client, Namespaced(namespace), name).await
}

/// Check whether a **cluster-scoped** resource exists.
///
/// Delegates to [`exists`] with [`Cluster`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::exists_cluster;
/// use koprs::traits::ClusterResource;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError> {
/// if exists_cluster::<MyCR>(client, "my-cr").await? {
///     // resource exists
/// }
/// # Ok(())
/// # }
/// ```
pub async fn exists_cluster<T>(client: Client, name: &str) -> Result<bool>
where
    T: ClusterResource,
{
    exists::<T, _>(client, Cluster, name).await
}

// ---------------------------------------------------------------------------
// Generic public API — ensure
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
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument. Prefer
/// [`ensure_namespaced_resource`] or [`ensure_cluster_resource`] for the common cases.
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

/// Ensure a **namespace-scoped** resource exists and matches the desired state.
///
/// Delegates to [`ensure_resource`] with [`Namespaced`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::{ensure_namespaced_resource, EnsureOutcome};
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client, resource: MyCR) -> Result<(), KubeGenericError> {
/// let outcome = ensure_namespaced_resource::<MyCR>(client, "my-namespace", &resource, "my-operator").await?;
/// if outcome.was_changed() {
///     // patch status, emit event, etc.
/// }
/// # Ok(())
/// # }
/// ```
pub async fn ensure_namespaced_resource<T>(
    client: Client,
    namespace: &str,
    resource: &T,
    field_manager: &str,
) -> Result<EnsureOutcome<T>>
where
    T: NamespacedResource,
{
    ensure_resource::<T, _>(client, Namespaced(namespace), resource, field_manager).await
}

/// Ensure a **cluster-scoped** resource exists and matches the desired state.
///
/// Delegates to [`ensure_resource`] with [`Cluster`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::{ensure_cluster_resource, EnsureOutcome};
/// use koprs::traits::ClusterResource;
///
/// # async fn example<MyCR: ClusterResource>(client: Client, resource: MyCR) -> Result<(), KubeGenericError> {
/// let outcome = ensure_cluster_resource::<MyCR>(client, &resource, "my-operator").await?;
/// if outcome.was_changed() {
///     // patch status, emit event, etc.
/// }
/// # Ok(())
/// # }
/// ```
pub async fn ensure_cluster_resource<T>(
    client: Client,
    resource: &T,
    field_manager: &str,
) -> Result<EnsureOutcome<T>>
where
    T: ClusterResource,
{
    ensure_resource::<T, _>(client, Cluster, resource, field_manager).await
}

// ---------------------------------------------------------------------------
// Listing
// ---------------------------------------------------------------------------

/// List resources of type `T` within the given scope using arbitrary [`ListParams`].
///
/// Pass [`Cluster`] to list across all namespaces (or for cluster-scoped
/// resources), or [`Namespaced`] to list within a single namespace. Build
/// a [`ListParams`] to filter by label or field selector.
///
/// Prefer the typed convenience wrappers ([`list_resources`],
/// [`list_namespaced_resources`], [`list_resources_by_label`], etc.) when the
/// scope and filter are known at the call site.
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

/// List all resources of type `T` across all namespaces.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::list_resources;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// let pods = list_resources::<Pod>(client).await?;
/// # Ok(())
/// # }
/// ```
pub async fn list_resources<T>(client: Client) -> Result<ObjectList<T>>
where
    T: KubeResource,
{
    list_inner(Api::all(client), Default::default()).await
}

/// List all resources of type `T` matching a label selector.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::list_resources_by_label;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// let pods = list_resources_by_label::<Pod>(client, "app=my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn list_resources_by_label<T>(
    client: Client,
    label_selector: &str,
) -> Result<ObjectList<T>>
where
    T: KubeResource,
{
    list_inner(
        Api::all(client),
        ListParams::default().labels(label_selector),
    )
    .await
}

/// List all resources of type `T` in a specific namespace.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::list_namespaced_resources;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// let pods = list_namespaced_resources::<Pod>(client, "my-namespace").await?;
/// # Ok(())
/// # }
/// ```
pub async fn list_namespaced_resources<T>(client: Client, namespace: &str) -> Result<ObjectList<T>>
where
    T: NamespacedResource,
{
    list_inner(Api::namespaced(client, namespace), Default::default()).await
}

/// List all resources of type `T` in a specific namespace matching a label selector.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::list_namespaced_resources_by_label;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// let pods = list_namespaced_resources_by_label::<Pod>(client, "my-namespace", "app=my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn list_namespaced_resources_by_label<T>(
    client: Client,
    namespace: &str,
    label_selector: &str,
) -> Result<ObjectList<T>>
where
    T: NamespacedResource,
{
    list_inner(
        Api::namespaced(client, namespace),
        ListParams::default().labels(label_selector),
    )
    .await
}

/// List all resources of type `T` matching a field selector.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::list_by_field;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// let pods = list_by_field::<Pod>(client, "spec.nodeName=my-node").await?;
/// # Ok(())
/// # }
/// ```
pub async fn list_by_field<T>(client: Client, field_selector: &str) -> Result<ObjectList<T>>
where
    T: KubeResource,
{
    list_inner(
        Api::all(client),
        ListParams::default().fields(field_selector),
    )
    .await
}

/// List all resources of type `T` in a specific namespace matching a field selector.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::list_namespaced_by_field;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// let pods = list_namespaced_by_field::<Pod>(client, "my-namespace", "status.phase=Running").await?;
/// # Ok(())
/// # }
/// ```
pub async fn list_namespaced_by_field<T>(
    client: Client,
    namespace: &str,
    field_selector: &str,
) -> Result<ObjectList<T>>
where
    T: NamespacedResource,
{
    list_inner(
        Api::namespaced(client, namespace),
        ListParams::default().fields(field_selector),
    )
    .await
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
    let list = list_resources_by_label::<T>(client, label_selector).await?;
    Ok(list.items.iter().map(|r| r.name_any()).collect())
}

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

/// Poll until at least one resource of type `T` exists, returning the full list.
///
/// Retries every `interval` on a healthy API returning zero results. On API
/// errors the interval is doubled (capped at 60 s) before retrying. Returns
/// as soon as one or more resources are found.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time. Prefer [`wait_for_resources_namespaced`]
/// or [`wait_for_resources_cluster`] for the common cases — they are thin
/// wrappers around this function.
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

/// Poll until at least one **namespace-scoped** resource of type `T` exists,
/// returning the full list.
///
/// Delegates to [`wait_for_resources`] with [`Namespaced`] as the scope. The
/// resource type `T` must implement [`NamespacedResource`].
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::wait_for_resources_namespaced;
/// use koprs::traits::NamespacedResource;
/// use std::time::Duration;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// let resources = wait_for_resources_namespaced::<MyCR>(
///     client,
///     "my-namespace",
///     Duration::from_secs(10),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn wait_for_resources_namespaced<T>(
    client: Client,
    namespace: &str,
    interval: Duration,
) -> Result<Vec<T>>
where
    T: NamespacedResource,
{
    wait_for_resources::<T, _>(client, Namespaced(namespace), interval).await
}

/// Poll until at least one **cluster-scoped** resource of type `T` exists,
/// returning the full list.
///
/// Delegates to [`wait_for_resources`] with [`Cluster`] as the scope. The
/// resource type `T` must implement [`ClusterResource`].
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::wait_for_resources_cluster;
/// use koprs::traits::ClusterResource;
/// use std::time::Duration;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError> {
/// let resources = wait_for_resources_cluster::<MyCR>(
///     client,
///     Duration::from_secs(10),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn wait_for_resources_cluster<T>(client: Client, interval: Duration) -> Result<Vec<T>>
where
    T: ClusterResource,
{
    wait_for_resources::<T, _>(client, Cluster, interval).await
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
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument. Prefer
/// [`wait_for_condition_namespaced`] or [`wait_for_condition_cluster`] for the
/// common cases.
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
///     |r| r.meta().generation == r.meta().generation, // replace with real predicate
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

/// Poll until a **namespace-scoped** resource satisfies `predicate`.
///
/// Delegates to [`wait_for_condition`] with [`Namespaced`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::wait_for_condition_namespaced;
/// use koprs::traits::NamespacedResource;
/// use std::time::Duration;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// let cr = wait_for_condition_namespaced::<MyCR, _>(
///     client,
///     "my-namespace",
///     "my-cr",
///     Duration::from_secs(5),
///     |r| r.meta().generation == r.meta().generation, // replace with real predicate
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn wait_for_condition_namespaced<T, F>(
    client: Client,
    namespace: &str,
    name: &str,
    interval: Duration,
    predicate: F,
) -> Result<T>
where
    T: NamespacedResource,
    F: Fn(&T) -> bool,
{
    wait_for_condition::<T, _, F>(client, Namespaced(namespace), name, interval, predicate).await
}

/// Poll until a **cluster-scoped** resource satisfies `predicate`.
///
/// Delegates to [`wait_for_condition`] with [`Cluster`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::wait_for_condition_cluster;
/// use koprs::traits::ClusterResource;
/// use std::time::Duration;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError> {
/// let cr = wait_for_condition_cluster::<MyCR, _>(
///     client,
///     "my-cr",
///     Duration::from_secs(5),
///     |r| r.meta().generation == r.meta().generation, // replace with real predicate
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn wait_for_condition_cluster<T, F>(
    client: Client,
    name: &str,
    interval: Duration,
    predicate: F,
) -> Result<T>
where
    T: ClusterResource,
    F: Fn(&T) -> bool,
{
    wait_for_condition::<T, _, F>(client, Cluster, name, interval, predicate).await
}

// ---------------------------------------------------------------------------
// Generic public API — patch labels / annotations
// ---------------------------------------------------------------------------

/// Merge labels onto a Kubernetes resource without replacing existing ones.
///
/// Uses a JSON merge patch so only the specified keys are added or updated —
/// other labels on the resource are preserved. Pass an empty slice to no-op.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument. Prefer
/// [`patch_labels_namespaced`] or [`patch_labels_cluster`] for the common cases.
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

/// Merge labels onto a **namespace-scoped** resource.
///
/// Delegates to [`patch_labels`] with [`Namespaced`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::patch_labels_namespaced;
/// use koprs::traits::NamespacedResource;
/// use kube::Client;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// patch_labels_namespaced::<MyCR>(
///     client,
///     "my-ns",
///     "my-cr",
///     &[("app.kubernetes.io/managed-by", "my-operator")],
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_labels_namespaced<T>(
    client: Client,
    namespace: &str,
    name: &str,
    labels: &[(&str, &str)],
) -> Result<T>
where
    T: NamespacedResource,
{
    patch_labels::<T, _>(client, Namespaced(namespace), name, labels).await
}

/// Merge labels onto a **cluster-scoped** resource.
///
/// Delegates to [`patch_labels`] with [`Cluster`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::patch_labels_cluster;
/// use koprs::traits::ClusterResource;
/// use kube::Client;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError> {
/// patch_labels_cluster::<MyCR>(
///     client,
///     "my-cr",
///     &[("app.kubernetes.io/managed-by", "my-operator")],
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_labels_cluster<T>(
    client: Client,
    name: &str,
    labels: &[(&str, &str)],
) -> Result<T>
where
    T: ClusterResource,
{
    patch_labels::<T, _>(client, Cluster, name, labels).await
}

/// Merge annotations onto a Kubernetes resource without replacing existing ones.
///
/// Uses a JSON merge patch so only the specified keys are added or updated —
/// other annotations on the resource are preserved.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument. Prefer
/// [`patch_annotations_namespaced`] or [`patch_annotations_cluster`] for the
/// common cases.
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
///     &[("kubectl.kubernetes.io/last-applied-configuration", "...")],
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

/// Merge annotations onto a **namespace-scoped** resource.
///
/// Delegates to [`patch_annotations`] with [`Namespaced`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::patch_annotations_namespaced;
/// use koprs::traits::NamespacedResource;
/// use kube::Client;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// patch_annotations_namespaced::<MyCR>(
///     client,
///     "my-ns",
///     "my-cr",
///     &[("my-operator/last-synced", "2024-01-01")],
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_annotations_namespaced<T>(
    client: Client,
    namespace: &str,
    name: &str,
    annotations: &[(&str, &str)],
) -> Result<T>
where
    T: NamespacedResource,
{
    patch_annotations::<T, _>(client, Namespaced(namespace), name, annotations).await
}

/// Merge annotations onto a **cluster-scoped** resource.
///
/// Delegates to [`patch_annotations`] with [`Cluster`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::patch_annotations_cluster;
/// use koprs::traits::ClusterResource;
/// use kube::Client;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError> {
/// patch_annotations_cluster::<MyCR>(
///     client,
///     "my-cr",
///     &[("my-operator/last-synced", "2024-01-01")],
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_annotations_cluster<T>(
    client: Client,
    name: &str,
    annotations: &[(&str, &str)],
) -> Result<T>
where
    T: ClusterResource,
{
    patch_annotations::<T, _>(client, Cluster, name, annotations).await
}

// ---------------------------------------------------------------------------
// Generic public API — remove labels / annotations
// ---------------------------------------------------------------------------

/// Remove specific label keys from a Kubernetes resource.
///
/// Uses a JSON merge patch with `null` values — the specified keys are deleted
/// while all other labels are preserved. Pass an empty slice to no-op.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument. Prefer
/// [`remove_labels_namespaced`] or [`remove_labels_cluster`] for the common cases.
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

/// Remove specific label keys from a **namespace-scoped** resource.
///
/// Delegates to [`remove_labels`] with [`Namespaced`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::remove_labels_namespaced;
/// use koprs::traits::NamespacedResource;
/// use kube::Client;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// remove_labels_namespaced::<MyCR>(client, "my-ns", "my-cr", &["stale-label"]).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_labels_namespaced<T>(
    client: Client,
    namespace: &str,
    name: &str,
    keys: &[&str],
) -> Result<T>
where
    T: NamespacedResource,
{
    remove_labels::<T, _>(client, Namespaced(namespace), name, keys).await
}

/// Remove specific label keys from a **cluster-scoped** resource.
///
/// Delegates to [`remove_labels`] with [`Cluster`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::remove_labels_cluster;
/// use koprs::traits::ClusterResource;
/// use kube::Client;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError> {
/// remove_labels_cluster::<MyCR>(client, "my-cr", &["stale-label"]).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_labels_cluster<T>(client: Client, name: &str, keys: &[&str]) -> Result<T>
where
    T: ClusterResource,
{
    remove_labels::<T, _>(client, Cluster, name, keys).await
}

/// Remove specific annotation keys from a Kubernetes resource.
///
/// Uses a JSON merge patch with `null` values — the specified keys are deleted
/// while all other annotations are preserved.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument. Prefer
/// [`remove_annotations_namespaced`] or [`remove_annotations_cluster`] for the
/// common cases.
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
///     &["kubectl.kubernetes.io/last-applied-configuration"],
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

/// Remove specific annotation keys from a **namespace-scoped** resource.
///
/// Delegates to [`remove_annotations`] with [`Namespaced`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::remove_annotations_namespaced;
/// use koprs::traits::NamespacedResource;
/// use kube::Client;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// remove_annotations_namespaced::<MyCR>(client, "my-ns", "my-cr", &["my-operator/last-synced"]).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_annotations_namespaced<T>(
    client: Client,
    namespace: &str,
    name: &str,
    keys: &[&str],
) -> Result<T>
where
    T: NamespacedResource,
{
    remove_annotations::<T, _>(client, Namespaced(namespace), name, keys).await
}

/// Remove specific annotation keys from a **cluster-scoped** resource.
///
/// Delegates to [`remove_annotations`] with [`Cluster`] as the scope.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::resources::remove_annotations_cluster;
/// use koprs::traits::ClusterResource;
/// use kube::Client;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError> {
/// remove_annotations_cluster::<MyCR>(client, "my-cr", &["my-operator/last-synced"]).await?;
/// # Ok(())
/// # }
/// ```
pub async fn remove_annotations_cluster<T>(client: Client, name: &str, keys: &[&str]) -> Result<T>
where
    T: ClusterResource,
{
    remove_annotations::<T, _>(client, Cluster, name, keys).await
}
