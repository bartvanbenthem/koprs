use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, ListParams, ObjectList, Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Api, Client, Resource, ResourceExt};
use serde::Serialize;
use tracing::{error, info};

use crate::error::{KubeGenericError, Result};
use crate::scope::{ApiScope, Cluster, Namespaced};
use crate::traits::{ClusterResource, KubeResource, NamespacedResource};

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
    let name = resource.metadata().name.as_deref().unwrap_or("[unnamed]");
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
    let name = resource.metadata().name.as_deref().unwrap_or("[unnamed]");
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
// Listing
// ---------------------------------------------------------------------------

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
    let api: Api<T> = Api::all(client);
    Ok(api.list(&Default::default()).await?)
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
    let api: Api<T> = Api::all(client);
    let lp = ListParams::default().labels(label_selector);
    Ok(api.list(&lp).await?)
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
    let api: Api<T> = Api::namespaced(client, namespace);
    Ok(api.list(&Default::default()).await?)
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
// ObjectRef helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Private core helpers
// ---------------------------------------------------------------------------

async fn build_object_refs<T>(api: Api<T>) -> Result<Vec<kube_runtime::reflector::ObjectRef<T>>>
where
    T: Resource<DynamicType = ()> + Clone + std::fmt::Debug + serde::de::DeserializeOwned,
{
    let resources = api.list(&Default::default()).await?;
    let mut refs = Vec::new();
    for resource in resources.items {
        let meta = resource.meta();
        let name = meta
            .name
            .clone()
            .ok_or_else(|| KubeGenericError::MissingMetadata("name".into()))?;
        info!(%name, "Building ObjectRef");
        refs.push(kube_runtime::reflector::ObjectRef::new(&name));
    }
    Ok(refs)
}

// ---------------------------------------------------------------------------
// Generic public API
// ---------------------------------------------------------------------------

/// Generate `ObjectRef`s for all instances of a resource type.
///
/// Works for both namespaced and cluster-scoped resources — pass [`Namespaced`]
/// or [`Cluster`] as the `scope` argument to select the correct API surface at
/// compile time.
///
/// Prefer [`make_object_refs_namespaced`] or [`make_object_refs_cluster`] for
/// the common cases — they are thin wrappers around this function.
///
/// Useful for setting up watched relations in `kube-runtime` controllers.
/// See: <https://kube.rs/controllers/relations/#watched-relations>
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::make_object_refs;
/// use koprs::scope::Namespaced;
///
/// # async fn example<MyCR>(client: Client) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: kube::Resource<DynamicType = (), Scope = k8s_openapi::NamespaceResourceScope>
/// #         + Clone
/// #         + std::fmt::Debug
/// #         + serde::Serialize
/// #         + for<'de> serde::Deserialize<'de>
/// #         + k8s_openapi::Metadata<Ty = k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta>
/// #         + Send
/// #         + Sync
/// #         + 'static,
/// # {
/// let refs = make_object_refs::<MyCR, _>(client, Namespaced("my-namespace")).await?;
/// # Ok(())
/// # }
/// ```
pub async fn make_object_refs<T, Scope>(
    client: Client,
    scope: Scope,
) -> Result<Vec<kube_runtime::reflector::ObjectRef<T>>>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    build_object_refs(scope.into_api(client)).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — namespaced
// ---------------------------------------------------------------------------

/// Generate `ObjectRef`s for all instances of a **namespace-scoped** resource type.
///
/// Delegates to [`make_object_refs`] with [`Namespaced`] as the scope. The
/// resource type `T` must implement `Resource<Scope = NamespaceResourceScope>`,
/// which the compiler enforces — passing a cluster-scoped type is a compile
/// error.
///
/// Useful for setting up watched relations in `kube-runtime` controllers.
/// See: <https://kube.rs/controllers/relations/#watched-relations>
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::make_object_refs_namespaced;
///
/// # async fn example<MyCR>(client: Client) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: koprs::traits::NamespacedResource,
/// # {
/// let refs = make_object_refs_namespaced::<MyCR>(client, "my-namespace").await?;
/// # Ok(())
/// # }
/// ```
pub async fn make_object_refs_namespaced<T>(
    client: Client,
    namespace: &str,
) -> Result<Vec<kube_runtime::reflector::ObjectRef<T>>>
where
    T: NamespacedResource,
{
    make_object_refs::<T, _>(client, Namespaced(namespace)).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — cluster-scoped
// ---------------------------------------------------------------------------

/// Generate `ObjectRef`s for all instances of a **cluster-scoped** resource type.
///
/// Delegates to [`make_object_refs`] with [`Cluster`] as the scope. The
/// resource type `T` must implement `Resource<Scope = ClusterResourceScope>`,
/// which the compiler enforces — passing a namespace-scoped type is a compile
/// error.
///
/// Useful for setting up watched relations in `kube-runtime` controllers.
/// See: <https://kube.rs/controllers/relations/#watched-relations>
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::resources::make_object_refs_cluster;
///
/// # async fn example<MyCR>(client: Client) -> Result<(), KubeGenericError>
/// # where
/// #     MyCR: koprs::traits::ClusterResource,
/// # {
/// let refs = make_object_refs_cluster::<MyCR>(client).await?;
/// # Ok(())
/// # }
/// ```
pub async fn make_object_refs_cluster<T>(
    client: Client,
) -> Result<Vec<kube_runtime::reflector::ObjectRef<T>>>
where
    T: ClusterResource,
{
    make_object_refs::<T, _>(client, Cluster).await
}

/// Build a mapper function that returns a fixed set of `ObjectRef`s for any
/// triggering resource. Useful for cross-resource reconcile triggers.
///
/// See: <https://kube.rs/controllers/relations/#watched-relations>
pub fn make_object_ref_mapper<T, CR>(
    refs: std::sync::Arc<Vec<kube_runtime::reflector::ObjectRef<CR>>>,
) -> impl Fn(T) -> Vec<kube_runtime::reflector::ObjectRef<CR>>
where
    CR: Clone + Resource<DynamicType = ()> + 'static,
    T: kube::core::object::HasSpec + 'static,
{
    move |_: T| (*refs).clone()
}

// ---------------------------------------------------------------------------
// Fetch and persist
// ---------------------------------------------------------------------------

/// Fetch all resources of type `T` and write them as JSON to a file on disk.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use koprs::resources::fetch_and_write_to_file;
///
/// # async fn example(client: Client) -> Result<(), KubeGenericError> {
/// // Use _ to let the compiler infer the path type automatically
/// fetch_and_write_to_file::<Pod, _>(client, "/tmp", "pods.json").await?;
/// # Ok(())
/// # }
/// ```
pub async fn fetch_and_write_to_file<T, P>(client: Client, path: P, file_name: &str) -> Result<()>
where
    T: KubeResource,
    P: AsRef<Path>,
{
    let file_path = path.as_ref().join(file_name);
    let list = list_resources::<T>(client).await?;
    write_json_to_file(&list.items, &file_path).await
}

async fn write_json_to_file<T, P>(items: &[T], path: P) -> Result<()>
where
    T: Serialize,
    P: AsRef<Path>,
{
    let json = serde_json::to_string_pretty(items)?;
    tokio::fs::write(path.as_ref(), json).await?;

    Ok(())
}
