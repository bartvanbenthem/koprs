use std::collections::HashSet;
use std::path::Path;

use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, ListParams, ObjectList, Patch, PatchParams};
use kube::core::ObjectMeta;
use kube::{Api, Client, Resource, ResourceExt};
use serde::Serialize;
use tracing::info;

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
/// use kube::Client;
/// use kube_genops::resources::ensure_namespace;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
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
    let kind = T::kind(&());
    info!(%kind, %name, "Applying resource");
    let params = PatchParams::apply(field_manager).force();
    Ok(api.patch(name, &params, &Patch::Apply(resource)).await?)
}

async fn delete_resource_inner<T>(api: Api<T>, name: &str) -> Result<bool>
where
    T: KubeResource,
{
    let kind = T::kind(&());
    info!(%kind, %name, "Deleting resource");
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
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time. Prefer [`apply_cluster_resource`] or
/// [`apply_namespaced_resource`] for the common cases — they are thin wrappers
/// around this function.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::{NamespaceResourceScope, Metadata};
/// use kube::core::ObjectMeta;
/// use kube_genops::resources::apply_resource;
/// use kube_genops::scope::Namespaced;
/// use serde::{Deserialize, Serialize};
///
/// # async fn example<MyCR>(client: Client, resource: MyCR) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Metadata<Ty = ObjectMeta>
/// #         + Clone + std::fmt::Debug + Default + Send + Sync
/// #         + Serialize + for<'de> Deserialize<'de> + 'static,
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
    apply_resource_inner(scope.into_api(client), resource, field_manager).await
}

// ---------------------------------------------------------------------------
// Generic public API — delete
// ---------------------------------------------------------------------------

/// Delete a Kubernetes resource by name.
///
/// Returns `Ok(false)` if the resource did not exist.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time. Prefer [`delete_cluster_resource`] or
/// [`delete_namespaced_resource`] for the common cases — they are thin wrappers
/// around this function.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::{NamespaceResourceScope, Metadata};
/// use kube::core::ObjectMeta;
/// use kube_genops::resources::delete_resource;
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
    delete_resource_inner(scope.into_api(client), name).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — cluster-scoped
// ---------------------------------------------------------------------------

/// Apply (create or update) a **cluster-scoped** resource using Server-Side Apply.
///
/// Delegates to [`apply_resource`] with [`Cluster`] as the scope. The resource
/// type `T` must implement `Resource<Scope = ClusterResourceScope>`, which the
/// compiler enforces — passing a namespace-scoped type is a compile error.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::{ClusterResourceScope, Metadata};
/// use kube::core::ObjectMeta;
/// use kube_genops::resources::apply_cluster_resource;
/// use serde::{Deserialize, Serialize};
///
/// # async fn example<MyCR>(client: Client, resource: MyCR) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = ClusterResourceScope>
/// #         + Metadata<Ty = ObjectMeta>
/// #         + Clone + std::fmt::Debug + Default + Send + Sync
/// #         + Serialize + for<'de> Deserialize<'de> + 'static,
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

/// Delete a **cluster-scoped** resource by name.
///
/// Delegates to [`delete_resource`] with [`Cluster`] as the scope. Returns
/// `Ok(false)` if the resource did not exist. The resource type `T` must
/// implement `Resource<Scope = ClusterResourceScope>`.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::ClusterResourceScope;
/// use kube_genops::resources::delete_cluster_resource;
/// use serde::Deserialize;
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = ClusterResourceScope>
/// #         + Clone + std::fmt::Debug + Send + Sync
/// #         + for<'de> Deserialize<'de> + 'static,
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

/// Apply (create or update) a **namespace-scoped** resource using Server-Side Apply.
///
/// Delegates to [`apply_resource`] with [`Namespaced`] as the scope. The
/// resource type `T` must implement `Resource<Scope = NamespaceResourceScope>`,
/// which the compiler enforces — passing a cluster-scoped type is a compile
/// error.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::{NamespaceResourceScope, Metadata};
/// use kube::core::ObjectMeta;
/// use kube_genops::resources::apply_namespaced_resource;
/// use serde::{Deserialize, Serialize};
///
/// # async fn example<MyCR>(client: Client, resource: MyCR) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Metadata<Ty = ObjectMeta>
/// #         + Clone + std::fmt::Debug + Default + Send + Sync
/// #         + Serialize + for<'de> Deserialize<'de> + 'static,
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

/// Delete a **namespace-scoped** resource by name.
///
/// Delegates to [`delete_resource`] with [`Namespaced`] as the scope. Returns
/// `Ok(false)` if the resource did not exist. The resource type `T` must
/// implement `Resource<Scope = NamespaceResourceScope>`.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::NamespaceResourceScope;
/// use kube_genops::resources::delete_namespaced_resource;
/// use serde::Deserialize;
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Clone + std::fmt::Debug + Send + Sync
/// #         + for<'de> Deserialize<'de> + 'static,
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
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use kube_genops::resources::list_resources;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
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
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use kube_genops::resources::list_resources_by_label;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
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
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use kube_genops::resources::list_namespaced_resources;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
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
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use kube_genops::resources::list_resource_names;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
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
// Fetch and persist
// ---------------------------------------------------------------------------

/// Fetch all resources of type `T` and write them as JSON to a file on disk.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use k8s_openapi::api::core::v1::Pod;
/// use kube_genops::resources::fetch_and_write_to_file;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// fetch_and_write_to_file::<Pod>(client, "/tmp", "pods.json").await?;
/// # Ok(())
/// # }
/// ```
pub async fn fetch_and_write_to_file<T>(client: Client, path: &str, file_name: &str) -> Result<()>
where
    T: KubeResource,
{
    let file_path = Path::new(path).join(file_name);
    let file_str = file_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in file path"))?;
    let list = list_resources::<T>(client).await?;
    write_json_to_file(&list.items, file_str).await
}

async fn write_json_to_file<T: Serialize>(items: &[T], path: &str) -> Result<()> {
    let json = serde_json::to_string_pretty(items)?;
    tokio::fs::write(path, json)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to write file: {}", e))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ObjectRef helpers
// ---------------------------------------------------------------------------

/// Generate `ObjectRef`s for all instances of a namespaced resource type.
///
/// Useful for setting up watched relations in `kube-runtime` controllers.
/// See: <https://kube.rs/controllers/relations/#watched-relations>
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::NamespaceResourceScope;
/// use kube_genops::resources::make_object_refs;
/// use serde::{Deserialize, Serialize};
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<DynamicType = (), Scope = NamespaceResourceScope>
/// #         + Clone + std::fmt::Debug + Send + Sync
/// #         + Serialize + for<'de> Deserialize<'de> + 'static,
/// # {
/// let refs = make_object_refs::<MyCR>(client, Some("my-namespace")).await?;
/// # Ok(())
/// # }
/// ```
pub async fn make_object_refs<T>(
    client: Client,
    namespace: Option<&str>,
) -> Result<Vec<kube_runtime::reflector::ObjectRef<T>>>
where
    T: NamespacedResource,
{
    let api: Api<T> = match namespace {
        Some(ns) => Api::namespaced(client, ns),
        None => Api::all(client),
    };

    let resources = api.list(&Default::default()).await?;
    let mut refs = Vec::new();

    for resource in resources.items {
        let meta = resource.meta();
        let name = meta
            .name
            .clone()
            .ok_or_else(|| KubeGenericError::MissingMetadata("name".into()))?;
        let ns = meta
            .namespace
            .clone()
            .unwrap_or_else(|| "default".to_string());

        info!(%name, %ns, "Building ObjectRef");
        refs.push(kube_runtime::reflector::ObjectRef::new(&name).within(&ns));
    }

    Ok(refs)
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
