use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;

use k8s_openapi::{ClusterResourceScope, Metadata, NamespaceResourceScope};
use k8s_openapi::api::core::v1::Namespace;
use kube::api::{DeleteParams, ObjectList, Patch, PatchParams};
use kube::{Api, Client, Resource, ResourceExt};
use kube::core::ObjectMeta;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::info;

use crate::error::{KubeGenericError, Result};

// --- Namespace ---

/// Ensure a namespace exists, creating or updating it via Server-Side Apply.
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

// --- Cluster-scoped ---

/// Apply (create or update) a cluster-scoped resource using Server-Side Apply.
pub async fn apply_cluster_resource<T>(
    client: Client,
    resource: &T,
    field_manager: &str,
) -> Result<T>
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
    let name = resource.metadata().name.as_deref().unwrap_or("[unnamed]");
    let kind = T::kind(&());

    info!(%kind, %name, "Applying cluster-scoped resource");

    let params = PatchParams::apply(field_manager).force();
    Ok(api.patch(name, &params, &Patch::Apply(resource)).await?)
}

/// Delete a cluster-scoped resource by name.
/// Returns `Ok(false)` if the resource did not exist.
pub async fn delete_cluster_resource<T>(client: Client, name: &str) -> Result<bool>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = ClusterResourceScope>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let api: Api<T> = Api::all(client);
    let kind = T::kind(&());

    info!(%kind, %name, "Deleting cluster-scoped resource");

    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(false),
        Err(e) => Err(e.into()),
    }
}

// --- Namespaced ---

/// Apply (create or update) a namespaced resource using Server-Side Apply.
pub async fn apply_namespaced_resource<T>(
    client: Client,
    namespace: &str,
    resource: &T,
    field_manager: &str,
) -> Result<T>
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
    let api: Api<T> = Api::namespaced(client, namespace);
    let name = resource.metadata().name.as_deref().unwrap_or("[unnamed]");
    let kind = T::kind(&());

    info!(%kind, %name, %namespace, "Applying namespaced resource");

    let params = PatchParams::apply(field_manager).force();
    Ok(api.patch(name, &params, &Patch::Apply(resource)).await?)
}

/// Delete a namespaced resource by name.
/// Returns `Ok(false)` if the resource did not exist.
pub async fn delete_namespaced_resource<T>(
    client: Client,
    namespace: &str,
    name: &str,
) -> Result<bool>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let api: Api<T> = Api::namespaced(client, namespace);
    let kind = T::kind(&());

    info!(%kind, %name, %namespace, "Deleting namespaced resource");

    match api.delete(name, &DeleteParams::default()).await {
        Ok(_) => Ok(true),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(false),
        Err(e) => Err(e.into()),
    }
}

// --- Listing ---

/// List all resources of type `T` across all namespaces.
pub async fn list_resources<T>(client: Client) -> Result<ObjectList<T>>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let api: Api<T> = Api::all(client);
    Ok(api.list(&Default::default()).await?)
}

/// List all resources of type `T` matching a label selector.
pub async fn list_resources_by_label<T>(
    client: Client,
    label_selector: &str,
) -> Result<ObjectList<T>>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let api: Api<T> = Api::all(client);
    let lp = kube::api::ListParams::default().labels(label_selector);
    Ok(api.list(&lp).await?)
}

/// List all resources of type `T` in a specific namespace.
pub async fn list_namespaced_resources<T>(
    client: Client,
    namespace: &str,
) -> Result<ObjectList<T>>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let api: Api<T> = Api::namespaced(client, namespace);
    Ok(api.list(&Default::default()).await?)
}

// --- Fetch and persist ---

/// Fetch all resources of type `T` and write them as JSON to a file on disk.
pub async fn fetch_and_write_to_file<T>(
    client: Client,
    path: &str,
    file_name: &str,
) -> Result<()>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Serialize
        + Send
        + Sync
        + 'static,
{
    let file_path = Path::new(path).join(file_name);
    let file_str = file_path
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid UTF-8 in file path"))?;

    let list = list_resources::<T>(client).await?;
    write_json_to_file(&list.items, file_str).await?;
    Ok(())
}

async fn write_json_to_file<T: Serialize>(items: &[T], path: &str) -> Result<()> {
    let json = serde_json::to_string_pretty(items)?;
    tokio::fs::write(path, json)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to write file: {}", e))?;
    Ok(())
}

// --- ObjectRef helpers ---

/// Generate `ObjectRef`s for all instances of a namespaced resource type.
/// Useful for setting up watched relations in `kube-runtime` controllers.
/// See: <https://kube.rs/controllers/relations/#watched-relations>
pub async fn make_object_refs<T>(
    client: Client,
    namespace: Option<&str>,
) -> Result<Vec<kube_runtime::reflector::ObjectRef<T>>>
where
    T: Clone
        + Debug
        + Resource<DynamicType = (), Scope = NamespaceResourceScope>
        + DeserializeOwned
        + Serialize
        + Send
        + Sync
        + 'static,
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

/// List the names of all resources of type `T` matching a label selector,
/// returned as a `HashSet<String>`. Useful for garbage collection diffing.
pub async fn list_resource_names<T>(
    client: Client,
    label_selector: &str,
) -> Result<HashSet<String>>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let list = list_resources_by_label::<T>(client, label_selector).await?;
    Ok(list.items.iter().map(|r| r.name_any()).collect())
}
