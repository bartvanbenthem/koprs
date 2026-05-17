use k8s_openapi::NamespaceResourceScope;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tracing::info;

use crate::error::Result;

/// Patch the status subresource of a cluster-scoped custom resource using Server-Side Apply.
///
/// # Example
/// ```no_run
/// use kube::Client;
/// use kube_genops::status::patch_cluster_status;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example(client: Client, name: &str) -> anyhow::Result<()> {
/// // patch_cluster_status::<MyCR, _>(client, name, MyStatus { ready: true }, "my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_cluster_status<K, S>(
    client: Client,
    name: &str,
    status: S,
    field_manager: &str,
) -> Result<K>
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
    S: Serialize,
{
    let api: Api<K> = Api::all(client);
    let ctx = K::DynamicType::default();
    let kind = K::kind(&ctx);

    info!(%kind, %name, "Patching cluster-scoped status");

    let patch = json!({
        "apiVersion": K::api_version(&ctx),
        "kind": kind,
        "status": status,
    });

    let params = PatchParams::apply(field_manager).force();
    Ok(api.patch_status(name, &params, &Patch::Apply(&patch)).await?)
}

/// Patch the status subresource of a namespaced custom resource using Server-Side Apply.
///
/// # Example
/// ```no_run
/// use kube::Client;
/// use kube_genops::status::patch_namespaced_status;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// // patch_namespaced_status::<MyCR, _>(client, "my-ns", "my-cr", MyStatus { ready: true }, "my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_namespaced_status<K, S>(
    client: Client,
    namespace: &str,
    name: &str,
    status: S,
    field_manager: &str,
) -> Result<K>
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
    S: Serialize,
{
    let api: Api<K> = Api::namespaced(client, namespace);
    let ctx = K::DynamicType::default();
    let kind = K::kind(&ctx);

    info!(%kind, %name, %namespace, "Patching namespaced status");

    let patch = json!({
        "apiVersion": K::api_version(&ctx),
        "kind": kind,
        "status": status,
    });

    let params = PatchParams::apply(field_manager).force();
    Ok(api.patch_status(name, &params, &Patch::Apply(&patch)).await?)
}
