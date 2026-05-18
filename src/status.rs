use k8s_openapi::NamespaceResourceScope;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tracing::info;

use crate::error::Result;

// ---------------------------------------------------------------------------
// Scope types
// ---------------------------------------------------------------------------

/// Marker type for cluster-scoped resources.
pub struct Cluster;

/// Marker type for namespace-scoped resources, carrying the target namespace.
pub struct Namespaced<'a>(pub &'a str);

mod private {
    pub trait Sealed {}
    impl Sealed for super::Cluster {}
    impl Sealed for super::Namespaced<'_> {}
}

/// Sealed trait that constructs the correct [`Api`] for a given scope.
pub trait ApiScope<K>: private::Sealed
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    fn into_api(self, client: Client) -> Api<K>;
}

impl<K> ApiScope<K> for Cluster
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    fn into_api(self, client: Client) -> Api<K> {
        Api::all(client)
    }
}

impl<K> ApiScope<K> for Namespaced<'_>
where
    K: Resource<Scope = NamespaceResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
{
    fn into_api(self, client: Client) -> Api<K> {
        Api::namespaced(client, self.0)
    }
}

// ---------------------------------------------------------------------------
// Private core helper
// ---------------------------------------------------------------------------

async fn apply_status_patch<K, S>(
    api: Api<K>,
    name: &str,
    status: S,
    field_manager: &str,
) -> Result<K>
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
    S: Serialize,
{
    let ctx = K::DynamicType::default();
    let patch = json!({
        "apiVersion": K::api_version(&ctx),
        "kind": K::kind(&ctx),
        "status": status,
    });
    let params = PatchParams::apply(field_manager).force();
    Ok(api.patch_status(name, &params, &Patch::Apply(&patch)).await?)
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Patch the status subresource of a Kubernetes resource using Server-Side Apply.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::status::{patch_status, Cluster, Namespaced};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// // Cluster-scoped
/// // patch_status::<MyCR, _, _>(client.clone(), Cluster, "my-cr", MyStatus { ready: true }, "my-op").await?;
///
/// // Namespace-scoped
/// // patch_status::<MyCR, _, _>(client, Namespaced("my-ns"), "my-cr", MyStatus { ready: true }, "my-op").await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_status<K, S, Scope>(
    client: Client,
    scope: Scope,
    name: &str,
    status: S,
    field_manager: &str,
) -> Result<K>
where
    K: Resource + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
    S: Serialize,
    Scope: ApiScope<K>,
{
    let ctx = K::DynamicType::default();
    let kind = K::kind(&ctx);
    info!(%kind, %name, "Patching status");
    let api = scope.into_api(client);
    apply_status_patch(api, name, status, field_manager).await
}

/// Patch the status subresource of a cluster-scoped custom resource using Server-Side Apply.
///
/// # Example
///
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
    let ctx = K::DynamicType::default();
    let kind = K::kind(&ctx);
    info!(%kind, %name, "Patching cluster-scoped status");
    let api: Api<K> = Api::all(client);
    apply_status_patch(api, name, status, field_manager).await
}

/// Patch the status subresource of a namespaced custom resource using Server-Side Apply.
///
/// # Example
///
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
    let ctx = K::DynamicType::default();
    let kind = K::kind(&ctx);
    info!(%kind, %name, %namespace, "Patching namespaced status");
    let api: Api<K> = Api::namespaced(client, namespace);
    apply_status_patch(api, name, status, field_manager).await
}