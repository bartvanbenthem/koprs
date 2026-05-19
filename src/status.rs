use crate::error::Result;
use crate::scope::{ApiScope, Cluster, Namespaced};
use k8s_openapi::{ClusterResourceScope, NamespaceResourceScope};
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use tracing::info;

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
    Ok(api
        .patch_status(name, &params, &Patch::Apply(&patch))
        .await?)
}

// ---------------------------------------------------------------------------
// Generic public API
// ---------------------------------------------------------------------------

/// Patch the status subresource of a Kubernetes resource using Server-Side Apply.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time.
///
/// Prefer [`patch_status_namespaced`] or [`patch_status_cluster`] for the
/// common cases — this generic form is available when the scope is determined
/// dynamically or passed through from a caller.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::scope::{Cluster, Namespaced};
/// use kube_genops::status::patch_status;
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
    apply_status_patch(scope.into_api(client), name, status, field_manager).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers
// ---------------------------------------------------------------------------

/// Patch the status subresource of a **namespace-scoped** Kubernetes resource
/// using Server-Side Apply.
///
/// This is a convenience wrapper around [`patch_status`] that fixes the scope
/// to [`Namespaced`], so callers don't need to import or spell out the scope
/// type themselves.
///
/// # Arguments
///
/// * `client`        – A cloned [`kube::Client`].
/// * `namespace`     – The namespace that owns the resource.
/// * `name`          – The name of the resource to patch.
/// * `status`        – Any serialisable value that represents the desired
///                     `.status` subresource body.
/// * `field_manager` – The field-manager string used for Server-Side Apply
///                     (typically your operator's name, e.g. `"my-operator"`).
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::NamespaceResourceScope;
/// use kube_genops::status::patch_status_namespaced;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<Scope = NamespaceResourceScope> + Clone + Serialize
/// #         + for<'de> Deserialize<'de> + 'static,
/// #     MyCR::DynamicType: Default,
/// # {
/// patch_status_namespaced::<MyCR, _>(
///     client,
///     "my-namespace",
///     "my-cr",
///     MyStatus { ready: true },
///     "my-operator",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_status_namespaced<K, S>(
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
    patch_status::<K, S, _>(client, Namespaced(namespace), name, status, field_manager).await
}

/// Patch the status subresource of a **cluster-scoped** Kubernetes resource
/// using Server-Side Apply.
///
/// This is a convenience wrapper around [`patch_status`] that fixes the scope
/// to [`Cluster`], so callers don't need to import or spell out the scope
/// type themselves.
///
/// # Arguments
///
/// * `client`        – A cloned [`kube::Client`].
/// * `name`          – The name of the resource to patch.
/// * `status`        – Any serialisable value that represents the desired
///                     `.status` subresource body.
/// * `field_manager` – The field-manager string used for Server-Side Apply
///                     (typically your operator's name, e.g. `"my-operator"`).
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube::Resource;
/// use k8s_openapi::ClusterResourceScope;
/// use kube_genops::status::patch_status_cluster;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Serialize, Deserialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example<MyCR>(client: Client) -> anyhow::Result<()>
/// # where
/// #     MyCR: Resource<Scope = ClusterResourceScope> + Clone + Serialize
/// #         + for<'de> Deserialize<'de> + 'static,
/// #     MyCR::DynamicType: Default,
/// # {
/// patch_status_cluster::<MyCR, _>(
///     client,
///     "my-cr",
///     MyStatus { ready: true },
///     "my-operator",
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn patch_status_cluster<K, S>(
    client: Client,
    name: &str,
    status: S,
    field_manager: &str,
) -> Result<K>
where
    K: Resource<Scope = ClusterResourceScope> + Clone + DeserializeOwned + Serialize + 'static,
    K::DynamicType: Default,
    S: Serialize,
{
    patch_status::<K, S, _>(client, Cluster, name, status, field_manager).await
}
