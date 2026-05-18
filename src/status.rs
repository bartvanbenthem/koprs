use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use tracing::info;

use crate::error::Result;
use crate::scope::ApiScope;

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