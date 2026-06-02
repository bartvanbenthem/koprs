use chrono::Utc;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use kube::api::{Patch, PatchParams};
use kube::{Api, Client};
use serde::Serialize;
use serde_json::json;
use tracing::info;

use crate::error::Result;
use crate::scope::{ApiScope, Cluster, Namespaced};
use crate::traits::{ClusterResource, KubeResource, NamespacedResource};

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
    K: KubeResource,
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
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::scope::Namespaced;
/// use koprs::status::patch_status;
/// use koprs::traits::NamespacedResource;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError>
/// # where MyCR::DynamicType: Default {
/// patch_status::<MyCR, _, _>(
///     client,
///     Namespaced("my-namespace"),
///     "my-cr",
///     MyStatus { ready: true },
///     "my-operator",
/// ).await?;
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
    K: KubeResource,
    K::DynamicType: Default,
    S: Serialize,
    Scope: ApiScope<K>,
{
    let ctx = K::DynamicType::default();
    let kind = K::kind(&ctx);

    // Read the namespace out safely before consuming the scope
    match scope.namespace() {
        Some(ns) => info!(namespace = %ns, %kind, %name, "Patching status"),
        None => info!(%kind, %name, "Patching status"),
    }

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
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::status::patch_status_namespaced;
/// use koprs::traits::NamespacedResource;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError>
/// # where MyCR::DynamicType: Default {
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
    K: NamespacedResource,
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
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::status::patch_status_cluster;
/// use koprs::traits::ClusterResource;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct MyStatus { ready: bool }
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> Result<(), KubeGenericError>
/// # where MyCR::DynamicType: Default {
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
    K: ClusterResource,
    K::DynamicType: Default,
    S: Serialize,
{
    patch_status::<K, S, _>(client, Cluster, name, status, field_manager).await
}

// ---------------------------------------------------------------------------
// Condition helpers — pure
// ---------------------------------------------------------------------------

/// Build a [`Condition`] with `lastTransitionTime` set to now.
///
/// Use [`upsert_condition`] to merge it into an existing conditions `Vec`
/// before calling [`patch_conditions`].
///
/// # Examples
///
/// ```
/// use koprs::status::make_condition;
///
/// let c = make_condition("Ready", "True", "Reconciled", "All good", None);
/// assert_eq!(c.type_, "Ready");
/// assert_eq!(c.status, "True");
/// ```
pub fn make_condition(
    type_: impl Into<String>,
    status: impl Into<String>,
    reason: impl Into<String>,
    message: impl Into<String>,
    observed_generation: Option<i64>,
) -> Condition {
    Condition {
        type_: type_.into(),
        status: status.into(),
        reason: reason.into(),
        message: message.into(),
        observed_generation,
        last_transition_time: Time(Utc::now()),
    }
}

/// Update-or-insert `new` into `conditions` by `type_`.
///
/// - If a condition with the same `type_` already exists **and its `status`
///   has not changed**, `lastTransitionTime` is preserved so the transition
///   clock is not reset unnecessarily.
/// - If the `status` changed, `lastTransitionTime` from `new` is used.
/// - If no matching condition exists, `new` is appended.
///
/// # Examples
///
/// ```
/// use koprs::status::{make_condition, upsert_condition};
///
/// let mut conditions = vec![
///     make_condition("Ready", "False", "Initializing", "Not ready yet", None),
/// ];
///
/// // Status changed: lastTransitionTime will be updated
/// upsert_condition(&mut conditions, make_condition("Ready", "True", "Reconciled", "Done", None));
/// assert_eq!(conditions.len(), 1);
/// assert_eq!(conditions[0].status, "True");
/// ```
pub fn upsert_condition(conditions: &mut Vec<Condition>, new: Condition) {
    if let Some(existing) = conditions.iter_mut().find(|c| c.type_ == new.type_) {
        let last_transition_time = if existing.status == new.status {
            existing.last_transition_time.clone()
        } else {
            new.last_transition_time.clone()
        };
        *existing = new;
        existing.last_transition_time = last_transition_time;
    } else {
        conditions.push(new);
    }
}
