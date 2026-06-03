//! Resource relationship helpers.
//! Covers two kinds of relationships between Kubernetes resources:
//!
//! - **Ownership** — [`owner_ref`], [`controller_ref`], and [`set_owner_refs`]
//!   build and attach `metadata.ownerReferences` so Kubernetes can garbage
//!   collect child resources when their owner is deleted.
//!
//! - **Controller wiring** — [`make_object_refs`], [`make_object_ref_mapper`],
//!   and [`owner_label_mapper`] build [`ObjectRef`] sets and mapper closures
//!   for cross-resource reconcile triggers in `kube-runtime` controllers.
//!
//! See: <https://kube.rs/controllers/relations/#watched-relations>

use std::sync::Arc;

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{Api, Client, Resource, ResourceExt};
use kube_runtime::reflector::ObjectRef;
use tracing::info;

use crate::error::{KubeGenericError, Result};
use crate::scope::ApiScope;
use crate::traits::KubeResource;

// ---------------------------------------------------------------------------
// Private core helpers
// ---------------------------------------------------------------------------

fn build_owner_ref<T: KubeResource>(owner: &T, controller: bool) -> Result<OwnerReference> {
    let meta = owner.meta();
    let name = meta
        .name
        .clone()
        .ok_or_else(|| KubeGenericError::MissingMetadata("name".into()))?;
    let uid = meta
        .uid
        .clone()
        .ok_or_else(|| KubeGenericError::MissingMetadata("uid".into()))?;

    Ok(OwnerReference {
        api_version: T::api_version(&()).to_string(),
        kind: T::kind(&()).to_string(),
        name,
        uid,
        controller: Some(controller),
        block_owner_deletion: Some(controller),
    })
}

async fn build_object_refs<T>(api: Api<T>) -> Result<Vec<ObjectRef<T>>>
where
    T: KubeResource,
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
        refs.push(ObjectRef::new(&name));
    }
    Ok(refs)
}

// ---------------------------------------------------------------------------
// Ownership — public API
// ---------------------------------------------------------------------------

/// Build a non-controller `OwnerReference` pointing at `owner`.
///
/// The resulting reference has `controller: false` and
/// `block_owner_deletion: false`. Use [`controller_ref`] when the resource
/// is the sole managing owner and should block deletion.
///
/// Returns [`KubeGenericError::MissingMetadata`] if `name` or `uid` is absent
/// from the owner's metadata (both are required by the Kubernetes API).
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::owners::owner_ref;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// # fn example(parent: &ConfigMap) -> Result<(), KubeGenericError> {
/// let oref = owner_ref(parent)?;
/// # Ok(())
/// # }
/// ```
pub fn owner_ref<T: KubeResource>(owner: &T) -> Result<OwnerReference> {
    build_owner_ref(owner, false)
}

/// Build a controller `OwnerReference` pointing at `owner`.
///
/// Sets `controller: true` and `block_owner_deletion: true`. Use this when
/// `owner` is the single controlling owner of a child resource — Kubernetes
/// enforces that at most one owner has `controller: true`.
///
/// Returns [`KubeGenericError::MissingMetadata`] if `name` or `uid` is absent
/// from the owner's metadata.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::owners::controller_ref;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// # fn example(parent: &ConfigMap) -> Result<(), KubeGenericError> {
/// let oref = controller_ref(parent)?;
/// # Ok(())
/// # }
/// ```
pub fn controller_ref<T: KubeResource>(owner: &T) -> Result<OwnerReference> {
    build_owner_ref(owner, true)
}

/// Overwrite `metadata.ownerReferences` on `child` with `refs`.
///
/// Replaces any existing owner references. To add a single owner reference
/// produced by [`owner_ref`] or [`controller_ref`], pass a `vec![oref]`.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use koprs::owners::{controller_ref, set_owner_refs};
/// use k8s_openapi::api::apps::v1::Deployment;
/// use k8s_openapi::api::core::v1::ConfigMap;
///
/// # fn example(parent: &ConfigMap, child: &mut Deployment) -> Result<(), KubeGenericError> {
/// let oref = controller_ref(parent)?;
/// set_owner_refs(child, vec![oref]);
/// # Ok(())
/// # }
/// ```
pub fn set_owner_refs<T: KubeResource>(child: &mut T, refs: Vec<OwnerReference>) {
    child.meta_mut().owner_references = Some(refs);
}

// ---------------------------------------------------------------------------
// Controller wiring — generic public API
// ---------------------------------------------------------------------------

/// Generate [`ObjectRef`]s for all live instances of a resource type.
///
/// Queries the Kubernetes API and returns one `ObjectRef` per resource found.
/// Pass [`Cluster`][crate::scope::Cluster] or [`Namespaced`][crate::scope::Namespaced]
/// as the `scope` argument to select the correct API surface at compile time.
///
/// Useful for setting up watched relations in `kube-runtime` controllers.
/// See: <https://kube.rs/controllers/relations/#watched-relations>
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::owners::make_object_refs;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// let refs = make_object_refs::<MyCR, _>(client, Namespaced("my-namespace")).await?;
/// # Ok(())
/// # }
/// ```
pub async fn make_object_refs<T, Scope>(client: Client, scope: Scope) -> Result<Vec<ObjectRef<T>>>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    build_object_refs(scope.into_api(client)).await
}

/// Build a mapper closure that finds a CR owner from a label on a trigger resource.
///
/// The most common cross-resource watch pattern: the trigger resource carries
/// a label whose *value* is the name of the CR to re-queue. The trigger
/// resource's namespace is used as the CR's namespace.
///
/// Returns `None` (drops the event) when the label is absent or either name
/// or namespace is empty.
///
/// Pass the returned closure to [`ControllerBuilder::watch`][crate::controller::ControllerBuilder::watch]
/// or directly to `kube_runtime::Controller::watches`.
///
/// # Examples
///
/// ```no_run
/// use k8s_openapi::api::core::v1::ConfigMap;
/// use koprs::owners::owner_label_mapper;
/// use koprs::controller::{ControllerBuilder, watcher};
///
/// # type MyCR = ConfigMap;
/// # async fn example(api: kube::Api<MyCR>, cm_api: kube::Api<ConfigMap>) {
/// // owner_label_mapper returns a Fn(ConfigMap) -> Option<ObjectRef<MyCR>>
/// // ready to pass directly to .watch()
/// let _mapper = owner_label_mapper::<ConfigMap, MyCR>("my-operator/owner");
/// # }
/// ```
pub fn owner_label_mapper<Trigger, CR>(
    label: impl Into<String>,
) -> impl Fn(Trigger) -> Option<ObjectRef<CR>>
where
    Trigger: KubeResource,
    CR: KubeResource,
{
    let label = label.into();
    move |resource: Trigger| {
        let owner = resource.labels().get(&label).cloned()?;
        let ns = resource.namespace()?;
        if owner.is_empty() {
            return None;
        }
        Some(ObjectRef::<CR>::new(&owner).within(&ns))
    }
}

/// Build a mapper closure that returns a fixed set of [`ObjectRef`]s for any
/// triggering resource `T`.
///
/// The returned closure ignores the concrete value of `T` and always yields a
/// clone of `refs`. Pass it to `kube_runtime::Controller::watches` to trigger
/// reconciliation of a set of CRs whenever any `T` changes.
///
/// The triggering type `T` is unconstrained beyond `'static` — any resource
/// type (including those without a `.spec`) may be used as a trigger.
///
/// See: <https://kube.rs/controllers/relations/#watched-relations>
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::owners::{make_object_refs, make_object_ref_mapper};
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use k8s_openapi::api::core::v1::ConfigMap;
/// use std::sync::Arc;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// let refs = make_object_refs::<MyCR, _>(client, Namespaced("my-namespace")).await?;
/// let mapper = make_object_ref_mapper::<ConfigMap, MyCR>(Arc::new(refs));
/// # Ok(())
/// # }
/// ```
pub fn make_object_ref_mapper<T, CR>(
    refs: Arc<Vec<ObjectRef<CR>>>,
) -> impl Fn(T) -> Vec<ObjectRef<CR>>
where
    CR: Clone + Resource<DynamicType = ()> + 'static,
    T: 'static,
{
    move |_: T| (*refs).clone()
}
