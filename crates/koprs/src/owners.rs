use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;

use crate::error::{KubeGenericError, Result};
use crate::traits::KubeResource;

// ---------------------------------------------------------------------------
// Private core helper
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

// ---------------------------------------------------------------------------
// Public API
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
