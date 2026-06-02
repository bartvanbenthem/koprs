// src/reconciler.rs
//
// Core reconcile logic for the ConfigMapSync operator.
//
// Per reconcile loop iteration the operator:
//   1. Checks whether the CR is being deleted (finalizer present + deletionTimestamp set).
//   2. On deletion  → removes the synced ConfigMap, then strips the finalizer.
//   3. On creation  → ensures the finalizer is present, applies the desired
//                     ConfigMap in the target namespace, and patches status.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::SecondsFormat;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::runtime::controller::Action;
use kube::{Client, ResourceExt};
use tokio::time::Duration;
use tracing::{error, info, warn};

use koprs::error::KubeGenericError;
use koprs::finalizers::{add_finalizer_namespaced, remove_finalizers_namespaced};
use koprs::gc::gc_namespaced_resources;
use koprs::resources::{
    apply_namespaced_resource, delete_namespaced_resource, patch_labels_namespaced,
};
use koprs::status::{make_condition, patch_conditions_namespaced, patch_status_namespaced};

use crate::types::{ConfigMapSync, ConfigMapSyncStatus, SyncCondition};

const FINALIZER: &str = "configmapsync.example.io/cleanup";
const FIELD_MANAGER: &str = "configmapsync-operator";
const MANAGED_LABEL: &str = "app.kubernetes.io/managed-by=configmapsync-operator";

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

pub struct Context {
    pub client: Client,
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("koprs error: {0}")]
    Koprs(#[from] KubeGenericError),

    #[error("missing field: {0}")]
    MissingField(&'static str),
}

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

pub async fn reconcile(
    cr: Arc<ConfigMapSync>,
    ctx: Arc<Context>,
) -> Result<Action, ReconcileError> {
    let client = ctx.client.clone();
    let name = cr.name_any();
    let namespace = cr
        .namespace()
        .ok_or(ReconcileError::MissingField("namespace"))?;

    info!(cr = %name, ns = %namespace, "reconciling ConfigMapSync");

    // -----------------------------------------------------------------------
    // Deletion path
    // -----------------------------------------------------------------------
    if cr.metadata.deletion_timestamp.is_some() {
        info!(cr = %name, "deletion timestamp set — running cleanup");

        let target_ns = &cr.spec.target_namespace;
        let cm_name = configmap_name(&name);

        match delete_namespaced_resource::<ConfigMap>(client.clone(), target_ns, &cm_name).await {
            Ok(true) => info!(cm = %cm_name, ns = %target_ns, "deleted synced ConfigMap"),
            Ok(false) => info!(cm = %cm_name, "ConfigMap was already gone"),
            Err(e) => {
                error!(error = %e, "failed to delete ConfigMap during cleanup");
                return Err(e.into());
            }
        }

        remove_finalizers_namespaced::<ConfigMapSync>(client.clone(), &namespace, &name).await?;
        info!(cr = %name, "finalizer removed — deletion complete");
        return Ok(Action::await_change());
    }

    // -----------------------------------------------------------------------
    // Normal reconcile path
    // -----------------------------------------------------------------------

    // 1. Ensure finalizer is present.
    add_finalizer_namespaced::<ConfigMapSync>(client.clone(), &namespace, &name, FINALIZER).await?;

    // 2. Build and apply the desired ConfigMap via koprs SSA.
    let target_ns = &cr.spec.target_namespace;
    let cm_name = configmap_name(&name);
    let desired_cm = build_configmap(&cm_name, target_ns, &name, &cr.spec.data);

    apply_namespaced_resource::<ConfigMap>(client.clone(), target_ns, &desired_cm, FIELD_MANAGER)
        .await?;
    info!(cm = %cm_name, ns = %target_ns, "applied ConfigMap");

    // 3. Garbage-collect stale ConfigMaps previously owned by this CR.
    gc_namespaced_resources::<ConfigMap>(client.clone(), target_ns, MANAGED_LABEL, |cm| {
        cm.name_any() == cm_name
    })
    .await?;

    // 4. Stamp the target namespace as a label on the CR so it is visible
    //    without fetching the full spec.
    patch_labels_namespaced::<ConfigMapSync>(
        client.clone(),
        &namespace,
        &name,
        &[("configmapsync.example.io/synced-to", target_ns)],
    )
    .await?;

    // 5. Patch the standard conditions array with a Ready=True condition.
    //    We use koprs::status::make_condition to get the correct timestamp,
    //    then convert to SyncCondition which is declared in the CRD schema.
    let generation = cr.metadata.generation;
    let koprs_condition = make_condition(
        "Ready",
        "True",
        "ConfigMapSynced",
        &format!("ConfigMap '{cm_name}' synced to namespace '{target_ns}'"),
        generation,
    );
    let conditions = vec![SyncCondition {
        type_: koprs_condition.type_,
        status: koprs_condition.status,
        reason: koprs_condition.reason,
        message: koprs_condition.message,
        last_transition_time: koprs_condition
            .last_transition_time
            .0
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        observed_generation: koprs_condition.observed_generation,
    }];
    patch_conditions_namespaced::<ConfigMapSync, SyncCondition>(
        client.clone(),
        &namespace,
        &name,
        conditions,
        FIELD_MANAGER,
    )
    .await?;

    // 6. Patch the typed status (drives the READY printer column).
    patch_status_namespaced::<ConfigMapSync, ConfigMapSyncStatus>(
        client.clone(),
        &namespace,
        &name,
        ConfigMapSyncStatus {
            ready: true,
            message: format!("ConfigMap '{cm_name}' synced to namespace '{target_ns}'"),
            conditions: vec![],
        },
        FIELD_MANAGER,
    )
    .await?;

    info!(cr = %name, "reconcile complete");
    Ok(Action::requeue(Duration::from_secs(300)))
}

// ---------------------------------------------------------------------------
// Error handler
// ---------------------------------------------------------------------------

pub fn error_policy(cr: Arc<ConfigMapSync>, error: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(cr = %cr.name_any(), error = %error, "reconcile failed — retrying in 30s");
    Action::requeue(Duration::from_secs(30))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn configmap_name(cr_name: &str) -> String {
    format!("cms-{cr_name}")
}

fn build_configmap(
    name: &str,
    namespace: &str,
    owner_cr: &str,
    data: &BTreeMap<String, String>,
) -> ConfigMap {
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some(BTreeMap::from([
                (
                    "app.kubernetes.io/managed-by".to_string(),
                    "configmapsync-operator".to_string(),
                ),
                (
                    "configmapsync.example.io/owner".to_string(),
                    owner_cr.to_string(),
                ),
            ])),
            ..Default::default()
        },
        data: Some(data.clone()),
        ..Default::default()
    }
}
