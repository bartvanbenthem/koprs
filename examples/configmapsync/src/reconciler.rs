// src/reconciler.rs

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::ResourceExt;
use tokio::time::Duration;
use tracing::{error, info, warn};

use koprs::controller::{Action, Context, Reconciler};
use koprs::error::KubeGenericError;
use koprs::events::{EventType, record_event};
use koprs::finalizers::{add_finalizer_namespaced, remove_finalizers};
use koprs::gc::gc_resources;
use koprs::is_being_deleted;
use koprs::meta::ObjectMetaBuilder;
use koprs::resources::{EnsureOutcome, delete_resource, ensure_resource, patch_labels};
use koprs::scope::Namespaced;
use koprs::status::{make_condition, patch_status_namespaced, upsert_condition};

use crate::types::{ConfigMapSync, ConfigMapSyncStatus};

const FINALIZER: &str = "configmapsync.example.io/cleanup";
const FIELD_MANAGER: &str = "configmapsync-operator";
const MANAGED_LABEL: &str = "app.kubernetes.io/managed-by=configmapsync-operator";

// ---------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------

pub struct ConfigMapSyncReconciler;

impl Reconciler<ConfigMapSync> for ConfigMapSyncReconciler {
    type Error = KubeGenericError;

    async fn reconcile(
        &self,
        cr: Arc<ConfigMapSync>,
        ctx: Arc<Context>,
    ) -> Result<Action, KubeGenericError> {
        let client = ctx.client.clone();
        let name = cr.name_any();
        let namespace = cr
            .namespace()
            .ok_or(KubeGenericError::MissingMetadata("namespace".into()))?;

        info!(cr = %name, ns = %namespace, "reconciling ConfigMapSync");

        // -------------------------------------------------------------------
        // Deletion path
        // -------------------------------------------------------------------
        if is_being_deleted(&*cr) {
            info!(cr = %name, "deletion timestamp set — running cleanup");

            let target_ns = &cr.spec.target_namespace;
            let cm_name = configmap_name(&name);

            match delete_resource::<ConfigMap, _>(client.clone(), Namespaced(target_ns), &cm_name)
                .await
            {
                Ok(true) => info!(cm = %cm_name, ns = %target_ns, "deleted synced ConfigMap"),
                Ok(false) => info!(cm = %cm_name, "ConfigMap was already gone"),
                Err(e) => {
                    error!(error = %e, "failed to delete ConfigMap during cleanup");
                    return Err(e.into());
                }
            }

            remove_finalizers::<ConfigMapSync, _>(client.clone(), Namespaced(&namespace), &name)
                .await?;
            info!(cr = %name, "finalizer removed — deletion complete");
            return Ok(Action::await_change());
        }

        // -------------------------------------------------------------------
        // Normal reconcile path
        // -------------------------------------------------------------------

        // 1. Ensure finalizer is present.
        add_finalizer_namespaced::<ConfigMapSync>(client.clone(), &cr, FINALIZER).await?;

        // 2. Build and ensure the desired ConfigMap.
        let target_ns = &cr.spec.target_namespace;
        let cm_name = configmap_name(&name);
        let desired_cm = build_configmap(&cm_name, target_ns, &name, &cr.spec.data);

        let outcome = ensure_resource::<ConfigMap, _>(
            client.clone(),
            Namespaced(target_ns),
            &desired_cm,
            FIELD_MANAGER,
        )
        .await?;
        info!(cm = %cm_name, ns = %target_ns, "applied ConfigMap");

        if outcome.was_changed() {
            let (reason, note) = match &outcome {
                EnsureOutcome::Created(_) => (
                    "ConfigMapCreated",
                    format!("ConfigMap '{cm_name}' created in namespace '{target_ns}'"),
                ),
                EnsureOutcome::Updated(_) => (
                    "ConfigMapDriftCorrected",
                    format!("ConfigMap '{cm_name}' corrected in namespace '{target_ns}'"),
                ),
                EnsureOutcome::Unchanged(_) => unreachable!(),
            };
            record_event(
                client.clone(),
                &*cr,
                EventType::Normal,
                "Sync",
                reason,
                note,
                FIELD_MANAGER,
            )
            .await?;
        }

        // 3. Garbage-collect stale ConfigMaps previously owned by this CR.
        gc_resources::<ConfigMap, _>(client.clone(), Namespaced(target_ns), MANAGED_LABEL, |cm| {
            cm.name_any() == cm_name
        })
        .await?;

        // 4. Stamp the target namespace as a label on the CR.
        patch_labels::<ConfigMapSync, _>(
            client.clone(),
            Namespaced(&namespace),
            &name,
            &[("configmapsync.example.io/synced-to", target_ns)],
        )
        .await?;

        // 5. Write the full status in one SSA patch.
        let generation = cr.metadata.generation;
        let status_message = format!("ConfigMap '{cm_name}' synced to namespace '{target_ns}'");

        let mut conditions = cr
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        upsert_condition(
            &mut conditions,
            make_condition(
                "Ready",
                "True",
                "ConfigMapSynced",
                &status_message,
                generation,
            ),
        );

        patch_status_namespaced::<ConfigMapSync, ConfigMapSyncStatus>(
            client.clone(),
            &namespace,
            &name,
            ConfigMapSyncStatus {
                ready: true,
                message: status_message,
                conditions,
            },
            FIELD_MANAGER,
        )
        .await?;

        info!(cr = %name, "reconcile complete");
        Ok(Action::requeue(Duration::from_secs(300)))
    }

    fn error_policy(
        &self,
        cr: Arc<ConfigMapSync>,
        error: &KubeGenericError,
        _ctx: Arc<Context>,
    ) -> Action {
        warn!(cr = %cr.name_any(), error = %error, "reconcile failed — retrying in 30s");
        Action::requeue(Duration::from_secs(30))
    }
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
        metadata: ObjectMetaBuilder::new()
            .name(name)
            .namespace(namespace)
            .label("app.kubernetes.io/managed-by", "configmapsync-operator")
            .label("configmapsync.example.io/owner", owner_cr)
            .build(),
        data: Some(data.clone()),
        ..Default::default()
    }
}
