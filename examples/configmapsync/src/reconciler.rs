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

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta};
use kube::ResourceExt;
use tokio::time::Duration;
use tracing::{error, info, warn};

use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::Resource;

use koprs::controller::{Action, Context, Reconciler};
use koprs::error::KubeGenericError;
use koprs::finalizers::{add_finalizer_namespaced, remove_finalizers_namespaced};
use koprs::gc::gc_namespaced_resources;
use koprs::resources::{
    delete_namespaced_resource, ensure_namespaced_resource, patch_labels_namespaced, EnsureOutcome,
};
use koprs::status::{make_condition, patch_status_namespaced, upsert_condition};

use crate::types::{ConfigMapSync, ConfigMapSyncStatus, SyncCondition};

const FINALIZER: &str = "configmapsync.example.io/cleanup";
const FIELD_MANAGER: &str = "configmapsync-operator";
const MANAGED_LABEL: &str = "app.kubernetes.io/managed-by=configmapsync-operator";

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
// Reconciler
// ---------------------------------------------------------------------------

pub struct ConfigMapSyncReconciler;

impl Reconciler<ConfigMapSync> for ConfigMapSyncReconciler {
    type Error = ReconcileError;

    async fn reconcile(
        &self,
        cr: Arc<ConfigMapSync>,
        ctx: Arc<Context>,
    ) -> Result<Action, ReconcileError> {
        let client = ctx.client.clone();
        let name = cr.name_any();
        let namespace = cr
            .namespace()
            .ok_or(ReconcileError::MissingField("namespace"))?;

        info!(cr = %name, ns = %namespace, "reconciling ConfigMapSync");

        // -------------------------------------------------------------------
        // Deletion path
        // -------------------------------------------------------------------
        if cr.metadata.deletion_timestamp.is_some() {
            info!(cr = %name, "deletion timestamp set — running cleanup");

            let target_ns = &cr.spec.target_namespace;
            let cm_name = configmap_name(&name);

            match delete_namespaced_resource::<ConfigMap>(client.clone(), target_ns, &cm_name).await
            {
                Ok(true) => info!(cm = %cm_name, ns = %target_ns, "deleted synced ConfigMap"),
                Ok(false) => info!(cm = %cm_name, "ConfigMap was already gone"),
                Err(e) => {
                    error!(error = %e, "failed to delete ConfigMap during cleanup");
                    return Err(e.into());
                }
            }

            remove_finalizers_namespaced::<ConfigMapSync>(client.clone(), &namespace, &name)
                .await?;
            info!(cr = %name, "finalizer removed — deletion complete");
            return Ok(Action::await_change());
        }

        // -------------------------------------------------------------------
        // Normal reconcile path
        // -------------------------------------------------------------------

        // 1. Ensure finalizer is present.
        add_finalizer_namespaced::<ConfigMapSync>(client.clone(), &cr, FINALIZER).await?;

        // 2. Build and ensure the desired ConfigMap via koprs SSA.
        //    ensure_namespaced_resource returns an EnsureOutcome so we can emit a
        //    Kubernetes Event only when the ConfigMap was actually written.  Plain
        //    SSA is already idempotent, but Kubernetes Events are append-only — we
        //    must not publish one on every no-op reconcile.
        let target_ns = &cr.spec.target_namespace;
        let cm_name = configmap_name(&name);
        let desired_cm = build_configmap(&cm_name, target_ns, &name, &cr.spec.data);

        let outcome = ensure_namespaced_resource::<ConfigMap>(
            client.clone(),
            target_ns,
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
            let recorder = Recorder::new(
                client.clone(),
                Reporter { controller: FIELD_MANAGER.into(), instance: None },
            );
            recorder
                .publish(
                    &Event {
                        type_: EventType::Normal,
                        reason: reason.into(),
                        note: Some(note),
                        action: "Sync".into(),
                        secondary: None,
                    },
                    &cr.object_ref(&()),
                )
                .await
                .map_err(kube::Error::from)
                .map_err(KubeGenericError::from)?;
        }

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

        // 5. Write the full status in one SSA patch so a single field manager owns
        //    all status fields. Two separate patches by the same manager would cause
        //    each one to drop the other's fields on every reconcile, triggering an
        //    endless watch-event loop.
        let generation = cr.metadata.generation;
        let status_message = format!("ConfigMap '{cm_name}' synced to namespace '{target_ns}'");

        let mut conditions: Vec<Condition> = cr
            .status
            .as_ref()
            .map(|s| s.conditions.iter().map(sync_to_k8s_condition).collect())
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
                conditions: conditions.into_iter().map(k8s_to_sync_condition).collect(),
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
        error: &ReconcileError,
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

fn sync_to_k8s_condition(sc: &SyncCondition) -> Condition {
    use chrono::DateTime;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    Condition {
        type_: sc.type_.clone(),
        status: sc.status.clone(),
        reason: sc.reason.clone(),
        message: sc.message.clone(),
        observed_generation: sc.observed_generation,
        last_transition_time: Time(
            DateTime::parse_from_rfc3339(&sc.last_transition_time)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .unwrap_or_else(|_| chrono::Utc::now()),
        ),
    }
}

fn k8s_to_sync_condition(c: Condition) -> SyncCondition {
    use chrono::SecondsFormat;
    SyncCondition {
        type_: c.type_,
        status: c.status,
        reason: c.reason,
        message: c.message,
        last_transition_time: c
            .last_transition_time
            .0
            .to_rfc3339_opts(SecondsFormat::Secs, true),
        observed_generation: c.observed_generation,
    }
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
