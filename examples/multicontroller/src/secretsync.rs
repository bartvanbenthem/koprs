// src/secretsync.rs
//
// Reconciler for the SecretSync CRD — ensures a Secret with the spec's
// string data exists in the target namespace. Structurally identical to
// the ServiceAccountSync reconciler; the two run as independent controllers
// (see main.rs) so CRs of either kind are reconciled concurrently.

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::Secret;
use kube::ResourceExt;
use tokio::time::Duration;
use tracing::{error, info};

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

use crate::types::{SecretSync, SecretSyncStatus};

const FINALIZER: &str = "multicontroller.example.io/secretsync-cleanup";
const FIELD_MANAGER: &str = "multicontroller-secretsync";
const MANAGED_LABEL: &str = "app.kubernetes.io/managed-by=multicontroller-secretsync";

pub struct SecretSyncReconciler;

impl Reconciler<SecretSync> for SecretSyncReconciler {
    type Error = KubeGenericError;

    async fn reconcile(
        &self,
        cr: Arc<SecretSync>,
        ctx: Arc<Context>,
    ) -> Result<Action, KubeGenericError> {
        let client = ctx.client.clone();
        let name = cr.name_any();
        let namespace = cr
            .namespace()
            .ok_or(KubeGenericError::MissingMetadata("namespace".into()))?;

        info!(cr = %name, ns = %namespace, "reconciling SecretSync");

        // -------------------------------------------------------------------
        // Deletion path
        // -------------------------------------------------------------------
        if is_being_deleted(&*cr) {
            info!(cr = %name, "deletion timestamp set — running cleanup");

            let target_ns = &cr.spec.target_namespace;
            let secret_name = secret_name(&name);

            match delete_resource::<Secret, _>(client.clone(), Namespaced(target_ns), &secret_name)
                .await
            {
                Ok(true) => info!(secret = %secret_name, ns = %target_ns, "deleted synced Secret"),
                Ok(false) => info!(secret = %secret_name, "Secret was already gone"),
                Err(e) => {
                    error!(error = %e, "failed to delete Secret during cleanup");
                    return Err(e.into());
                }
            }

            remove_finalizers::<SecretSync, _>(client.clone(), Namespaced(&namespace), &name)
                .await?;
            info!(cr = %name, "finalizer removed — deletion complete");
            return Ok(Action::await_change());
        }

        // -------------------------------------------------------------------
        // Normal reconcile path
        // -------------------------------------------------------------------

        // 1. Ensure finalizer is present.
        add_finalizer_namespaced::<SecretSync>(client.clone(), &cr, FINALIZER).await?;

        // 2. Build and ensure the desired Secret.
        let target_ns = &cr.spec.target_namespace;
        let secret_name = secret_name(&name);
        let desired_secret = build_secret(&secret_name, target_ns, &name, &cr.spec.string_data);

        let outcome = ensure_resource::<Secret, _>(
            client.clone(),
            Namespaced(target_ns),
            &desired_secret,
            FIELD_MANAGER,
        )
        .await?;
        info!(secret = %secret_name, ns = %target_ns, "applied Secret");

        if outcome.was_changed() {
            let (reason, note) = match &outcome {
                EnsureOutcome::Created(_) => (
                    "SecretCreated",
                    format!("Secret '{secret_name}' created in namespace '{target_ns}'"),
                ),
                EnsureOutcome::Updated(_) => (
                    "SecretDriftCorrected",
                    format!("Secret '{secret_name}' corrected in namespace '{target_ns}'"),
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

        // 3. Garbage-collect stale Secrets previously owned by this CR.
        gc_resources::<Secret, _>(client.clone(), Namespaced(target_ns), MANAGED_LABEL, |s| {
            s.name_any() == secret_name
        })
        .await?;

        // 4. Stamp the target namespace as a label on the CR.
        patch_labels::<SecretSync, _>(
            client.clone(),
            Namespaced(&namespace),
            &name,
            &[("multicontroller.example.io/synced-to", target_ns)],
        )
        .await?;

        // 5. Write the full status in one SSA patch.
        let generation = cr.metadata.generation;
        let status_message = format!("Secret '{secret_name}' synced to namespace '{target_ns}'");

        let mut conditions = cr
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        upsert_condition(
            &mut conditions,
            make_condition("Ready", "True", "SecretSynced", &status_message, generation),
        );

        patch_status_namespaced::<SecretSync, SecretSyncStatus>(
            client.clone(),
            &namespace,
            &name,
            SecretSyncStatus {
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
        cr: Arc<SecretSync>,
        error: &KubeGenericError,
        _ctx: Arc<Context>,
    ) -> Action {
        error!(cr = %cr.name_any(), error = %error, "reconcile failed — retrying in 5s");
        Action::requeue(Duration::from_secs(5))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn secret_name(cr_name: &str) -> String {
    format!("ss-{cr_name}")
}

fn build_secret(
    name: &str,
    namespace: &str,
    owner_cr: &str,
    string_data: &BTreeMap<String, String>,
) -> Secret {
    Secret {
        metadata: ObjectMetaBuilder::new()
            .name(name)
            .namespace(namespace)
            .label("app.kubernetes.io/managed-by", "multicontroller-secretsync")
            .label("multicontroller.example.io/owner", owner_cr)
            .build(),
        type_: Some("Opaque".to_string()),
        string_data: Some(string_data.clone()),
        ..Default::default()
    }
}
