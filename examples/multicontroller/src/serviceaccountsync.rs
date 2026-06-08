// src/serviceaccountsync.rs
//
// Reconciler for the ServiceAccountSync CRD — ensures a ServiceAccount with
// the spec's image-pull secrets exists in the target namespace. Structurally
// identical to the SecretSync reconciler; the two run as independent
// controllers (see main.rs) so CRs of either kind are reconciled concurrently.

use std::sync::Arc;

use k8s_openapi::api::core::v1::{LocalObjectReference, ServiceAccount};
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

use crate::types::{ServiceAccountSync, ServiceAccountSyncStatus};

const FINALIZER: &str = "multicontroller.example.io/serviceaccountsync-cleanup";
const FIELD_MANAGER: &str = "multicontroller-serviceaccountsync";
const MANAGED_LABEL: &str = "app.kubernetes.io/managed-by=multicontroller-serviceaccountsync";

pub struct ServiceAccountSyncReconciler;

impl Reconciler<ServiceAccountSync> for ServiceAccountSyncReconciler {
    type Error = KubeGenericError;

    async fn reconcile(
        &self,
        cr: Arc<ServiceAccountSync>,
        ctx: Arc<Context>,
    ) -> Result<Action, KubeGenericError> {
        let client = ctx.client.clone();
        let name = cr.name_any();
        let namespace = cr
            .namespace()
            .ok_or(KubeGenericError::MissingMetadata("namespace".into()))?;

        info!(cr = %name, ns = %namespace, "reconciling ServiceAccountSync");

        // -------------------------------------------------------------------
        // Deletion path
        // -------------------------------------------------------------------
        if is_being_deleted(&*cr) {
            info!(cr = %name, "deletion timestamp set — running cleanup");

            let target_ns = &cr.spec.target_namespace;
            let sa_name = service_account_name(&name);

            match delete_resource::<ServiceAccount, _>(
                client.clone(),
                Namespaced(target_ns),
                &sa_name,
            )
            .await
            {
                Ok(true) => info!(sa = %sa_name, ns = %target_ns, "deleted synced ServiceAccount"),
                Ok(false) => info!(sa = %sa_name, "ServiceAccount was already gone"),
                Err(e) => {
                    error!(error = %e, "failed to delete ServiceAccount during cleanup");
                    return Err(e.into());
                }
            }

            remove_finalizers::<ServiceAccountSync, _>(
                client.clone(),
                Namespaced(&namespace),
                &name,
            )
            .await?;
            info!(cr = %name, "finalizer removed — deletion complete");
            return Ok(Action::await_change());
        }

        // -------------------------------------------------------------------
        // Normal reconcile path
        // -------------------------------------------------------------------

        // 1. Ensure finalizer is present.
        add_finalizer_namespaced::<ServiceAccountSync>(client.clone(), &cr, FINALIZER).await?;

        // 2. Build and ensure the desired ServiceAccount.
        let target_ns = &cr.spec.target_namespace;
        let sa_name = service_account_name(&name);
        let desired_sa = build_service_account(
            &sa_name,
            target_ns,
            &name,
            cr.spec.automount_token,
            &cr.spec.image_pull_secrets,
        );

        let outcome = ensure_resource::<ServiceAccount, _>(
            client.clone(),
            Namespaced(target_ns),
            &desired_sa,
            FIELD_MANAGER,
        )
        .await?;
        info!(sa = %sa_name, ns = %target_ns, "applied ServiceAccount");

        if outcome.was_changed() {
            let (reason, note) = match &outcome {
                EnsureOutcome::Created(_) => (
                    "ServiceAccountCreated",
                    format!("ServiceAccount '{sa_name}' created in namespace '{target_ns}'"),
                ),
                EnsureOutcome::Updated(_) => (
                    "ServiceAccountDriftCorrected",
                    format!("ServiceAccount '{sa_name}' corrected in namespace '{target_ns}'"),
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

        // 3. Garbage-collect stale ServiceAccounts previously owned by this CR.
        gc_resources::<ServiceAccount, _>(
            client.clone(),
            Namespaced(target_ns),
            MANAGED_LABEL,
            |sa| sa.name_any() == sa_name,
        )
        .await?;

        // 4. Stamp the target namespace as a label on the CR.
        patch_labels::<ServiceAccountSync, _>(
            client.clone(),
            Namespaced(&namespace),
            &name,
            &[("multicontroller.example.io/synced-to", target_ns)],
        )
        .await?;

        // 5. Write the full status in one SSA patch.
        let generation = cr.metadata.generation;
        let status_message =
            format!("ServiceAccount '{sa_name}' synced to namespace '{target_ns}'");

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
                "ServiceAccountSynced",
                &status_message,
                generation,
            ),
        );

        patch_status_namespaced::<ServiceAccountSync, ServiceAccountSyncStatus>(
            client.clone(),
            &namespace,
            &name,
            ServiceAccountSyncStatus {
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
        cr: Arc<ServiceAccountSync>,
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

fn service_account_name(cr_name: &str) -> String {
    format!("sas-{cr_name}")
}

fn build_service_account(
    name: &str,
    namespace: &str,
    owner_cr: &str,
    automount_token: bool,
    image_pull_secrets: &[String],
) -> ServiceAccount {
    let refs: Vec<LocalObjectReference> = image_pull_secrets
        .iter()
        .map(|n| LocalObjectReference { name: n.clone() })
        .collect();

    ServiceAccount {
        metadata: ObjectMetaBuilder::new()
            .name(name)
            .namespace(namespace)
            .label(
                "app.kubernetes.io/managed-by",
                "multicontroller-serviceaccountsync",
            )
            .label("multicontroller.example.io/owner", owner_cr)
            .build(),
        automount_service_account_token: Some(automount_token),
        image_pull_secrets: if refs.is_empty() { None } else { Some(refs) },
        ..Default::default()
    }
}
